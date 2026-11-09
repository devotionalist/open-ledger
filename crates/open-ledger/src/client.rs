//! Client algorithms: quorum append, fence coverage, ledger recovery.
//!
//! These run in the broker (or a test), across bookies. A single bookie cannot
//! seal a ledger tail — striping means its highest entry is not the ledger tail.

use std::collections::{BTreeSet, HashMap, HashSet};

use bytes::Bytes;

use crate::distribution::{fragment_for, write_quorum_covered, write_set};
use crate::error::{Error, Result};
use crate::meta::{validate_ensemble, LedgerMeta, LedgerMetaStore};
use crate::store::Bookie;
use crate::types::{BookieId, Ensemble, Entry, EntryId, LedgerId, MetaState, Quorum};

const CAS_ATTEMPTS: usize = 16;

/// Move LastAddConfirmed only through a contiguous confirmed prefix.
///
/// Receipts advance in entry-id order. An entry that reaches Qa
/// before an earlier hole does not advance LAC past that hole.
pub fn advance_lac(lac: i64, completed: &BTreeSet<EntryId>) -> i64 {
    let mut next = if lac < 0 {
        0
    } else {
        (lac as u64).saturating_add(1)
    };
    let mut cur = lac;
    while completed.contains(&next) {
        cur = next as i64;
        if next == u64::MAX {
            break;
        }
        next += 1;
    }
    cur
}

/// Create metadata, then `createLedger` on every ensemble member.
///
/// If metadata already exists with the same quorum, ensemble, and master key, provisioning is retried.
pub fn create_open_ledger(
    metas: &dyn LedgerMetaStore,
    bookies: &HashMap<BookieId, &dyn Bookie>,
    ledger_id: LedgerId,
    quorum: Quorum,
    ensemble: Ensemble,
    master_key: &[u8],
) -> Result<LedgerMeta> {
    let fresh = LedgerMeta::new_open(ledger_id, quorum, ensemble, master_key)?;
    let stored = match metas.create(fresh.clone()) {
        Ok(meta) => meta,
        Err(Error::AlreadyExists(_)) => {
            let existing = metas.get(ledger_id)?;
            if existing.quorum != fresh.quorum
                || existing.fragments != fresh.fragments
                || existing.master_key != fresh.master_key
            {
                return Err(Error::MetaConflict(format!(
                    "ledger {ledger_id} already exists with different parameters"
                )));
            }
            existing
        }
        Err(err) => return Err(err),
    };
    create_ledger_on_ensemble(
        bookies,
        &stored.fragments[0].ensemble,
        ledger_id,
        &stored.master_key,
    )?;
    Ok(stored)
}

/// `createLedger` on each bookie in `ensemble`.
pub fn create_ledger_on_ensemble(
    bookies: &HashMap<BookieId, &dyn Bookie>,
    ensemble: &[BookieId],
    ledger_id: LedgerId,
    master_key: &[u8],
) -> Result<()> {
    for id in ensemble {
        let bookie = bookies.get(id).ok_or(Error::NotEnoughBookies)?;
        bookie.create_ledger(ledger_id, master_key)?;
    }
    Ok(())
}

/// Append one entry to its write quorum. Success means at least `Qa` journal acks.
///
/// The ledger metadata must be [`MetaState::Open`]. `lac` is whatever the writer
/// has already confirmed; this function does not advance it.
pub fn append_entry(
    bookies: &HashMap<BookieId, &dyn Bookie>,
    meta: &LedgerMeta,
    entry_id: EntryId,
    lac: i64,
    payload: Bytes,
) -> Result<usize> {
    if meta.state != MetaState::Open {
        return Err(Error::InvalidArgument(
            "append requires ledger metadata OPEN".into(),
        ));
    }
    let ensemble = &fragment_for(meta, entry_id)?.ensemble;
    let targets = write_set(ensemble, meta.quorum.qw, entry_id)?;
    let mut acks = 0usize;
    let mut saw_fence = false;
    for id in targets {
        let Some(bookie) = bookies.get(&id) else {
            continue;
        };
        match bookie.add_entry(
            meta.ledger_id,
            entry_id,
            lac,
            payload.clone(),
            &meta.master_key,
        ) {
            Ok(()) => acks += 1,
            Err(Error::Fenced(_)) => saw_fence = true,
            Err(Error::Conflict {
                ledger_id,
                entry_id,
            }) => {
                return Err(Error::Conflict {
                    ledger_id,
                    entry_id,
                });
            }
            Err(Error::Unauthorized(id)) => return Err(Error::Unauthorized(id)),
            Err(_) => {}
        }
    }
    if acks >= meta.quorum.qa as usize {
        return Ok(acks);
    }
    if saw_fence {
        return Err(Error::Fenced(meta.ledger_id));
    }
    Err(Error::NotEnoughBookies)
}

/// Read one entry from its write set. One matching copy is enough.
///
/// If every contacted bookie reports the entry missing, returns [`Error::NoSuchEntry`].
/// If a bookie that might hold the only copy does not respond, returns [`Error::NotEnoughBookies`]
/// instead of treating the entry as absent.
pub fn read_entry_quorum(
    bookies: &HashMap<BookieId, &dyn Bookie>,
    meta: &LedgerMeta,
    entry_id: EntryId,
) -> Result<Entry> {
    match probe_entry(bookies, meta, entry_id, /*fence=*/ false)? {
        Some(entry) => Ok(entry),
        None => Err(Error::NoSuchEntry {
            ledger_id: meta.ledger_id,
            entry_id,
        }),
    }
}

/// Append a fragment. Allowed only while metadata is OPEN.
pub fn change_ensemble(
    metas: &dyn LedgerMetaStore,
    ledger_id: LedgerId,
    first_entry_id: EntryId,
    ensemble: Ensemble,
) -> Result<LedgerMeta> {
    for _ in 0..CAS_ATTEMPTS {
        let meta = metas.get(ledger_id)?;
        if meta.state != MetaState::Open {
            return Err(Error::InvalidArgument(
                "ensemble change requires ledger metadata OPEN".into(),
            ));
        }
        validate_ensemble(&ensemble, meta.quorum)?;
        let last = meta
            .fragments
            .last()
            .ok_or_else(|| Error::InvalidArgument("ledger has no fragments".into()))?;
        if first_entry_id <= last.first_entry_id {
            return Err(Error::InvalidArgument(
                "new fragment must start after the current fragment".into(),
            ));
        }
        let mut next = meta.clone();
        next.fragments.push(crate::types::Fragment {
            first_entry_id,
            ensemble: ensemble.clone(),
        });
        match metas.compare_and_swap(meta.meta_version, next) {
            Ok(stored) => return Ok(stored),
            Err(Error::VersionConflict(_)) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::MetaConflict(format!(
        "ensemble change on {ledger_id} did not converge"
    )))
}

/// Writer close: CAS metadata to CLOSED. Does not fence bookies.
///
/// The caller must already have stopped adding. Crash recovery uses [`recover_ledger`].
pub fn close_ledger(
    metas: &dyn LedgerMetaStore,
    ledger_id: LedgerId,
    last_entry_id: i64,
) -> Result<i64> {
    if last_entry_id < -1 {
        return Err(Error::InvalidArgument("last_entry_id must be >= -1".into()));
    }
    for _ in 0..CAS_ATTEMPTS {
        let meta = metas.get(ledger_id)?;
        match meta.state {
            MetaState::Closed => {
                if meta.last_entry_id == last_entry_id {
                    return Ok(last_entry_id);
                }
                return Err(Error::MetaConflict(format!(
                    "ledger {ledger_id} closed at {} not {last_entry_id}",
                    meta.last_entry_id
                )));
            }
            MetaState::InRecovery => {
                return Err(Error::InvalidArgument(
                    "ledger is in recovery; finish recover_ledger".into(),
                ));
            }
            MetaState::Open => {
                let mut next = meta.clone();
                next.state = MetaState::Closed;
                next.last_entry_id = last_entry_id;
                match metas.compare_and_swap(meta.meta_version, next) {
                    Ok(_) => return Ok(last_entry_id),
                    Err(Error::VersionConflict(_)) => continue,
                    Err(err) => return Err(err),
                }
            }
        }
    }
    Err(Error::MetaConflict(format!(
        "close of {ledger_id} did not converge"
    )))
}

/// Fence until coverage, then forward-scan with **fenced recovery reads** (BK #2614),
/// repair with recovery adds, CAS CLOSED.
///
/// Idempotent. A second call after CLOSED returns the same `last_entry_id`.
/// If coverage or a read cannot be decided, metadata stays `IN_RECOVERY` and the error is returned.
pub fn recover_ledger(
    metas: &dyn LedgerMetaStore,
    bookies: &HashMap<BookieId, &dyn Bookie>,
    ledger_id: LedgerId,
) -> Result<i64> {
    for _ in 0..CAS_ATTEMPTS {
        match recover_once(metas, bookies, ledger_id)? {
            RecoverStep::Done(last) => return Ok(last),
            RecoverStep::Retry => continue,
        }
    }
    Err(Error::MetaConflict(format!(
        "recover of {ledger_id} did not converge"
    )))
}

enum RecoverStep {
    Done(i64),
    Retry,
}

fn recover_once(
    metas: &dyn LedgerMetaStore,
    bookies: &HashMap<BookieId, &dyn Bookie>,
    ledger_id: LedgerId,
) -> Result<RecoverStep> {
    let meta = metas.get(ledger_id)?;
    if meta.state == MetaState::Closed {
        return Ok(RecoverStep::Done(meta.last_entry_id));
    }
    if meta.state == MetaState::Open {
        let mut next = meta.clone();
        next.state = MetaState::InRecovery;
        match metas.compare_and_swap(meta.meta_version, next) {
            Ok(_) => {}
            Err(Error::VersionConflict(_)) => return Ok(RecoverStep::Retry),
            Err(err) => return Err(err),
        }
    }

    let meta = metas.get(ledger_id)?;
    if meta.state == MetaState::Closed {
        return Ok(RecoverStep::Done(meta.last_entry_id));
    }
    fence_to_coverage(bookies, &meta)?;
    let max_lac = max_lac(bookies, &meta)?;
    let last = scan_tail(bookies, &meta, max_lac)?;

    let meta = metas.get(ledger_id)?;
    if meta.state == MetaState::Closed {
        return Ok(RecoverStep::Done(meta.last_entry_id));
    }
    if meta.state != MetaState::InRecovery {
        return Ok(RecoverStep::Retry);
    }
    let mut next = meta.clone();
    next.state = MetaState::Closed;
    next.last_entry_id = last;
    match metas.compare_and_swap(meta.meta_version, next) {
        Ok(_) => Ok(RecoverStep::Done(last)),
        Err(Error::VersionConflict(_)) => {
            let cur = metas.get(ledger_id)?;
            if cur.state == MetaState::Closed {
                Ok(RecoverStep::Done(cur.last_entry_id))
            } else {
                Ok(RecoverStep::Retry)
            }
        }
        Err(err) => Err(err),
    }
}

fn fence_to_coverage(bookies: &HashMap<BookieId, &dyn Bookie>, meta: &LedgerMeta) -> Result<()> {
    let mut fenced = HashSet::new();
    let mut seen = HashSet::new();
    for frag in &meta.fragments {
        for id in &frag.ensemble {
            if !seen.insert(id.clone()) {
                continue;
            }
            let Some(bookie) = bookies.get(id) else {
                continue;
            };
            match bookie.fence_ledger(meta.ledger_id, &meta.master_key) {
                Ok(()) => {
                    fenced.insert(id.clone());
                }
                Err(Error::Unauthorized(_)) => return Err(Error::Unauthorized(meta.ledger_id)),
                Err(_) => {}
            }
        }
    }
    for frag in &meta.fragments {
        if !write_quorum_covered(&frag.ensemble, meta.quorum, &fenced) {
            return Err(Error::NotEnoughBookies);
        }
    }
    Ok(())
}

fn max_lac(bookies: &HashMap<BookieId, &dyn Bookie>, meta: &LedgerMeta) -> Result<i64> {
    let ensemble = &meta
        .fragments
        .last()
        .ok_or_else(|| Error::InvalidArgument("ledger has no fragments".into()))?
        .ensemble;
    let mut heard = 0usize;
    let mut max_lac = -1i64;
    for id in ensemble {
        let Some(bookie) = bookies.get(id) else {
            continue;
        };
        match bookie.read_last_confirmed(meta.ledger_id) {
            Ok(lac) => {
                heard += 1;
                if lac > max_lac {
                    max_lac = lac;
                }
            }
            Err(Error::NotFound(_)) => heard += 1,
            Err(_) => {}
        }
    }
    if heard < meta.quorum.qa as usize {
        return Err(Error::NotEnoughBookies);
    }
    if max_lac < -1 {
        max_lac = -1;
    }
    Ok(max_lac)
}

fn scan_tail(
    bookies: &HashMap<BookieId, &dyn Bookie>,
    meta: &LedgerMeta,
    max_lac: i64,
) -> Result<i64> {
    let mut last = max_lac;
    let mut entry_id = if max_lac < 0 {
        0
    } else {
        u64::try_from(max_lac)
            .map_err(|_| Error::InvalidArgument("lac does not fit in entry id".into()))?
            .saturating_add(1)
    };
    loop {
        if i64::try_from(entry_id).is_err() {
            return Err(Error::InvalidArgument("entry id exceeds i64".into()));
        }
        // RecoveryReadsFence (BK #2614): fence each write-set member before the probe.
        match probe_entry(bookies, meta, entry_id, /*fence=*/ true)? {
            None => return Ok(last),
            Some(entry) => {
                repair(bookies, meta, &entry)?;
                last = entry_id as i64;
                entry_id = entry_id
                    .checked_add(1)
                    .ok_or_else(|| Error::InvalidArgument("entry id overflow".into()))?;
            }
        }
    }
}

/// `Ok(None)` — every responding write-set bookie lacks the entry.
/// `Err(NotEnoughBookies)` — some bookie did not respond, so absence is not proven.
///
/// When `fence` is true, fence each contacted bookie before reading so a negative
/// answer cannot later flip to a successful add (BK #2614 / RecoveryReadsFence).
fn probe_entry(
    bookies: &HashMap<BookieId, &dyn Bookie>,
    meta: &LedgerMeta,
    entry_id: EntryId,
    fence: bool,
) -> Result<Option<Entry>> {
    let ensemble = &fragment_for(meta, entry_id)?.ensemble;
    let targets = write_set(ensemble, meta.quorum.qw, entry_id)?;
    let mut found: Option<Entry> = None;
    let mut any_down = false;
    let mut any_absence = false;
    for id in targets {
        let Some(bookie) = bookies.get(&id) else {
            any_down = true;
            continue;
        };
        if fence {
            if let Err(err) = bookie.fence_ledger(meta.ledger_id, &meta.master_key) {
                if matches!(err, Error::Unauthorized(_)) {
                    return Err(Error::Unauthorized(meta.ledger_id));
                }
                any_down = true;
                continue;
            }
        }
        match bookie.read_entry(meta.ledger_id, entry_id) {
            Ok(entry) => {
                if let Some(prev) = &found {
                    if prev.payload.as_ref() != entry.payload.as_ref() {
                        return Err(Error::Conflict {
                            ledger_id: meta.ledger_id,
                            entry_id,
                        });
                    }
                } else {
                    found = Some(entry);
                }
            }
            Err(Error::NoSuchEntry { .. } | Error::NotFound(_)) => any_absence = true,
            Err(_) => any_down = true,
        }
    }
    if found.is_some() {
        return Ok(found);
    }
    if any_down || !any_absence {
        return Err(Error::NotEnoughBookies);
    }
    Ok(None)
}

fn repair(
    bookies: &HashMap<BookieId, &dyn Bookie>,
    meta: &LedgerMeta,
    entry: &Entry,
) -> Result<()> {
    let ensemble = &fragment_for(meta, entry.entry_id)?.ensemble;
    let targets = write_set(ensemble, meta.quorum.qw, entry.entry_id)?;
    let mut acks = 0usize;
    for id in targets {
        let Some(bookie) = bookies.get(&id) else {
            continue;
        };
        match bookie.recovery_add_entry(
            meta.ledger_id,
            entry.entry_id,
            entry.lac,
            entry.payload.clone(),
            &meta.master_key,
        ) {
            Ok(()) => acks += 1,
            Err(Error::Conflict {
                ledger_id,
                entry_id,
            }) => {
                return Err(Error::Conflict {
                    ledger_id,
                    entry_id,
                });
            }
            Err(_) => {}
        }
    }
    if acks < meta.quorum.qa as usize {
        Err(Error::NotEnoughBookies)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryBookie;
    use crate::meta::MemoryMetaStore;

    const KEY: &[u8] = b"k";

    struct Cluster {
        bookies: Vec<MemoryBookie>,
        ids: Vec<BookieId>,
    }

    impl Cluster {
        fn new(n: usize) -> Self {
            Self {
                bookies: (0..n).map(|_| MemoryBookie::new()).collect(),
                ids: (0..n).map(|i| format!("b{i}")).collect(),
            }
        }

        fn map(&self) -> HashMap<BookieId, &dyn Bookie> {
            self.ids
                .iter()
                .cloned()
                .zip(self.bookies.iter().map(|b| b as &dyn Bookie))
                .collect()
        }
    }

    fn q(e: u32, qw: u32, qa: u32) -> Quorum {
        Quorum { e, qw, qa }
    }

    #[test]
    fn lac_does_not_skip_holes() {
        let done = BTreeSet::from([0, 1, 3]);
        assert_eq!(advance_lac(-1, &done), 1);
        let done = BTreeSet::from([0, 1, 2, 3]);
        assert_eq!(advance_lac(1, &done), 3);
    }

    #[test]
    fn confirmed_entries_survive_recovery() {
        let cluster = Cluster::new(3);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let meta =
            create_open_ledger(&metas, &map, 1, q(3, 3, 2), cluster.ids.clone(), KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"a")).unwrap();
        append_entry(&map, &meta, 1, 0, Bytes::from_static(b"b")).unwrap();

        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), 1);
        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), 1);
        let closed = metas.get(1).unwrap();
        assert_eq!(closed.state, MetaState::Closed);
        assert_eq!(
            read_entry_quorum(&map, &closed, 1)
                .unwrap()
                .payload
                .as_ref(),
            b"b"
        );
        assert!(matches!(
            append_entry(&map, &closed, 2, 1, Bytes::from_static(b"c")),
            Err(Error::InvalidArgument(_))
        ));
        assert!(cluster.bookies.iter().all(|b| b.is_fenced(1).unwrap()));
    }

    #[test]
    fn dirty_tail_on_one_bookie_is_repaired() {
        let cluster = Cluster::new(3);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let meta =
            create_open_ledger(&metas, &map, 1, q(3, 3, 2), cluster.ids.clone(), KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"a")).unwrap();
        append_entry(&map, &meta, 1, 0, Bytes::from_static(b"b")).unwrap();
        cluster.bookies[0]
            .add_entry(1, 2, 0, Bytes::from_static(b"dirty"), KEY)
            .unwrap();

        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), 2);
        for b in &cluster.bookies {
            assert_eq!(b.read_entry(1, 2).unwrap().payload.as_ref(), b"dirty");
        }
    }

    #[test]
    fn hole_stops_the_scan_even_if_a_later_entry_exists() {
        let cluster = Cluster::new(1);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let meta =
            create_open_ledger(&metas, &map, 1, Quorum::ONE, cluster.ids.clone(), KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"a")).unwrap();
        cluster.bookies[0]
            .add_entry(1, 5, 0, Bytes::from_static(b"skip"), KEY)
            .unwrap();
        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), 0);
        assert_eq!(
            cluster.bookies[0]
                .read_entry(1, 5)
                .unwrap()
                .payload
                .as_ref(),
            b"skip"
        );
    }

    #[test]
    fn fenced_quorum_blocks_old_writer_from_qa() {
        let cluster = Cluster::new(3);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let meta =
            create_open_ledger(&metas, &map, 1, q(3, 3, 2), cluster.ids.clone(), KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"a")).unwrap();
        cluster.bookies[0].fence_ledger(1, KEY).unwrap();
        cluster.bookies[1].fence_ledger(1, KEY).unwrap();
        let err = append_entry(&map, &meta, 1, 0, Bytes::from_static(b"zombie")).unwrap_err();
        assert!(matches!(err, Error::Fenced(1)));
        // The unfenced bookie stored the add; the fenced ones did not. Qa was not reached.
        assert!(cluster.bookies[2].read_entry(1, 1).is_ok());
        assert!(cluster.bookies[0].read_entry(1, 1).is_err());
    }

    #[test]
    fn striping_places_entries_on_rotating_quorums() {
        let cluster = Cluster::new(5);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let meta =
            create_open_ledger(&metas, &map, 1, q(5, 3, 2), cluster.ids.clone(), KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"e0")).unwrap();
        append_entry(&map, &meta, 1, 0, Bytes::from_static(b"e1")).unwrap();
        assert_eq!(cluster.bookies[0].list_entries(1).unwrap(), vec![0]);
        assert_eq!(cluster.bookies[1].list_entries(1).unwrap(), vec![0, 1]);
        assert_eq!(cluster.bookies[3].list_entries(1).unwrap(), vec![1]);
        assert!(cluster.bookies[4].list_entries(1).unwrap().is_empty());
        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), 1);
        assert_eq!(
            read_entry_quorum(&map, &metas.get(1).unwrap(), 1)
                .unwrap()
                .payload
                .as_ref(),
            b"e1"
        );
    }

    #[test]
    fn ensemble_change_uses_the_new_write_set() {
        let cluster = Cluster::new(4);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let first: Ensemble = cluster.ids[..3].to_vec();
        let meta = create_open_ledger(&metas, &map, 1, q(3, 3, 2), first, KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"old")).unwrap();
        let next: Ensemble = cluster.ids[1..].to_vec();
        let meta = change_ensemble(&metas, 1, 1, next).unwrap();
        create_ledger_on_ensemble(&map, &meta.fragments[1].ensemble, 1, KEY).unwrap();
        append_entry(&map, &meta, 1, 0, Bytes::from_static(b"new")).unwrap();
        assert!(cluster.bookies[0].read_entry(1, 1).is_err());
        assert_eq!(
            cluster.bookies[3]
                .read_entry(1, 1)
                .unwrap()
                .payload
                .as_ref(),
            b"new"
        );
        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), 1);
    }

    #[test]
    fn empty_ledger_closes_at_minus_one() {
        let cluster = Cluster::new(1);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        create_open_ledger(&metas, &map, 4, Quorum::ONE, cluster.ids.clone(), KEY).unwrap();
        assert_eq!(recover_ledger(&metas, &map, 4).unwrap(), -1);
        assert_eq!(close_ledger(&metas, 4, -1).unwrap(), -1);
    }

    #[test]
    fn missing_bookies_leave_recovery_open() {
        let cluster = Cluster::new(3);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        create_open_ledger(&metas, &map, 1, q(3, 3, 2), cluster.ids.clone(), KEY).unwrap();
        let empty = HashMap::<BookieId, &dyn Bookie>::new();
        assert!(matches!(
            recover_ledger(&metas, &empty, 1),
            Err(Error::NotEnoughBookies)
        ));
        assert_eq!(metas.get(1).unwrap().state, MetaState::InRecovery);
        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), -1);
    }

    /// BK #2614: coverage can leave a bookie unfenced; recover must fence it before read.
    #[test]
    fn recovery_reads_fence_uncovered_bookies() {
        let cluster = Cluster::new(3);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let meta =
            create_open_ledger(&metas, &map, 1, q(3, 3, 2), cluster.ids.clone(), KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"a")).unwrap();

        // Qa=2 coverage needs 2 fences; leave bookie-2 unfenced.
        cluster.bookies[0].fence_ledger(1, KEY).unwrap();
        cluster.bookies[1].fence_ledger(1, KEY).unwrap();
        assert!(!cluster.bookies[2].is_fenced(1).unwrap());

        assert_eq!(recover_ledger(&metas, &map, 1).unwrap(), 0);
        assert!(cluster.bookies[2].is_fenced(1).unwrap());
        assert!(matches!(
            cluster.bookies[2].add_entry(1, 1, 0, Bytes::from_static(b"late"), KEY),
            Err(Error::Fenced(1))
        ));
        let closed = metas.get(1).unwrap();
        assert_eq!(closed.state, MetaState::Closed);
        assert_eq!(closed.last_entry_id, 0);
    }

    /// C07: delay-injected bookie still completes append (Qa reached).
    #[test]
    fn slow_bookie_add_still_reaches_qa() {
        use std::time::{Duration, Instant};

        struct Slow<'a>(&'a MemoryBookie, Duration);
        impl Bookie for Slow<'_> {
            fn create_ledger(&self, id: LedgerId, key: &[u8]) -> Result<()> {
                self.0.create_ledger(id, key)
            }
            fn add_entry(
                &self,
                id: LedgerId,
                entry_id: EntryId,
                lac: i64,
                payload: Bytes,
                key: &[u8],
            ) -> Result<()> {
                std::thread::sleep(self.1);
                self.0.add_entry(id, entry_id, lac, payload, key)
            }
            fn recovery_add_entry(
                &self,
                id: LedgerId,
                entry_id: EntryId,
                lac: i64,
                payload: Bytes,
                key: &[u8],
            ) -> Result<()> {
                self.0.recovery_add_entry(id, entry_id, lac, payload, key)
            }
            fn read_entry(&self, id: LedgerId, entry_id: EntryId) -> Result<Entry> {
                self.0.read_entry(id, entry_id)
            }
            fn read_entries(
                &self,
                id: LedgerId,
                from: EntryId,
                max: usize,
            ) -> Result<Vec<Entry>> {
                self.0.read_entries(id, from, max)
            }
            fn list_entries(&self, id: LedgerId) -> Result<Vec<EntryId>> {
                self.0.list_entries(id)
            }
            fn fence_ledger(&self, id: LedgerId, key: &[u8]) -> Result<()> {
                self.0.fence_ledger(id, key)
            }
            fn set_explicit_lac(&self, id: LedgerId, lac: i64, key: &[u8]) -> Result<()> {
                self.0.set_explicit_lac(id, lac, key)
            }
            fn read_last_confirmed(&self, id: LedgerId) -> Result<i64> {
                self.0.read_last_confirmed(id)
            }
            fn last_entry_id(&self, id: LedgerId) -> Result<i64> {
                self.0.last_entry_id(id)
            }
            fn is_fenced(&self, id: LedgerId) -> Result<bool> {
                self.0.is_fenced(id)
            }
            fn delete_ledger(&self, id: LedgerId) -> Result<()> {
                self.0.delete_ledger(id)
            }
        }

        let b0 = MemoryBookie::new();
        let b1 = MemoryBookie::new();
        let b2 = MemoryBookie::new();
        let slow = Slow(&b2, Duration::from_millis(60));
        let ids: Vec<BookieId> = vec!["b0".into(), "b1".into(), "b2".into()];
        let map: HashMap<BookieId, &dyn Bookie> = [
            (ids[0].clone(), &b0 as &dyn Bookie),
            (ids[1].clone(), &b1 as &dyn Bookie),
            (ids[2].clone(), &slow as &dyn Bookie),
        ]
        .into_iter()
        .collect();
        let metas = MemoryMetaStore::new();
        let meta = create_open_ledger(&metas, &map, 1, q(3, 3, 2), ids, KEY).unwrap();
        let start = Instant::now();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"x")).unwrap();
        assert!(start.elapsed() >= Duration::from_millis(40));
        assert_eq!(b0.read_entry(1, 0).unwrap().payload.as_ref(), b"x");
    }

    /// C12: one bookie partitioned out of the map; Qa=2 still confirms.
    #[test]
    fn partitioned_bookie_still_reaches_qa() {
        let cluster = Cluster::new(3);
        let full = cluster.map();
        let metas = MemoryMetaStore::new();
        let meta =
            create_open_ledger(&metas, &full, 1, q(3, 3, 2), cluster.ids.clone(), KEY).unwrap();
        let mut live = full.clone();
        live.remove(&cluster.ids[2]);
        append_entry(&live, &meta, 0, -1, Bytes::from_static(b"ok")).unwrap();
        assert!(cluster.bookies[2].read_entry(1, 0).is_err());
        assert_eq!(
            cluster.bookies[0].read_entry(1, 0).unwrap().payload.as_ref(),
            b"ok"
        );
    }

    /// M2: change ensemble, drop an old member, keep writing, recover.
    #[test]
    fn ensemble_change_then_drop_old_member_recovers() {
        let cluster = Cluster::new(4);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let first: Ensemble = cluster.ids[..3].to_vec();
        let meta = create_open_ledger(&metas, &map, 1, q(3, 3, 2), first, KEY).unwrap();
        append_entry(&map, &meta, 0, -1, Bytes::from_static(b"old")).unwrap();
        let next: Ensemble = cluster.ids[1..].to_vec();
        let meta = change_ensemble(&metas, 1, 1, next).unwrap();
        create_ledger_on_ensemble(&map, &meta.fragments[1].ensemble, 1, KEY).unwrap();
        append_entry(&map, &meta, 1, 0, Bytes::from_static(b"new")).unwrap();

        let mut live = map.clone();
        live.remove(&cluster.ids[0]);
        append_entry(&live, &meta, 2, 1, Bytes::from_static(b"more")).unwrap();
        assert_eq!(recover_ledger(&metas, &live, 1).unwrap(), 2);
        let closed = metas.get(1).unwrap();
        assert_eq!(closed.state, MetaState::Closed);
        assert_eq!(
            read_entry_quorum(&live, &closed, 2)
                .unwrap()
                .payload
                .as_ref(),
            b"more"
        );
    }
}
