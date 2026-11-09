//! Sync and pipelined ledger writers (BK #4924 pending / ensemble reconciliation).

use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;

use crate::client::{
    advance_lac, append_entry, change_ensemble, close_ledger, create_ledger_on_ensemble,
    create_open_ledger, read_entry_quorum,
};
use crate::error::{Error, Result};
use crate::meta::{LedgerMeta, LedgerMetaStore};
use crate::store::Bookie;
use crate::types::{BookieId, Ensemble, Entry, EntryId, LedgerId, Quorum};

/// Single-writer handle. Each append waits for Qa before the next entry id is issued.
pub struct LedgerWriter<'a> {
    metas: &'a dyn LedgerMetaStore,
    bookies: &'a HashMap<BookieId, &'a dyn Bookie>,
    meta: LedgerMeta,
    next: EntryId,
    lac: i64,
}

impl<'a> LedgerWriter<'a> {
    /// Create an open ledger and a writer positioned at entry 0.
    pub fn create(
        metas: &'a dyn LedgerMetaStore,
        bookies: &'a HashMap<BookieId, &'a dyn Bookie>,
        ledger_id: LedgerId,
        quorum: Quorum,
        ensemble: Ensemble,
        master_key: &[u8],
    ) -> Result<Self> {
        let meta = create_open_ledger(metas, bookies, ledger_id, quorum, ensemble, master_key)?;
        Ok(Self {
            metas,
            bookies,
            meta,
            next: 0,
            lac: -1,
        })
    }

    /// Metadata this writer last observed.
    pub fn meta(&self) -> &LedgerMeta {
        &self.meta
    }

    /// LastAddConfirmed this writer has released.
    pub fn lac(&self) -> i64 {
        self.lac
    }

    /// Next entry id that will be assigned.
    pub fn next_entry_id(&self) -> EntryId {
        self.next
    }

    /// Append one payload. The entry piggybacks the previous LAC.
    pub fn append(&mut self, payload: Bytes) -> Result<EntryId> {
        let entry_id = self.next;
        append_entry(self.bookies, &self.meta, entry_id, self.lac, payload)?;
        self.next = entry_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("entry id overflow".into()))?;
        self.lac = entry_id as i64;
        Ok(entry_id)
    }

    /// Read one entry through the current ensemble.
    pub fn read(&self, entry_id: EntryId) -> Result<Entry> {
        read_entry_quorum(self.bookies, &self.meta, entry_id)
    }

    /// Switch ensemble at `first_entry_id` (must equal [`Self::next_entry_id`], BK #4924).
    pub fn change_ensemble(&mut self, first_entry_id: EntryId, ensemble: Ensemble) -> Result<()> {
        if first_entry_id != self.next {
            return Err(Error::InvalidArgument(format!(
                "ensemble change first_entry_id {first_entry_id} must equal next {}",
                self.next
            )));
        }
        let meta = change_ensemble(self.metas, self.meta.ledger_id, first_entry_id, ensemble)?;
        let newest = meta
            .fragments
            .last()
            .ok_or_else(|| Error::InvalidArgument("ledger has no fragments".into()))?;
        create_ledger_on_ensemble(
            self.bookies,
            &newest.ensemble,
            self.meta.ledger_id,
            &self.meta.master_key,
        )?;
        self.meta = meta;
        Ok(())
    }

    /// Seal metadata at the writer's LAC.
    pub fn close(self) -> Result<i64> {
        close_ledger(self.metas, self.meta.ledger_id, self.lac)
    }
}

/// Tracks in-flight appends so an ensemble change cannot retarget entries that
/// already left the writer (BK #4194 / #4924).
///
/// Contiguous LAC still uses [`advance_lac`]: confirming entry 3 before 2 does
/// not advance LAC past the hole.
pub struct PipelineWriter<'a> {
    metas: &'a dyn LedgerMetaStore,
    bookies: &'a HashMap<BookieId, &'a dyn Bookie>,
    meta: LedgerMeta,
    next: EntryId,
    lac: i64,
    pending: BTreeSet<EntryId>,
    completed: BTreeSet<EntryId>,
}

impl<'a> PipelineWriter<'a> {
    /// Create metadata + bookie storage and return a writer at entry 0.
    pub fn create(
        metas: &'a dyn LedgerMetaStore,
        bookies: &'a HashMap<BookieId, &'a dyn Bookie>,
        ledger_id: LedgerId,
        quorum: Quorum,
        ensemble: Ensemble,
        master_key: &[u8],
    ) -> Result<Self> {
        let meta = create_open_ledger(metas, bookies, ledger_id, quorum, ensemble, master_key)?;
        Ok(Self {
            metas,
            bookies,
            meta,
            next: 0,
            lac: -1,
            pending: BTreeSet::new(),
            completed: BTreeSet::new(),
        })
    }

    /// Last observed metadata.
    pub fn meta(&self) -> &LedgerMeta {
        &self.meta
    }

    /// LastAddConfirmed released by this writer.
    pub fn lac(&self) -> i64 {
        self.lac
    }

    /// Next entry id that will be assigned.
    pub fn next_entry_id(&self) -> EntryId {
        self.next
    }

    /// Entry ids submitted but not yet confirmed.
    pub fn pending(&self) -> &BTreeSet<EntryId> {
        &self.pending
    }

    /// Synchronous append: submit + confirm in one step (empty pending around the call).
    pub fn append(&mut self, payload: Bytes) -> Result<EntryId> {
        let id = self.submit(payload)?;
        self.confirm(id)?;
        Ok(id)
    }

    /// Assign an entry id, mark it pending, and send to the current write set.
    ///
    /// Call [`confirm`] after the caller considers the append durable at Qa, or
    /// [`abandon`] to drop a failed in-flight id from the pending set.
    pub fn submit(&mut self, payload: Bytes) -> Result<EntryId> {
        if self.meta.state != crate::types::MetaState::Open {
            return Err(Error::InvalidArgument(
                "append requires ledger metadata OPEN".into(),
            ));
        }
        let entry_id = self.next;
        self.pending.insert(entry_id);
        self.next = entry_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("entry id overflow".into()))?;
        if let Err(err) = append_entry(self.bookies, &self.meta, entry_id, self.lac, payload) {
            self.pending.remove(&entry_id);
            self.next = entry_id;
            return Err(err);
        }
        Ok(entry_id)
    }

    /// Mark a previously submitted entry confirmed and advance LAC contiguously.
    pub fn confirm(&mut self, entry_id: EntryId) -> Result<i64> {
        if !self.pending.remove(&entry_id) {
            return Err(Error::InvalidArgument(format!(
                "entry {entry_id} is not pending"
            )));
        }
        self.completed.insert(entry_id);
        self.lac = advance_lac(self.lac, &self.completed);
        self.completed.retain(|id| (*id as i64) > self.lac);
        Ok(self.lac)
    }

    /// Drop a pending entry after a failed or cancelled pipeline attempt.
    pub fn abandon(&mut self, entry_id: EntryId) -> Result<()> {
        if !self.pending.remove(&entry_id) {
            return Err(Error::InvalidArgument(format!(
                "entry {entry_id} is not pending"
            )));
        }
        Ok(())
    }

    /// Append a new fragment starting at the next unissued entry id.
    ///
    /// Refuses while any add is still pending — otherwise metadata could claim a
    /// new ensemble for entry ids that already went to the old write set (#4924).
    pub fn change_ensemble(&mut self, ensemble: Ensemble) -> Result<&LedgerMeta> {
        if !self.pending.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "ensemble change blocked by {} pending add(s); drain or abandon first",
                self.pending.len()
            )));
        }
        let first = self.next;
        let meta = change_ensemble(self.metas, self.meta.ledger_id, first, ensemble)?;
        let newest = meta
            .fragments
            .last()
            .ok_or_else(|| Error::InvalidArgument("ledger has no fragments".into()))?;
        create_ledger_on_ensemble(
            self.bookies,
            &newest.ensemble,
            self.meta.ledger_id,
            &meta.master_key,
        )?;
        self.meta = meta;
        Ok(&self.meta)
    }

    /// Seal metadata at this writer's LAC. Pending adds must be empty.
    pub fn close(&mut self) -> Result<i64> {
        if !self.pending.is_empty() {
            return Err(Error::InvalidArgument(
                "close blocked by pending adds".into(),
            ));
        }
        let last = close_ledger(self.metas, self.meta.ledger_id, self.lac)?;
        self.meta = self.metas.get(self.meta.ledger_id)?;
        Ok(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryBookie;
    use crate::meta::MemoryMetaStore;
    use crate::store::Bookie;

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

    #[test]
    fn ensemble_change_blocked_while_pending() {
        let cluster = Cluster::new(4);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let first: Ensemble = cluster.ids[..3].to_vec();
        let mut w = PipelineWriter::create(
            &metas,
            &map,
            1,
            Quorum {
                e: 3,
                qw: 3,
                qa: 2,
            },
            first,
            KEY,
        )
        .unwrap();
        let e0 = w.submit(Bytes::from_static(b"a")).unwrap();
        w.confirm(e0).unwrap();
        let e1 = w.submit(Bytes::from_static(b"b")).unwrap();
        assert_eq!(w.pending().len(), 1);
        let next: Ensemble = cluster.ids[1..].to_vec();
        let err = w.change_ensemble(next.clone()).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
        w.confirm(e1).unwrap();
        w.change_ensemble(next).unwrap();
        let e2 = w.append(Bytes::from_static(b"c")).unwrap();
        assert_eq!(e2, 2);
        assert!(cluster.bookies[0].read_entry(1, 2).is_err());
        assert_eq!(
            cluster.bookies[3].read_entry(1, 2).unwrap().payload.as_ref(),
            b"c"
        );
    }

    #[test]
    fn confirm_out_of_order_does_not_skip_lac() {
        let cluster = Cluster::new(3);
        let map = cluster.map();
        let metas = MemoryMetaStore::new();
        let mut w = PipelineWriter::create(
            &metas,
            &map,
            1,
            Quorum {
                e: 3,
                qw: 3,
                qa: 2,
            },
            cluster.ids.clone(),
            KEY,
        )
        .unwrap();
        let a = w.submit(Bytes::from_static(b"0")).unwrap();
        let b = w.submit(Bytes::from_static(b"1")).unwrap();
        let c = w.submit(Bytes::from_static(b"2")).unwrap();
        w.confirm(c).unwrap();
        assert_eq!(w.lac(), -1);
        w.confirm(a).unwrap();
        assert_eq!(w.lac(), 0);
        w.confirm(b).unwrap();
        assert_eq!(w.lac(), 2);
    }
}
