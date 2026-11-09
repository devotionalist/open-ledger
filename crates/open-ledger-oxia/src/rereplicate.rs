//! In-process rereplicate helpers (M3 unit e2e; no gRPC).

use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use open_ledger::{Bookie, Error, LedgerMeta, LedgerMetaStore, MetaState, Result};

use crate::task::RereplicateTask;

/// Copy missing-bookie fragments onto `local` and CAS ensemble ids.
///
/// Used by unit tests and as the bookie-agnostic core of AutoRecovery.
/// Returns `Ok(true)` when the ledger no longer lists `task.missing_bookie`.
pub fn rereplicate_task(
    metas: &dyn LedgerMetaStore,
    task: &RereplicateTask,
    local_bookie_id: &str,
    local: &dyn Bookie,
    bookies: &HashMap<String, &dyn Bookie>,
    open_ledger_grace: Duration,
    entry_interval: Duration,
    mut byte_budget: Option<usize>,
) -> Result<bool> {
    let mut meta = metas.get(task.ledger_id)?;
    // Ensure local storage exists (idempotent; same master key).
    local.create_ledger(task.ledger_id, &meta.master_key)?;
    let mut changed = false;

    for frag_idx in 0..meta.fragments.len() {
        let ensemble = &meta.fragments[frag_idx].ensemble;
        if !ensemble.iter().any(|b| b == &task.missing_bookie)
            || ensemble.iter().any(|b| b == local_bookie_id)
        {
            continue;
        }

        let is_last = frag_idx + 1 == meta.fragments.len();
        if meta.state == MetaState::Open && is_last {
            thread::sleep(open_ledger_grace);
            meta = metas.get(task.ledger_id)?;
            if meta.state == MetaState::Open {
                fence_open_sources(&meta, frag_idx, &task.missing_bookie, local, bookies);
            }
        }

        let end = fragment_end(&meta, frag_idx);
        if copy_fragment(
            &meta,
            frag_idx,
            end,
            &task.missing_bookie,
            local,
            bookies,
            entry_interval,
            &mut byte_budget,
        )? == CopyProgress::BudgetHit
        {
            // Stop before CAS so a partial copy does not claim the ensemble slot.
            return Ok(false);
        }

        meta = metas.get(task.ledger_id)?;
        let mut next = meta.clone();
        for bookie in &mut next.fragments[frag_idx].ensemble {
            if bookie == &task.missing_bookie {
                *bookie = local_bookie_id.to_string();
            }
        }
        match metas.compare_and_swap(meta.meta_version, next) {
            Ok(updated) => {
                meta = updated;
                changed = true;
            }
            Err(Error::VersionConflict(_)) => return Ok(false),
            Err(err) => return Err(err),
        }
    }

    Ok(changed && !ledger_still_missing(&meta, &task.missing_bookie))
}

#[derive(Debug, PartialEq, Eq)]
enum CopyProgress {
    Done,
    BudgetHit,
}

fn fence_open_sources(
    meta: &LedgerMeta,
    frag_idx: usize,
    missing: &str,
    local: &dyn Bookie,
    bookies: &HashMap<String, &dyn Bookie>,
) {
    for bookie_id in &meta.fragments[frag_idx].ensemble {
        if bookie_id == missing {
            continue;
        }
        if let Some(b) = bookies.get(bookie_id) {
            let _ = b.fence_ledger(meta.ledger_id, &meta.master_key);
        }
    }
    let _ = local.fence_ledger(meta.ledger_id, &meta.master_key);
}

fn copy_fragment(
    meta: &LedgerMeta,
    frag_idx: usize,
    end_inclusive: Option<u64>,
    missing: &str,
    local: &dyn Bookie,
    bookies: &HashMap<String, &dyn Bookie>,
    entry_interval: Duration,
    byte_budget: &mut Option<usize>,
) -> Result<CopyProgress> {
    let frag = &meta.fragments[frag_idx];
    let start = frag.first_entry_id;
    let sources: Vec<&dyn Bookie> = frag
        .ensemble
        .iter()
        .filter(|b| b.as_str() != missing)
        .filter_map(|b| bookies.get(b).copied())
        .collect();
    if sources.is_empty() {
        return Err(Error::NotEnoughBookies);
    }

    let mut entry_id = start;
    while !copy_done(meta, entry_id, end_inclusive) {
        match read_from_sources(&sources, meta.ledger_id, entry_id) {
            Some(entry) => {
                if charge_byte_budget(byte_budget, entry.payload.len()) {
                    return Ok(CopyProgress::BudgetHit);
                }
                local.recovery_add_entry(
                    meta.ledger_id,
                    entry.entry_id,
                    entry.lac,
                    entry.payload,
                    &meta.master_key,
                )?;
                if !entry_interval.is_zero() {
                    thread::sleep(entry_interval);
                }
                entry_id += 1;
            }
            None if end_inclusive.is_some() => {
                entry_id += 1;
            }
            None => break,
        }

        if end_inclusive.is_none() && entry_id.saturating_sub(start) > 1_000_000 {
            break;
        }
    }
    Ok(CopyProgress::Done)
}

/// Returns true when the budget is already exhausted (caller must not write).
/// Otherwise charges `nbytes`, allowing one oversized entry then zeroing the budget.
fn charge_byte_budget(budget: &mut Option<usize>, nbytes: usize) -> bool {
    let Some(left) = budget.as_mut() else {
        return false;
    };
    if *left == 0 {
        return true;
    }
    if nbytes > *left {
        *left = 0;
    } else {
        *left -= nbytes;
    }
    false
}

fn read_from_sources(
    sources: &[&dyn Bookie],
    ledger_id: u64,
    entry_id: u64,
) -> Option<open_ledger::Entry> {
    for src in sources {
        if let Ok(entry) = src.read_entry(ledger_id, entry_id) {
            return Some(entry);
        }
    }
    None
}

fn copy_done(meta: &LedgerMeta, entry_id: u64, end_inclusive: Option<u64>) -> bool {
    if let Some(end) = end_inclusive {
        return entry_id > end;
    }
    match meta.state {
        MetaState::Open => false,
        _ if meta.last_entry_id < 0 => true,
        _ => entry_id > meta.last_entry_id as u64,
    }
}

fn fragment_end(meta: &LedgerMeta, frag_idx: usize) -> Option<u64> {
    if frag_idx + 1 < meta.fragments.len() {
        Some(meta.fragments[frag_idx + 1].first_entry_id.saturating_sub(1))
    } else if meta.state == MetaState::Closed && meta.last_entry_id >= 0 {
        Some(meta.last_entry_id as u64)
    } else {
        None
    }
}

fn ledger_still_missing(meta: &LedgerMeta, missing: &str) -> bool {
    meta.fragments
        .iter()
        .any(|f| f.ensemble.iter().any(|b| b == missing))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::MemoryCoordStore;
    use crate::meta_store::OxiaMetaStore;
    use crate::registry::{BookieInfo, BookieRegistry};
    use crate::task::{enqueue_task, list_tasks};
    use open_ledger::{
        append_entry, close_ledger, create_open_ledger, Bookie, MemoryBookie, Quorum,
    };
    use std::sync::Arc;

    #[test]
    fn auditor_style_rereplicate_replaces_missing_bookie() {
        let coord = Arc::new(MemoryCoordStore::new());
        let metas = OxiaMetaStore::new(Arc::clone(&coord));
        let b0 = MemoryBookie::new();
        let b1 = MemoryBookie::new();
        let b2 = MemoryBookie::new();
        let b3 = MemoryBookie::new(); // replacement target
        let map: HashMap<String, &dyn Bookie> = [
            ("b0".into(), &b0 as &dyn Bookie),
            ("b1".into(), &b1 as &dyn Bookie),
            ("b2".into(), &b2 as &dyn Bookie),
            ("b3".into(), &b3 as &dyn Bookie),
        ]
        .into_iter()
        .collect();

        let meta = create_open_ledger(
            &metas,
            &map,
            1,
            Quorum {
                e: 3,
                qw: 3,
                qa: 2,
            },
            vec!["b0".into(), "b1".into(), "b2".into()],
            b"k",
        )
        .unwrap();
        append_entry(&map, &meta, 0, -1, bytes::Bytes::from_static(b"e0")).unwrap();
        append_entry(&map, &meta, 1, 0, bytes::Bytes::from_static(b"e1")).unwrap();
        close_ledger(&metas, 1, 1).unwrap();

        let reg = BookieRegistry::new(Arc::clone(&coord));
        for id in ["b0", "b1", "b3"] {
            reg.register(&BookieInfo {
                bookie_id: id.into(),
                address: format!("{id}:3181"),
            })
            .unwrap();
        }

        let task = RereplicateTask {
            ledger_id: 1,
            missing_bookie: "b2".into(),
            from_entry_id: 0,
            to_entry_id: -1,
            attempts: 0,
            not_before_unix: 0,
        };
        enqueue_task(coord.as_ref(), &task).unwrap();
        assert_eq!(list_tasks(coord.as_ref()).unwrap().len(), 1);

        let done = rereplicate_task(
            &metas,
            &task,
            "b3",
            &b3,
            &map,
            Duration::ZERO,
            Duration::ZERO,
            None,
        )
        .unwrap();
        assert!(done);
        let updated = metas.get(1).unwrap();
        assert!(!updated.fragments[0]
            .ensemble
            .iter()
            .any(|b| b == "b2"));
        assert!(updated.fragments[0]
            .ensemble
            .iter()
            .any(|b| b == "b3"));
        assert_eq!(b3.read_entry(1, 0).unwrap().payload.as_ref(), b"e0");
        assert_eq!(b3.read_entry(1, 1).unwrap().payload.as_ref(), b"e1");
    }

    #[test]
    fn byte_budget_stops_mid_copy() {
        let coord = Arc::new(MemoryCoordStore::new());
        let metas = OxiaMetaStore::new(Arc::clone(&coord));
        let b0 = MemoryBookie::new();
        let b1 = MemoryBookie::new();
        let b2 = MemoryBookie::new();
        let b3 = MemoryBookie::new();
        let map: HashMap<String, &dyn Bookie> = [
            ("b0".into(), &b0 as &dyn Bookie),
            ("b1".into(), &b1 as &dyn Bookie),
            ("b2".into(), &b2 as &dyn Bookie),
            ("b3".into(), &b3 as &dyn Bookie),
        ]
        .into_iter()
        .collect();
        let meta = create_open_ledger(
            &metas,
            &map,
            2,
            Quorum {
                e: 3,
                qw: 3,
                qa: 2,
            },
            vec!["b0".into(), "b1".into(), "b2".into()],
            b"k",
        )
        .unwrap();
        // Two 4-byte payloads.
        append_entry(&map, &meta, 0, -1, bytes::Bytes::from_static(b"aaaa")).unwrap();
        append_entry(&map, &meta, 1, 0, bytes::Bytes::from_static(b"bbbb")).unwrap();
        close_ledger(&metas, 2, 1).unwrap();

        let task = RereplicateTask {
            ledger_id: 2,
            missing_bookie: "b2".into(),
            from_entry_id: 0,
            to_entry_id: -1,
            attempts: 0,
            not_before_unix: 0,
        };
        let done = rereplicate_task(
            &metas,
            &task,
            "b3",
            &b3,
            &map,
            Duration::ZERO,
            Duration::ZERO,
            Some(4), // only first entry fits cleanly; second zeroes budget
        )
        .unwrap();
        assert!(!done);
        // First entry copied; ensemble not swapped yet (budget hit before CAS).
        assert!(b3.read_entry(2, 0).is_ok());
        let meta = metas.get(2).unwrap();
        assert!(meta.fragments[0].ensemble.iter().any(|b| b == "b2"));
    }
}
