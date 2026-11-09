//! ReplicationWorker: lock tasks, copy entries to the **local** bookie, CAS ensemble.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use open_ledger::{Error, LedgerMeta, LedgerMetaStore, MetaState, Result};
use tracing::{info, warn};

use crate::coord::CoordStore;
use crate::grpc_bookie::RemoteBookie;
use crate::meta_store::OxiaMetaStore;
use crate::registry::{BookieInfo, BookieRegistry};
use crate::task::{delete_task, list_tasks, try_lock, unlock, RereplicateTask};

/// Worker configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// How often to poll the under-replicated queue.
    pub poll_interval: Duration,
    /// Grace before fencing an OPEN ledger's last fragment (BK open-ledger grace).
    pub open_ledger_grace: Duration,
    /// Local bookie id (never rereplicate fragments that already include it).
    pub local_bookie_id: String,
    /// Local bookie gRPC address (usually `127.0.0.1:3181` in-pod).
    pub local_bookie_addr: String,
    /// Max tasks to lock and process in one poll tick (clamped to ≥1).
    pub max_tasks_per_tick: usize,
    /// Pause between `recoveryAdd` calls (`Duration::ZERO` disables).
    pub entry_interval: Duration,
    /// Max payload bytes copied per tick (`0` = unlimited). Stops mid-task to bound memory (#3408).
    pub max_copy_bytes_per_tick: usize,
}

/// BookKeeper-style worker: target is always the local bookie.
pub struct ReplicationWorker<C: CoordStore> {
    coord: Arc<C>,
    metas: OxiaMetaStore<Arc<C>>,
    registry: BookieRegistry<C>,
    config: WorkerConfig,
}

impl<C: CoordStore + 'static> ReplicationWorker<C> {
    /// Create a worker bound to one local bookie.
    pub fn new(coord: Arc<C>, config: WorkerConfig) -> Self {
        let metas = OxiaMetaStore::new(Arc::clone(&coord));
        let registry = BookieRegistry::new(Arc::clone(&coord));
        Self {
            coord,
            metas,
            registry,
            config,
        }
    }

    /// Background poll loop.
    pub fn spawn(self: &Arc<Self>) {
        let this = Arc::clone(self);
        thread::Builder::new()
            .name("replication-worker".into())
            .spawn(move || loop {
                if let Err(err) = this.tick() {
                    warn!(%err, "replication worker tick failed");
                }
                thread::sleep(this.config.poll_interval);
            })
            .expect("spawn replication worker");
    }

    /// Process ready tasks once.
    pub fn tick(&self) -> Result<()> {
        let now = now_unix();
        let local = RemoteBookie::connect(&self.config.local_bookie_addr)?;
        let by_id: HashMap<String, BookieInfo> = self
            .registry
            .list_alive()?
            .into_iter()
            .map(|b| (b.bookie_id.clone(), b))
            .collect();

        // Clamp so a misconfigured 0 still makes progress each tick.
        let limit = self.config.max_tasks_per_tick.max(1);
        let mut started = 0usize;
        let mut bytes_left = if self.config.max_copy_bytes_per_tick == 0 {
            None
        } else {
            Some(self.config.max_copy_bytes_per_tick)
        };
        for task in list_tasks(self.coord.as_ref())? {
            if started >= limit {
                break;
            }
            if matches!(bytes_left, Some(0)) {
                break;
            }
            if task.not_before_unix > now {
                continue;
            }
            if !try_lock(
                self.coord.as_ref(),
                task.ledger_id,
                &self.config.local_bookie_id,
            )? {
                continue;
            }
            started += 1;
            let result = self.process_task(&task, &local, &by_id, &mut bytes_left);
            unlock(self.coord.as_ref(), task.ledger_id);
            match result {
                Ok(true) => {
                    delete_task(self.coord.as_ref(), task.ledger_id)?;
                    info!(
                        ledger_id = task.ledger_id,
                        local = %self.config.local_bookie_id,
                        "rereplicate task completed"
                    );
                }
                Ok(false) => {
                    // Still under-replicated elsewhere; leave task for other workers.
                }
                Err(err) => warn!(%err, ledger_id = task.ledger_id, "rereplicate failed"),
            }
        }
        Ok(())
    }

    /// Returns Ok(true) when this ledger no longer needs the task for `missing_bookie`.
    fn process_task(
        &self,
        task: &RereplicateTask,
        local: &RemoteBookie,
        bookies: &HashMap<String, BookieInfo>,
        bytes_left: &mut Option<usize>,
    ) -> Result<bool> {
        let mut meta = self.metas.get(task.ledger_id)?;
        // Match in-process rereplicate: ensure local storage exists before recoveryAdd.
        local.create_ledger(task.ledger_id, &meta.master_key)?;
        let mut changed = false;

        for frag_idx in 0..meta.fragments.len() {
            if !needs_local_rereplicate(&meta.fragments[frag_idx].ensemble, task, &self.config) {
                continue;
            }

            let is_last = frag_idx + 1 == meta.fragments.len();
            if meta.state == MetaState::Open && is_last {
                meta = self.fence_open_tail(task, local, bookies, frag_idx)?;
            }

            let end = fragment_end(&meta, frag_idx);
            // Budget hit: stop before CAS and leave the task for the next tick.
            if !self.copy_fragment(
                &meta,
                frag_idx,
                end,
                local,
                bookies,
                &task.missing_bookie,
                bytes_left,
            )? {
                return Ok(false);
            }

            meta = self.metas.get(task.ledger_id)?;
            let mut next = meta.clone();
            for bookie in &mut next.fragments[frag_idx].ensemble {
                if bookie == &task.missing_bookie {
                    *bookie = self.config.local_bookie_id.clone();
                }
            }
            match self.metas.compare_and_swap(meta.meta_version, next) {
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

    fn fence_open_tail(
        &self,
        task: &RereplicateTask,
        local: &RemoteBookie,
        bookies: &HashMap<String, BookieInfo>,
        frag_idx: usize,
    ) -> Result<LedgerMeta> {
        // Grace then fence before touching the open tail (BK semantics).
        thread::sleep(self.config.open_ledger_grace);
        let meta = self.metas.get(task.ledger_id)?;
        if meta.state != MetaState::Open {
            return Ok(meta);
        }
        for bookie_id in &meta.fragments[frag_idx].ensemble {
            if bookie_id == &task.missing_bookie {
                continue;
            }
            if let Some(info) = bookies.get(bookie_id) {
                if let Ok(remote) = RemoteBookie::connect(&info.address) {
                    let _ = remote.fence_ledger(task.ledger_id, &meta.master_key);
                }
            }
        }
        let _ = local.fence_ledger(task.ledger_id, &meta.master_key);
        Ok(meta)
    }

    /// Returns Ok(true) when the fragment copy finished; Ok(false) if byte budget exhausted.
    fn copy_fragment(
        &self,
        meta: &LedgerMeta,
        frag_idx: usize,
        end_inclusive: Option<u64>,
        local: &RemoteBookie,
        bookies: &HashMap<String, BookieInfo>,
        missing: &str,
        bytes_left: &mut Option<usize>,
    ) -> Result<bool> {
        let frag = &meta.fragments[frag_idx];
        let start = frag.first_entry_id;
        let sources: Vec<&BookieInfo> = frag
            .ensemble
            .iter()
            .filter(|b| b.as_str() != missing)
            .filter_map(|b| bookies.get(b))
            .collect();
        if sources.is_empty() {
            return Err(Error::NotEnoughBookies);
        }

        let mut entry_id = start;
        while !copy_done(meta, entry_id, end_inclusive) {
            match read_from_sources(&sources, meta.ledger_id, entry_id) {
                Some(entry) => {
                    if charge_byte_budget(bytes_left, entry.payload.len()) {
                        return Ok(false);
                    }
                    local.recovery_add_entry(
                        meta.ledger_id,
                        entry.entry_id,
                        entry.lac,
                        entry.payload,
                        &meta.master_key,
                    )?;
                    if !self.config.entry_interval.is_zero() {
                        thread::sleep(self.config.entry_interval);
                    }
                    entry_id += 1;
                }
                None if end_inclusive.is_some() => {
                    // Gap inside a known closed range: skip and keep scanning.
                    entry_id += 1;
                }
                None => break,
            }

            // Safety bound for open ledgers without end.
            if end_inclusive.is_none() && entry_id.saturating_sub(start) > 1_000_000 {
                warn!(ledger_id = meta.ledger_id, "open ledger copy hit safety bound");
                break;
            }
        }
        Ok(true)
    }
}

fn needs_local_rereplicate(
    ensemble: &[String],
    task: &RereplicateTask,
    config: &WorkerConfig,
) -> bool {
    ensemble.iter().any(|b| b == &task.missing_bookie)
        && !ensemble.iter().any(|b| b == &config.local_bookie_id)
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
    sources: &[&BookieInfo],
    ledger_id: u64,
    entry_id: u64,
) -> Option<open_ledger::Entry> {
    for src in sources {
        let Ok(remote) = RemoteBookie::connect(&src.address) else {
            continue;
        };
        if let Ok(entry) = remote.read_entry(ledger_id, entry_id) {
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

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_ledger::{LedgerMeta, Quorum};

    #[test]
    fn fragment_end_closed() {
        let mut meta = LedgerMeta::new_open(
            1,
            Quorum {
                e: 2,
                qw: 2,
                qa: 1,
            },
            vec!["a".into(), "b".into()],
            b"k",
        )
        .unwrap();
        meta.state = MetaState::Closed;
        meta.last_entry_id = 5;
        assert_eq!(fragment_end(&meta, 0), Some(5));
    }
}
