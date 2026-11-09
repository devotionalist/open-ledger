//! Auditor: watch bookie registry, enqueue under-replicated ledgers.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use open_ledger::keys;
use open_ledger::{LedgerMeta, Result};
use tracing::{info, warn};

use crate::coord::CoordStore;
use crate::registry::BookieRegistry;
use crate::task::{enqueue_task, RereplicateTask};

/// Auditor configuration.
#[derive(Debug, Clone)]
pub struct AuditorConfig {
    /// Delay before publishing tasks after a bookie disappears.
    pub lost_bookie_delay: Duration,
    /// How often to scan when leader.
    pub scan_interval: Duration,
}

impl Default for AuditorConfig {
    fn default() -> Self {
        Self {
            lost_bookie_delay: Duration::from_secs(30),
            scan_interval: Duration::from_secs(5),
        }
    }
}

/// Runs only while `is_leader` is true.
pub struct Auditor<C: CoordStore> {
    coord: Arc<C>,
    registry: BookieRegistry<C>,
    is_leader: Arc<AtomicBool>,
    config: AuditorConfig,
}

impl<C: CoordStore + 'static> Auditor<C> {
    /// Create an auditor.
    pub fn new(
        coord: Arc<C>,
        is_leader: Arc<AtomicBool>,
        config: AuditorConfig,
    ) -> Self {
        let registry = BookieRegistry::new(Arc::clone(&coord));
        Self {
            coord,
            registry,
            is_leader,
            config,
        }
    }

    /// Background loop.
    pub fn spawn(self: &Arc<Self>) {
        let this = Arc::clone(self);
        thread::Builder::new()
            .name("auditor".into())
            .spawn(move || {
                let mut prev_alive = HashSet::new();
                loop {
                    if this.is_leader.load(Ordering::SeqCst) {
                        this.scan_once(&mut prev_alive);
                    }
                    thread::sleep(this.config.scan_interval);
                }
            })
            .expect("spawn auditor");
    }

    fn scan_once(&self, prev_alive: &mut HashSet<String>) {
        match self.registry.list_alive() {
            Ok(alive) => {
                let now: HashSet<String> = alive.into_iter().map(|b| b.bookie_id).collect();
                for lost in prev_alive.difference(&now) {
                    info!(bookie_id = %lost, "auditor detected lost bookie");
                    if let Err(err) = self.on_bookie_lost(lost) {
                        warn!(%err, bookie_id = %lost, "enqueue underreplicated failed");
                    }
                }
                *prev_alive = now;
            }
            Err(err) => warn!(%err, "auditor list_alive failed"),
        }
    }

    fn on_bookie_lost(&self, missing: &str) -> Result<()> {
        let not_before = now_unix() + self.config.lost_bookie_delay.as_secs() as i64;
        for meta in self.scan_ledgers()? {
            if !ledger_uses_bookie(&meta, missing) {
                continue;
            }
            let task = RereplicateTask {
                ledger_id: meta.ledger_id,
                missing_bookie: missing.to_string(),
                from_entry_id: 0,
                to_entry_id: -1,
                attempts: 0,
                not_before_unix: not_before,
            };
            enqueue_task(self.coord.as_ref(), &task)?;
            info!(
                ledger_id = meta.ledger_id,
                missing_bookie = %missing,
                "enqueued underreplicated task"
            );
        }
        Ok(())
    }

    fn scan_ledgers(&self) -> Result<Vec<LedgerMeta>> {
        let recs = self
            .coord
            .range_scan(keys::ledgers_prefix(), keys::ledgers_prefix_end())?;
        let mut out = Vec::new();
        for rec in recs {
            match serde_json::from_slice::<LedgerMeta>(&rec.value) {
                Ok(mut meta) => {
                    if let Ok(v) = u64::try_from(rec.version_id) {
                        meta.meta_version = v;
                    }
                    out.push(meta);
                }
                Err(err) => warn!(%err, key = %rec.key, "skip bad ledger meta"),
            }
        }
        Ok(out)
    }
}

fn ledger_uses_bookie(meta: &LedgerMeta, bookie_id: &str) -> bool {
    meta.fragments
        .iter()
        .any(|f| f.ensemble.iter().any(|b| b == bookie_id))
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
    use crate::coord::MemoryCoordStore;
    use crate::meta_store::OxiaMetaStore;
    use crate::registry::BookieInfo;
    use open_ledger::{LedgerMetaStore, Quorum};

    #[test]
    fn lost_bookie_enqueues_task() {
        let coord = Arc::new(MemoryCoordStore::new());
        let metas = OxiaMetaStore::new(Arc::clone(&coord));
        let meta = open_ledger::LedgerMeta::new_open(
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
        metas.create(meta).unwrap();

        let reg = BookieRegistry::new(Arc::clone(&coord));
        for id in ["b0", "b1", "b2"] {
            reg.register(&BookieInfo {
                bookie_id: id.into(),
                address: format!("{id}:3181"),
            })
            .unwrap();
        }

        let leader = Arc::new(AtomicBool::new(true));
        let auditor = Auditor::new(
            Arc::clone(&coord),
            leader,
            AuditorConfig {
                lost_bookie_delay: Duration::from_secs(0),
                scan_interval: Duration::from_millis(1),
            },
        );
        auditor.on_bookie_lost("b1").unwrap();
        let tasks = crate::task::list_tasks(coord.as_ref()).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].missing_bookie, "b1");
        assert_eq!(tasks[0].ledger_id, 1);
    }
}
