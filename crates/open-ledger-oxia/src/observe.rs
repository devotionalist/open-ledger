//! Optional AutoRecovery observation hooks (metrics / tracing sinks).

use std::sync::Arc;

/// No-op observer used when metrics are not wired.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

/// Callbacks fired by Auditor / Worker / registry / election.
pub trait ArObserver: Send + Sync {
    /// Auditor election leadership changed or refreshed.
    fn set_auditor_leader(&self, is_leader: bool) {
        let _ = is_leader;
    }

    /// Bookie registry heartbeat finished.
    fn registry_heartbeat(&self, ok: bool) {
        let _ = ok;
    }

    /// Auditor enqueued an under-replicated task.
    fn task_enqueued(&self, ledger_id: u64, missing_bookie: &str) {
        let _ = (ledger_id, missing_bookie);
    }

    /// Worker finished one poll tick.
    fn worker_tick(&self, ready_tasks: u64, waiting_not_before: u64, lock_busy: u64) {
        let _ = (ready_tasks, waiting_not_before, lock_busy);
    }

    /// Worker completed a rereplicate task (ensemble no longer lists missing).
    fn task_completed(&self, ledger_id: u64, entries: u64, bytes: u64, secs: f64) {
        let _ = (ledger_id, entries, bytes, secs);
    }

    /// Worker left a task incomplete (budget / partial / still missing elsewhere).
    fn task_incomplete(&self, ledger_id: u64) {
        let _ = ledger_id;
    }

    /// Worker failed a task.
    fn task_failed(&self, ledger_id: u64, err: &str) {
        let _ = (ledger_id, err);
    }

    /// Byte budget exhausted mid-copy (#3408).
    fn budget_hit(&self, ledger_id: u64) {
        let _ = ledger_id;
    }

    /// Metadata CAS conflict while replacing ensemble.
    fn cas_conflict(&self, ledger_id: u64) {
        let _ = ledger_id;
    }
}

impl ArObserver for NoopObserver {}

/// Shared observer handle.
pub type SharedObserver = Arc<dyn ArObserver>;

/// Build a no-op shared observer.
pub fn noop_observer() -> SharedObserver {
    Arc::new(NoopObserver)
}
