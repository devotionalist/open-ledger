//! Prometheus sink for AutoRecovery observer hooks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use open_ledger_oxia::ArObserver;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

/// Prometheus-backed [`ArObserver`].
pub struct ArMetrics {
    registry: Registry,
    auditor_leader: IntGauge,
    registry_heartbeat_total: IntCounterVec,
    tasks_enqueued: IntCounter,
    worker_ready: IntCounter,
    worker_waiting: IntCounter,
    worker_lock_busy: IntCounter,
    tasks_completed: IntCounter,
    tasks_incomplete: IntCounter,
    tasks_failed: IntCounter,
    budget_hits: IntCounter,
    cas_conflicts: IntCounter,
    copy_entries: IntCounter,
    copy_bytes: IntCounter,
    copy_seconds: Histogram,
    leader_flag: AtomicBool,
}

impl ArMetrics {
    /// Register collectors.
    pub fn new() -> prometheus::Result<Arc<Self>> {
        let registry = Registry::new();
        let auditor_leader = IntGauge::new(
            "openledger_ar_auditor_leader",
            "1 if this process is the Auditor leader",
        )?;
        let registry_heartbeat_total = IntCounterVec::new(
            Opts::new(
                "openledger_ar_registry_heartbeat_total",
                "Bookie registry heartbeat attempts",
            ),
            &["result"],
        )?;
        let tasks_enqueued = IntCounter::new(
            "openledger_ar_tasks_enqueued_total",
            "Under-replicated tasks enqueued by Auditor",
        )?;
        let worker_ready = IntCounter::new(
            "openledger_ar_worker_ready_tasks_total",
            "Tasks locked and started by the local worker",
        )?;
        let worker_waiting = IntCounter::new(
            "openledger_ar_worker_waiting_not_before_total",
            "Tasks skipped because not_before has not elapsed",
        )?;
        let worker_lock_busy = IntCounter::new(
            "openledger_ar_worker_lock_busy_total",
            "Tasks skipped because another worker held the lock",
        )?;
        let tasks_completed = IntCounter::new(
            "openledger_ar_tasks_completed_total",
            "Rereplicate tasks completed on this worker",
        )?;
        let tasks_incomplete = IntCounter::new(
            "openledger_ar_tasks_incomplete_total",
            "Rereplicate tasks left incomplete (budget / CAS / still missing)",
        )?;
        let tasks_failed = IntCounter::new(
            "openledger_ar_tasks_failed_total",
            "Rereplicate tasks that failed with an error",
        )?;
        let budget_hits = IntCounter::new(
            "openledger_ar_budget_hits_total",
            "Times the per-tick byte budget stopped a copy (#3408)",
        )?;
        let cas_conflicts = IntCounter::new(
            "openledger_ar_cas_conflicts_total",
            "Ensemble CAS version conflicts",
        )?;
        let copy_entries = IntCounter::new(
            "openledger_ar_copy_entries_total",
            "Entries recoveryAdd'ed by this worker",
        )?;
        let copy_bytes = IntCounter::new(
            "openledger_ar_copy_bytes_total",
            "Payload bytes recoveryAdd'ed by this worker",
        )?;
        let copy_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "openledger_ar_task_duration_seconds",
                "Wall time to complete a rereplicate task",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0]),
        )?;

        for c in [
            Box::new(auditor_leader.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(registry_heartbeat_total.clone()),
            Box::new(tasks_enqueued.clone()),
            Box::new(worker_ready.clone()),
            Box::new(worker_waiting.clone()),
            Box::new(worker_lock_busy.clone()),
            Box::new(tasks_completed.clone()),
            Box::new(tasks_incomplete.clone()),
            Box::new(tasks_failed.clone()),
            Box::new(budget_hits.clone()),
            Box::new(cas_conflicts.clone()),
            Box::new(copy_entries.clone()),
            Box::new(copy_bytes.clone()),
            Box::new(copy_seconds.clone()),
        ] {
            registry.register(c)?;
        }

        Ok(Arc::new(Self {
            registry,
            auditor_leader,
            registry_heartbeat_total,
            tasks_enqueued,
            worker_ready,
            worker_waiting,
            worker_lock_busy,
            tasks_completed,
            tasks_incomplete,
            tasks_failed,
            budget_hits,
            cas_conflicts,
            copy_entries,
            copy_bytes,
            copy_seconds,
            leader_flag: AtomicBool::new(false),
        }))
    }

    /// Whether this process currently believes it is Auditor leader.
    pub fn is_leader(&self) -> bool {
        self.leader_flag.load(Ordering::SeqCst)
    }

    /// Encode Prometheus text.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}

impl ArObserver for ArMetrics {
    fn set_auditor_leader(&self, is_leader: bool) {
        self.leader_flag.store(is_leader, Ordering::SeqCst);
        self.auditor_leader.set(if is_leader { 1 } else { 0 });
    }

    fn registry_heartbeat(&self, ok: bool) {
        let result = if ok { "ok" } else { "error" };
        self.registry_heartbeat_total
            .with_label_values(&[result])
            .inc();
    }

    fn task_enqueued(&self, _ledger_id: u64, _missing_bookie: &str) {
        self.tasks_enqueued.inc();
    }

    fn worker_tick(&self, ready_tasks: u64, waiting_not_before: u64, lock_busy: u64) {
        self.worker_ready.inc_by(ready_tasks);
        self.worker_waiting.inc_by(waiting_not_before);
        self.worker_lock_busy.inc_by(lock_busy);
    }

    fn task_completed(&self, _ledger_id: u64, entries: u64, bytes: u64, secs: f64) {
        self.tasks_completed.inc();
        self.copy_entries.inc_by(entries);
        self.copy_bytes.inc_by(bytes);
        self.copy_seconds.observe(secs);
    }

    fn task_incomplete(&self, _ledger_id: u64) {
        self.tasks_incomplete.inc();
    }

    fn task_failed(&self, _ledger_id: u64, _err: &str) {
        self.tasks_failed.inc();
    }

    fn budget_hit(&self, _ledger_id: u64) {
        self.budget_hits.inc();
    }

    fn cas_conflict(&self, _ledger_id: u64) {
        self.cas_conflicts.inc();
    }
}
