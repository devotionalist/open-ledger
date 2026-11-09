//! Prometheus metrics for bookie RPCs and process gauges.

use std::sync::Arc;
use std::time::Instant;

use open_ledger::AdminSnapshot;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};

/// Process-wide bookie metrics.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    rpc_total: IntCounterVec,
    rpc_errors: IntCounterVec,
    rpc_latency: HistogramVec,
    rpc_bytes: IntCounterVec,
    journal_bytes: IntGauge,
    ledger_count: IntGauge,
    fenced_count: IntGauge,
}

impl Metrics {
    /// Register default collectors.
    pub fn new() -> prometheus::Result<Arc<Self>> {
        let registry = Registry::new();
        let rpc_total = IntCounterVec::new(
            Opts::new(
                "openledger_bookie_rpc_total",
                "Bookie gRPC calls completed",
            ),
            &["method", "result"],
        )?;
        let rpc_errors = IntCounterVec::new(
            Opts::new(
                "openledger_bookie_rpc_errors_total",
                "Bookie gRPC calls that returned an error",
            ),
            &["method", "error_kind"],
        )?;
        let rpc_latency = HistogramVec::new(
            HistogramOpts::new(
                "openledger_bookie_rpc_duration_seconds",
                "Bookie gRPC handler latency",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            ]),
            &["method"],
        )?;
        let rpc_bytes = IntCounterVec::new(
            Opts::new(
                "openledger_bookie_rpc_bytes_total",
                "Payload bytes handled by write/read RPCs",
            ),
            &["method", "direction"],
        )?;
        let journal_bytes = IntGauge::new(
            "openledger_bookie_journal_bytes",
            "Durable journal length in bytes",
        )?;
        let ledger_count =
            IntGauge::new("openledger_bookie_ledgers", "Ledgers present on this bookie")?;
        let fenced_count = IntGauge::new(
            "openledger_bookie_fenced_ledgers",
            "Fenced ledgers on this bookie",
        )?;

        for c in [
            Box::new(rpc_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(rpc_errors.clone()),
            Box::new(rpc_latency.clone()),
            Box::new(rpc_bytes.clone()),
            Box::new(journal_bytes.clone()),
            Box::new(ledger_count.clone()),
            Box::new(fenced_count.clone()),
        ] {
            registry.register(c)?;
        }

        Ok(Arc::new(Self {
            registry,
            rpc_total,
            rpc_errors,
            rpc_latency,
            rpc_bytes,
            journal_bytes,
            ledger_count,
            fenced_count,
        }))
    }

    /// Observe one RPC outcome.
    pub fn observe_rpc(
        &self,
        method: &str,
        ok: bool,
        error_kind: Option<&str>,
        bytes: Option<(u64, &'static str)>,
        started: Instant,
    ) {
        let result = if ok { "ok" } else { "error" };
        self.rpc_total.with_label_values(&[method, result]).inc();
        self.rpc_latency
            .with_label_values(&[method])
            .observe(started.elapsed().as_secs_f64());
        if !ok {
            self.rpc_errors
                .with_label_values(&[method, error_kind.unwrap_or("unknown")])
                .inc();
        }
        if let Some((n, direction)) = bytes {
            self.rpc_bytes
                .with_label_values(&[method, direction])
                .inc_by(n);
        }
    }

    /// Refresh gauges from an admin snapshot.
    pub fn refresh_gauges(&self, snap: &AdminSnapshot) {
        self.journal_bytes.set(snap.journal_len as i64);
        self.ledger_count.set(snap.ledger_count as i64);
        self.fenced_count.set(snap.fenced_count as i64);
    }

    /// Encode Prometheus text exposition.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}
