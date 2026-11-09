//! Colocated AutoRecovery: bookie registry heartbeat, auditor election,
//! auditor scan, and a ReplicationWorker targeting the local bookie.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use open_ledger_oxia::{
    Auditor, AuditorConfig, AuditorElection, BookieInfo, BookieRegistry, OxiaCoord,
    ReplicationWorker, WorkerConfig,
};
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "openledger-autorecovery")]
#[command(about = "BookKeeper-style AutoRecovery sidecar for open-ledger")]
struct Args {
    /// Oxia service address (`host:port`).
    #[arg(long, env = "OXIA_SERVICE", default_value = "oxia:6648")]
    oxia: String,

    /// Local bookie id (usually the StatefulSet pod name).
    #[arg(long, env = "BOOKIE_ID")]
    bookie_id: String,

    /// Reachable address of this bookie for other workers (`host:port`).
    #[arg(long, env = "BOOKIE_ADDRESS")]
    bookie_address: String,

    /// Local bookie gRPC for recoveryAdd (in-pod loopback).
    #[arg(long, env = "LOCAL_BOOKIE_ADDR", default_value = "127.0.0.1:3181")]
    local_bookie_addr: String,

    /// Identity used for auditor election (defaults to bookie id).
    #[arg(long, env = "AUDITOR_IDENTITY")]
    auditor_identity: Option<String>,

    /// lostBookieRecoveryDelay.
    #[arg(long, default_value = "30")]
    lost_bookie_delay_secs: u64,

    /// OPEN ledger grace before fencing the last fragment.
    #[arg(long, default_value = "5")]
    open_ledger_grace_secs: u64,

    /// Registry / election refresh interval.
    #[arg(long, default_value = "5")]
    heartbeat_secs: u64,

    /// Worker poll interval.
    #[arg(long, default_value = "2")]
    worker_poll_secs: u64,

    /// Max under-replicated tasks per poll tick (clamped to ≥1).
    #[arg(long, default_value = "4")]
    worker_max_tasks_per_tick: usize,

    /// Milliseconds to pause between recoveryAdd calls (`0` disables).
    #[arg(long, default_value = "0")]
    worker_entry_interval_ms: u64,

    /// Max payload bytes copied per worker tick (`0` = unlimited).
    #[arg(long, default_value = "67108864")]
    worker_max_copy_bytes_per_tick: usize,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let identity = args
        .auditor_identity
        .unwrap_or_else(|| args.bookie_id.clone());
    let heartbeat = Duration::from_secs(args.heartbeat_secs);

    info!(
        oxia = %args.oxia,
        bookie_id = %args.bookie_id,
        bookie_address = %args.bookie_address,
        "starting openledger-autorecovery"
    );

    let coord = Arc::new(
        OxiaCoord::connect(&args.oxia).unwrap_or_else(|err| {
            panic!("oxia connect failed: {err}");
        }),
    );

    let registry = Arc::new(BookieRegistry::new(Arc::clone(&coord)));
    let info = BookieInfo {
        bookie_id: args.bookie_id.clone(),
        address: args.bookie_address,
    };
    if let Err(err) = registry.register(&info) {
        warn!(%err, "initial bookie register failed");
    }
    registry.spawn_heartbeat(info, heartbeat);

    let election = Arc::new(AuditorElection::new(Arc::clone(&coord), identity));
    election.spawn(heartbeat);

    let auditor = Arc::new(Auditor::new(
        Arc::clone(&coord),
        election.leader_flag(),
        AuditorConfig {
            lost_bookie_delay: Duration::from_secs(args.lost_bookie_delay_secs),
            scan_interval: heartbeat,
        },
    ));
    auditor.spawn();

    let worker = Arc::new(ReplicationWorker::new(
        Arc::clone(&coord),
        WorkerConfig {
            poll_interval: Duration::from_secs(args.worker_poll_secs),
            open_ledger_grace: Duration::from_secs(args.open_ledger_grace_secs),
            local_bookie_id: args.bookie_id,
            local_bookie_addr: args.local_bookie_addr,
            max_tasks_per_tick: args.worker_max_tasks_per_tick.max(1),
            entry_interval: Duration::from_millis(args.worker_entry_interval_ms),
            max_copy_bytes_per_tick: args.worker_max_copy_bytes_per_tick,
        },
    ));
    worker.spawn();

    // Park forever; background threads do the work.
    loop {
        std::thread::park();
    }
}
