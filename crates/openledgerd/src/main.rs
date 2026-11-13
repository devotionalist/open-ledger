//! openledgerd — local Bookie process for open-ledger.
//!
//! Serves Bookie gRPC with a **Callback / async reaction** model (tonic):
//! receive schedules `spawn_blocking` journal work; the reaction future finishes
//! the RPC — equivalent discipline to OpenKV's CallbackService + WorkPool.
//!
//! Graceful shutdown: SIGTERM / SIGINT → stop accept → drain in-flight RPCs
//! (up to `--shutdown-grace-secs`) → journal flush → exit.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use open_ledger::{LedgerStore, LocalBookie, Quorum};
use tonic::transport::Server;
use tracing::{info, warn, Level};
use tracing_subscriber::EnvFilter;

mod grpc;

use grpc::pb::bookie_server::BookieServer;
use grpc::BookieService;

#[derive(Debug, Parser)]
#[command(name = "openledgerd", about = "open-ledger bookie daemon")]
struct Args {
    /// Data directory (Journal + ledger storage).
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Bookie identity (must match ensemble membership from broker).
    #[arg(long, default_value = "bookie-0")]
    bookie_id: String,

    /// gRPC listen address (Callback/async service).
    #[arg(long, default_value = "0.0.0.0:3181")]
    listen: SocketAddr,

    /// After SIGTERM/SIGINT, seconds to drain in-flight RPCs before abort.
    #[arg(long, default_value_t = 25)]
    shutdown_grace_secs: u64,

    /// Optional smoke: create ledger 1 and append one entry then exit.
    #[arg(long, default_value_t = false)]
    smoke: bool,
}

#[tokio::main]
async fn main() -> open_ledger::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_max_level(Level::INFO)
        .init();

    let args = Args::parse();
    info!(
        data_dir = %args.data_dir.display(),
        bookie_id = %args.bookie_id,
        "opening local bookie"
    );

    let bookie = Arc::new(LocalBookie::open(&args.data_dir, args.bookie_id.clone())?);

    if args.smoke {
        let ens = vec![args.bookie_id.clone()];
        match bookie.create(1, Quorum::ONE, ens, &args.bookie_id) {
            Ok(()) => {}
            Err(open_ledger::Error::AlreadyExists(_)) => {}
            Err(e) => return Err(e),
        }
        let next = bookie.read(1, 0, 1024)?.len() as u64;
        bookie.append(1, next, next as i64 - 1, bytes::Bytes::from_static(b"smoke"))?;
        info!(entry_id = next, "smoke append ok");
        bookie.flush()?;
        return Ok(());
    }

    let grace = Duration::from_secs(args.shutdown_grace_secs);

    info!(
        %args.listen,
        model = "callback/async-reaction+spawn_blocking",
        shutdown_grace_secs = args.shutdown_grace_secs,
        "serving bookie gRPC"
    );

    let serve = Server::builder()
        .add_service(BookieServer::new(BookieService::new(Arc::clone(&bookie))))
        .serve_with_shutdown(args.listen, async move {
            wait_shutdown_signal().await;
            info!(
                grace_secs = grace.as_secs(),
                "shutdown signal; draining gRPC"
            );
        });

    // Second signal waiter: if drain exceeds grace, abort the serve future.
    tokio::select! {
        res = serve => {
            res.map_err(|e| open_ledger::Error::InvalidArgument(format!("grpc serve: {e}")))?;
            info!("gRPC drained");
        }
        _ = async {
            wait_shutdown_signal().await;
            tokio::time::sleep(grace).await;
        } => {
            warn!(
                grace_secs = grace.as_secs(),
                "shutdown grace elapsed; aborting remaining connections"
            );
        }
    }

    match bookie.flush() {
        Ok(()) => info!("journal flushed; exiting"),
        Err(e) => warn!(error = %e, "journal flush failed on shutdown"),
    }
    Ok(())
}

async fn wait_shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "ctrl_c handler failed");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => warn!(error = %e, "SIGTERM handler failed"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
