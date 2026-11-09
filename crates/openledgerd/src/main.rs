//! openledgerd — local Bookie process for open-ledger.
//!
//! Opens a data directory and serves the Bookie gRPC API plus an HTTP admin /
//! metrics surface.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use open_ledger::{Bookie, LocalBookie};
use tonic::transport::Server;
use tracing::info;

mod grpc;
mod http;
mod logging;
mod metrics;

use grpc::pb::bookie_server::BookieServer;
use grpc::BookieService;
use metrics::Metrics;

#[derive(Debug, Parser)]
#[command(name = "openledgerd", about = "open-ledger bookie daemon")]
struct Args {
    /// Data directory (Journal + ledger storage).
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Bookie identity (must match ensemble membership from broker).
    #[arg(long, default_value = "bookie-0")]
    bookie_id: String,

    /// gRPC listen address.
    #[arg(long, default_value = "0.0.0.0:3181")]
    listen: SocketAddr,

    /// HTTP admin / metrics listen address (`/`, `/metrics`, `/ready`, `/live`).
    #[arg(long, default_value = "0.0.0.0:3182")]
    http: SocketAddr,

    /// Optional smoke: create ledger 1 and append one entry then exit.
    #[arg(long, default_value_t = false)]
    smoke: bool,

    /// Exit 0 when `{data_dir}/bookie_id` exists. Container readiness probe.
    /// Does not open the journal.
    #[arg(long, default_value_t = false)]
    ready: bool,
}

#[tokio::main]
async fn main() -> open_ledger::Result<()> {
    logging::init();

    let args = Args::parse();
    if args.ready {
        let id = std::fs::read_to_string(args.data_dir.join("bookie_id"))?;
        if id.trim().is_empty() {
            return Err(open_ledger::Error::InvalidArgument(
                "empty bookie_id cookie".into(),
            ));
        }
        return Ok(());
    }
    info!(
        data_dir = %args.data_dir.display(),
        bookie_id = %args.bookie_id,
        "opening local bookie"
    );

    let bookie = Arc::new(LocalBookie::open(&args.data_dir, args.bookie_id.clone())?);
    let metrics = Metrics::new()
        .map_err(|e| open_ledger::Error::InvalidArgument(format!("metrics registry: {e}")))?;
    metrics.refresh_gauges(&bookie.admin_snapshot(0));

    if args.smoke {
        let key = b"smoke";
        bookie.create_ledger(1, key)?;
        let next = (bookie.last_entry_id(1)? + 1) as u64;
        let lac = bookie.read_last_confirmed(1)?;
        bookie.add_entry(1, next, lac, bytes::Bytes::from_static(b"smoke"), key)?;
        info!(entry_id = next, "smoke append ok");
        return Ok(());
    }

    tokio::spawn({
        let bookie = Arc::clone(&bookie);
        let metrics = Arc::clone(&metrics);
        async move {
            if let Err(err) = http::serve(args.http, bookie, metrics).await {
                tracing::error!(%err, "HTTP admin server exited");
            }
        }
    });

    info!(%args.listen, "serving bookie gRPC");
    Server::builder()
        .add_service(BookieServer::new(BookieService::new(bookie, metrics)))
        .serve(args.listen)
        .await
        .map_err(|e| open_ledger::Error::InvalidArgument(format!("grpc serve: {e}")))?;
    Ok(())
}
