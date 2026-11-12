//! openledgerd — local Bookie process for open-ledger.
//!
//! M0: opens a data directory and keeps the process alive.
//! gRPC / multi-bookie quorum comes in later milestones.

use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use clap::Parser;
use open_ledger::{LedgerStore, LocalBookie, Quorum};
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "openledgerd", about = "open-ledger bookie daemon")]
struct Args {
    /// Data directory (Journal + ledger storage).
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Bookie identity (must match ensemble membership from broker).
    #[arg(long, default_value = "bookie-0")]
    bookie_id: String,

    /// Optional smoke: create ledger 1 and append one entry then exit.
    #[arg(long, default_value_t = false)]
    smoke: bool,
}

fn main() -> open_ledger::Result<()> {
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

    let bookie = LocalBookie::open(&args.data_dir, args.bookie_id.clone())?;

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
        return Ok(());
    }

    info!("bookie ready (RPC server not yet wired; Ctrl-C to stop)");
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
