//! open-ledger: BookKeeper-style primary store for open-message.
//!
//! Bookie has **no topic concept** — only `ledgerId + entryId`.
//! Topic / subscription / ownership live in open-message Metadata + Broker.
//!
//! Hard rule: an entry must be durable on Journal (WAL) before Append returns
//! success (Receipt / LAC path in the broker).

#![deny(missing_docs)]

mod error;
mod local;
mod memory;
mod store;
mod types;

pub use error::{Error, Result};
pub use local::LocalBookie;
pub use memory::MemoryBookie;
pub use store::LedgerStore;
pub use types::{
    BookieId, Ensemble, Entry, EntryId, Fragment, LedgerId, LedgerState, Quorum, WriteQuorum,
};
