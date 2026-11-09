//! open-ledger: durable bookie (Journal + ledger storage).
//!
//! A bookie stores entry bytes (Journal + ledger storage). It does not know
//! topics, ensembles, or the sealed tail. Quorum append and ledger recovery
//! run in the client / broker.

#![deny(missing_docs)]

mod client;
mod disk;
mod distribution;
mod error;
mod journal;
pub mod keys;
mod local;
mod memory;
mod meta;
mod state;
mod store;
mod types;
mod writer;

pub use client::{
    advance_lac, append_entry, change_ensemble, close_ledger, create_ledger_on_ensemble,
    create_open_ledger, read_entry_quorum, recover_ledger,
};
pub use distribution::{fragment_for, write_quorum_covered, write_set};
pub use error::{Error, Result};
pub use local::LocalBookie;
pub use memory::MemoryBookie;
pub use meta::{LedgerMeta, LedgerMetaStore, MemoryMetaStore};
pub use store::Bookie;
pub use types::{BookieId, Ensemble, Entry, EntryId, Fragment, LedgerId, MetaState, Quorum};
pub use writer::{LedgerWriter, PipelineWriter};
