//! Errors for LedgerStore operations.

use thiserror::Error;

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// LedgerStore / Bookie errors.
#[derive(Debug, Error)]
pub enum Error {
    /// Ledger already exists on this bookie.
    #[error("ledger {0} already exists")]
    AlreadyExists(u64),

    /// Ledger not found.
    #[error("ledger {0} not found")]
    NotFound(u64),

    /// Ledger is fenced; writer must stop (treat like uncertain result upstream).
    #[error("ledger {0} is fenced")]
    Fenced(u64),

    /// Ledger is sealed/closed; no more appends.
    #[error("ledger {0} is sealed")]
    Sealed(u64),

    /// Entry id not the expected next id (gap / duplicate).
    #[error("ledger {ledger_id}: bad entry id {got}, expected {expected}")]
    BadEntryId {
        /// Ledger id.
        ledger_id: u64,
        /// Expected next entry id.
        expected: u64,
        /// Received entry id.
        got: u64,
    },

    /// Invalid quorum or ensemble.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// I/O failure (journal / ledger disk).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization / encoding.
    #[error("codec: {0}")]
    Codec(String),
}
