//! Errors for bookie and ledger-client operations.

use thiserror::Error;

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Bookie / ledger client errors.
#[derive(Debug, Error)]
pub enum Error {
    /// Ledger already exists.
    #[error("ledger {0} already exists")]
    AlreadyExists(u64),

    /// Ledger not found on this bookie (or in metadata).
    #[error("ledger {0} not found")]
    NotFound(u64),

    /// This bookie does not store that entry (gap or not in its write set).
    #[error("ledger {ledger_id} entry {entry_id} not on this bookie")]
    NoSuchEntry {
        /// Ledger id.
        ledger_id: u64,
        /// Missing entry id.
        entry_id: u64,
    },

    /// Same entry id already stored with a different payload.
    #[error("ledger {ledger_id} entry {entry_id} payload conflict")]
    Conflict {
        /// Ledger id.
        ledger_id: u64,
        /// Entry id.
        entry_id: u64,
    },

    /// Ledger is fenced; a normal add cannot succeed.
    #[error("ledger {0} is fenced")]
    Fenced(u64),

    /// Master key does not match the key stored on the bookie.
    #[error("ledger {0} master key mismatch")]
    Unauthorized(u64),

    /// Metadata compare-and-swap lost the race.
    #[error("ledger {0} metadata version conflict")]
    VersionConflict(u64),

    /// Metadata update was rejected (closed tail mismatch or divergent create).
    #[error("metadata conflict: {0}")]
    MetaConflict(String),

    /// Quorum coverage or ack quorum was not reached.
    #[error("not enough bookies for quorum")]
    NotEnoughBookies,

    /// Invalid quorum, ensemble, key, or entry argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// I/O failure (journal / ledger disk).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization / journal corruption.
    #[error("codec: {0}")]
    Codec(String),
}
