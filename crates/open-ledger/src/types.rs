//! Core ledger types.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Globally unique ledger identifier.
pub type LedgerId = u64;

/// Entry id within a ledger. A single bookie may store a non-contiguous subset (striping).
pub type EntryId = u64;

/// Bookie identity.
pub type BookieId = String;

/// Ensemble size / write quorum / ack quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quorum {
    /// Ensemble size (bookies in the fragment).
    pub e: u32,
    /// Write quorum (bookies that receive each entry).
    pub qw: u32,
    /// Ack quorum (journal acks required before the entry is confirmed).
    pub qa: u32,
}

impl Quorum {
    /// Single-replica quorum.
    pub const ONE: Self = Self { e: 1, qw: 1, qa: 1 };

    /// Validate `E >= Qw >= Qa >= 1`.
    pub fn validate(self) -> Result<(), &'static str> {
        if self.e == 0 || self.qw == 0 || self.qa == 0 {
            return Err("E/Qw/Qa must be >= 1");
        }
        if self.e < self.qw || self.qw < self.qa {
            return Err("require E >= Qw >= Qa");
        }
        Ok(())
    }
}

/// Ensemble membership for one fragment (length = E).
pub type Ensemble = Vec<BookieId>;

/// One fragment of a ledger. Historical fragments are never rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fragment {
    /// First entry id covered by this fragment.
    pub first_entry_id: EntryId,
    /// Bookie ids; length must equal [`Quorum::e`].
    pub ensemble: Ensemble,
}

/// Authoritative ledger state. Lives in Metadata, not on the bookie.
///
/// Metadata service stores this. A bookie only knows entries,
/// the master key, the fenced bit, and explicit LAC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaState {
    /// Accepting adds from the current writer.
    Open,
    /// Fencing / tail recovery in progress.
    InRecovery,
    /// Sealed. `last_entry_id` is final.
    Closed,
}

/// One stored record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Ledger id.
    pub ledger_id: LedgerId,
    /// Entry id within the ledger.
    pub entry_id: EntryId,
    /// LastAddConfirmed piggybacked on this add.
    pub lac: i64,
    /// Opaque payload.
    pub payload: Bytes,
}

impl Entry {
    /// Build an entry.
    pub fn new(
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            ledger_id,
            entry_id,
            lac,
            payload: payload.into(),
        }
    }
}
