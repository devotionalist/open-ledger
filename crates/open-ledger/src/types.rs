//! Core ledger types (aligned with open-message LedgerStore semantics).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Globally unique ledger identifier.
pub type LedgerId = u64;

/// Monotonic entry id within a ledger (0-based).
pub type EntryId = u64;

/// Bookie / storage-node identity.
pub type BookieId = String;

/// Ensemble size / write quorum / ack quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quorum {
    /// Ensemble size (number of bookies holding the fragment).
    pub e: u32,
    /// Write quorum (parallel write targets).
    pub qw: u32,
    /// Ack quorum (durable confirms required before success).
    pub qa: u32,
}

impl Quorum {
    /// Single-replica quorum used by M0/M1.
    pub const ONE: Self = Self { e: 1, qw: 1, qa: 1 };

    /// Validate E ≥ Qw ≥ Qa ≥ 1.
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

/// Ensemble membership for a fragment (length = E).
pub type Ensemble = Vec<BookieId>;

/// Alias kept for API clarity.
pub type WriteQuorum = Quorum;

/// One fragment of a ledger (ensemble may change over time).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fragment {
    /// First entry id covered by this fragment.
    pub first_entry_id: EntryId,
    /// Bookie ids; length must equal Quorum.e.
    pub ensemble: Ensemble,
}

/// Local / protocol ledger state on a bookie (authoritative CLOSED also in Metadata).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerState {
    /// Accepting appends from the current writer.
    Open,
    /// Fenced; recovery in progress or writer blocked.
    Fenced,
    /// Sealed; immutable.
    Closed,
}

/// One durable log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Ledger id.
    pub ledger_id: LedgerId,
    /// Entry id within the ledger.
    pub entry_id: EntryId,
    /// Last Add Confirmed hint carried with the write (BK-style).
    pub lac: i64,
    /// Opaque payload (message bytes).
    pub payload: Bytes,
}

impl Entry {
    /// Build an entry.
    pub fn new(ledger_id: LedgerId, entry_id: EntryId, lac: i64, payload: impl Into<Bytes>) -> Self {
        Self {
            ledger_id,
            entry_id,
            lac,
            payload: payload.into(),
        }
    }
}
