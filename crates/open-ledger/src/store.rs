//! LedgerStore API — broker-facing shape (open-message architecture §10).

use bytes::Bytes;

use crate::error::Result;
use crate::types::{BookieId, Ensemble, Entry, EntryId, LedgerId, Quorum};

/// Primary store operations on a bookie (or quorum facade).
///
/// Durability: [`Self::append`] must not return `Ok` until the entry is on Journal
/// (WAL fsynced) for this replica. Broker Receipt waits for Qa such acks.
///
/// M0/M1 may use `E=Qw=Qa=1` on a single node; Fence/Recover APIs remain.
pub trait LedgerStore: Send + Sync {
    /// Create a ledger fragment on this bookie.
    fn create(
        &self,
        ledger_id: LedgerId,
        quorum: Quorum,
        ensemble: Ensemble,
        this_bookie: &BookieId,
    ) -> Result<()>;

    /// Append one entry. Journal durable before Ok.
    fn append(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
    ) -> Result<()>;

    /// Fence the ledger so the old writer cannot gather Qa.
    fn fence(&self, ledger_id: LedgerId) -> Result<()>;

    /// Recover: determine durable tail and seal; returns last entry id (-1 if empty).
    fn recover(&self, ledger_id: LedgerId) -> Result<i64>;

    /// Normal close/seal with explicit last entry id.
    fn close(&self, ledger_id: LedgerId, last_entry_id: i64) -> Result<()>;

    /// Read up to `max` entries starting at `from_entry_id`.
    fn read(&self, ledger_id: LedgerId, from_entry_id: EntryId, max: usize) -> Result<Vec<Entry>>;

    /// Delete ledger data on this bookie (after Metadata trim allows).
    fn delete(&self, ledger_id: LedgerId) -> Result<()>;
}
