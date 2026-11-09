//! Bookie operations. Single-node storage surface; ledger recovery lives in the client.
//!
//! Recovery (fence coverage, forward read, seal `lastEntryId`) is a client
//! algorithm over many bookies. See [`crate::recover_ledger`].

use bytes::Bytes;

use crate::error::Result;
use crate::types::{Entry, EntryId, LedgerId};

/// One storage node.
///
/// Durability: [`Bookie::add_entry`] and [`Bookie::recovery_add_entry`] return
/// `Ok` only after this replica's Journal is fsynced. Ledger storage may lag;
/// restart replays the journal from the last checkpoint.
///
/// A bookie does not know E/Qw/Qa. Striping means entry ids on one bookie have gaps.
/// The same entry id with the same payload is idempotent; a different payload conflicts.
pub trait Bookie: Send + Sync {
    /// Create ledger storage. Same master key is idempotent and does not clear fence.
    fn create_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()>;

    /// Normal add. Fails if the ledger is fenced. Does not require `entry_id == last + 1`.
    fn add_entry(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
        master_key: &[u8],
    ) -> Result<()>;

    /// Recovery add. Allowed after fence so tail repair can finish.
    fn recovery_add_entry(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
        master_key: &[u8],
    ) -> Result<()>;

    /// Read one entry stored on this bookie.
    fn read_entry(&self, ledger_id: LedgerId, entry_id: EntryId) -> Result<Entry>;

    /// Entries with `entry_id >= from_entry_id`, in order, at most `max`.
    ///
    /// Gaps are skipped. This is the bookie's local set, not a contiguous ledger read.
    fn read_entries(
        &self,
        ledger_id: LedgerId,
        from_entry_id: EntryId,
        max: usize,
    ) -> Result<Vec<Entry>>;

    /// Entry ids this bookie stores, ascending.
    fn list_entries(&self, ledger_id: LedgerId) -> Result<Vec<EntryId>>;

    /// Persist fence. If the ledger is absent, create it already fenced (blocks a later create/add).
    fn fence_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()>;

    /// Persist explicit LastAddConfirmed. Rejected once fenced.
    fn set_explicit_lac(&self, ledger_id: LedgerId, lac: i64, master_key: &[u8]) -> Result<()>;

    /// `max(explicit LAC, piggyback LAC of the highest stored entry)`. `-1` if neither is set.
    fn read_last_confirmed(&self, ledger_id: LedgerId) -> Result<i64>;

    /// Highest entry id stored here, or `-1`. This can be past LastAddConfirmed.
    fn last_entry_id(&self, ledger_id: LedgerId) -> Result<i64>;

    /// Whether this bookie has fenced the ledger.
    fn is_fenced(&self, ledger_id: LedgerId) -> Result<bool>;

    /// Drop ledger data on this bookie.
    fn delete_ledger(&self, ledger_id: LedgerId) -> Result<()>;
}
