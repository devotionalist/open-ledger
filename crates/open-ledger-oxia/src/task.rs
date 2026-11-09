//! Under-replicated recovery tasks stored in Oxia.

use open_ledger::keys;
use open_ledger::{Error, LedgerId, Result};
use serde::{Deserialize, Serialize};

use crate::coord::{CoordStore, PutExpect};

/// Task published by the Auditor (BK `/underreplicated` analogue).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RereplicateTask {
    /// Ledger that lost a replica.
    pub ledger_id: LedgerId,
    /// Bookie id that disappeared from the registry.
    pub missing_bookie: String,
    /// Inclusive start of the copy range (usually fragment start).
    pub from_entry_id: u64,
    /// Inclusive end; `-1` means "through current max / open tail".
    pub to_entry_id: i64,
    /// Attempt counter.
    pub attempts: u32,
    /// Unix seconds; workers skip until this time (lostBookieRecoveryDelay).
    pub not_before_unix: i64,
}

impl RereplicateTask {
    /// Encode for Oxia.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| Error::Codec(e.to_string()))
    }

    /// Decode from Oxia.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
    }
}

/// Enqueue or refresh a task (idempotent create/overwrite).
pub fn enqueue_task(coord: &dyn CoordStore, task: &RereplicateTask) -> Result<()> {
    let key = keys::underreplicated_key(task.ledger_id);
    coord.put(&key, &task.to_bytes()?, PutExpect::Any, false)?;
    Ok(())
}

/// List all under-replicated tasks.
pub fn list_tasks(coord: &dyn CoordStore) -> Result<Vec<RereplicateTask>> {
    coord
        .range_scan(
            keys::underreplicated_prefix(),
            keys::underreplicated_prefix_end(),
        )?
        .into_iter()
        .map(|rec| RereplicateTask::from_bytes(&rec.value))
        .collect()
}

/// Delete a completed task.
pub fn delete_task(coord: &dyn CoordStore, ledger_id: LedgerId) -> Result<()> {
    match coord.delete(&keys::underreplicated_key(ledger_id), None) {
        Ok(()) | Err(Error::NotFound(_)) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Try to acquire an ephemeral lock for a task. Returns false if held.
pub fn try_lock(coord: &dyn CoordStore, ledger_id: LedgerId, holder: &str) -> Result<bool> {
    match coord.put(
        &keys::underreplicated_lock_key(ledger_id),
        holder.as_bytes(),
        PutExpect::NotExists,
        true,
    ) {
        Ok(_) => Ok(true),
        // Memory store uses AlreadyExists; Oxia maps not-exists races to VersionConflict.
        Err(Error::AlreadyExists(_)) | Err(Error::VersionConflict(_)) => Ok(false),
        Err(err) => Err(err),
    }
}

/// Release the ephemeral lock (best-effort).
pub fn unlock(coord: &dyn CoordStore, ledger_id: LedgerId) {
    let _ = coord.delete(&keys::underreplicated_lock_key(ledger_id), None);
}
