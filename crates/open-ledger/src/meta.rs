//! Ledger metadata (owned by the metadata service).
//!
//! [`MemoryMetaStore`] is an in-process stand-in so recovery can be tested here.
//! Production metadata is provided by an external service.

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::types::{Ensemble, Fragment, LedgerId, MetaState, Quorum};

/// Authoritative ledger metadata. Not stored as the bookie's source of truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerMeta {
    /// Ledger id.
    pub ledger_id: LedgerId,
    /// Replication parameters. Fixed for the life of the ledger.
    pub quorum: Quorum,
    /// `OPEN` / `IN_RECOVERY` / `CLOSED`.
    pub state: MetaState,
    /// Final tail once [`MetaState::Closed`]. `-1` if the ledger has no entries.
    pub last_entry_id: i64,
    /// Ensemble history. The first fragment starts at entry 0.
    pub fragments: Vec<Fragment>,
    /// Ledger master key. Sent to bookies on add and fence.
    pub master_key: Vec<u8>,
    /// CAS version. The store assigns this.
    pub meta_version: u64,
}

impl LedgerMeta {
    /// Open ledger with a single fragment covering entry 0 onward.
    pub fn new_open(
        ledger_id: LedgerId,
        quorum: Quorum,
        ensemble: Ensemble,
        master_key: impl Into<Vec<u8>>,
    ) -> Result<Self> {
        quorum
            .validate()
            .map_err(|e| Error::InvalidArgument(e.into()))?;
        let master_key = master_key.into();
        if master_key.is_empty() {
            return Err(Error::InvalidArgument(
                "master key must not be empty".into(),
            ));
        }
        validate_ensemble(&ensemble, quorum)?;
        Ok(Self {
            ledger_id,
            quorum,
            state: MetaState::Open,
            last_entry_id: -1,
            fragments: vec![Fragment {
                first_entry_id: 0,
                ensemble,
            }],
            master_key,
            meta_version: 0,
        })
    }
}

pub(crate) fn validate_ensemble(ensemble: &[String], quorum: Quorum) -> Result<()> {
    if ensemble.len() != quorum.e as usize {
        return Err(Error::InvalidArgument(
            "ensemble length must equal E".into(),
        ));
    }
    let mut seen = HashSet::new();
    for id in ensemble {
        if id.is_empty() || !seen.insert(id.clone()) {
            return Err(Error::InvalidArgument(
                "ensemble bookie ids must be unique and non-empty".into(),
            ));
        }
    }
    Ok(())
}

/// CAS store for [`LedgerMeta`]. Production uses an external metadata service.
pub trait LedgerMetaStore: Send + Sync {
    /// Fetch metadata.
    fn get(&self, ledger_id: LedgerId) -> Result<LedgerMeta>;

    /// Insert a new ledger. Fails if the id exists.
    fn create(&self, meta: LedgerMeta) -> Result<LedgerMeta>;

    /// Replace the record when `expected_version` is current. Stores `expected_version + 1`.
    fn compare_and_swap(&self, expected_version: u64, meta: LedgerMeta) -> Result<LedgerMeta>;
}

/// Process-local metadata. Tests and single-process recovery use this.
#[derive(Debug, Default)]
pub struct MemoryMetaStore {
    inner: Mutex<BTreeMap<LedgerId, LedgerMeta>>,
}

impl MemoryMetaStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl LedgerMetaStore for MemoryMetaStore {
    fn get(&self, ledger_id: LedgerId) -> Result<LedgerMeta> {
        self.inner
            .lock()
            .expect("meta")
            .get(&ledger_id)
            .cloned()
            .ok_or(Error::NotFound(ledger_id))
    }

    fn create(&self, mut meta: LedgerMeta) -> Result<LedgerMeta> {
        let mut g = self.inner.lock().expect("meta");
        if g.contains_key(&meta.ledger_id) {
            return Err(Error::AlreadyExists(meta.ledger_id));
        }
        meta.meta_version = 1;
        g.insert(meta.ledger_id, meta.clone());
        Ok(meta)
    }

    fn compare_and_swap(&self, expected_version: u64, mut meta: LedgerMeta) -> Result<LedgerMeta> {
        let mut g = self.inner.lock().expect("meta");
        let cur = g
            .get(&meta.ledger_id)
            .ok_or(Error::NotFound(meta.ledger_id))?;
        if cur.meta_version != expected_version {
            return Err(Error::VersionConflict(meta.ledger_id));
        }
        meta.meta_version = expected_version + 1;
        g.insert(meta.ledger_id, meta.clone());
        Ok(meta)
    }
}
