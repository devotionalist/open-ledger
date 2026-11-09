//! Coordination store abstraction (Oxia, OpenKV, or in-memory).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use open_ledger::Error;

/// Conditional put expectation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutExpect {
    /// Unconditional put (at-least-once under retries).
    Any,
    /// Create only (record must not exist).
    NotExists,
    /// Compare-and-swap against this Oxia version id.
    Version(i64),
}

/// One key/value/version triple from a range scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRecord {
    /// Full key.
    pub key: String,
    /// Value bytes.
    pub value: Vec<u8>,
    /// Oxia version id (or memory fake version).
    pub version_id: i64,
}

/// Sync coordination API used by metadata, registry, and AutoRecovery.
pub trait CoordStore: Send + Sync {
    /// Get a key. `NotFound` when absent.
    fn get(&self, key: &str) -> open_ledger::Result<KvRecord>;

    /// Put with optional CAS / create-only / ephemeral.
    fn put(
        &self,
        key: &str,
        value: &[u8],
        expect: PutExpect,
        ephemeral: bool,
    ) -> open_ledger::Result<i64>;

    /// Delete; optional version check.
    fn delete(&self, key: &str, expect: Option<i64>) -> open_ledger::Result<()>;

    /// List records in `[min_inclusive, max_exclusive)`.
    fn range_scan(
        &self,
        min_inclusive: &str,
        max_exclusive: &str,
    ) -> open_ledger::Result<Vec<KvRecord>>;
}

impl<C: CoordStore + ?Sized> CoordStore for Arc<C> {
    fn get(&self, key: &str) -> open_ledger::Result<KvRecord> {
        (**self).get(key)
    }

    fn put(
        &self,
        key: &str,
        value: &[u8],
        expect: PutExpect,
        ephemeral: bool,
    ) -> open_ledger::Result<i64> {
        (**self).put(key, value, expect, ephemeral)
    }

    fn delete(&self, key: &str, expect: Option<i64>) -> open_ledger::Result<()> {
        (**self).delete(key, expect)
    }

    fn range_scan(
        &self,
        min_inclusive: &str,
        max_exclusive: &str,
    ) -> open_ledger::Result<Vec<KvRecord>> {
        (**self).range_scan(min_inclusive, max_exclusive)
    }
}

#[derive(Debug, Default)]
struct MemoryInner {
    map: BTreeMap<String, (Vec<u8>, i64, bool)>,
    next_version: i64,
}

impl MemoryInner {
    fn alloc_version(&mut self) -> i64 {
        self.next_version += 1;
        self.next_version
    }
}

/// Process-local coordination store for unit tests.
#[derive(Debug, Default)]
pub struct MemoryCoordStore {
    inner: Mutex<MemoryInner>,
}

impl MemoryCoordStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl CoordStore for MemoryCoordStore {
    fn get(&self, key: &str) -> open_ledger::Result<KvRecord> {
        let g = self.inner.lock().expect("coord");
        let (value, version_id, _) = g
            .map
            .get(key)
            .ok_or_else(|| Error::NotFound(0))?;
        Ok(KvRecord {
            key: key.to_string(),
            value: value.clone(),
            version_id: *version_id,
        })
    }

    fn put(
        &self,
        key: &str,
        value: &[u8],
        expect: PutExpect,
        ephemeral: bool,
    ) -> open_ledger::Result<i64> {
        let mut g = self.inner.lock().expect("coord");
        match expect {
            PutExpect::NotExists if g.map.contains_key(key) => {
                return Err(Error::AlreadyExists(0));
            }
            PutExpect::Version(v) => match g.map.get(key) {
                Some((_, cur, _)) if *cur == v => {}
                Some(_) => return Err(Error::VersionConflict(0)),
                None => return Err(Error::NotFound(0)),
            },
            PutExpect::Any | PutExpect::NotExists => {}
        }
        let version_id = g.alloc_version();
        g.map
            .insert(key.to_string(), (value.to_vec(), version_id, ephemeral));
        Ok(version_id)
    }

    fn delete(&self, key: &str, expect: Option<i64>) -> open_ledger::Result<()> {
        let mut g = self.inner.lock().expect("coord");
        match g.map.get(key) {
            None => Err(Error::NotFound(0)),
            Some((_, cur, _)) => {
                if expect.is_some_and(|v| *cur != v) {
                    return Err(Error::VersionConflict(0));
                }
                g.map.remove(key);
                Ok(())
            }
        }
    }

    fn range_scan(
        &self,
        min_inclusive: &str,
        max_exclusive: &str,
    ) -> open_ledger::Result<Vec<KvRecord>> {
        let g = self.inner.lock().expect("coord");
        Ok(g.map
            .range(min_inclusive.to_string()..max_exclusive.to_string())
            .map(|(k, (v, ver, _))| KvRecord {
                key: k.clone(),
                value: v.clone(),
                version_id: *ver,
            })
            .collect())
    }
}
