//! [`LedgerMetaStore`] backed by a [`CoordStore`] (Oxia or memory).

use open_ledger::keys;
use open_ledger::{Error, LedgerMeta, LedgerMetaStore, Result};

use crate::coord::{CoordStore, PutExpect};

/// Ledger metadata in Oxia (or any [`CoordStore`]).
///
/// `LedgerMeta::meta_version` is the Oxia record `version_id`.
pub struct OxiaMetaStore<C: CoordStore> {
    coord: C,
}

impl<C: CoordStore> OxiaMetaStore<C> {
    /// Wrap a coordination store.
    pub fn new(coord: C) -> Self {
        Self { coord }
    }

    /// Borrow the underlying store (registry / recovery share it).
    pub fn coord(&self) -> &C {
        &self.coord
    }

    fn put_meta(
        &self,
        ledger_id: u64,
        mut meta: LedgerMeta,
        expect: PutExpect,
    ) -> Result<LedgerMeta> {
        let bytes = encode_meta(&meta)?;
        let version = self
            .coord
            .put(&keys::ledger_key(ledger_id), &bytes, expect, false)
            .map_err(|e| remap_put(e, ledger_id, expect))?;
        meta.meta_version = version_u64(version)?;
        Ok(meta)
    }
}

impl<C: CoordStore> LedgerMetaStore for OxiaMetaStore<C> {
    fn get(&self, ledger_id: u64) -> Result<LedgerMeta> {
        let rec = self
            .coord
            .get(&keys::ledger_key(ledger_id))
            .map_err(|e| remap_not_found(e, ledger_id))?;
        let mut meta: LedgerMeta = decode_meta(&rec.value)?;
        meta.ledger_id = ledger_id;
        meta.meta_version = version_u64(rec.version_id)?;
        Ok(meta)
    }

    fn create(&self, meta: LedgerMeta) -> Result<LedgerMeta> {
        let ledger_id = meta.ledger_id;
        let mut stored = meta;
        stored.meta_version = 0;
        self.put_meta(ledger_id, stored, PutExpect::NotExists)
    }

    fn compare_and_swap(&self, expected_version: u64, meta: LedgerMeta) -> Result<LedgerMeta> {
        let ledger_id = meta.ledger_id;
        let expected = version_i64(expected_version)?;
        let mut stored = meta;
        stored.meta_version = expected_version;
        self.put_meta(ledger_id, stored, PutExpect::Version(expected))
    }
}

fn encode_meta(meta: &LedgerMeta) -> Result<Vec<u8>> {
    serde_json::to_vec(meta).map_err(|e| Error::Codec(format!("ledger meta json: {e}")))
}

fn decode_meta(bytes: &[u8]) -> Result<LedgerMeta> {
    serde_json::from_slice(bytes).map_err(|e| Error::Codec(format!("ledger meta json: {e}")))
}

fn version_u64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| Error::InvalidArgument(format!("oxia version_id {v}")))
}

fn version_i64(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| Error::InvalidArgument(format!("meta_version {v} too large")))
}

fn remap_not_found(err: Error, ledger_id: u64) -> Error {
    match err {
        Error::NotFound(_) => Error::NotFound(ledger_id),
        other => other,
    }
}

fn remap_put(err: Error, ledger_id: u64, expect: PutExpect) -> Error {
    match (expect, err) {
        (PutExpect::NotExists, Error::AlreadyExists(_) | Error::VersionConflict(_)) => {
            Error::AlreadyExists(ledger_id)
        }
        (PutExpect::Version(_), Error::NotFound(_)) => Error::NotFound(ledger_id),
        (PutExpect::Version(_), Error::VersionConflict(_)) => Error::VersionConflict(ledger_id),
        (_, other) => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::MemoryCoordStore;
    use open_ledger::{MetaState, Quorum};

    #[test]
    fn create_get_cas_roundtrip() {
        let store = OxiaMetaStore::new(MemoryCoordStore::new());
        let meta = LedgerMeta::new_open(7, Quorum::ONE, vec!["b0".into()], b"k").unwrap();
        let created = store.create(meta).unwrap();
        assert!(created.meta_version > 0);
        let got = store.get(7).unwrap();
        assert_eq!(got.ledger_id, 7);
        assert_eq!(got.meta_version, created.meta_version);

        let mut next = got.clone();
        next.state = MetaState::Closed;
        next.last_entry_id = 3;
        let swapped = store
            .compare_and_swap(created.meta_version, next)
            .unwrap();
        assert_eq!(swapped.state, MetaState::Closed);
        assert_ne!(swapped.meta_version, created.meta_version);

        assert!(matches!(
            store.compare_and_swap(created.meta_version, swapped.clone()),
            Err(Error::VersionConflict(7))
        ));
    }
}
