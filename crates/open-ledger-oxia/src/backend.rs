//! Select Oxia or OpenKV as the coordination / LedgerMeta backend.

use std::sync::Arc;
use std::time::Duration;

use open_ledger::Error;

use crate::coord::{CoordStore, KvRecord, PutExpect};
use crate::openkv_bridge::{OpenKvCoord, OpenKvOptions};
use crate::oxia_bridge::OxiaCoord;

/// Which coordination store to use for metadata and AutoRecovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordBackend {
    /// Apache Oxia (`version_id` + ephemeral sessions).
    Oxia,
    /// OpenKV Gateway (`revision` CAS + TTL; DS Set indexes for listing).
    OpenKv,
}

impl CoordBackend {
    /// Parse `oxia` / `openkv` (case-insensitive).
    pub fn parse(s: &str) -> open_ledger::Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "oxia" => Ok(Self::Oxia),
            "openkv" => Ok(Self::OpenKv),
            other => Err(Error::InvalidArgument(format!(
                "unknown coord backend {other:?}; expected oxia|openkv"
            ))),
        }
    }
}

/// Sized wrapper so registry / auditor / worker stay generic over a concrete type.
#[derive(Clone)]
pub enum AnyCoord {
    /// Oxia bridge.
    Oxia(OxiaCoord),
    /// OpenKV bridge.
    OpenKv(OpenKvCoord),
}

impl CoordStore for AnyCoord {
    fn get(&self, key: &str) -> open_ledger::Result<KvRecord> {
        match self {
            Self::Oxia(c) => c.get(key),
            Self::OpenKv(c) => c.get(key),
        }
    }

    fn put(
        &self,
        key: &str,
        value: &[u8],
        expect: PutExpect,
        ephemeral: bool,
    ) -> open_ledger::Result<i64> {
        match self {
            Self::Oxia(c) => c.put(key, value, expect, ephemeral),
            Self::OpenKv(c) => c.put(key, value, expect, ephemeral),
        }
    }

    fn delete(&self, key: &str, expect: Option<i64>) -> open_ledger::Result<()> {
        match self {
            Self::Oxia(c) => c.delete(key, expect),
            Self::OpenKv(c) => c.delete(key, expect),
        }
    }

    fn range_scan(
        &self,
        min_inclusive: &str,
        max_exclusive: &str,
    ) -> open_ledger::Result<Vec<KvRecord>> {
        match self {
            Self::Oxia(c) => c.range_scan(min_inclusive, max_exclusive),
            Self::OpenKv(c) => c.range_scan(min_inclusive, max_exclusive),
        }
    }
}

/// Connect to the selected backend.
pub fn connect_coord(
    backend: CoordBackend,
    oxia_addr: &str,
    openkv_addr: &str,
    ephemeral_ttl: Duration,
) -> open_ledger::Result<Arc<AnyCoord>> {
    match backend {
        CoordBackend::Oxia => Ok(Arc::new(AnyCoord::Oxia(OxiaCoord::connect(oxia_addr)?))),
        CoordBackend::OpenKv => Ok(Arc::new(AnyCoord::OpenKv(OpenKvCoord::connect_with(
            openkv_addr,
            OpenKvOptions { ephemeral_ttl },
        )?))),
    }
}
