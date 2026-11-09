//! Bookie registry using Oxia ephemeral records (BK `/available` analogue).

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use open_ledger::keys;
use open_ledger::{Error, Result};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::coord::{CoordStore, PutExpect};
use crate::observe::{noop_observer, SharedObserver};

/// Bookie registration payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookieInfo {
    /// Bookie id (matches `--bookie-id` / pod name).
    pub bookie_id: String,
    /// Reachable gRPC address (`host:port`).
    pub address: String,
}

/// Lists live bookies and can keep a local registration alive.
pub struct BookieRegistry<C: CoordStore> {
    coord: Arc<C>,
    observer: SharedObserver,
}

impl<C: CoordStore + 'static> BookieRegistry<C> {
    /// Share a coordination store.
    pub fn new(coord: Arc<C>) -> Self {
        Self::with_observer(coord, noop_observer())
    }

    /// Share a coordination store with metrics.
    pub fn with_observer(coord: Arc<C>, observer: SharedObserver) -> Self {
        Self { coord, observer }
    }

    /// Register / refresh this bookie as an ephemeral record.
    pub fn register(&self, info: &BookieInfo) -> Result<()> {
        let bytes = serde_json::to_vec(info).map_err(|e| Error::Codec(e.to_string()))?;
        let result = self.coord.put(
            &keys::bookie_key(&info.bookie_id),
            &bytes,
            PutExpect::Any,
            true,
        );
        self.observer.registry_heartbeat(result.is_ok());
        result?;
        Ok(())
    }

    /// Spawn a background refresher (re-put ephemeral on an interval).
    pub fn spawn_heartbeat(self: &Arc<Self>, info: BookieInfo, every: Duration) {
        let this = Arc::clone(self);
        thread::Builder::new()
            .name("bookie-registry".into())
            .spawn(move || loop {
                if let Err(err) = this.register(&info) {
                    warn!(%err, bookie_id = %info.bookie_id, "bookie registry refresh failed");
                }
                thread::sleep(every);
            })
            .expect("spawn bookie registry");
    }

    /// Snapshot currently registered bookies.
    pub fn list_alive(&self) -> Result<Vec<BookieInfo>> {
        let recs = self
            .coord
            .range_scan(keys::bookies_prefix(), keys::bookies_prefix_end())?;
        Ok(recs
            .into_iter()
            .filter_map(|rec| match serde_json::from_slice::<BookieInfo>(&rec.value) {
                Ok(info) => Some(info),
                Err(err) => {
                    warn!(%err, key = %rec.key, "skip bad bookie registry record");
                    None
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::MemoryCoordStore;

    #[test]
    fn register_and_list() {
        let coord = Arc::new(MemoryCoordStore::new());
        let reg = BookieRegistry::new(coord);
        reg.register(&BookieInfo {
            bookie_id: "bookie-0".into(),
            address: "127.0.0.1:3181".into(),
        })
        .unwrap();
        let alive = reg.list_alive().unwrap();
        assert_eq!(alive.len(), 1);
        assert_eq!(alive[0].bookie_id, "bookie-0");
    }
}
