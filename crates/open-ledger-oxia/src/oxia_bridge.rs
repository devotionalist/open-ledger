//! Sync [`CoordStore`] backed by [`oxia::OxiaClient`] on a dedicated compio thread.
//!
//! `oxia-client` 0.2 is single-threaded (compio). Other threads talk to it through
//! a channel, matching the SDK guidance for embedding in tokio apps.

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Duration;

use open_ledger::Error;
use oxia::{GetResult, OxiaClient, OxiaError};
use tracing::{error, info};

use crate::coord::{CoordStore, KvRecord, PutExpect};

enum Req {
    Get {
        key: String,
        resp: SyncSender<open_ledger::Result<KvRecord>>,
    },
    Put {
        key: String,
        value: Vec<u8>,
        expect: PutExpect,
        ephemeral: bool,
        resp: SyncSender<open_ledger::Result<i64>>,
    },
    Delete {
        key: String,
        expect: Option<i64>,
        resp: SyncSender<open_ledger::Result<()>>,
    },
    RangeScan {
        min: String,
        max: String,
        resp: SyncSender<open_ledger::Result<Vec<KvRecord>>>,
    },
}

/// Thread-safe handle to an Oxia client living on a compio runtime.
#[derive(Clone)]
pub struct OxiaCoord {
    tx: SyncSender<Req>,
}

impl OxiaCoord {
    /// Connect to Oxia and spawn the bridge thread.
    pub fn connect(service_address: impl Into<String>) -> open_ledger::Result<Self> {
        let addr = service_address.into();
        let (tx, rx) = mpsc::sync_channel(64);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("oxia-coord".into())
            .spawn(move || {
                if let Err(err) = run_bridge(addr, rx, ready_tx) {
                    error!(%err, "oxia bridge exited");
                }
            })
            .map_err(|e| Error::Io(std::io::Error::other(e)))?;
        ready_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| Error::InvalidArgument("oxia bridge start timeout".into()))?
            .map_err(|e| Error::InvalidArgument(format!("oxia connect: {e}")))?;
        Ok(Self { tx })
    }

    fn call<T>(
        &self,
        make: impl FnOnce(SyncSender<open_ledger::Result<T>>) -> Req,
    ) -> open_ledger::Result<T> {
        let (resp_tx, resp_rx) = mpsc::sync_channel(1);
        self.tx.send(make(resp_tx)).map_err(|_| bridge_closed())?;
        resp_rx.recv().map_err(|_| bridge_closed())?
    }
}

impl CoordStore for OxiaCoord {
    fn get(&self, key: &str) -> open_ledger::Result<KvRecord> {
        self.call(|resp| Req::Get {
            key: key.to_string(),
            resp,
        })
    }

    fn put(
        &self,
        key: &str,
        value: &[u8],
        expect: PutExpect,
        ephemeral: bool,
    ) -> open_ledger::Result<i64> {
        self.call(|resp| Req::Put {
            key: key.to_string(),
            value: value.to_vec(),
            expect,
            ephemeral,
            resp,
        })
    }

    fn delete(&self, key: &str, expect: Option<i64>) -> open_ledger::Result<()> {
        self.call(|resp| Req::Delete {
            key: key.to_string(),
            expect,
            resp,
        })
    }

    fn range_scan(
        &self,
        min_inclusive: &str,
        max_exclusive: &str,
    ) -> open_ledger::Result<Vec<KvRecord>> {
        self.call(|resp| Req::RangeScan {
            min: min_inclusive.to_string(),
            max: max_exclusive.to_string(),
            resp,
        })
    }
}

fn bridge_closed() -> Error {
    Error::InvalidArgument("oxia bridge closed".into())
}

fn map_err(err: OxiaError) -> Error {
    match err {
        OxiaError::KeyNotFound => Error::NotFound(0),
        // expected_record_not_exists and expected_version_id both surface as this.
        OxiaError::UnexpectedVersionId => Error::VersionConflict(0),
        other => Error::InvalidArgument(format!("oxia: {other}")),
    }
}

fn kv_from_record(rec: GetResult) -> KvRecord {
    KvRecord {
        key: rec.key,
        value: rec.value.unwrap_or_default().to_vec(),
        version_id: rec.version.version_id,
    }
}

fn run_bridge(
    addr: String,
    rx: Receiver<Req>,
    ready: SyncSender<Result<(), String>>,
) -> Result<(), String> {
    let rt = compio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    rt.block_on(async move {
        let client = match OxiaClient::connect(addr).await {
            Ok(c) => c,
            Err(err) => {
                let _ = ready.send(Err(err.to_string()));
                return;
            }
        };
        info!("oxia coord bridge connected");
        let _ = ready.send(Ok(()));

        while let Ok(req) = rx.recv() {
            match req {
                Req::Get { key, resp } => {
                    let out = client.get(key).await.map(kv_from_record).map_err(map_err);
                    let _ = resp.send(out);
                }
                Req::Put {
                    key,
                    value,
                    expect,
                    ephemeral,
                    resp,
                } => {
                    let mut put = client.put(key, value);
                    match expect {
                        PutExpect::Any => {}
                        PutExpect::NotExists => put = put.expected_record_not_exists(),
                        PutExpect::Version(v) => put = put.expected_version_id(v),
                    }
                    if ephemeral {
                        put = put.ephemeral();
                    }
                    let out = put
                        .await
                        .map(|r| r.version.version_id)
                        .map_err(map_err);
                    let _ = resp.send(out);
                }
                Req::Delete { key, expect, resp } => {
                    let mut del = client.delete(key);
                    if let Some(v) = expect {
                        del = del.expected_version_id(v);
                    }
                    let out = del.await.map_err(map_err);
                    let _ = resp.send(out);
                }
                Req::RangeScan { min, max, resp } => {
                    let out = client
                        .range_scan(min, max)
                        .await
                        .map(|recs| recs.into_iter().map(kv_from_record).collect())
                        .map_err(map_err);
                    let _ = resp.send(out);
                }
            }
        }
    });
    Ok(())
}
