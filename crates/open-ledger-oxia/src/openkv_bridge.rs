//! Sync [`CoordStore`] backed by OpenKV Gateway (`KVService` + `DSService`).
//!
//! Point ops use Get / Put (CAS + TTL). Listing uses DS Set indexes under
//! [`open_ledger::keys::index_ledgers`] and siblings, because Gateway has no
//! range-scan RPC.

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Duration;

use open_ledger::keys;
use open_ledger::Error;
use tracing::{error, info};

use crate::coord::{CoordStore, KvRecord, PutExpect};

mod pb {
    tonic::include_proto!("kv.v1");
}

use pb::ds_service_client::DsServiceClient;
use pb::kv_service_client::KvServiceClient;
use pb::{
    DeleteRequest, GetRequest, PutRequest, SAddRequest, SMembersRequest, SRemRequest,
};

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

/// Thread-safe OpenKV coordination handle (dedicated tokio bridge thread).
#[derive(Clone)]
pub struct OpenKvCoord {
    tx: SyncSender<Req>,
}

/// Options for [`OpenKvCoord::connect_with`].
#[derive(Debug, Clone)]
pub struct OpenKvOptions {
    /// Relative TTL for ephemeral puts (registry / election / locks).
    pub ephemeral_ttl: Duration,
}

impl Default for OpenKvOptions {
    fn default() -> Self {
        Self {
            ephemeral_ttl: Duration::from_secs(15),
        }
    }
}

impl OpenKvCoord {
    /// Connect to OpenKV Gateway (`host:port` or `http://…`) with default TTL.
    pub fn connect(gateway: impl Into<String>) -> open_ledger::Result<Self> {
        Self::connect_with(gateway, OpenKvOptions::default())
    }

    /// Connect with explicit ephemeral TTL.
    pub fn connect_with(
        gateway: impl Into<String>,
        opts: OpenKvOptions,
    ) -> open_ledger::Result<Self> {
        let addr = gateway.into();
        let (tx, rx) = mpsc::sync_channel(64);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("openkv-coord".into())
            .spawn(move || {
                if let Err(err) = run_bridge(addr, opts, rx, ready_tx) {
                    error!(%err, "openkv bridge exited");
                }
            })
            .map_err(|e| Error::Io(std::io::Error::other(e)))?;
        ready_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| Error::InvalidArgument("openkv bridge start timeout".into()))?
            .map_err(|e| Error::InvalidArgument(format!("openkv connect: {e}")))?;
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

impl CoordStore for OpenKvCoord {
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
    Error::InvalidArgument("openkv bridge closed".into())
}

fn rev_i64(v: u64) -> open_ledger::Result<i64> {
    i64::try_from(v).map_err(|_| Error::InvalidArgument(format!("openkv revision {v} too large")))
}

fn rev_u64(v: i64) -> open_ledger::Result<u64> {
    u64::try_from(v).map_err(|_| Error::InvalidArgument(format!("negative version {v}")))
}

fn map_status(status: tonic::Status) -> Error {
    Error::InvalidArgument(format!("openkv: {status}"))
}

fn run_bridge(
    addr: String,
    opts: OpenKvOptions,
    rx: Receiver<Req>,
    ready: SyncSender<Result<(), String>>,
) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async move {
        let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else {
            format!("http://{addr}")
        };
        let channel = match tonic::transport::Endpoint::from_shared(endpoint.clone()) {
            Ok(ep) => match ep.connect().await {
                Ok(c) => c,
                Err(err) => {
                    let _ = ready.send(Err(err.to_string()));
                    return;
                }
            },
            Err(err) => {
                let _ = ready.send(Err(err.to_string()));
                return;
            }
        };
        let mut kv = KvServiceClient::new(channel.clone());
        let mut ds = DsServiceClient::new(channel);
        info!(%endpoint, "openkv coord bridge connected");
        let _ = ready.send(Ok(()));

        let ttl_ms = opts.ephemeral_ttl.as_millis().min(i64::MAX as u128) as i64;
        let ttl_ms = ttl_ms.max(1);

        while let Ok(req) = rx.recv() {
            match req {
                Req::Get { key, resp } => {
                    let out = do_get(&mut kv, &key).await;
                    let _ = resp.send(out);
                }
                Req::Put {
                    key,
                    value,
                    expect,
                    ephemeral,
                    resp,
                } => {
                    let ttl = if ephemeral { ttl_ms } else { 0 };
                    let out = do_put(&mut kv, &mut ds, &key, value, expect, ttl).await;
                    let _ = resp.send(out);
                }
                Req::Delete { key, expect, resp } => {
                    let out = do_delete(&mut kv, &mut ds, &key, expect).await;
                    let _ = resp.send(out);
                }
                Req::RangeScan { min, max, resp } => {
                    let out = do_range(&mut kv, &mut ds, &min, &max).await;
                    let _ = resp.send(out);
                }
            }
        }
    });
    Ok(())
}

async fn do_get(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    key: &str,
) -> open_ledger::Result<KvRecord> {
    let resp = kv
        .get(GetRequest {
            key: key.as_bytes().to_vec(),
            linearizable: true,
        })
        .await
        .map_err(map_status)?
        .into_inner();
    if !resp.found {
        return Err(Error::NotFound(0));
    }
    Ok(KvRecord {
        key: key.to_string(),
        value: resp.value,
        version_id: rev_i64(resp.revision)?,
    })
}

async fn do_put(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    ds: &mut DsServiceClient<tonic::transport::Channel>,
    key: &str,
    value: Vec<u8>,
    expect: PutExpect,
    ttl_ms: i64,
) -> open_ledger::Result<i64> {
    let (cas, expected_revision) = match expect {
        PutExpect::Any => (false, 0),
        PutExpect::NotExists => (true, 0),
        PutExpect::Version(v) => (true, rev_u64(v)?),
    };
    let resp = kv
        .put(PutRequest {
            key: key.as_bytes().to_vec(),
            value,
            ttl_ms,
            cas,
            expected_revision,
        })
        .await
        .map_err(map_status)?
        .into_inner();
    if resp.cas_failed {
        return Err(match expect {
            PutExpect::NotExists => Error::AlreadyExists(0),
            _ => Error::VersionConflict(0),
        });
    }
    if !resp.ok {
        return Err(Error::InvalidArgument("openkv put rejected".into()));
    }
    let version = rev_i64(resp.revision)?;
    if let Some(index) = keys::index_set_for_key(key) {
        let _ = ds
            .s_add(SAddRequest {
                key: index.as_bytes().to_vec(),
                members: vec![key.as_bytes().to_vec()],
            })
            .await
            .map_err(map_status)?;
    }
    Ok(version)
}

async fn do_delete(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    ds: &mut DsServiceClient<tonic::transport::Channel>,
    key: &str,
    expect: Option<i64>,
) -> open_ledger::Result<()> {
    let (cas, expected_revision) = match expect {
        None => (false, 0),
        Some(v) => (true, rev_u64(v)?),
    };
    let resp = kv
        .delete(DeleteRequest {
            key: key.as_bytes().to_vec(),
            cas,
            expected_revision,
        })
        .await
        .map_err(map_status)?
        .into_inner();
    if resp.cas_failed {
        return Err(Error::VersionConflict(0));
    }
    if !resp.deleted && expect.is_some() {
        return Err(Error::NotFound(0));
    }
    if let Some(index) = keys::index_set_for_key(key) {
        let _ = ds
            .s_rem(SRemRequest {
                key: index.as_bytes().to_vec(),
                members: vec![key.as_bytes().to_vec()],
            })
            .await
            .map_err(map_status)?;
    }
    Ok(())
}

async fn do_range(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    ds: &mut DsServiceClient<tonic::transport::Channel>,
    min: &str,
    max: &str,
) -> open_ledger::Result<Vec<KvRecord>> {
    let Some(index) = keys::index_set_for_range(min, max) else {
        return Err(Error::InvalidArgument(format!(
            "openkv range_scan unsupported range [{min}, {max})"
        )));
    };
    let members = ds
        .s_members(SMembersRequest {
            key: index.as_bytes().to_vec(),
        })
        .await
        .map_err(map_status)?
        .into_inner()
        .members;
    let mut out = Vec::new();
    for raw in members {
        let key = String::from_utf8_lossy(&raw).into_owned();
        if key.as_str() < min || key.as_str() >= max {
            continue;
        }
        match do_get(kv, &key).await {
            Ok(rec) => out.push(rec),
            Err(Error::NotFound(_)) => {
                // Stale index member — drop from set.
                let _ = ds
                    .s_rem(SRemRequest {
                        key: index.as_bytes().to_vec(),
                        members: vec![raw],
                    })
                    .await;
            }
            Err(err) => return Err(err),
        }
    }
    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_ledger::keys;

    #[test]
    fn index_routing_matches_ol_prefixes() {
        assert_eq!(
            keys::index_set_for_key("/open-ledger/ledgers/9"),
            Some(keys::index_ledgers())
        );
        assert_eq!(
            keys::index_set_for_range(keys::bookies_prefix(), keys::bookies_prefix_end()),
            Some(keys::index_bookies())
        );
        assert!(keys::index_set_for_range("/a", "/b").is_none());
    }

    #[test]
    fn rev_roundtrip() {
        assert_eq!(rev_i64(42).unwrap(), 42);
        assert_eq!(rev_u64(7).unwrap(), 7);
        assert!(rev_u64(-1).is_err());
    }
}
