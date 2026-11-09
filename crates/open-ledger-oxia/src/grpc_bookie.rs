//! Blocking gRPC helpers for AutoRecovery workers.

use std::time::Duration;

use bytes::Bytes;
use open_ledger::{Error, Entry, EntryId, LedgerId, Result};
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

/// Generated Bookie gRPC types (docs omitted).
#[allow(missing_docs)]
pub mod pb {
    tonic::include_proto!("openledger.bookie.v1");
}

use pb::bookie_client::BookieClient;
use pb::{
    CreateLedgerRequest, FenceLedgerRequest, ListEntriesRequest, ReadEntryRequest,
    RecoveryAddEntryRequest,
};

/// Thin blocking client for the RPCs AutoRecovery needs.
pub struct RemoteBookie {
    rt: Runtime,
    addr: String,
}

impl RemoteBookie {
    /// Dial `http://host:port`.
    pub fn connect(addr: impl Into<String>) -> Result<Self> {
        let addr = normalize_addr(addr.into());
        let rt = Runtime::new().map_err(Error::Io)?;
        // Eager connect check.
        let _ = rt.block_on(connect_channel(&addr))?;
        Ok(Self { rt, addr })
    }

    fn client(&self) -> Result<BookieClient<Channel>> {
        let ch = self.rt.block_on(connect_channel(&self.addr))?;
        Ok(BookieClient::new(ch))
    }

    /// Create ledger on this bookie (idempotent when already present with same key).
    pub fn create_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()> {
        let mut c = self.client()?;
        match self.rt.block_on(c.create_ledger(Request::new(CreateLedgerRequest {
            ledger_id,
            master_key: master_key.to_vec(),
        }))) {
            Ok(_) => Ok(()),
            Err(status) if status.code() == Code::AlreadyExists => Ok(()),
            Err(status) => Err(map_status(status)),
        }
    }

    /// List entry ids on this bookie.
    pub fn list_entries(&self, ledger_id: LedgerId) -> Result<Vec<EntryId>> {
        let mut c = self.client()?;
        let resp = self
            .rt
            .block_on(c.list_entries(Request::new(ListEntriesRequest { ledger_id })))
            .map_err(map_status)?;
        Ok(resp.into_inner().entry_ids)
    }

    /// Read one entry.
    pub fn read_entry(&self, ledger_id: LedgerId, entry_id: EntryId) -> Result<Entry> {
        let mut c = self.client()?;
        let resp = self
            .rt
            .block_on(c.read_entry(Request::new(ReadEntryRequest {
                ledger_id,
                entry_id,
            })))
            .map_err(map_status)?;
        let e = resp
            .into_inner()
            .entry
            .ok_or_else(|| Error::Codec("empty ReadEntryResponse".into()))?;
        Ok(Entry::new(
            e.ledger_id,
            e.entry_id,
            e.lac,
            Bytes::from(e.payload),
        ))
    }

    /// Recovery add onto this bookie.
    pub fn recovery_add_entry(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
        master_key: &[u8],
    ) -> Result<()> {
        let mut c = self.client()?;
        self.rt
            .block_on(c.recovery_add_entry(Request::new(RecoveryAddEntryRequest {
                ledger_id,
                entry_id,
                lac,
                payload: payload.to_vec(),
                master_key: master_key.to_vec(),
            })))
            .map_err(map_status)
            .map(|_| ())
    }

    /// Fence ledger on this bookie.
    pub fn fence_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()> {
        let mut c = self.client()?;
        self.rt
            .block_on(c.fence_ledger(Request::new(FenceLedgerRequest {
                ledger_id,
                master_key: master_key.to_vec(),
            })))
            .map_err(map_status)
            .map(|_| ())
    }
}

fn normalize_addr(addr: String) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr
    } else {
        format!("http://{addr}")
    }
}

async fn connect_channel(addr: &str) -> Result<Channel> {
    Channel::from_shared(addr.to_string())
        .map_err(|e| Error::InvalidArgument(format!("bookie addr: {e}")))?
        .timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|e| Error::InvalidArgument(format!("dial bookie: {e}")))
}

fn map_status(status: Status) -> Error {
    match status.code() {
        Code::NotFound => Error::NotFound(0),
        Code::AlreadyExists => Error::AlreadyExists(0),
        Code::FailedPrecondition if status.message().contains("fenced") => Error::Fenced(0),
        Code::Aborted if status.message().contains("conflict") => Error::Conflict {
            ledger_id: 0,
            entry_id: 0,
        },
        _ => Error::InvalidArgument(format!("bookie rpc: {status}")),
    }
}
