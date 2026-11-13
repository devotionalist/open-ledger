//! Bookie gRPC service — **Callback / async reaction model**.
//!
//! Mirrors OpenKV's CallbackService discipline (Rust/tonic equivalent of
//! C++ `ServerUnaryReactor`):
//!
//! 1. **Receive thread** (tonic poll / reactor): parse request, schedule work, return a future.
//! 2. **Work pool** (`spawn_blocking`): Journal fsync / ledger I/O — never block the reactor on disk.
//! 3. **Finish**: the future completes → tonic sends the response (no sync wait on the receive path).
//!
//! Hot path must not call blocking gRPC stubs or sleep on the receive thread.

use std::sync::Arc;

use bytes::Bytes;
use open_ledger::{Error, LedgerStore, LocalBookie, Quorum};
use prost::Message;
use prost_types::Any;
use tonic::{Request, Response, Status};

/// Wire shape of `google.rpc.Status` (grpc-status-details-bin).
#[derive(Clone, PartialEq, Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

pub mod pb {
    tonic::include_proto!("openledger.bookie.v1");
}

use pb::bookie_error::Kind as ErrKind;
use pb::bookie_server::Bookie as BookieRpc;
use pb::*;

/// Async Bookie service over [`LocalBookie`].
pub struct BookieService {
    bookie: Arc<LocalBookie>,
}

impl BookieService {
    /// Serve RPCs; journal work is offloaded via `spawn_blocking`.
    pub fn new(bookie: Arc<LocalBookie>) -> Self {
        Self { bookie }
    }
}

fn map_err(err: Error) -> Status {
    let (code, kind, ledger_id, entry_id, message) = match &err {
        Error::AlreadyExists(id) => (
            tonic::Code::AlreadyExists,
            ErrKind::AlreadyExists,
            *id,
            0,
            err.to_string(),
        ),
        Error::NotFound(id) => (
            tonic::Code::NotFound,
            ErrKind::NotFound,
            *id,
            0,
            err.to_string(),
        ),
        Error::Fenced(id) => (
            tonic::Code::FailedPrecondition,
            ErrKind::Fenced,
            *id,
            0,
            err.to_string(),
        ),
        Error::Sealed(id) => (
            tonic::Code::FailedPrecondition,
            ErrKind::Fenced,
            *id,
            0,
            err.to_string(),
        ),
        Error::BadEntryId {
            ledger_id,
            expected: _,
            got,
        } => (
            tonic::Code::InvalidArgument,
            ErrKind::Conflict,
            *ledger_id,
            *got,
            err.to_string(),
        ),
        Error::InvalidArgument(msg) => (
            tonic::Code::InvalidArgument,
            ErrKind::InvalidArgument,
            0,
            0,
            msg.clone(),
        ),
        Error::Io(_) | Error::Codec(_) => (
            tonic::Code::Internal,
            ErrKind::Internal,
            0,
            0,
            err.to_string(),
        ),
    };
    let detail = BookieError {
        kind: kind.into(),
        ledger_id,
        entry_id,
        message: message.clone(),
    };
    let rpc = GoogleRpcStatus {
        code: code as i32,
        message: message.clone(),
        details: vec![Any {
            type_url: "type.googleapis.com/openledger.bookie.v1.BookieError".into(),
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details(code, message, rpc.encode_to_vec().into())
}

fn to_pb_entry(e: open_ledger::Entry) -> Entry {
    Entry {
        ledger_id: e.ledger_id,
        entry_id: e.entry_id,
        lac: e.lac,
        payload: e.payload.to_vec(),
    }
}

/// Offload synchronous LedgerStore work; reaction thread only awaits the join.
#[allow(clippy::result_large_err)] // tonic::Status is large; matching gRPC handlers.
async fn react<T, F>(bookie: &Arc<LocalBookie>, f: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce(&LocalBookie) -> open_ledger::Result<T> + Send + 'static,
{
    let bookie = Arc::clone(bookie);
    tokio::task::spawn_blocking(move || f(&bookie))
        .await
        .map_err(|e| Status::internal(format!("work pool join: {e}")))?
        .map_err(map_err)
}

#[tonic::async_trait]
impl BookieRpc for BookieService {
    async fn create_ledger(
        &self,
        req: Request<CreateLedgerRequest>,
    ) -> Result<Response<CreateLedgerResponse>, Status> {
        let r = req.into_inner();
        let bookie_id = self.bookie.bookie_id().to_string();
        let _master_key = r.master_key; // M0/M1: auth key reserved for later
        react(&self.bookie, move |b| {
            b.create(
                r.ledger_id,
                Quorum::ONE,
                vec![bookie_id.clone()],
                &bookie_id,
            )
        })
        .await?;
        Ok(Response::new(CreateLedgerResponse {}))
    }

    async fn add_entry(
        &self,
        req: Request<AddEntryRequest>,
    ) -> Result<Response<AddEntryResponse>, Status> {
        let r = req.into_inner();
        react(&self.bookie, move |b| {
            b.append(
                r.ledger_id,
                r.entry_id,
                r.lac,
                Bytes::from(r.payload),
            )
        })
        .await?;
        Ok(Response::new(AddEntryResponse {}))
    }

    async fn recovery_add_entry(
        &self,
        req: Request<RecoveryAddEntryRequest>,
    ) -> Result<Response<RecoveryAddEntryResponse>, Status> {
        // M0 LocalBookie has no separate recovery path; append after fence via recover/close.
        let r = req.into_inner();
        react(&self.bookie, move |b| {
            b.append(
                r.ledger_id,
                r.entry_id,
                r.lac,
                Bytes::from(r.payload),
            )
        })
        .await?;
        Ok(Response::new(RecoveryAddEntryResponse {}))
    }

    async fn read_entry(
        &self,
        req: Request<ReadEntryRequest>,
    ) -> Result<Response<ReadEntryResponse>, Status> {
        let r = req.into_inner();
        let entry = react(&self.bookie, move |b| {
            let entries = b.read(r.ledger_id, r.entry_id, 1)?;
            entries
                .into_iter()
                .next()
                .ok_or(Error::NotFound(r.ledger_id))
        })
        .await?;
        Ok(Response::new(ReadEntryResponse {
            entry: Some(to_pb_entry(entry)),
        }))
    }

    async fn read_entries(
        &self,
        req: Request<ReadEntriesRequest>,
    ) -> Result<Response<ReadEntriesResponse>, Status> {
        let r = req.into_inner();
        let max = r.max as usize;
        let entries = react(&self.bookie, move |b| {
            b.read(r.ledger_id, r.from_entry_id, max)
        })
        .await?;
        Ok(Response::new(ReadEntriesResponse {
            entries: entries.into_iter().map(to_pb_entry).collect(),
        }))
    }

    async fn list_entries(
        &self,
        req: Request<ListEntriesRequest>,
    ) -> Result<Response<ListEntriesResponse>, Status> {
        let r = req.into_inner();
        let entry_ids = react(&self.bookie, move |b| {
            Ok(b.read(r.ledger_id, 0, usize::MAX)?
                .into_iter()
                .map(|e| e.entry_id)
                .collect::<Vec<_>>())
        })
        .await?;
        Ok(Response::new(ListEntriesResponse { entry_ids }))
    }

    async fn fence_ledger(
        &self,
        req: Request<FenceLedgerRequest>,
    ) -> Result<Response<FenceLedgerResponse>, Status> {
        let r = req.into_inner();
        react(&self.bookie, move |b| b.fence(r.ledger_id)).await?;
        Ok(Response::new(FenceLedgerResponse {}))
    }

    async fn set_explicit_lac(
        &self,
        req: Request<SetExplicitLacRequest>,
    ) -> Result<Response<SetExplicitLacResponse>, Status> {
        let _ = req;
        Err(Status::unimplemented(
            "SetExplicitLac not wired on M1 LocalBookie",
        ))
    }

    async fn read_last_confirmed(
        &self,
        req: Request<ReadLastConfirmedRequest>,
    ) -> Result<Response<ReadLastConfirmedResponse>, Status> {
        let r = req.into_inner();
        let lac = react(&self.bookie, move |b| b.last_entry_id(r.ledger_id)).await?;
        Ok(Response::new(ReadLastConfirmedResponse { lac }))
    }

    async fn last_entry_id(
        &self,
        req: Request<LastEntryIdRequest>,
    ) -> Result<Response<LastEntryIdResponse>, Status> {
        let r = req.into_inner();
        let entry_id = react(&self.bookie, move |b| b.last_entry_id(r.ledger_id)).await?;
        Ok(Response::new(LastEntryIdResponse { entry_id }))
    }

    async fn is_fenced(
        &self,
        req: Request<IsFencedRequest>,
    ) -> Result<Response<IsFencedResponse>, Status> {
        let r = req.into_inner();
        let fenced = react(&self.bookie, move |b| b.is_fenced(r.ledger_id)).await?;
        Ok(Response::new(IsFencedResponse { fenced }))
    }

    async fn delete_ledger(
        &self,
        req: Request<DeleteLedgerRequest>,
    ) -> Result<Response<DeleteLedgerResponse>, Status> {
        let r = req.into_inner();
        react(&self.bookie, move |b| b.delete(r.ledger_id)).await?;
        Ok(Response::new(DeleteLedgerResponse {}))
    }
}
