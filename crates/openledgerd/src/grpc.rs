//! gRPC Bookie service over [`LocalBookie`].

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use open_ledger::{Bookie, Error, LocalBookie};
use prost::Message;
use prost_types::Any;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::metrics::Metrics;

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

/// Wraps a durable local bookie for tonic.
pub struct BookieService {
    bookie: Arc<LocalBookie>,
    metrics: Arc<Metrics>,
}

impl BookieService {
    /// Serve RPCs against `bookie`, recording Prometheus metrics.
    pub fn new(bookie: Arc<LocalBookie>, metrics: Arc<Metrics>) -> Self {
        Self { bookie, metrics }
    }

    /// Run a bookie call on a blocking thread with span + metrics.
    ///
    /// `fixed_bytes` is recorded on both success and failure (write paths).
    /// `bytes_from_ok` supplies payload size only after a successful read.
    #[allow(clippy::result_large_err)]
    async fn run_rpc<T, F, B>(
        &self,
        method: &'static str,
        ledger_id: Option<u64>,
        entry_id: Option<u64>,
        fixed_bytes: Option<(u64, &'static str)>,
        f: F,
        bytes_from_ok: B,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&LocalBookie) -> open_ledger::Result<T> + Send + 'static,
        B: FnOnce(&T) -> Option<(u64, &'static str)>,
    {
        let span = tracing::info_span!(
            "bookie.rpc",
            otel.kind = "server",
            rpc = method,
            ledger_id = ledger_id,
            entry_id = entry_id,
        );
        let bookie = Arc::clone(&self.bookie);
        let metrics = Arc::clone(&self.metrics);
        async move {
            let started = Instant::now();
            let raw = tokio::task::spawn_blocking(move || f(&bookie))
                .await
                .map_err(|e| Status::internal(format!("join: {e}")))?;
            match raw {
                Ok(v) => {
                    let bytes = fixed_bytes.or_else(|| bytes_from_ok(&v));
                    metrics.observe_rpc(method, true, None, bytes, started);
                    Ok(v)
                }
                Err(err) => {
                    let kind = error_kind(&err);
                    tracing::warn!(
                        method,
                        ledger_id,
                        entry_id,
                        error_kind = kind,
                        err = %err,
                        "bookie rpc failed"
                    );
                    metrics.observe_rpc(method, false, Some(kind), fixed_bytes, started);
                    Err(map_err(err))
                }
            }
        }
        .instrument(span)
        .await
    }

    #[allow(clippy::result_large_err)]
    async fn run_rpc_simple<T, F>(
        &self,
        method: &'static str,
        ledger_id: Option<u64>,
        entry_id: Option<u64>,
        fixed_bytes: Option<(u64, &'static str)>,
        f: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&LocalBookie) -> open_ledger::Result<T> + Send + 'static,
    {
        self.run_rpc(method, ledger_id, entry_id, fixed_bytes, f, |_| None)
            .await
    }
}

fn error_kind(err: &Error) -> &'static str {
    match err {
        Error::AlreadyExists(_) => "AlreadyExists",
        Error::NotFound(_) => "NotFound",
        Error::NoSuchEntry { .. } => "NoSuchEntry",
        Error::Conflict { .. } => "Conflict",
        Error::Fenced(_) => "Fenced",
        Error::Unauthorized(_) => "Unauthorized",
        Error::VersionConflict(_) => "VersionConflict",
        Error::MetaConflict(_) => "MetaConflict",
        Error::NotEnoughBookies => "NotEnoughBookies",
        Error::InvalidArgument(_) => "InvalidArgument",
        Error::Io(_) => "Io",
        Error::Codec(_) => "Codec",
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
        Error::NoSuchEntry {
            ledger_id,
            entry_id,
        } => (
            tonic::Code::NotFound,
            ErrKind::NoSuchEntry,
            *ledger_id,
            *entry_id,
            err.to_string(),
        ),
        Error::Conflict {
            ledger_id,
            entry_id,
        } => (
            tonic::Code::FailedPrecondition,
            ErrKind::Conflict,
            *ledger_id,
            *entry_id,
            err.to_string(),
        ),
        Error::Fenced(id) => (
            tonic::Code::FailedPrecondition,
            ErrKind::Fenced,
            *id,
            0,
            err.to_string(),
        ),
        Error::Unauthorized(id) => (
            tonic::Code::PermissionDenied,
            ErrKind::Unauthorized,
            *id,
            0,
            err.to_string(),
        ),
        Error::VersionConflict(id) => (
            tonic::Code::Aborted,
            ErrKind::VersionConflict,
            *id,
            0,
            err.to_string(),
        ),
        Error::MetaConflict(msg) => (
            tonic::Code::Aborted,
            ErrKind::MetaConflict,
            0,
            0,
            msg.clone(),
        ),
        Error::NotEnoughBookies => (
            tonic::Code::Unavailable,
            ErrKind::NotEnoughBookies,
            0,
            0,
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

#[tonic::async_trait]
impl BookieRpc for BookieService {
    async fn create_ledger(
        &self,
        req: Request<CreateLedgerRequest>,
    ) -> Result<Response<CreateLedgerResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        self.run_rpc_simple(
            "CreateLedger",
            Some(ledger_id),
            None,
            None,
            move |b| b.create_ledger(ledger_id, &r.master_key),
        )
        .await?;
        Ok(Response::new(CreateLedgerResponse {}))
    }

    async fn add_entry(
        &self,
        req: Request<AddEntryRequest>,
    ) -> Result<Response<AddEntryResponse>, Status> {
        let r = req.into_inner();
        let nbytes = r.payload.len() as u64;
        let ledger_id = r.ledger_id;
        let entry_id = r.entry_id;
        self.run_rpc_simple(
            "AddEntry",
            Some(ledger_id),
            Some(entry_id),
            Some((nbytes, "in")),
            move |b| {
                b.add_entry(
                    ledger_id,
                    entry_id,
                    r.lac,
                    Bytes::from(r.payload),
                    &r.master_key,
                )
            },
        )
        .await?;
        Ok(Response::new(AddEntryResponse {}))
    }

    async fn recovery_add_entry(
        &self,
        req: Request<RecoveryAddEntryRequest>,
    ) -> Result<Response<RecoveryAddEntryResponse>, Status> {
        let r = req.into_inner();
        let nbytes = r.payload.len() as u64;
        let ledger_id = r.ledger_id;
        let entry_id = r.entry_id;
        self.run_rpc_simple(
            "RecoveryAddEntry",
            Some(ledger_id),
            Some(entry_id),
            Some((nbytes, "in")),
            move |b| {
                b.recovery_add_entry(
                    ledger_id,
                    entry_id,
                    r.lac,
                    Bytes::from(r.payload),
                    &r.master_key,
                )
            },
        )
        .await?;
        Ok(Response::new(RecoveryAddEntryResponse {}))
    }

    async fn read_entry(
        &self,
        req: Request<ReadEntryRequest>,
    ) -> Result<Response<ReadEntryResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        let entry_id = r.entry_id;
        let entry = self
            .run_rpc(
                "ReadEntry",
                Some(ledger_id),
                Some(entry_id),
                None,
                move |b| b.read_entry(ledger_id, entry_id),
                |e| Some((e.payload.len() as u64, "out")),
            )
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
        let ledger_id = r.ledger_id;
        let from = r.from_entry_id;
        let entries = self
            .run_rpc(
                "ReadEntries",
                Some(ledger_id),
                Some(from),
                None,
                move |b| b.read_entries(ledger_id, from, max),
                |entries| {
                    let nbytes: u64 = entries.iter().map(|e| e.payload.len() as u64).sum();
                    Some((nbytes, "out"))
                },
            )
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
        let ledger_id = r.ledger_id;
        let entry_ids = self
            .run_rpc_simple("ListEntries", Some(ledger_id), None, None, move |b| {
                b.list_entries(ledger_id)
            })
            .await?;
        Ok(Response::new(ListEntriesResponse { entry_ids }))
    }

    async fn fence_ledger(
        &self,
        req: Request<FenceLedgerRequest>,
    ) -> Result<Response<FenceLedgerResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        self.run_rpc_simple(
            "FenceLedger",
            Some(ledger_id),
            None,
            None,
            move |b| b.fence_ledger(ledger_id, &r.master_key),
        )
        .await?;
        Ok(Response::new(FenceLedgerResponse {}))
    }

    async fn set_explicit_lac(
        &self,
        req: Request<SetExplicitLacRequest>,
    ) -> Result<Response<SetExplicitLacResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        self.run_rpc_simple(
            "SetExplicitLac",
            Some(ledger_id),
            None,
            None,
            move |b| b.set_explicit_lac(ledger_id, r.lac, &r.master_key),
        )
        .await?;
        Ok(Response::new(SetExplicitLacResponse {}))
    }

    async fn read_last_confirmed(
        &self,
        req: Request<ReadLastConfirmedRequest>,
    ) -> Result<Response<ReadLastConfirmedResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        let lac = self
            .run_rpc_simple(
                "ReadLastConfirmed",
                Some(ledger_id),
                None,
                None,
                move |b| b.read_last_confirmed(ledger_id),
            )
            .await?;
        Ok(Response::new(ReadLastConfirmedResponse { lac }))
    }

    async fn last_entry_id(
        &self,
        req: Request<LastEntryIdRequest>,
    ) -> Result<Response<LastEntryIdResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        let entry_id = self
            .run_rpc_simple("LastEntryId", Some(ledger_id), None, None, move |b| {
                b.last_entry_id(ledger_id)
            })
            .await?;
        Ok(Response::new(LastEntryIdResponse { entry_id }))
    }

    async fn is_fenced(
        &self,
        req: Request<IsFencedRequest>,
    ) -> Result<Response<IsFencedResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        let fenced = self
            .run_rpc_simple("IsFenced", Some(ledger_id), None, None, move |b| {
                b.is_fenced(ledger_id)
            })
            .await?;
        Ok(Response::new(IsFencedResponse { fenced }))
    }

    async fn delete_ledger(
        &self,
        req: Request<DeleteLedgerRequest>,
    ) -> Result<Response<DeleteLedgerResponse>, Status> {
        let r = req.into_inner();
        let ledger_id = r.ledger_id;
        self.run_rpc_simple("DeleteLedger", Some(ledger_id), None, None, move |b| {
            b.delete_ledger(ledger_id)
        })
        .await?;
        Ok(Response::new(DeleteLedgerResponse {}))
    }
}
