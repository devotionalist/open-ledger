//! Coordination-backed metadata, bookie registry, and BookKeeper-style AutoRecovery.
//!
//! Backends:
//! - **Oxia** ([`OxiaCoord`]): [`oxia-client`](https://github.com/oxia-db/oxia-client-rust) 0.2
//!   on a dedicated compio thread.
//! - **OpenKV** ([`OpenKvCoord`]): Gateway gRPC (`KVService` + `DSService` Set indexes)
//!   on a dedicated tokio thread.
//!
//! Pick with [`CoordBackend`] / [`connect_coord`]. The rest of the process stays on
//! blocking worker threads talking through [`CoordStore`].

#![deny(missing_docs)]

pub mod auditor;
pub mod backend;
pub mod coord;
pub mod election;
pub mod grpc_bookie;
pub mod meta_store;
pub mod observe;
pub mod openkv_bridge;
pub mod oxia_bridge;
pub mod registry;
pub mod rereplicate;
pub mod task;
pub mod worker;

pub use auditor::{Auditor, AuditorConfig};
pub use backend::{connect_coord, AnyCoord, CoordBackend};
pub use coord::{CoordStore, KvRecord, MemoryCoordStore, PutExpect};
pub use election::AuditorElection;
pub use grpc_bookie::RemoteBookie;
pub use meta_store::OxiaMetaStore;
pub use observe::{noop_observer, ArObserver, NoopObserver, SharedObserver};
pub use openkv_bridge::{OpenKvCoord, OpenKvOptions};
pub use oxia_bridge::OxiaCoord;
pub use registry::{BookieInfo, BookieRegistry};
pub use task::{
    delete_task, enqueue_task, list_tasks, try_lock, unlock, RereplicateTask,
};
pub use rereplicate::rereplicate_task;
pub use worker::{ReplicationWorker, WorkerConfig};
