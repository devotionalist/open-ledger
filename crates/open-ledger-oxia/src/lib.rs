//! Oxia-backed metadata, bookie registry, and BookKeeper-style AutoRecovery.
//!
//! Uses [`oxia-client`](https://github.com/oxia-db/oxia-client-rust) 0.2 via a
//! dedicated compio thread ([`OxiaCoord`]) so the rest of the process can stay
//! on tokio / blocking worker threads.

#![deny(missing_docs)]

pub mod auditor;
pub mod coord;
pub mod election;
pub mod grpc_bookie;
pub mod meta_store;
pub mod oxia_bridge;
pub mod registry;
pub mod rereplicate;
pub mod task;
pub mod worker;

pub use auditor::{Auditor, AuditorConfig};
pub use coord::{CoordStore, KvRecord, MemoryCoordStore, PutExpect};
pub use election::AuditorElection;
pub use grpc_bookie::RemoteBookie;
pub use meta_store::OxiaMetaStore;
pub use oxia_bridge::OxiaCoord;
pub use registry::{BookieInfo, BookieRegistry};
pub use task::{
    delete_task, enqueue_task, list_tasks, try_lock, unlock, RereplicateTask,
};
pub use rereplicate::rereplicate_task;
pub use worker::{ReplicationWorker, WorkerConfig};
