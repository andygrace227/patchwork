use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub mod access_stats_actor;
pub mod cache;
mod store;

pub use access_stats_actor::AccessStatsActor;
pub use patchwork_common::dataplane_client::Telemetry as Model;

#[derive(Debug, kameo::Reply, serde::Serialize, PartialEq, Eq)]
pub struct WriteBacklog {
    pub queued: usize,
    pub in_flight: usize,
    pub total: usize,
}

#[derive(Clone)]
pub struct Access {
    pub partition_key: i64,
    pub time: Instant,
    pub is_write: bool,
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Wall-clock timestamps let queued accesses retain their age after a restart.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct QueuedAccess {
    pub table_id: i64,
    pub partition_key: i64,
    pub is_write: bool,
    pub partition_id: String,
    pub time_ms: i64,
}
