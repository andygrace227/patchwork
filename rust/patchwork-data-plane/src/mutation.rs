use patchwork_common::dataplane_client::Record;
use serde::{Deserialize, Serialize};

// Shared by the WAL, pending write queue, and SQLite batches.
#[derive(Clone, Deserialize, Serialize)]
pub enum Mutation {
    Write(Record),
    DeleteTable {
        table_id: i64,
    },
    DeletePartitionKey {
        table_id: i64,
        partition_key: i64,
    },
    DeleteRange {
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    },
}
