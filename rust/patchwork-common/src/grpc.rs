//! Shared gRPC messages and conversions. Only record payloads contain JSON.
use crate::dataplane_client::{AccessStatistics, Record, Telemetry};
use tonic::Status;

pub mod proto {
    tonic::include_proto!("patchwork.dataplane");
}

/// Applies to both requests and responses, including range/copy replies.
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

impl TryFrom<proto::Record> for Record {
    type Error = Status;

    fn try_from(record: proto::Record) -> Result<Self, Status> {
        Ok(Self {
            table_id: record.table_id,
            partition_key: record.partition_key,
            secondary_key: record.secondary_key,
            data: serde_json::from_slice(&record.data_json)
                .map_err(|error| Status::data_loss(error.to_string()))?,
            timestamp: record.timestamp,
            deleted: record.deleted,
        })
    }
}

impl From<Telemetry> for proto::Telemetry {
    fn from(entry: Telemetry) -> Self {
        let stats = entry.statistics;
        Self {
            table_id: entry.table_id,
            partition_key: entry.partition_key,
            reads_per_second: stats.reads_per_second,
            writes_per_second: stats.writes_per_second,
            contributing_nodes: stats.contributing_nodes,
            average_write_position: stats.average_write_position,
            median_write_position: stats.median_write_position,
            read_count: stats.read_count,
            write_count: stats.write_count,
            write_position_sum: stats.write_position_sum.to_string(),
            window_seconds: stats.window_seconds,
        }
    }
}

impl TryFrom<proto::Telemetry> for Telemetry {
    type Error = Status;

    fn try_from(entry: proto::Telemetry) -> Result<Self, Status> {
        Ok(Self {
            table_id: entry.table_id,
            partition_key: entry.partition_key,
            statistics: AccessStatistics {
                reads_per_second: entry.reads_per_second,
                writes_per_second: entry.writes_per_second,
                contributing_nodes: entry.contributing_nodes,
                average_write_position: entry.average_write_position,
                median_write_position: entry.median_write_position,
                read_count: entry.read_count,
                write_count: entry.write_count,
                write_position_sum: entry.write_position_sum.parse().map_err(
                    |error: std::num::ParseIntError| Status::data_loss(error.to_string()),
                )?,
                window_seconds: entry.window_seconds,
            },
        })
    }
}
