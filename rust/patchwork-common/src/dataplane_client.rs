//! Data-plane calls. Pass the node's base URL, e.g. http://127.0.0.1:3000.
//! Uses cached gRPC channels exclusively; URLs still use http:// or https://.
//! Write timestamps are caller-supplied Unix microseconds; preserve them on retries.
use crate::grpc::{
    MAX_MESSAGE_SIZE,
    proto::{self, data_plane_client::DataPlaneClient},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::{
    Response, Status,
    transport::{Channel, Endpoint},
};

/// Matches the data plane's data::Model, including its caller-supplied write timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub table_id: i64,
    pub partition_key: i64,
    pub secondary_key: i64,
    pub data: Vec<u8>,
    pub timestamp: i64,
    pub deleted: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AccessStatistics {
    pub reads_per_second: f64,
    pub writes_per_second: f64,
    /// Reporting nodes; merge each node once per partition key.
    pub contributing_nodes: u64,
    pub average_write_position: Option<i64>,
    // Upper middle key for an even number of writes; always an observed key.
    pub median_write_position: Option<i64>,
    pub read_count: u64,
    pub write_count: u64,
    #[serde(with = "write_position_sum")]
    pub write_position_sum: i128,
    // For merged statistics, this is the shortest contributing observation window.
    pub window_seconds: f64,
}

// Carry wide sums as decimal strings through the flattened JSON response.
mod write_position_sum {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(sum: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(sum)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl AccessStatistics {
    /// Merge sampled writes and add each node's independently measured rates.
    pub fn merge(&mut self, other: &Self) {
        // Individual medians cannot determine the median of combined samples.
        self.median_write_position = match (self.write_count, other.write_count) {
            (0, _) => other.median_write_position,
            (_, 0) => self.median_write_position,
            _ => None,
        };
        self.reads_per_second += other.reads_per_second;
        self.writes_per_second += other.writes_per_second;
        self.contributing_nodes += other.contributing_nodes;
        self.read_count += other.read_count;
        self.write_count += other.write_count;
        self.write_position_sum += other.write_position_sum;
        self.average_write_position = if self.write_count == 0 {
            None
        } else {
            Some((self.write_position_sum / self.write_count as i128) as i64)
        };
        if self.window_seconds == 0.0 {
            self.window_seconds = other.window_seconds;
        } else if other.window_seconds > 0.0 {
            self.window_seconds = self.window_seconds.min(other.window_seconds);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Telemetry {
    pub table_id: i64,
    pub partition_key: i64,
    #[serde(flatten)]
    pub statistics: AccessStatistics,
}

/// A reusable data-plane client. Clones share cached connections by node URL.
#[derive(Clone, Default)]
pub struct Client {
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl Client {
    pub fn new() -> Self {
        Self::default()
    }

    /// Save on this node and all supplied replica URLs (excluding this node).
    /// An error may leave some copies written. An empty list writes locally only.
    pub async fn put_with_replicas(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Vec<u8>,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), Status> {
        self.write_with_replicas(
            url,
            table_id,
            partition_key,
            secondary_key,
            data,
            replicas,
            timestamp,
            false,
        )
        .await
    }

    pub async fn write_with_replicas(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Vec<u8>,
        replicas: &[String],
        timestamp: i64,
        deleted: bool,
    ) -> Result<(), Status> {
        rpc(
            self.grpc(url)?.write_with_replicas(write_request(
                table_id,
                partition_key,
                secondary_key,
                data,
                timestamp,
                deleted,
                replicas,
            )?),
            60,
        )
        .await?;
        Ok(())
    }

    /// Wait for local storage, then let the neighbor actor send remote writes.
    /// Requires at least one remote URL, excluding this node.
    /// Neighbor requests are queued independently; their failures are logged.
    pub async fn put_with_replicas_inconsistent(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Vec<u8>,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), Status> {
        self.write_with_replicas_inconsistent(
            url,
            table_id,
            partition_key,
            secondary_key,
            data,
            replicas,
            timestamp,
            false,
        )
        .await
    }

    pub async fn write_with_replicas_inconsistent(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Vec<u8>,
        replicas: &[String],
        timestamp: i64,
        deleted: bool,
    ) -> Result<(), Status> {
        rpc(
            self.grpc(url)?
                .write_with_replicas_inconsistent(write_request(
                    table_id,
                    partition_key,
                    secondary_key,
                    data,
                    timestamp,
                    deleted,
                    replicas,
                )?),
            30,
        )
        .await?;
        Ok(())
    }

    /// Delete all records for this table on one node; returns {"deleted": N}.
    pub async fn delete_table(&self, url: &str, table_id: i64) -> Result<Value, Status> {
        let result = rpc(
            self.grpc(url)?
                .delete_table(proto::TableRequest { table_id }),
            30,
        )
        .await?;
        Ok(serde_json::json!({"deleted": result.count}))
    }

    /// Returns {"size_bytes": N} for the main SQLite file, excluding WAL and SHM.
    pub async fn get_size(&self, url: &str) -> Result<Value, Status> {
        let result = rpc(self.grpc(url)?.get_size(proto::Empty {}), 30).await?;
        Ok(serde_json::json!({"size_bytes": result.size_bytes}))
    }

    pub async fn get_write_backlog(&self, url: &str) -> Result<Value, Status> {
        let result = rpc(self.grpc(url)?.get_write_backlog(proto::Empty {}), 30).await?;
        Ok(serde_json::json!({
            "queued": result.queued,
            "in_flight": result.in_flight,
            "total": result.total,
        }))
    }

    /// Recent local accesses, grouped by table and partition key.
    pub async fn get_telemetry(&self, url: &str) -> Result<Vec<Telemetry>, Status> {
        rpc(self.grpc(url)?.get_telemetry(proto::Empty {}), 30)
            .await?
            .entries
            .into_iter()
            .map(TryInto::try_into)
            .collect()
    }

    /// Recent local accesses for one table, keyed by partition key.
    pub async fn get_telemetry_for_table(
        &self,
        url: &str,
        table_id: i64,
    ) -> Result<HashMap<i64, Telemetry>, Status> {
        rpc(
            self.grpc(url)?
                .get_telemetry_for_table(proto::TableRequest { table_id }),
            30,
        )
        .await?
        .entries
        .into_iter()
        .map(|entry| {
            let entry: Telemetry = entry.try_into()?;
            Ok((entry.partition_key, entry))
        })
        .collect()
    }

    /// Returns {"count": N} records in [lower_bound, upper_bound).
    pub async fn count_range(
        &self,
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Value, Status> {
        let result = rpc(
            self.grpc(url)?
                .count_range(range_request(table_id, upper_bound, lower_bound, false)),
            30,
        )
        .await?;
        Ok(serde_json::json!({"count": result.count}))
    }

    /// Returns {"size_bytes": N} stored JSON payload bytes, excluding keys and SQLite overhead.
    pub async fn get_range_size(
        &self,
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Value, Status> {
        let result = rpc(
            self.grpc(url)?.get_range_size(range_request(
                table_id,
                upper_bound,
                lower_bound,
                false,
            )),
            30,
        )
        .await?;
        Ok(serde_json::json!({"size_bytes": result.size_bytes}))
    }

    /// Preserve this timestamp when retrying or copying a previously written record.
    pub async fn upsert(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Vec<u8>,
        timestamp: i64,
    ) -> Result<(), Status> {
        self.write_record(
            url,
            table_id,
            partition_key,
            secondary_key,
            data,
            timestamp,
            false,
        )
        .await
    }

    pub async fn write_record(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Vec<u8>,
        timestamp: i64,
        deleted: bool,
    ) -> Result<(), Status> {
        rpc(
            self.grpc(url)?.write_record(write_request(
                table_id,
                partition_key,
                secondary_key,
                data,
                timestamp,
                deleted,
                &[],
            )?),
            30,
        )
        .await?;
        Ok(())
    }

    /// Returns the full record, including its caller-supplied write timestamp.
    /// A missing record returns gRPC NotFound.
    pub async fn get(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
    ) -> Result<Record, Status> {
        rpc(
            self.grpc(url)?.get_record(proto::RecordRequest {
                table_id,
                partition_key,
                secondary_key,
                include_deleted: false,
            }),
            30,
        )
        .await?
        .try_into()
    }

    pub async fn delete(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        timestamp: i64,
    ) -> Result<Value, Status> {
        let result = rpc(
            self.grpc(url)?.delete_record(proto::DeleteRequest {
                table_id,
                partition_key,
                secondary_key,
                timestamp: Some(timestamp),
            }),
            30,
        )
        .await?;
        Ok(serde_json::json!({"deleted": result.count}))
    }

    pub async fn delete_with_replicas(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), Status> {
        self.write_with_replicas(
            url,
            table_id,
            partition_key,
            secondary_key,
            &Vec::new(),
            replicas,
            timestamp,
            true,
        )
        .await
    }

    pub async fn delete_with_replicas_inconsistent(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), Status> {
        self.write_with_replicas_inconsistent(
            url,
            table_id,
            partition_key,
            secondary_key,
            &Vec::new(),
            replicas,
            timestamp,
            true,
        )
        .await
    }

    /// Read the stored record, including a tombstone, for reconciliation.
    pub async fn get_including_deleted(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
    ) -> Result<Record, Status> {
        rpc(
            self.grpc(url)?.get_record(proto::RecordRequest {
                table_id,
                partition_key,
                secondary_key,
                include_deleted: true,
            }),
            30,
        )
        .await?
        .try_into()
    }

    /// Copy/reconciliation reads must include deletion markers.
    pub async fn get_range_including_deleted(
        &self,
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Vec<Record>, Status> {
        rpc(
            self.grpc(url)?
                .get_range(range_request(table_id, upper_bound, lower_bound, true)),
            30,
        )
        .await?
        .records
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    pub async fn get_partition_key(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
    ) -> Result<Vec<Record>, Status> {
        rpc(
            self.grpc(url)?.get_partition_key(proto::PartitionRequest {
                table_id,
                partition_key,
                include_deleted: false,
            }),
            30,
        )
        .await?
        .records
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    pub async fn delete_partition_key(
        &self,
        url: &str,
        table_id: i64,
        partition_key: i64,
    ) -> Result<Value, Status> {
        let result = rpc(
            self.grpc(url)?
                .delete_partition_key(proto::PartitionRequest {
                    table_id,
                    partition_key,
                    include_deleted: false,
                }),
            30,
        )
        .await?;
        Ok(serde_json::json!({"deleted": result.count}))
    }

    /// Bounds are [lower_bound, upper_bound); argument order matches ReadShard::get_range.
    pub async fn get_range(
        &self,
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Vec<Record>, Status> {
        rpc(
            self.grpc(url)?
                .get_range(range_request(table_id, upper_bound, lower_bound, false)),
            30,
        )
        .await?
        .records
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    /// Deletes partition keys in [lower_bound, upper_bound); returns {"deleted": N}.
    pub async fn delete_range(
        &self,
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Value, Status> {
        let result = rpc(
            self.grpc(url)?
                .delete_range(range_request(table_id, upper_bound, lower_bound, false)),
            30,
        )
        .await?;
        Ok(serde_json::json!({"deleted": result.count}))
    }

    // Cloning a channel shares its HTTP/2 connection and allows concurrent requests.
    fn grpc(&self, url: &str) -> Result<DataPlaneClient<Channel>, Status> {
        let url = url.trim_end_matches('/');
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| Status::internal("channel cache poisoned"))?;
        let channel = match channels.get(url) {
            Some(channel) => channel.clone(),
            None => {
                let endpoint = Endpoint::new(url.to_owned())
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
                    .connect_timeout(Duration::from_secs(5))
                    .tcp_nodelay(true);
                let channel = endpoint.connect_lazy();
                channels.insert(url.to_owned(), channel.clone());
                channel
            }
        };
        Ok(DataPlaneClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE))
    }
}

async fn rpc<T>(
    call: impl std::future::Future<Output = Result<Response<T>, Status>>,
    seconds: u64,
) -> Result<T, Status> {
    tokio::time::timeout(Duration::from_secs(seconds), call)
        .await
        .map_err(|_| Status::deadline_exceeded("data-plane request timed out"))?
        .map(Response::into_inner)
}

fn write_request(
    table_id: i64,
    partition_key: i64,
    secondary_key: i64,
    data: &Vec<u8>,
    timestamp: i64,
    deleted: bool,
    replicas: &[String],
) -> Result<proto::WriteRequest, Status> {
    Ok(proto::WriteRequest {
        table_id,
        partition_key,
        secondary_key,
        data_bin: data.clone(),
        timestamp: Some(timestamp),
        deleted,
        replicas: replicas.to_vec(),
    })
}

fn range_request(
    table_id: i64,
    upper_bound: i64,
    lower_bound: i64,
    include_deleted: bool,
) -> proto::RangeRequest {
    proto::RangeRequest {
        table_id,
        lower_bound,
        upper_bound,
        include_deleted,
    }
}

#[cfg(test)]
mod tests;
