//! Data-plane calls. Pass the node's base URL, e.g. http://127.0.0.1:3000.
//! Record responses are typed; HTTP errors remain errors.
//! Write timestamps are caller-supplied Unix microseconds; preserve them on retries.
use reqwest::{Client as HttpClient, Method};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{collections::HashMap, sync::LazyLock, time::Duration};

static HTTP: LazyLock<HttpClient> = LazyLock::new(HttpClient::new);

/// Matches the data plane's data::Model, including its caller-supplied write timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub table_id: i64,
    pub partition_key: i64,
    pub secondary_key: i64,
    pub data: Value,
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

pub struct Client;

impl Client {
    /// Save on this node and all supplied replica URLs (excluding this node).
    /// An error may leave some copies written. An empty list writes locally only.
    pub async fn put_with_replicas(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Value,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), reqwest::Error> {
        Self::write_with_replicas(
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
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Value,
        replicas: &[String],
        timestamp: i64,
        deleted: bool,
    ) -> Result<(), reqwest::Error> {
        HTTP.put(format!(
            "{}/with-replicas",
            record_url(url, table_id, partition_key, secondary_key)
        ))
        .timeout(Duration::from_secs(60))
        .header("x-patchwork-timestamp", timestamp.to_string())
        .header("x-patchwork-deleted", deleted.to_string())
        .json(&serde_json::json!({ "data": data, "replicas": replicas }))
        .send()
        .await?
        .error_for_status()?;
        Ok(())
    }

    /// Wait for local storage and the first replica, then queue the remaining writes.
    /// Requires at least one remote URL, excluding this node. Order selects failover.
    /// Remaining writes are best effort; the queue is not durable.
    pub async fn put_with_replicas_inconsistent(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Value,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), reqwest::Error> {
        Self::write_with_replicas_inconsistent(
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
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Value,
        replicas: &[String],
        timestamp: i64,
        deleted: bool,
    ) -> Result<(), reqwest::Error> {
        HTTP.put(format!(
            "{}/with-replicas-inconsistent",
            record_url(url, table_id, partition_key, secondary_key)
        ))
        .timeout(Duration::from_secs(30))
        .header("x-patchwork-timestamp", timestamp.to_string())
        .header("x-patchwork-deleted", deleted.to_string())
        .json(&serde_json::json!({ "data": data, "replicas": replicas }))
        .send()
        .await?
        .error_for_status()?;
        Ok(())
    }

    /// Delete all records for this table on one node; returns {"deleted": N}.
    pub async fn delete_table(url: &str, table_id: i64) -> Result<Value, reqwest::Error> {
        json(
            Method::DELETE,
            format!("{}/tables/{table_id}", url.trim_end_matches('/')),
        )
        .await
    }

    /// Returns {"size_bytes": N} for the main SQLite file, excluding WAL and SHM.
    pub async fn get_size(url: &str) -> Result<Value, reqwest::Error> {
        json(
            Method::GET,
            format!("{}/shard/size", url.trim_end_matches('/')),
        )
        .await
    }

    /// Recent local accesses, grouped by table and partition key.
    pub async fn get_telemetry(url: &str) -> Result<Vec<Telemetry>, reqwest::Error> {
        json(
            Method::GET,
            format!("{}/shard/telemetry", url.trim_end_matches('/')),
        )
        .await
    }

    /// Recent local accesses for one table, keyed by partition key.
    pub async fn get_telemetry_for_table(
        url: &str,
        table_id: i64,
    ) -> Result<HashMap<i64, Telemetry>, reqwest::Error> {
        json(
            Method::GET,
            format!("{}/tables/{table_id}/telemetry", url.trim_end_matches('/')),
        )
        .await
    }

    /// Returns {"count": N} records in [lower_bound, upper_bound).
    pub async fn count_range(
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Value, reqwest::Error> {
        json(Method::GET, format!(
            "{}/tables/{table_id}/range/count?lower_bound={lower_bound}&upper_bound={upper_bound}",
            url.trim_end_matches('/')
        )).await
    }

    /// Returns {"size_bytes": N} stored JSON payload bytes, excluding keys and SQLite overhead.
    pub async fn get_range_size(
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Value, reqwest::Error> {
        json(Method::GET, format!(
            "{}/tables/{table_id}/range/size?lower_bound={lower_bound}&upper_bound={upper_bound}",
            url.trim_end_matches('/')
        )).await
    }

    /// Preserve this timestamp when retrying or copying a previously written record.
    pub async fn upsert(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Value,
        timestamp: i64,
    ) -> Result<(), reqwest::Error> {
        Self::write_record(
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
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &Value,
        timestamp: i64,
        deleted: bool,
    ) -> Result<(), reqwest::Error> {
        HTTP.put(record_url(url, table_id, partition_key, secondary_key))
            .timeout(Duration::from_secs(30))
            .header("x-patchwork-timestamp", timestamp.to_string())
            .header("x-patchwork-deleted", deleted.to_string())
            .json(data)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Returns the full record, including its caller-supplied write timestamp.
    /// A missing record returns an HTTP 404 error.
    pub async fn get(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
    ) -> Result<Record, reqwest::Error> {
        json(
            Method::GET,
            record_url(url, table_id, partition_key, secondary_key),
        )
        .await
    }

    pub async fn delete(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        timestamp: i64,
    ) -> Result<Value, reqwest::Error> {
        HTTP.delete(record_url(url, table_id, partition_key, secondary_key))
            .timeout(Duration::from_secs(30))
            .header("x-patchwork-timestamp", timestamp.to_string())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    pub async fn delete_with_replicas(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), reqwest::Error> {
        Self::write_with_replicas(
            url,
            table_id,
            partition_key,
            secondary_key,
            &Value::Null,
            replicas,
            timestamp,
            true,
        )
        .await
    }

    pub async fn delete_with_replicas_inconsistent(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        replicas: &[String],
        timestamp: i64,
    ) -> Result<(), reqwest::Error> {
        Self::write_with_replicas_inconsistent(
            url,
            table_id,
            partition_key,
            secondary_key,
            &Value::Null,
            replicas,
            timestamp,
            true,
        )
        .await
    }

    /// Read the stored record, including a tombstone, for reconciliation.
    pub async fn get_including_deleted(
        url: &str,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
    ) -> Result<Record, reqwest::Error> {
        json(
            Method::GET,
            format!(
                "{}?include_deleted=true",
                record_url(url, table_id, partition_key, secondary_key)
            ),
        )
        .await
    }

    /// Copy/reconciliation reads must include deletion markers.
    pub async fn get_range_including_deleted(
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Vec<Record>, reqwest::Error> {
        json(Method::GET, format!("{}/tables/{table_id}/range?lower_bound={lower_bound}&upper_bound={upper_bound}&include_deleted=true", url.trim_end_matches('/'))).await
    }

    pub async fn get_partition_key(
        url: &str,
        table_id: i64,
        partition_key: i64,
    ) -> Result<Vec<Record>, reqwest::Error> {
        json(Method::GET, partition_key_url(url, table_id, partition_key)).await
    }

    pub async fn delete_partition_key(
        url: &str,
        table_id: i64,
        partition_key: i64,
    ) -> Result<Value, reqwest::Error> {
        json(
            Method::DELETE,
            partition_key_url(url, table_id, partition_key),
        )
        .await
    }

    /// Bounds are [lower_bound, upper_bound); argument order matches ShardActor::get_range.
    pub async fn get_range(
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Vec<Record>, reqwest::Error> {
        json(
            Method::GET,
            format!(
                "{}/tables/{table_id}/range?lower_bound={lower_bound}&upper_bound={upper_bound}",
                url.trim_end_matches('/')
            ),
        )
        .await
    }

    /// Deletes partition keys in [lower_bound, upper_bound); returns {"deleted": N}.
    pub async fn delete_range(
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Value, reqwest::Error> {
        json(
            Method::DELETE,
            format!(
                "{}/tables/{table_id}/range?lower_bound={lower_bound}&upper_bound={upper_bound}",
                url.trim_end_matches('/')
            ),
        )
        .await
    }
}

fn partition_key_url(url: &str, table_id: i64, partition_key: i64) -> String {
    format!(
        "{}/tables/{table_id}/partition-keys/{partition_key}",
        url.trim_end_matches('/')
    )
}

fn record_url(url: &str, table_id: i64, partition_key: i64, secondary_key: i64) -> String {
    format!(
        "{}/records/{secondary_key}",
        partition_key_url(url, table_id, partition_key)
    )
}

async fn json<T: DeserializeOwned>(method: Method, url: String) -> Result<T, reqwest::Error> {
    HTTP.request(method, url)
        .timeout(Duration::from_secs(30))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merging_keeps_write_weight_across_nodes_and_windows() {
        let samples = [
            AccessStatistics {
                reads_per_second: 2.0,
                writes_per_second: 1.0,
                contributing_nodes: 1,
                average_write_position: Some(10),
                median_write_position: Some(10),
                read_count: 2,
                write_count: 1,
                write_position_sum: 10,
                window_seconds: 1.0,
            },
            AccessStatistics {
                reads_per_second: 3.0,
                writes_per_second: 4.5,
                contributing_nodes: 1,
                average_write_position: Some(100),
                median_write_position: Some(100),
                read_count: 6,
                write_count: 9,
                write_position_sum: 900,
                window_seconds: 2.0,
            },
            AccessStatistics {
                reads_per_second: 5.0,
                writes_per_second: 0.5,
                contributing_nodes: 1,
                average_write_position: Some(-50),
                median_write_position: Some(-50),
                read_count: 20,
                write_count: 2,
                write_position_sum: -100,
                window_seconds: 4.0,
            },
        ];
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
            let mut merged = AccessStatistics::default();
            for index in order {
                merged.merge(&samples[index]);
            }
            assert_eq!(merged.reads_per_second, 10.0);
            assert_eq!(merged.writes_per_second, 6.0);
            assert_eq!(merged.contributing_nodes, 3);
            assert_eq!(merged.read_count, 28);
            assert_eq!(merged.write_count, 12);
            assert_eq!(merged.write_position_sum, 810);
            assert_eq!(merged.average_write_position, Some(67));
            assert_eq!(merged.median_write_position, None);
            assert_eq!(merged.window_seconds, 1.0);
        }
    }

    #[test]
    fn read_only_merges_preserve_the_write_position() {
        let mut merged = AccessStatistics::default();
        assert_eq!(merged.contributing_nodes, 0);
        merged.merge(&AccessStatistics {
            reads_per_second: 1.0,
            contributing_nodes: 1,
            read_count: 10,
            window_seconds: 10.0,
            ..Default::default()
        });
        assert_eq!(merged.average_write_position, None);
        assert_eq!(merged.median_write_position, None);
        merged.merge(&AccessStatistics {
            writes_per_second: 0.1,
            contributing_nodes: 1,
            average_write_position: Some(-10),
            median_write_position: Some(-10),
            write_count: 1,
            write_position_sum: -10,
            window_seconds: 10.0,
            ..Default::default()
        });
        merged.merge(&AccessStatistics::default());
        assert_eq!(merged.average_write_position, Some(-10));
        assert_eq!(merged.median_write_position, Some(-10));
        assert_eq!(merged.window_seconds, 10.0);
        assert_eq!(merged.read_count, 10);
        assert_eq!(merged.contributing_nodes, 2);
    }

    #[test]
    fn large_key_sums_survive_json_and_merge_without_losing_precision() {
        let large = AccessStatistics {
            writes_per_second: 3.0,
            contributing_nodes: 1,
            average_write_position: Some(i64::MAX),
            median_write_position: Some(i64::MAX),
            write_count: 3,
            write_position_sum: i64::MAX as i128 * 3,
            window_seconds: 1.0,
            ..Default::default()
        };
        let mut merged: AccessStatistics =
            serde_json::from_slice(&serde_json::to_vec(&large).unwrap()).unwrap();
        let telemetry = Telemetry {
            table_id: 1,
            partition_key: 10,
            statistics: large,
        };
        let decoded: Telemetry =
            serde_json::from_slice(&serde_json::to_vec(&telemetry).unwrap()).unwrap();
        assert_eq!(decoded.statistics.contributing_nodes, 1);
        assert_eq!(
            decoded.statistics.write_position_sum,
            merged.write_position_sum
        );
        assert_eq!(merged.write_position_sum, 27_670_116_110_564_327_421);
        merged.merge(&AccessStatistics {
            writes_per_second: 1.0,
            contributing_nodes: 1,
            average_write_position: Some(i64::MIN),
            median_write_position: Some(i64::MIN),
            write_count: 1,
            write_position_sum: i64::MIN as i128,
            window_seconds: 1.0,
            ..Default::default()
        });
        assert_eq!(
            merged.average_write_position,
            Some(4_611_686_018_427_387_903)
        );
        assert_eq!(merged.write_count, 4);
        assert_eq!(merged.contributing_nodes, 2);
    }
}
