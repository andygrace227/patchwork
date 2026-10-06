//! Static shard calls. Pass the node's base URL, e.g. http://127.0.0.1:3000.
//! Record responses are typed; HTTP errors remain errors.
//! Write timestamps are caller-supplied Unix microseconds; preserve them on retries.
use reqwest::{Client as HttpClient, Method};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{sync::LazyLock, time::Duration};

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
