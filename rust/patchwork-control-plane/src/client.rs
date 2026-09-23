//! Static shard calls. Pass the node's base URL, e.g. http://127.0.0.1:3000.
//! JSON responses are returned unchanged; HTTP errors remain errors.
use reqwest::{Client as HttpClient, Method};
use serde_json::Value;
use std::{sync::LazyLock, time::Duration};

static HTTP: LazyLock<HttpClient> = LazyLock::new(HttpClient::new);

pub struct Client;

impl Client {
    /// Returns {"size_bytes": N} for the main SQLite file, excluding WAL and SHM.
    pub async fn get_size(url: &str) -> Result<Value, reqwest::Error> {
        json(
            Method::GET,
            format!("{}/shard/size", url.trim_end_matches('/')),
        )
        .await
    }

    /// Sends the JSON payload directly. A successful upsert has no response body.
    pub async fn upsert(
        url: &str,
        table_id: i64,
        partition: i64,
        hash: i64,
        data: &Value,
    ) -> Result<(), reqwest::Error> {
        HTTP.put(record_url(url, table_id, partition, hash))
            .timeout(Duration::from_secs(30))
            .json(data)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Returns the full record, including its server-managed version.
    /// A missing record returns an HTTP 404 error.
    pub async fn get(
        url: &str,
        table_id: i64,
        partition: i64,
        hash: i64,
    ) -> Result<Value, reqwest::Error> {
        json(Method::GET, record_url(url, table_id, partition, hash)).await
    }

    pub async fn delete(
        url: &str,
        table_id: i64,
        partition: i64,
        hash: i64,
    ) -> Result<Value, reqwest::Error> {
        json(Method::DELETE, record_url(url, table_id, partition, hash)).await
    }

    pub async fn get_partition(
        url: &str,
        table_id: i64,
        partition: i64,
    ) -> Result<Value, reqwest::Error> {
        json(Method::GET, partition_url(url, table_id, partition)).await
    }

    pub async fn delete_partition(
        url: &str,
        table_id: i64,
        partition: i64,
    ) -> Result<Value, reqwest::Error> {
        json(Method::DELETE, partition_url(url, table_id, partition)).await
    }

    /// Bounds are exclusive; argument order matches ShardActor::get_range.
    pub async fn get_range(
        url: &str,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Value, reqwest::Error> {
        json(
            Method::GET,
            format!(
                "{}/tables/{table_id}/range?lower_bound={lower_bound}&upper_bound={upper_bound}",
                url.trim_end_matches('/')
            ),
        )
        .await
    }
}

fn partition_url(url: &str, table_id: i64, partition: i64) -> String {
    format!(
        "{}/tables/{table_id}/partitions/{partition}",
        url.trim_end_matches('/')
    )
}

fn record_url(url: &str, table_id: i64, partition: i64, hash: i64) -> String {
    format!("{}/records/{hash}", partition_url(url, table_id, partition))
}

async fn json(method: Method, url: String) -> Result<Value, reqwest::Error> {
    HTTP.request(method, url)
        .timeout(Duration::from_secs(30))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}
