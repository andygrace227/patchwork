use anyhow::Result;
use patchwork_common::client::Client;
use serde_json::Value;
use tokio::task::JoinSet;

/// Write every remote copy and wait for all replies, including failures.
pub(crate) async fn put(
    table_id: i64,
    partition_key: i64,
    secondary_key: i64,
    data: Value,
    replicas: Vec<String>,
    timestamp: i64,
    deleted: bool,
) -> Result<()> {
    let mut replicas: Vec<_> = replicas
        .into_iter()
        .map(|url| url.trim_end_matches('/').to_owned())
        .collect();
    replicas.sort_unstable();
    replicas.dedup();
    let mut writes = JoinSet::new();
    for url in replicas {
        let data = data.clone();
        writes.spawn(async move {
            Client::write_record(
                &url,
                table_id,
                partition_key,
                secondary_key,
                &data,
                timestamp,
                deleted,
            )
            .await
            .map_err(|error| anyhow::anyhow!("Replica {url}: {error}"))
        });
    }
    // Wait for every replica even when another has failed.
    let mut failures = Vec::new();
    while let Some(result) = writes.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(error.to_string()),
            Err(error) => failures.push(error.to_string()),
        }
    }
    anyhow::ensure!(
        failures.is_empty(),
        "Replica writes failed: {}",
        failures.join("; ")
    );
    Ok(())
}
