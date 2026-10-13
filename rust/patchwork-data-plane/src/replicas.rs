use crate::{
    shard_actor::{ShardActor, WriteRecord},
    subordinate_shard_writer::{Put, SubordinateShardWriter},
};
use anyhow::Result;
use kameo::actor::ActorRef;
use patchwork_common::dataplane_client::Client;
use serde_json::Value;
use tokio::task::JoinSet;

/// Shared acknowledgement rules for HTTP and gRPC ingress.
pub(crate) async fn write(
    shard: &ActorRef<ShardActor>,
    writer: &ActorRef<SubordinateShardWriter>,
    request: Put,
    inconsistent: bool,
) -> Result<()> {
    anyhow::ensure!(
        !inconsistent || !request.replicas.is_empty(),
        "at least one remote replica is required"
    );
    shard
        .ask(WriteRecord {
            table_id: request.table_id,
            partition_key: request.partition_key,
            secondary_key: request.secondary_key,
            data: request.data.clone(),
            timestamp: request.timestamp,
            deleted: request.deleted,
        })
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    if !inconsistent {
        return put(
            request.table_id,
            request.partition_key,
            request.secondary_key,
            request.data,
            request.replicas,
            request.timestamp,
            request.deleted,
        )
        .await;
    }
    let first = &request.replicas[0];
    Client::write_record(
        first,
        request.table_id,
        request.partition_key,
        request.secondary_key,
        &request.data,
        request.timestamp,
        request.deleted,
    )
    .await?;
    let replicas = request
        .replicas
        .iter()
        .skip(1)
        .filter(|url| url.trim_end_matches('/') != first.trim_end_matches('/'))
        .cloned()
        .collect::<Vec<_>>();
    if !replicas.is_empty() {
        writer
            .tell(Put {
                replicas,
                ..request
            })
            .await?;
    }
    Ok(())
}

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
