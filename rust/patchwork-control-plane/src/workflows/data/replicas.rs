use crate::clients::PartitionClient;
use anyhow::{Context, Result, ensure};

use crate::datamodels::partition::Model;

// Resolve exact partition boundaries, not a second ownership lookup.
// Reads use the final source; writes include each partition along the way.
pub(super) async fn resolve(
    actor: &PartitionClient,
    partition: Model,
    writes: bool,
) -> Result<Vec<i64>> {
    resolve_inner(actor, partition, writes, false).await
}

pub(super) async fn resolve_cached(
    actor: &PartitionClient,
    partition: Model,
    writes: bool,
) -> Result<Vec<i64>> {
    resolve_inner(actor, partition, writes, true).await
}

async fn resolve_inner(
    actor: &PartitionClient,
    mut partition: Model,
    writes: bool,
    cached: bool,
) -> Result<Vec<i64>> {
    let mut visited = Vec::new();
    let mut replicas = Vec::new();
    loop {
        ensure!(
            !visited.contains(&partition.hash_start),
            "Partition forwarding loop"
        );
        visited.push(partition.hash_start);
        ensure!(
            !partition.replicas.0.is_empty(),
            "Partition has no replicas"
        );
        if writes || partition.forward_to.is_none() {
            replicas.extend(partition.replicas.0);
        }
        let Some(hash_start) = partition.forward_to else {
            break;
        };
        let id = (partition.table_id, hash_start);
        partition = if cached {
            actor.get_partition_cached(id).await?
        } else {
            actor.get_partition(id).await?
        }
        .context("Forwarded partition no longer exists")?;
    }
    replicas.sort_unstable();
    replicas.dedup();
    Ok(replicas)
}

#[cfg(test)]
mod tests;
