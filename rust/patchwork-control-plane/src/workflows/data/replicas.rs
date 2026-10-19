use crate::actors::partition::{GetPartition, GetPartitionCached};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use kameo_actors::pool::{ActorPool, Dispatch};

use crate::{actors::PartitionActor, datamodels::partition::Model};

// Resolve exact partition boundaries, not a second ownership lookup.
// Reads use the final source; writes include each partition along the way.
pub(super) async fn resolve(
    actor: &ActorRef<ActorPool<PartitionActor>>,
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
            actor.ask(Dispatch(GetPartitionCached { id })).await?
        } else {
            actor.ask(Dispatch(GetPartition { id })).await?
        }
        .context("Forwarded partition no longer exists")?;
    }
    replicas.sort_unstable();
    replicas.dedup();
    Ok(replicas)
}

#[cfg(test)]
mod tests;
