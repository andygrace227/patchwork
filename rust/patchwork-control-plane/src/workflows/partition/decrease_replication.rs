use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;

use crate::{
    client::Client,
    db::{
        NodeActor, PartitionActor,
        actor::Get,
        parition_lock::{AttemptLock, FreeLock},
        partition::{self, GetTablePartitions, UpdateLockedPartition},
    },
};

pub struct DecreaseReplicationContext {
    pub table_id: i64,
    pub hash_start: i64,
    /// Desired total number of copies, including the root.
    pub replication_factor: usize,
}

pub struct DecreaseReplication {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
}

fn boundaries(
    partitions: &[partition::Model],
    hash_start: i64,
) -> Result<(&partition::Model, &partition::Model)> {
    let idx = partitions
        .binary_search_by_key(&hash_start, |p| p.hash_start)
        .map_err(|_| anyhow::anyhow!("Partition does not exist"))?;
    Ok((&partitions[idx], &partitions[(idx + 1) % partitions.len()]))
}

fn now() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}

fn check_lease(lease_end: i64) -> Result<()> {
    ensure!(now()? < lease_end, "Replication lease expired");
    Ok(())
}

impl DecreaseReplication {
    pub async fn run(&self, ctx: DecreaseReplicationContext) -> Result<()> {
        // Fetch the hash ring. The next boundary marks the end of this partition.
        let partitions = self
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        let (source, next) = boundaries(&partitions, ctx.hash_start)?;
        ensure!(
            source.forward_to == 0,
            "Cannot change replication while bootstrapping"
        );
        ensure!(
            source.replicas.0.contains(&source.node_id),
            "Replica list must include the root node"
        );
        let mut unique = source.replicas.0.clone();
        unique.sort_unstable();
        unique.dedup();
        ensure!(
            unique.len() == source.replication_factor(),
            "Replica list contains duplicate nodes"
        );
        ensure!(ctx.replication_factor > 0, "Must retain at least one copy");
        ensure!(
            ctx.replication_factor <= source.replication_factor(),
            "Requested replication factor must decrease"
        );
        if ctx.replication_factor == source.replication_factor() {
            return Ok(());
        }
        // Same range restriction as split and merge until ring ranges are supported.
        ensure!(
            source.hash_start < next.hash_start,
            "Wraparound copying is not supported by the range API yet"
        );

        // Stage 0: Lock this partition and its next boundary in a consistent order.
        // This keeps a split or merge from changing the range while we work.
        let mut keys = vec![source.hash_start, next.hash_start];
        keys.sort_unstable();
        keys.dedup();
        let lease_end = now()? + 300;
        let mut locks = Vec::new();
        // Keep errors inside this block so every acquired lock reaches the unlocks.
        let result: Result<()> = async {
            for hash_start in keys {
                locks.push(
                    self.partition
                        .ask(AttemptLock {
                            table_id: ctx.table_id,
                            hash_start,
                            lease_end,
                        })
                        .await?
                        .context("Partition is already locked; retry the replication change")?,
                );
            }
            // Another operation may have finished while we were taking the locks.
            let current = self
                .partition
                .ask(GetTablePartitions {
                    table_id: ctx.table_id,
                })
                .await?;
            ensure!(
                boundaries(&current, ctx.hash_start)? == (source, next),
                "Partition ring changed; retry the replication change"
            );

            // Stage 1: Keep the root and enough existing replicas to reach the target.
            // Resolve the removed nodes before changing the replica list.
            let replicas = retained_replicas(source, ctx.replication_factor);
            let mut obsolete = Vec::new();
            for id in &source.replicas.0 {
                if !replicas.contains(id) {
                    obsolete.push(
                        self.node
                            .ask(Get { id: *id })
                            .await?
                            .context("Replica node no longer exists")?,
                    );
                }
            }

            // Stage 2: Remove those nodes from the replica list first.
            // Keep the root unchanged. Updating checks that we still hold the leases.

            self.partition
                .ask(UpdateLockedPartition {
                    data: partition::ActiveModel {
                        table_id: Set(ctx.table_id),
                        hash_start: Set(ctx.hash_start),
                        replicas: Set(partition::ReplicaNodes(replicas)),
                        ..Default::default()
                    },
                    locks: locks.clone(),
                })
                .await?;

            // Stage 3: Clean up this partition's records on the removed replicas.
            // Leave their other partitions alone. Kept replicas never enter this list.
            for node in obsolete {
                check_lease(lease_end)
                    .context("Replication decreased, but old replica cleanup is incomplete")?;
                Client::delete_range(&node.url, ctx.table_id, next.hash_start, source.hash_start)
                    .await
                    .context("Replication decreased, but old replica cleanup failed")?;
            }

            Ok(())
        }
        .await;

        // Stage 4: Unlock everything, even when copying, updating or cleanup failed.
        // Release only our own locks, in reverse order, and attempt every unlock.
        let mut unlock_error = None;
        for lock in locks.into_iter().rev() {
            if let Err(error) = self
                .partition
                .ask(FreeLock {
                    table_id: lock.table_id,
                    hash_start: lock.hash_start,
                    lock_token: lock.lock_token,
                })
                .await
            {
                unlock_error = Some(error);
            }
        }
        if let Some(error) = unlock_error {
            anyhow::bail!("Replication result: {result:?}; failed to release a lease: {error}");
        }
        result
    }
}

// Preserve replica order, always retaining the root even when it is not first.
fn retained_replicas(source: &partition::Model, count: usize) -> Vec<i64> {
    let mut remaining = count - 1;
    source
        .replicas
        .0
        .iter()
        .copied()
        .filter(|id| {
            if *id == source.node_id {
                return true;
            }
            if remaining == 0 {
                return false;
            }
            remaining -= 1;
            true
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrease_keeps_root_even_when_it_is_last() {
        let source = partition::Model {
            table_id: 1,
            hash_start: 10,
            node_id: 3,
            forward_to: 0,
            replicas: partition::ReplicaNodes(vec![1, 2, 3]),
        };
        assert_eq!(retained_replicas(&source, 1), vec![3]);
        assert_eq!(retained_replicas(&source, 2), vec![1, 3]);
        assert_eq!(retained_replicas(&source, 3), vec![1, 2, 3]);
        assert!(boundaries(&[], 10).is_err());
    }
}
