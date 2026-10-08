use super::copy_range::copy_range;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;

use crate::{
    client::Client,
    db::{
        NodeActor, PartitionActor,
        actor::Get,
        parition_lock::{AttemptLock, FreeLock},
        partition::{DeleteLockedPartition, GetTablePartitions, Model},
    },
};

/// Remove this boundary, extending the previous partition through its range.
pub struct MergePartitionContext {
    pub table_id: i64,
    pub hash_start: i64,
}

pub struct MergePartition {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
}

fn neighbors(partitions: &[Model], hash_start: i64) -> Result<(&Model, &Model, &Model)> {
    ensure!(partitions.len() > 1, "Cannot merge the last partition");
    let idx = partitions
        .binary_search_by_key(&hash_start, |p| p.hash_start)
        .map_err(|_| anyhow::anyhow!("Partition does not exist"))?;
    Ok((
        &partitions[(idx + partitions.len() - 1) % partitions.len()],
        &partitions[idx],
        &partitions[(idx + 1) % partitions.len()],
    ))
}

fn check_lease(lease_end: i64) -> Result<()> {
    ensure!(now()? < lease_end, "Merge lease expired");
    Ok(())
}

fn now() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}

impl MergePartition {
    pub async fn run(&self, ctx: MergePartitionContext) -> Result<()> {
        // Fetch the hash ring for the table.
        // The source partition is going away. The previous partition takes its range.
        let partitions = self
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        let (previous, source, next) = neighbors(&partitions, ctx.hash_start)?;
        ensure!(
            source.forward_to.is_none() && previous.forward_to.is_none(),
            "Cannot merge a bootstrapping partition"
        );
        for partition in [source, previous] {
            ensure!(
                !partition.replicas.0.is_empty(),
                "Partition has no replicas"
            );
            let mut unique = partition.replicas.0.clone();
            unique.sort_unstable();
            unique.dedup();
            ensure!(
                unique.len() == partition.replicas.0.len(),
                "Replica list contains duplicate nodes"
            );
        }
        // Same range restriction as split_partition until ring ranges are supported.
        ensure!(
            source.hash_start < next.hash_start,
            "Wraparound copying is not supported by the range API yet"
        );
        // Stage 0: Lock partitions in the database.
        // Lock the previous partition, the source, and the next boundary.
        // Take locks in a consistent order. With two partitions, previous == next,
        // so only lock that key once.
        let mut keys = vec![previous.hash_start, source.hash_start, next.hash_start];
        keys.sort_unstable();
        keys.dedup();
        let lease_end = now()? + 300;
        let mut locks = Vec::new();
        // Keep errors inside this block so we reach the unlocks even if a step fails.
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
                        .context("Partition is already locked; retry the merge")?,
                );
            }
            // Another split or merge may have finished while we were taking locks.
            // Check that these are still the same partitions before moving any data.
            let current = self
                .partition
                .ask(GetTablePartitions {
                    table_id: ctx.table_id,
                })
                .await?;
            ensure!(
                neighbors(&current, ctx.hash_start)? == (previous, source, next),
                "Partition ring changed; retry the merge"
            );

            ensure!(
                !current
                    .iter()
                    .any(|p| p.forward_to == Some(source.hash_start)
                        || p.forward_to == Some(previous.hash_start)),
                "Another partition is still copying from a merge partition"
            );

            // Stage 1: Resolve obsolete replicas before copying or changing the ring.
            let mut obsolete = Vec::new();
            for id in &source.replicas.0 {
                if !previous.replicas.0.contains(id) {
                    obsolete.push(
                        self.node
                            .ask(Get { id: *id })
                            .await?
                            .context("Source replica no longer exists")?,
                    );
                }
            }

            // Stage 2: Copy every source replica into the previous partition's replicas.
            // Keep the source boundary until every destination acknowledges the copy.
            copy_range(
                &self.node,
                FetchRangeCtx {
                    table_id: ctx.table_id,
                    lower_bound: source.hash_start,
                    upper_bound: next.hash_start,
                },
                &source.replicas.0,
                &previous.replicas.0,
                lease_end,
            )
            .await?;

            // Stage 3: Remove the source partition from the database.
            // The previous partition now owns the combined range; its row stays as-is.
            // Deletion checks that we still hold all the leases.
            self.partition
                .ask(DeleteLockedPartition {
                    table_id: ctx.table_id,
                    hash_start: ctx.hash_start,
                    locks: locks.clone(),
                })
                .await?;

            // Stage 4: Clean up the source range on its old replicas.
            // Only remove the records we moved, not the node's other partitions.
            // Shared replicas are excluded from obsolete: they must keep their copies.
            for node in obsolete {
                check_lease(lease_end)?;
                Client::delete_range(&node.url, ctx.table_id, next.hash_start, source.hash_start)
                    .await
                    .context("Merge completed, but old replica cleanup failed")?;
            }
            Ok(())
        }
        .await;

        // Stage 5: Unlock everything, including when copying or cleanup failed.
        // Release only our own locks, in reverse order, and try every unlock.
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
            anyhow::bail!("Merge result: {result:?}; failed to release a lease: {error}");
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::partition::ReplicaNodes;

    #[test]
    fn merge_selects_previous_and_next_and_keeps_last_partition() {
        let ring: Vec<_> = [10, 20, 30]
            .into_iter()
            .map(|hash_start| Model {
                table_id: 1,
                hash_start,

                forward_to: None,
                replicas: ReplicaNodes(vec![1]),
            })
            .collect();
        let (previous, source, next) = neighbors(&ring, 10).unwrap();
        assert_eq!(
            (previous.hash_start, source.hash_start, next.hash_start),
            (30, 10, 20)
        );
        assert!(neighbors(&ring[..1], 10).is_err());
        assert!(neighbors(&[], 10).is_err());
        assert!(neighbors(&ring, 15).is_err());
    }
}
