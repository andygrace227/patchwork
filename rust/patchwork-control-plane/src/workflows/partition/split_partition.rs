use super::copy_range::copy_range;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use crate::{dataplane_client, db::actor::Get};
use crate::{
    db::{
        NodeActor, PartitionActor, TableActor,
        node::GetNodeExcluding,
        parition_lock::{AttemptLock, FreeLock},
        partition::{self, CreateLockedPartition, GetTablePartitions, UpdateLockedPartition},
    },
    workflows::partition::circular_get,
};
use anyhow::{Context, Result};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;

/// Split the existing partition containing `new_partition` at that hash boundary.
pub struct SplitPartitionContext {
    pub table_id: i64,
    pub new_partition: i64,
}

pub struct SplitPartition {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

impl SplitPartition {
    pub async fn run(&self, ctx: SplitPartitionContext) -> Result<()> {
        // Fetch the hash ring for the table.
        let partitions = self
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;

        if partitions.is_empty() {
            return Err(anyhow::anyhow!("Cannot split an empty partition ring"));
        }

        let partition_search =
            partitions.binary_search_by_key(&ctx.new_partition, |p| p.hash_start);

        if partition_search.is_ok() {
            // This is a failure and should be logged as such.
            // Splitting a partition, at an existing partition, does nothing.
            // This should be handled with increasing replication instead, unless resuming a split.
            return Err(anyhow::anyhow!(
                "Partition already exists; check its bootstrap state"
            ));
        }

        // Now we have the index of the partition to be split.
        // Binary search gives the insertion position, so take the partition before it.
        let partition_idx_to_split = partition_search.unwrap_err() + partitions.len() - 1;
        let split_p =
            circular_get(&partitions, partition_idx_to_split).expect("partition ring is nonempty");
        let next_p = circular_get(&partitions, partition_idx_to_split + 1)
            .expect("partition ring is nonempty");
        let end_hash_range = next_p.hash_start;
        let replication_factor = split_p.replication_factor();
        // The current HTTP range API cannot express a range crossing the ring boundary.
        if ctx.new_partition >= end_hash_range {
            return Err(anyhow::anyhow!(
                "Wraparound copying is not supported by the range API yet"
            ));
        }

        // Stage 0: Lock partitions in the database.
        // Lock the previous partition, the next partition, and the new partition key.

        // Lock in a consistent order, including the new key before it is published.
        let mut lock_keys = vec![split_p.hash_start, next_p.hash_start, ctx.new_partition];
        lock_keys.sort_unstable();
        lock_keys.dedup();
        let lease_end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64
            + 300;
        let mut locks = Vec::new();

        // Keep errors inside this block so we always reach the unlocks below.
        let result: Result<()> = async {
            for hash_start in lock_keys {
                let lock = self
                    .partition
                    .ask(AttemptLock {
                        table_id: ctx.table_id,
                        hash_start,
                        lease_end,
                    })
                    .await?;
                let Some(lock) = lock else {
                    return Err(anyhow::anyhow!(
                        "Partition is already locked; retry the split"
                    ));
                };
                locks.push(lock);
            }

            // Another split may have finished while we were acquiring the locks.
            let current = self
                .partition
                .ask(GetTablePartitions {
                    table_id: ctx.table_id,
                })
                .await?;
            let insertion_idx =
                match current.binary_search_by_key(&ctx.new_partition, |p| p.hash_start) {
                    Ok(_) => {
                        return Err(anyhow::anyhow!(
                            "Partition already exists; check its bootstrap state"
                        ));
                    }
                    Err(idx) => idx,
                };
            if current.is_empty() {
                return Err(anyhow::anyhow!("Partition ring changed; retry the split"));
            }
            let current_idx = insertion_idx + current.len() - 1;
            if circular_get(&current, current_idx) != Some(split_p)
                || circular_get(&current, current_idx + 1) != Some(next_p)
            {
                return Err(anyhow::anyhow!("Partition ring changed; retry the split"));
            }

            anyhow::ensure!(
                !current
                    .iter()
                    .any(|p| p.forward_to == Some(split_p.hash_start)),
                "Another partition is still copying from this partition"
            );

            // Stage 1: Identify the new nodes in the database.
            // All nodes are equal replicas.
            if replication_factor == 0 {
                return Err(anyhow::anyhow!("Replication factor must be positive"));
            }

            let mut node_ids = Vec::new();

            if split_p.forward_to.is_some() {
                return Err(anyhow::anyhow!(
                    "Cannot split a partition that is still bootstrapping"
                ));
            }
            let mut unique_replicas = split_p.replicas.0.clone();
            unique_replicas.sort_unstable();
            unique_replicas.dedup();
            if unique_replicas.len() != replication_factor {
                return Err(anyhow::anyhow!("The replica list contains duplicate nodes"));
            }
            let mut replicas = Vec::new();
            for _ in 0..replication_factor {
                let node_id_copy = node_ids.clone();
                let node = self
                    .node
                    .ask(GetNodeExcluding {
                        node_ids: node_id_copy,
                    })
                    .await?;
                let Some(node) = node else {
                    return Err(anyhow::anyhow!("No node available outside the replica set"));
                };

                node_ids.push(node.node_id);
                replicas.push(node.node_id);
            }

            // Stage 2: Push the new partition into the DB. Its key is already locked.
            // Insertion checks that we still hold all the leases.

            // So:
            // Replication_factor is the length of replicas
            let proposed_partition = partition::ActiveModel {
                table_id: Set(ctx.table_id),
                hash_start: Set(ctx.new_partition),
                forward_to: Set(Some(split_p.hash_start)), // Reads use the source partition until copying finishes.
                replicas: Set(partition::ReplicaNodes(replicas.clone())), // Preserve the old partition's number of copies.
            };

            self.partition
                .ask(CreateLockedPartition {
                    data: proposed_partition,
                    locks: locks.clone(),
                })
                .await?;

            // Stage 3: Scan all old replicas and consistently write every version.
            // Timestamp ordering on the destinations also preserves tombstones.
            copy_range(
                &self.node,
                FetchRangeCtx {
                    table_id: ctx.table_id,
                    lower_bound: ctx.new_partition,
                    upper_bound: end_hash_range,
                },
                &split_p.replicas.0,
                &replicas,
                lease_end,
            )
            .await?;

            // Stage 4: Mark the new partition as live.
            self.partition
                .ask(UpdateLockedPartition {
                    data: partition::ActiveModel {
                        table_id: Set(ctx.table_id),
                        hash_start: Set(ctx.new_partition),
                        forward_to: Set(None), // No forwarding: reads and writes use the new nodes.
                        ..Default::default()
                    },
                    locks: locks.clone(),
                })
                .await?;

            // Stage 5: Start removing that chunk from split_p's old replicas.
            // Nodes shared with the new replica list must keep their copies.
            for replica in &split_p.replicas.0 {
                if replicas.contains(replica) {
                    continue;
                }
                if std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs() as i64
                    >= lease_end
                {
                    return Err(anyhow::anyhow!("Split lease expired during cleanup"));
                }
                let node = self
                    .node
                    .ask(Get { id: *replica })
                    .await?
                    .context("Old replica node no longer exists")?;
                dataplane_client::Client::delete_range(
                    &node.url,
                    ctx.table_id,
                    end_hash_range,
                    ctx.new_partition,
                )
                .await?;
            }

            Ok(())
        }
        .await;

        // Stage 6: Unlock everything, including when a previous step failed.
        let mut unlock_error = None;
        for lock in locks.into_iter().rev() {
            if let Err(err) = self
                .partition
                .ask(FreeLock {
                    table_id: lock.table_id,
                    hash_start: lock.hash_start,
                    lock_token: lock.lock_token,
                })
                .await
            {
                unlock_error = Some(err);
            }
        }
        if let Some(err) = unlock_error {
            return Err(anyhow::anyhow!(
                "Split result: {:?}; failed to release a lease: {}",
                result,
                err
            ));
        }

        result
    }
}
