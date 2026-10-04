use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;

use crate::{
    client::Client,
    db::{
        NodeActor, PartitionActor,
        actor::Get,
        node::GetNodeExcluding,
        parition_lock::{AttemptLock, FreeLock},
        partition::{self, GetTablePartitions, UpdateLockedPartition},
    },
};

pub struct IncreaseReplicationContext {
    pub table_id: i64,
    pub hash_start: i64,
    /// Desired total number of copies, including the root.
    pub replication_factor: usize,
}

pub struct IncreaseReplication {
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

impl IncreaseReplication {
    pub async fn run(&self, ctx: IncreaseReplicationContext) -> Result<()> {
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
            ctx.replication_factor >= source.replication_factor(),
            "Requested replication factor must increase"
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

            // Stage 1: Select additional nodes outside the current replica list.
            // Resolve all destinations before starting the copy.
            let source_node = self
                .node
                .ask(Get { id: source.node_id })
                .await?
                .context("Source node no longer exists")?;
            let mut replicas = source.replicas.0.clone();
            let mut targets = Vec::new();
            while replicas.len() < ctx.replication_factor {
                let node = self
                    .node
                    .ask(GetNodeExcluding {
                        node_ids: replicas.clone(),
                    })
                    .await?
                    .context("Not enough nodes to reach the requested replication factor")?;
                replicas.push(node.node_id);
                targets.push(node);
            }

            // Stage 2: Copy the partition range from the root to each new replica.
            // Existing replicas stay in place. New ones are published only after copying.
            let records = Client::get_range(
                &source_node.url,
                ctx.table_id,
                next.hash_start,
                source.hash_start,
            )
            .await?;
            for node in targets {
                for record in &records {
                    check_lease(lease_end)?;
                    Client::upsert(
                        &node.url,
                        record.table_id,
                        record.partition_key,
                        record.secondary_key,
                        &record.data,
                    )
                    .await?;
                }
            }

            // Stage 3: Publish the expanded replica list.
            // Updating checks that we still hold both boundary leases.

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
