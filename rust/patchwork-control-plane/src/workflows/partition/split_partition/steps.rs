use super::*;

impl Run {
    pub(super) async fn prepare(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        // Fetch the hash ring for the table.
        let partitions = workflow
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

        self.boundaries = Some((split_p.clone(), next_p.clone()));
        self.keys = lock_keys;
        self.lease_end = lease_end;
        Ok(State::Lock)
    }
    pub(super) async fn lock(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let locks = &mut self.locks;
        let lease_end = self.lease_end;
        let (split_p, next_p) = self.boundaries.as_ref().expect("Prepare must run first");
        for hash_start in self.keys.clone() {
            let lock = workflow
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
        let current = workflow
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        let insertion_idx = match current.binary_search_by_key(&ctx.new_partition, |p| p.hash_start)
        {
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
        Ok(State::SelectNodes)
    }
    pub(super) async fn select_nodes(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let (split_p, _) = self.boundaries.as_ref().expect("Prepare must run first");
        let replication_factor = split_p.replication_factor();
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
        let replicas = &mut self.replicas;
        for _ in 0..replication_factor {
            let node_id_copy = node_ids.clone();
            let node = workflow
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
        Ok(State::Publish)
    }
    pub(super) async fn publish(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let locks = &mut self.locks;
        let (split_p, _) = self.boundaries.as_ref().expect("Prepare must run first");
        // Stage 2: Push the new partition into the DB. Its key is already locked.
        // Insertion checks that we still hold all the leases.

        // So:
        // Replication_factor is the length of replicas
        let proposed_partition = partition::ActiveModel {
            table_id: Set(ctx.table_id),
            hash_start: Set(ctx.new_partition),
            forward_to: Set(Some(split_p.hash_start)), // Reads use the source partition until copying finishes.
            replicas: Set(partition::ReplicaNodes(self.replicas.clone())), // Preserve the old partition's number of copies.
        };

        workflow
            .partition
            .ask(CreateLockedPartition {
                data: proposed_partition,
                locks: locks.clone(),
            })
            .await?;
        Ok(State::Copy)
    }
    pub(super) async fn copy(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let lease_end = self.lease_end;
        let (split_p, next_p) = self.boundaries.as_ref().expect("Prepare must run first");
        let end_hash_range = next_p.hash_start;
        // Stage 3: Scan all old replicas and consistently write every version.
        // Timestamp ordering on the destinations also preserves tombstones.
        copy_range(
            &workflow.node,
            FetchRangeCtx {
                table_id: ctx.table_id,
                lower_bound: ctx.new_partition,
                upper_bound: end_hash_range,
            },
            &split_p.replicas.0,
            &self.replicas,
            lease_end,
        )
        .await?;
        Ok(State::Activate)
    }
    pub(super) async fn activate(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let locks = &mut self.locks;
        // Stage 4: Mark the new partition as live.
        workflow
            .partition
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
        Ok(State::Cleanup)
    }
    pub(super) async fn cleanup(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let lease_end = self.lease_end;
        let (split_p, next_p) = self.boundaries.as_ref().expect("Prepare must run first");
        let end_hash_range = next_p.hash_start;
        // Stage 5: Start removing that chunk from split_p's old replicas.
        // Nodes shared with the new replica list must keep their copies.
        for replica in &split_p.replicas.0 {
            if self.replicas.contains(replica) {
                continue;
            }
            if std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs() as i64
                >= lease_end
            {
                return Err(anyhow::anyhow!("Split lease expired during cleanup"));
            }
            let node = workflow
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
        Ok(State::Unlock)
    }
    pub(super) async fn unlock(&mut self) -> Result<State> {
        // Stage 6: Unlock everything, including when a previous step failed.
        unlock(&self.workflow.partition, &mut self.locks).await?;
        Ok(State::Done)
    }
}
