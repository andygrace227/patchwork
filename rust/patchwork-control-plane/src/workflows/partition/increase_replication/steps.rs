use super::*;

impl Run {
    pub(super) async fn prepare(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        // Fetch the hash ring. The next boundary marks the end of this partition.
        let partitions = workflow
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        let (source, next) = boundaries(&partitions, ctx.hash_start)?;
        ensure!(
            source.forward_to.is_none(),
            "Cannot change replication while bootstrapping"
        );
        ensure!(!source.replicas.0.is_empty(), "Partition has no replicas");
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
            return Ok(State::Done);
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

        self.boundaries = Some((source.clone(), next.clone()));
        self.keys = keys;
        self.lease_end = lease_end;
        Ok(State::Lock)
    }
    pub(super) async fn lock(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let lease_end = self.lease_end;
        let locks = &mut self.locks;
        let (source, next) = self.boundaries.as_ref().expect("Prepare must run first");
        for hash_start in self.keys.clone() {
            locks.push(
                workflow
                    .partition
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
        let current = workflow
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        ensure!(
            boundaries(&current, ctx.hash_start)? == (source, next),
            "Partition ring changed; retry the replication change"
        );

        ensure!(
            !current
                .iter()
                .any(|p| p.forward_to == Some(source.hash_start)),
            "Another partition is still copying from this partition"
        );
        Ok(State::SelectNodes)
    }
    pub(super) async fn select_nodes(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let (source, _) = self.boundaries.as_ref().expect("Prepare must run first");
        // Stage 1: Select additional nodes outside the current replica list.
        // Resolve all destinations before starting the copy.
        let replicas = &mut self.replicas;
        *replicas = source.replicas.0.clone();
        let targets = &mut self.targets;
        while replicas.len() < ctx.replication_factor {
            let node = workflow
                .node
                .ask(GetNodeExcluding {
                    node_ids: replicas.clone(),
                })
                .await?
                .context("Not enough nodes to reach the requested replication factor")?;
            replicas.push(node.node_id);
            targets.push(node.node_id);
        }
        Ok(State::Copy)
    }
    pub(super) async fn copy(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let lease_end = self.lease_end;
        let (source, next) = self.boundaries.as_ref().expect("Prepare must run first");
        // Stage 2: Copy all source versions before publishing the new replicas.
        copy_range(
            &workflow.node,
            FetchRangeCtx {
                table_id: ctx.table_id,
                lower_bound: source.hash_start,
                upper_bound: next.hash_start,
            },
            &source.replicas.0,
            &self.targets,
            lease_end,
        )
        .await?;
        Ok(State::Metadata)
    }
    pub(super) async fn metadata(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let locks = &mut self.locks;
        // Stage 3: Publish the expanded replica list.
        // Updating checks that we still hold both boundary leases.

        workflow
            .partition
            .ask(UpdateLockedPartition {
                data: partition::ActiveModel {
                    table_id: Set(ctx.table_id),
                    hash_start: Set(ctx.hash_start),
                    replicas: Set(partition::ReplicaNodes(self.replicas.clone())),
                    ..Default::default()
                },
                locks: locks.clone(),
            })
            .await?;
        Ok(State::Unlock)
    }
    pub(super) async fn unlock(&mut self) -> Result<State> {
        // Stage 4: Unlock everything, even when copying, updating or cleanup failed.
        // Release only our own locks, in reverse order, and attempt every unlock.
        unlock(&self.workflow.partition, &mut self.locks).await?;
        Ok(State::Done)
    }
}
