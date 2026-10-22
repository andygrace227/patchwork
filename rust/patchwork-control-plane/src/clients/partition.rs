use anyhow::Result;
use kameo::actor::ActorRef;
use kameo_actors::pool::{ActorPool, Dispatch};
use patchwork_common::cache::FastCache;

use crate::{
    actors::{
        PartitionActor,
        parition_lock::{AttemptLock, FreeLock},
        partition::{
            CreateLockedPartition, CreatePartition, DeleteLockedPartition, DeletePartition,
            GetPartition, GetPartitionAtOrBefore, GetTablePartitions, UpdateLockedPartition,
            UpdatePartition,
        },
    },
    datamodels::{
        parition_lock,
        partition::{ActiveModel, Model},
    },
};

#[derive(Clone)]
pub struct PartitionClient {
    actor: ActorRef<ActorPool<PartitionActor>>,
    rows: FastCache<(i64, i64), Model>,
    rings: FastCache<i64, Vec<Model>>,
}

impl PartitionClient {
    pub fn new(actor: ActorRef<ActorPool<PartitionActor>>) -> Self {
        Self {
            actor,
            rows: FastCache::new(5),
            rings: FastCache::new(5),
        }
    }

    pub async fn get_partition(&self, id: (i64, i64)) -> Result<Option<Model>> {
        Ok(self.actor.ask(Dispatch(GetPartition { id })).await?)
    }

    pub async fn get_partition_cached(&self, id: (i64, i64)) -> Result<Option<Model>> {
        if let Some(value) = self.rows.read(&id) {
            return Ok(value);
        }
        let value = self.get_partition(id).await?;
        self.rows.write(id, value.clone());
        Ok(value)
    }

    pub async fn get_table_partitions(&self, table_id: i64) -> Result<Vec<Model>> {
        Ok(self
            .actor
            .ask(Dispatch(GetTablePartitions { table_id }))
            .await?)
    }

    pub async fn get_table_partitions_cached(&self, table_id: i64) -> Result<Vec<Model>> {
        if let Some(Some(value)) = self.rings.read(&table_id) {
            return Ok(value);
        }
        let value = self.get_table_partitions(table_id).await?;
        self.rings.write(table_id, Some(value.clone()));
        Ok(value)
    }

    pub async fn get_partition_at_or_before(
        &self,
        table_id: i64,
        hash_start: i64,
    ) -> Result<Option<Model>> {
        Ok(self
            .actor
            .ask(Dispatch(GetPartitionAtOrBefore {
                table_id,
                hash_start,
            }))
            .await?)
    }

    pub async fn get_partition_at_or_before_cached(
        &self,
        table_id: i64,
        hash_start: i64,
    ) -> Result<Option<Model>> {
        let ring = self.get_table_partitions_cached(table_id).await?;
        let next = ring.partition_point(|partition| partition.hash_start <= hash_start);
        Ok(next.checked_sub(1).map(|index| ring[index].clone()))
    }

    pub async fn create_partition(&self, data: ActiveModel) -> Result<Model> {
        let value = self.actor.ask(Dispatch(CreatePartition { data })).await?;
        self.rows
            .write((value.table_id, value.hash_start), Some(value.clone()));
        self.rings.write(value.table_id, None);
        Ok(value)
    }

    pub async fn update_partition(&self, data: Model) -> Result<Model> {
        let value = self.actor.ask(Dispatch(UpdatePartition { data })).await?;
        self.rows
            .write((value.table_id, value.hash_start), Some(value.clone()));
        self.rings.write(value.table_id, None);
        Ok(value)
    }

    pub async fn delete_partition(&self, id: (i64, i64)) -> Result<u64> {
        let deleted = self.actor.ask(Dispatch(DeletePartition { id })).await?;
        self.rows.write(id, None);
        self.rings.write(id.0, None);
        Ok(deleted)
    }

    pub async fn create_locked_partition(
        &self,
        data: ActiveModel,
        locks: Vec<parition_lock::Model>,
    ) -> Result<Model> {
        let value = self
            .actor
            .ask(Dispatch(CreateLockedPartition { data, locks }))
            .await?;
        self.rows
            .write((value.table_id, value.hash_start), Some(value.clone()));
        self.rings.write(value.table_id, None);
        Ok(value)
    }

    pub async fn update_locked_partition(
        &self,
        data: ActiveModel,
        locks: Vec<parition_lock::Model>,
    ) -> Result<Model> {
        let value = self
            .actor
            .ask(Dispatch(UpdateLockedPartition { data, locks }))
            .await?;
        self.rows
            .write((value.table_id, value.hash_start), Some(value.clone()));
        self.rings.write(value.table_id, None);
        Ok(value)
    }

    pub async fn delete_locked_partition(
        &self,
        table_id: i64,
        hash_start: i64,
        locks: Vec<parition_lock::Model>,
    ) -> Result<u64> {
        let deleted = self
            .actor
            .ask(Dispatch(DeleteLockedPartition {
                table_id,
                hash_start,
                locks,
            }))
            .await?;
        self.rows.write((table_id, hash_start), None);
        self.rings.write(table_id, None);
        Ok(deleted)
    }

    pub async fn attempt_lock(
        &self,
        table_id: i64,
        hash_start: i64,
        lease_end: i64,
    ) -> Result<Option<parition_lock::Model>> {
        Ok(self
            .actor
            .ask(Dispatch(AttemptLock {
                table_id,
                hash_start,
                lease_end,
            }))
            .await?)
    }

    pub async fn free_lock(
        &self,
        table_id: i64,
        hash_start: i64,
        lock_token: String,
    ) -> Result<bool> {
        Ok(self
            .actor
            .ask(Dispatch(FreeLock {
                table_id,
                hash_start,
                lock_token,
            }))
            .await?)
    }
}
