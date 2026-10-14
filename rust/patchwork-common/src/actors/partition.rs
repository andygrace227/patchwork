use super::PartitionActor;
use crate::datamodels::partition::{ActiveModel, Column, Entity, Model};
use kameo::{
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::entity::prelude::*;
use sea_orm::{QueryFilter, QueryOrder, QuerySelect};

/// Returns the table's partitions ordered by hash start.
pub struct GetTablePartitions {
    pub table_id: i64,
}

impl Message<GetTablePartitions> for PartitionActor {
    type Reply = DelegatedReply<Result<Vec<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetTablePartitions,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let generation = {
            let cache = self.cache.lock().unwrap();
            if let Some(partitions) = cache.partitions.get(&msg.table_id) {
                return ctx.reply(Ok(partitions.clone()));
            }
            cache.generation
        };
        let db = self.db.clone();
        let cache = self.cache.clone();
        let ttl = self.cache_ttl;
        ctx.spawn(async move {
            let partitions = Entity::find()
                .filter(Column::TableId.eq(msg.table_id))
                .order_by_asc(Column::HashStart)
                .all(&db)
                .await?;
            let mut cache = cache.lock().unwrap();
            if cache.generation == generation {
                cache
                    .partitions
                    .insert(msg.table_id, partitions.clone(), ttl);
            }
            Ok(partitions)
        })
    }
}

/// Fresh metadata for topology changes and table deletion.
pub struct GetTablePartitionsUncached {
    pub table_id: i64,
}

impl Message<GetTablePartitionsUncached> for PartitionActor {
    type Reply = DelegatedReply<Result<Vec<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetTablePartitionsUncached,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move {
            Entity::find()
                .filter(Column::TableId.eq(msg.table_id))
                .order_by_asc(Column::HashStart)
                .all(&db)
                .await
        })
    }
}

/// Returns the table's closest partition at or before the supplied hash, or None.
pub struct GetPartitionAtOrBefore {
    pub table_id: i64,
    pub hash_start: i64,
}

impl Message<GetPartitionAtOrBefore> for PartitionActor {
    type Reply = DelegatedReply<Result<Option<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetPartitionAtOrBefore,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move {
            Entity::find()
                .filter(Column::TableId.eq(msg.table_id))
                .filter(Column::HashStart.lte(msg.hash_start))
                .order_by_desc(Column::HashStart)
                .limit(1)
                .one(&db)
                .await
        })
    }
}

/// Publish a bootstrapping partition only while all supplied leases are still ours.
pub struct CreateLockedPartition {
    pub data: ActiveModel,
    pub locks: Vec<crate::datamodels::parition_lock::Model>,
}

impl Message<CreateLockedPartition> for PartitionActor {
    type Reply = DelegatedReply<Result<Model, DbErr>>;

    async fn handle(
        &mut self,
        msg: CreateLockedPartition,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        use sea_orm::{DbBackend, Statement, TransactionTrait};
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            if msg.locks.is_empty() {
                return Err(DbErr::Custom("Partition leases are required".into()));
            }
            let txn = db.begin().await?;
            // Hold the lease rows until commit, preventing replacement during insertion.
            for lock in &msg.locks {
                let held = txn.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::MySql,
                    "SELECT lock_token FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lock_token = ? AND lease_end > UNIX_TIMESTAMP() FOR UPDATE",
                    [lock.table_id.into(), lock.hash_start.into(), lock.lock_token.clone().into()],
                )).await?;
                if held.is_none() {
                    txn.rollback().await?;
                    return Err(DbErr::Custom("Partition lease expired or changed; retry the split".into()));
                }
            }
            let partition = msg.data.insert(&txn).await?;
            txn.commit().await?;
            cache.lock().unwrap().clear();
            Ok(partition)
        })
    }
}

/// Update a partition only while all supplied leases are still ours.
pub struct UpdateLockedPartition {
    pub data: ActiveModel,
    pub locks: Vec<crate::datamodels::parition_lock::Model>,
}

impl Message<UpdateLockedPartition> for PartitionActor {
    type Reply = DelegatedReply<Result<Model, DbErr>>;

    async fn handle(
        &mut self,
        msg: UpdateLockedPartition,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        use sea_orm::{DbBackend, Statement, TransactionTrait};
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            if msg.locks.is_empty() {
                return Err(DbErr::Custom("Partition leases are required".into()));
            }
            let txn = db.begin().await?;
            // Hold the lease rows until commit, preventing replacement during the update.
            for lock in &msg.locks {
                let held = txn.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::MySql,
                    "SELECT lock_token FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lock_token = ? AND lease_end > UNIX_TIMESTAMP() FOR UPDATE",
                    [lock.table_id.into(), lock.hash_start.into(), lock.lock_token.clone().into()],
                )).await?;
                if held.is_none() {
                    txn.rollback().await?;
                    return Err(DbErr::Custom("Partition lease expired or changed; retry the split".into()));
                }
            }
            let partition = msg.data.update(&txn).await?;
            txn.commit().await?;
            cache.lock().unwrap().clear();
            Ok(partition)
        })
    }
}

/// Remove a boundary only while the merge still owns its leases.
pub struct DeleteLockedPartition {
    pub table_id: i64,
    pub hash_start: i64,
    pub locks: Vec<crate::datamodels::parition_lock::Model>,
}

impl Message<DeleteLockedPartition> for PartitionActor {
    type Reply = DelegatedReply<Result<u64, DbErr>>;

    async fn handle(
        &mut self,
        msg: DeleteLockedPartition,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        use sea_orm::{DbBackend, Statement, TransactionTrait};
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            if msg.locks.is_empty() {
                return Err(DbErr::Custom("Partition leases are required".into()));
            }
            let txn = db.begin().await?;
            // Hold the lease rows until commit, preventing replacement during deletion.
            for lock in &msg.locks {
                let held = txn.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::MySql,
                    "SELECT lock_token FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lock_token = ? AND lease_end > UNIX_TIMESTAMP() FOR UPDATE",
                    [lock.table_id.into(), lock.hash_start.into(), lock.lock_token.clone().into()],
                )).await?;
                if held.is_none() {
                    txn.rollback().await?;
                    return Err(DbErr::Custom("Partition lease expired or changed; retry the merge".into()));
                }
            }
            let deleted = Entity::delete_by_id((msg.table_id, msg.hash_start)).exec(&txn).await?;
            if deleted.rows_affected != 1 {
                return Err(DbErr::Custom("Partition no longer exists".into()));
            }
            txn.commit().await?;
            cache.lock().unwrap().clear();
            Ok(deleted.rows_affected)
        })
    }
}

/// Use the cached, ordered table ring for inconsistent ownership lookups.
pub struct GetPartitionAtOrBeforeCached {
    pub table_id: i64,
    pub hash_start: i64,
}

impl Message<GetPartitionAtOrBeforeCached> for PartitionActor {
    type Reply = DelegatedReply<Result<Option<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetPartitionAtOrBeforeCached,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Ask through the same handler so ownership and range reads share one cache.
        let ring = self
            .cache
            .lock()
            .unwrap()
            .partitions
            .get(&msg.table_id)
            .cloned();
        if let Some(ring) = ring {
            let next = ring.partition_point(|p| p.hash_start <= msg.hash_start);
            return ctx.reply(Ok(next.checked_sub(1).map(|i| ring[i].clone())));
        }
        let db = self.db.clone();
        let cache = self.cache.clone();
        let ttl = self.cache_ttl;
        let generation = cache.lock().unwrap().generation;
        ctx.spawn(async move {
            let ring = Entity::find()
                .filter(Column::TableId.eq(msg.table_id))
                .order_by_asc(Column::HashStart)
                .all(&db)
                .await?;
            let next = ring.partition_point(|p| p.hash_start <= msg.hash_start);
            let partition = next.checked_sub(1).map(|i| ring[i].clone());
            let mut cache = cache.lock().unwrap();
            if cache.generation == generation {
                cache.partitions.insert(msg.table_id, ring, ttl);
            }
            Ok(partition)
        })
    }
}
