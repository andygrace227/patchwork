use super::PartitionActor;
use kameo::{
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::entity::prelude::*;
use sea_orm::{FromJsonQueryResult, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "partition")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub hash_start: i64,

    #[sea_orm(indexed)]
    pub node_id: i64,

    pub forward_to: i64, // A flag. If this is not NULL, then send reads to the forwarded node exclusively, and writes here and to the forwarded node.

    #[sea_orm(column_type = "Json")]
    pub replicas: ReplicaNodes,
}

/// All nodes holding this partition, including its primary node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ReplicaNodes(pub Vec<i64>);

impl Model {
    pub fn replication_factor(&self) -> usize {
        self.replicas.0.len()
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

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

/// Publish a bootstrapping partition only while all supplied leases are still ours.
pub struct CreateLockedPartition {
    pub data: ActiveModel,
    pub locks: Vec<super::parition_lock::Model>,
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
            Ok(partition)
        })
    }
}

/// Update a partition only while all supplied leases are still ours.
pub struct UpdateLockedPartition {
    pub data: ActiveModel,
    pub locks: Vec<super::parition_lock::Model>,
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
            Ok(partition)
        })
    }
}

/// Remove a boundary only while the merge still owns its leases.
pub struct DeleteLockedPartition {
    pub table_id: i64,
    pub hash_start: i64,
    pub locks: Vec<super::parition_lock::Model>,
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
            Ok(deleted.rows_affected)
        })
    }
}
