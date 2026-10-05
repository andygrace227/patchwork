use super::PartitionActor;
use kameo::{
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::entity::prelude::*;
use sea_orm::{DbBackend, QueryFilter, SqlErr, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "partition_lock")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub hash_start: i64,
    pub lease_end: i64,
    pub lock_token: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

/// lease_end is Unix seconds. Returns None if held or the requested lease has expired.
pub struct AttemptLock {
    pub table_id: i64,
    pub hash_start: i64,
    pub lease_end: i64,
}

impl Message<AttemptLock> for PartitionActor {
    type Reply = DelegatedReply<Result<Option<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: AttemptLock,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move {
            let txn = db.begin().await?;

            // Remove an expired lease, then compete for the unique partition key.
            txn.execute_raw(Statement::from_sql_and_values(
                DbBackend::MySql,
                "DELETE FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lease_end <= UNIX_TIMESTAMP()",
                [msg.table_id.into(), msg.hash_start.into()],
            )).await?;

            let inserted = txn.execute_raw(Statement::from_sql_and_values(
                DbBackend::MySql,
                "INSERT INTO partition_lock (table_id, hash_start, lease_end, lock_token) SELECT ?, ?, ?, UUID() WHERE ? > UNIX_TIMESTAMP()",
                [msg.table_id.into(), msg.hash_start.into(), msg.lease_end.into(), msg.lease_end.into()],
            )).await;

            match inserted {
                Ok(result) if result.rows_affected() == 0 => {
                    txn.rollback().await?;
                    return Ok(None);
                }
                Ok(_) => {}
                Err(err) => {
                    txn.rollback().await?;
                    if matches!(err.sql_err(), Some(SqlErr::UniqueConstraintViolation(_))) {
                        return Ok(None);
                    }
                    return Err(err);
                }
            }

            let lock = Entity::find_by_id((msg.table_id, msg.hash_start)).one(&txn).await?;
            txn.commit().await?;
            Ok(lock)
        })
    }
}

pub struct FreeLock {
    pub table_id: i64,
    pub hash_start: i64,
    // Use the token returned by AttemptLock.
    pub lock_token: String,
}

impl Message<FreeLock> for PartitionActor {
    type Reply = DelegatedReply<Result<bool, DbErr>>;

    async fn handle(&mut self, msg: FreeLock, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move {
            let result = Entity::delete_many()
                .filter(Column::TableId.eq(msg.table_id))
                .filter(Column::HashStart.eq(msg.hash_start))
                .filter(Column::LockToken.eq(msg.lock_token))
                .exec(&db)
                .await?;
            Ok(result.rows_affected > 0)
        })
    }
}
