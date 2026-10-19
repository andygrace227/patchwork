use super::PartitionActor;
use crate::datamodels::parition_lock::{Column, Entity, Model};
use kameo::{message::Context, messages, reply::DelegatedReply};
use sea_orm::entity::prelude::*;
use sea_orm::{DbBackend, QueryFilter, SqlErr, Statement, TransactionTrait};

#[messages]
impl PartitionActor {
    /// lease_end is Unix seconds. Returns None if held or the requested lease has expired.
    #[message(ctx)]
    pub async fn attempt_lock(
        &mut self,
        table_id: i64,
        hash_start: i64,
        lease_end: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            let txn = db.begin().await?;

            // Remove an expired lease, then compete for the unique partition key.
            txn.execute_raw(Statement::from_sql_and_values(
                DbBackend::MySql,
                "DELETE FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lease_end <= UNIX_TIMESTAMP()",
                [table_id.into(), hash_start.into()],
            )).await?;

            let inserted = txn.execute_raw(Statement::from_sql_and_values(
                DbBackend::MySql,
                "INSERT INTO partition_lock (table_id, hash_start, lease_end, lock_token) SELECT ?, ?, ?, UUID() WHERE ? > UNIX_TIMESTAMP()",
                [table_id.into(), hash_start.into(), lease_end.into(), lease_end.into()],
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

            let lock = Entity::find_by_id((table_id, hash_start)).one(&txn).await?;
            txn.commit().await?;
            Ok(lock)
        })
    }

    #[message(ctx)]
    pub async fn free_lock(
        &mut self,
        table_id: i64,
        hash_start: i64,
        /// Use the token returned by AttemptLock.
        lock_token: String,
        ctx: &mut Context<Self, DelegatedReply<Result<bool, DbErr>>>,
    ) -> DelegatedReply<Result<bool, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            let result = Entity::delete_many()
                .filter(Column::TableId.eq(table_id))
                .filter(Column::HashStart.eq(hash_start))
                .filter(Column::LockToken.eq(lock_token))
                .exec(&db)
                .await?;
            Ok(result.rows_affected > 0)
        })
    }
}
