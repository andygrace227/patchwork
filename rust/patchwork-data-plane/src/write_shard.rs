use anyhow::Result;
use kameo::{
    Actor,
    actor::{ActorRef, Spawn, WeakActorRef},
    messages,
};
use kameo_actors::pool::ActorPool;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, QueryFilter,
    QueryTrait, Statement, TransactionTrait, Value,
};

use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::oneshot;
use wal_db::Lsn;

use crate::{
    data,
    mutation::Mutation,
    read_shard::{ReadShard, ReadShardPool},
    telemetry::{
        self, AccessStatsActor,
        access_stats_actor::{
            GetTelemetry as GetAccessTelemetry, GetTelemetryForTable as GetAccessTelemetryForTable,
            TrackAccess,
        },
    },
    write_handler::{BatchFinished, WriteHandler},
};

pub const SQL_WRITE_BATCH_SIZE: usize = 256;

/// Owns the single write connection for one shard.
#[derive(Actor)]
pub struct WriteShard {
    // SeaORM uses SQLx's per-connection prepared statement cache for data queries.
    db: DatabaseConnection,
    path: std::path::PathBuf,
    access_stats: ActorRef<AccessStatsActor>,
}

impl WriteShard {
    /// Build bound SQL without executing it. SQLite prepares and caches it on execution.
    pub fn write_record_statement(
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &serde_json::Value,
        timestamp: i64,
        deleted: bool,
    ) -> Result<Statement> {
        Ok(Self::write_statement(Vec::from(Self::write_values(
            table_id,
            partition_key,
            secondary_key,
            data,
            timestamp,
            deleted,
        )?)))
    }

    fn write_values(
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: &serde_json::Value,
        timestamp: i64,
        deleted: bool,
    ) -> Result<[Value; 6]> {
        anyhow::ensure!(timestamp >= 0, "timestamp must be nonnegative");
        let data = if deleted {
            &serde_json::Value::Null
        } else {
            data
        };
        Ok([
            table_id.into(),
            partition_key.into(),
            secondary_key.into(),
            serde_json::to_string(data)?.into(),
            timestamp.into(),
            deleted.into(),
        ])
    }

    fn write_statement(values: Vec<Value>) -> Statement {
        let rows = vec!["(?, ?, ?, ?, ?, ?)"; values.len() / 6].join(", ");
        // Keep the same timestamp, tombstone, and JSON tie ordering for every row.
        Statement::from_sql_and_values(
            DbBackend::Sqlite,
            format!("INSERT INTO data (table_id, partition_key, secondary_key, data, timestamp, deleted)
             VALUES {rows}
             ON CONFLICT (table_id, partition_key, secondary_key) DO UPDATE
             SET data = excluded.data, timestamp = excluded.timestamp, deleted = excluded.deleted
             WHERE excluded.timestamp > data.timestamp
                OR (excluded.timestamp = data.timestamp
                    AND (excluded.deleted > data.deleted
                         OR (excluded.deleted = data.deleted AND CAST(excluded.data AS BLOB) > CAST(data.data AS BLOB))))"),
            values,
        )
    }

    fn batch_statements(mutations: &[Mutation]) -> Result<Vec<Statement>> {
        let mut statements = Vec::new();
        let mut values = Vec::new();
        for mutation in mutations {
            if let Mutation::Write(record) = mutation {
                values.extend(Self::write_values(
                    record.table_id,
                    record.partition_key,
                    record.secondary_key,
                    &record.data,
                    record.timestamp,
                    record.deleted,
                )?);
                if values.len() == SQL_WRITE_BATCH_SIZE * 6 {
                    statements.push(Self::write_statement(std::mem::take(&mut values)));
                }
            } else {
                // Scoped deletes must stay between the writes that surround them.
                if !values.is_empty() {
                    statements.push(Self::write_statement(std::mem::take(&mut values)));
                }
                statements.push(Self::mutation_statement(mutation)?);
            }
        }
        if !values.is_empty() {
            statements.push(Self::write_statement(values));
        }
        Ok(statements)
    }

    pub fn delete_table_statement(table_id: i64) -> Statement {
        data::Entity::delete_many()
            .filter(data::Column::TableId.eq(table_id))
            .build(DbBackend::Sqlite)
    }

    pub fn delete_partition_key_statement(table_id: i64, partition_key: i64) -> Statement {
        data::Entity::delete_many()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::PartitionKey.eq(partition_key))
            .build(DbBackend::Sqlite)
    }

    pub fn delete_range_statement(table_id: i64, upper_bound: i64, lower_bound: i64) -> Statement {
        data::Entity::delete_many()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::PartitionKey.gte(lower_bound))
            .filter(data::Column::PartitionKey.lt(upper_bound))
            .build(DbBackend::Sqlite)
    }

    pub fn mutation_statement(mutation: &Mutation) -> Result<Statement> {
        Ok(match mutation {
            Mutation::Write(record) => Self::write_record_statement(
                record.table_id,
                record.partition_key,
                record.secondary_key,
                &record.data,
                record.timestamp,
                record.deleted,
            )?,
            Mutation::DeleteTable { table_id } => Self::delete_table_statement(*table_id),
            Mutation::DeletePartitionKey {
                table_id,
                partition_key,
            } => Self::delete_partition_key_statement(*table_id, *partition_key),
            Mutation::DeleteRange {
                table_id,
                upper_bound,
                lower_bound,
            } => Self::delete_range_statement(*table_id, *upper_bound, *lower_bound),
        })
    }

    pub fn new(
        db: DatabaseConnection,
        path: PathBuf,
        access_stats: ActorRef<AccessStatsActor>,
    ) -> Self {
        Self {
            db,
            path,
            access_stats,
        }
    }

    pub fn read_pool(&self, size: usize) -> Result<ReadShardPool> {
        anyhow::ensure!(size > 0, "Read shard pool must have at least one actor");
        let path = self.path.clone();
        let stats = self.access_stats.clone();
        Ok(ActorPool::spawn(ActorPool::new(size, move || {
            ReadShard::spawn((path.clone(), stats.clone()))
        })))
    }
}

#[messages]
impl WriteShard {
    /// Apply the queued mutations in order, committing the entire batch together.
    #[message]
    pub async fn apply_batch(&mut self, mutations: Arc<Vec<Mutation>>) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let statements = Self::batch_statements(&mutations)?;
        let txn = self.db.begin().await?;
        for statement in statements {
            if let Err(error) = txn.execute_raw(statement).await {
                txn.rollback().await?;
                return Err(error.into());
            }
        }
        txn.commit().await?;
        for mutation in mutations.iter() {
            if let Mutation::Write(record) = mutation {
                if let Err(error) = self
                    .access_stats
                    .tell(TrackAccess {
                        table_id: record.table_id,
                        partition_key: record.partition_key,
                        secondary_key: record.secondary_key,
                        is_write: true,
                    })
                    .await
                {
                    eprintln!("Could not record write statistics: {error}");
                }
            }
        }
        Ok(())
    }

    #[message]
    pub async fn apply_queued_batch(
        &mut self,
        mutations: Arc<Vec<Mutation>>,
        end_lsn: Lsn,
        handler: WeakActorRef<WriteHandler>,
        completed: oneshot::Sender<Result<()>>,
    ) {
        let result = self.apply_batch(mutations).await;
        let _ = completed.send(result);
        if let Some(handler) = handler.upgrade() {
            // The timer also checks completion if the handler's mailbox is full.
            let _ = handler.tell(BatchFinished { end_lsn }).try_send();
        }
    }

    /// Current unflushed host accesses, grouped by table and partition key.
    #[message]
    pub async fn get_telemetry(&mut self) -> Result<Vec<telemetry::Model>> {
        self.access_stats
            .ask(GetAccessTelemetry {})
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    #[message]
    pub async fn get_telemetry_for_table(
        &mut self,
        table_id: i64,
    ) -> Result<HashMap<i64, telemetry::Model>> {
        self.access_stats
            .ask(GetAccessTelemetryForTable { table_id })
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    /// Main SQLite file length in bytes, excluding WAL and SHM sidecar files.
    #[message]
    pub async fn get_size(&self) -> Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    #[message]
    pub async fn upsert(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: serde_json::Value,
        timestamp: i64,
    ) -> Result<()> {
        self.write_record(
            table_id,
            partition_key,
            secondary_key,
            data,
            timestamp,
            false,
        )
        .await?;
        Ok(())
    }

    #[message]
    pub async fn write_record(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: serde_json::Value,
        timestamp: i64,
        deleted: bool,
    ) -> Result<u64> {
        let result = self
            .db
            .execute_raw(Self::write_record_statement(
                table_id,
                partition_key,
                secondary_key,
                &data,
                timestamp,
                deleted,
            )?)
            .await?;
        if let Err(error) = self
            .access_stats
            .tell(TrackAccess {
                table_id,
                partition_key,
                secondary_key,
                is_write: true,
            })
            .await
        {
            eprintln!("Could not record write statistics: {error}");
        }
        Ok(result.rows_affected())
    }

    /// Delete all records for one table, including the full hash ring.
    #[message]
    pub async fn delete_table(&mut self, table_id: i64) -> Result<u64> {
        let result = self
            .db
            .execute_raw(Self::delete_table_statement(table_id))
            .await?;
        Ok(result.rows_affected())
    }

    #[message]
    pub async fn delete(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        timestamp: i64,
    ) -> Result<u64> {
        self.write_record(
            table_id,
            partition_key,
            secondary_key,
            serde_json::Value::Null,
            timestamp,
            true,
        )
        .await
    }

    #[message]
    pub async fn delete_partition_key(&mut self, table_id: i64, partition_key: i64) -> Result<u64> {
        let result = self
            .db
            .execute_raw(Self::delete_partition_key_statement(
                table_id,
                partition_key,
            ))
            .await?;
        Ok(result.rows_affected())
    }

    /// Deletes partition keys in [lower_bound, upper_bound), scoped to one table.
    #[message]
    pub async fn delete_range(
        &mut self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<u64> {
        let result = self
            .db
            .execute_raw(Self::delete_range_statement(
                table_id,
                upper_bound,
                lower_bound,
            ))
            .await?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests;
