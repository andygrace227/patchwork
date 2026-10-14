use anyhow::Result;
use kameo::{
    Actor,
    actor::{ActorRef, Spawn},
    messages,
};
use kameo_actors::pool::ActorPool;
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend,
    EntityTrait, QueryFilter, QueryTrait, Schema, Statement, TransactionTrait,
};

use std::collections::HashMap;

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
};

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
        anyhow::ensure!(timestamp >= 0, "timestamp must be nonnegative");
        // Atomic comparison: old arrivals and retries cannot overwrite newer data.
        // Deletion wins ties; live values then compare serialized JSON bytes.
        let data = if deleted {
            &serde_json::Value::Null
        } else {
            data
        };
        Ok(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO data (table_id, partition_key, secondary_key, data, timestamp, deleted)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (table_id, partition_key, secondary_key) DO UPDATE
             SET data = excluded.data, timestamp = excluded.timestamp, deleted = excluded.deleted
             WHERE excluded.timestamp > data.timestamp
                OR (excluded.timestamp = data.timestamp
                    AND (excluded.deleted > data.deleted
                         OR (excluded.deleted = data.deleted AND CAST(excluded.data AS BLOB) > CAST(data.data AS BLOB))))",
                [
                    table_id.into(),
                    partition_key.into(),
                    secondary_key.into(),
                    serde_json::to_string(data)?.into(),
                    timestamp.into(),
                    deleted.into(),
                ],
            ))
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

    pub async fn new(path: String) -> Result<Self> {
        let stats = AccessStatsActor::spawn(
            AccessStatsActor::new(format!("{path}.access-stats.sqlite")).await?,
        );
        Self::with_access_stats(path, stats).await
    }

    /// Share one host statistics actor across shards and their read pools.
    pub async fn with_access_stats(
        path: String,
        access_stats: ActorRef<AccessStatsActor>,
    ) -> Result<Self> {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let mut options = ConnectOptions::new(format!("sqlite://{path}?mode=rwc"));
        options.max_connections(1).sqlx_logging(false);
        let db = Database::connect(options).await?;
        // These one-time setup commands do not need cached prepared statements.
        db.execute_unprepared("PRAGMA auto_vacuum = FULL").await?;
        // Existing databases without auto-vacuum need a one-time rebuild.
        let mode = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Sqlite,
                "PRAGMA auto_vacuum",
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("Missing auto_vacuum result"))?;
        if mode.try_get::<i64>("", "auto_vacuum")? == 0 {
            db.execute_unprepared("VACUUM").await?;
        }
        db.execute_unprepared("PRAGMA journal_mode = WAL").await?;

        let backend = db.get_database_backend();
        let mut statement = Schema::new(backend).create_table_from_entity(data::Entity);
        statement.if_not_exists();
        db.execute(&statement).await?;
        Ok(Self {
            db,
            path: std::fs::canonicalize(path)?,
            access_stats,
        })
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
    pub async fn apply_batch(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let statements = mutations
            .iter()
            .map(Self::mutation_statement)
            .collect::<Result<Vec<_>>>()?;
        let txn = self.db.begin().await?;
        for statement in statements {
            if let Err(error) = txn.execute_raw(statement).await {
                txn.rollback().await?;
                return Err(error.into());
            }
        }
        txn.commit().await?;
        for mutation in mutations {
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
