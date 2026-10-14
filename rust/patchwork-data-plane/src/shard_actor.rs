use anyhow::Result;
use kameo::{Actor, actor::Spawn, messages};
use kameo_actors::pool::ActorPool;
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
    QueryFilter, Schema,
};

use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use crate::{
    data,
    read_shard::{ReadShard, ReadShardPool},
    telemetry::{self, KeyTracker},
};

/// Owns the single write connection for one shard.
#[derive(Actor)]
pub struct ShardActor {
    db: DatabaseConnection,
    path: std::path::PathBuf,
    telemetry: BTreeMap<(i64, i64), KeyTracker>,
    telemetry_cleanup: Instant,
}

impl ShardActor {
    pub async fn new(path: String) -> Result<Self> {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let mut options = ConnectOptions::new(format!("sqlite://{path}?mode=rwc"));
        options.max_connections(1).sqlx_logging(false);
        let db = Database::connect(options).await?;
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
            telemetry: BTreeMap::new(),
            telemetry_cleanup: Instant::now(),
        })
    }

    pub fn read_pool(&self, size: usize) -> Result<ReadShardPool> {
        anyhow::ensure!(size > 0, "Read shard pool must have at least one actor");
        let path = self.path.clone();
        Ok(ActorPool::spawn(ActorPool::new(size, move || {
            ReadShard::spawn(path.clone())
        })))
    }
}

impl ShardActor {
    fn track_access(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        is_write: bool,
    ) {
        if self.telemetry_cleanup.elapsed() >= telemetry::WINDOW {
            self.telemetry.retain(|_, tracker| !tracker.is_idle());
            self.telemetry_cleanup = Instant::now();
        }
        self.telemetry
            .entry((table_id, partition_key))
            .or_insert_with(|| KeyTracker::new(telemetry::BUFFER_SIZE))
            .inc(secondary_key, is_write);
    }
}

#[messages]
impl ShardActor {
    /// Recent local record accesses, grouped by table and partition key.
    #[message]
    pub async fn get_telemetry(&mut self) -> Vec<telemetry::Model> {
        self.telemetry.retain(|_, tracker| !tracker.is_idle());
        self.telemetry
            .iter()
            .map(|(&(table_id, partition_key), tracker)| telemetry::Model {
                table_id,
                partition_key,
                statistics: tracker.stats(),
            })
            .collect()
    }

    /// Recent accesses for one table, keyed by partition key.
    #[message]
    pub async fn get_telemetry_for_table(
        &mut self,
        table_id: i64,
    ) -> HashMap<i64, telemetry::Model> {
        self.telemetry.retain(|_, tracker| !tracker.is_idle());
        self.telemetry
            .iter()
            .filter(|((table, _), _)| *table == table_id)
            .map(|(&(_, partition_key), tracker)| {
                (
                    partition_key,
                    telemetry::Model {
                        table_id,
                        partition_key,
                        statistics: tracker.stats(),
                    },
                )
            })
            .collect()
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
        anyhow::ensure!(timestamp >= 0, "timestamp must be nonnegative");
        // Atomic comparison: old arrivals and retries cannot overwrite newer data.
        // Deletion wins ties; live values then compare serialized JSON bytes.
        let data = if deleted {
            serde_json::Value::Null
        } else {
            data
        };
        let result = self.db
            .execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Sqlite,
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
                    serde_json::to_string(&data)?.into(),
                    timestamp.into(),
                    deleted.into(),
                ],
            ))
            .await?;
        self.track_access(table_id, partition_key, secondary_key, true);
        Ok(result.rows_affected())
    }

    /// Delete all records for one table, including the full hash ring.
    #[message]
    pub async fn delete_table(&mut self, table_id: i64) -> Result<u64> {
        let result = data::Entity::delete_many()
            .filter(data::Column::TableId.eq(table_id))
            .exec(&self.db)
            .await?;
        self.telemetry.retain(|&(table, _), _| table != table_id);
        Ok(result.rows_affected)
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
        let result = data::Entity::delete_many()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::PartitionKey.eq(partition_key))
            .exec(&self.db)
            .await?;
        self.telemetry.remove(&(table_id, partition_key));
        Ok(result.rows_affected)
    }

    /// Deletes partition keys in [lower_bound, upper_bound), scoped to one table.
    #[message]
    pub async fn delete_range(
        &mut self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<u64> {
        let result = data::Entity::delete_many()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::PartitionKey.gte(lower_bound))
            .filter(data::Column::PartitionKey.lt(upper_bound))
            .exec(&self.db)
            .await?;
        self.telemetry.retain(|&(table, key), _| {
            table != table_id || key < lower_bound || key >= upper_bound
        });
        Ok(result.rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pragma(db: &DatabaseConnection, name: &str) -> i64 {
        db.query_one_raw(sea_orm::Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            format!("PRAGMA {name}"),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", name)
        .unwrap()
    }

    #[tokio::test]
    async fn auto_vacuum_handles_new_and_existing_shards() {
        let dir = tempfile::tempdir().unwrap();
        for existing in [false, true] {
            let path = dir
                .path()
                .join(format!("{existing}.sqlite"))
                .to_str()
                .unwrap()
                .to_owned();
            if existing {
                let db = Database::connect(format!("sqlite://{path}?mode=rwc"))
                    .await
                    .unwrap();
                db.execute_unprepared("PRAGMA auto_vacuum = NONE")
                    .await
                    .unwrap();
                db.execute_unprepared("CREATE TABLE legacy (id INTEGER PRIMARY KEY, value TEXT)")
                    .await
                    .unwrap();
                db.execute_unprepared("INSERT INTO legacy VALUES (1, 'preserved')")
                    .await
                    .unwrap();
                db.close().await.unwrap();
            }
            let mut shard = ShardActor::new(path.clone()).await.unwrap();
            assert_eq!(pragma(&shard.db, "auto_vacuum").await, 1);
            if existing {
                let row = shard
                    .db
                    .query_one_raw(sea_orm::Statement::from_string(
                        sea_orm::DbBackend::Sqlite,
                        "SELECT value FROM legacy WHERE id = 1",
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(row.try_get::<String>("", "value").unwrap(), "preserved");
            }
            shard
                .upsert(1, 1, 1, serde_json::json!("x".repeat(100_000)), 1)
                .await
                .unwrap();
            let before = pragma(&shard.db, "page_count").await;
            shard.delete(1, 1, 1, 2).await.unwrap();
            assert!(pragma(&shard.db, "page_count").await < before);
            assert_eq!(pragma(&shard.db, "freelist_count").await, 0);
            shard.db.close().await.unwrap();
            let reopened = ShardActor::new(path).await.unwrap();
            assert_eq!(pragma(&reopened.db, "auto_vacuum").await, 1);
            reopened.db.close().await.unwrap();
        }
    }
}
