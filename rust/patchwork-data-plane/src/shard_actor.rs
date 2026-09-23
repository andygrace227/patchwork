use anyhow::Result;
use kameo::{Actor, messages};
use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
    QueryFilter, QueryOrder, Schema,
    sea_query::{Expr, ExprTrait, OnConflict},
};

use crate::data;

/// Serializes database access for one shard.
#[derive(Actor)]
pub struct ShardActor {
    db: DatabaseConnection,
    path: std::path::PathBuf,
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
        })
    }
}

#[messages]
impl ShardActor {
    /// Main SQLite file length in bytes, excluding WAL and SHM sidecar files.
    #[message]
    pub async fn get_size(&self) -> Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    #[message]
    pub async fn upsert(
        &mut self,
        table_id: i64,
        partition: i64,
        hash: i64,
        data: serde_json::Value,
    ) -> Result<()> {
        let model = data::ActiveModel {
            table_id: Set(table_id),
            partition: Set(partition),
            hash: Set(hash),
            data: Set(data),
            version: Set(1),
        };
        data::Entity::insert(model)
            .on_conflict(
                OnConflict::columns([
                    data::Column::TableId,
                    data::Column::Partition,
                    data::Column::Hash,
                ])
                .update_column(data::Column::Data)
                .value(
                    data::Column::Version,
                    Expr::col((data::Entity, data::Column::Version)).add(1),
                )
                .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await?;
        Ok(())
    }

    #[message]
    pub async fn delete(&mut self, table_id: i64, partition: i64, hash: i64) -> Result<u64> {
        Ok(data::Entity::delete_by_id((table_id, partition, hash))
            .exec(&self.db)
            .await?
            .rows_affected)
    }

    #[message]
    pub async fn delete_partition(&mut self, table_id: i64, partition: i64) -> Result<u64> {
        Ok(data::Entity::delete_many()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::Partition.eq(partition))
            .exec(&self.db)
            .await?
            .rows_affected)
    }

    #[message]
    pub async fn get(
        &self,
        table_id: i64,
        partition: i64,
        hash: i64,
    ) -> Result<Option<data::Model>> {
        Ok(data::Entity::find_by_id((table_id, partition, hash))
            .one(&self.db)
            .await?)
    }

    #[message]
    pub async fn get_partition(&self, table_id: i64, partition: i64) -> Result<Vec<data::Model>> {
        Ok(data::Entity::find()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::Partition.eq(partition))
            .order_by_asc(data::Column::Hash)
            .all(&self.db)
            .await?)
    }

    /// Reads partitions strictly between the two bounds.
    #[message]
    pub async fn get_range(
        &self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<Vec<data::Model>> {
        Ok(data::Entity::find()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::Partition.gt(lower_bound))
            .filter(data::Column::Partition.lt(upper_bound))
            .order_by_asc(data::Column::Partition)
            .order_by_asc(data::Column::Hash)
            .all(&self.db)
            .await?)
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
                .upsert(1, 1, 1, serde_json::json!("x".repeat(100_000)))
                .await
                .unwrap();
            let before = pragma(&shard.db, "page_count").await;
            shard.delete(1, 1, 1).await.unwrap();
            assert!(pragma(&shard.db, "page_count").await < before);
            assert_eq!(pragma(&shard.db, "freelist_count").await, 0);
            shard.db.close().await.unwrap();
            let reopened = ShardActor::new(path).await.unwrap();
            assert_eq!(pragma(&reopened.db, "auto_vacuum").await, 1);
            reopened.db.close().await.unwrap();
        }
    }
}
