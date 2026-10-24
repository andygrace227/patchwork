use anyhow::Result;
use patchwork_common::datamodels::access_stats::{self, Entity};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
    IntoActiveModel, NotSet, QueryFilter, Schema, TransactionTrait,
};
use std::{path::Path, sync::Arc, time::Duration};
use wal_db::{Lsn, SegmentedStore, Wal};

pub(super) async fn open(path: &Path) -> Result<DatabaseConnection> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(2).sqlx_logging(false);
    let db = Database::connect(options).await?;
    db.execute_unprepared("PRAGMA journal_mode = WAL").await?;
    let mut schema = Schema::new(db.get_database_backend()).create_table_from_entity(Entity);
    schema.if_not_exists();
    db.execute(&schema).await?;
    let columns = db
        .query_all_raw(sea_orm::Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            "PRAGMA table_info(access_statistics)",
        ))
        .await?;
    if !columns
        .iter()
        .any(|column| column.try_get::<String>("", "name").as_deref() == Ok("partition_id"))
    {
        db.execute_unprepared("ALTER TABLE access_statistics ADD COLUMN partition_id TEXT NULL")
            .await?;
    }
    db.execute_unprepared(
        "CREATE INDEX IF NOT EXISTS access_statistics_end_ms ON access_statistics (end_ms)",
    )
    .await?;
    // Old history keeps its optional partition label; future samples use IDs only.
    if columns
        .iter()
        .any(|column| column.try_get::<String>("", "name").as_deref() == Ok("hash_start"))
    {
        db.execute_unprepared("DROP INDEX IF EXISTS access_statistics_key_time")
            .await?;
        db.execute_unprepared("ALTER TABLE access_statistics DROP COLUMN hash_start")
            .await?;
    }
    db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS access_statistics_partition_time ON access_statistics (table_id, partition_id, start_ms)",
        ).await?;
    db.execute_unprepared("CREATE TABLE IF NOT EXISTS telemetry_checkpoint (id INTEGER PRIMARY KEY CHECK (id = 1), end_lsn INTEGER NOT NULL)").await?;
    Ok(db)
}

pub(super) async fn checkpoint(db: &DatabaseConnection) -> Result<u64> {
    let value = db
        .query_one_raw(sea_orm::Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            "SELECT end_lsn FROM telemetry_checkpoint WHERE id = 1",
        ))
        .await?
        .map(|row| row.try_get::<i64>("", "end_lsn"))
        .transpose()?
        .unwrap_or(0);
    Ok(u64::try_from(value)?)
}

/// Store the sample and its journal position together, then reclaim the applied prefix.
pub(super) async fn flush(
    db: DatabaseConnection,
    wal: Arc<Wal<SegmentedStore>>,
    rows: Vec<access_stats::Model>,
    end_lsn: u64,
    retention: Duration,
) -> Result<()> {
    let sync_wal = wal.clone();
    tokio::task::spawn_blocking(move || sync_wal.sync()).await??;
    let txn = db.begin().await?;
    for chunk in rows.chunks(50) {
        Entity::insert_many(chunk.iter().cloned().map(|row| {
            let mut row = row.into_active_model();
            row.id = NotSet;
            row
        }))
        .exec_without_returning(&txn)
        .await?;
    }
    Entity::delete_many()
        .filter(
            access_stats::Column::EndMs
                .lt(super::now_ms().saturating_sub(retention.as_millis() as i64)),
        )
        .exec(&txn)
        .await?;
    txn.execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Sqlite,
                "INSERT INTO telemetry_checkpoint (id, end_lsn) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET end_lsn = excluded.end_lsn",
                [i64::try_from(end_lsn)?.into()],
            )).await?;
    txn.commit().await?;
    // A cleanup failure must not retry an already committed sample.
    tokio::task::spawn_blocking(move || {
        if let Err(error) = wal.truncate_before(Lsn::new(end_lsn)) {
            eprintln!("Could not reclaim telemetry journal: {error}");
        }
    })
    .await
    .unwrap_or_else(|error| {
        eprintln!("Could not finish telemetry journal cleanup: {error}");
    });
    Ok(())
}
