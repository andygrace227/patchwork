//! SQLite shard setup, separate from the actors that use it.
use anyhow::Result;
use kameo::actor::{ActorRef, Spawn};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, Schema};

use crate::{data, telemetry::AccessStatsActor, write_shard::WriteShard};

pub async fn shard(path: String) -> Result<WriteShard> {
    let stats = AccessStatsActor::spawn(
        AccessStatsActor::new(format!("{path}.access-stats.sqlite")).await?,
    );
    shard_with_access_stats(path, stats).await
}

/// Share one host statistics actor across shards and their read pools.
pub async fn shard_with_access_stats(
    path: String,
    access_stats: ActorRef<AccessStatsActor>,
) -> Result<WriteShard> {
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

    let mut statement =
        Schema::new(db.get_database_backend()).create_table_from_entity(data::Entity);
    statement.if_not_exists();
    db.execute(&statement).await?;
    Ok(WriteShard::new(
        db,
        std::fs::canonicalize(path)?,
        access_stats,
    ))
}
