//! Database setup and service helpers.
use anyhow::Result;
use kameo::actor::ActorRef;
#[cfg(test)]
use kameo::actor::Spawn;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Schema};

use crate::{data, telemetry::AccessStatsActor, write_shard::WriteShard};
use anyhow::Context;
use patchwork_common::grpc::proto;
use tonic::{Response, Status};

#[cfg(test)]
pub(crate) async fn shard(path: String) -> Result<WriteShard> {
    let stats = AccessStatsActor::spawn(
        AccessStatsActor::new(format!("{path}.access-stats.sqlite")).await?,
    );
    shard_with_access_stats(path, stats).await
}

/// Share one host statistics actor across shards.
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
    let columns = db
        .query_all_raw(sea_orm::Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            "PRAGMA table_info(data)",
        ))
        .await?;
    if !columns
        .iter()
        .any(|column| column.try_get::<String>("", "name").as_deref() == Ok("partition_id"))
    {
        db.execute_unprepared("ALTER TABLE data ADD COLUMN partition_id TEXT NULL")
            .await?;
    }
    db.execute_unprepared("CREATE INDEX IF NOT EXISTS data_partition_id ON data (partition_id)")
        .await?;
    Ok(WriteShard::new(
        db,
        std::fs::canonicalize(path)?,
        access_stats,
    ))
}

pub(crate) fn internal_error(error: impl std::fmt::Display) -> Status {
    eprintln!("Shard request failed: {error}");
    Status::internal("shard request failed")
}

pub(crate) fn write_error(error: impl crate::write_handler::WriteFailure) -> Status {
    if let Some(status) = error
        .status()
        .filter(|status| status.code() == tonic::Code::ResourceExhausted)
    {
        return status.clone();
    }
    internal_error(error)
}

pub(crate) fn timestamp(value: Option<i64>) -> Result<i64, Status> {
    value.filter(|value| *value >= 0).ok_or_else(|| {
        Status::invalid_argument("timestamp is required and must be nonnegative Unix microseconds")
    })
}

pub(crate) fn validate_range(range: &proto::RangeRequest) -> Result<(), Status> {
    if range.lower_bound >= range.upper_bound {
        return Err(Status::invalid_argument(
            "lower_bound must be less than upper_bound",
        ));
    }
    Ok(())
}

pub(crate) fn record(record: data::Model) -> Result<proto::Record, Status> {
    Ok(proto::Record {
        table_id: record.table_id,
        partition_id: record.partition_id,
        partition_key: record.partition_key,
        secondary_key: record.secondary_key,
        data_bin: record.data,
        timestamp: record.timestamp,
        deleted: record.deleted,
    })
}

pub(crate) fn records(values: Vec<data::Model>) -> Result<Response<proto::Records>, Status> {
    Ok(Response::new(proto::Records {
        records: values.into_iter().map(record).collect::<Result<_, _>>()?,
    }))
}

pub async fn read_connection(path: &std::path::Path, size: u32) -> Result<DatabaseConnection> {
    anyhow::ensure!(
        size > 0,
        "Read connection pool must have at least one connection"
    );
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options
        .min_connections(size)
        .max_connections(size)
        .test_before_acquire(false)
        .sqlx_logging(false);
    Ok(Database::connect(options).await?)
}

pub fn positive_env(name: &str, default: u64) -> Result<u64> {
    let value = std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    anyhow::ensure!(value > 0, "{name} must be a positive integer");
    Ok(value)
}

pub(crate) async fn range_total(
    db: &DatabaseConnection,
    range: &proto::RangeRequest,
    aggregate: &str,
) -> Result<u64> {
    let row = db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        format!("SELECT {aggregate} AS total FROM data WHERE table_id = ? AND partition_key >= ? AND partition_key < ? AND deleted = 0"),
        [range.table_id.into(), range.lower_bound.into(), range.upper_bound.into()],
    )).await?.ok_or_else(|| anyhow::anyhow!("Missing range total"))?;
    Ok(u64::try_from(row.try_get::<i64>("", "total")?)?)
}
