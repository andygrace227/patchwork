//! Read workers for Kameo's actor pool, each with one read-only SQLite connection.
use crate::{
    data,
    telemetry::{AccessStatsActor, access_stats_actor::TrackAccess},
};
use anyhow::Result;
use kameo::{
    Actor,
    actor::{ActorRef, WeakActorRef},
    error::ActorStopReason,
    messages,
};
use kameo_actors::pool::ActorPool;
pub use kameo_actors::pool::Dispatch;
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend,
    EntityTrait, FromQueryResult, QueryFilter, QueryOrder, Statement,
};
use std::path::PathBuf;

pub type ReadShardPool = ActorRef<ActorPool<ReadShard>>;

pub struct ReadShard {
    // SeaORM uses SQLx's per-connection prepared statement cache for data queries.
    db: DatabaseConnection,
    access_stats: ActorRef<AccessStatsActor>,
}

impl Actor for ReadShard {
    type Args = (PathBuf, ActorRef<AccessStatsActor>);
    type Error = sea_orm::DbErr;

    async fn on_start(
        (path, access_stats): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
        options
            .min_connections(1)
            .max_connections(1)
            .sqlx_logging(false);
        Ok(Self {
            db: Database::connect(options).await?,
            access_stats,
        })
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.db.clone().close().await
    }
}

impl ReadShard {
    async fn track_access(&self, table_id: i64, partition_key: i64, partition_id: Option<String>) {
        if let Err(error) = self
            .access_stats
            .tell(TrackAccess {
                table_id,
                partition_key,
                is_write: false,
                partition_id,
            })
            .await
        {
            eprintln!("Could not record read statistics: {error}");
        }
    }

    async fn range_total(
        &self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
        aggregate: &str,
    ) -> Result<u64> {
        anyhow::ensure!(
            lower_bound < upper_bound,
            "lower_bound must be less than upper_bound"
        );
        let row = self.db.query_one_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            format!("SELECT {aggregate} AS total FROM data WHERE table_id = ? AND partition_key >= ? AND partition_key < ? AND deleted = 0"),
            [table_id.into(), lower_bound.into(), upper_bound.into()],
        )).await?.ok_or_else(|| anyhow::anyhow!("Missing range total"))?;
        Ok(u64::try_from(row.try_get::<i64>("", "total")?)?)
    }
}

#[messages]
impl ReadShard {
    /// Number of live records in one table's [lower_bound, upper_bound) range.
    #[message]
    pub async fn count_range(
        &mut self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<u64> {
        self.range_total(table_id, upper_bound, lower_bound, "COUNT(*)")
            .await
    }

    /// Stored payload bytes, excluding keys, indexes and SQLite overhead.
    #[message]
    pub async fn get_range_size(
        &mut self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<u64> {
        self.range_total(
            table_id,
            upper_bound,
            lower_bound,
            "COALESCE(SUM(LENGTH(CAST(data AS BLOB))), 0)",
        )
        .await
    }

    #[message]
    pub async fn get(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        include_deleted: bool,
    ) -> Result<Option<data::Model>> {
        let sql = if include_deleted {
            "SELECT table_id, partition_id, partition_key, secondary_key, data, timestamp, deleted
             FROM data WHERE table_id = ? AND partition_key = ? AND secondary_key = ?"
        } else {
            "SELECT table_id, partition_id, partition_key, secondary_key, data, timestamp, deleted
             FROM data WHERE table_id = ? AND partition_key = ? AND secondary_key = ? AND deleted = 0"
        };
        let record = data::Model::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            [table_id.into(), partition_key.into(), secondary_key.into()],
        ))
        .one(&self.db)
        .await?;
        self.track_access(
            table_id,
            partition_key,
            record.as_ref().and_then(|row| row.partition_id.clone()),
        )
        .await;
        Ok(record)
    }

    #[message]
    pub async fn get_partition_key(
        &mut self,
        table_id: i64,
        partition_key: i64,
        include_deleted: bool,
    ) -> Result<Vec<data::Model>> {
        let records = data::Entity::find()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::PartitionKey.eq(partition_key))
            .order_by_asc(data::Column::SecondaryKey)
            .filter(if include_deleted {
                sea_orm::Condition::all()
            } else {
                sea_orm::Condition::all().add(data::Column::Deleted.eq(false))
            })
            .all(&self.db)
            .await?;
        for record in &records {
            self.track_access(
                record.table_id,
                record.partition_key,
                record.partition_id.clone(),
            )
            .await;
        }
        Ok(records)
    }

    /// Reads partition keys in [lower_bound, upper_bound).
    #[message]
    pub async fn get_range(
        &mut self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
        include_deleted: bool,
    ) -> Result<Vec<data::Model>> {
        let records = data::Entity::find()
            .filter(data::Column::TableId.eq(table_id))
            .filter(data::Column::PartitionKey.gte(lower_bound))
            .filter(data::Column::PartitionKey.lt(upper_bound))
            .order_by_asc(data::Column::PartitionKey)
            .order_by_asc(data::Column::SecondaryKey)
            .filter(if include_deleted {
                sea_orm::Condition::all()
            } else {
                sea_orm::Condition::all().add(data::Column::Deleted.eq(false))
            })
            .all(&self.db)
            .await?;
        for record in &records {
            self.track_access(
                record.table_id,
                record.partition_key,
                record.partition_id.clone(),
            )
            .await;
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests;
