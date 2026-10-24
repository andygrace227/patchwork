//! Data-plane gRPC service.
use crate::write_handler::{self, WriteHandler};
use crate::{
    data,
    replica_writer::ReplicaWriter,
    telemetry::{
        AccessStatsActor, QueuedAccess,
        access_stats_actor::{GetTelemetry, GetTelemetryForTable, RecordBatch, TrackAccess},
    },
    utils::{
        internal_error, range_total, read_connection, record, records, shard_with_access_stats,
        timestamp, validate_range, write_error,
    },
    write_shard::{GetSize, WriteShard},
};
use kameo::actor::{ActorRef, Spawn};
use patchwork_common::dataplane_client::Record;
use patchwork_common::grpc::{
    MAX_MESSAGE_SIZE,
    proto::{
        self,
        data_plane_server::{DataPlane, DataPlaneServer},
    },
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, Statement,
};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct Service {
    pub(crate) write_shard: ActorRef<WriteShard>,
    pub(crate) db: DatabaseConnection,
    pub(crate) telemetry: ActorRef<AccessStatsActor>,
    pub(crate) handler: ActorRef<WriteHandler>,
    writer: ActorRef<ReplicaWriter>,
}

impl Service {
    pub async fn new(
        path: &std::path::Path,
        wal_path: &std::path::Path,
        telemetry: ActorRef<AccessStatsActor>,
        read_connections: u32,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            read_connections > 0,
            "Read connection pool must have at least one connection"
        );
        let shard = shard_with_access_stats(path.display().to_string(), telemetry.clone()).await?;
        let db = read_connection(&std::fs::canonicalize(path)?, read_connections).await?;
        let write_shard = WriteShard::spawn(shard);
        let writer = ReplicaWriter::spawn(ReplicaWriter::default());
        let handler = WriteHandler::spawn(WriteHandler::new(
            write_shard.clone(),
            writer.clone(),
            wal_path,
        )?);
        handler
            .wait_for_startup_with_result(|result| {
                result
                    .map(|_| ())
                    .map_err(|error| anyhow::anyhow!("{error}"))
            })
            .await?;
        Ok(Self {
            write_shard,
            db,
            telemetry,
            handler,
            writer,
        })
    }

    pub fn server(&self) -> DataPlaneServer<Self> {
        DataPlaneServer::new(self.clone())
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE)
    }

    /// Stop accepting RPCs before calling this; telemetry is shared and stopped by its owner.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.handler.stop_gracefully().await?;
        self.handler
            .wait_for_shutdown_with_result(|result| {
                result
                    .map(|_| ())
                    .map_err(|error| anyhow::anyhow!("{error}"))
            })
            .await?;
        self.writer.stop_gracefully().await?;
        self.writer.wait_for_shutdown().await;
        self.write_shard.stop_gracefully().await?;
        self.write_shard
            .wait_for_shutdown_with_result(|result| {
                result
                    .map(|_| ())
                    .map_err(|error| anyhow::anyhow!("{error}"))
            })
            .await?;
        self.db.close().await?;
        Ok(())
    }

    async fn track_read(&self, record: &data::Model) -> Result<(), Status> {
        if record
            .partition_id
            .as_deref()
            .is_some_and(|id| !id.is_empty())
        {
            self.telemetry
                .tell(TrackAccess {
                    table_id: record.table_id,
                    partition_key: record.partition_key,
                    is_write: false,
                    partition_id: record.partition_id.clone(),
                })
                .await
                .map_err(internal_error)?;
        }
        Ok(())
    }

    async fn track_reads(&self, records: &[data::Model]) -> Result<(), Status> {
        let time_ms = crate::telemetry::now_ms();
        let accesses = records
            .iter()
            .filter_map(|record| {
                let partition_id = record.partition_id.as_ref().filter(|id| !id.is_empty())?;
                Some(QueuedAccess {
                    table_id: record.table_id,
                    partition_key: record.partition_key,
                    is_write: false,
                    partition_id: partition_id.clone(),
                    time_ms,
                })
            })
            .collect::<Vec<_>>();
        if !accesses.is_empty() {
            self.telemetry
                .tell(RecordBatch { accesses })
                .await
                .map_err(internal_error)?;
        }
        Ok(())
    }

    async fn write(
        &self,
        request: proto::WriteRequest,
        replicated: bool,
        inconsistent: bool,
    ) -> Result<Response<proto::Empty>, Status> {
        let timestamp = timestamp(request.timestamp)?;
        if inconsistent && request.replicas.is_empty() {
            return Err(Status::invalid_argument(
                "at least one remote replica is required",
            ));
        }
        let data = request.data_bin;
        self.handler
            .ask(write_handler::WriteRecord {
                record: Record {
                    table_id: request.table_id,
                    partition_id: request.partition_id,
                    partition_key: request.partition_key,
                    secondary_key: request.secondary_key,
                    data,
                    timestamp,
                    deleted: request.deleted,
                },
                replicas: if replicated {
                    request.replicas
                } else {
                    Vec::new()
                },
                consistent: !inconsistent,
            })
            .await
            .map_err(write_error)?;
        Ok(Response::new(proto::Empty {}))
    }
}

#[tonic::async_trait]
impl DataPlane for Service {
    async fn write_record(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        self.write(request.into_inner(), false, false).await
    }

    async fn write_with_replicas(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        self.write(request.into_inner(), true, false).await
    }

    async fn write_with_replicas_inconsistent(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        self.write(request.into_inner(), true, true).await
    }

    async fn delete_record(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        let count = self
            .handler
            .ask(write_handler::Delete {
                table_id: request.table_id,
                partition_key: request.partition_key,
                secondary_key: request.secondary_key,
                timestamp: timestamp(request.timestamp)?,
            })
            .await
            .map_err(write_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn delete_partition_key(
        &self,
        request: Request<proto::PartitionRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        let count = self
            .handler
            .ask(write_handler::DeletePartitionKey {
                table_id: request.table_id,
                partition_key: request.partition_key,
            })
            .await
            .map_err(write_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn delete_range(
        &self,
        request: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        validate_range(&request)?;
        let count = self
            .handler
            .ask(write_handler::DeleteRange {
                table_id: request.table_id,
                lower_bound: request.lower_bound,
                upper_bound: request.upper_bound,
            })
            .await
            .map_err(write_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn get_record(
        &self,
        request: Request<proto::RecordRequest>,
    ) -> Result<Response<proto::Record>, Status> {
        let request = request.into_inner();

        let sql = if request.include_deleted {
            "SELECT table_id, partition_id, partition_key, secondary_key, data, timestamp, deleted
             FROM data WHERE table_id = ? AND partition_key = ? AND secondary_key = ?"
        } else {
            "SELECT table_id, partition_id, partition_key, secondary_key, data, timestamp, deleted
             FROM data WHERE table_id = ? AND partition_key = ? AND secondary_key = ? AND deleted = 0"
        };

        let rec = data::Model::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            [
                request.table_id.into(),
                request.partition_key.into(),
                request.secondary_key.into(),
            ],
        ))
        .one(&self.db)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| Status::not_found("record not found"))?;

        self.track_read(&rec).await?;
        Ok(Response::new(record(rec)?))
    }

    async fn get_partition_key(
        &self,
        request: Request<proto::PartitionRequest>,
    ) -> Result<Response<proto::Records>, Status> {
        let request = request.into_inner();

        let recs = data::Entity::find()
            .filter(data::Column::TableId.eq(request.table_id))
            .filter(data::Column::PartitionKey.eq(request.partition_key))
            .order_by_asc(data::Column::SecondaryKey)
            .filter(if request.include_deleted {
                sea_orm::Condition::all()
            } else {
                sea_orm::Condition::all().add(data::Column::Deleted.eq(false))
            })
            .all(&self.db)
            .await
            .map_err(internal_error)?;

        self.track_reads(&recs).await?;

        records(recs)
    }

    async fn get_range(
        &self,
        request: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Records>, Status> {
        let request = request.into_inner();
        validate_range(&request)?;

        let recs = data::Entity::find()
            .filter(data::Column::TableId.eq(request.table_id))
            .filter(data::Column::PartitionKey.gte(request.lower_bound))
            .filter(data::Column::PartitionKey.lt(request.upper_bound))
            .order_by_asc(data::Column::PartitionKey)
            .order_by_asc(data::Column::SecondaryKey)
            .filter(if request.include_deleted {
                sea_orm::Condition::all()
            } else {
                sea_orm::Condition::all().add(data::Column::Deleted.eq(false))
            })
            .all(&self.db)
            .await
            .map_err(internal_error)?;

        self.track_reads(&recs).await?;

        records(recs)
    }

    async fn count_range(
        &self,
        request: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        validate_range(&request)?;

        let count = range_total(&self.db, &request, "COUNT(*)")
            .await
            .map_err(internal_error)?;

        Ok(Response::new(proto::Count { count }))
    }

    async fn get_range_size(
        &self,
        request: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Size>, Status> {
        let request = request.into_inner();
        validate_range(&request)?;
        let size_bytes = range_total(&self.db, &request, "COALESCE(SUM(LENGTH(data)), 0)")
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::Size { size_bytes }))
    }

    async fn delete_table(
        &self,
        request: Request<proto::TableRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let count = self
            .handler
            .ask(write_handler::DeleteTable {
                table_id: request.into_inner().table_id,
            })
            .await
            .map_err(write_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn get_size(&self, _: Request<proto::Empty>) -> Result<Response<proto::Size>, Status> {
        let size_bytes = self
            .write_shard
            .ask(GetSize {})
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::Size { size_bytes }))
    }

    async fn get_write_backlog(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::WriteBacklog>, Status> {
        let backlog = self
            .handler
            .ask(write_handler::GetWriteBacklog {})
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::WriteBacklog {
            queued: backlog.queued as u64,
            in_flight: backlog.in_flight as u64,
            total: backlog.total as u64,
        }))
    }

    async fn get_telemetry(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::TelemetryEntries>, Status> {
        let entries = self
            .telemetry
            .ask(GetTelemetry {})
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::TelemetryEntries {
            entries: entries.into_iter().map(Into::into).collect(),
        }))
    }

    async fn get_telemetry_for_table(
        &self,
        request: Request<proto::TableRequest>,
    ) -> Result<Response<proto::TelemetryEntries>, Status> {
        let entries = self
            .telemetry
            .ask(GetTelemetryForTable {
                table_id: request.into_inner().table_id,
            })
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::TelemetryEntries {
            entries: entries.into_values().map(Into::into).collect(),
        }))
    }
}

#[cfg(test)]
mod tests;
