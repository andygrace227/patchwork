//! gRPC handlers use the same shard actor and replica writer as the HTTP API.
use crate::{
    data,
    read_shard::{CountRange, Get, GetPartitionKey, GetRange, GetRangeSize, ReadShardPool},
    shard_actor::*,
    subordinate_shard_writer::{Put, SubordinateShardWriter},
};
use kameo::actor::ActorRef;
use kameo_actors::pool::Dispatch;
use patchwork_common::grpc::{
    MAX_MESSAGE_SIZE,
    proto::{
        self,
        data_plane_server::{DataPlane, DataPlaneServer},
    },
};
use tonic::{Request, Response, Status};

pub fn router(
    shard: ActorRef<ShardActor>,
    readers: ReadShardPool,
    writer: ActorRef<SubordinateShardWriter>,
) -> axum::Router {
    tonic::service::Routes::new(
        DataPlaneServer::new(Service {
            shard,
            readers,
            writer,
        })
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE),
    )
    .into_axum_router()
}

struct Service {
    shard: ActorRef<ShardActor>,
    readers: ReadShardPool,
    writer: ActorRef<SubordinateShardWriter>,
}

fn internal_error(error: impl std::fmt::Display) -> Status {
    eprintln!("Shard request failed: {error}");
    Status::internal("shard request failed")
}

fn timestamp(value: Option<i64>) -> Result<i64, Status> {
    value.filter(|value| *value >= 0).ok_or_else(|| {
        Status::invalid_argument("timestamp is required and must be nonnegative Unix microseconds")
    })
}

fn validate_range(range: &proto::RangeRequest) -> Result<(), Status> {
    if range.lower_bound >= range.upper_bound {
        return Err(Status::invalid_argument(
            "lower_bound must be less than upper_bound",
        ));
    }
    Ok(())
}

fn record(record: data::Model) -> Result<proto::Record, Status> {
    Ok(proto::Record {
        table_id: record.table_id,
        partition_key: record.partition_key,
        secondary_key: record.secondary_key,
        data_json: serde_json::to_vec(&record.data).map_err(internal_error)?,
        timestamp: record.timestamp,
        deleted: record.deleted,
    })
}

fn records(values: Vec<data::Model>) -> Result<Response<proto::Records>, Status> {
    Ok(Response::new(proto::Records {
        records: values.into_iter().map(record).collect::<Result<_, _>>()?,
    }))
}

impl Service {
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
        let data = serde_json::from_slice(&request.data_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if replicated {
            crate::subordinate_shard_writer::write(
                &self.shard,
                &self.writer,
                Put {
                    table_id: request.table_id,
                    partition_key: request.partition_key,
                    secondary_key: request.secondary_key,
                    data,
                    replicas: request.replicas,
                    timestamp,
                    deleted: request.deleted,
                },
                inconsistent,
            )
            .await
            .map_err(internal_error)?;
        } else {
            self.shard
                .ask(WriteRecord {
                    table_id: request.table_id,
                    partition_key: request.partition_key,
                    secondary_key: request.secondary_key,
                    data,
                    timestamp,
                    deleted: request.deleted,
                })
                .await
                .map_err(internal_error)?;
        }
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

    async fn get_record(
        &self,
        request: Request<proto::RecordRequest>,
    ) -> Result<Response<proto::Record>, Status> {
        let request = request.into_inner();
        let value = self
            .readers
            .ask(Dispatch(Get {
                table_id: request.table_id,
                partition_key: request.partition_key,
                secondary_key: request.secondary_key,
                include_deleted: request.include_deleted,
            }))
            .await
            .map_err(internal_error)?
            .ok_or_else(|| Status::not_found("record not found"))?;
        Ok(Response::new(record(value)?))
    }

    async fn delete_record(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        let count = self
            .shard
            .ask(Delete {
                table_id: request.table_id,
                partition_key: request.partition_key,
                secondary_key: request.secondary_key,
                timestamp: timestamp(request.timestamp)?,
            })
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn get_partition_key(
        &self,
        request: Request<proto::PartitionRequest>,
    ) -> Result<Response<proto::Records>, Status> {
        let request = request.into_inner();
        records(
            self.readers
                .ask(Dispatch(GetPartitionKey {
                    table_id: request.table_id,
                    partition_key: request.partition_key,
                    include_deleted: request.include_deleted,
                }))
                .await
                .map_err(internal_error)?,
        )
    }

    async fn delete_partition_key(
        &self,
        request: Request<proto::PartitionRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        let count = self
            .shard
            .ask(DeletePartitionKey {
                table_id: request.table_id,
                partition_key: request.partition_key,
            })
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn get_range(
        &self,
        request: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Records>, Status> {
        let request = request.into_inner();
        validate_range(&request)?;
        records(
            self.readers
                .ask(Dispatch(GetRange {
                    table_id: request.table_id,
                    lower_bound: request.lower_bound,
                    upper_bound: request.upper_bound,
                    include_deleted: request.include_deleted,
                }))
                .await
                .map_err(internal_error)?,
        )
    }

    async fn delete_range(
        &self,
        request: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        validate_range(&request)?;
        let count = self
            .shard
            .ask(DeleteRange {
                table_id: request.table_id,
                lower_bound: request.lower_bound,
                upper_bound: request.upper_bound,
            })
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn count_range(
        &self,
        request: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let request = request.into_inner();
        validate_range(&request)?;
        let count = self
            .readers
            .ask(Dispatch(CountRange {
                table_id: request.table_id,
                lower_bound: request.lower_bound,
                upper_bound: request.upper_bound,
            }))
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
        let size_bytes = self
            .readers
            .ask(Dispatch(GetRangeSize {
                table_id: request.table_id,
                lower_bound: request.lower_bound,
                upper_bound: request.upper_bound,
            }))
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::Size { size_bytes }))
    }

    async fn delete_table(
        &self,
        request: Request<proto::TableRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        let count = self
            .shard
            .ask(DeleteTable {
                table_id: request.into_inner().table_id,
            })
            .await
            .map_err(internal_error)?;
        Ok(Response::new(proto::Count { count }))
    }

    async fn get_size(&self, _: Request<proto::Empty>) -> Result<Response<proto::Size>, Status> {
        let size_bytes = self.shard.ask(GetSize {}).await.map_err(internal_error)?;
        Ok(Response::new(proto::Size { size_bytes }))
    }

    async fn get_telemetry(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::TelemetryEntries>, Status> {
        let entries = self
            .shard
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
            .shard
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
