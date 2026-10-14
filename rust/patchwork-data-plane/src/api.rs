use crate::write_handler::{self, WriteHandler};
use crate::{
    data,
    read_shard::{CountRange, Get, GetPartitionKey, GetRange, GetRangeSize, ReadShardPool},
    subordinate_shard_writer::ReplicaWriter,
    telemetry,
    write_shard::{GetSize, GetTelemetry, GetTelemetryForTable, WriteShard},
};
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use kameo::actor::{ActorRef, Spawn};
use kameo_actors::pool::Dispatch;
use patchwork_common::dataplane_client::Record;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Inject the writer and Kameo read pool into every handler.
pub fn router(
    shard: ActorRef<WriteShard>,
    readers: ReadShardPool,
    wal_path: &std::path::Path,
) -> anyhow::Result<Router> {
    router_with_writer(
        shard,
        readers,
        ReplicaWriter::spawn(ReplicaWriter::default()),
        wal_path,
    )
}

pub fn router_with_writer(
    shard: ActorRef<WriteShard>,
    readers: ReadShardPool,
    writer: ActorRef<ReplicaWriter>,
    wal_path: &std::path::Path,
) -> anyhow::Result<Router> {
    let handler = WriteHandler::spawn(WriteHandler::new(shard.clone(), writer, wal_path)?);
    Ok(router_with_handler(shard, readers, handler))
}

pub fn router_with_handler(
    shard: ActorRef<WriteShard>,
    readers: ReadShardPool,
    handler: ActorRef<WriteHandler>,
) -> Router {
    let grpc = crate::grpc::router_with_handler(shard.clone(), readers.clone(), handler.clone());
    Router::new()
        .route("/shard/size", get(get_size))
        .route("/shard/telemetry", get(get_telemetry))
        .route("/tables/{table_id}/telemetry", get(get_telemetry_for_table))
        .route(
            "/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}/with-replicas-inconsistent",
            axum::routing::put(put_with_replicas_inconsistent),
        )
        .route("/tables/{table_id}", axum::routing::delete(delete_table))
        .route(
            "/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}/with-replicas",
            axum::routing::put(put_with_replicas),
        )
        .route(
            "/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}",
            get(get_record).delete(delete_record).put(upsert),
        )
        .route(
            "/tables/{table_id}/partition-keys/{partition_key}",
            get(get_partition_key).delete(delete_partition_key),
        )
        .route(
            "/tables/{table_id}/range",
            get(get_range).delete(delete_range),
        )
        .route("/tables/{table_id}/range/count", get(count_range))
        .route("/tables/{table_id}/range/size", get(get_range_size))
        .layer(Extension(readers))
        .layer(Extension(handler))
        .with_state(shard)
        .merge(grpc)
}

type ApiError = (StatusCode, Json<serde_json::Value>);

fn internal_error(error: impl std::fmt::Display) -> ApiError {
    eprintln!("Shard request failed: {error}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": "shard request failed"})),
    )
}

fn write_timestamp(headers: &HeaderMap) -> Result<i64, ApiError> {
    headers.get("x-patchwork-timestamp")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "x-patchwork-timestamp is required and must be nonnegative Unix microseconds"
        }))))
}

fn write_deleted(headers: &HeaderMap) -> Result<bool, ApiError> {
    match headers.get("x-patchwork-deleted") {
        None => Ok(false),
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "x-patchwork-deleted must be true or false"})),
                )
            }),
    }
}

#[derive(Default, Deserialize)]
struct ReadOptions {
    #[serde(default)]
    include_deleted: bool,
}

async fn upsert(
    Extension(handler): Extension<ActorRef<WriteHandler>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
    Json(data): Json<serde_json::Value>,
) -> Result<StatusCode, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = write_deleted(&headers)?;
    handler
        .ask(write_handler::WriteRecord {
            record: Record {
                table_id,
                partition_key,
                secondary_key,
                data,
                timestamp,
                deleted,
            },
            replicas: Vec::new(),
            consistent: false,
        })
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct PutWithReplicas {
    data: serde_json::Value,
    replicas: Vec<String>,
}

async fn put_with_replicas(
    Extension(handler): Extension<ActorRef<WriteHandler>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
    Json(request): Json<PutWithReplicas>,
) -> Result<StatusCode, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = write_deleted(&headers)?;
    handler
        .ask(write_handler::WriteRecord {
            record: Record {
                table_id,
                partition_key,
                secondary_key,
                data: request.data,
                timestamp,
                deleted,
            },
            replicas: request.replicas,
            consistent: true,
        })
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn put_with_replicas_inconsistent(
    Extension(handler): Extension<ActorRef<WriteHandler>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
    Json(request): Json<PutWithReplicas>,
) -> Result<StatusCode, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = write_deleted(&headers)?;
    if request.replicas.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "at least one remote replica is required"})),
        ));
    }
    handler
        .ask(write_handler::WriteRecord {
            record: Record {
                table_id,
                partition_key,
                secondary_key,
                data: request.data,
                timestamp,
                deleted,
            },
            replicas: request.replicas,
            consistent: false,
        })
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_record(
    Extension(readers): Extension<ReadShardPool>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<data::Model>, ApiError> {
    readers
        .ask(Dispatch(Get {
            table_id,
            partition_key,
            secondary_key,
            include_deleted: options.include_deleted,
        }))
        .await
        .map_err(internal_error)?
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "record not found"})),
            )
        })
}

#[derive(Serialize)]
struct Deleted {
    deleted: u64,
}

async fn delete_record(
    Extension(handler): Extension<ActorRef<WriteHandler>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
) -> Result<Json<Deleted>, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = handler
        .ask(write_handler::Delete {
            table_id,
            partition_key,
            secondary_key,
            timestamp,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}

async fn get_partition_key(
    Extension(readers): Extension<ReadShardPool>,
    Path((table_id, partition_key)): Path<(i64, i64)>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Vec<data::Model>>, ApiError> {
    Ok(Json(
        readers
            .ask(Dispatch(GetPartitionKey {
                table_id,
                partition_key,
                include_deleted: options.include_deleted,
            }))
            .await
            .map_err(internal_error)?,
    ))
}

async fn delete_partition_key(
    Extension(handler): Extension<ActorRef<WriteHandler>>,
    Path((table_id, partition_key)): Path<(i64, i64)>,
) -> Result<Json<Deleted>, ApiError> {
    let deleted = handler
        .ask(write_handler::DeletePartitionKey {
            table_id,
            partition_key,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}

#[derive(Deserialize)]
struct Range {
    #[serde(default)]
    include_deleted: bool,
    lower_bound: i64,
    upper_bound: i64,
}

async fn get_range(
    Extension(readers): Extension<ReadShardPool>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<Vec<data::Model>>, ApiError> {
    if range.lower_bound >= range.upper_bound {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "lower_bound must be less than upper_bound"})),
        ));
    }
    Ok(Json(
        readers
            .ask(Dispatch(GetRange {
                table_id,
                include_deleted: range.include_deleted,
                lower_bound: range.lower_bound,
                upper_bound: range.upper_bound,
            }))
            .await
            .map_err(internal_error)?,
    ))
}

async fn delete_range(
    Extension(handler): Extension<ActorRef<WriteHandler>>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<Deleted>, ApiError> {
    if range.lower_bound >= range.upper_bound {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "lower_bound must be less than upper_bound"})),
        ));
    }
    let deleted = handler
        .ask(write_handler::DeleteRange {
            table_id,
            lower_bound: range.lower_bound,
            upper_bound: range.upper_bound,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}

async fn get_size(
    State(shard): State<ActorRef<WriteShard>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let size_bytes = shard.ask(GetSize {}).await.map_err(internal_error)?;
    Ok(Json(serde_json::json!({ "size_bytes": size_bytes })))
}

async fn get_telemetry(
    State(shard): State<ActorRef<WriteShard>>,
) -> Result<Json<Vec<telemetry::Model>>, ApiError> {
    Ok(Json(
        shard.ask(GetTelemetry {}).await.map_err(internal_error)?,
    ))
}

async fn get_telemetry_for_table(
    State(shard): State<ActorRef<WriteShard>>,
    Path(table_id): Path<i64>,
) -> Result<Json<HashMap<i64, telemetry::Model>>, ApiError> {
    Ok(Json(
        shard
            .ask(GetTelemetryForTable { table_id })
            .await
            .map_err(internal_error)?,
    ))
}

async fn count_range(
    Extension(readers): Extension<ReadShardPool>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if range.lower_bound >= range.upper_bound {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "lower_bound must be less than upper_bound"})),
        ));
    }
    let total = readers
        .ask(Dispatch(CountRange {
            table_id,
            lower_bound: range.lower_bound,
            upper_bound: range.upper_bound,
        }))
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({"count": total})))
}

async fn get_range_size(
    Extension(readers): Extension<ReadShardPool>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if range.lower_bound >= range.upper_bound {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "lower_bound must be less than upper_bound"})),
        ));
    }
    let total = readers
        .ask(Dispatch(GetRangeSize {
            table_id,
            lower_bound: range.lower_bound,
            upper_bound: range.upper_bound,
        }))
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({"size_bytes": total})))
}

async fn delete_table(
    Extension(handler): Extension<ActorRef<WriteHandler>>,
    Path(table_id): Path<i64>,
) -> Result<Json<Deleted>, ApiError> {
    let deleted = handler
        .ask(write_handler::DeleteTable { table_id })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}
