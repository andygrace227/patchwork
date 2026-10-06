use crate::{
    data,
    shard_actor::{
        CountRange, Delete, DeletePartitionKey, DeleteRange, DeleteTable, Get, GetPartitionKey,
        GetRange, GetRangeSize, GetSize, ShardActor, WriteRecord,
    },
    subordinate_shard_writer::{Put, SubordinateShardWriter},
};
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use kameo::actor::{ActorRef, Spawn};
use serde::{Deserialize, Serialize};

/// Inject the same actor into every handler.
pub fn router(shard: ActorRef<ShardActor>) -> Router {
    router_with_writer(shard, SubordinateShardWriter::spawn(SubordinateShardWriter))
}

pub fn router_with_writer(
    shard: ActorRef<ShardActor>,
    writer: ActorRef<SubordinateShardWriter>,
) -> Router {
    Router::new()
        .route("/shard/size", get(get_size))
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
        .layer(Extension(writer))
        .with_state(shard)
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
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
    Json(data): Json<serde_json::Value>,
) -> Result<StatusCode, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = write_deleted(&headers)?;
    shard
        .ask(WriteRecord {
            table_id: table_id,
            partition_key: partition_key,
            secondary_key: secondary_key,
            data: data,
            timestamp,
            deleted,
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
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
    Json(request): Json<PutWithReplicas>,
) -> Result<StatusCode, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = write_deleted(&headers)?;
    shard
        .ask(WriteRecord {
            table_id,
            partition_key,
            secondary_key,
            data: request.data.clone(),
            timestamp,
            deleted,
        })
        .await
        .map_err(internal_error)?;
    crate::replicas::put(
        table_id,
        partition_key,
        secondary_key,
        request.data,
        request.replicas,
        timestamp,
        deleted,
    )
    .await
    .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn put_with_replicas_inconsistent(
    State(shard): State<ActorRef<ShardActor>>,
    Extension(writer): Extension<ActorRef<SubordinateShardWriter>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
    Json(request): Json<PutWithReplicas>,
) -> Result<StatusCode, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = write_deleted(&headers)?;
    let Some((first, remaining)) = request.replicas.split_first() else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "at least one remote replica is required"
            })),
        ));
    };
    shard
        .ask(WriteRecord {
            table_id,
            partition_key,
            secondary_key,
            data: request.data.clone(),
            timestamp,
            deleted,
        })
        .await
        .map_err(internal_error)?;
    crate::client::Client::write_record(
        first,
        table_id,
        partition_key,
        secondary_key,
        &request.data,
        timestamp,
        deleted,
    )
    .await
    .map_err(internal_error)?;
    let replicas: Vec<_> = remaining
        .iter()
        .filter(|url| url.trim_end_matches('/') != first.trim_end_matches('/'))
        .cloned()
        .collect();
    if replicas.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }
    writer
        .tell(Put {
            table_id,
            partition_key,
            secondary_key,
            data: request.data,
            replicas,
            timestamp,
            deleted,
        })
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_record(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<data::Model>, ApiError> {
    shard
        .ask(Get {
            table_id,
            partition_key,
            secondary_key,
            include_deleted: options.include_deleted,
        })
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
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    headers: HeaderMap,
) -> Result<Json<Deleted>, ApiError> {
    let timestamp = write_timestamp(&headers)?;
    let deleted = shard
        .ask(Delete {
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
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key)): Path<(i64, i64)>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Vec<data::Model>>, ApiError> {
    Ok(Json(
        shard
            .ask(GetPartitionKey {
                table_id,
                partition_key,
                include_deleted: options.include_deleted,
            })
            .await
            .map_err(internal_error)?,
    ))
}

async fn delete_partition_key(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key)): Path<(i64, i64)>,
) -> Result<Json<Deleted>, ApiError> {
    let deleted = shard
        .ask(DeletePartitionKey {
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
    State(shard): State<ActorRef<ShardActor>>,
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
        shard
            .ask(GetRange {
                table_id,
                include_deleted: range.include_deleted,
                lower_bound: range.lower_bound,
                upper_bound: range.upper_bound,
            })
            .await
            .map_err(internal_error)?,
    ))
}

async fn delete_range(
    State(shard): State<ActorRef<ShardActor>>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<Deleted>, ApiError> {
    if range.lower_bound >= range.upper_bound {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "lower_bound must be less than upper_bound"})),
        ));
    }
    let deleted = shard
        .ask(DeleteRange {
            table_id,
            lower_bound: range.lower_bound,
            upper_bound: range.upper_bound,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}

async fn get_size(
    State(shard): State<ActorRef<ShardActor>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let size_bytes = shard.ask(GetSize {}).await.map_err(internal_error)?;
    Ok(Json(serde_json::json!({ "size_bytes": size_bytes })))
}

async fn count_range(
    State(shard): State<ActorRef<ShardActor>>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if range.lower_bound >= range.upper_bound {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "lower_bound must be less than upper_bound"})),
        ));
    }
    let total = shard
        .ask(CountRange {
            table_id,
            lower_bound: range.lower_bound,
            upper_bound: range.upper_bound,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({"count": total})))
}

async fn get_range_size(
    State(shard): State<ActorRef<ShardActor>>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if range.lower_bound >= range.upper_bound {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "lower_bound must be less than upper_bound"})),
        ));
    }
    let total = shard
        .ask(GetRangeSize {
            table_id,
            lower_bound: range.lower_bound,
            upper_bound: range.upper_bound,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({"size_bytes": total})))
}

async fn delete_table(
    State(shard): State<ActorRef<ShardActor>>,
    Path(table_id): Path<i64>,
) -> Result<Json<Deleted>, ApiError> {
    let deleted = shard
        .ask(DeleteTable { table_id })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}
