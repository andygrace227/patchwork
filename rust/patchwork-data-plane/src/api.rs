use crate::{
    data,
    shard_actor::{
        Delete, DeletePartition, Get, GetPartition, GetRange, GetSize, ShardActor, Upsert,
    },
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use kameo::actor::ActorRef;
use serde::{Deserialize, Serialize};

/// Inject the same actor into every handler.
pub fn router(shard: ActorRef<ShardActor>) -> Router {
    Router::new()
        .route("/shard/size", get(get_size))
        .route(
            "/tables/{table_id}/partitions/{partition}/records/{hash}",
            get(get_record).delete(delete_record).put(upsert),
        )
        .route(
            "/tables/{table_id}/partitions/{partition}",
            get(get_partition).delete(delete_partition),
        )
        .route("/tables/{table_id}/range", get(get_range))
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

async fn upsert(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition, hash)): Path<(i64, i64, i64)>,
    Json(data): Json<serde_json::Value>,
) -> Result<StatusCode, ApiError> {
    shard
        .ask(Upsert {
            table_id: table_id,
            partition: partition,
            hash: hash,
            data: data,
        })
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_record(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition, hash)): Path<(i64, i64, i64)>,
) -> Result<Json<data::Model>, ApiError> {
    shard
        .ask(Get {
            table_id,
            partition,
            hash,
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
    Path((table_id, partition, hash)): Path<(i64, i64, i64)>,
) -> Result<Json<Deleted>, ApiError> {
    let deleted = shard
        .ask(Delete {
            table_id,
            partition,
            hash,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}

async fn get_partition(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition)): Path<(i64, i64)>,
) -> Result<Json<Vec<data::Model>>, ApiError> {
    Ok(Json(
        shard
            .ask(GetPartition {
                table_id,
                partition,
            })
            .await
            .map_err(internal_error)?,
    ))
}

async fn delete_partition(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition)): Path<(i64, i64)>,
) -> Result<Json<Deleted>, ApiError> {
    let deleted = shard
        .ask(DeletePartition {
            table_id,
            partition,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}

#[derive(Deserialize)]
struct Range {
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
                lower_bound: range.lower_bound,
                upper_bound: range.upper_bound,
            })
            .await
            .map_err(internal_error)?,
    ))
}

async fn get_size(
    State(shard): State<ActorRef<ShardActor>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let size_bytes = shard.ask(GetSize {}).await.map_err(internal_error)?;
    Ok(Json(serde_json::json!({ "size_bytes": size_bytes })))
}
