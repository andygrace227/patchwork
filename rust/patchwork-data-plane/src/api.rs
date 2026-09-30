use crate::{
    data,
    shard_actor::{
        CountRange, Delete, DeletePartitionKey, DeleteRange, Get, GetPartitionKey, GetRange,
        GetRangeSize, GetSize, ShardActor, Upsert,
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
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
    Json(data): Json<serde_json::Value>,
) -> Result<StatusCode, ApiError> {
    shard
        .ask(Upsert {
            table_id: table_id,
            partition_key: partition_key,
            secondary_key: secondary_key,
            data: data,
        })
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_record(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key, secondary_key)): Path<(i64, i64, i64)>,
) -> Result<Json<data::Model>, ApiError> {
    shard
        .ask(Get {
            table_id,
            partition_key,
            secondary_key,
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
) -> Result<Json<Deleted>, ApiError> {
    let deleted = shard
        .ask(Delete {
            table_id,
            partition_key,
            secondary_key,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(Deleted { deleted }))
}

async fn get_partition_key(
    State(shard): State<ActorRef<ShardActor>>,
    Path((table_id, partition_key)): Path<(i64, i64)>,
) -> Result<Json<Vec<data::Model>>, ApiError> {
    Ok(Json(
        shard
            .ask(GetPartitionKey {
                table_id,
                partition_key,
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
