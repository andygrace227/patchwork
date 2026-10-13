use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{delete, get, post},
};
use kameo::actor::ActorRef;
use serde::Deserialize;

use crate::{
    db::{NodeActor, PartitionActor, TableActor, table},
    workflows::{
        Workflow,
        data::{
            fetch_one_consistent::{GetOneConsistent, GetOneConsistentCtx},
            fetch_one_inconsistent::{GetOneInconsistent, GetOneInconsistentCtx},
            fetch_range::{FetchRange, FetchRangeCtx},
            put_one_consistent::{PutOneConsistent, PutOneConsistentCtx},
            put_one_inconsistent::{PutOneInconsistent, PutOneInconsistentCtx},
        },
        executor::{LongRunningWorkflowExecutor, SubmitWorkflow},
        table::{
            delete_table::{DeleteTable, DeleteTableContext},
            make_table::{MakeTable, MakeTableContext},
        },
    },
};

#[derive(Clone)]
struct AppState {
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
    table: ActorRef<TableActor>,
    executor: ActorRef<LongRunningWorkflowExecutor>,
    telemetry: Arc<crate::telemetry::Timings>,
}

pub fn router(
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
    table: ActorRef<TableActor>,
    executor: ActorRef<LongRunningWorkflowExecutor>,
) -> Router {
    Router::new()
        .route(
            "/telemetry",
            get(operation_telemetry).delete(reset_telemetry),
        )
        .route("/tables", post(make_table))
        .route("/tables/{table_id}", delete(delete_table))
        .route(
            "/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}",
            get(get_record).put(put_record),
        )
        .route("/tables/{table_id}/range", get(get_range))
        .with_state(AppState {
            node,
            partition,
            table,
            executor,
            telemetry: Arc::new(crate::telemetry::Timings::default()),
        })
}

async fn operation_telemetry(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(state.telemetry.snapshot())
}

async fn reset_telemetry(State(state): State<AppState>) -> StatusCode {
    state.telemetry.reset();
    StatusCode::NO_CONTENT
}

type ApiError = (StatusCode, Json<serde_json::Value>);

fn api_error(status: StatusCode, message: &str) -> ApiError {
    (status, Json(serde_json::json!({"error": message})))
}

fn internal_error(error: impl std::fmt::Display) -> ApiError {
    eprintln!("Control plane request failed: {error}");
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "control plane request failed",
    )
}

// Keep table changes in the executor's queue and reply only after completion.
async fn submit<W: Workflow>(
    state: &AppState,
    workflow: W,
    context: W::Context,
) -> Result<Arc<W::Output>, ApiError> {
    let job = state
        .executor
        .ask(SubmitWorkflow::new(workflow, context))
        .await
        .map_err(|error| {
            eprintln!("Could not submit workflow: {error}");
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "workflow executor unavailable",
            )
        })?
        .ok_or_else(|| api_error(StatusCode::CONFLICT, "workflow already queued or running"))?;
    job.result().await.map_err(internal_error)
}

#[derive(Deserialize)]
struct MakeTableRequest {
    table_name: String,
    table_owner: i64,
    partition_key_name: String,
    sort_key_name: String,
}

async fn make_table(
    State(state): State<AppState>,
    Json(request): Json<MakeTableRequest>,
) -> Result<(StatusCode, Json<table::Model>), ApiError> {
    if [
        &request.table_name,
        &request.partition_key_name,
        &request.sort_key_name,
    ]
    .iter()
    .any(|name| name.trim().is_empty())
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "table and key names must not be empty",
        ));
    }
    let table = submit(
        &state,
        MakeTable {
            node: state.node.clone(),
            partition: state.partition.clone(),
            table: state.table.clone(),
        },
        MakeTableContext {
            table_name: request.table_name,
            table_owner: request.table_owner,
            partition_key_name: request.partition_key_name,
            sort_key_name: request.sort_key_name,
        },
    )
    .await?;
    Ok((StatusCode::CREATED, Json((*table).clone())))
}

async fn delete_table(
    State(state): State<AppState>,
    Path(table_id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    submit(
        &state,
        DeleteTable {
            node: state.node.clone(),
            partition: state.partition.clone(),
            table: state.table.clone(),
        },
        DeleteTableContext { table_id },
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct RecordOptions {
    #[serde(default = "consistent_by_default")]
    consistent: bool,
    #[serde(default)]
    telemetry: bool,
}

fn consistent_by_default() -> bool {
    true
}

async fn get_record(
    State(state): State<AppState>,
    Path((table_id, primary_key, secondary_key)): Path<(i64, i64, i64)>,
    Query(options): Query<RecordOptions>,
) -> Response {
    let timings = state.telemetry.clone();
    timings
        .run(
            if options.consistent {
                "fetch_consistent"
            } else {
                "fetch_inconsistent"
            },
            options.telemetry,
            async move {
                let record = if options.consistent {
                    GetOneConsistent {
                        node: state.node,
                        partition: state.partition,
                        table: state.table,
                    }
                    .run(GetOneConsistentCtx {
                        table_id,
                        primary_key,
                        secondary_key,
                    })
                    .await
                } else {
                    GetOneInconsistent {
                        node: state.node,
                        partition: state.partition,
                        table: state.table,
                    }
                    .run(GetOneInconsistentCtx {
                        table_id,
                        primary_key,
                        secondary_key,
                    })
                    .await
                }
                .map_err(internal_error)?;
                record
                    .map(Json)
                    .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "record not found"))
            },
        )
        .await
}

async fn put_record(
    State(state): State<AppState>,
    Path((table_id, primary_key, secondary_key)): Path<(i64, i64, i64)>,
    Query(options): Query<RecordOptions>,
    headers: HeaderMap,
    Json(data): Json<serde_json::Value>,
) -> Response {
    let timings = state.telemetry.clone();
    timings
        .run(
            if options.consistent {
                "put_consistent"
            } else {
                "put_inconsistent"
            },
            options.telemetry,
            async move {
                let timestamp = headers
        .get("x-patchwork-timestamp")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(|| {
            api_error(
                StatusCode::BAD_REQUEST,
                "x-patchwork-timestamp is required and must be nonnegative Unix microseconds",
            )
        })?;
                if options.consistent {
                    PutOneConsistent {
                        node: state.node,
                        partition: state.partition,
                        table: state.table,
                    }
                    .run(PutOneConsistentCtx {
                        table_id,
                        primary_key,
                        secondary_key,
                        data,
                        timestamp,
                    })
                    .await
                } else {
                    PutOneInconsistent {
                        node: state.node,
                        partition: state.partition,
                        table: state.table,
                    }
                    .run(PutOneInconsistentCtx {
                        table_id,
                        primary_key,
                        secondary_key,
                        data,
                        timestamp,
                    })
                    .await
                }
                .map_err(internal_error)?;
                Ok::<_, ApiError>(StatusCode::NO_CONTENT)
            },
        )
        .await
}

#[derive(Deserialize)]
struct Range {
    lower_bound: i64,
    upper_bound: i64,
    #[serde(default)]
    telemetry: bool,
}

async fn get_range(
    State(state): State<AppState>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Response {
    let timings = state.telemetry.clone();
    timings
        .run("fetch_range", range.telemetry, async move {
            if range.lower_bound > range.upper_bound {
                return Err(api_error(
                    StatusCode::BAD_REQUEST,
                    "range bounds are reversed",
                ));
            }
            FetchRange {
                node: state.node,
                partition: state.partition,
                table: state.table,
            }
            .run(FetchRangeCtx {
                table_id,
                lower_bound: range.lower_bound,
                upper_bound: range.upper_bound,
            })
            .await
            .map(Json)
            .map_err(internal_error)
        })
        .await
}
