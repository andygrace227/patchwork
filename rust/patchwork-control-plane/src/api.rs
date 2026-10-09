use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use kameo::actor::ActorRef;
use serde::Deserialize;

use crate::{
    dataplane_client::Record,
    db::{NodeActor, PartitionActor, TableActor, table},
    workflows::{
        Workflow,
        data::{
            fetch_one_consistent::{GetOneConsistent, GetOneConsistentCtx},
            fetch_one_inconsistent::{GetOneInconsistent, GetOneInconsistentCtx},
            fetch_range::{FetchRange, FetchRangeCtx},
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
}

pub fn router(
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
    table: ActorRef<TableActor>,
    executor: ActorRef<LongRunningWorkflowExecutor>,
) -> Router {
    Router::new()
        .route("/tables", post(make_table))
        .route("/tables/{table_id}", delete(delete_table))
        .route(
            "/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}",
            get(get_record),
        )
        .route("/tables/{table_id}/range", get(get_range))
        .with_state(AppState {
            node,
            partition,
            table,
            executor,
        })
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
struct ReadOptions {
    #[serde(default = "consistent_by_default")]
    consistent: bool,
}

fn consistent_by_default() -> bool {
    true
}

async fn get_record(
    State(state): State<AppState>,
    Path((table_id, primary_key, secondary_key)): Path<(i64, i64, i64)>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Record>, ApiError> {
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
}

#[derive(Deserialize)]
struct Range {
    lower_bound: i64,
    upper_bound: i64,
}

async fn get_range(
    State(state): State<AppState>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Result<Json<Vec<Record>>, ApiError> {
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
}
