use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    middleware,
    response::Response,
    routing::{delete, get, post},
};
use kameo::actor::ActorRef;
use serde::Deserialize;

use crate::{
    actors::{NodeActor, PartitionActor, TableActor, crud::GetCached},
    auth::{Auth, Principal},
    datamodels::table,
    dataplane_client::Client,
    encryption_client::EncryptionClient,
    permission_client::{Action, PermissionClient, Resource},
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
    client: Client,
    permissions: PermissionClient,
    encryption: EncryptionClient,
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
    auth: Auth,
) -> Router {
    let router = Router::new()
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
        .route("/tables/{table_id}/range", get(get_range));
    #[cfg(feature = "test-api")]
    let router = router.route("/test/tables/{table_id}/split", post(split_partition));
    router
        .route_layer(middleware::from_fn_with_state(
            Arc::new(auth),
            crate::auth::authenticate,
        ))
        .with_state(AppState {
            client: Client::new(),
            permissions: PermissionClient,
            encryption: EncryptionClient,
            node,
            partition,
            table,
            executor,
            telemetry: Arc::new(crate::telemetry::Timings::default()),
        })
}

async fn operation_telemetry(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&state, &principal, Action::ReadTelemetry, Resource::Cluster).await?;
    Ok(Json(state.telemetry.snapshot()))
}

async fn reset_telemetry(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<StatusCode, ApiError> {
    authorize(
        &state,
        &principal,
        Action::ResetTelemetry,
        Resource::Cluster,
    )
    .await?;
    state.telemetry.reset();
    Ok(StatusCode::NO_CONTENT)
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

fn write_error(error: anyhow::Error) -> ApiError {
    if let Some(status) = error
        .downcast_ref::<tonic::Status>()
        .filter(|status| status.code() == tonic::Code::ResourceExhausted)
    {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, status.message());
    }
    internal_error(error)
}

async fn authorize(
    state: &AppState,
    principal: &Principal,
    action: Action,
    resource: Resource,
) -> Result<(), ApiError> {
    state
        .permissions
        .check(principal, action, resource)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "permission service is not configured",
            )
        })
}

async fn secret_key_id(state: &AppState, table_id: i64) -> Result<Option<i64>, ApiError> {
    state
        .table
        .ask(GetCached { id: table_id })
        .await
        .map_err(internal_error)?
        .map(|table| table.secret_key_id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "table not found"))
}

async fn record_json(
    state: &AppState,
    key_id: Option<i64>,
    record: crate::dataplane_client::Record,
) -> Result<serde_json::Value, ApiError> {
    let data = state
        .encryption
        .decrypt(key_id, record.data)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "encryption service is not configured",
            )
        })?;
    let data: serde_json::Value = serde_json::from_slice(&data).map_err(internal_error)?;
    Ok(serde_json::json!({
        "table_id": record.table_id,
        "partition_key": record.partition_key,
        "secondary_key": record.secondary_key,
        "data": data,
        "timestamp": record.timestamp,
        "deleted": record.deleted,
    }))
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
    secret_key_id: Option<i64>,
}

async fn make_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<MakeTableRequest>,
) -> Result<(StatusCode, Json<table::Model>), ApiError> {
    authorize(
        &state,
        &principal,
        Action::CreateTable,
        Resource::Account(request.table_owner),
    )
    .await?;
    if request.secret_key_id.is_some_and(|id| id <= 0) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "secret_key_id must be positive",
        ));
    }
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
            secret_key_id: request.secret_key_id,
        },
    )
    .await?;
    Ok((StatusCode::CREATED, Json((*table).clone())))
}

async fn delete_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(table_id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    authorize(
        &state,
        &principal,
        Action::DeleteTable,
        Resource::Table(table_id),
    )
    .await?;
    submit(
        &state,
        DeleteTable {
            client: state.client.clone(),
            node: state.node.clone(),
            partition: state.partition.clone(),
            table: state.table.clone(),
        },
        DeleteTableContext { table_id },
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(feature = "test-api")]
#[derive(Deserialize)]
struct SplitPartitionRequest {
    new_partition: i64,
}

#[cfg(feature = "test-api")]
async fn split_partition(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(table_id): Path<i64>,
    Json(request): Json<SplitPartitionRequest>,
) -> Result<StatusCode, ApiError> {
    use crate::workflows::partition::split_partition::{SplitPartition, SplitPartitionContext};

    authorize(
        &state,
        &principal,
        Action::SplitPartition,
        Resource::Table(table_id),
    )
    .await?;
    let table = state
        .table
        .ask(GetCached { id: table_id })
        .await
        .map_err(internal_error)?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "table not found"))?;
    if !table.is_ready {
        return Err(api_error(StatusCode::CONFLICT, "table is not ready"));
    }
    submit(
        &state,
        SplitPartition {
            client: state.client.clone(),
            node: state.node.clone(),
            partition: state.partition.clone(),
            table: state.table.clone(),
        },
        SplitPartitionContext {
            table_id,
            new_partition: request.new_partition,
        },
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
    Extension(principal): Extension<Principal>,
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
                authorize(&state, &principal, Action::Read, Resource::Table(table_id)).await?;
                let key_id = secret_key_id(&state, table_id).await?;
                let record = if options.consistent {
                    GetOneConsistent {
                        client: state.client.clone(),
                        node: state.node.clone(),
                        partition: state.partition.clone(),
                        table: state.table.clone(),
                    }
                    .run(GetOneConsistentCtx {
                        table_id,
                        primary_key,
                        secondary_key,
                    })
                    .await
                } else {
                    GetOneInconsistent {
                        client: state.client.clone(),
                        node: state.node.clone(),
                        partition: state.partition.clone(),
                        table: state.table.clone(),
                    }
                    .run(GetOneInconsistentCtx {
                        table_id,
                        primary_key,
                        secondary_key,
                    })
                    .await
                }
                .map_err(internal_error)?;
                let record =
                    record.ok_or_else(|| api_error(StatusCode::NOT_FOUND, "record not found"))?;
                record_json(&state, key_id, record).await.map(Json)
            },
        )
        .await
}

async fn put_record(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
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
                authorize(&state, &principal, Action::Write, Resource::Table(table_id)).await?;
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
                let key_id = secret_key_id(&state, table_id).await?;
                let data = state
                    .encryption
                    .encrypt(key_id, serde_json::to_vec(&data).map_err(internal_error)?)
                    .await
                    .map_err(|_| {
                        api_error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "encryption service is not configured",
                        )
                    })?;
                if options.consistent {
                    PutOneConsistent {
                        client: state.client.clone(),
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
                        client: state.client.clone(),
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
                .map_err(write_error)?;
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
    Extension(principal): Extension<Principal>,
    Path(table_id): Path<i64>,
    Query(range): Query<Range>,
) -> Response {
    let timings = state.telemetry.clone();
    timings
        .run("fetch_range", range.telemetry, async move {
            authorize(&state, &principal, Action::Read, Resource::Table(table_id)).await?;
            let key_id = secret_key_id(&state, table_id).await?;
            if range.lower_bound > range.upper_bound {
                return Err(api_error(
                    StatusCode::BAD_REQUEST,
                    "range bounds are reversed",
                ));
            }
            let records = FetchRange {
                client: state.client.clone(),
                node: state.node.clone(),
                partition: state.partition.clone(),
                table: state.table.clone(),
            }
            .run(FetchRangeCtx {
                table_id,
                lower_bound: range.lower_bound,
                upper_bound: range.upper_bound,
            })
            .await
            .map_err(internal_error)?;
            let mut result = Vec::with_capacity(records.len());
            for record in records {
                result.push(record_json(&state, key_id, record).await?);
            }
            Ok::<_, ApiError>(Json(result))
        })
        .await
}
