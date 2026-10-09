use crate::workflows::stages::{RunState, task_error};
use cano::{
    CancellationToken, CanoError, Resources, TaskConfig, TaskResult, Workflow as CanoWorkflow,
};
use std::collections::HashMap;
use tokio::sync::Mutex;

use anyhow::{Context, Result};
use kameo::actor::ActorRef;

use crate::{
    dataplane_client::{self, AccessStatistics},
    db::{NodeActor, PartitionActor, TableActor, actor::Get, partition::GetTablePartitions},
    workflows::executor::LongRunningWorkflowExecutor,
};

const HOT_WRITE_PARTITION: f64 = 200.0;
const HOT_READ_PARTITION: f64 = 1000.0;
const COLD_WRITE_PARTITION: f64 = 10.0;
const COLD_READ_PARTITION: f64 = 50.0;

#[derive(Clone)]
pub struct CheckTableScaleContext {
    pub table_id: i64,
}

#[derive(Clone)]
pub struct CheckTableScale {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
    pub orchestrator: ActorRef<LongRunningWorkflowExecutor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    FetchPartitions,
    CollectTelemetry,
    Decide,
    Done,
}

struct ScaleCheck {
    workflow: CheckTableScale,
    ctx: CheckTableScaleContext,
    partitions: Vec<crate::db::partition::Model>,
    global_telemetry: HashMap<i64, AccessStatistics>,
}

#[cano::task]
impl cano::Task<State> for State {
    fn config(&self) -> TaskConfig {
        TaskConfig::minimal()
    }

    async fn run(
        &self,
        resources: &Resources,
    ) -> std::result::Result<TaskResult<State>, CanoError> {
        let check = resources.get::<RunState<ScaleCheck>, _>("check")?;
        let mut check = check.0.lock().await;
        let next = match self {
            Self::FetchPartitions => check.fetch_partitions().await,
            Self::CollectTelemetry => check.collect_telemetry().await,
            Self::Decide => check.decide(),
            Self::Done => Ok(State::Done),
        }
        .map_err(task_error)?;
        Ok(TaskResult::Single(next))
    }
}

impl CheckTableScale {
    pub async fn run(&self, ctx: CheckTableScaleContext) -> Result<()> {
        let workflow = CanoWorkflow::new(Resources::new().insert(
            "check",
            RunState(Mutex::new(ScaleCheck {
                workflow: self.clone(),
                ctx,
                partitions: Vec::new(),
                global_telemetry: HashMap::new(),
            })),
        ))
        .register(State::FetchPartitions, State::FetchPartitions)
        .register(State::CollectTelemetry, State::CollectTelemetry)
        .register(State::Decide, State::Decide)
        .add_exit_state(State::Done);
        workflow
            .orchestrate(State::FetchPartitions, CancellationToken::disabled())
            .await?;
        Ok(())
    }
}

impl ScaleCheck {
    async fn fetch_partitions(&mut self) -> Result<State> {
        let partitions = self
            .workflow
            .partition
            .ask(GetTablePartitions {
                table_id: self.ctx.table_id,
            })
            .await?;

        self.partitions = partitions;
        Ok(State::CollectTelemetry)
    }
    async fn collect_telemetry(&mut self) -> Result<State> {
        let global_telemetry = &mut self.global_telemetry;

        let mut nodes_for_table: Vec<i64> = self
            .partitions
            .iter()
            .flat_map(|p| p.replicas.0.iter().copied())
            .collect();
        nodes_for_table.sort_unstable();
        nodes_for_table.dedup();

        for node in nodes_for_table {
            let node = self
                .workflow
                .node
                .ask(Get { id: node })
                .await?
                .context("Replica node no longer exists; table cleanup is incomplete")?;
            let telemetry =
                dataplane_client::Client::get_telemetry_for_table(&node.url, self.ctx.table_id)
                    .await?;
            for (p, t) in telemetry {
                let entry = global_telemetry.entry(p).or_default();
                entry.merge(&t.statistics);
            }
        }
        Ok(State::Decide)
    }
    fn decide(&mut self) -> Result<State> {
        let partition_lookup_map: HashMap<i64, usize> = self
            .partitions
            .iter()
            .enumerate()
            .map(|(idx, p)| (p.hash_start, idx))
            .collect();

        let mut partitions_to_split: Vec<i64> = Vec::new();
        let mut partitions_to_merge: Vec<i64> = Vec::new();
        let mut partitions_to_replicate: Vec<i64> = Vec::new();
        let mut partitions_to_consolidate: Vec<i64> = Vec::new();

        for (partition_start, stats) in self.global_telemetry.drain() {
            let reads_per_second_per_node =
                stats.reads_per_second / stats.contributing_nodes as f64;
            let writes_per_second_per_node =
                stats.writes_per_second / stats.contributing_nodes as f64;

            if reads_per_second_per_node > HOT_READ_PARTITION {
                partitions_to_replicate.push(partition_start);
            } else if reads_per_second_per_node < COLD_READ_PARTITION {
                partitions_to_consolidate.push(partition_start);
            }

            if writes_per_second_per_node > HOT_WRITE_PARTITION {
                partitions_to_split.push(partition_start);
            } else if writes_per_second_per_node < COLD_READ_PARTITION {
                partitions_to_merge.push(partition_start);
            }
        }

        // Queue replication + splits immediately.
        for p in partitions_to_replicate {}
        Ok(State::Done)
    }
}

impl crate::workflows::Workflow for CheckTableScale {
    type Context = CheckTableScaleContext;
    type Output = ();

    fn key(&self, ctx: &Self::Context) -> String {
        format!("check_table_scale:{}", ctx.table_id)
    }

    fn table_id(&self, ctx: &Self::Context) -> Option<i64> {
        Some(ctx.table_id)
    }

    async fn run(self, ctx: Self::Context) -> Result<Self::Output> {
        CheckTableScale::run(&self, ctx).await
    }
}
