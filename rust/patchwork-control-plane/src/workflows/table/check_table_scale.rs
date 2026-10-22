use crate::clients::{NodeClient, PartitionClient, TableClient};
use crate::workflows::stages::workflow_steps;
use std::collections::HashMap;

use anyhow::{Context, Result};
use kameo::actor::ActorRef;

use crate::{
    dataplane_client::{AccessStatistics, Client},
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
    pub client: Client,
    pub node: NodeClient,
    pub partition: PartitionClient,
    pub table: TableClient,
    pub orchestrator: ActorRef<LongRunningWorkflowExecutor>,
}

struct ScaleCheck {
    workflow: CheckTableScale,
    ctx: CheckTableScaleContext,
    partitions: Vec<crate::datamodels::partition::Model>,
    global_telemetry: HashMap<String, AccessStatistics>,
}

workflow_steps! {
    run: ScaleCheck,
    state: State,
    start: FetchPartitions,
    done: Done,
    cleanup: none,
    FetchPartitions => fetch_partitions,
    CollectTelemetry => collect_telemetry,
    Decide => decide,
}

impl CheckTableScale {
    pub async fn run(&self, ctx: CheckTableScaleContext) -> Result<()> {
        ScaleCheck {
            workflow: self.clone(),
            ctx,
            partitions: Vec::new(),
            global_telemetry: HashMap::new(),
        }
        .execute()
        .await
        .map(|_| ())
    }
}

impl ScaleCheck {
    async fn fetch_partitions(&mut self) -> Result<State> {
        let partitions = self
            .workflow
            .partition
            .get_table_partitions(self.ctx.table_id)
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
                .get_node(node)
                .await?
                .context("Replica node no longer exists; table cleanup is incomplete")?;
            let telemetry = self
                .workflow
                .client
                .get_telemetry_for_table(&node.url, self.ctx.table_id)
                .await?;
            for (p, t) in telemetry {
                let entry = global_telemetry.entry(p).or_default();
                entry.merge(&t.statistics);
            }
        }
        Ok(State::Decide)
    }
    async fn decide(&mut self) -> Result<State> {
        let partition_lookup_map: HashMap<&str, usize> = self
            .partitions
            .iter()
            .enumerate()
            .map(|(idx, p)| (p.partition_id.as_str(), idx))
            .collect();

        let mut partitions_to_split: Vec<i64> = Vec::new();
        let mut partitions_to_merge: Vec<i64> = Vec::new();
        let mut partitions_to_replicate: Vec<i64> = Vec::new();
        let mut partitions_to_consolidate: Vec<i64> = Vec::new();

        for (partition_id, stats) in self.global_telemetry.drain() {
            let Some(&index) = partition_lookup_map.get(partition_id.as_str()) else {
                continue;
            };
            let partition_start = self.partitions[index].hash_start;
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
