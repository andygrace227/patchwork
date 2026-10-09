use std::collections::HashMap;

use anyhow::{Context, Result};
use kameo::actor::ActorRef;

use crate::{
    dataplane_client::{self, AccessStatistics},
    db::{NodeActor, PartitionActor, TableActor, actor::Get, partition::GetTablePartitions},
};


const HOT_WRITE_PARTITION : f64 = 200.0;
const HOT_READ_PARTITION : f64 = 1000.0;
const COLD_WRITE_PARTITION : f64 = 10.0;
const COLD_READ_PARTITION : f64 = 50.0;

pub struct CheckTableScaleContext {
    pub table_id: i64,
}

pub struct CheckTableScale {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

impl CheckTableScale {
    pub async fn run(&self, ctx: CheckTableScaleContext) -> Result<()> {
        let partitions = self
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;

        let partition_lookup_map : HashMap<i64, usize> = partitions
            .iter()
            .enumerate()
            .map(
                |(idx, p)| (p.hash_start, idx)
            )
            .collect();

        let mut global_telemetry: HashMap<i64, AccessStatistics> = HashMap::new();

        let mut nodes_for_table: Vec<i64> = partitions
            .iter()
            .flat_map(|p| p.replicas.0.iter().copied())
            .collect();
        nodes_for_table.sort_unstable();
        nodes_for_table.dedup();

        for node in nodes_for_table {
            let node = self
                .node
                .ask(Get { id: node })
                .await?
                .context("Replica node no longer exists; table cleanup is incomplete")?;
            let telemetry =
                dataplane_client::Client::get_telemetry_for_table(&node.url, ctx.table_id).await?;
            for (p, t) in telemetry {
                let entry = global_telemetry.entry(p).or_default();
                entry.merge(&t.statistics);
            }
        }
        let mut partitions_to_split : Vec<i64> = Vec::new();
        let mut partitions_to_merge : Vec<i64> = Vec::new();
        let mut partitions_to_replicate : Vec<i64> = Vec::new();
        let mut partitions_to_consolidate : Vec<i64> = Vec::new();

        for (partition_start, stats) in global_telemetry {

            let reads_per_second_per_node = stats.reads_per_second / stats.contributing_nodes as f64;
            let writes_per_second_per_node = stats.writes_per_second / stats.contributing_nodes as f64;

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
        for p in partitions_to_replicate {
            



        }









        Ok(())
    }
}
