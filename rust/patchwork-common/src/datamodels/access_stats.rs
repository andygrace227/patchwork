use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "access_statistics")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub table_id: i64,
    pub hash_start: i64,
    pub start_ms: i64,
    pub end_ms: i64,
    pub reads_per_second: f64,
    pub writes_per_second: f64,
    pub contributing_nodes: i64,
    pub average_write_position: Option<i64>,
    // Upper middle key for an even number of writes; always an observed key.
    pub median_write_position: Option<i64>,
    pub read_count: i64,
    pub write_count: i64,
    // SQLite integers cannot hold an i128. Keep the exact sum as decimal text.
    pub write_position_sum: String,
    pub window_seconds: f64,
}

impl Model {
    pub fn into_telemetry(
        self,
    ) -> Result<crate::dataplane_client::Telemetry, std::num::ParseIntError> {
        Ok(crate::dataplane_client::Telemetry {
            table_id: self.table_id,
            partition_key: self.hash_start,
            statistics: crate::dataplane_client::AccessStatistics {
                reads_per_second: self.reads_per_second,
                writes_per_second: self.writes_per_second,
                contributing_nodes: self.contributing_nodes as u64,
                average_write_position: self.average_write_position,
                median_write_position: self.median_write_position,
                read_count: self.read_count as u64,
                write_count: self.write_count as u64,
                write_position_sum: self.write_position_sum.parse()?,
                window_seconds: self.window_seconds,
            },
        })
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
