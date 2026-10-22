use sea_orm::FromJsonQueryResult;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "partition")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub hash_start: i64,

    /// Stable identity, independent of boundaries and the metadata storage backend.
    #[sea_orm(unique, column_type = "String(StringLen::N(32))")]
    pub partition_id: String,

    /// Another partition's hash_start in this table. Reads use its replicas;
    /// writes use both replica lists until copying finishes.
    pub forward_to: Option<i64>,

    #[sea_orm(column_type = "Json")]
    pub replicas: ReplicaNodes,
}

/// All nodes holding this partition, with no designated primary.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ReplicaNodes(pub Vec<i64>);

impl Model {
    pub fn replication_factor(&self) -> usize {
        self.replicas.0.len()
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
