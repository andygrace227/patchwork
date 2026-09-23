use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "table")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub table_id: i64,
    pub table_name: i64,
    pub owner: i64,
    pub partition_key_name: String,
    pub sort_key_name: String
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
