use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "data")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub partition: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub hash: i64,
    pub data: Json,
    pub version: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
