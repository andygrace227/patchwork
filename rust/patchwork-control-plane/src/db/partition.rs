use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "partition")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub table_id: i64,
    #[sea_orm(primary_key)]
    pub hash_start: i64,
    pub node_id: i64,
    pub forward_to: i64

}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

