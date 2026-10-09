use kameo::{
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::entity::prelude::*;
use sea_orm::{DbBackend, Order, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

use super::TableActor;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "table")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub table_id: i64,
    pub table_name: String,
    pub owner: i64,
    #[sea_orm(default_value = 0)]
    pub owner_control_node: i64,
    #[sea_orm(default_value = 0)]
    pub backup_control_node: i64,
    pub partition_key_name: String,
    pub sort_key_name: String,
    pub is_ready: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

/// Pick a ready table for a background scaling check.
pub struct GetRandomReadyTable;

impl Message<GetRandomReadyTable> for TableActor {
    type Reply = DelegatedReply<Result<Option<Model>, DbErr>>;

    async fn handle(
        &mut self,
        _: GetRandomReadyTable,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move {
            let random = match db.get_database_backend() {
                DbBackend::MySql => "RAND()",
                _ => "RANDOM()",
            };
            Entity::find()
                .filter(Column::IsReady.eq(true))
                .order_by(Expr::cust(random), Order::Asc)
                .one(&db)
                .await
        })
    }
}
