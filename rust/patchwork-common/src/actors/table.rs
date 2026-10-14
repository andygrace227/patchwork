use crate::datamodels::table::{Column, Entity, Model};
use kameo::{
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::entity::prelude::*;
use sea_orm::{DbBackend, Order, QueryFilter, QueryOrder};

use super::TableActor;

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
