use super::NodeActor;
use kameo::{
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::entity::prelude::*;
use sea_orm::{QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "node")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub node_id: i64,
    pub url: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

/// Selects one node outside the existing replica set.
pub struct GetNodeExcluding {
    pub node_ids: Vec<i64>,
}

impl Message<GetNodeExcluding> for NodeActor {
    type Reply = DelegatedReply<Result<Option<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetNodeExcluding,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move {
            Entity::find()
                .filter(Column::NodeId.is_not_in(msg.node_ids))
                .order_by_asc(Column::NodeId)
                .one(&db)
                .await
        })
    }
}
