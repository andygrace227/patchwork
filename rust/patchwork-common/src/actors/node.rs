use crate::datamodels::node::{ActiveModel, Column, Entity, Model};
use kameo::{Actor, message::Context, messages, reply::DelegatedReply};
use sea_orm::entity::prelude::*;
use sea_orm::{QueryFilter, QueryOrder};

#[derive(Actor)]
pub struct NodeActor {
    db: DatabaseConnection,
}

impl NodeActor {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[messages]
impl NodeActor {
    #[message(ctx)]
    pub async fn get_node(
        &mut self,
        id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move { Entity::find_by_id(id).one(&db).await })
    }

    /// Leave the auto-increment primary key NotSet to let the database assign it.
    #[message(ctx)]
    pub async fn create_node(
        &mut self,
        data: ActiveModel,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move { data.insert(&db).await })
    }

    /// Replaces non-key fields. Returns an error if the row does not exist.
    #[message(ctx)]
    pub async fn update_node(
        &mut self,
        data: Model,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            use sea_orm::IntoActiveModel;
            data.into_active_model().reset_all().update(&db).await
        })
    }

    /// Returns the number of rows deleted (zero or one).
    #[message(ctx)]
    pub async fn delete_node(
        &mut self,
        id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<u64, DbErr>>>,
    ) -> DelegatedReply<Result<u64, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            let result = Entity::delete_by_id(id).exec(&db).await?;
            Ok(result.rows_affected)
        })
    }

    /// Selects one node outside the existing replica set.
    #[message(ctx)]
    pub async fn get_node_excluding(
        &mut self,
        node_ids: Vec<i64>,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            Entity::find()
                .filter(Column::NodeId.is_not_in(node_ids))
                .order_by_asc(Column::NodeId)
                .one(&db)
                .await
        })
    }

    /// Pick distinct nodes at random for a new table's initial replicas.
    #[message(ctx)]
    pub async fn get_random_nodes(
        &mut self,
        count: u64,
        ctx: &mut Context<Self, DelegatedReply<Result<Vec<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Vec<Model>, DbErr>> {
        use sea_orm::{DbBackend, Order, QuerySelect, sea_query::Expr};
        let db = self.db.clone();
        ctx.spawn(async move {
            let random = match db.get_database_backend() {
                DbBackend::MySql => "RAND()",
                _ => "RANDOM()",
            };
            Entity::find()
                .order_by(Expr::cust(random), Order::Asc)
                .limit(count)
                .all(&db)
                .await
        })
    }
}
