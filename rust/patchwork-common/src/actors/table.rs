use crate::datamodels::table::{ActiveModel, Column, Entity, Model};
use kameo::{Actor, message::Context, messages, reply::DelegatedReply};
use sea_orm::IntoActiveModel;
use sea_orm::entity::prelude::*;
use sea_orm::{DbBackend, Order, QueryFilter, QueryOrder};

/// CRUD and table queries using a shared SeaORM connection pool.
/// Requests may finish out of order. Await writes before dependent reads.
/// In-flight queries continue if the actor stops; await replies before shutdown.
#[derive(Actor)]
pub struct TableActor {
    db: DatabaseConnection,
}

impl TableActor {
    /// Cloning DatabaseConnection shares its existing pool.
    /// Configure max_connections > 1 when opening it to allow concurrent queries.
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[messages]
impl TableActor {
    /// Leave the auto-increment primary key NotSet to let the database assign it.
    #[message(ctx)]
    pub async fn create_table(
        &mut self,
        data: ActiveModel,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move { data.insert(&db).await })
    }

    #[message(ctx)]
    pub async fn get_table(
        &mut self,
        id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move { Entity::find_by_id(id).one(&db).await })
    }

    /// Replaces non-key fields. Returns an error if the row does not exist.
    #[message(ctx)]
    pub async fn update_table(
        &mut self,
        data: Model,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            // Model conversion marks fields Unchanged; reset them so updates are written.
            data.into_active_model().reset_all().update(&db).await
        })
    }

    /// Returns the number of rows deleted (zero or one).
    #[message(ctx)]
    pub async fn delete_table(
        &mut self,
        id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<u64, DbErr>>>,
    ) -> DelegatedReply<Result<u64, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move { Ok(Entity::delete_by_id(id).exec(&db).await?.rows_affected) })
    }

    /// Pick a ready table for a background scaling check.
    #[message(ctx)]
    pub async fn get_random_ready_table(
        &mut self,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
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
