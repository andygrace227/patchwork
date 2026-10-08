//! CRUD by primary key, using a caller-supplied SeaORM connection pool.
//! Requests may finish out of order. Await writes before dependent reads.
//! In-flight queries continue if the actor stops; await replies before shutdown.
use std::marker::PhantomData;

use kameo::{
    Actor,
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::{
    ActiveModelTrait, DatabaseConnection, DbErr, EntityTrait, IntoActiveModel, PrimaryKeyTrait,
};

#[derive(Actor)]
pub struct CrudActor<E: EntityTrait> {
    pub(super) db: DatabaseConnection,
    entity: PhantomData<E>,
}

impl<E: EntityTrait> CrudActor<E> {
    /// Cloning DatabaseConnection shares its existing pool.
    /// Configure max_connections > 1 when opening it to allow concurrent queries.
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
            entity: PhantomData,
        }
    }
}

/// Leave the auto-increment primary key NotSet to let the database assign it.
pub struct Create<E: EntityTrait> {
    pub data: E::ActiveModel,
}

pub struct Get<K = i64> {
    pub id: K,
}

/// Replaces non-key fields of the row identified by the model's primary key.
/// Returns an error if that row does not exist.
pub struct Update<E: EntityTrait> {
    pub data: E::Model,
}

/// Returns the number of rows deleted (zero or one).
pub struct Delete<K = i64> {
    pub id: K,
}

impl<E> Message<Create<E>> for CrudActor<E>
where
    E: EntityTrait,
    E::ActiveModel: Send,
    E::Model: IntoActiveModel<E::ActiveModel>,
{
    type Reply = DelegatedReply<Result<E::Model, DbErr>>;

    async fn handle(
        &mut self,
        msg: Create<E>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move { msg.data.insert(&db).await })
    }
}

impl<E, K> Message<Get<K>> for CrudActor<E>
where
    E: EntityTrait,
    E::PrimaryKey: PrimaryKeyTrait<ValueType = K>,
    K: Send + 'static,
{
    type Reply = DelegatedReply<Result<Option<E::Model>, DbErr>>;

    async fn handle(&mut self, msg: Get<K>, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move { E::find_by_id(msg.id).one(&db).await })
    }
}

impl<E> Message<Update<E>> for CrudActor<E>
where
    E: EntityTrait,
    E::ActiveModel: Send,
    E::Model: IntoActiveModel<E::ActiveModel>,
{
    type Reply = DelegatedReply<Result<E::Model, DbErr>>;

    async fn handle(
        &mut self,
        msg: Update<E>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move {
            // Model conversion marks fields Unchanged; reset them so updates are written.
            msg.data.into_active_model().reset_all().update(&db).await
        })
    }
}

impl<E, K> Message<Delete<K>> for CrudActor<E>
where
    E: EntityTrait,
    E::PrimaryKey: PrimaryKeyTrait<ValueType = K>,
    K: Send + 'static,
{
    type Reply = DelegatedReply<Result<u64, DbErr>>;

    async fn handle(
        &mut self,
        msg: Delete<K>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        ctx.spawn(async move { Ok(E::delete_by_id(msg.id).exec(&db).await?.rows_affected) })
    }
}

/// Cache hook. Reads through to the database until caching is implemented.
pub struct GetCached<K = i64> {
    pub id: K,
}

impl<E, K> Message<GetCached<K>> for CrudActor<E>
where
    E: EntityTrait,
    E::PrimaryKey: PrimaryKeyTrait<ValueType = K>,
    K: Send + 'static,
{
    type Reply = DelegatedReply<Result<Option<E::Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetCached<K>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        <Self as Message<Get<K>>>::handle(self, Get { id: msg.id }, ctx).await
    }
}
