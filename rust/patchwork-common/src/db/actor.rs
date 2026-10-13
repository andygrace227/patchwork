//! CRUD by primary key, using a caller-supplied SeaORM connection pool.
//! Requests may finish out of order. Await writes before dependent reads.
//! In-flight queries continue if the actor stops; await replies before shutdown.
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};
use ttl_cache::TtlCache;

pub const CACHE_TTL: Duration = Duration::from_secs(5);
pub const CACHE_SIZE: usize = 1024;

pub(super) struct MetadataCache<M> {
    pub rows: TtlCache<String, M>,
    pub partitions: TtlCache<i64, Vec<M>>,
    pub generation: u64,
}

impl<M> MetadataCache<M> {
    pub fn clear(&mut self) {
        self.rows.clear();
        self.partitions.clear();
        // Older in-flight reads must not refill the cache after a write.
        self.generation = self.generation.wrapping_add(1);
    }
}

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
    pub(super) cache: Arc<Mutex<MetadataCache<E::Model>>>,
    pub(super) cache_ttl: Duration,
}

impl<E: EntityTrait> CrudActor<E> {
    /// Cloning DatabaseConnection shares its existing pool.
    /// Configure max_connections > 1 when opening it to allow concurrent queries.
    pub fn new(db: DatabaseConnection) -> Self {
        Self::with_cache_ttl(db, CACHE_TTL)
    }

    pub fn with_cache_ttl(db: DatabaseConnection, cache_ttl: Duration) -> Self {
        Self {
            db,
            entity: PhantomData,
            cache_ttl,
            cache: Arc::new(Mutex::new(MetadataCache {
                rows: TtlCache::new(CACHE_SIZE),
                partitions: TtlCache::new(CACHE_SIZE),
                generation: 0,
            })),
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
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let row = msg.data.insert(&db).await?;
            cache.lock().unwrap().clear();
            Ok(row)
        })
    }
}

impl<E, K> Message<Get<K>> for CrudActor<E>
where
    E: EntityTrait,
    E::PrimaryKey: PrimaryKeyTrait<ValueType = K>,
    K: Send + std::fmt::Debug + 'static,
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
        let cache = self.cache.clone();
        ctx.spawn(async move {
            // Model conversion marks fields Unchanged; reset them so updates are written.
            let row = msg.data.into_active_model().reset_all().update(&db).await?;
            cache.lock().unwrap().clear();
            Ok(row)
        })
    }
}

impl<E, K> Message<Delete<K>> for CrudActor<E>
where
    E: EntityTrait,
    E::PrimaryKey: PrimaryKeyTrait<ValueType = K>,
    K: Send + std::fmt::Debug + 'static,
{
    type Reply = DelegatedReply<Result<u64, DbErr>>;

    async fn handle(
        &mut self,
        msg: Delete<K>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let deleted = E::delete_by_id(msg.id).exec(&db).await?.rows_affected;
            cache.lock().unwrap().clear();
            Ok(deleted)
        })
    }
}

/// Five-second bounded read cache. Missing rows and failures are not cached.
pub struct GetCached<K = i64> {
    pub id: K,
}

impl<E, K> Message<GetCached<K>> for CrudActor<E>
where
    E: EntityTrait,
    E::PrimaryKey: PrimaryKeyTrait<ValueType = K>,
    K: Send + std::fmt::Debug + 'static,
{
    type Reply = DelegatedReply<Result<Option<E::Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetCached<K>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let key = format!("{:?}", msg.id);
        let generation = {
            let cache = self.cache.lock().unwrap();
            if let Some(row) = cache.rows.get(&key) {
                return ctx.reply(Ok(Some(row.clone())));
            }
            cache.generation
        };
        let db = self.db.clone();
        let cache = self.cache.clone();
        let ttl = self.cache_ttl;
        ctx.spawn(async move {
            let row = E::find_by_id(msg.id).one(&db).await?;
            if let Some(row) = &row {
                let mut cache = cache.lock().unwrap();
                if cache.generation == generation {
                    cache.rows.insert(key, row.clone(), ttl);
                }
            }
            Ok(row)
        })
    }
}
