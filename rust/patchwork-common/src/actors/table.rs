use crate::datamodels::table::{ActiveModel, Column, Entity, Model};
use kameo::{Actor, message::Context, messages, reply::DelegatedReply};
use sea_orm::IntoActiveModel;
use sea_orm::entity::prelude::*;
use sea_orm::{DbBackend, Order, QueryFilter, QueryOrder};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use ttl_cache::TtlCache;

pub const CACHE_TTL: Duration = Duration::from_secs(5);
pub const CACHE_SIZE: usize = 1024;

struct TableCache {
    rows: TtlCache<i64, Model>,
    generation: u64,
}

impl TableCache {
    fn new() -> Self {
        Self {
            rows: TtlCache::new(CACHE_SIZE),
            generation: 0,
        }
    }

    fn clear(&mut self) {
        self.rows.clear();
        // Older in-flight reads must not refill the cache after a write.
        self.generation = self.generation.wrapping_add(1);
    }
}

/// CRUD and table queries using a shared SeaORM connection pool.
/// Requests may finish out of order. Await writes before dependent reads.
/// In-flight queries continue if the actor stops; await replies before shutdown.
#[derive(Actor)]
pub struct TableActor {
    db: DatabaseConnection,
    cache: Arc<Mutex<TableCache>>,
    cache_ttl: Duration,
}

impl TableActor {
    /// Cloning DatabaseConnection shares its existing pool.
    /// Configure max_connections > 1 when opening it to allow concurrent queries.
    pub fn new(db: DatabaseConnection) -> Self {
        Self::with_cache_ttl(db, CACHE_TTL)
    }

    pub fn with_cache_ttl(db: DatabaseConnection, cache_ttl: Duration) -> Self {
        Self {
            db,
            cache_ttl,
            cache: Arc::new(Mutex::new(TableCache::new())),
        }
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
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let row = data.insert(&db).await?;
            cache.lock().unwrap().clear();
            Ok(row)
        })
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
        let cache = self.cache.clone();
        ctx.spawn(async move {
            // Model conversion marks fields Unchanged; reset them so updates are written.
            let row = data.into_active_model().reset_all().update(&db).await?;
            cache.lock().unwrap().clear();
            Ok(row)
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
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let deleted = Entity::delete_by_id(id).exec(&db).await?.rows_affected;
            cache.lock().unwrap().clear();
            Ok(deleted)
        })
    }

    /// Uses the actor's bounded read cache. Missing rows and failures are not cached.
    #[message(ctx)]
    pub async fn get_table_cached(
        &mut self,
        id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        let generation = {
            let cache = self.cache.lock().unwrap();
            if let Some(row) = cache.rows.get(&id) {
                return ctx.reply(Ok(Some(row.clone())));
            }
            cache.generation
        };
        let db = self.db.clone();
        let cache = self.cache.clone();
        let ttl = self.cache_ttl;
        ctx.spawn(async move {
            let row = Entity::find_by_id(id).one(&db).await?;
            if let Some(row) = &row {
                let mut cache = cache.lock().unwrap();
                if cache.generation == generation {
                    cache.rows.insert(id, row.clone(), ttl);
                }
            }
            Ok(row)
        })
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
