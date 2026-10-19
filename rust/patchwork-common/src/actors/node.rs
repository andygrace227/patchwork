use crate::datamodels::node::{ActiveModel, Column, Entity, Model};
use kameo::{Actor, message::Context, messages, reply::DelegatedReply};
use sea_orm::entity::prelude::*;
use sea_orm::{QueryFilter, QueryOrder};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use ttl_cache::TtlCache;

pub const CACHE_TTL: Duration = Duration::from_secs(5 * 60);
pub const CACHE_SIZE: usize = 1024;

struct NodeCache {
    entries: TtlCache<i64, Model>,
    generation: u64,
}

impl NodeCache {
    fn clear(&mut self) {
        self.entries.clear();
        // Reads started before a write must not refill the cache with old data.
        self.generation = self.generation.wrapping_add(1);
    }
}

#[derive(Actor)]
pub struct NodeActor {
    db: DatabaseConnection,
    cache: Arc<Mutex<NodeCache>>,
    cache_ttl: Duration,
}

impl NodeActor {
    pub fn new(db: DatabaseConnection) -> Self {
        Self::with_cache_ttl(db, CACHE_TTL)
    }

    pub fn with_cache_ttl(db: DatabaseConnection, cache_ttl: Duration) -> Self {
        Self {
            db,
            cache_ttl,
            cache: Arc::new(Mutex::new(NodeCache {
                entries: TtlCache::new(CACHE_SIZE),
                generation: 0,
            })),
        }
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
        let generation = {
            let cache = self.cache.lock().unwrap();
            if let Some(node) = cache.entries.get(&id) {
                return ctx.reply(Ok(Some(node.clone())));
            }
            cache.generation
        };
        let db = self.db.clone();
        let cache = self.cache.clone();
        let cache_ttl = self.cache_ttl;
        ctx.spawn(async move {
            let node = Entity::find_by_id(id).one(&db).await?;
            if let Some(node) = &node {
                let mut cache = cache.lock().unwrap();
                if cache.generation == generation {
                    cache.entries.insert(id, node.clone(), cache_ttl);
                }
            }
            Ok(node)
        })
    }

    /// Uses the actor's bounded read cache. Missing rows and failures are not cached.
    #[message(ctx)]
    pub async fn get_node_cached(
        &mut self,
        id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        self.get_node(id, ctx).await
    }

    /// Leave the auto-increment primary key NotSet to let the database assign it.
    #[message(ctx)]
    pub async fn create_node(
        &mut self,
        data: ActiveModel,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let node = data.insert(&db).await?;
            cache.lock().unwrap().clear();
            Ok(node)
        })
    }

    /// Replaces non-key fields. Returns an error if the row does not exist.
    #[message(ctx)]
    pub async fn update_node(
        &mut self,
        data: Model,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            use sea_orm::IntoActiveModel;
            let node = data.into_active_model().reset_all().update(&db).await?;
            cache.lock().unwrap().clear();
            Ok(node)
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
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let result = Entity::delete_by_id(id).exec(&db).await?;
            cache.lock().unwrap().clear();
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
