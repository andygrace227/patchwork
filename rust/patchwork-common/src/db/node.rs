use super::actor::{Create, Delete, Get, GetCached, Update};
use kameo::{
    Actor,
    message::{Context, Message},
    reply::DelegatedReply,
};
use sea_orm::entity::prelude::*;
use sea_orm::{QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};
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

impl Message<Get> for NodeActor {
    type Reply = DelegatedReply<Result<Option<Model>, DbErr>>;

    async fn handle(&mut self, msg: Get, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let generation = {
            let cache = self.cache.lock().unwrap();
            if let Some(node) = cache.entries.get(&msg.id) {
                return ctx.reply(Ok(Some(node.clone())));
            }
            cache.generation
        };
        let db = self.db.clone();
        let cache = self.cache.clone();
        let cache_ttl = self.cache_ttl;
        ctx.spawn(async move {
            let node = Entity::find_by_id(msg.id).one(&db).await?;
            if let Some(node) = &node {
                let mut cache = cache.lock().unwrap();
                if cache.generation == generation {
                    cache.entries.insert(msg.id, node.clone(), cache_ttl);
                }
            }
            Ok(node)
        })
    }
}

impl Message<GetCached> for NodeActor {
    type Reply = DelegatedReply<Result<Option<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetCached,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        <Self as Message<Get>>::handle(self, Get { id: msg.id }, ctx).await
    }
}

impl Message<Create<Entity>> for NodeActor {
    type Reply = DelegatedReply<Result<Model, DbErr>>;

    async fn handle(
        &mut self,
        msg: Create<Entity>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let node = msg.data.insert(&db).await?;
            cache.lock().unwrap().clear();
            Ok(node)
        })
    }
}

impl Message<Update<Entity>> for NodeActor {
    type Reply = DelegatedReply<Result<Model, DbErr>>;

    async fn handle(
        &mut self,
        msg: Update<Entity>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            use sea_orm::IntoActiveModel;
            let node = msg.data.into_active_model().reset_all().update(&db).await?;
            cache.lock().unwrap().clear();
            Ok(node)
        })
    }
}

impl Message<Delete> for NodeActor {
    type Reply = DelegatedReply<Result<u64, DbErr>>;

    async fn handle(&mut self, msg: Delete, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            let result = Entity::delete_by_id(msg.id).exec(&db).await?;
            cache.lock().unwrap().clear();
            Ok(result.rows_affected)
        })
    }
}

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

/// Pick distinct nodes at random for a new table's initial replicas.
pub struct GetRandomNodes {
    pub count: u64,
}

impl Message<GetRandomNodes> for NodeActor {
    type Reply = DelegatedReply<Result<Vec<Model>, DbErr>>;

    async fn handle(
        &mut self,
        msg: GetRandomNodes,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        use sea_orm::{DbBackend, Order, QuerySelect, sea_query::Expr};
        let db = self.db.clone();
        ctx.spawn(async move {
            let random = match db.get_database_backend() {
                DbBackend::MySql => "RAND()",
                _ => "RANDOM()",
            };
            Entity::find()
                .order_by(Expr::cust(random), Order::Asc)
                .limit(msg.count)
                .all(&db)
                .await
        })
    }
}
