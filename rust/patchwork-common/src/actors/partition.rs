use crate::datamodels::partition::{ActiveModel, Column, Entity, Model};
use kameo::{Actor, message::Context, messages, reply::DelegatedReply};
use sea_orm::IntoActiveModel;
use sea_orm::entity::prelude::*;
use sea_orm::{QueryFilter, QueryOrder, QuerySelect};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use ttl_cache::TtlCache;

pub const CACHE_TTL: Duration = Duration::from_secs(5);
pub const CACHE_SIZE: usize = 1024;

struct PartitionCache {
    rows: TtlCache<(i64, i64), Model>,
    partitions: TtlCache<i64, Vec<Model>>,
    generation: u64,
}

impl PartitionCache {
    fn new() -> Self {
        Self {
            rows: TtlCache::new(CACHE_SIZE),
            partitions: TtlCache::new(CACHE_SIZE),
            generation: 0,
        }
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.partitions.clear();
        // Older in-flight reads must not refill the cache after a write.
        self.generation = self.generation.wrapping_add(1);
    }
}

/// CRUD and partition queries using a shared SeaORM connection pool.
/// Requests may finish out of order. Await writes before dependent reads.
/// In-flight queries continue if the actor stops; await replies before shutdown.
#[derive(Actor)]
pub struct PartitionActor {
    pub(super) db: DatabaseConnection,
    cache: Arc<Mutex<PartitionCache>>,
    cache_ttl: Duration,
}

impl PartitionActor {
    /// Cloning DatabaseConnection shares its existing pool.
    /// Configure max_connections > 1 when opening it to allow concurrent queries.
    pub fn new(db: DatabaseConnection) -> Self {
        Self::with_cache_ttl(db, CACHE_TTL)
    }

    pub fn with_cache_ttl(db: DatabaseConnection, cache_ttl: Duration) -> Self {
        Self {
            db,
            cache_ttl,
            cache: Arc::new(Mutex::new(PartitionCache::new())),
        }
    }
}

#[messages]
impl PartitionActor {
    /// Supply the table ID and hash start that identify this partition.
    #[message(ctx)]
    pub async fn create_partition(
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
    pub async fn get_partition(
        &mut self,
        id: (i64, i64),
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move { Entity::find_by_id(id).one(&db).await })
    }

    /// Replaces non-key fields. Returns an error if the row does not exist.
    #[message(ctx)]
    pub async fn update_partition(
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
    pub async fn delete_partition(
        &mut self,
        id: (i64, i64),
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
    pub async fn get_partition_cached(
        &mut self,
        id: (i64, i64),
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

    /// Returns the table's partitions ordered by hash start.
    #[message(ctx)]
    pub async fn get_table_partitions(
        &mut self,
        table_id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Vec<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Vec<Model>, DbErr>> {
        let generation = {
            let cache = self.cache.lock().unwrap();
            if let Some(partitions) = cache.partitions.get(&table_id) {
                return ctx.reply(Ok(partitions.clone()));
            }
            cache.generation
        };
        let db = self.db.clone();
        let cache = self.cache.clone();
        let ttl = self.cache_ttl;
        ctx.spawn(async move {
            let partitions = Entity::find()
                .filter(Column::TableId.eq(table_id))
                .order_by_asc(Column::HashStart)
                .all(&db)
                .await?;
            let mut cache = cache.lock().unwrap();
            if cache.generation == generation {
                cache.partitions.insert(table_id, partitions.clone(), ttl);
            }
            Ok(partitions)
        })
    }

    /// Fresh metadata for topology changes and table deletion.
    #[message(ctx)]
    pub async fn get_table_partitions_uncached(
        &mut self,
        table_id: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Vec<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Vec<Model>, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            Entity::find()
                .filter(Column::TableId.eq(table_id))
                .order_by_asc(Column::HashStart)
                .all(&db)
                .await
        })
    }

    /// Returns the table's closest partition at or before the supplied hash, or None.
    #[message(ctx)]
    pub async fn get_partition_at_or_before(
        &mut self,
        table_id: i64,
        hash_start: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        let db = self.db.clone();
        ctx.spawn(async move {
            Entity::find()
                .filter(Column::TableId.eq(table_id))
                .filter(Column::HashStart.lte(hash_start))
                .order_by_desc(Column::HashStart)
                .limit(1)
                .one(&db)
                .await
        })
    }

    /// Publish a bootstrapping partition only while all supplied leases are still ours.
    #[message(ctx)]
    pub async fn create_locked_partition(
        &mut self,
        data: ActiveModel,
        locks: Vec<crate::datamodels::parition_lock::Model>,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        use sea_orm::{DbBackend, Statement, TransactionTrait};
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            if locks.is_empty() {
                return Err(DbErr::Custom("Partition leases are required".into()));
            }
            let txn = db.begin().await?;
            // Hold the lease rows until commit, preventing replacement during insertion.
            for lock in &locks {
                let held = txn.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::MySql,
                    "SELECT lock_token FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lock_token = ? AND lease_end > UNIX_TIMESTAMP() FOR UPDATE",
                    [lock.table_id.into(), lock.hash_start.into(), lock.lock_token.clone().into()],
                )).await?;
                if held.is_none() {
                    txn.rollback().await?;
                    return Err(DbErr::Custom("Partition lease expired or changed; retry the split".into()));
                }
            }
            let partition = data.insert(&txn).await?;
            txn.commit().await?;
            cache.lock().unwrap().clear();
            Ok(partition)
        })
    }

    /// Update a partition only while all supplied leases are still ours.
    #[message(ctx)]
    pub async fn update_locked_partition(
        &mut self,
        data: ActiveModel,
        locks: Vec<crate::datamodels::parition_lock::Model>,
        ctx: &mut Context<Self, DelegatedReply<Result<Model, DbErr>>>,
    ) -> DelegatedReply<Result<Model, DbErr>> {
        use sea_orm::{DbBackend, Statement, TransactionTrait};
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            if locks.is_empty() {
                return Err(DbErr::Custom("Partition leases are required".into()));
            }
            let txn = db.begin().await?;
            // Hold the lease rows until commit, preventing replacement during the update.
            for lock in &locks {
                let held = txn.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::MySql,
                    "SELECT lock_token FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lock_token = ? AND lease_end > UNIX_TIMESTAMP() FOR UPDATE",
                    [lock.table_id.into(), lock.hash_start.into(), lock.lock_token.clone().into()],
                )).await?;
                if held.is_none() {
                    txn.rollback().await?;
                    return Err(DbErr::Custom("Partition lease expired or changed; retry the split".into()));
                }
            }
            let partition = data.update(&txn).await?;
            txn.commit().await?;
            cache.lock().unwrap().clear();
            Ok(partition)
        })
    }

    /// Remove a boundary only while the merge still owns its leases.
    #[message(ctx)]
    pub async fn delete_locked_partition(
        &mut self,
        table_id: i64,
        hash_start: i64,
        locks: Vec<crate::datamodels::parition_lock::Model>,
        ctx: &mut Context<Self, DelegatedReply<Result<u64, DbErr>>>,
    ) -> DelegatedReply<Result<u64, DbErr>> {
        use sea_orm::{DbBackend, Statement, TransactionTrait};
        let db = self.db.clone();
        let cache = self.cache.clone();
        ctx.spawn(async move {
            if locks.is_empty() {
                return Err(DbErr::Custom("Partition leases are required".into()));
            }
            let txn = db.begin().await?;
            // Hold the lease rows until commit, preventing replacement during deletion.
            for lock in &locks {
                let held = txn.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::MySql,
                    "SELECT lock_token FROM partition_lock WHERE table_id = ? AND hash_start = ? AND lock_token = ? AND lease_end > UNIX_TIMESTAMP() FOR UPDATE",
                    [lock.table_id.into(), lock.hash_start.into(), lock.lock_token.clone().into()],
                )).await?;
                if held.is_none() {
                    txn.rollback().await?;
                    return Err(DbErr::Custom("Partition lease expired or changed; retry the merge".into()));
                }
            }
            let deleted = Entity::delete_by_id((table_id, hash_start)).exec(&txn).await?;
            if deleted.rows_affected != 1 {
                return Err(DbErr::Custom("Partition no longer exists".into()));
            }
            txn.commit().await?;
            cache.lock().unwrap().clear();
            Ok(deleted.rows_affected)
        })
    }

    /// Use the cached, ordered table ring for inconsistent ownership lookups.
    #[message(ctx)]
    pub async fn get_partition_at_or_before_cached(
        &mut self,
        table_id: i64,
        hash_start: i64,
        ctx: &mut Context<Self, DelegatedReply<Result<Option<Model>, DbErr>>>,
    ) -> DelegatedReply<Result<Option<Model>, DbErr>> {
        // Ask through the same handler so ownership and range reads share one cache.
        let ring = self
            .cache
            .lock()
            .unwrap()
            .partitions
            .get(&table_id)
            .cloned();
        if let Some(ring) = ring {
            let next = ring.partition_point(|p| p.hash_start <= hash_start);
            return ctx.reply(Ok(next.checked_sub(1).map(|i| ring[i].clone())));
        }
        let db = self.db.clone();
        let cache = self.cache.clone();
        let ttl = self.cache_ttl;
        let generation = cache.lock().unwrap().generation;
        ctx.spawn(async move {
            let ring = Entity::find()
                .filter(Column::TableId.eq(table_id))
                .order_by_asc(Column::HashStart)
                .all(&db)
                .await?;
            let next = ring.partition_point(|p| p.hash_start <= hash_start);
            let partition = next.checked_sub(1).map(|i| ring[i].clone());
            let mut cache = cache.lock().unwrap();
            if cache.generation == generation {
                cache.partitions.insert(table_id, ring, ttl);
            }
            Ok(partition)
        })
    }
}
