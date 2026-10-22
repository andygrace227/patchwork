use anyhow::Result;
use kameo::actor::ActorRef;
use kameo_actors::pool::{ActorPool, Dispatch};
use patchwork_common::cache::FastCache;

use crate::{
    actors::{
        TableActor,
        table::{CreateTable, DeleteTable, GetRandomReadyTable, GetTable, UpdateTable},
    },
    datamodels::table::{ActiveModel, Model},
};

#[derive(Clone)]
pub struct TableClient {
    actor: ActorRef<ActorPool<TableActor>>,
    cache: FastCache<i64, Model>,
}

impl TableClient {
    pub fn new(actor: ActorRef<ActorPool<TableActor>>) -> Self {
        Self {
            actor,
            cache: FastCache::new(5),
        }
    }

    pub async fn get_table(&self, id: i64) -> Result<Option<Model>> {
        if let Some(value) = self.cache.read(&id) {
            return Ok(value);
        }
        let value = self.get_table_uncached(id).await?;
        self.cache.write(id, value.clone());
        Ok(value)
    }

    pub async fn get_table_uncached(&self, id: i64) -> Result<Option<Model>> {
        Ok(self.actor.ask(Dispatch(GetTable { id })).await?)
    }

    pub async fn create_table(&self, data: ActiveModel) -> Result<Model> {
        let value = self.actor.ask(Dispatch(CreateTable { data })).await?;
        self.cache.write(value.table_id, Some(value.clone()));
        Ok(value)
    }

    pub async fn update_table(&self, data: Model) -> Result<Model> {
        let value = self.actor.ask(Dispatch(UpdateTable { data })).await?;
        self.cache.write(value.table_id, Some(value.clone()));
        Ok(value)
    }

    pub async fn delete_table(&self, id: i64) -> Result<u64> {
        let deleted = self.actor.ask(Dispatch(DeleteTable { id })).await?;
        self.cache.write(id, None);
        Ok(deleted)
    }

    pub async fn get_random_ready_table(&self) -> Result<Option<Model>> {
        Ok(self.actor.ask(Dispatch(GetRandomReadyTable)).await?)
    }
}
