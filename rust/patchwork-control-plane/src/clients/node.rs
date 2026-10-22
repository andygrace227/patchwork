use anyhow::Result;
use kameo::actor::ActorRef;
use kameo_actors::pool::{ActorPool, Dispatch};
use patchwork_common::cache::FastCache;

use crate::{
    actors::{
        NodeActor,
        node::{CreateNode, DeleteNode, GetNode, GetNodeExcluding, GetRandomNodes, UpdateNode},
    },
    datamodels::node::{ActiveModel, Model},
};

#[derive(Clone)]
pub struct NodeClient {
    actor: ActorRef<ActorPool<NodeActor>>,
    cache: FastCache<i64, Model>,
}

impl NodeClient {
    pub fn new(actor: ActorRef<ActorPool<NodeActor>>) -> Self {
        Self {
            actor,
            cache: FastCache::new(300),
        }
    }

    pub async fn get_node(&self, id: i64) -> Result<Option<Model>> {
        if let Some(value) = self.cache.read(&id) {
            return Ok(value);
        }
        let value = self.actor.ask(Dispatch(GetNode { id })).await?;
        self.cache.write(id, value.clone());
        Ok(value)
    }

    pub async fn create_node(&self, data: ActiveModel) -> Result<Model> {
        let value = self.actor.ask(Dispatch(CreateNode { data })).await?;
        self.cache.write(value.node_id, Some(value.clone()));
        Ok(value)
    }

    pub async fn update_node(&self, data: Model) -> Result<Model> {
        let value = self.actor.ask(Dispatch(UpdateNode { data })).await?;
        self.cache.write(value.node_id, Some(value.clone()));
        Ok(value)
    }

    pub async fn delete_node(&self, id: i64) -> Result<u64> {
        let deleted = self.actor.ask(Dispatch(DeleteNode { id })).await?;
        self.cache.write(id, None);
        Ok(deleted)
    }

    pub async fn get_node_excluding(&self, node_ids: Vec<i64>) -> Result<Option<Model>> {
        Ok(self
            .actor
            .ask(Dispatch(GetNodeExcluding { node_ids }))
            .await?)
    }

    pub async fn get_random_nodes(&self, count: u64) -> Result<Vec<Model>> {
        Ok(self.actor.ask(Dispatch(GetRandomNodes { count })).await?)
    }
}
