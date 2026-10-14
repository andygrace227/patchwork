//! Database actors and their messages.
use crate::datamodels;

pub mod crud;
pub mod node;
pub mod parition_lock;
pub mod partition;
pub mod table;

pub use node::NodeActor;
pub type PartitionActor = crud::CrudActor<datamodels::partition::Entity>;
pub type TableActor = crud::CrudActor<datamodels::table::Entity>;
