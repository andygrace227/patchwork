pub mod actor;
pub mod node;
pub mod parition_lock;
pub mod partition;
pub mod table;

pub use node::NodeActor;
pub type PartitionActor = actor::CrudActor<partition::Entity>;
pub type TableActor = actor::CrudActor<table::Entity>;
