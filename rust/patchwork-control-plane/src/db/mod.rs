pub mod actor;
pub mod node;
pub mod partition;
pub mod table;

pub type NodeActor = actor::CrudActor<node::Entity>;
pub type PartitionActor = actor::CrudActor<partition::Entity>;
pub type TableActor = actor::CrudActor<table::Entity>;
