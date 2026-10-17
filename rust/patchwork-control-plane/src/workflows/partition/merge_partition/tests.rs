use super::*;
use crate::datamodels::partition::ReplicaNodes;

#[test]
fn merge_selects_previous_and_next_and_keeps_last_partition() {
    let ring: Vec<_> = [10, 20, 30]
        .into_iter()
        .map(|hash_start| Model {
            table_id: 1,
            hash_start,
            partition_id: format!("partition-{hash_start}"),

            forward_to: None,
            replicas: ReplicaNodes(vec![1]),
        })
        .collect();
    let (previous, source, next) = neighbors(&ring, 10).unwrap();
    assert_eq!(
        (previous.hash_start, source.hash_start, next.hash_start),
        (30, 10, 20)
    );
    assert!(neighbors(&ring[..1], 10).is_err());
    assert!(neighbors(&[], 10).is_err());
    assert!(neighbors(&ring, 15).is_err());
}
