use super::*;

#[test]
fn decrease_keeps_requested_number_of_replicas() {
    let source = partition::Model {
        table_id: 1,
        hash_start: 10,
        partition_id: "test-partition".into(),

        forward_to: None,
        replicas: partition::ReplicaNodes(vec![1, 2, 3]),
    };
    assert_eq!(retained_replicas(&source, 1), vec![1]);
    assert_eq!(retained_replicas(&source, 2), vec![1, 2]);
    assert_eq!(retained_replicas(&source, 3), vec![1, 2, 3]);
    assert!(boundaries(&[], 10).is_err());
}
