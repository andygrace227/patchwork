use super::*;

#[test]
fn initial_ring_has_even_ranges_and_three_copies() {
    let partitions = initial_partitions(42, [7, 11, 19]);
    let starts: Vec<_> = partitions
        .iter()
        .map(|p| *p.hash_start.as_ref() as i128)
        .collect();
    let widths = [
        starts[1] - starts[0],
        starts[2] - starts[1],
        (1_i128 << 64) + starts[0] - starts[2],
    ];
    assert_eq!(starts[0], i64::MIN as i128);
    assert!(widths.iter().max().unwrap() - widths.iter().min().unwrap() <= 1);
    assert_eq!(widths.iter().sum::<i128>(), 1_i128 << 64);
    for p in &partitions {
        assert_eq!(*p.table_id.as_ref(), 42);
        let mut replicas = p.replicas.as_ref().0.clone();
        replicas.sort_unstable();
        assert_eq!(replicas, vec![7, 11, 19]);
        assert_eq!(*p.forward_to.as_ref(), None);
    }
}
