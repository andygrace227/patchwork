use super::*;

#[test]
fn merging_keeps_write_weight_across_nodes_and_windows() {
    let samples = [
        AccessStatistics {
            reads_per_second: 2.0,
            writes_per_second: 1.0,
            contributing_nodes: 1,
            average_write_position: Some(10),
            median_write_position: Some(10),
            read_count: 2,
            write_count: 1,
            write_position_sum: 10,
            window_seconds: 1.0,
        },
        AccessStatistics {
            reads_per_second: 3.0,
            writes_per_second: 4.5,
            contributing_nodes: 1,
            average_write_position: Some(100),
            median_write_position: Some(100),
            read_count: 6,
            write_count: 9,
            write_position_sum: 900,
            window_seconds: 2.0,
        },
        AccessStatistics {
            reads_per_second: 5.0,
            writes_per_second: 0.5,
            contributing_nodes: 1,
            average_write_position: Some(-50),
            median_write_position: Some(-50),
            read_count: 20,
            write_count: 2,
            write_position_sum: -100,
            window_seconds: 4.0,
        },
    ];
    for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
        let mut merged = AccessStatistics::default();
        for index in order {
            merged.merge(&samples[index]);
        }
        assert_eq!(merged.reads_per_second, 10.0);
        assert_eq!(merged.writes_per_second, 6.0);
        assert_eq!(merged.contributing_nodes, 3);
        assert_eq!(merged.read_count, 28);
        assert_eq!(merged.write_count, 12);
        assert_eq!(merged.write_position_sum, 810);
        assert_eq!(merged.average_write_position, Some(67));
        assert_eq!(merged.median_write_position, None);
        assert_eq!(merged.window_seconds, 1.0);
    }
}

#[test]
fn read_only_merges_preserve_the_write_position() {
    let mut merged = AccessStatistics::default();
    assert_eq!(merged.contributing_nodes, 0);
    merged.merge(&AccessStatistics {
        reads_per_second: 1.0,
        contributing_nodes: 1,
        read_count: 10,
        window_seconds: 10.0,
        ..Default::default()
    });
    assert_eq!(merged.average_write_position, None);
    assert_eq!(merged.median_write_position, None);
    merged.merge(&AccessStatistics {
        writes_per_second: 0.1,
        contributing_nodes: 1,
        average_write_position: Some(-10),
        median_write_position: Some(-10),
        write_count: 1,
        write_position_sum: -10,
        window_seconds: 10.0,
        ..Default::default()
    });
    merged.merge(&AccessStatistics::default());
    assert_eq!(merged.average_write_position, Some(-10));
    assert_eq!(merged.median_write_position, Some(-10));
    assert_eq!(merged.window_seconds, 10.0);
    assert_eq!(merged.read_count, 10);
    assert_eq!(merged.contributing_nodes, 2);
}

#[test]
fn large_key_sums_survive_json_and_merge_without_losing_precision() {
    let large = AccessStatistics {
        writes_per_second: 3.0,
        contributing_nodes: 1,
        average_write_position: Some(i64::MAX),
        median_write_position: Some(i64::MAX),
        write_count: 3,
        write_position_sum: i64::MAX as i128 * 3,
        window_seconds: 1.0,
        ..Default::default()
    };
    let mut merged: AccessStatistics =
        serde_json::from_slice(&serde_json::to_vec(&large).unwrap()).unwrap();
    let telemetry = Telemetry {
        table_id: 1,
        partition_key: 10,
        statistics: large,
    };
    let decoded: Telemetry =
        serde_json::from_slice(&serde_json::to_vec(&telemetry).unwrap()).unwrap();
    assert_eq!(decoded.statistics.contributing_nodes, 1);
    assert_eq!(
        decoded.statistics.write_position_sum,
        merged.write_position_sum
    );
    assert_eq!(merged.write_position_sum, 27_670_116_110_564_327_421);
    merged.merge(&AccessStatistics {
        writes_per_second: 1.0,
        contributing_nodes: 1,
        average_write_position: Some(i64::MIN),
        median_write_position: Some(i64::MIN),
        write_count: 1,
        write_position_sum: i64::MIN as i128,
        window_seconds: 1.0,
        ..Default::default()
    });
    assert_eq!(
        merged.average_write_position,
        Some(4_611_686_018_427_387_903)
    );
    assert_eq!(merged.write_count, 4);
    assert_eq!(merged.contributing_nodes, 2);
}
