use super::*;
use std::time::Duration;

#[test]
fn groups_by_partition_id_and_keeps_exact_partition_key_positions() {
    let mut cache = TelemetryCache::new();
    assert!(cache.stats().is_empty());
    for key in [i64::MAX, i64::MAX, i64::MAX, i64::MIN] {
        cache.inc(1, key, true, Some("a".into()));
    }
    cache.inc(1, 10, false, Some("a".into()));
    cache.inc(2, 10, false, Some("a".into()));
    // The same key moving to another partition must not relabel earlier accesses.
    cache.inc(1, 10, false, Some("b".into()));
    let rows = cache.stats();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].table_id, 1);
    assert_eq!(rows[0].partition_id.as_deref(), Some("a"));
    assert_eq!(rows[0].write_count, 4);
    assert_eq!(rows[0].read_count, 1);
    assert_eq!(
        rows[0].write_position_sum,
        (3 * i64::MAX as i128 + i64::MIN as i128).to_string()
    );
    assert_eq!(rows[0].median_write_position, Some(i64::MAX));
    assert!((rows[0].writes_per_second / rows[0].reads_per_second - 4.0).abs() < 1e-9);
    assert_eq!(rows[1].partition_id.as_deref(), Some("b"));
    assert_eq!(rows[1].read_count, 1);
    assert_eq!(rows[1].average_write_position, None);
    assert_eq!(rows[1].median_write_position, None);
    assert_eq!(rows[2].table_id, 2);
}

#[test]
fn accesses_without_a_partition_id_are_not_grouped_by_key() {
    let mut cache = TelemetryCache::new();
    cache.inc(1, 10, true, None);
    cache.inc(1, 20, false, Some(String::new()));
    assert!(cache.stats().is_empty());
}

#[test]
fn overflow_only_shortens_its_own_partition_window() {
    let mut cache = TelemetryCache::new();
    cache.start -= Duration::from_secs(60);
    cache.start_ms -= 60_000;
    cache.inc(1, 20, false, Some("b".into()));
    for key in 0..BUFFER_SIZE + 5 {
        cache.inc(1, key as i64, true, Some("a".into()));
    }
    let rows = cache.stats();
    assert_eq!(rows[0].write_count, BUFFER_SIZE as i64);
    assert_eq!(
        rows[0].average_write_position,
        Some((5 + BUFFER_SIZE as i64 + 4) / 2)
    );
    assert!(rows[0].window_seconds < rows[1].window_seconds);
    assert!(rows[1].window_seconds >= 60.0);
    assert!(rows[0].start_ms >= rows[1].start_ms);
}
