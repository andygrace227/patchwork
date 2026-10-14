use super::*;
use std::time::Duration;

#[test]
fn groups_keys_and_keeps_exact_write_positions() {
    let mut cache = TelemetryCache::new();
    assert!(cache.stats().is_empty());
    for key in [i64::MAX, i64::MAX, i64::MAX, i64::MIN] {
        cache.inc(1, 10, key, true);
    }
    cache.inc(1, 10, 0, false);
    cache.inc(2, 10, 0, false);
    let rows = cache.stats();
    assert_eq!(rows.len(), 2);
    assert_eq!((rows[0].table_id, rows[0].hash_start), (1, 10));
    assert_eq!(rows[0].write_count, 4);
    assert_eq!(rows[0].read_count, 1);
    assert_eq!(
        rows[0].write_position_sum,
        (3 * i64::MAX as i128 + i64::MIN as i128).to_string()
    );
    assert_eq!(rows[0].median_write_position, Some(i64::MAX));
    assert!((rows[0].writes_per_second / rows[0].reads_per_second - 4.0).abs() < 1e-9);
    assert_eq!(rows[1].average_write_position, None);
    assert_eq!(rows[1].median_write_position, None);
}

#[test]
fn overflow_only_shortens_its_own_keys_window() {
    let mut cache = TelemetryCache::new();
    cache.start -= Duration::from_secs(60);
    cache.start_ms -= 60_000;
    cache.inc(1, 20, 0, false);
    for key in 0..BUFFER_SIZE + 5 {
        cache.inc(1, 10, key as i64, true);
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
