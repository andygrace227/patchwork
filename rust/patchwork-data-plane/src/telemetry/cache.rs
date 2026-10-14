use std::{collections::BTreeMap, time::Instant};

use patchwork_common::db::access_stats::Model;
use ringbuffer::{AllocRingBuffer, RingBuffer};

use super::Access;

pub const BUFFER_SIZE: usize = 4096;

pub struct TelemetryCache {
    bins: BTreeMap<(i64, i64), AllocRingBuffer<Access>>,
    start: Instant,
    start_ms: i64,
}

impl Default for TelemetryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryCache {
    pub fn new() -> Self {
        Self {
            bins: BTreeMap::new(),
            start: Instant::now(),
            start_ms: super::now_ms(),
        }
    }

    pub fn inc(&mut self, table_id: i64, primary_key: i64, secondary_key: i64, is_write: bool) {
        let entry = self
            .bins
            .entry((table_id, primary_key))
            .or_insert_with(|| AllocRingBuffer::new(BUFFER_SIZE));
        entry.enqueue(Access {
            secondary_key,
            time: Instant::now(),
            is_write,
        });
    }

    /// A bounded sample per key, with its own observation window when full.
    pub fn stats(&self) -> Vec<Model> {
        let now = Instant::now();
        let end_ms = self.start_ms + now.duration_since(self.start).as_millis() as i64;
        self.bins
            .iter()
            .map(|(&(table_id, hash_start), accesses)| {
                let start = if accesses.len() == BUFFER_SIZE {
                    accesses.front().unwrap().time
                } else {
                    self.start
                };
                let window_seconds = now.duration_since(start).as_secs_f64();
                let mut read_count = 0;
                let mut writes = Vec::new();
                let mut sum: i128 = 0;
                for access in accesses {
                    if access.is_write {
                        writes.push(access.secondary_key);
                        sum += access.secondary_key as i128;
                    } else {
                        read_count += 1;
                    }
                }
                let write_count = writes.len();
                let median_write_position = if writes.is_empty() {
                    None
                } else {
                    Some(*writes.select_nth_unstable(write_count / 2).1)
                };
                Model {
                    id: 0,
                    table_id,
                    hash_start,
                    start_ms: self.start_ms + start.duration_since(self.start).as_millis() as i64,
                    end_ms,
                    reads_per_second: if window_seconds > 0.0 {
                        read_count as f64 / window_seconds
                    } else {
                        0.0
                    },
                    writes_per_second: if window_seconds > 0.0 {
                        write_count as f64 / window_seconds
                    } else {
                        0.0
                    },
                    contributing_nodes: 1,
                    average_write_position: if writes.is_empty() {
                        None
                    } else {
                        Some((sum / write_count as i128) as i64)
                    },
                    median_write_position,
                    read_count,
                    write_count: write_count as i64,
                    write_position_sum: sum.to_string(),
                    window_seconds,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
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
}
