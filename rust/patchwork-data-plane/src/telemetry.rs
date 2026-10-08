use std::time::{Duration, Instant};

use ringbuffer::{AllocRingBuffer, RingBuffer};
use serde::{Deserialize, Serialize};

pub const WINDOW: Duration = Duration::from_secs(10);
pub const BUFFER_SIZE: usize = 1024;

pub struct Access {
    pub secondary_key: i64,
    pub time: Instant,
    pub is_write: bool,
}

pub struct KeyTracker {
    bins: AllocRingBuffer<Access>,
    start: Instant,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AccessStatistics {
    pub reads_per_second: f64,
    pub writes_per_second: f64,
    pub average_write_position: Option<i64>,
    // Upper middle key for an even number of writes; always an observed key.
    pub median_write_position: Option<i64>,
    pub window_seconds: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Model {
    pub table_id: i64,
    pub partition_key: i64,
    #[serde(flatten)]
    pub statistics: AccessStatistics,
}

impl KeyTracker {
    pub fn new(size: usize) -> KeyTracker {
        KeyTracker {
            bins: AllocRingBuffer::new(size),
            start: Instant::now(),
        }
    }

    pub fn inc(&mut self, secondary_key: i64, is_write: bool) {
        let now = Instant::now();
        while self
            .bins
            .front()
            .is_some_and(|a| now.duration_since(a.time) >= WINDOW)
        {
            self.bins.dequeue();
        }
        if let Some(oldest) = self.bins.enqueue(Access {
            secondary_key,
            time: now,
            is_write,
        }) {
            // A full buffer shortens the observation window, not the reported rate.
            self.start = oldest.time;
        }
    }

    pub fn is_idle(&self) -> bool {
        self.bins.back().is_none_or(|a| a.time.elapsed() >= WINDOW)
    }

    pub fn stats(&self) -> AccessStatistics {
        let now = Instant::now();
        let window_seconds = now.duration_since(self.start).min(WINDOW).as_secs_f64();
        let mut read_count = 0;
        let mut writes = Vec::new();
        let mut sum: i128 = 0;

        for a in &self.bins {
            if now.duration_since(a.time) >= WINDOW {
                continue;
            }
            if a.is_write {
                writes.push(a.secondary_key);
                sum += a.secondary_key as i128;
            } else {
                read_count += 1;
            }
        }

        let write_count = writes.len();
        let median_write_position = if write_count == 0 {
            None
        } else {
            Some(*writes.select_nth_unstable(write_count / 2).1)
        };
        AccessStatistics {
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
            average_write_position: if write_count == 0 {
                None
            } else {
                Some((sum / write_count as i128) as i64)
            },
            median_write_position,
            window_seconds,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_expired_writes_have_no_position() {
        let mut tracker = KeyTracker::new(4);
        let empty = tracker.stats();
        assert_eq!(empty.reads_per_second, 0.0);
        assert_eq!(empty.writes_per_second, 0.0);
        assert_eq!(empty.average_write_position, None);
        assert_eq!(empty.median_write_position, None);

        let now = Instant::now();
        tracker.start = now - Duration::from_secs(20);
        tracker.bins.enqueue(Access {
            secondary_key: 100,
            time: now - Duration::from_secs(11),
            is_write: true,
        });
        assert!(tracker.is_idle());
        tracker.inc(0, false);
        let stats = tracker.stats();
        assert_eq!(tracker.bins.len(), 1);
        assert_eq!(stats.reads_per_second, 0.1);
        assert_eq!(stats.writes_per_second, 0.0);
        assert_eq!(stats.average_write_position, None);
        assert_eq!(stats.median_write_position, None);
        assert!(!tracker.is_idle());
    }

    #[test]
    fn positions_handle_extreme_keys_and_repeated_writes() {
        let mut tracker = KeyTracker::new(8);
        for key in [i64::MIN, i64::MAX] {
            tracker.inc(key, true);
        }
        let stats = tracker.stats();
        assert_eq!(stats.average_write_position, Some(0));
        assert_eq!(stats.median_write_position, Some(i64::MAX));
        tracker.inc(i64::MIN, true);
        tracker.inc(0, false);
        let stats = tracker.stats();
        assert_eq!(stats.median_write_position, Some(i64::MIN));
        assert!(stats.writes_per_second.is_finite());
        assert!(stats.writes_per_second > 0.0);
        assert!((stats.writes_per_second / stats.reads_per_second - 3.0).abs() < 1e-9);
    }

    #[test]
    fn overflow_shortens_the_shared_rate_window() {
        let mut tracker = KeyTracker::new(2);
        let now = Instant::now();
        tracker.start = now - Duration::from_secs(20);
        for (seconds, is_write) in [(3, true), (2, false)] {
            tracker.bins.enqueue(Access {
                secondary_key: 10,
                time: now - Duration::from_secs(seconds),
                is_write,
            });
        }
        tracker.inc(90, true);
        assert_eq!(tracker.start, now - Duration::from_secs(3));
        let stats = tracker.stats();
        assert!((stats.reads_per_second * stats.window_seconds - 1.0).abs() < 1e-9);
        assert!((stats.writes_per_second * stats.window_seconds - 1.0).abs() < 1e-9);
        assert_eq!(stats.average_write_position, Some(90));
        assert_eq!(stats.median_write_position, Some(90));
    }
}
