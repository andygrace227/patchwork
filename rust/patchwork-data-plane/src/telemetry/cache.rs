use std::{collections::BTreeMap, time::Instant};

use patchwork_common::datamodels::access_stats::Model;
use ringbuffer::{AllocRingBuffer, RingBuffer};

use super::Access;

pub const BUFFER_SIZE: usize = 4096;

pub struct TelemetryCache {
    bins: BTreeMap<(i64, String), AllocRingBuffer<Access>>,
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

    pub fn inc(
        &mut self,
        table_id: i64,
        partition_key: i64,
        is_write: bool,
        partition_id: Option<String>,
    ) {
        let Some(partition_id) = partition_id.filter(|id| !id.is_empty()) else {
            return;
        };
        let entry = self
            .bins
            .entry((table_id, partition_id))
            .or_insert_with(|| AllocRingBuffer::new(BUFFER_SIZE));
        entry.enqueue(Access {
            partition_key,
            time: Instant::now(),
            is_write,
        });
    }

    /// A bounded sample per partition ID, with its own observation window when full.
    pub fn stats(&self) -> Vec<Model> {
        let now = Instant::now();
        let end_ms = self.start_ms + now.duration_since(self.start).as_millis() as i64;
        self.bins
            .iter()
            .map(|((table_id, partition_id), accesses)| {
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
                        writes.push(access.partition_key);
                        sum += access.partition_key as i128;
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
                    table_id: *table_id,
                    partition_id: Some(partition_id.clone()),
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
mod tests;
