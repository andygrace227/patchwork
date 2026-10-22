use evmap::{
    StableHashEq,
    handles::{ReadHandle, WriteHandle},
};
use std::{
    hash::Hash,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use thread_local::ThreadLocal;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheEntry<V> {
    data: Option<V>,
    write_time: Instant,
}

#[derive(Clone)]
pub struct FastCache<K, V>
where
    K: StableHashEq + Clone + Send + Sync,
    V: Clone + Eq + Hash + Send + Sync,
{
    writer: Arc<Mutex<WriteHandle<K, CacheEntry<V>>>>,
    readers: Arc<ThreadLocal<ReadHandle<K, CacheEntry<V>>>>,
    expire_duration: Duration,
}

impl<K, V> FastCache<K, V>
where
    K: StableHashEq + Clone + Send + Sync,
    V: Clone + Eq + Hash + Send + Sync,
{
    /// Expiry is in seconds. Cached values must keep stable Hash/Eq implementations.
    pub fn new(expire_duration: u64) -> Self {
        // SAFETY: keys have StableHashEq; entries derive Hash/Eq from their value
        // and timestamp. Cached models derive these traits from immutable fields.
        let (mut writer, _) = unsafe { evmap::new_assert_stable() };
        writer.publish();
        Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: Arc::new(ThreadLocal::new()),
            expire_duration: Duration::from_secs(expire_duration),
        }
    }

    /// None is a miss or expiry; Some(None) is a cached missing value.
    pub fn read(&self, key: &K) -> Option<Option<V>> {
        // Each thread creates its reader once; subsequent reads take no lock.
        let reader = self.readers.get_or(|| self.writer.lock().unwrap().clone());
        let entry = reader.get_one(key)?;
        if entry.write_time.elapsed() >= self.expire_duration {
            return None;
        }
        Some(entry.data.clone())
    }

    pub fn write(&self, key: K, data: Option<V>) {
        let mut writer = self.writer.lock().unwrap();
        writer.update(
            key,
            CacheEntry {
                data,
                write_time: Instant::now(),
            },
        );
        writer.publish();
    }
}
