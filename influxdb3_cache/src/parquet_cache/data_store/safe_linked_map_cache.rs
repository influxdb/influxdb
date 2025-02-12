use std::{
    sync::{
        atomic::{AtomicI64, AtomicUsize},
        Arc,
    },
    time::Duration,
};

use anyhow::bail;
use hashlink::LinkedHashMap;
use iox_time::TimeProvider;
use metric::Registry;
use object_store::path::Path;
use observability_deps::tracing::debug;

use crate::parquet_cache::{
    data_store::CacheProvider,
    metrics::{AccessMetrics, SizeMetrics},
    CacheEntry, CacheEntryState, CacheValue,
};

/// A cache for storing objects from object storage by their [`Path`]
///
/// This acts as a Least-Recently-Used (LRU) cache that allows for concurrent reads and writes. See
/// the [`Cache::prune`] method for implementation of how the cache entries are pruned. Pruning must
/// be invoked externally, e.g., on an interval.
#[derive(Debug)]
pub(crate) struct Cache {
    /// The maximum amount of memory this cache should occupy in bytes
    capacity: usize,
    /// The current amount of memory being used by the cache in bytes
    _used: AtomicUsize,
    /// What percentage of the total number of cache entries will be pruned during a pruning operation
    _prune_percent: f64,
    /// The map storing cache entries
    map: LruMap,
    /// Provides timestamps for updating the hit time of each cache entry
    time_provider: Arc<dyn TimeProvider>,
    query_cache_duration: Duration,
}

impl Cache {
    /// Create a new cache with a given capacity and prune percent
    pub(crate) fn new(
        capacity: usize,
        prune_percent: f64,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        query_cache_duration: Duration,
    ) -> Self {
        Self {
            capacity,
            _used: AtomicUsize::new(0),
            _prune_percent: prune_percent,
            map: LruMap::new(capacity, metric_registry),
            time_provider,
            query_cache_duration,
        }
    }
}

impl CacheProvider for Cache {
    fn get(&self, path: &Path) -> Option<crate::parquet_cache::CacheEntryState> {
        self.map.get(path)
    }

    fn get_used(&self) -> usize {
        self.map.get_used()
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    fn get_query_cache_duration(&self) -> Duration {
        self.query_cache_duration
    }

    fn get_time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    fn path_already_fetched(&self, path: &Path) -> bool {
        self.map.has_key(path)
    }

    fn set_fetching(&self, path: &Path, fut: crate::parquet_cache::SharedCacheValueFuture) {
        let entry = CacheEntry {
            state: CacheEntryState::Fetching(fut),
            // don't really need to track this
            hit_time: AtomicI64::new(0),
        };
        self.map.put(path, entry);
    }

    fn set_cache_value_directly(&self, path: &Path, cache_value: Arc<CacheValue>) {
        let entry = CacheEntry {
            state: CacheEntryState::Success(cache_value),
            // don't need to track this
            hit_time: AtomicI64::new(0),
        };
        self.map.put(path, entry);
    }

    fn set_success(&self, path: &Path, value: Arc<CacheValue>) -> Result<(), anyhow::Error> {
        self.map.update(path, value)
    }

    fn remove(&self, path: &Path) {
        self.map.remove(path);
    }

    fn prune(&self) -> Option<usize> {
        // no op
        None
    }
}

#[derive(Debug)]
struct LruMap {
    inner: parking_lot::RwLock<LruMapInner>,
}

impl LruMap {
    pub(crate) fn new(max_capacity_bytes: usize, registry: Arc<Registry>) -> Self {
        LruMap {
            inner: parking_lot::RwLock::new(LruMapInner::new(max_capacity_bytes, registry)),
        }
    }

    pub(crate) fn get(&self, path: &Path) -> Option<CacheEntryState> {
        self.inner.write().get(path)
    }

    pub(crate) fn has_key(&self, path: &Path) -> bool {
        self.inner.read().has_key(path)
    }

    pub(crate) fn get_used(&self) -> usize {
        self.inner.read().curr_capacity_bytes
    }

    pub(crate) fn put(&self, key: &Path, val: CacheEntry) {
        self.inner.write().put(key, val);
    }

    pub(crate) fn update(&self, key: &Path, val: Arc<CacheValue>) -> Result<(), anyhow::Error> {
        self.inner.write().update_success(key, val)
    }

    pub(crate) fn remove(&self, key: &Path) {
        self.inner.write().remove(key);
    }
}

#[derive(Debug)]
struct LruMapInner {
    map: LinkedHashMap<Path, CacheEntry>,
    max_capacity_bytes: usize,
    curr_capacity_bytes: usize,
    access_metrics: AccessMetrics,
    size_metrics: SizeMetrics,
}

impl LruMapInner {
    pub(crate) fn new(capacity_bytes: usize, metric_registry: Arc<Registry>) -> Self {
        LruMapInner {
            map: LinkedHashMap::new(),
            max_capacity_bytes: capacity_bytes,
            curr_capacity_bytes: 0,
            access_metrics: AccessMetrics::new(&metric_registry),
            size_metrics: SizeMetrics::new(&metric_registry),
        }
    }

    pub(crate) fn put(&mut self, key: &Path, val: CacheEntry) {
        let new_val_size = val.size();
        debug!(?self.curr_capacity_bytes, ?new_val_size, ?self.max_capacity_bytes, ">>> current view");
        if self.curr_capacity_bytes + new_val_size > self.max_capacity_bytes {
            let mut to_deduct = self.curr_capacity_bytes + new_val_size - self.max_capacity_bytes;
            debug!(
                ?self.curr_capacity_bytes,
                ?new_val_size,
                ?self.max_capacity_bytes,
                ?to_deduct,
                ">>> need to evict");
            while to_deduct > 0 && !self.map.is_empty() {
                // need to drop elements
                if let Some((popped_key, popped_val)) = self.map.pop_front() {
                    debug!(
                        ?popped_key,
                        size = ?popped_val.size(),
                        ">>> removed key from parquet cache to reclaim space"
                    );
                    let popped_val_size = popped_val.size();
                    to_deduct = to_deduct.saturating_sub(popped_val_size);
                    self.curr_capacity_bytes =
                        self.curr_capacity_bytes.saturating_sub(popped_val_size);
                }
            }
        }
        // at this point there should be enough space to add new val
        self.curr_capacity_bytes += val.size();
        self.size_metrics
            .record_file_additions(val.size() as u64, 1);
        self.map.insert(key.clone(), val);
    }

    pub(crate) fn get(&mut self, key: &Path) -> Option<CacheEntryState> {
        if let Some(entry) = self.map.get(key) {
            if entry.is_success() {
                self.access_metrics.record_cache_hit();
            } else if entry.is_fetching() {
                self.access_metrics.record_cache_miss_while_fetching();
            }
            let state = entry.state.clone();

            self.map.to_back(key);
            Some(state)
        } else {
            self.access_metrics.record_cache_miss();
            None
        }
    }

    pub(crate) fn has_key(&self, key: &Path) -> bool {
        self.map.contains_key(key)
    }

    pub(crate) fn update_success(
        &mut self,
        key: &Path,
        val: Arc<CacheValue>,
    ) -> Result<(), anyhow::Error> {
        match self.map.get_mut(key) {
            Some(entry) => {
                if !entry.is_fetching() {
                    bail!("attempted to store value in non-fetching cache entry");
                }
                let current_size = entry.size();
                entry.state = CacheEntryState::Success(val);
                // TODO(trevor): what if size is greater than cache capacity?
                let additional_bytes = entry.size() - current_size;
                self.size_metrics
                    .record_file_additions(additional_bytes as u64, 0);
                Ok(())
            }
            None => {
                bail!("attempted to set success state on an empty cache entry")
            }
        }
    }

    pub(crate) fn remove(&mut self, key: &Path) {
        if let Some(val) = self.map.get(key) {
            self.size_metrics
                .record_file_deletions(val.size() as u64, 1);
        };
        self.map.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicI64, Arc};

    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use metric::Registry;
    use object_store::{path::Path, ObjectMeta};
    use observability_deps::tracing::debug;

    use crate::parquet_cache::{
        data_store::safe_linked_map_cache::LruMapInner, CacheEntry, CacheEntryState, CacheValue,
    };

    #[test_log::test(test)]
    fn test_safe_lru_map() {
        let mut cache = LruMapInner::new(100, Arc::new(Registry::new()));
        let key_1 = Path::from("/some/path_1");
        cache.put(&key_1, build_entry(&key_1, "hello world"));
        debug!(">>> test: running");
        let key_2 = Path::from("/some/path_2");
        let text_2 = r#"
            Lorem Ipsum
            "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit..."
            "There is no one who loves pain itself, who seeks after it and wants to have it, simply because it is pain...""#;

        debug!("Running test 2");
        cache.put(&key_2, build_entry(&key_2, text_2));
        let val = cache.get(&key_1);

        debug!("Running test 3");
        debug!(?val, ">>> from get");
        let val = cache.get(&key_2);
        debug!(?val, ">>> from get");
        assert!(val.is_some());

        let val = cache.get(&key_1);
        // this should be none
        debug!(?val, ">>> from get");
        assert!(val.is_none());
    }

    fn build_entry(path: &Path, text: &'static str) -> CacheEntry {
        CacheEntry {
            state: CacheEntryState::Success(Arc::new(CacheValue {
                data: Bytes::from(text.as_bytes()),
                meta: ObjectMeta {
                    location: path.clone(),
                    last_modified: DateTime::<Utc>::from_timestamp_nanos(0),
                    size: 100,
                    e_tag: None,
                    version: None,
                },
            })),
            hit_time: AtomicI64::new(0),
        }
    }
}
