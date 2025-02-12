use std::{
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicI64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::bail;
use dashmap::{DashMap, Entry};
use iox_time::TimeProvider;
use metric::Registry;
use object_store::path::Path;

use crate::parquet_cache::{
    data_store::CacheProvider,
    metrics::{AccessMetrics, SizeMetrics},
    CacheEntry, CacheEntryState, CacheValue, PruneHeapItem, SharedCacheValueFuture,
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
    used: AtomicUsize,
    /// What percentage of the total number of cache entries will be pruned during a pruning operation
    prune_percent: f64,
    /// The map storing cache entries
    map: DashMap<Path, CacheEntry>,
    /// Provides timestamps for updating the hit time of each cache entry
    time_provider: Arc<dyn TimeProvider>,
    /// Track metrics for observing accesses to the cache
    access_metrics: AccessMetrics,
    /// Track metrics for observing the size of the cache
    size_metrics: SizeMetrics,
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
            used: AtomicUsize::new(0),
            prune_percent,
            map: DashMap::new(),
            time_provider,
            access_metrics: AccessMetrics::new(&metric_registry),
            size_metrics: SizeMetrics::new(&metric_registry),
            query_cache_duration,
        }
    }
}

impl CacheProvider for Cache {
    /// Get an entry in the cache or `None` if there is not an entry
    ///
    /// This updates the hit time of the entry and returns a cloned copy of the entry state so that
    /// the reference into the map is dropped
    fn get(&self, path: &Path) -> Option<CacheEntryState> {
        let Some(entry) = self.map.get(path) else {
            self.access_metrics.record_cache_miss();
            return None;
        };
        if entry.is_success() {
            self.access_metrics.record_cache_hit();
            entry
                .hit_time
                .store(self.time_provider.now().timestamp_nanos(), Ordering::SeqCst);
        } else if entry.is_fetching() {
            self.access_metrics.record_cache_miss_while_fetching();
        }
        Some(entry.state.clone())
    }

    fn get_used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
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

    /// Check if an entry in the cache is in process of being fetched or if it was already fetched
    /// successfully
    ///
    /// This does not update the hit time of the entry
    fn path_already_fetched(&self, path: &Path) -> bool {
        self.map.get(path).is_some()
    }

    /// Insert a `Fetching` entry to the cache along with the shared future for polling the value
    /// being fetched
    fn set_fetching(&self, path: &Path, fut: SharedCacheValueFuture) {
        let entry = CacheEntry {
            state: CacheEntryState::Fetching(fut),
            hit_time: AtomicI64::new(self.time_provider.now().timestamp_nanos()),
        };
        let additional = entry.size();
        self.size_metrics
            .record_file_additions(additional as u64, 1);
        self.map.insert(path.clone(), entry);
        self.used.fetch_add(additional, Ordering::SeqCst);
    }

    /// When parquet bytes are in hand this method can be used to update the cache value
    /// directly without going through Fetching -> Success lifecycle
    fn set_cache_value_directly(&self, path: &Path, cache_value: Arc<CacheValue>) {
        let entry = CacheEntry {
            state: CacheEntryState::Success(cache_value),
            hit_time: AtomicI64::new(self.time_provider.now().timestamp_nanos()),
        };
        let additional = entry.size();
        self.size_metrics
            .record_file_additions(additional as u64, 1);
        self.map.insert(path.clone(), entry);
        self.used.fetch_add(additional, Ordering::SeqCst);
    }

    /// Update a `Fetching` entry to a `Success` entry in the cache
    fn set_success(&self, path: &Path, value: Arc<CacheValue>) -> Result<(), anyhow::Error> {
        match self.map.entry(path.clone()) {
            Entry::Occupied(mut o) => {
                let entry = o.get_mut();
                if !entry.is_fetching() {
                    // NOTE(trevor): the only other state is Success, so bailing here just
                    // means that we leave the entry alone, and since objects in the store are
                    // treated as immutable, this should be okay.
                    bail!("attempted to store value in non-fetching cache entry");
                }
                let current_size = entry.size();
                entry.state = CacheEntryState::Success(value);
                entry
                    .hit_time
                    .store(self.time_provider.now().timestamp_nanos(), Ordering::SeqCst);
                // TODO(trevor): what if size is greater than cache capacity?
                let additional_bytes = entry.size() - current_size;
                self.size_metrics
                    .record_file_additions(additional_bytes as u64, 0);
                self.used.fetch_add(additional_bytes, Ordering::SeqCst);
                Ok(())
            }
            Entry::Vacant(_) => bail!("attempted to set success state on an empty cache entry"),
        }
    }

    /// Remove an entry from the cache, as well as its associated size from the used capacity
    fn remove(&self, path: &Path) {
        let Some((_, entry)) = self.map.remove(path) else {
            return;
        };
        let removed_bytes = entry.size();
        self.size_metrics
            .record_file_deletions(removed_bytes as u64, 1);
        self.used.fetch_sub(removed_bytes, Ordering::SeqCst);
    }

    /// Prune least recently hit entries from the cache
    ///
    /// This is a no-op if the `used` amount on the cache is not >= its `capacity`
    fn prune(&self) -> Option<usize> {
        let used = self.used.load(Ordering::SeqCst);
        let n_to_prune = (self.map.len() as f64 * self.prune_percent).floor() as usize;
        if used < self.capacity || n_to_prune == 0 {
            return None;
        }
        // use a BinaryHeap to determine the cut-off time, at which, entries that were
        // last hit before that time will be pruned:
        let mut prune_heap = BinaryHeap::with_capacity(n_to_prune);

        for map_ref in self.map.iter() {
            let hit_time = map_ref.value().hit_time.load(Ordering::SeqCst);
            let size = map_ref.value().size();
            let path = map_ref.key().as_ref();
            if prune_heap.len() < n_to_prune {
                // if the heap isn't full yet, throw this item on:
                prune_heap.push(PruneHeapItem {
                    hit_time,
                    path_ref: path.into(),
                    size,
                });
            } else if hit_time < prune_heap.peek().map(|item| item.hit_time).unwrap() {
                // otherwise, the heap is at its capacity, so only push if the hit_time
                // in question is older than the top of the heap (after pop'ing the top
                // of the heap to make room)
                prune_heap.pop();
                prune_heap.push(PruneHeapItem {
                    path_ref: path.into(),
                    hit_time,
                    size,
                });
            }
        }

        // track the total size of entries that get freed:
        let mut freed = 0;
        let n_files = prune_heap.len() as u64;
        // drop entries with hit times before the cut-off:
        for item in prune_heap {
            self.map.remove(&Path::from(item.path_ref.as_ref()));
            freed += item.size;
        }
        self.size_metrics
            .record_file_deletions(freed as u64, n_files);
        // update used mem size with freed amount:
        self.used.fetch_sub(freed, Ordering::SeqCst);

        Some(freed)
    }
}
