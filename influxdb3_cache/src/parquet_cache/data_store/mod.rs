use std::{fmt::Debug, sync::Arc, time::Duration};

use iox_time::TimeProvider;
use object_store::path::Path;

use crate::parquet_cache::{CacheEntryState, CacheValue, SharedCacheValueFuture};

pub(crate) mod safe_linked_map_cache;
pub(crate) mod sharded_map_cache;

pub(crate) trait CacheProvider: Debug + Send + Sync + 'static {
    /// Get an entry in the cache or `None` if there is not an entry
    ///
    /// This updates the hit time of the entry and returns a cloned copy of the entry state so that
    /// the reference into the map is dropped
    fn get(&self, path: &Path) -> Option<CacheEntryState>;

    fn get_used(&self) -> usize;

    fn get_capacity(&self) -> usize;

    fn get_query_cache_duration(&self) -> Duration;

    fn get_time_provider(&self) -> Arc<dyn TimeProvider>;

    /// Check if an entry in the cache is in process of being fetched or if it was already fetched
    /// successfully
    ///
    /// This does not update the hit time of the entry
    fn path_already_fetched(&self, path: &Path) -> bool;

    /// Insert a `Fetching` entry to the cache along with the shared future for polling the value
    /// being fetched
    fn set_fetching(&self, path: &Path, fut: SharedCacheValueFuture);

    /// When parquet bytes are in hand this method can be used to update the cache value
    /// directly without going through Fetching -> Success lifecycle
    fn set_cache_value_directly(&self, path: &Path, cache_value: Arc<CacheValue>);

    /// Update a `Fetching` entry to a `Success` entry in the cache
    fn set_success(&self, path: &Path, value: Arc<CacheValue>) -> Result<(), anyhow::Error>;

    /// Remove an entry from the cache, as well as its associated size from the used capacity
    fn remove(&self, path: &Path);

    /// Prune least recently hit entries from the cache
    ///
    /// This is a no-op if the `used` amount on the cache is not >= its `capacity`
    fn prune(&self) -> Option<usize>;
}
