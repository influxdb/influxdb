//! An in-memory cache of Parquet files that are persisted to object storage
use std::{
    fmt::Debug,
    ops::Range,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use data_types::{TimestampMinMax, TimestampRange};
use futures::{
    future::{BoxFuture, Shared},
    stream::BoxStream,
    FutureExt, StreamExt, TryStreamExt,
};
use iox_time::TimeProvider;
use metric::Registry;
use object_store::{
    path::Path, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use observability_deps::tracing::{debug, error, info, warn};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot, watch,
};

use crate::parquet_cache::data_store::{sharded_map_cache::Cache, CacheProvider};

mod data_store;
mod experimental;
mod metrics;

/// Shared future type for cache values that are being fetched
type SharedCacheValueFuture = Shared<BoxFuture<'static, Result<Arc<CacheValue>, DynError>>>;

/// Dynamic error type that can be cloned
type DynError = Arc<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub struct ParquetFileDataToCache {
    bytes: Bytes,
    object_meta: ObjectMeta,
}

impl ParquetFileDataToCache {
    pub fn new(
        path: &Path,
        last_modified: DateTime<Utc>,
        bytes: Bytes,
        put_result: PutResult,
    ) -> Self {
        let object_meta = ObjectMeta {
            // one time cost
            location: path.clone(),
            last_modified,
            size: bytes.len(),
            e_tag: put_result.e_tag,
            version: put_result.version,
        };
        Self { bytes, object_meta }
    }
}

#[derive(Debug)]
pub struct ImmediateCacheRequest {
    path: Path,
    parquet_data: ParquetFileDataToCache,
}

#[derive(Debug)]
pub struct EventualCacheRequest {
    path: Path,
    notifier: oneshot::Sender<()>,
    file_timestamp_min_max: Option<TimestampMinMax>,
}

impl EventualCacheRequest {
    fn get_path_and_notifier_and_timestamp(
        self,
    ) -> (Path, oneshot::Sender<()>, Option<TimestampMinMax>) {
        (self.path, self.notifier, self.file_timestamp_min_max)
    }
}

#[derive(Debug)]
pub struct EvictionCacheRequest {
    path: Path,
}

#[derive(Debug)]
pub enum CacheRequest {
    // When creating parquet files, the serialized bytes are already
    // present so it can be directly loaded into the cache. This
    // Immediate mode request caters for that use case.
    Immediate(ImmediateCacheRequest),
    // When there is only a path to a parquet file in object store then a
    // GET request is needed to pull the actual data and store it in
    // the cache. This Eventual mode request caters for that use case.
    Eventual(EventualCacheRequest),
    /// These requests allow immediate eviction of a particular path
    /// from the cache
    Evict(EvictionCacheRequest),
}

impl CacheRequest {
    // Create a new [`CacheRequest::Immediate`] for loading the bytes passed in immediately into the cache.
    pub fn create_immediate_mode_cache_request(path: Path, bytes: ParquetFileDataToCache) -> Self {
        Self::Immediate(ImmediateCacheRequest {
            path,
            parquet_data: bytes,
        })
    }

    // Create a new [`CacheRequest::Eventual`] for eventually loading the path passed in, by doing
    // a GET request from object store for the path. Since it is async operation, the receiver is
    // passed back to notify once the data is loaded into the cache
    pub fn create_eventual_mode_cache_request(
        path: Path,
        file_timestamp_min_max: Option<TimestampMinMax>,
    ) -> (Self, oneshot::Receiver<()>) {
        let (notifier, receiver) = oneshot::channel();
        (
            Self::Eventual(EventualCacheRequest {
                path,
                notifier,
                file_timestamp_min_max,
            }),
            receiver,
        )
    }

    pub fn get_path(&self) -> &Path {
        match self {
            CacheRequest::Immediate(ImmediateCacheRequest {
                path,
                parquet_data: _,
            }) => path,
            CacheRequest::Eventual(EventualCacheRequest {
                path,
                notifier: _,
                file_timestamp_min_max: _,
            }) => path,
            CacheRequest::Evict(EvictionCacheRequest { path }) => path,
        }
    }
}

/// An interface for interacting with a Parquet Cache by registering [`CacheRequest`]s to it.
pub trait ParquetCacheOracle: Send + Sync + Debug {
    /// Register a cache request with the oracle
    fn register(&self, cache_request: CacheRequest);

    // Get a receiver that is notified when a prune takes place and how much memory was freed
    fn prune_notifier(&self) -> watch::Receiver<usize>;
}

/// Concrete implementation of the [`ParquetCacheOracle`]
///
/// This implementation keeps a local reference to [`MemCachedObjectStore`] to immediately cache
/// any requests. This also spins up the background job to cache any requests that require looking
/// up parquet files from object store. In the latter case when the GET request to object store
/// completes eventually a notification is sent to the caller to indicate that the request has been
/// fulfilled.
#[derive(Debug, Clone)]
pub struct MemCacheOracle {
    cache_request_tx: Sender<EventualCacheRequest>,
    prune_notifier_tx: watch::Sender<usize>,
    mem_store: Arc<MemCachedObjectStore>,
}

// TODO(trevor): make this configurable with reasonable default
const CACHE_REQUEST_BUFFER_SIZE: usize = 1_000_000;

impl MemCacheOracle {
    /// Create a new [`MemCacheOracle`]
    ///
    /// This spawns two background tasks:
    /// * one to handle registered [`CacheRequest`]s
    /// * one to prune deleted and un-needed cache entries on an interval
    fn new(mem_cached_store: Arc<MemCachedObjectStore>, _prune_interval: Duration) -> Self {
        let (cache_request_tx, cache_request_rx) = channel(CACHE_REQUEST_BUFFER_SIZE);
        background_cache_request_handler(Arc::clone(&mem_cached_store), cache_request_rx);
        let (prune_notifier_tx, _prune_notifier_rx) = watch::channel(0);
        // background_cache_pruner(
        //     Arc::clone(&mem_cached_store),
        //     prune_notifier_tx.clone(),
        //     prune_interval,
        // );
        Self {
            cache_request_tx,
            prune_notifier_tx,
            mem_store: Arc::clone(&mem_cached_store),
        }
    }
}

impl ParquetCacheOracle for MemCacheOracle {
    fn register(&self, request: CacheRequest) {
        let path = request.get_path();
        // We assume that objects on object store are immutable, so we can skip objects that
        // we have already fetched, in eventual mode we send the notification immediately, so
        // that it doesn't wait it's turn in the queue.
        let already_in_cache = self.mem_store.cache.path_already_fetched(path);
        debug!(
            ?already_in_cache,
            ?path,
            ">>> is path already in parquet cache"
        );
        match request {
            CacheRequest::Immediate(ImmediateCacheRequest { path, parquet_data }) => {
                if !already_in_cache {
                    let cache_value = CacheValue {
                        data: parquet_data.bytes,
                        meta: parquet_data.object_meta,
                    };
                    self.mem_store
                        .cache
                        .set_cache_value_directly(&path, Arc::new(cache_value));
                }
            }
            CacheRequest::Eventual(eventual_cache_req) => {
                if !already_in_cache {
                    let tx = self.cache_request_tx.clone();
                    tokio::spawn(async move {
                        if let Err(error) = tx.send(eventual_cache_req).await {
                            error!(%error, "error registering cache request");
                        }
                    });
                } else {
                    let _ = eventual_cache_req.notifier.send(());
                }
            }
            CacheRequest::Evict(eviction_req) => {
                debug!(path = ?eviction_req.path, ">>> removing path from cache");
                self.mem_store.cache.remove(&eviction_req.path);
            }
        }
    }

    fn prune_notifier(&self) -> watch::Receiver<usize> {
        self.prune_notifier_tx.subscribe()
    }
}

/// Helper function for creation of a [`MemCachedObjectStore`] and [`MemCacheOracle`]
/// that returns them as their `Arc<dyn _>` equivalent.
pub fn create_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
    cache_capacity: usize,
    query_cache_duration: Duration,
    prune_percent: f64,
    prune_interval: Duration,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new_with_linked_map(
        MemCachedObjectStoreArgs {
            // let store = Arc::new(MemCachedObjectStore::new(MemCachedObjectStoreArgs {
            time_provider,
            metric_registry,
            inner: object_store,
            memory_capacity: cache_capacity,
            prune_percent,
            query_cache_duration,
        },
    ));
    let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store), prune_interval));
    (store, oracle)
}

/// Create a test cached object store with a cache capacity of 1GB
pub fn test_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    create_cached_obj_store_and_oracle(
        object_store,
        time_provider,
        metric_registry,
        1024 * 1024 * 1024,
        Duration::from_millis(1000),
        0.1,
        Duration::from_millis(10),
    )
}

/// Create a test cached object store with concrete types
pub fn test_cached_obj_store_and_oracle_with_size(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
    cache_capacity: usize,
) -> (Arc<MemCachedObjectStore>, Arc<MemCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new(MemCachedObjectStoreArgs {
        time_provider,
        metric_registry,
        inner: object_store,
        memory_capacity: cache_capacity,
        prune_percent: 0.1, // prune 10%
        query_cache_duration: Duration::from_secs(86_400 * 3),
    }));
    // let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store), Duration::from_secs(1)));
    let oracle = Arc::new(MemCacheOracle::new(
        Arc::clone(&store),
        Duration::from_millis(1),
    ));
    (store, oracle)
}

/// Create a test cached object store with concrete types / linked map
pub fn test_cached_obj_store_and_oracle_with_size_linked_map(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
    cache_capacity: usize,
) -> (Arc<MemCachedObjectStore>, Arc<MemCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new_with_linked_map(
        MemCachedObjectStoreArgs {
            time_provider,
            metric_registry,
            inner: object_store,
            memory_capacity: cache_capacity,
            prune_percent: 0.1, // prune 10%
            query_cache_duration: Duration::from_secs(86_400 * 3),
        },
    ));
    // let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store), Duration::from_secs(1)));
    let oracle = Arc::new(MemCacheOracle::new(
        Arc::clone(&store),
        Duration::from_millis(1),
    ));
    (store, oracle)
}

/// A value in the cache, containing the actual bytes as well as object store metadata
#[derive(Debug)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
}

impl CacheValue {
    /// Get the size of the cache value's memory footprint in bytes
    fn size(&self) -> usize {
        let Self {
            data,
            meta:
                ObjectMeta {
                    location,
                    last_modified: _,
                    size: _,
                    e_tag,
                    version,
                },
        } = self;

        data.len()
            + location.as_ref().len()
            + e_tag.as_ref().map(|s| s.capacity()).unwrap_or_default()
            + version.as_ref().map(|s| s.capacity()).unwrap_or_default()
    }

    /// Fetch the value from an object store
    async fn fetch(store: Arc<dyn ObjectStore>, path: Path) -> object_store::Result<Self> {
        let res = store.get(&path).await?;
        let meta = res.meta.clone();
        let data = res.bytes().await?;
        Ok(Self { data, meta })
    }
}

/// Holds the state and hit time for an entry in the cache
#[derive(Debug)]
struct CacheEntry {
    state: CacheEntryState,
    /// The nano-second timestamp of when this value was last hit
    hit_time: AtomicI64,
}

impl CacheEntry {
    /// Get the approximate memory footprint of this entry in bytes
    fn size(&self) -> usize {
        self.state.size() + std::mem::size_of::<AtomicI64>()
    }

    fn is_fetching(&self) -> bool {
        matches!(self.state, CacheEntryState::Fetching(_))
    }

    fn is_success(&self) -> bool {
        matches!(self.state, CacheEntryState::Success(_))
    }
}

/// The state of a cache entry
///
/// This implements `Clone` so that a reference to the entry in the `Cache` does not need to be
/// held for long.
#[derive(Debug, Clone)]
enum CacheEntryState {
    /// The cache entry is being fetched from object store
    Fetching(SharedCacheValueFuture),
    /// The cache entry was successfully fetched and is stored in the cache as a [`CacheValue`]
    Success(Arc<CacheValue>),
}

impl CacheEntryState {
    /// Get the approximate size of the cache entry in bytes
    fn size(&self) -> usize {
        match self {
            CacheEntryState::Fetching(_) => 0,
            CacheEntryState::Success(v) => v.size(),
        }
    }

    /// Get the value in this state, or wait for it if it is still fetching
    ///
    /// This takes `self` as it is meant to be used on an entry's state that has been cloned.
    async fn value(self) -> object_store::Result<Arc<CacheValue>> {
        match self {
            CacheEntryState::Fetching(fut) => fut.await.map_err(|e| Error::Generic {
                store: STORE_NAME,
                source: Box::new(e),
            }),
            CacheEntryState::Success(v) => Ok(v),
        }
    }
}

/// An item that stores what is needed for pruning [`CacheEntry`]s
#[derive(Debug, Eq)]
struct PruneHeapItem {
    /// Reference to the entry's `Path` key
    path_ref: Arc<str>,
    /// Entry's hit time for comparison and heap insertion
    hit_time: i64,
    /// Entry size used to calculate the amount of memory freed after a prune
    size: usize,
}

impl PartialEq for PruneHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.hit_time.eq(&other.hit_time)
    }
}

impl PartialOrd for PruneHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.hit_time.cmp(&other.hit_time))
    }
}

impl Ord for PruneHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hit_time.cmp(&other.hit_time)
    }
}

/// Placeholder name for formatting datafusion errors
const STORE_NAME: &str = "mem_cached_object_store";

/// An object store with an associated cache that can serve GET-style requests using the cache
#[derive(Debug)]
pub struct MemCachedObjectStore {
    /// An inner object store for which items will be cached
    inner: Arc<dyn ObjectStore>,
    cache: Arc<dyn CacheProvider>,
}

#[derive(Debug)]
pub struct MemCachedObjectStoreArgs {
    pub time_provider: Arc<dyn TimeProvider>,
    pub metric_registry: Arc<Registry>,
    pub inner: Arc<dyn ObjectStore>,
    pub memory_capacity: usize,
    pub prune_percent: f64,
    pub query_cache_duration: Duration,
}

impl MemCachedObjectStore {
    /// Create a new [`MemCachedObjectStore`]
    fn new(
        MemCachedObjectStoreArgs {
            time_provider,
            metric_registry,
            inner,
            memory_capacity,
            prune_percent,
            query_cache_duration,
        }: MemCachedObjectStoreArgs,
    ) -> Self {
        Self {
            inner,
            cache: Arc::new(Cache::new(
                memory_capacity,
                prune_percent,
                Arc::clone(&time_provider),
                metric_registry,
                query_cache_duration,
            )),
        }
    }

    /// new with linked map
    fn new_with_linked_map(
        MemCachedObjectStoreArgs {
            time_provider,
            metric_registry,
            inner,
            memory_capacity,
            prune_percent,
            query_cache_duration,
        }: MemCachedObjectStoreArgs,
    ) -> Self {
        Self {
            inner,
            cache: Arc::new(data_store::safe_linked_map_cache::Cache::new(
                memory_capacity,
                prune_percent,
                Arc::clone(&time_provider),
                metric_registry,
                query_cache_duration,
            )),
        }
    }
}

impl std::fmt::Display for MemCachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemCachedObjectStore({})", self.inner)
    }
}

/// [`MemCachedObjectStore`] implements most [`ObjectStore`] methods as a pass-through, since
/// caching is decided externally. The exception is `delete`, which will have the entry removed
/// from the cache if the delete to the object store was successful.
///
/// GET-style methods will first check the cache for the object at the given path, before forwarding
/// to the inner [`ObjectStore`]. They do not, however, populate the cache after data has been fetched
/// from the inner store.
#[async_trait]
impl ObjectStore for MemCachedObjectStore {
    async fn put(&self, location: &Path, bytes: PutPayload) -> object_store::Result<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    /// Get an object from the object store. If this object is cached, then it will not make a request
    /// to the inner object store.
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        if let Some(state) = self.cache.get(location) {
            let v = state.value().await?;
            Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::iter([Ok(v.data.clone())]).boxed(),
                ),
                meta: v.meta.clone(),
                range: 0..v.data.len(),
                attributes: Default::default(),
            })
        } else {
            self.inner.get(location).await
        }
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // NOTE(trevor): this could probably be supported through the cache if we need it via the
        // ObjectMeta stored in the cache. For now this is conservative:
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        Ok(self
            .get_ranges(location, &[range])
            .await?
            .into_iter()
            .next()
            .expect("requested one range"))
    }

    /// This request is used by DataFusion when requesting metadata for Parquet files, so we need
    /// to use the cache to prevent excess network calls during query planning.
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> object_store::Result<Vec<Bytes>> {
        if let Some(state) = self.cache.get(location) {
            let v = state.value().await?;
            ranges
                .iter()
                .map(|range| {
                    if range.end > v.data.len() {
                        return Err(Error::Generic {
                            store: STORE_NAME,
                            source: format!(
                                "Range end ({}) out of bounds, object size is {}",
                                range.end,
                                v.data.len()
                            )
                            .into(),
                        });
                    }
                    if range.start > range.end {
                        return Err(Error::Generic {
                            store: STORE_NAME,
                            source: format!(
                                "Range end ({}) is before range start ({})",
                                range.end, range.start
                            )
                            .into(),
                        });
                    }
                    Ok(v.data.slice(range.clone()))
                })
                .collect()
        } else {
            self.inner.get_ranges(location, ranges).await
        }
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        if let Some(state) = self.cache.get(location) {
            let v = state.value().await?;
            Ok(v.meta.clone())
        } else {
            self.inner.head(location).await
        }
    }

    /// Delete an object on object store, but also remove it from the cache.
    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let result = self.inner.delete(location).await?;
        self.cache.remove(location);
        Ok(result)
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        locations
            .and_then(|_| futures::future::err(Error::NotImplemented))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

/// Handle [`CacheRequest`]s in a background task
///
/// This waits on the given `Receiver` for new eventual cache requests to be registered (via oracle).
/// If a cache request is received for an entry that has not already been fetched successfully, or
/// one that is in the process of being fetched, then this will spin a separate background task to
/// fetch the object from object store and update the cache. This is so that cache requests can be
/// handled in parallel.
fn background_cache_request_handler(
    mem_store: Arc<MemCachedObjectStore>,
    mut rx: Receiver<EventualCacheRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(cache_request) = rx.recv().await {
            let (path, notifier, file_timestamp_min_max) =
                cache_request.get_path_and_notifier_and_timestamp();

            if !should_request_be_cached(file_timestamp_min_max, &mem_store.cache) {
                debug!(?path, ">>> not caching parquet file path");
                let _ = notifier.send(());
                continue;
            }

            debug!(?path, ">>> caching parquet file path");
            // Create a future that will go and fetch the cache value from the store:
            let path_cloned = path.clone();
            let store_cloned = Arc::clone(&mem_store.inner);
            let fut = async move {
                CacheValue::fetch(store_cloned, path_cloned)
                    .await
                    .map(Arc::new)
                    .map_err(|e| Arc::new(e) as _)
            }
            .boxed()
            .shared();
            // Put a `Fetching` state in the entry to prevent concurrent requests to the same path:
            mem_store.cache.set_fetching(&path, fut.clone());
            let mem_store_captured = Arc::clone(&mem_store);
            tokio::spawn(async move {
                match fut.await {
                    Ok(value) => {
                        if let Err(error) = mem_store_captured.cache.set_success(&path, value) {
                            // NOTE(trevor): this would be an error if A) it tried to insert on an already
                            // successful entry, or B) it tried to insert on an empty entry, in either case
                            // we do not need to remove the entry to clear a fetching state, as in the
                            // other failure modes below...
                            warn!(%error, "failed to set the success state on the cache");
                        };
                    }
                    Err(error) => {
                        error!(%error, "failed to fulfill cache request with object store");
                        mem_store_captured.cache.remove(&path);
                    }
                };
                // notify that the cache request has been fulfilled, if receiver has been
                // dropped this error is not bubbled up
                let _ = notifier.send(());
            });
        }
        info!("cache request handler closed");
    })
}

fn should_request_be_cached(
    file_timestamp_min_max: Option<TimestampMinMax>,
    cache: &Arc<dyn CacheProvider>,
) -> bool {
    // If there's a timestamp range, check if there's capacity to add these
    // files. These are currently expected to come through from query path
    // which could be fetching older file. Check it's within allowed interval
    // before adding to cache
    file_timestamp_min_max
        .map(|file_timestamp_min_max| {
            if cache.get_used() < cache.get_capacity() {
                // TODO pass time_provider
                let end = cache.get_time_provider().now();
                let start = end - cache.get_query_cache_duration();
                let allowed_time_range =
                    TimestampRange::new(start.timestamp_nanos(), end.timestamp_nanos());
                debug!(
                    ?file_timestamp_min_max,
                    ?allowed_time_range,
                    ">>> parquet file timestamp min max to cache"
                );

                file_timestamp_min_max.overlaps(allowed_time_range)
            } else {
                false
            }
        })
        .unwrap_or(true)
}

/// A background task for pruning un-needed entries in the cache
#[allow(dead_code)]
fn background_cache_pruner(
    mem_store: Arc<MemCachedObjectStore>,
    prune_notifier_tx: watch::Sender<usize>,
    interval_duration: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!(">>> background cache pruning running");
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Some(freed) = mem_store.cache.prune() {
                debug!(freed, ">>> number of bytes freed in prune cycle");
                let _ = prune_notifier_tx.send(freed);
            }
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{sync::Arc, time::Duration};

    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use data_types::TimestampMinMax;
    use influxdb3_test_helpers::object_store::{
        RequestCountedObjectStore, SynchronizedObjectStore,
    };
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::{Attributes, Metric, Registry, U64Counter, U64Gauge};
    use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload, PutResult};

    use pretty_assertions::assert_eq;
    use tokio::sync::Notify;

    use crate::parquet_cache::{
        create_cached_obj_store_and_oracle,
        data_store::CacheProvider,
        metrics::{CACHE_ACCESS_NAME, CACHE_SIZE_BYTES_NAME, CACHE_SIZE_N_FILES_NAME},
        should_request_be_cached, test_cached_obj_store_and_oracle, Cache, CacheRequest,
        ParquetFileDataToCache,
    };

    macro_rules! assert_payload_at_equals {
        ($store:ident, $expected:ident, $path:ident) => {
            assert_eq!(
                $expected,
                $store
                    .get(&$path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .to_byte_slice()
            )
        };
    }

    #[tokio::test]
    async fn hit_cache_instead_of_object_store_eventual() {
        // set up the inner test object store and then wrap it with the mem cached store:
        let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (cached_store, oracle) = test_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider),
            Default::default(),
        );
        // PUT a paylaod into the object store through the outer mem cached store:
        let path = Path::from("0.parquet");
        let payload = b"hello world";
        cached_store
            .put(&path, PutPayload::from_static(payload))
            .await
            .unwrap();

        // GET the payload from the object store before caching:
        assert_payload_at_equals!(cached_store, payload, path);
        assert_eq!(1, inner_store.total_read_request_count(&path));

        // cache the entry:
        let (cache_request, notifier_rx) =
            CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
        oracle.register(cache_request);

        // wait for cache notify:
        let _ = notifier_rx.await;

        // another request to inner store should have been made:
        assert_eq!(2, inner_store.total_read_request_count(&path));

        // get the payload from the outer store again:
        assert_payload_at_equals!(cached_store, payload, path);

        // should hit the cache this time, so the inner store should not have been hit, and counts
        // should therefore be same as previous:
        assert_eq!(2, inner_store.total_read_request_count(&path));
    }

    #[test_log::test(tokio::test)]
    async fn hit_cache_instead_of_object_store_immediate() {
        // set up the inner test object store and then wrap it with the mem cached store:
        let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (cached_store, oracle) = test_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider),
            Default::default(),
        );
        let path = Path::from("0.parquet");
        let payload = b"hello world";

        let put_result = PutResult {
            e_tag: Some("some-etag".to_string()),
            version: Some("version-abc".to_string()),
        };

        let to_cache = ParquetFileDataToCache::new(
            &path,
            time_provider.now().date_time(),
            Bytes::from_static(payload),
            put_result,
        );

        // cache the entry:
        let cache_request =
            CacheRequest::create_immediate_mode_cache_request(path.clone(), to_cache);
        oracle.register(cache_request);

        let _ = cached_store.get(&path).await;
        // create request to inner store, this data is not in object store
        assert_eq!(0, inner_store.total_read_request_count(&path));

        // get the payload from the outer store and it should exist
        assert_payload_at_equals!(cached_store, payload, path);
    }

    #[test_log::test(tokio::test)]
    async fn hit_cache_instead_of_object_store_immediate_and_eventual() {
        // set up the inner test object store and then wrap it with the mem cached store:
        let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (cached_store, oracle) = test_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider),
            Default::default(),
        );
        let path_1_eventually_cached = Path::from("0.parquet");
        let payload_1_eventually_cached = b"hello world";

        let path_2_immediately_cached = Path::from("1.parquet");
        let payload_2_immediately_cached = b"good-bye world";

        // Add file to object store
        cached_store
            .put(
                &path_1_eventually_cached,
                PutPayload::from_static(payload_1_eventually_cached),
            )
            .await
            .unwrap();

        // GET the payload from the object store before caching:
        assert_payload_at_equals!(
            cached_store,
            payload_1_eventually_cached,
            path_1_eventually_cached
        );
        assert_eq!(
            1,
            inner_store.total_read_request_count(&path_1_eventually_cached)
        );

        // prepare for immediate request
        let put_result = PutResult {
            e_tag: Some("some-etag".to_string()),
            version: Some("version-abc".to_string()),
        };
        let to_cache = ParquetFileDataToCache::new(
            &path_2_immediately_cached,
            time_provider.now().date_time(),
            Bytes::from_static(payload_2_immediately_cached),
            put_result,
        );
        // Do an immediate cache request to other file
        let immediate_cache_request = CacheRequest::create_immediate_mode_cache_request(
            path_2_immediately_cached.clone(),
            to_cache,
        );
        oracle.register(immediate_cache_request);

        // Now try to cache 1st path eventually
        let (cache_request, notifier_rx) = CacheRequest::create_eventual_mode_cache_request(
            path_1_eventually_cached.clone(),
            None,
        );
        oracle.register(cache_request);

        // Oracle should've fulfilled the immediate cache request
        assert_payload_at_equals!(
            cached_store,
            payload_2_immediately_cached,
            path_2_immediately_cached
        );

        // Now wait for eventual request to finish
        let _ = notifier_rx.await;

        // Because eventual mode request went through, there will be another GET request to inner
        // store
        assert_eq!(
            2,
            inner_store.total_read_request_count(&path_1_eventually_cached)
        );

        // get the payload from the outer store again:
        assert_payload_at_equals!(
            cached_store,
            payload_1_eventually_cached,
            path_1_eventually_cached
        );

        // should hit the cache this time, so the inner store should not have been hit, and counts
        // should therefore be same as previous:
        assert_eq!(
            2,
            inner_store.total_read_request_count(&path_1_eventually_cached)
        );

        // Now try to cache 1st path again eventually
        let (cache_request, notifier_rx) = CacheRequest::create_eventual_mode_cache_request(
            path_1_eventually_cached.clone(),
            None,
        );
        oracle.register(cache_request);
        // should resolve immediately as path is already in cache
        let _ = notifier_rx.await;

        // no changes to inner store
        assert_eq!(
            2,
            inner_store.total_read_request_count(&path_1_eventually_cached)
        );

        // Try caching 2nd path (previously immediately cached)
        let (cache_request, notifier_rx) = CacheRequest::create_eventual_mode_cache_request(
            path_2_immediately_cached.clone(),
            None,
        );
        oracle.register(cache_request);
        // should resolve immediately as path is already in cache
        let _ = notifier_rx.await;

        // again - no changes to inner store
        assert_eq!(
            2,
            inner_store.total_read_request_count(&path_1_eventually_cached)
        );
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
    async fn cache_evicts_lru_when_full() {
        let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // these are magic numbers that will make it so the third entry exceeds the cache capacity:
        let cache_capacity_bytes = 20;
        let cache_prune_percent = 0.4;
        let cache_prune_interval = Duration::from_millis(10);
        let (cached_store, oracle) = create_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
            Default::default(),
            cache_capacity_bytes,
            Duration::from_millis(10),
            cache_prune_percent,
            cache_prune_interval,
        );
        // PUT an entry into the store:
        let path_1 = Path::from("0.parquet");
        let payload_1 = b"Janeway";
        cached_store
            .put(&path_1, PutPayload::from_static(payload_1))
            .await
            .unwrap();

        // cache the entry and wait for it to complete:
        let (cache_request, notifier_rx) =
            CacheRequest::create_eventual_mode_cache_request(path_1.clone(), None);
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // there will have been one get request made by the cache oracle:
        assert_eq!(1, inner_store.total_read_request_count(&path_1));

        // GET the entry to check its there and was retrieved from cache, i.e., that the request
        // counts do not change:
        assert_payload_at_equals!(cached_store, payload_1, path_1);
        assert_eq!(1, inner_store.total_read_request_count(&path_1));

        // PUT a second entry into the store:
        let path_2 = Path::from("1.parquet");
        let payload_2 = b"Paris";
        cached_store
            .put(&path_2, PutPayload::from_static(payload_2))
            .await
            .unwrap();

        // cache the second entry and wait for it to complete, this will not evict the first entry
        // as both can fit in the cache:
        let (cache_request, notifier_rx) =
            CacheRequest::create_eventual_mode_cache_request(path_2.clone(), None);
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // will have another request for the second path to the inner store, by the oracle:
        assert_eq!(1, inner_store.total_read_request_count(&path_1));
        assert_eq!(1, inner_store.total_read_request_count(&path_2));

        // GET the second entry and assert that it was retrieved from the cache, i.e., that the
        // request counts do not change:
        assert_payload_at_equals!(cached_store, payload_2, path_2);
        assert_eq!(1, inner_store.total_read_request_count(&path_1));
        assert_eq!(1, inner_store.total_read_request_count(&path_2));

        // GET the first entry again and assert that it was retrieved from the cache as before. This
        // will also update the hit count so that the first entry (janeway) was used more recently
        // than the second entry (paris):
        assert_payload_at_equals!(cached_store, payload_1, path_1);
        assert_eq!(1, inner_store.total_read_request_count(&path_1));
        assert_eq!(1, inner_store.total_read_request_count(&path_2));

        // before adding this, access path_2

        // PUT a third entry into the store:
        let path_3 = Path::from("2.parquet");
        let payload_3 = b"Neelix";
        cached_store
            .put(&path_3, PutPayload::from_static(payload_3))
            .await
            .unwrap();

        // cache the third entry and wait for it to complete, this will push the cache past its
        // capacity:
        let (cache_request, notifier_rx) =
            CacheRequest::create_eventual_mode_cache_request(path_3.clone(), None);
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // will now have another request for the third path to the inner store, by the oracle:
        assert_eq!(1, inner_store.total_read_request_count(&path_1));
        assert_eq!(1, inner_store.total_read_request_count(&path_2));
        assert_eq!(1, inner_store.total_read_request_count(&path_3));

        // GET the new entry from the strore, and check that it was served by the cache:
        assert_payload_at_equals!(cached_store, payload_3, path_3);
        assert_eq!(1, inner_store.total_read_request_count(&path_1));
        assert_eq!(1, inner_store.total_read_request_count(&path_2));
        assert_eq!(1, inner_store.total_read_request_count(&path_3));

        // prune_notifier.changed().await.unwrap();
        // assert_eq!(23, *prune_notifier.borrow_and_update());

        // GET paris from the cached store, this will not be served by the cache, because paris was
        // evicted by neelix:
        assert_payload_at_equals!(cached_store, payload_2, path_2);
        assert_eq!(1, inner_store.total_read_request_count(&path_1));
        assert_eq!(2, inner_store.total_read_request_count(&path_2));
        assert_eq!(1, inner_store.total_read_request_count(&path_3));
    }

    #[test_log::test(tokio::test)]
    async fn cache_hit_while_fetching() {
        // Create the object store with the following layers:
        // Synchronized -> RequestCounted -> Inner
        let to_store_notify = Arc::new(Notify::new());
        let from_store_notify = Arc::new(Notify::new());
        let counter = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let inner_store = Arc::new(
            SynchronizedObjectStore::new(Arc::clone(&counter) as _)
                .with_get_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
        );
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (cached_store, oracle) = test_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
            Default::default(),
        );

        // PUT an entry into the store:
        let path = Path::from("0.parquet");
        let payload = b"Picard";
        cached_store
            .put(&path, PutPayload::from_static(payload))
            .await
            .unwrap();

        // cache the entry, but don't wait on it until below in spawned task:
        let (cache_request, notifier_rx) =
            CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
        oracle.register(cache_request);

        // we are in the middle of a get request, i.e., the cache entry is "fetching"
        // once this call to notified wakes:
        let _ = from_store_notify.notified().await;

        // spawn a thread to wake the in-flight get request initiated by the cache oracle
        // after we have started a get request below, such that the get request below hits
        // the cache while the entry is still "fetching" state:
        let h = tokio::spawn(async move {
            to_store_notify.notify_one();
            let _ = notifier_rx.await;
        });

        // make the request to the store, which hits the cache in the "fetching" state
        // since we haven't made the call to notify the store to continue yet:
        assert_payload_at_equals!(cached_store, payload, path);

        // drive the task to completion to ensure that the cache request has been fulfilled:
        h.await.unwrap();

        // there should only have been one request made, i.e., from the cache oracle:
        assert_eq!(1, counter.total_read_request_count(&path));

        // make another request to the store, to be sure that it is in the cache:
        assert_payload_at_equals!(cached_store, payload, path);
        assert_eq!(1, counter.total_read_request_count(&path));
    }

    struct MetricVerifier {
        access_metrics: Metric<U64Counter>,
        size_mb_metrics: Metric<U64Gauge>,
        size_n_files_metrics: Metric<U64Gauge>,
    }

    impl MetricVerifier {
        fn new(metric_registry: Arc<Registry>) -> Self {
            let access_metrics = metric_registry
                .get_instrument::<Metric<U64Counter>>(CACHE_ACCESS_NAME)
                .unwrap();
            let size_mb_metrics = metric_registry
                .get_instrument::<Metric<U64Gauge>>(CACHE_SIZE_BYTES_NAME)
                .unwrap();
            let size_n_files_metrics = metric_registry
                .get_instrument::<Metric<U64Gauge>>(CACHE_SIZE_N_FILES_NAME)
                .unwrap();
            Self {
                access_metrics,
                size_mb_metrics,
                size_n_files_metrics,
            }
        }

        fn assert_access(
            &self,
            hits_expected: u64,
            misses_expected: u64,
            misses_while_fetching_expected: u64,
        ) {
            let hits_actual = self
                .access_metrics
                .get_observer(&Attributes::from(&[("status", "cached")]))
                .unwrap()
                .fetch();
            let misses_actual = self
                .access_metrics
                .get_observer(&Attributes::from(&[("status", "miss")]))
                .unwrap()
                .fetch();
            let misses_while_fetching_actual = self
                .access_metrics
                .get_observer(&Attributes::from(&[("status", "miss_while_fetching")]))
                .unwrap()
                .fetch();
            assert_eq!(
                hits_actual, hits_expected,
                "cache hits did not match expectation"
            );
            assert_eq!(
                misses_actual, misses_expected,
                "cache misses did not match expectation"
            );
            assert_eq!(
                misses_while_fetching_actual, misses_while_fetching_expected,
                "cache misses while fetching did not match expectation"
            );
        }

        fn assert_size(&self, size_bytes_expected: u64, size_n_files_expected: u64) {
            let size_bytes_actual = self
                .size_mb_metrics
                .get_observer(&Attributes::from(&[]))
                .unwrap()
                .fetch();
            let size_n_files_actual = self
                .size_n_files_metrics
                .get_observer(&Attributes::from(&[]))
                .unwrap()
                .fetch();
            assert_eq!(
                size_bytes_actual, size_bytes_expected,
                "cache size in bytes did not match actual"
            );
            assert_eq!(
                size_n_files_actual, size_n_files_expected,
                "cache size in number of files did not match actual"
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn cache_metrics() {
        // test setup
        let to_store_notify = Arc::new(Notify::new());
        let from_store_notify = Arc::new(Notify::new());
        let counted_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let inner_store = Arc::new(
            SynchronizedObjectStore::new(Arc::clone(&counted_store) as _)
                .with_get_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
        );
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Arc::new(Registry::new());
        let (cached_store, oracle) = test_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
            Arc::clone(&metric_registry),
        );
        let metric_verifier = MetricVerifier::new(metric_registry);

        // put something in the object store:
        let path = Path::from("0.parquet");
        let payload = b"Janeway";
        cached_store
            .put(&path, PutPayload::from_static(payload))
            .await
            .unwrap();

        // spin off a task to make a request to the object store on a separate thread. We will drive
        // the notifiers from here, as we just need the request to go through to register a cache
        // miss.
        let cached_store_cloned = Arc::clone(&cached_store);
        let path_cloned = path.clone();
        let h = tokio::spawn(async move {
            assert_payload_at_equals!(cached_store_cloned, payload, path_cloned);
        });

        // drive the synchronized store using the notifiers:
        from_store_notify.notified().await;
        to_store_notify.notify_one();
        h.await.unwrap();

        // check that there is a single cache miss:
        metric_verifier.assert_access(0, 1, 0);
        // nothing in the cache so sizes are 0
        metric_verifier.assert_size(0, 0);

        // there should be a single request made to the inner counted store from above:
        assert_eq!(1, counted_store.total_read_request_count(&path));

        // have the cache oracle cache the object:
        let (cache_request, notifier_rx) =
            CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
        oracle.register(cache_request);

        // we are in the middle of a get request, i.e., the cache entry is "fetching" once this
        // call to notified wakes:
        let _ = from_store_notify.notified().await;
        // just a fetching entry in the cache, so it will have n files of 1 and 8 bytes for the atomic
        // i64
        metric_verifier.assert_size(8, 1);

        // spawn a thread to wake the in-flight get request initiated by the cache oracle after we
        // have started a get request below, such that the get request below hits the cache while
        // the entry is still in the "fetching" state:
        let h = tokio::spawn(async move {
            to_store_notify.notify_one();
            let _ = notifier_rx.await;
        });

        // make the request to the store, which hits the cache in the "fetching" state since we
        // haven't made the call to notify the store to continue yet:
        assert_payload_at_equals!(cached_store, payload, path);

        // check that there is a single miss while fetching, note, the metrics are cumulative, so
        // the original miss is still there:
        metric_verifier.assert_access(0, 1, 1);

        // drive the task to completion to ensure that the cache request has been fulfilled:
        h.await.unwrap();

        // there should only have been two requests made, i.e., one from the request before the
        // object was cached, and one from the cache oracle:
        assert_eq!(2, counted_store.total_read_request_count(&path));

        // make another request, this time, it should use the cache:
        assert_payload_at_equals!(cached_store, payload, path);

        // there have now been one of each access metric:
        metric_verifier.assert_access(1, 1, 1);
        // now the cache has a the full entry, which includes the atomic i64, the metadata, and
        // the payload itself:
        metric_verifier.assert_size(25, 1);

        cached_store.delete(&path).await.unwrap();
        // removing the entry should bring the cache sizes back to zero:
        metric_verifier.assert_size(0, 0);
    }

    #[test_log::test(test)]
    fn test_should_request_be_cached_partial_overlap_of_file_time() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(100)));
        let max_size_bytes = 100;
        let cache: Arc<dyn CacheProvider> = Arc::new(Cache::new(
            max_size_bytes,
            0.1,
            Arc::clone(&time_provider),
            Arc::new(Registry::new()),
            Duration::from_nanos(100),
        ));

        let file_timestamp_min_max = Some(TimestampMinMax::new(0, 100));
        let should_cache = should_request_be_cached(file_timestamp_min_max, &cache);
        assert!(should_cache);
    }

    #[test_log::test(test)]
    fn test_should_request_be_cached_no_overlap_of_file_time() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(1000)));
        let max_size_bytes = 100;
        let cache: Arc<dyn CacheProvider> = Arc::new(Cache::new(
            max_size_bytes,
            0.1,
            Arc::clone(&time_provider),
            Arc::new(Registry::new()),
            Duration::from_nanos(100),
        ));

        let file_timestamp_min_max = Some(TimestampMinMax::new(0, 100));
        let should_cache = should_request_be_cached(file_timestamp_min_max, &cache);
        assert!(!should_cache);
    }

    #[test_log::test(test)]
    fn test_should_request_be_cached_no_timestamp_set() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(1000)));
        let max_size_bytes = 100;
        let cache: Arc<dyn CacheProvider> = Arc::new(Cache::new(
            max_size_bytes,
            0.1,
            Arc::clone(&time_provider),
            Arc::new(Registry::new()),
            Duration::from_nanos(100),
        ));

        let file_timestamp_min_max = Some(TimestampMinMax::new(0, 100));
        let should_cache = should_request_be_cached(file_timestamp_min_max, &cache);
        assert!(!should_cache);
    }
}
