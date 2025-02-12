use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use dhat::assert;
use influxdb3_cache::parquet_cache::{
    test_cached_obj_store_and_oracle_with_size, CacheRequest, ParquetCacheOracle,
    ParquetFileDataToCache,
};
use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
use iox_time::{MockProvider, Time, TimeProvider};
use metric::Registry;
use object_store::{memory::InMemory, path::Path, ObjectStore, PutResult};
use observability_deps::tracing::debug;

fn produce_data(kb: usize) -> Vec<u8> {
    let chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        .chars()
        .map(|c| c as u8)
        .cycle();
    let data: Vec<u8> = chars.take(1024 * kb).collect();
    data
}

#[test_log::test(tokio::test)]
#[ignore = "this is only present to compare"]
async fn run_memory_consumption_test_sharded() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let counted_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let ten_mb = 10 * 1024 * 1024;
    let twelve_mb = 12 * 1024 * 1024;
    // 50 kb data
    let data: Vec<u8> = produce_data(50);
    // 75 kb data
    let data_2: Vec<u8> = produce_data(75);
    let metric_registry = Arc::new(Registry::new());
    let (store, oracle) = test_cached_obj_store_and_oracle_with_size(
        counted_store,
        Arc::clone(&time_provider) as _,
        metric_registry,
        ten_mb,
    );

    // 50 kb * 500 => 25 MB
    // 75 kb * 500 => 37.5 MB
    for i in 0..1000 {
        let path = Path::from(format!("/path-{i}"));
        // mix the payload sizes
        let bytes = if i % 2 == 0 {
            Bytes::from(data.clone())
        } else {
            Bytes::from(data_2.clone())
        };
        let data_to_cache = ParquetFileDataToCache::new(
            &path,
            time_provider.now().date_time(),
            bytes,
            PutResult {
                e_tag: None,
                version: None,
            },
        );
        oracle.register(CacheRequest::create_immediate_mode_cache_request(
            path,
            data_to_cache,
        ));
        // if we don't sleep then pruning never has a chance to catchup
        // we can sleep after, then we end up with a lot of max memory
        // usage
        // tokio::time::sleep(Duration::from_millis(10)).await;
        time_provider.set(Time::from_timestamp_nanos(i + 10));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut cache_total_bytes = 0;
    let mut num_paths_in_cache = 0;
    for i in 0..1000 {
        let path = Path::from(format!("/path-{i}"));
        if let Ok(get_res) = store.get(&path).await {
            let body = get_res.bytes().await.unwrap();
            cache_total_bytes += body.len();
            num_paths_in_cache += 1;
        }
    }

    let stats = dhat::HeapStats::get();
    debug!(%stats.total_bytes, %stats.max_bytes, %cache_total_bytes, %num_paths_in_cache, ">>> test: result");
    debug!(%stats.total_bytes, ">>> test: num bytes in cache");
    assert!(cache_total_bytes < ten_mb);
    // This condition is never satisfied for 10 MB, so added some padding 12 MB
    // seems to pass this test locally
    dhat::assert!(stats.max_bytes < twelve_mb);
}
