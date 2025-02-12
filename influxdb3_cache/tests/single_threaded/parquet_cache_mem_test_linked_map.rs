use std::sync::Arc;

use bytes::Bytes;
use dhat::assert;
use influxdb3_cache::parquet_cache::{
    test_cached_obj_store_and_oracle_with_size_linked_map, CacheRequest, ParquetCacheOracle,
    ParquetFileDataToCache,
};
use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
use iox_time::{MockProvider, Time, TimeProvider};
use metric::Registry;
use object_store::{memory::InMemory, path::Path, ObjectStore, PutResult};
use observability_deps::tracing::debug;

fn produce_data(kib: usize) -> Vec<u8> {
    let chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        .chars()
        .map(|c| c as u8)
        .cycle();
    let data: Vec<u8> = chars.take(1024 * kib).collect();
    data
}

#[test_log::test(tokio::test)]
async fn run_memory_consumption_test_linked_map() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let counted_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let ten_mib = 10 * 1024 * 1024;
    let _twelve_mb = 12 * 1024 * 1024;
    // 50 kb data
    let data: Vec<u8> = produce_data(50);
    // 75 kb data
    let data_2: Vec<u8> = produce_data(75);
    let metric_registry = Arc::new(Registry::new());
    // NOTE: only difference to the other test is this function call, everything
    //       else remains the same
    let (store, oracle) = test_cached_obj_store_and_oracle_with_size_linked_map(
        counted_store,
        Arc::clone(&time_provider) as _,
        metric_registry,
        ten_mib,
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
        // we don't need to sleep
        // tokio::time::sleep(Duration::from_millis(10)).await;
        // time_provider.set(Time::from_timestamp_nanos(i + 10));
    }

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
    debug!(%stats.total_bytes, %cache_total_bytes, %num_paths_in_cache, ">>> test: result");
    debug!(%stats.total_bytes, ">>> test: num bytes in cache");
    assert!(cache_total_bytes < ten_mib);
    // This condition is dhat max mem of 10 MB, it goes slightly
    // above 10MB, but the actual cache itself never goes beyond
    // 10 MB
    let padding_bytes = 200 * 1024; // 200 kb
    dhat::assert!(stats.max_bytes < ten_mib + padding_bytes);
}
