use criterion::{BenchmarkId, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_cache_t5");
    group.bench_function(BenchmarkId::new("dashmap", 0), |b| {
        b.iter(|| {
            dashmap_based_cache_read_write();
        })
    });

    group.bench_function(BenchmarkId::new("linked_hashmap_safe", 0), |b| {
        b.iter(|| {
            dashmap_based_cache_read_write();
        })
    });

    group.bench_function(BenchmarkId::new("linked_hashmap_unsafe", 0), |b| {
        b.iter(|| {
            dashmap_based_cache_read_write();
        })
    });
    group.finish();
}

fn dashmap_based_cache_read_write() {}
