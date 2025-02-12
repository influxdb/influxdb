use std::sync::Arc;

use bytes::Bytes;
use hashlink::LinkedHashMap;
use observability_deps::tracing::debug;

#[allow(dead_code)]
struct LruCache {
    map: LinkedHashMap<Arc<str>, Bytes>,
    max_capacity_bytes: u64,
    curr_capacity_bytes: u64,
}

#[allow(dead_code)]
impl LruCache {
    pub(crate) fn new(capacity_bytes: u64) -> Self {
        LruCache {
            map: LinkedHashMap::new(),
            max_capacity_bytes: capacity_bytes,
            curr_capacity_bytes: 0,
        }
    }

    pub(crate) fn put(&mut self, key: Arc<str>, val: Bytes) {
        let new_val_size = val.len() as u64;
        if self.curr_capacity_bytes + new_val_size > self.max_capacity_bytes {
            let mut to_deduct = self.curr_capacity_bytes + new_val_size - self.max_capacity_bytes;
            while to_deduct > 0 && !self.map.is_empty() {
                // need to drop elements
                if let Some((popped_key, popped_val)) = self.map.pop_front() {
                    debug!(
                        ?popped_key,
                        ">>> removed key from parquet cache to reclaim space"
                    );
                    to_deduct -= popped_val.len() as u64;
                }
            }
        }
        // at this point there should be enough space to add new val
        self.curr_capacity_bytes += val.len() as u64;
        self.map.insert(key, val);
    }

    pub(crate) fn get(&mut self, key: Arc<str>) -> Option<Bytes> {
        if let Some(val) = self.map.get(&key).cloned() {
            self.map.to_back(&key);
            return Some(val);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use observability_deps::tracing::debug;

    use crate::parquet_cache::experimental::safe_lru::LruCache;

    #[test_log::test(test)]
    #[ignore]
    fn test_safe_lru() {
        let mut cache = LruCache::new(100);
        let key_1 = Arc::from("/some/path_1");
        cache.put(Arc::clone(&key_1), Bytes::from_static(b"hello"));
        debug!("Running test");
        let key_2 = Arc::from("/some/path_2");
        let text_2 = Bytes::from_static(
            r#"
            Lorem Ipsum
            "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit..."
            "There is no one who loves pain itself, who seeks after it and wants to have it, simply because it is pain...""#.as_bytes()
        );
        debug!("Running test 2");
        cache.put(Arc::clone(&key_2), text_2);
        let val = cache.get(Arc::clone(&key_1));

        debug!("Running test 3");
        debug!(?val, ">>> from get");
        let val = cache.get(Arc::clone(&key_2));
        debug!(?val, ">>> from get");

        let val = cache.get(Arc::clone(&key_1));
        // this should be none
        debug!(?val, ">>> from get");
    }
}
