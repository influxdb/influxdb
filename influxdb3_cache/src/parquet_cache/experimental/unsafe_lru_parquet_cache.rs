// First variation - using unsafe

use std::{
    ptr::NonNull,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use observability_deps::tracing::debug;

// The idea here is to create a map that points to the [`CacheNode`] to allow O(1) access
// based on the key (path). Also create a pointer that points to the same node by adding it
// to the head of a linked list. This means we can keep a consistent view of LRU as we add
// or remove least recently used items.
//
// Whenever an item is accessed or added the node which holds the pointer is moved to the
// head of the list. This in turn means the tail of the list always is pointing to the
// least used. Usually getting to an item in linked list is O(n), but to avoid that we hold
// a map which points to the node so this allows to get hold of [`CacheNode`] in O(1)
// through the map and the `CacheNode` itself holds next/prev, it can be detached and moved
// in O(1) as well
//
// The eviction for this cache is probably a bit naive it steps through the list and drops
// it in a loop until it can bring down the cache size to be
//   (new_cache_item_size + total_size_mb) < max_size_mb
// This works pretty well when size of files are larger but runs poorly when size of files
// are really small. Eg. trying to save a 1 GB file with 100_000 1 kb files will mean it'd
// roughly require 1000 cycles to bring the buffer down to accommodate 1 gb file. This can
// be done in the background but that just moves the problem and will still lead to
// contention of locks.
#[allow(dead_code)]
pub(crate) struct LruCache {
    map: hashbrown::HashMap<Arc<str>, Option<NonNull<CacheNode>>>,
    // should they be cache padded? We need to move nodes from middle
    // of the list to head.
    head: Option<NonNull<CacheNode>>,
    tail: Option<NonNull<CacheNode>>,
    total_size_mb: AtomicU64,
    max_size_in_mb: u64,
}

#[allow(dead_code)]
impl LruCache {
    pub(crate) fn new(max_size_in_mb: u64) -> Self {
        LruCache {
            map: hashbrown::HashMap::new(),
            head: None,
            tail: None,
            total_size_mb: AtomicU64::new(0),
            max_size_in_mb,
        }
    }

    pub(crate) fn put(&mut self, key: Arc<str>, bytes: Bytes) {
        // The keys we're working with and files they're pointing to are immutable
        // for a given path it'd always be same file so we don't really need to
        // overwrite
        if !self.map.contains_key(&key) {
            // first check if there's enough space
            let new_total = bytes.len() as u64 + self.total_size_mb.load(Ordering::SeqCst);
            if new_total > self.max_size_in_mb {
                // not enough space - we need to evict
                let mut to_deduct = new_total - self.max_size_in_mb;
                debug!(?to_deduct, ">>> need to evict bytes");

                while to_deduct > 0 && self.tail.is_some() {
                    let num_bytes_popped = self.pop_tail();
                    debug!(?num_bytes_popped, ">>> evicted one");
                    to_deduct = to_deduct.saturating_sub(num_bytes_popped)
                }
            }

            let node = Box::new(CacheNode {
                key: Arc::clone(&key),
                value: Some(bytes),
                next: None,
                prev: None,
            });

            let node_ptr = NonNull::new(Box::into_raw(node));
            self.map.insert(key, node_ptr);
            // this should go immediately to replace head (as it's the most recent)
            self.push_head(node_ptr);
        }
    }

    pub(crate) fn get(&mut self, key: Arc<str>) -> Option<Bytes> {
        if let Some(node_ptr) = &self.map.get(&key) {
            if let Some(node) = node_ptr.as_ref() {
                // SAFETY: ??
                // clone() here is ok, these are Bytes::clone (cheap)
                let value = unsafe { node.as_ref().value.clone() };
                // now that we know we can access it, we need to remove it from
                // list and move it to head
                self.detach_node_from_list(*node);
                self.push_head(Some(*node));

                return value;
            }
        }
        None
    }

    fn detach_node_from_list(&self, node_ptr: NonNull<CacheNode>) {
        // find previous' next and point to current's next
        // find next's prev and point to current's prev
        unsafe {
            let curr_prev = node_ptr.as_ref().prev;
            let curr_next = node_ptr.as_ref().next;

            if let Some(mut prev) = curr_prev {
                prev.as_mut().next = curr_next;
            }

            if let Some(mut next) = curr_next {
                next.as_mut().prev = curr_prev;
            }
        }
    }

    fn push_head(&mut self, node_ptr: Option<NonNull<CacheNode>>) {
        if let Some(mut node_ptr) = node_ptr {
            unsafe {
                node_ptr.as_mut().next = self.head;
                if let Some(mut curr_head) = self.head {
                    curr_head.as_mut().prev = Some(node_ptr);
                }
            }

            if self.tail.is_none() {
                self.tail = Some(node_ptr);
            }
        }
    }

    fn pop_tail(&mut self) -> u64 {
        let mut bytes_popped = 0;
        if let Some(mut current) = self.tail {
            unsafe {
                let prev = current.as_mut().prev;
                // if prev is present make that current
                if let Some(mut prev) = prev {
                    prev.as_mut().next = None;
                    if let Some(bytes) = current.as_ref().value.as_ref() {
                        bytes_popped = bytes.len() as u64;
                    }
                } else {
                    // just need to pop current
                    if let Some(bytes) = current.as_ref().value.as_ref() {
                        bytes_popped = bytes.len() as u64;
                    }
                }
                self.map.remove(&current.as_ref().key);
                current.drop_in_place();
                self.tail = None;
            }
        }
        debug!(bytes_popped, ">>> bytes popped");
        bytes_popped
    }
}

impl Drop for LruCache {
    fn drop(&mut self) {
        let mut current = self.head;
        while let Some(mut node) = current {
            unsafe {
                current = node.as_mut().next;
                drop(Box::from_raw(node.as_ptr()));
            }
        }
    }
}

/// Cache node
struct CacheNode {
    key: Arc<str>,
    value: Option<Bytes>,
    next: Option<NonNull<CacheNode>>,
    prev: Option<NonNull<CacheNode>>,
}

impl CacheNode {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use observability_deps::tracing::debug;

    use crate::parquet_cache::experimental::unsafe_lru_parquet_cache::LruCache;

    #[test_log::test(test)]
    #[ignore]
    fn test_unsafe_lru() {
        let mut cache = LruCache::new(1);
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
