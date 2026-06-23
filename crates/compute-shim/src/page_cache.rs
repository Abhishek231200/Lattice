/// Local page cache in front of the pageserver — avoids a network round-trip for
/// recently accessed pages.
///
/// Simple LRU keyed by (rel, blk, lsn).  In the real system the LSN dimension is
/// handled differently (a single WAL position watermark), but for the demo an exact
/// key is sufficient.

use std::collections::HashMap;
use parking_lot::Mutex;
use bytes::Bytes;

use lattice_common::{RelTag, BlockNumber, Lsn, PageImage, PAGE_SIZE};

const DEFAULT_CAPACITY: usize = 1024; // 1024 pages = 8 MiB

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub rel: RelTag,
    pub blk: BlockNumber,
    pub lsn: Lsn,
}

pub struct PageCache {
    inner: Mutex<LruMap>,
    capacity: usize,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruMap::new(capacity)),
            capacity,
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<PageImage> {
        self.inner.lock().get(key)
    }

    pub fn put(&self, key: CacheKey, page: PageImage) {
        self.inner.lock().put(key, page);
    }

    pub fn invalidate(&self, rel: RelTag, blk: BlockNumber) {
        self.inner.lock().invalidate(rel, blk);
    }

    pub fn hit_rate(&self) -> f64 {
        let inner = self.inner.lock();
        let total = inner.hits + inner.misses;
        if total == 0 { 0.0 } else { inner.hits as f64 / total as f64 }
    }
}

struct LruMap {
    map: HashMap<CacheKey, (PageImage, u64)>,
    clock: u64,
    capacity: usize,
    hits: u64,
    misses: u64,
}

impl LruMap {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            clock: 0,
            capacity,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<PageImage> {
        if let Some((page, ts)) = self.map.get_mut(key) {
            self.clock += 1;
            *ts = self.clock;
            self.hits += 1;
            Some(page.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    fn put(&mut self, key: CacheKey, page: PageImage) {
        if self.map.len() >= self.capacity {
            // Evict the entry with the lowest timestamp (LRU approximation).
            if let Some(evict_key) = self.map.iter()
                .min_by_key(|(_, (_, ts))| *ts)
                .map(|(k, _)| k.clone())
            {
                self.map.remove(&evict_key);
            }
        }
        self.clock += 1;
        self.map.insert(key, (page, self.clock));
    }

    fn invalidate(&mut self, rel: RelTag, blk: BlockNumber) {
        self.map.retain(|k, _| !(k.rel == rel && k.blk == blk));
    }
}
