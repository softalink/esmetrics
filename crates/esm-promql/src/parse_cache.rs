//! Cache for parsed (and optimized) MetricsQL expressions.
//! Port of `parse_cache.go`: 128 buckets keyed by a hash of the query, at
//! most ~10k entries in total; on bucket overflow a random ~10% of the
//! bucket entries are evicted. Both successful parses and parse errors are
//! cached; the cached AST is post-`Optimize` + `adjustCmpOps`.

use esm_metricsql::Expr;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

const BUCKETS_COUNT: usize = 128;
const MAX_CACHE_LEN: usize = 10_000;
/// Entries to evict per bucket on overflow (~10% of the per-bucket cap).
const DELETE_PER_BUCKET: usize = MAX_CACHE_LEN / BUCKETS_COUNT / 10 + 1;

/// Cached parse outcome.
pub type ParseCacheValue = Result<Arc<Expr>, crate::Error>;

pub struct ParseCache {
    buckets: Vec<RwLock<HashMap<String, ParseCacheValue>>>,
}

impl Default for ParseCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseCache {
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(BUCKETS_COUNT);
        for _ in 0..BUCKETS_COUNT {
            buckets.push(RwLock::new(HashMap::new()));
        }
        ParseCache { buckets }
    }

    fn bucket(&self, q: &str) -> &RwLock<HashMap<String, ParseCacheValue>> {
        // FNV-1a; the Go original uses xxhash — any stable hash works here.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in q.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        &self.buckets[(h % BUCKETS_COUNT as u64) as usize]
    }

    pub fn get(&self, q: &str) -> Option<ParseCacheValue> {
        self.bucket(q).read().get(q).cloned()
    }

    pub fn put(&self, q: &str, v: ParseCacheValue) {
        let mut bucket = self.bucket(q).write();
        if bucket.len() >= MAX_CACHE_LEN / BUCKETS_COUNT {
            // Overflow: evict an arbitrary ~10% of this bucket
            // (HashMap iteration order plays the role of Go's random
            // map-range eviction).
            let victims: Vec<String> = bucket.keys().take(DELETE_PER_BUCKET).cloned().collect();
            for k in victims {
                bucket.remove(&k);
            }
        }
        bucket.insert(q.to_string(), v);
    }

    #[cfg(test)]
    fn entry_count(&self) -> usize {
        self.buckets.iter().map(|b| b.read().len()).sum()
    }
}

/// Global parse cache used by [`crate::exec::exec`].
pub fn parse_cache() -> &'static ParseCache {
    use std::sync::OnceLock;
    static CACHE: OnceLock<ParseCache> = OnceLock::new();
    CACHE.get_or_init(ParseCache::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let c = ParseCache::new();
        assert!(c.get("foo").is_none());
        let e = esm_metricsql::parse("foo{bar=\"baz\"}").unwrap();
        c.put("foo", Ok(Arc::new(e)));
        assert!(matches!(c.get("foo"), Some(Ok(_))));
        c.put("bad", Err(crate::Error::new("parse error")));
        assert!(matches!(c.get("bad"), Some(Err(_))));
        assert_eq!(c.entry_count(), 2);
    }

    #[test]
    fn overflow_eviction_keeps_cache_bounded() {
        let c = ParseCache::new();
        for i in 0..MAX_CACHE_LEN * 2 {
            c.put(&format!("query_{i}"), Err(crate::Error::new("x")));
        }
        assert!(c.entry_count() <= MAX_CACHE_LEN + BUCKETS_COUNT);
    }
}
