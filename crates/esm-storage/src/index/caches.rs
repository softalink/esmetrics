//! Caches used by the indexDB layer.
//!
//! - [`WorkingSetCache`] — a byte-keyed, size-accounted cache with two
//!   generations (curr/prev) that are rotated when curr exceeds half of the
//!   maximum size. This mirrors the behavior of the upstream VictoriaMetrics
//!   `lib/workingsetcache` / `lib/lrucache` closely enough for the index
//!   layer: recently used entries survive one rotation, stale entries are
//!   dropped after two.
//!   Used for `tagFiltersToMetricIDsCache` (values are `Arc<Set>`),
//!   `loopsPerDateTagFilterCache` (24-byte values) and — by stage 4 — for
//!   the storage-level `metricIDCache`/`metricNameCache`/`tsidCache`.
//! - [`MetricIdCache`] — port of `metric_id_cache.go`: a membership set of
//!   metricIDs known to exist in an indexDB (ingestion-only).
//! - [`DateMetricIdCache`] — port of `date_metric_id_cache.go`: a membership
//!   set of (date, metricID) entries (ingestion-only).
//!
//! Go rotates the two ingestion caches on a timer (one shard per ~1min/~1h
//! tick); this port rotates them by size instead, which keeps the same
//! two-generation working-set shape without background threads. Eviction
//! fidelity is a memory concern, not a correctness one (spec §3.6).

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use esm_common::uint64set::Set;
use hashbrown::HashTable;
use parking_lot::{Mutex, RwLock};
use xxhash_rust::xxh64::xxh64;

/// A trivial identity hasher for u64-keyed maps whose keys are already
/// well-distributed (dates, metricIDs).
#[derive(Default)]
pub(crate) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("IdentityHasher only supports u64 keys");
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        // Spread low-entropy keys (dates are small sequential integers).
        self.0 = v.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// Size accounting for values stored in a [`WorkingSetCache`].
pub trait EntrySize {
    /// An approximate in-memory size of the entry in bytes.
    fn entry_size(&self) -> u64;
}

impl EntrySize for Box<[u8]> {
    fn entry_size(&self) -> u64 {
        self.len() as u64
    }
}

impl EntrySize for Arc<Set> {
    fn entry_size(&self) -> u64 {
        self.size_bytes()
    }
}

impl EntrySize for crate::tsid::Tsid {
    fn entry_size(&self) -> u64 {
        std::mem::size_of::<crate::tsid::Tsid>() as u64
    }
}

/// Per-entry constant overhead used in size accounting (map entry, key box).
const ENTRY_OVERHEAD_BYTES: u64 = 48;

/// The number of independent shards of a [`WorkingSetCache`]. Reduces lock
/// contention when several ingestion threads hit the same cache (Go's
/// fastcache uses 512 shards; 16 is plenty for one lookup per row on
/// typical core counts).
const WSC_SHARDS: usize = 16;

/// A cache entry with its precomputed xxh64 key hash, so a `get`/`set` only
/// hashes the (~100-byte MetricNameRaw) key once for both the shard
/// selection and the table probes (Go's fastcache does the same).
struct WscEntry<V> {
    hash: u64,
    key: Box<[u8]>,
    value: V,
}

struct WscState<V> {
    curr: HashTable<WscEntry<V>>,
    prev: HashTable<WscEntry<V>>,
    curr_bytes: u64,
    prev_bytes: u64,
}

impl<V> Default for WscState<V> {
    fn default() -> Self {
        WscState {
            curr: HashTable::new(),
            prev: HashTable::new(),
            curr_bytes: 0,
            prev_bytes: 0,
        }
    }
}

/// A byte-keyed, sharded cache with two generations rotated when the current
/// generation of a shard exceeds half of its share of `max_bytes`.
pub struct WorkingSetCache<V> {
    shards: Vec<Mutex<WscState<V>>>,
    max_bytes_per_shard: u64,
    requests: AtomicU64,
    misses: AtomicU64,
    resets: AtomicU64,
}

impl<V: Clone + EntrySize> WorkingSetCache<V> {
    /// Creates a cache with the given maximum size in bytes.
    pub fn new(max_bytes: u64) -> WorkingSetCache<V> {
        WorkingSetCache {
            shards: (0..WSC_SHARDS).map(|_| Mutex::default()).collect(),
            max_bytes_per_shard: (max_bytes / WSC_SHARDS as u64).max(1),
            requests: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            resets: AtomicU64::new(0),
        }
    }

    /// Returns the value for the given key, or None on a cache miss.
    ///
    /// A hit in the previous generation promotes the entry into the current
    /// generation (working-set semantics).
    pub fn get(&self, key: &[u8]) -> Option<V> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let hash = xxh64(key, 0);
        let mut state = self.shards[(hash as usize) & (WSC_SHARDS - 1)].lock();
        if let Some(e) = state.curr.find(hash, |e| &*e.key == key) {
            return Some(e.value.clone());
        }
        if let Ok(entry) = state.prev.find_entry(hash, |e| &*e.key == key) {
            // Promote the entry from prev to curr.
            let (e, _) = entry.remove();
            let v = e.value.clone();
            let size = key.len() as u64 + v.entry_size() + ENTRY_OVERHEAD_BYTES;
            state.prev_bytes = state.prev_bytes.saturating_sub(size);
            Self::insert_locked(&mut state, e);
            state.curr_bytes += size;
            self.maybe_rotate_locked(&mut state);
            return Some(v);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Stores the value under the given key.
    pub fn set(&self, key: &[u8], value: V) {
        let size = key.len() as u64 + value.entry_size() + ENTRY_OVERHEAD_BYTES;
        let hash = xxh64(key, 0);
        let mut state = self.shards[(hash as usize) & (WSC_SHARDS - 1)].lock();
        state.curr_bytes += size;
        Self::insert_locked(
            &mut state,
            WscEntry {
                hash,
                key: key.into(),
                value,
            },
        );
        self.maybe_rotate_locked(&mut state);
    }

    /// Inserts (or replaces) the entry in the current generation.
    fn insert_locked(state: &mut WscState<V>, e: WscEntry<V>) {
        match state.curr.entry(e.hash, |o| o.key == e.key, |o| o.hash) {
            hashbrown::hash_table::Entry::Occupied(mut o) => {
                *o.get_mut() = e;
            }
            hashbrown::hash_table::Entry::Vacant(v) => {
                v.insert(e);
            }
        }
    }

    fn maybe_rotate_locked(&self, state: &mut WscState<V>) {
        if state.curr_bytes <= self.max_bytes_per_shard / 2 {
            return;
        }
        state.prev = std::mem::take(&mut state.curr);
        state.prev_bytes = state.curr_bytes;
        state.curr_bytes = 0;
    }

    /// Removes all the entries from the cache.
    pub fn reset(&self) {
        for shard in &self.shards {
            let mut state = shard.lock();
            state.curr.clear();
            state.prev.clear();
            state.curr_bytes = 0;
            state.prev_bytes = 0;
        }
        self.resets.fetch_add(1, Ordering::Relaxed);
    }

    /// The number of entries in the cache.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let state = shard.lock();
                state.curr.len() + state.prev.len()
            })
            .sum()
    }

    /// Returns true if the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An approximate size of the cache in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| {
                let state = shard.lock();
                state.curr_bytes + state.prev_bytes
            })
            .sum()
    }

    /// The maximum size of the cache in bytes.
    pub fn size_max_bytes(&self) -> u64 {
        self.max_bytes_per_shard * WSC_SHARDS as u64
    }

    /// The number of get requests served by the cache.
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// The number of cache misses.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// The number of cache resets.
    pub fn resets(&self) -> u64 {
        self.resets.load(Ordering::Relaxed)
    }
}

/// A byte-keyed byte-valued working-set cache (fastcache-style usage).
pub type BytesCache = WorkingSetCache<Box<[u8]>>;

// --- metricIDCache (metric_id_cache.go) ---

struct MetricIdCacheState {
    curr: Set,
    prev: Set,
}

/// A cache of metricIDs that exist in an indexDB. Ingestion-only.
///
/// Not populated on startup and never authoritative: a miss triggers a
/// `FirstItemWithPrefix` probe of the index (see `IndexSearch::has_metric_id`).
///
/// `has` is called once per ingested row from all the ingestion threads, so
/// the state is behind a `RwLock`: lookups only take the (shared) read lock.
pub struct MetricIdCache {
    state: RwLock<MetricIdCacheState>,
    max_items_per_generation: usize,
    rotations: AtomicU64,
}

impl MetricIdCache {
    /// Creates a cache. The generation size is derived from the allowed
    /// process memory (Go rotates by time instead; see the module docs).
    pub fn new() -> MetricIdCache {
        // ~16 bytes per metricID in a uint64set; use mem/256 bytes per
        // generation.
        let max_items = (esm_common::memory::allowed() / 256 / 16).max(1024);
        MetricIdCache {
            state: RwLock::new(MetricIdCacheState {
                curr: Set::default(),
                prev: Set::default(),
            }),
            max_items_per_generation: max_items,
            rotations: AtomicU64::new(0),
        }
    }

    /// Returns true if the cache contains the given metricID.
    pub fn has(&self, metric_id: u64) -> bool {
        let state = self.state.read();
        state.curr.has(metric_id) || state.prev.has(metric_id)
    }

    /// Adds the given metricID to the cache.
    pub fn set(&self, metric_id: u64) {
        let mut state = self.state.write();
        state.curr.add(metric_id);
        if state.curr.len() > self.max_items_per_generation {
            state.prev = std::mem::take(&mut state.curr);
            self.rotations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The number of metricIDs in the cache.
    pub fn len(&self) -> usize {
        let state = self.state.read();
        state.curr.len() + state.prev.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An approximate size of the cache in bytes.
    pub fn size_bytes(&self) -> u64 {
        let state = self.state.read();
        state.curr.size_bytes() + state.prev.size_bytes()
    }
}

impl Default for MetricIdCache {
    fn default() -> Self {
        Self::new()
    }
}

// --- dateMetricIDCache (date_metric_id_cache.go) ---

type DateMap = HashMap<u64, Set, BuildHasherDefault<IdentityHasher>>;

struct DateMetricIdCacheState {
    curr: DateMap,
    prev: DateMap,
    curr_items: usize,
}

/// A cache of (date, metricID) entries. Ingestion-only.
///
/// Like [`MetricIdCache`], lookups happen once per ingested row, so `has`
/// only takes the shared read lock (Go's byDateMetricIDMap is lock-free on
/// reads via an atomic pointer).
pub struct DateMetricIdCache {
    state: RwLock<DateMetricIdCacheState>,
    max_items_per_generation: usize,
    rotations: AtomicU64,
}

impl DateMetricIdCache {
    /// Creates a cache sized from the allowed process memory.
    pub fn new() -> DateMetricIdCache {
        let max_items = (esm_common::memory::allowed() / 256 / 16).max(1024);
        DateMetricIdCache {
            state: RwLock::new(DateMetricIdCacheState {
                curr: DateMap::default(),
                prev: DateMap::default(),
                curr_items: 0,
            }),
            max_items_per_generation: max_items,
            rotations: AtomicU64::new(0),
        }
    }

    /// Returns true if the cache contains the given (date, metricID) entry.
    pub fn has(&self, date: u64, metric_id: u64) -> bool {
        let state = self.state.read();
        if let Some(s) = state.curr.get(&date) {
            if s.has(metric_id) {
                return true;
            }
        }
        state.prev.get(&date).is_some_and(|s| s.has(metric_id))
    }

    /// Adds the given (date, metricID) entry to the cache.
    pub fn set(&self, date: u64, metric_id: u64) {
        let mut state = self.state.write();
        state.curr.entry(date).or_default().add(metric_id);
        state.curr_items += 1;
        if state.curr_items > self.max_items_per_generation {
            state.prev = std::mem::take(&mut state.curr);
            state.curr_items = 0;
            self.rotations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The total number of (date, metricID) entries in the cache.
    pub fn len(&self) -> usize {
        let state = self.state.read();
        let n: usize = state.curr.values().map(|s| s.len()).sum();
        let m: usize = state.prev.values().map(|s| s.len()).sum();
        n + m
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An approximate size of the cache in bytes.
    pub fn size_bytes(&self) -> u64 {
        let state = self.state.read();
        let n: u64 = state.curr.values().map(|s| s.size_bytes()).sum();
        let m: u64 = state.prev.values().map(|s| s.size_bytes()).sum();
        n + m
    }
}

impl Default for DateMetricIdCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn working_set_cache_get_set_promote() {
        let c: BytesCache = WorkingSetCache::new(1 << 20);
        assert!(c.get(b"foo").is_none());
        c.set(b"foo", b"bar".to_vec().into_boxed_slice());
        assert_eq!(c.get(b"foo").as_deref(), Some(&b"bar"[..]));
        assert_eq!(c.len(), 1);
        assert!(c.size_bytes() > 0);
        c.reset();
        assert!(c.get(b"foo").is_none());
        assert_eq!(c.resets(), 1);
    }

    #[test]
    fn working_set_cache_rotation_keeps_working_set() {
        // Sizes are accounted per shard: at most two entries fit a shard
        // before its generations rotate. Whether or not the keys share a
        // shard, recently set entries must stay visible (via prev +
        // promotion on get).
        let c: BytesCache = WorkingSetCache::new(220 * 16);
        c.set(b"a", vec![0u8; 4].into_boxed_slice());
        c.set(b"b", vec![0u8; 4].into_boxed_slice());
        assert!(c.get(b"a").is_some());
        assert!(c.get(b"b").is_some());

        // Tiny per-shard budget: every set/promotion rotates the shard.
        // A recently used entry survives each rotation via promotion.
        let c: BytesCache = WorkingSetCache::new(16);
        c.set(b"k", vec![0u8; 4].into_boxed_slice());
        assert!(c.get(b"k").is_some(), "entry must survive one rotation");
        assert!(c.get(b"k").is_some(), "promotion must keep the entry alive");
    }

    #[test]
    fn metric_id_cache_roundtrip() {
        let c = MetricIdCache::new();
        assert!(!c.has(123));
        c.set(123);
        assert!(c.has(123));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn date_metric_id_cache_roundtrip() {
        let c = DateMetricIdCache::new();
        assert!(!c.has(10, 123));
        c.set(10, 123);
        assert!(c.has(10, 123));
        assert!(!c.has(11, 123));
        assert_eq!(c.len(), 1);
    }
}
