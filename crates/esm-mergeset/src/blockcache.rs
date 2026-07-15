//! Port of `lib/blockcache` (v1.146.0): a sharded, size-bounded cache for
//! decoded index/data blocks.
//!
//! Deviations from Go:
//!
//! - `Key.Part` (pointer identity of the owning part) is replaced with a
//!   `part_id: u64` assigned from a process-global counter at `Part`
//!   construction (see `part.rs`).
//! - Go's `Block` interface becomes the [`CachedBlock`] trait and the cache
//!   is generic over the cached value (`Cache<V>`); values are shared as
//!   `Arc<V>` clones instead of interface pointers.
//! - Go runs a background cleaner goroutine per cache with jittered tickers
//!   (~1 minute: drop entries idle for more than 3 minutes; ~3 minutes:
//!   reset the per-key miss counters). Here cleaning is **amortized into
//!   shard accesses** instead: every `get`/`put` on a shard first runs any
//!   cleaning whose per-shard deadline has passed. This avoids background
//!   thread lifecycle management (there is no `MustStop`), at the cost of an
//!   idle shard retaining its entries until the next access. The size budget
//!   is still enforced on every insert and the mergeset caches see constant
//!   traffic, so the difference is immaterial in practice.
//! - The number of shards is a fixed `next_power_of_two(available CPUs)`
//!   (Go uses `cpus * min(cpus, 16)`), so the shard for a key can be picked
//!   with a mask.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

/// The number of cache misses for a key before the block is actually stored
/// in the cache (Go flag `-blockcache.missesBeforeCaching`, default 2).
///
/// Blocks with up to this many misses aren't cached in order to prevent
/// one-time-wonder scans from evicting frequently accessed items.
pub const MISSES_BEFORE_CACHING: u32 = 2;

/// How often a shard drops idle entries (Go: jittered `time.Minute`).
const CLEAN_INTERVAL_SECS: u64 = 60;

/// Upper bound on entries freed per clean trigger, so an idle sweep can
/// never stall shard readers (observed as multi-ms tail latency on
/// Windows, where heap frees are expensive).
const MAX_CLEAN_PER_TRIGGER: usize = 64;

/// How often a shard resets its per-key miss counters
/// (Go: jittered `3 * time.Minute`).
const PER_KEY_MISSES_CLEAN_INTERVAL_SECS: u64 = 3 * 60;

/// Entries idle for longer than this are dropped by the periodic cleaning.
/// "This time should be enough for repeated queries" (Go comment).
const MAX_IDLE_SECS: u64 = 3 * 60;

/// A value that may be stored in a [`Cache`].
pub trait CachedBlock: Send + Sync + 'static {
    /// Returns the approximate size of the block in bytes.
    fn size_bytes(&self) -> usize;
}

/// Uniquely identifies a cached block.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Key {
    /// Unique id of the part the block belongs to
    /// (Go uses the part pointer).
    pub part_id: u64,

    /// The offset of the block in the part.
    pub offset: u64,
}

fn mix64(mut z: u64) -> u64 {
    // SplitMix64 finalizer.
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Key {
    /// Replaces Go's xxhash over the raw struct bytes; only shard selection
    /// depends on it.
    fn hash_u64(&self) -> u64 {
        mix64(mix64(self.part_id.wrapping_add(0x9E37_79B9_7F4A_7C15)) ^ self.offset)
    }
}

/// Caches blocks of type `V`, keyed by [`Key`].
///
/// The total cache size in bytes is limited by the value returned by the
/// `get_max_size_bytes` callback passed to [`Cache::new`].
pub struct Cache<V: CachedBlock> {
    shards: Vec<Shard<V>>,
    get_max_size_bytes: Box<dyn Fn() -> usize + Send + Sync>,
    misses_before_caching: u32,
}

impl<V: CachedBlock> Cache<V> {
    pub fn new(get_max_size_bytes: impl Fn() -> usize + Send + Sync + 'static) -> Cache<V> {
        Cache::with_misses_before_caching(get_max_size_bytes, MISSES_BEFORE_CACHING)
    }

    /// Like [`Cache::new`], but with a custom miss threshold before a block
    /// is actually stored (`0` stores blocks on the first put).
    pub fn with_misses_before_caching(
        get_max_size_bytes: impl Fn() -> usize + Send + Sync + 'static,
        misses_before_caching: u32,
    ) -> Cache<V> {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let shards_count = cpus.next_power_of_two();
        let shards = (0..shards_count).map(|_| Shard::new()).collect();
        Cache {
            shards,
            get_max_size_bytes: Box::new(get_max_size_bytes),
            misses_before_caching,
        }
    }

    fn shard(&self, k: &Key) -> &Shard<V> {
        let idx = k.hash_u64() & (self.shards.len() as u64 - 1);
        &self.shards[idx as usize]
    }

    fn max_shard_bytes(&self) -> usize {
        (self.get_max_size_bytes)() / self.shards.len()
    }

    /// Returns the block for the given key, if cached.
    pub fn get_block(&self, k: &Key) -> Option<Arc<V>> {
        self.shard(k).get_block(k)
    }

    /// Puts the given block under the given key.
    ///
    /// Returns true if the block was added to the cache (either now or by a
    /// concurrent caller).
    pub fn try_put_block(&self, k: &Key, b: &Arc<V>) -> bool {
        let max_shard_bytes = self.max_shard_bytes();
        self.shard(k)
            .try_put_block(k, b, max_shard_bytes, self.misses_before_caching)
    }

    /// Removes all the blocks belonging to the given part.
    pub fn remove_blocks_for_part(&self, part_id: u64) {
        for shard in &self.shards {
            shard.remove_blocks_for_part(part_id);
        }
    }

    /// The number of cached blocks.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.inner.lock().heap.len()).sum()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An approximate size in bytes of all the cached blocks.
    pub fn size_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.size_bytes.load(Ordering::Relaxed).max(0) as usize)
            .sum()
    }

    /// The max allowed size in bytes (as enforced per shard).
    pub fn size_max_bytes(&self) -> usize {
        self.max_shard_bytes() * self.shards.len()
    }

    /// The number of requests served by the cache.
    pub fn requests(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.requests.load(Ordering::Relaxed))
            .sum()
    }

    /// The number of cache misses.
    pub fn misses(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.misses.load(Ordering::Relaxed))
            .sum()
    }

    /// Drops entries idle for more than [`MAX_IDLE_SECS`] from all shards.
    /// In production this happens amortized on shard access; tests call it
    /// directly.
    #[cfg(test)]
    fn clean_by_timeout(&self) {
        let now = esm_common::fasttime::unix_timestamp();
        for shard in &self.shards {
            let mut evicted = Vec::new();
            loop {
                let capped = shard.inner.lock().clean_by_timeout(
                    now,
                    &shard.size_bytes,
                    &mut evicted,
                    usize::MAX,
                );
                if !capped {
                    break;
                }
            }
        }
    }

    /// Resets the per-key miss counters on all shards.
    #[cfg(test)]
    fn clean_per_key_misses(&self) {
        for shard in &self.shards {
            shard.inner.lock().per_key_misses.clear();
        }
    }
}

struct Shard<V: CachedBlock> {
    requests: AtomicU64,
    misses: AtomicU64,

    /// An approximate size of all the blocks stored in the shard.
    ///
    /// Updated only while `inner` is locked, but read lock-free by
    /// [`Cache::size_bytes`].
    size_bytes: AtomicI64,

    inner: Mutex<ShardInner<V>>,
}

struct Entry<V> {
    /// The timestamp in seconds of the last access to this entry.
    last_access_time: u64,

    /// The position of this entry in `ShardInner::heap`.
    heap_idx: usize,

    key: Key,

    block: Arc<V>,

    /// Size captured at insertion time (cached blocks are immutable).
    size: usize,
}

struct ShardInner<V> {
    /// part_id -> offset -> slot index into `entries`.
    m: HashMap<u64, HashMap<u64, usize>>,

    /// Per-block cache misses; see [`MISSES_BEFORE_CACHING`].
    per_key_misses: HashMap<Key, u32>,

    /// Slab of entries; `None` slots are free and listed in `free`.
    entries: Vec<Option<Entry<V>>>,
    free: Vec<usize>,

    /// Binary min-heap of slot indices, ordered by the entries'
    /// `last_access_time` (Go's `lastAccessHeap`), for LRU eviction.
    heap: Vec<usize>,

    /// Deadlines (unix seconds) for the amortized cleaning.
    next_clean_deadline: u64,
    next_per_key_misses_clean_deadline: u64,
}

impl<V: CachedBlock> Shard<V> {
    fn new() -> Shard<V> {
        let now = esm_common::fasttime::unix_timestamp();
        Shard {
            requests: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            size_bytes: AtomicI64::new(0),
            inner: Mutex::new(ShardInner {
                m: HashMap::new(),
                per_key_misses: HashMap::new(),
                entries: Vec::new(),
                free: Vec::new(),
                heap: Vec::new(),
                next_clean_deadline: now + CLEAN_INTERVAL_SECS,
                next_per_key_misses_clean_deadline: now + PER_KEY_MISSES_CLEAN_INTERVAL_SECS,
            }),
        }
    }

    fn get_block(&self, k: &Key) -> Option<Arc<V>> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let now = esm_common::fasttime::unix_timestamp();
        let mut evicted = Vec::new();
        let mut inner = self.inner.lock();
        inner.maybe_clean(now, &self.size_bytes, &mut evicted);
        // Freed blocks are dropped after the lock is released (scopes below
        // return through `ret`).
        let ret = self.get_block_locked(&mut inner, k, now);
        drop(inner);
        drop(evicted);
        ret
    }

    fn get_block_locked(
        &self,
        inner: &mut parking_lot::MutexGuard<'_, ShardInner<V>>,
        k: &Key,
        now: u64,
    ) -> Option<Arc<V>> {
        let slot = inner.m.get(&k.part_id).and_then(|pes| pes.get(&k.offset));
        if let Some(&slot) = slot {
            // Fast path - the block exists in the cache.
            let e = inner.entries[slot]
                .as_mut()
                .expect("BUG: cache map points to a free slot");
            let block = Arc::clone(&e.block);
            let heap_idx = e.heap_idx;
            let touched = e.last_access_time != now;
            if touched {
                e.last_access_time = now;
            }
            if touched {
                inner.heap_fix(heap_idx);
            }
            return Some(block);
        }

        // Slow path - the entry is missing in the cache.
        *inner.per_key_misses.entry(*k).or_insert(0) += 1;
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn try_put_block(
        &self,
        k: &Key,
        b: &Arc<V>,
        max_shard_bytes: usize,
        misses_before_caching: u32,
    ) -> bool {
        let now = esm_common::fasttime::unix_timestamp();
        let mut evicted = Vec::new();
        let mut inner = self.inner.lock();
        inner.maybe_clean(now, &self.size_bytes, &mut evicted);

        let misses = inner.per_key_misses.get(k).copied().unwrap_or(0);
        if misses > 0 && misses <= misses_before_caching {
            // If the entry wasn't accessed yet (misses == 0), then cache it,
            // since it has been just created without consulting the cache and
            // will be accessed soon.
            //
            // Do not cache the entry if there were up to MISSES_BEFORE_CACHING
            // unsuccessful attempts to access it. This may be a
            // one-time-wonder entry, which won't be accessed anymore, so do
            // not cache it in order to save memory for frequent items.
            return false;
        }

        // Reset the key misses counter; this helps reducing memory usage
        // on fast cache eviction.
        inner.per_key_misses.insert(*k, 0);

        if inner
            .m
            .get(&k.part_id)
            .is_some_and(|pes| pes.contains_key(&k.offset))
        {
            // The block has been already registered by a concurrent thread.
            return true;
        }

        // Store b in the cache.
        let size = b.size_bytes();
        let slot = inner.alloc_slot(Entry {
            last_access_time: now,
            heap_idx: 0,
            key: *k,
            block: Arc::clone(b),
            size,
        });
        inner.heap_push(slot);
        inner.m.entry(k.part_id).or_default().insert(k.offset, slot);
        self.size_bytes.fetch_add(size as i64, Ordering::Relaxed);

        while self.size_bytes.load(Ordering::Relaxed) > max_shard_bytes as i64
            && !inner.heap.is_empty()
        {
            let block = inner.remove_least_recently_accessed(&self.size_bytes);
            evicted.push(block);
        }
        drop(inner);
        // Evicted blocks are dropped here, outside the shard lock.
        drop(evicted);
        true
    }

    fn remove_blocks_for_part(&self, part_id: u64) {
        let mut removed = Vec::new();
        let mut inner = self.inner.lock();
        let Some(pes) = inner.m.remove(&part_id) else {
            return;
        };
        let mut removed_size = 0i64;
        for (_, slot) in pes {
            let heap_idx = inner.entries[slot]
                .as_ref()
                .expect("BUG: cache map points to a free slot")
                .heap_idx;
            inner.heap_remove_at(heap_idx);
            let e = inner.free_slot(slot);
            removed_size += e.size as i64;
            removed.push(e.block);
            // Do not delete the key from per_key_misses; the periodic
            // cleaning resets the map (as in Go).
        }
        self.size_bytes.fetch_add(-removed_size, Ordering::Relaxed);
        drop(inner);
        // The removed blocks are dropped here, outside the shard lock, so
        // concurrent readers of this shard don't stall on the frees.
        drop(removed);
    }
}

impl<V: CachedBlock> ShardInner<V> {
    fn maybe_clean(&mut self, now: u64, size_bytes: &AtomicI64, evicted: &mut Vec<Arc<V>>) {
        if now >= self.next_clean_deadline {
            // Cap the work done per trigger so a large idle sweep cannot
            // stall the readers of this shard: clean at most
            // MAX_CLEAN_PER_TRIGGER entries, and come back a second later
            // for the rest. Freed blocks are handed to the caller and
            // dropped after the shard lock is released.
            let capped = self.clean_by_timeout(now, size_bytes, evicted, MAX_CLEAN_PER_TRIGGER);
            self.next_clean_deadline = if capped {
                now + 1
            } else {
                now + CLEAN_INTERVAL_SECS
            };
        }
        if now >= self.next_per_key_misses_clean_deadline {
            self.next_per_key_misses_clean_deadline = now + PER_KEY_MISSES_CLEAN_INTERVAL_SECS;
            // clear() keeps capacity; Key/u32 are Copy, so this frees
            // nothing under the lock.
            self.per_key_misses.clear();
        }
    }

    /// Deletes entries accessed more than [`MAX_IDLE_SECS`] ago.
    /// Returns true when the per-trigger cap was hit (more idle entries
    /// remain).
    fn clean_by_timeout(
        &mut self,
        now: u64,
        size_bytes: &AtomicI64,
        evicted: &mut Vec<Arc<V>>,
        max_entries: usize,
    ) -> bool {
        let last_access_deadline = now.saturating_sub(MAX_IDLE_SECS);
        let mut n = 0;
        while let Some(&slot) = self.heap.first() {
            let t = self.entries[slot]
                .as_ref()
                .expect("BUG: heap points to a free slot")
                .last_access_time;
            if last_access_deadline < t {
                return false;
            }
            if n >= max_entries {
                return true;
            }
            evicted.push(self.remove_least_recently_accessed(size_bytes));
            n += 1;
        }
        false
    }

    fn remove_least_recently_accessed(&mut self, size_bytes: &AtomicI64) -> Arc<V> {
        let slot = self.heap_remove_at(0);
        let e = self.free_slot(slot);
        size_bytes.fetch_add(-(e.size as i64), Ordering::Relaxed);
        let pes = self
            .m
            .get_mut(&e.key.part_id)
            .expect("BUG: evicted entry is missing from the cache map");
        pes.remove(&e.key.offset);
        if pes.is_empty() {
            // Remove the per-part map in order to free up the memory it
            // occupies.
            self.m.remove(&e.key.part_id);
        }
        e.block
    }

    // --- slab ---

    fn alloc_slot(&mut self, e: Entry<V>) -> usize {
        match self.free.pop() {
            Some(slot) => {
                self.entries[slot] = Some(e);
                slot
            }
            None => {
                self.entries.push(Some(e));
                self.entries.len() - 1
            }
        }
    }

    fn free_slot(&mut self, slot: usize) -> Entry<V> {
        let e = self.entries[slot]
            .take()
            .expect("BUG: freeing an already free slot");
        self.free.push(slot);
        e
    }

    // --- last-access min-heap (Go's lastAccessHeap) ---

    fn heap_less(&self, i: usize, j: usize) -> bool {
        let a = self.entries[self.heap[i]]
            .as_ref()
            .unwrap()
            .last_access_time;
        let b = self.entries[self.heap[j]]
            .as_ref()
            .unwrap()
            .last_access_time;
        a < b
    }

    fn heap_swap(&mut self, i: usize, j: usize) {
        self.heap.swap(i, j);
        let si = self.heap[i];
        self.entries[si].as_mut().unwrap().heap_idx = i;
        let sj = self.heap[j];
        self.entries[sj].as_mut().unwrap().heap_idx = j;
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if !self.heap_less(i, parent) {
                break;
            }
            self.heap_swap(i, parent);
            i = parent;
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        loop {
            let left = 2 * i + 1;
            if left >= self.heap.len() {
                break;
            }
            let mut smallest = left;
            let right = left + 1;
            if right < self.heap.len() && self.heap_less(right, left) {
                smallest = right;
            }
            if !self.heap_less(smallest, i) {
                break;
            }
            self.heap_swap(i, smallest);
            i = smallest;
        }
    }

    fn heap_push(&mut self, slot: usize) {
        let i = self.heap.len();
        self.entries[slot].as_mut().unwrap().heap_idx = i;
        self.heap.push(slot);
        self.sift_up(i);
    }

    /// Restores the heap invariant after `heap[i]`'s access time changed.
    fn heap_fix(&mut self, i: usize) {
        self.sift_down(i);
        self.sift_up(i);
    }

    /// Removes the heap element at position `i` and returns its slot index.
    /// Does NOT free the slot.
    fn heap_remove_at(&mut self, i: usize) -> usize {
        let last = self.heap.len() - 1;
        if i != last {
            self.heap_swap(i, last);
        }
        let slot = self.heap.pop().expect("BUG: removing from an empty heap");
        if i < self.heap.len() {
            self.heap_fix(i);
        }
        slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestBlock;

    impl CachedBlock for TestBlock {
        fn size_bytes(&self) -> usize {
            42
        }
    }

    /// Port of Go `TestCache`.
    #[test]
    fn test_cache() {
        // 1 MiB is divisible by any power-of-two shard count we may get.
        let size_max_bytes = 1024 * 1024;
        let c: Cache<TestBlock> = Cache::new(move || size_max_bytes);
        assert_eq!(c.size_bytes(), 0);
        assert_eq!(c.size_max_bytes(), size_max_bytes);

        let offset = 1234u64;
        let part_id = 1u64;
        let k = Key { part_id, offset };
        let b = Arc::new(TestBlock);
        let block_size = b.size_bytes();

        // Put a single entry into the cache.
        c.try_put_block(&k, &b);
        assert_eq!(c.len(), 1);
        assert_eq!(c.size_bytes(), block_size);
        assert_eq!(c.requests(), 0);
        assert_eq!(c.misses(), 0);

        // Obtain this entry from the cache.
        let b1 = c.get_block(&k).expect("expected a cached block");
        assert!(Arc::ptr_eq(&b1, &b), "unexpected block obtained");
        assert_eq!(c.requests(), 1);
        assert_eq!(c.misses(), 0);

        // Obtain a non-existing entry from the cache.
        assert!(c
            .get_block(&Key {
                part_id: 0,
                offset: offset + 1
            })
            .is_none());
        assert_eq!(c.requests(), 2);
        assert_eq!(c.misses(), 1);

        // Remove entries for the given part from the cache.
        c.remove_blocks_for_part(part_id);
        assert_eq!(c.size_bytes(), 0);
        assert_eq!(c.len(), 0);

        // Verify that the entry has been removed from the cache.
        assert!(c.get_block(&k).is_none());
        assert_eq!(c.requests(), 3);
        assert_eq!(c.misses(), 2);

        for i in 0..MISSES_BEFORE_CACHING as u64 {
            // Store the missed entry to the cache. It shouldn't be stored
            // because of the previous cache misses.
            assert!(!c.try_put_block(&k, &b));
            assert_eq!(c.size_bytes(), 0);
            // Verify that the entry wasn't stored to the cache.
            assert!(c.get_block(&k).is_none());
            assert_eq!(c.requests(), 4 + i);
            assert_eq!(c.misses(), 3 + i);
        }

        // Store the entry again. Now it must be stored because of the
        // exceeded miss count.
        assert!(c.try_put_block(&k, &b));
        assert_eq!(c.size_bytes(), block_size);
        let b1 = c.get_block(&k).expect("expected a cached block");
        assert!(Arc::ptr_eq(&b1, &b));
        assert_eq!(c.requests(), 4 + MISSES_BEFORE_CACHING as u64);
        assert_eq!(c.misses(), 2 + MISSES_BEFORE_CACHING as u64);

        // Manually clean the cache. The entry shouldn't be deleted because it
        // was recently accessed.
        c.clean_per_key_misses();
        c.clean_by_timeout();
        assert_eq!(c.size_bytes(), block_size);
    }

    /// Port of Go `TestCacheConcurrentAccess`.
    #[test]
    fn test_cache_concurrent_access() {
        const SIZE_MAX_BYTES: usize = 16 * 1024 * 1024;
        let c: Cache<TestBlock> = Cache::new(|| SIZE_MAX_BYTES);

        std::thread::scope(|s| {
            for worker in 0..5u64 {
                let c = &c;
                s.spawn(move || {
                    for i in 0..1000u64 {
                        let k = Key {
                            part_id: i,
                            offset: worker * 1000 + i,
                        };
                        let b = Arc::new(TestBlock);
                        c.try_put_block(&k, &b);
                        let b1 = c.get_block(&k).expect("expected a cached block");
                        assert!(Arc::ptr_eq(&b1, &b), "unexpected block obtained");
                        assert!(c
                            .get_block(&Key {
                                part_id: u64::MAX,
                                offset: 0
                            })
                            .is_none());
                    }
                });
            }
        });
    }

    struct BigBlock;

    impl CachedBlock for BigBlock {
        fn size_bytes(&self) -> usize {
            512
        }
    }

    #[test]
    fn test_cache_size_budget_eviction() {
        // Give each shard room for two 512-byte blocks.
        let shards = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .next_power_of_two();
        let budget = shards * 1024;
        let c: Cache<BigBlock> = Cache::new(move || budget);
        assert_eq!(c.shards.len(), shards);

        let entries_count = 100 * shards;
        for i in 0..entries_count as u64 {
            let k = Key {
                part_id: i,
                offset: i,
            };
            assert!(c.try_put_block(&k, &Arc::new(BigBlock)));
        }
        // Each shard evicts down to its per-shard budget, so the total size
        // stays within the total budget while some entries remain cached.
        assert!(
            c.size_bytes() <= budget,
            "size {} exceeds the budget {budget}",
            c.size_bytes()
        );
        assert!(c.len() < entries_count, "no entries were evicted");
        assert!(!c.is_empty(), "all entries were evicted");
    }

    #[test]
    fn test_cache_remove_blocks_for_part_keeps_other_parts() {
        let c: Cache<TestBlock> = Cache::new(|| 1024 * 1024);
        for part_id in 0..4u64 {
            for offset in 0..10u64 {
                assert!(c.try_put_block(&Key { part_id, offset }, &Arc::new(TestBlock)));
            }
        }
        assert_eq!(c.len(), 40);
        c.remove_blocks_for_part(2);
        assert_eq!(c.len(), 30);
        assert_eq!(c.size_bytes(), 30 * 42);
        for offset in 0..10u64 {
            assert!(c.get_block(&Key { part_id: 2, offset }).is_none());
            assert!(c.get_block(&Key { part_id: 3, offset }).is_some());
        }
        // Removing a part with no cached blocks is a no-op.
        c.remove_blocks_for_part(1234);
        assert_eq!(c.len(), 30);
    }
}
