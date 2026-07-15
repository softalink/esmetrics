//! Port of the `rawItemsShards`/`rawItemsShard` machinery from `table.go`.

use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::inmemory_block::{InmemoryBlock, MAX_INMEMORY_BLOCK_SIZE};
use crate::util::available_cpus;

/// The interval for flushing buffered items to parts, so they become visible
/// to search.
pub(crate) const PENDING_ITEMS_FLUSH_INTERVAL_MS: i64 = 1000;

/// The default maximum number of blocks per shard.
const DEFAULT_MAX_BLOCKS_PER_SHARD: usize = 256;

// Test hooks (0 = use defaults). Set once per process before opening tables.
static RAW_ITEMS_SHARDS_PER_TABLE_OVERRIDE: AtomicUsize = AtomicUsize::new(0);
static MAX_BLOCKS_PER_SHARD_OVERRIDE: AtomicUsize = AtomicUsize::new(0);

/// Test-only hook overriding the number of shards per table and the maximum
/// number of raw blocks per shard. Pass 0 to keep a default.
#[doc(hidden)]
pub fn set_raw_items_shard_params_for_tests(shards_per_table: usize, max_blocks_per_shard: usize) {
    RAW_ITEMS_SHARDS_PER_TABLE_OVERRIDE.store(shards_per_table, Ordering::Relaxed);
    MAX_BLOCKS_PER_SHARD_OVERRIDE.store(max_blocks_per_shard, Ordering::Relaxed);
}

/// The number of shards for raw items per table.
///
/// Higher number of shards reduces CPU contention and increases the max
/// bandwidth on multi-core systems.
fn raw_items_shards_per_table() -> usize {
    let v = RAW_ITEMS_SHARDS_PER_TABLE_OVERRIDE.load(Ordering::Relaxed);
    if v > 0 {
        return v;
    }
    let cpus = available_cpus();
    cpus * cpus.min(16)
}

pub(crate) fn max_blocks_per_shard() -> usize {
    let v = MAX_BLOCKS_PER_SHARD_OVERRIDE.load(Ordering::Relaxed);
    if v > 0 {
        return v;
    }
    DEFAULT_MAX_BLOCKS_PER_SHARD
}

/// The total number of too-long items dropped from ingestion.
pub(crate) static TOO_LONG_ITEMS_TOTAL: AtomicU64 = AtomicU64::new(0);

// Timestamp (unix seconds) after which the next too-long-item error may be
// logged (5s throttling, mirroring Go's logger.WithThrottler).
static TOO_LONG_ITEM_LOG_DEADLINE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn log_too_long_item(item: &[u8]) {
    TOO_LONG_ITEMS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let now = esm_common::fasttime::unix_timestamp();
    let deadline = TOO_LONG_ITEM_LOG_DEADLINE.load(Ordering::Relaxed);
    if now >= deadline
        && TOO_LONG_ITEM_LOG_DEADLINE
            .compare_exchange(deadline, now + 5, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        let prefix = &item[..item.len().min(128)];
        log::error!(
            "skipping adding too long item to indexdb: len(item)={}; it shouldn't exceed {} bytes; item prefix={:?}",
            item.len(),
            MAX_INMEMORY_BLOCK_SIZE,
            String::from_utf8_lossy(prefix)
        );
    }
}

/// A single shard of recently added raw items.
///
/// The alignment prevents false sharing between shards.
#[repr(align(64))]
pub(crate) struct RawItemsShard {
    flush_deadline_ms: AtomicI64,
    ibs: Mutex<Vec<InmemoryBlock>>,
}

impl RawItemsShard {
    fn new() -> RawItemsShard {
        RawItemsShard {
            flush_deadline_ms: AtomicI64::new(0),
            ibs: Mutex::new(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        let ibs = self.ibs.lock();
        ibs.iter().map(|ib| ib.items.len()).sum()
    }

    fn update_flush_deadline(&self) {
        self.flush_deadline_ms.store(
            now_unix_ms() + PENDING_ITEMS_FLUSH_INTERVAL_MS,
            Ordering::Relaxed,
        );
    }

    /// Adds items to the shard.
    ///
    /// Returns the number of consumed items and, when the shard overflows,
    /// the accumulated blocks that must be flushed. The unconsumed tail
    /// items must be re-added (typically to another shard).
    pub fn add_items(&self, items: &[&[u8]]) -> (usize, Vec<InmemoryBlock>) {
        let mut ibs_to_flush: Vec<InmemoryBlock> = Vec::new();
        let mut consumed = items.len();

        let max_blocks = max_blocks_per_shard();
        let mut ibs = self.ibs.lock();
        if ibs.is_empty() {
            ibs.push(InmemoryBlock::default());
            self.update_flush_deadline();
        }
        for (i, item) in items.iter().enumerate() {
            let last = ibs.last_mut().expect("ibs cannot be empty");
            if last.add(item) {
                continue;
            }
            if ibs.len() >= max_blocks {
                ibs_to_flush = std::mem::take(&mut *ibs);
                consumed = i;
                break;
            }
            let mut ib = InmemoryBlock::default();
            if ib.add(item) {
                ibs.push(ib);
                continue;
            }

            // Skip too long item.
            log_too_long_item(item);
        }
        drop(ibs);

        (consumed, ibs_to_flush)
    }

    /// Appends the shard's blocks to `dst` if the shard's flush deadline has
    /// passed or `is_final` is set.
    pub fn append_blocks_to_flush(
        &self,
        dst: &mut Vec<InmemoryBlock>,
        current_time_ms: i64,
        is_final: bool,
    ) {
        let flush_deadline_ms = self.flush_deadline_ms.load(Ordering::Relaxed);
        if !is_final && current_time_ms < flush_deadline_ms {
            // Fast path - nothing to flush.
            return;
        }

        // Slow path - move the blocks to dst.
        let mut ibs = self.ibs.lock();
        dst.append(&mut ibs);
    }
}

/// Sharded buffer of recently added items that haven't been converted to
/// parts yet. These items aren't visible to search.
pub(crate) struct RawItemsShards {
    flush_deadline_ms: AtomicI64,
    shard_idx: AtomicU32,
    shards: Vec<RawItemsShard>,
    ibs_to_flush: Mutex<Vec<InmemoryBlock>>,
}

impl RawItemsShards {
    pub fn new() -> RawItemsShards {
        let n = raw_items_shards_per_table();
        RawItemsShards {
            flush_deadline_ms: AtomicI64::new(0),
            shard_idx: AtomicU32::new(0),
            shards: (0..n).map(|_| RawItemsShard::new()).collect(),
            ibs_to_flush: Mutex::new(Vec::new()),
        }
    }

    /// Picks the next shard round-robin and adds items to it.
    ///
    /// See [`RawItemsShard::add_items`] for the return contract.
    pub fn add_items_to_shard(&self, items: &[&[u8]]) -> (usize, Vec<InmemoryBlock>) {
        let n = self.shard_idx.fetch_add(1, Ordering::Relaxed) as usize;
        let idx = n % self.shards.len();
        self.shards[idx].add_items(items)
    }

    /// Accumulates overflowed shard blocks. Returns blocks that must be
    /// merged into in-memory parts once enough of them are accumulated.
    pub fn add_ibs_to_flush(&self, ibs: Vec<InmemoryBlock>) -> Vec<InmemoryBlock> {
        if ibs.is_empty() {
            return Vec::new();
        }

        let mut ibs_to_merge = Vec::new();
        let mut pending = self.ibs_to_flush.lock();
        if pending.is_empty() {
            self.update_flush_deadline();
        }
        pending.extend(ibs);
        if pending.len() >= max_blocks_per_shard() * available_cpus() {
            ibs_to_merge = std::mem::take(&mut *pending);
        }
        drop(pending);

        ibs_to_merge
    }

    /// Collects all the blocks that are due for flushing.
    pub fn take_blocks_to_flush(&self, is_final: bool) -> Vec<InmemoryBlock> {
        let mut dst: Vec<InmemoryBlock> = Vec::new();

        let current_time_ms = now_unix_ms();
        let flush_deadline_ms = self.flush_deadline_ms.load(Ordering::Relaxed);
        if is_final || current_time_ms >= flush_deadline_ms {
            let mut pending = self.ibs_to_flush.lock();
            dst.append(&mut pending);
        }

        for shard in &self.shards {
            shard.append_blocks_to_flush(&mut dst, current_time_ms, is_final);
        }

        dst
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    fn update_flush_deadline(&self) {
        self.flush_deadline_ms.store(
            now_unix_ms() + PENDING_ITEMS_FLUSH_INTERVAL_MS,
            Ordering::Relaxed,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_items_fills_blocks() {
        let shard = RawItemsShard::new();
        let items: Vec<&[u8]> = vec![b"foo", b"bar", b"baz"];
        let (consumed, to_flush) = shard.add_items(&items);
        assert_eq!(consumed, 3);
        assert!(to_flush.is_empty());
        assert_eq!(shard.len(), 3);
    }

    #[test]
    fn too_long_items_are_dropped() {
        let shard = RawItemsShard::new();
        let long_item = vec![0u8; MAX_INMEMORY_BLOCK_SIZE + 1];
        let before = TOO_LONG_ITEMS_TOTAL.load(Ordering::Relaxed);
        let items: Vec<&[u8]> = vec![&long_item];
        let (consumed, to_flush) = shard.add_items(&items);
        assert_eq!(consumed, 1);
        assert!(to_flush.is_empty());
        assert_eq!(shard.len(), 0);
        assert_eq!(TOO_LONG_ITEMS_TOTAL.load(Ordering::Relaxed), before + 1);
    }

    #[test]
    fn append_blocks_to_flush_honors_deadline() {
        let shard = RawItemsShard::new();
        let items: Vec<&[u8]> = vec![b"a"];
        shard.add_items(&items);

        let mut dst = Vec::new();
        // Not due yet.
        shard.append_blocks_to_flush(&mut dst, now_unix_ms(), false);
        assert!(dst.is_empty());
        // Final flush takes everything.
        shard.append_blocks_to_flush(&mut dst, now_unix_ms(), true);
        assert_eq!(dst.len(), 1);
        assert_eq!(shard.len(), 0);
    }
}
