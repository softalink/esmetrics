//! The rawRows ingestion shards of partition.go (`rawRowsShards` /
//! `rawRowsShard`): recently added rows that haven't been converted into
//! in-memory parts yet. They aren't visible for search for performance
//! reasons.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::partition::{
    max_raw_rows_per_shard, PtInner, DEFAULT_PARTS_TO_MERGE, PENDING_ROWS_FLUSH_INTERVAL,
};
use crate::raw_row::RawRow;

// --- rawRowsShards (Go: rawRowsShards / rawRowsShard) ---

pub(crate) struct RawRowsShards {
    flush_deadline_ms: AtomicU64,
    shard_idx: AtomicU32,
    /// Shards reduce lock contention when adding rows on multi-CPU systems.
    shards: Vec<RawRowsShard>,
    rowss_to_flush: Mutex<Vec<Vec<RawRow>>>,
}

struct RawRowsShard {
    flush_deadline_ms: AtomicU64,
    rows: Mutex<Vec<RawRow>>,
    len: AtomicUsize,
}

impl RawRowsShards {
    pub(crate) fn new(shards: usize) -> RawRowsShards {
        RawRowsShards {
            flush_deadline_ms: AtomicU64::new(0),
            shard_idx: AtomicU32::new(0),
            shards: (0..shards.max(1))
                .map(|_| RawRowsShard {
                    flush_deadline_ms: AtomicU64::new(0),
                    rows: Mutex::new(Vec::new()),
                    len: AtomicUsize::new(0),
                })
                .collect(),
            rowss_to_flush: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn add_rows(&self, pt: &Arc<PtInner>, mut rows: &[RawRow]) {
        let shards_len = self.shards.len() as u32;
        while !rows.is_empty() {
            let n = self
                .shard_idx
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1);
            let idx = (n % shards_len) as usize;
            let (tail, rows_to_flush) = self.shards[idx].add_rows(rows);
            self.add_rows_to_flush(pt, rows_to_flush);
            rows = tail;
        }
    }

    fn add_rows_to_flush(&self, pt: &Arc<PtInner>, rows_to_flush: Vec<RawRow>) {
        if rows_to_flush.is_empty() {
            return;
        }

        let mut rowss_to_merge = Vec::new();
        {
            let mut guard = self.rowss_to_flush.lock();
            if guard.is_empty() {
                self.update_flush_deadline();
            }
            guard.push(rows_to_flush);
            if guard.len() >= DEFAULT_PARTS_TO_MERGE {
                rowss_to_merge = std::mem::take(&mut *guard);
            }
        }

        pt.flush_rowss_to_inmemory_parts(rowss_to_merge);
    }

    pub(crate) fn len(&self) -> usize {
        let mut n: usize = self
            .shards
            .iter()
            .map(|s| s.len.load(Ordering::Acquire))
            .sum();
        n += self
            .rowss_to_flush
            .lock()
            .iter()
            .map(|rows| rows.len())
            .sum::<usize>();
        n
    }

    fn update_flush_deadline(&self) {
        let deadline = crate::sync_util::now_unix_milli() as u64
            + PENDING_ROWS_FLUSH_INTERVAL.as_millis() as u64;
        self.flush_deadline_ms.store(deadline, Ordering::Release);
    }

    pub(crate) fn flush(&self, pt: &Arc<PtInner>, is_final: bool) {
        let mut dst: Vec<Vec<RawRow>> = Vec::new();

        let current_time_ms = crate::sync_util::now_unix_milli() as u64;
        let flush_deadline_ms = self.flush_deadline_ms.load(Ordering::Acquire);
        if is_final || current_time_ms >= flush_deadline_ms {
            let mut guard = self.rowss_to_flush.lock();
            dst = std::mem::take(&mut *guard);
        }

        for shard in &self.shards {
            shard.append_raw_rows_to_flush(&mut dst, current_time_ms, is_final);
        }

        pt.flush_rowss_to_inmemory_parts(dst);
    }
}

impl RawRowsShard {
    fn add_rows<'a>(&self, rows: &'a [RawRow]) -> (&'a [RawRow], Vec<RawRow>) {
        let max_rows = max_raw_rows_per_shard();
        let mut rows_to_flush = Vec::new();

        let mut guard = self.rows.lock();
        if guard.capacity() == 0 {
            guard.reserve_exact(max_rows);
        }
        if guard.is_empty() {
            self.update_flush_deadline();
        }
        let n = rows.len().min(max_rows - guard.len());
        guard.extend_from_slice(&rows[..n]);
        let mut rows = &rows[n..];
        if !rows.is_empty() {
            rows_to_flush = std::mem::replace(&mut *guard, Vec::with_capacity(max_rows));
            self.update_flush_deadline();
            let n = rows.len().min(max_rows);
            guard.extend_from_slice(&rows[..n]);
            rows = &rows[n..];
        }
        self.len.store(guard.len(), Ordering::Release);
        drop(guard);

        (rows, rows_to_flush)
    }

    fn append_raw_rows_to_flush(
        &self,
        dst: &mut Vec<Vec<RawRow>>,
        current_time_ms: u64,
        is_final: bool,
    ) {
        let flush_deadline_ms = self.flush_deadline_ms.load(Ordering::Acquire);
        if !is_final && current_time_ms < flush_deadline_ms {
            // Fast path - nothing to flush.
            return;
        }

        // Slow path - move the shard rows to dst.
        let mut guard = self.rows.lock();
        if !guard.is_empty() {
            dst.push(std::mem::take(&mut *guard));
        }
        self.len.store(0, Ordering::Release);
    }

    fn update_flush_deadline(&self) {
        let deadline = crate::sync_util::now_unix_milli() as u64
            + PENDING_ROWS_FLUSH_INTERVAL.as_millis() as u64;
        self.flush_deadline_ms.store(deadline, Ordering::Release);
    }
}
