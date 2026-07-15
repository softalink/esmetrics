//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/table_search.go: a k-way
//! merge heap over per-partition searches.

use std::sync::Arc;

use crate::part::BlockRef;
use crate::partition::search::PartitionSearch;
use crate::table::{PartitionWrapper, Table};
use crate::time_range::TimeRange;
use crate::tsid::Tsid;

enum TsError {
    Eof,
    Other(String),
}

/// A search over all the partitions of a table. The blocks are returned in
/// `(TSID, MinTimestamp)` order; two subsequent blocks for the same TSID may
/// contain overlapping time ranges. Go: tableSearch.
pub(crate) struct TableSearch {
    /// Partitions snapshot (keeps the partitions alive during the search).
    _ptws: Vec<Arc<PartitionWrapper>>,

    pts_pool: Vec<PartitionSearch>,
    /// Heap of indices into `pts_pool` ordered by the current block header.
    heap: Vec<usize>,

    err: Option<TsError>,
    next_block_noop: bool,
}

fn heap_less(pool: &[PartitionSearch], heap: &[usize], i: usize, j: usize) -> bool {
    let a = pool[heap[i]].block_ref().header();
    let b = pool[heap[j]].block_ref().header();
    a.less(b)
}

fn sift_down(pool: &[PartitionSearch], heap: &mut [usize], mut i: usize) {
    let n = heap.len();
    loop {
        let left = 2 * i + 1;
        if left >= n {
            return;
        }
        let mut smallest = left;
        let right = left + 1;
        if right < n && heap_less(pool, heap, right, left) {
            smallest = right;
        }
        if !heap_less(pool, heap, smallest, i) {
            return;
        }
        heap.swap(i, smallest);
        i = smallest;
    }
}

impl TableSearch {
    /// Initializes the search over `tb` for the given sorted `tsids` within
    /// `tr`. `tr.min_timestamp` is clamped to the retention.
    /// Go: tableSearch.Init.
    pub fn new(tb: &Table, tsids: Vec<Tsid>, mut tr: TimeRange) -> TableSearch {
        // Adjust tr.min_timestamp, so the search doesn't return data older
        // than the configured retention.
        let now = crate::sync_util::now_unix_milli();
        let min_timestamp = now - tb.inner_env_retention_msecs();
        if tr.min_timestamp < min_timestamp {
            tr.min_timestamp = min_timestamp;
        }

        let mut ts = TableSearch {
            _ptws: Vec::new(),
            pts_pool: Vec::new(),
            heap: Vec::new(),
            err: None,
            next_block_noop: false,
        };

        if tsids.is_empty() {
            // Fast path - zero tsids.
            ts.err = Some(TsError::Eof);
            return ts;
        }
        let tsids = Arc::new(tsids);

        ts._ptws = tb.get_all_partitions();

        // Initialize pts_pool and the heap.
        ts.pts_pool = ts
            ._ptws
            .iter()
            .map(|ptw| PartitionSearch::new(ptw.pt(), &tsids, tr))
            .collect();
        for i in 0..ts.pts_pool.len() {
            if !ts.pts_pool[i].next_block() {
                if let Some(err) = ts.pts_pool[i].error() {
                    // Return only the first error, since it has no sense in
                    // returning all errors.
                    ts.err = Some(TsError::Other(format!(
                        "cannot initialize table search: {err}"
                    )));
                    return ts;
                }
                continue;
            }
            ts.heap.push(i);
        }
        if ts.heap.is_empty() {
            ts.err = Some(TsError::Eof);
            return ts;
        }
        let n = ts.heap.len();
        for i in (0..n / 2).rev() {
            sift_down(&ts.pts_pool, &mut ts.heap, i);
        }
        ts.next_block_noop = true;
        ts
    }

    /// The reference to the block found by the last successful `next_block`.
    pub fn block_ref(&self) -> &BlockRef {
        self.pts_pool[self.heap[0]].block_ref()
    }

    /// Advances to the next block. Go: tableSearch.NextBlock.
    pub fn next_block(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        if self.next_block_noop {
            self.next_block_noop = false;
            return true;
        }

        let idx = self.heap[0];
        if self.pts_pool[idx].next_block() {
            sift_down(&self.pts_pool, &mut self.heap, 0);
            return true;
        }
        if let Some(err) = self.pts_pool[idx].error() {
            self.err = Some(TsError::Other(format!(
                "cannot obtain the next block to search in the table: {err}"
            )));
            return false;
        }

        // The partition search is exhausted: pop it from the heap.
        let n = self.heap.len();
        self.heap.swap(0, n - 1);
        self.heap.pop();
        if self.heap.is_empty() {
            self.err = Some(TsError::Eof);
            return false;
        }
        sift_down(&self.pts_pool, &mut self.heap, 0);
        true
    }

    /// Returns the last error, ignoring EOF. Go: tableSearch.Error.
    pub fn error(&self) -> Option<String> {
        match &self.err {
            Some(TsError::Other(msg)) => Some(msg.clone()),
            _ => None,
        }
    }
}

impl Table {
    fn inner_env_retention_msecs(&self) -> i64 {
        self.inner.env.retention_msecs
    }
}
