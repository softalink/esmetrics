//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/partition_search.go: a heap
//! of per-part searches over one partition.

use std::sync::Arc;

use crate::part::{BlockRef, PartSearch};
use crate::partition::{PartWrapper, Partition};
use crate::time_range::TimeRange;
use crate::tsid::Tsid;

enum PtsError {
    Eof,
    Other(String),
}

/// A search over a single partition. The blocks are returned in
/// `(TSID, MinTimestamp)` order. Go: partitionSearch.
pub(crate) struct PartitionSearch {
    /// Parts snapshot for the given partition (keeps the parts alive).
    _pws: Vec<Arc<PartWrapper>>,

    ps_pool: Vec<PartSearch>,
    /// Heap of indices into `ps_pool` ordered by the current block header.
    heap: Vec<usize>,

    err: Option<PtsError>,
    next_block_noop: bool,
}

fn heap_less(pool: &[PartSearch], heap: &[usize], i: usize, j: usize) -> bool {
    let a = pool[heap[i]].block_ref().header();
    let b = pool[heap[j]].block_ref().header();
    a.less(b)
}

fn sift_down(pool: &[PartSearch], heap: &mut [usize], mut i: usize) {
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

fn heap_init(pool: &[PartSearch], heap: &mut [usize]) {
    let n = heap.len();
    for i in (0..n / 2).rev() {
        sift_down(pool, heap, i);
    }
}

impl PartitionSearch {
    /// Initializes the search in the given partition for the given sorted
    /// tsids and tr. Go: partitionSearch.Init.
    pub fn new(pt: &Partition, tsids: &Arc<Vec<Tsid>>, tr: TimeRange) -> PartitionSearch {
        let mut pts = PartitionSearch {
            _pws: Vec::new(),
            ps_pool: Vec::new(),
            heap: Vec::new(),
            err: None,
            next_block_noop: false,
        };

        if tsids.is_empty() || !pt.time_range().overlaps_with(tr) {
            // Fast path - zero tsids, or the partition doesn't contain rows
            // for the given time range.
            pts.err = Some(PtsError::Eof);
            return pts;
        }

        // Skip the tsids of deleted series.
        let mut filtered_tsids = Arc::clone(tsids);
        let dmis = pt.idb().get_deleted_metric_ids();
        if !dmis.is_empty() {
            filtered_tsids = Arc::new(
                tsids
                    .iter()
                    .filter(|tsid| !dmis.has(tsid.metric_id))
                    .copied()
                    .collect(),
            );
        }
        if filtered_tsids.is_empty() {
            // Fast path - zero tsids.
            pts.err = Some(PtsError::Eof);
            return pts;
        }

        pts._pws = pt.get_parts(true);

        // Initialize ps_pool and the heap.
        pts.ps_pool = pts
            ._pws
            .iter()
            .map(|pw| PartSearch::new(Arc::clone(&pw.p), Arc::clone(&filtered_tsids), tr))
            .collect();
        for i in 0..pts.ps_pool.len() {
            if !pts.ps_pool[i].next_block() {
                if let Some(err) = pts.ps_pool[i].error() {
                    // Return only the first error, since it has no sense in
                    // returning all errors.
                    pts.err = Some(PtsError::Other(format!(
                        "cannot initialize partition search: {err}"
                    )));
                    return pts;
                }
                continue;
            }
            pts.heap.push(i);
        }
        if pts.heap.is_empty() {
            pts.err = Some(PtsError::Eof);
            return pts;
        }
        heap_init(&pts.ps_pool, &mut pts.heap);
        pts.next_block_noop = true;
        pts
    }

    /// The reference to the block found by the last successful `next_block`.
    pub fn block_ref(&self) -> &BlockRef {
        self.ps_pool[self.heap[0]].block_ref()
    }

    /// Advances to the next block. The blocks are sorted by
    /// `(TSID, MinTimestamp)`; two subsequent blocks for the same TSID may
    /// contain overlapping time ranges. Go: partitionSearch.NextBlock.
    pub fn next_block(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        if self.next_block_noop {
            self.next_block_noop = false;
            return true;
        }

        let idx = self.heap[0];
        if self.ps_pool[idx].next_block() {
            sift_down(&self.ps_pool, &mut self.heap, 0);
            return true;
        }
        if let Some(err) = self.ps_pool[idx].error() {
            self.err = Some(PtsError::Other(format!(
                "cannot obtain the next block to search in the partition: {err}"
            )));
            return false;
        }

        // The part search is exhausted: pop it from the heap.
        let n = self.heap.len();
        self.heap.swap(0, n - 1);
        self.heap.pop();
        if self.heap.is_empty() {
            self.err = Some(PtsError::Eof);
            return false;
        }
        sift_down(&self.ps_pool, &mut self.heap, 0);
        true
    }

    /// Returns the last error, ignoring EOF. Go: partitionSearch.Error.
    pub fn error(&self) -> Option<String> {
        match &self.err {
            Some(PtsError::Other(msg)) => Some(msg.clone()),
            _ => None,
        }
    }
}
