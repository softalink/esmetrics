//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/block_stream_merger.go and
//! merge.go: k-way merging of block streams ordered by
//! `(TSID, MinTimestamp)`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use esm_common::{decimal, fasttime, uint64set};

use crate::block::{Block, MAX_ROWS_PER_BLOCK};
use crate::block_stream::reader::BlockStreamReader;
use crate::block_stream::writer::BlockStreamWriter;
use crate::part::header::PartHeader;

/// Error returned by [`merge_block_streams`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeError {
    /// The merge was interrupted via the stop flag.
    /// Go: errForciblyStopped.
    ForciblyStopped,
    /// Any other (corruption/IO) error.
    Other(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::ForciblyStopped => write!(f, "forcibly stopped"),
            MergeError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for MergeError {}

/// Merges `bsrs` into `bsw` and updates `ph` with the merged part header.
///
/// Returns immediately with [`MergeError::ForciblyStopped`] once `stop` is
/// set. Blocks of metric IDs contained in `dmis` are dropped, as are blocks
/// and rows older than `retention_deadline`; both increment `rows_deleted`.
/// `rows_merged` is atomically updated with the number of merged rows.
///
/// `bsw` is always closed; for in-memory destinations the four stream
/// buffers (`[timestamps, values, index, metaindex]`) are returned on
/// success. Go: mergeBlockStreams.
#[allow(clippy::too_many_arguments)]
pub fn merge_block_streams(
    ph: &mut PartHeader,
    bsw: &mut BlockStreamWriter,
    bsrs: &mut [BlockStreamReader],
    stop: Option<&AtomicBool>,
    dmis: Option<&uint64set::Set>,
    retention_deadline: i64,
    rows_merged: &AtomicU64,
    rows_deleted: &AtomicU64,
) -> Result<Option<[Vec<u8>; 4]>, MergeError> {
    ph.reset();

    let mut bsm = BlockStreamMerger::new(retention_deadline);
    bsm.init(bsrs);
    let res = merge_block_streams_internal(
        ph,
        bsw,
        &mut bsm,
        bsrs,
        stop,
        dmis,
        rows_merged,
        rows_deleted,
    );
    let bufs = bsw.must_close();
    match res {
        Ok(()) => Ok(bufs),
        Err(MergeError::ForciblyStopped) => Err(MergeError::ForciblyStopped),
        Err(MergeError::Other(msg)) => Err(MergeError::Other(format!(
            "cannot merge {} streams: {msg}",
            bsrs.len()
        ))),
    }
}

enum BsmError {
    Eof,
    Other(String),
}

/// Merger of block streams. Go: blockStreamMerger.
struct BlockStreamMerger {
    /// Heap of indices into the `bsrs` slice, ordered by the current block
    /// header (TSID, then MinTimestamp). Go: bsrHeap.
    heap: Vec<usize>,

    /// Blocks with smaller timestamps are removed because of retention.
    retention_deadline: i64,

    /// Whether the next call to `next_block` must be a no-op.
    next_block_noop: bool,

    /// The last error.
    err: Option<BsmError>,
}

fn heap_less(bsrs: &[BlockStreamReader], heap: &[usize], i: usize, j: usize) -> bool {
    let a = bsrs[heap[i]].block.header();
    let b = bsrs[heap[j]].block.header();
    a.less(b)
}

fn sift_down(bsrs: &[BlockStreamReader], heap: &mut [usize], mut i: usize) {
    let n = heap.len();
    loop {
        let left = 2 * i + 1;
        if left >= n {
            return;
        }
        let mut smallest = left;
        let right = left + 1;
        if right < n && heap_less(bsrs, heap, right, left) {
            smallest = right;
        }
        if !heap_less(bsrs, heap, smallest, i) {
            return;
        }
        heap.swap(i, smallest);
        i = smallest;
    }
}

fn heap_init(bsrs: &[BlockStreamReader], heap: &mut [usize]) {
    let n = heap.len();
    for i in (0..n / 2).rev() {
        sift_down(bsrs, heap, i);
    }
}

impl BlockStreamMerger {
    fn new(retention_deadline: i64) -> BlockStreamMerger {
        BlockStreamMerger {
            heap: Vec::new(),
            retention_deadline,
            next_block_noop: false,
            err: None,
        }
    }

    /// Go: blockStreamMerger.Init.
    fn init(&mut self, bsrs: &mut [BlockStreamReader]) {
        for (i, bsr) in bsrs.iter_mut().enumerate() {
            if bsr.next_block() {
                self.heap.push(i);
                continue;
            }
            if let Some(err) = bsr.error() {
                self.err = Some(BsmError::Other(format!(
                    "cannot obtain the next block to merge: {err}"
                )));
                return;
            }
        }

        if self.heap.is_empty() {
            self.err = Some(BsmError::Eof);
            return;
        }

        heap_init(bsrs, &mut self.heap);
        self.next_block_noop = true;
    }

    /// The index (into bsrs) of the reader holding the current block.
    fn top(&self) -> usize {
        self.heap[0]
    }

    /// Advances to the next block, ordered by (TSID, MinTimestamp). Two
    /// subsequent blocks for the same TSID may contain overlapping time
    /// ranges. Go: blockStreamMerger.NextBlock.
    fn next_block(&mut self, bsrs: &mut [BlockStreamReader]) -> bool {
        if self.err.is_some() {
            return false;
        }
        if self.next_block_noop {
            self.next_block_noop = false;
            return true;
        }

        match self.next_block_internal(bsrs) {
            Ok(()) => true,
            Err(BsmError::Eof) => {
                self.err = Some(BsmError::Eof);
                false
            }
            Err(BsmError::Other(msg)) => {
                self.err = Some(BsmError::Other(format!(
                    "cannot obtain the next block to merge: {msg}"
                )));
                false
            }
        }
    }

    /// Go: blockStreamMerger.nextBlock.
    fn next_block_internal(&mut self, bsrs: &mut [BlockStreamReader]) -> Result<(), BsmError> {
        let bsr_min = self.heap[0];
        if bsrs[bsr_min].next_block() {
            sift_down(bsrs, &mut self.heap, 0);
            return Ok(());
        }

        if let Some(err) = bsrs[bsr_min].error() {
            return Err(BsmError::Other(err));
        }

        // Pop the exhausted reader off the heap.
        let n = self.heap.len();
        self.heap.swap(0, n - 1);
        self.heap.pop();
        sift_down(bsrs, &mut self.heap, 0);

        if self.heap.is_empty() {
            return Err(BsmError::Eof);
        }
        Ok(())
    }

    /// Go: blockStreamMerger.Error.
    fn error(&self) -> Option<String> {
        match &self.err {
            Some(BsmError::Other(msg)) => Some(msg.clone()),
            _ => None,
        }
    }
}

/// Go: mergeBlockStreamsInternal.
#[allow(clippy::too_many_arguments)]
fn merge_block_streams_internal(
    ph: &mut PartHeader,
    bsw: &mut BlockStreamWriter,
    bsm: &mut BlockStreamMerger,
    bsrs: &mut [BlockStreamReader],
    stop: Option<&AtomicBool>,
    dmis: Option<&uint64set::Set>,
    rows_merged: &AtomicU64,
    rows_deleted: &AtomicU64,
) -> Result<(), MergeError> {
    let mut pending_block_is_empty = true;
    let mut pending_block = Block::default();
    let mut tmp_block = Block::default();

    // Use local variables for tracking the number of merged and deleted rows
    // and periodically propagate the collected stats to the caller, so it can
    // be reflected in the exposed metrics. This minimizes expensive updates
    // of rows_merged and rows_deleted from concurrently running merges.
    let mut update_stats_deadline = 0u64;
    let mut local_rows_merged = 0u64;
    let mut local_rows_deleted = 0u64;

    fn flush_stats(
        rows_merged: &AtomicU64,
        rows_deleted: &AtomicU64,
        local_rows_merged: &mut u64,
        local_rows_deleted: &mut u64,
    ) {
        rows_deleted.fetch_add(*local_rows_deleted, Ordering::Relaxed);
        *local_rows_deleted = 0;
        rows_merged.fetch_add(*local_rows_merged, Ordering::Relaxed);
        *local_rows_merged = 0;
    }

    let res = loop {
        if !bsm.next_block(bsrs) {
            break Ok(());
        }

        let ct = fasttime::unix_timestamp();
        if ct > update_stats_deadline {
            flush_stats(
                rows_merged,
                rows_deleted,
                &mut local_rows_merged,
                &mut local_rows_deleted,
            );
            // Update the external stats once per second.
            update_stats_deadline = ct + 1;
        }

        if stop.is_some_and(|s| s.load(Ordering::Relaxed)) {
            break Err(MergeError::ForciblyStopped);
        }

        let b_idx = bsm.top();
        let bh = *bsrs[b_idx].block.header();
        if dmis.is_some_and(|s| s.has(bh.tsid.metric_id)) {
            // Skip blocks for deleted metrics.
            local_rows_deleted += bh.rows_count as u64;
            continue;
        }
        let retention_deadline = bsm.retention_deadline;
        if bh.max_timestamp < retention_deadline {
            // Skip blocks out of the given retention.
            local_rows_deleted += bh.rows_count as u64;
            continue;
        }
        if pending_block_is_empty {
            // Load the next block if pending_block is empty.
            pending_block.copy_from(&bsrs[b_idx].block);
            pending_block_is_empty = false;
            continue;
        }

        // Verify whether pending_block may be merged with b (the current
        // block).
        if pending_block.header().tsid.metric_id != bh.tsid.metric_id {
            // Fast path - blocks belong to distinct time series.
            // Write the pending_block and then deal with b.
            assert!(
                bh.tsid >= pending_block.header().tsid,
                "BUG: the next TSID={:?} is smaller than the current TSID={:?}",
                bh.tsid,
                pending_block.header().tsid
            );
            bsw.write_external_block(&mut pending_block, ph, &mut local_rows_merged);
            pending_block.copy_from(&bsrs[b_idx].block);
            continue;
        }
        if pending_block.too_big() && pending_block.header().max_timestamp <= bh.min_timestamp {
            // Fast path - pending_block is too big and it doesn't overlap
            // with b. Write the pending_block and then deal with b.
            bsw.write_external_block(&mut pending_block, ph, &mut local_rows_merged);
            pending_block.copy_from(&bsrs[b_idx].block);
            continue;
        }

        // Slow path - pending_block and b belong to the same time series,
        // so they must be merged.
        let b = &mut bsrs[b_idx].block;
        if let Err(err) = unmarshal_and_calibrate_scale(&mut pending_block, b) {
            break Err(MergeError::Other(format!(
                "cannot unmarshal and calibrate scale for blocks to be merged: {err}"
            )));
        }
        tmp_block.reset();
        tmp_block.bh.tsid = b.header().tsid;
        tmp_block.bh.scale = b.header().scale;
        tmp_block.bh.precision_bits = pending_block
            .header()
            .precision_bits
            .min(b.header().precision_bits);
        merge_blocks(
            &mut tmp_block,
            &mut pending_block,
            b,
            retention_deadline,
            &mut local_rows_deleted,
        );
        if tmp_block.timestamps.len() <= MAX_ROWS_PER_BLOCK {
            // More entries may be added to tmp_block. Swap it with
            // pending_block, so more entries may be added to pending_block on
            // the next iteration.
            if !tmp_block.timestamps.is_empty() {
                fixup_timestamps(&mut tmp_block);
            } else {
                pending_block_is_empty = true;
            }
            std::mem::swap(&mut pending_block, &mut tmp_block);
            continue;
        }

        // Write the first MAX_ROWS_PER_BLOCK rows of tmp_block to bsw,
        // leave the rest in pending_block.
        let (tsid, scale, precision_bits) = (
            tmp_block.header().tsid,
            tmp_block.header().scale,
            tmp_block.header().precision_bits,
        );
        let tail_timestamps = tmp_block.timestamps[MAX_ROWS_PER_BLOCK..].to_vec();
        let tail_values = tmp_block.values[MAX_ROWS_PER_BLOCK..].to_vec();
        pending_block.init(&tsid, &tail_timestamps, &tail_values, scale, precision_bits);
        tmp_block.timestamps.truncate(MAX_ROWS_PER_BLOCK);
        tmp_block.values.truncate(MAX_ROWS_PER_BLOCK);
        fixup_timestamps(&mut tmp_block);
        bsw.write_external_block(&mut tmp_block, ph, &mut local_rows_merged);
    };

    if res.is_ok() {
        if let Some(err) = bsm.error() {
            flush_stats(
                rows_merged,
                rows_deleted,
                &mut local_rows_merged,
                &mut local_rows_deleted,
            );
            return Err(MergeError::Other(format!(
                "cannot read block to be merged: {err}"
            )));
        }
        if !pending_block_is_empty {
            bsw.write_external_block(&mut pending_block, ph, &mut local_rows_merged);
        }
    }
    flush_stats(
        rows_merged,
        rows_deleted,
        &mut local_rows_merged,
        &mut local_rows_deleted,
    );
    res
}

/// Replica of the private `Block.fixupTimestamps`: refreshes the header's
/// min/max timestamps from the not-yet-consumed samples.
fn fixup_timestamps(b: &mut Block) {
    b.bh.min_timestamp = b.timestamps()[0];
    b.bh.max_timestamp = *b.timestamps().last().unwrap();
}

/// Merges `ib1` and `ib2` into `ob`. Go: mergeBlocks.
fn merge_blocks(
    ob: &mut Block,
    ib1: &mut Block,
    ib2: &mut Block,
    retention_deadline: i64,
    rows_deleted: &mut u64,
) {
    ib1.assert_mergeable(ib2);
    ib1.assert_unmarshaled();
    ib2.assert_unmarshaled();

    skip_samples_outside_retention(ib1, retention_deadline, rows_deleted);
    skip_samples_outside_retention(ib2, retention_deadline, rows_deleted);

    if ib1.header().max_timestamp < ib2.header().min_timestamp {
        // Fast path - ib1 values have smaller timestamps than ib2 values.
        append_rows(ob, ib1);
        append_rows(ob, ib2);
        return;
    }
    if ib2.header().max_timestamp < ib1.header().min_timestamp {
        // Fast path - ib2 values have smaller timestamps than ib1 values.
        append_rows(ob, ib2);
        append_rows(ob, ib1);
        return;
    }
    if ib1.timestamps().is_empty() {
        append_rows(ob, ib2);
        return;
    }
    if ib2.timestamps().is_empty() {
        append_rows(ob, ib1);
        return;
    }
    let mut ib1 = ib1;
    let mut ib2 = ib2;
    loop {
        let ts2 = ib2.timestamps()[0];
        let n = ib1.timestamps().partition_point(|&ts| ts <= ts2);
        ob.timestamps.extend_from_slice(&ib1.timestamps()[..n]);
        ob.values.extend_from_slice(&ib1.values()[..n]);
        for _ in 0..n {
            ib1.next_row();
        }
        if ib1.timestamps().is_empty() {
            append_rows(ob, ib2);
            return;
        }
        std::mem::swap(&mut ib1, &mut ib2);
    }
}

/// Go: skipSamplesOutsideRetention.
fn skip_samples_outside_retention(b: &mut Block, retention_deadline: i64, rows_deleted: &mut u64) {
    if b.header().min_timestamp >= retention_deadline {
        // Fast path - the block contains only samples with timestamps
        // bigger than retention_deadline.
        return;
    }
    let mut n = 0u64;
    while b
        .timestamps()
        .first()
        .is_some_and(|&ts| ts < retention_deadline)
    {
        b.next_row();
        n += 1;
    }
    *rows_deleted += n;
}

/// Go: appendRows.
fn append_rows(ob: &mut Block, ib: &mut Block) {
    ob.timestamps.extend_from_slice(ib.timestamps());
    ob.values.extend_from_slice(ib.values());
    // Consume all remaining rows of ib.
    while ib.next_row() {}
}

/// Go: unmarshalAndCalibrateScale.
fn unmarshal_and_calibrate_scale(b1: &mut Block, b2: &mut Block) -> Result<(), String> {
    b1.unmarshal_data()?;
    b2.unmarshal_data()?;

    // PORT-NOTE: Go calibrates values[nextIdx:]; in this port both blocks
    // always have nextIdx == 0 here (pending blocks come from copy_from/init
    // and reader blocks are freshly unpacked).
    debug_assert_eq!(b1.timestamps().len(), b1.timestamps.len());
    debug_assert_eq!(b2.timestamps().len(), b2.timestamps.len());
    let (scale1, scale2) = (b1.header().scale, b2.header().scale);
    let scale = decimal::calibrate_scale(&mut b1.values, scale1, &mut b2.values, scale2);
    b1.bh.scale = scale;
    b2.bh.scale = scale;
    Ok(())
}
