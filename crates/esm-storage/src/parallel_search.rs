//! Parallel per-series block unpacking, the Rust counterpart of Go
//! vmselect's `netstorage.ProcessSearchQuery` + `Results.RunParallel`
//! (app/vmselect/netstorage/netstorage.go).
//!
//! The query-side data path used to decode every block on the calling HTTP
//! thread via [`Search::next_series`]. Here it is split in two passes:
//!
//! 1. **Collect** (calling thread): walk the `(TSID, MinTimestamp)`-ordered
//!    block refs via [`Search::next_metric_block`] *without decoding data*,
//!    grouping the refs per series into [`SeriesRefs`] (a [`BlockRef`] is
//!    just an `Arc<Part>` + the 81-byte block header). Metric names are
//!    resolved here through the shared metricID→metricName cache.
//! 2. **Unpack** (shared worker pool): fan the per-series
//!    decode+merge+trim+dedup work across a process-wide pool of
//!    `available_parallelism` threads, spawned once. A single query claims
//!    at most `esm_common::query_workers::max_workers()` of them (the
//!    `-search.maxWorkersPerQuery` cap). Workers claim batches of series from
//!    an atomic cursor (work stealing like Go's `unpackWorker`) and reuse
//!    per-thread scratch buffers; the calling thread participates too, so
//!    small queries never block behind the pool and pool saturation degrades
//!    gracefully to the serial path.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, OnceLock};

use esm_common::decimal;
use parking_lot::{Condvar, Mutex};

use crate::block::Block;
use crate::block_stream::unmarshal_block_data;
use crate::index::{SearchError, TagFilters};
use crate::part::BlockRef;
use crate::search::{Search, SeriesBlock};
use crate::storage::Storage;
use crate::time_range::TimeRange;

/// The block refs of a single series (one TSID), in `(TSID, MinTimestamp)`
/// order, plus its marshaled canonical metric name. Go: packedTimeseries.
pub struct SeriesRefs {
    /// Marshaled canonical metric name (unmarshaled by the unpack workers).
    pub metric_name: Vec<u8>,
    /// The refs of all the blocks of the series, possibly overlapping.
    pub brs: Vec<BlockRef>,
}

// BlockRef (Arc<Part> + BlockHeader) crosses threads in SeriesRefs.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SeriesRefs>();
};

/// Serial fallback threshold: with this many series or fewer, the handoff
/// to the pool costs more than it saves (Go fast-paths 1 series / 1 CPU).
const MIN_PARALLEL_SERIES: usize = 3;

/// Upper bound for the number of series claimed per cursor step.
const MAX_BATCH_SIZE: usize = 8;

impl Storage {
    /// Searches the series matching `tfss` within `tr` and unpacks them in
    /// parallel on the shared worker pool. The result is equivalent to
    /// draining [`Search::next_series`], but the per-series block
    /// decode/merge/trim/dedup runs across all CPUs.
    pub fn search_series_parallel(
        &self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
        deadline: u64,
    ) -> Result<Vec<SeriesBlock>, SearchError> {
        // Debug knob: `ESM_TRACE_SEARCH=1` logs per-phase timings.
        static TRACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        let trace = *TRACE.get_or_init(|| std::env::var("ESM_TRACE_SEARCH").is_ok());
        if !trace {
            let mut search = self.search(tfss, tr, max_metrics, deadline)?;
            let series = collect_series_refs(&mut search)?;
            // The search (and its partition references) can be dropped now:
            // the BlockRefs keep the underlying immutable parts alive via
            // Arc.
            drop(search);
            return unpack_series_parallel(series, tr, deadline);
        }
        let t0 = std::time::Instant::now();
        let mut search = self.search(tfss, tr, max_metrics, deadline)?;
        let t1 = std::time::Instant::now();
        let series = collect_series_refs(&mut search)?;
        drop(search);
        let t2 = std::time::Instant::now();
        let n_series = series.len();
        let n_blocks: usize = series.iter().map(|s| s.brs.len()).sum();
        let res = unpack_series_parallel(series, tr, deadline);
        let t3 = std::time::Instant::now();
        log::warn!(
            "trace search tr=[{},{}]: tsids {:.1}ms, collect {:.1}ms, unpack {:.1}ms, \
             series {n_series}, blocks {n_blocks}",
            tr.min_timestamp,
            tr.max_timestamp,
            (t1 - t0).as_secs_f64() * 1e3,
            (t2 - t1).as_secs_f64() * 1e3,
            (t3 - t2).as_secs_f64() * 1e3,
        );
        res
    }
}

/// First pass: collects the per-series block refs without decoding any
/// block data. Go: ProcessSearchQuery's NextMetricBlock loop.
fn collect_series_refs(search: &mut Search<'_>) -> Result<Vec<SeriesRefs>, SearchError> {
    let mut series: Vec<SeriesRefs> = Vec::new();
    let mut prev_metric_id: Option<u64> = None;
    while search.next_metric_block() {
        let br = search.block_ref();
        let metric_id = br.header().tsid.metric_id;
        if prev_metric_id != Some(metric_id) {
            series.push(SeriesRefs {
                metric_name: search.metric_name().to_vec(),
                brs: Vec::new(),
            });
            prev_metric_id = Some(metric_id);
        }
        series
            .last_mut()
            .expect("BUG: series is non-empty here")
            .brs
            .push(br.clone());
    }
    match search.error() {
        Some(err) => Err(err.clone()),
        None => Ok(series),
    }
}

/// Per-thread scratch buffers reused across series (Go: tmpStorageBlockPool
/// + sortBlock pools).
#[derive(Default)]
struct Scratch {
    block: Block,
    /// Decoded per-block cursors for the overlapping-blocks merge
    /// (Go: sortBlock pool), reused across series.
    sort_blocks: Vec<SortCursor>,
    /// Compressed timestamps/values read buffers (reused across blocks;
    /// `read_block_reusing` keeps them at high-water length).
    timestamps_data: Vec<u8>,
    values_data: Vec<u8>,
}

/// One decoded block of a series awaiting the k-way merge, trimmed to
/// `[pos, end)`. Go: sortBlock (app/vmselect/netstorage).
struct SortCursor {
    db: Arc<crate::part::DecodedBlock>,
    pos: usize,
    end: usize,
}

/// Returns the fully decoded block for `br`, consulting the process-global
/// decoded-block cache first. On a miss the block is decoded via the scratch
/// buffers and offered to the cache.
fn get_decoded_block(
    br: &BlockRef,
    scratch: &mut Scratch,
) -> Result<Arc<crate::part::DecodedBlock>, SearchError> {
    // `values_block_offset` is only unique per part for blocks that actually
    // wrote values data: constant-encoded values blocks (any single-sample
    // block, constant gauges, ...) have `values_block_size == 0`, the writer
    // never advances the offset for them, and EVERY such block in a part
    // shares offset 0. Caching them would hand one series' samples to
    // another (the "task #17" mislabeling), so they bypass the cache — their
    // values decode is header-only and trivially cheap anyway.
    let cacheable = br.header().values_block_size > 0;
    let key = esm_mergeset::blockcache::Key {
        part_id: br.part().id,
        offset: br.header().values_block_offset,
    };
    let cache = crate::part::decoded_block_cache();
    if cacheable {
        if let Some(db) = cache.get_block(&key) {
            return Ok(db);
        }
    }
    read_block_reusing(br, scratch)?;
    let mut db = crate::part::DecodedBlock {
        timestamps: scratch.block.timestamps().to_vec(),
        values: Vec::with_capacity(scratch.block.values().len()),
    };
    decimal::append_decimal_to_float(
        &mut db.values,
        scratch.block.values(),
        scratch.block.header().scale,
    );
    let db = Arc::new(db);
    if cacheable {
        cache.try_put_block(&key, &db);
    }
    Ok(db)
}

/// [`BlockRef::read_block`] with reused compressed-data buffers instead of
/// two fresh allocations per block.
fn read_block_reusing(br: &BlockRef, scratch: &mut Scratch) -> Result<(), SearchError> {
    let bh = *br.header();
    let ts_size = bh.timestamps_block_size as usize;
    if scratch.timestamps_data.len() < ts_size {
        scratch.timestamps_data.resize(ts_size, 0);
    }
    let timestamps_data = &mut scratch.timestamps_data[..ts_size];
    br.part()
        .timestamps_file
        .must_read_at(timestamps_data, bh.timestamps_block_offset);

    let val_size = bh.values_block_size as usize;
    if scratch.values_data.len() < val_size {
        scratch.values_data.resize(val_size, 0);
    }
    let values_data = &mut scratch.values_data[..val_size];
    br.part()
        .values_file
        .must_read_at(values_data, bh.values_block_offset);

    unmarshal_block_data(&mut scratch.block, &bh, timestamps_data, values_data)
        .map_err(SearchError::Other)
}

/// Returns the `[lo, hi)` index range of the block samples inside `tr`.
/// Block timestamps are guaranteed non-decreasing after decode.
fn trim_range(timestamps: &[i64], tr: TimeRange) -> (usize, usize) {
    let lo = timestamps.partition_point(|&ts| ts < tr.min_timestamp);
    let hi = timestamps.partition_point(|&ts| ts <= tr.max_timestamp);
    (lo, hi)
}

/// Decodes and merges all the blocks of one series: trim to `tr`, sort,
/// collapse duplicate timestamps (first in block order wins) and apply the
/// global dedup interval — exactly mirroring [`Search::next_series`].
fn unpack_series(
    sr: &SeriesRefs,
    tr: TimeRange,
    dedup_interval: i64,
    scratch: &mut Scratch,
) -> Result<SeriesBlock, SearchError> {
    let mut dst = SeriesBlock::default();
    dst.metric_name
        .unmarshal(&sr.metric_name)
        .map_err(|err| SearchError::Other(format!("cannot unmarshal metricName: {err}")))?;

    // The blocks of a series arrive in MinTimestamp order; when they don't
    // overlap (the common case — decided from the headers alone), the
    // trimmed samples can be appended block by block without the
    // sort-and-collapse pass below.
    let non_overlapping = sr
        .brs
        .windows(2)
        .all(|w| w[0].header().max_timestamp < w[1].header().min_timestamp);
    let rows_total: usize = sr.brs.iter().map(BlockRef::rows_count).sum();

    if non_overlapping {
        dst.timestamps.reserve(rows_total);
        dst.values.reserve(rows_total);
        for br in &sr.brs {
            let db = get_decoded_block(br, scratch)?;
            let (lo, hi) = trim_range(&db.timestamps, tr);
            if lo >= hi {
                continue;
            }
            append_run(&mut dst, &db.timestamps[lo..hi], &db.values[lo..hi]);
        }
    } else {
        // Overlapping blocks: k-way-merge the (already sorted) per-block
        // runs into dst, collapsing duplicate timestamps so the first sample
        // in block order wins — equivalent to concatenating the runs in
        // block order, stable-sorting by timestamp and collapsing, but
        // without sorting. Go: mergeSortBlocks.
        scratch.sort_blocks.clear();
        for br in &sr.brs {
            let db = get_decoded_block(br, scratch)?;
            let (lo, hi) = trim_range(&db.timestamps, tr);
            if lo >= hi {
                continue;
            }
            scratch.sort_blocks.push(SortCursor {
                db,
                pos: lo,
                end: hi,
            });
        }
        let samples_total: usize = scratch.sort_blocks.iter().map(|c| c.end - c.pos).sum();
        dst.timestamps.reserve_exact(samples_total);
        dst.values.reserve_exact(samples_total);
        merge_sort_blocks(&mut dst, &mut scratch.sort_blocks);
        // Drop the Arc references so evicted cache blocks can be freed.
        scratch.sort_blocks.clear();
    }

    if dedup_interval > 0 {
        crate::dedup::deduplicate_samples(&mut dst.timestamps, &mut dst.values, dedup_interval);
    }
    Ok(dst)
}

/// K-way-merges the sorted per-block sample runs into `dst`, collapsing
/// duplicate timestamps so the first sample in block order wins. Equivalent
/// to concatenating the runs in block order, stable-sorting by timestamp and
/// collapsing duplicates. Go: mergeSortBlocks.
fn merge_sort_blocks(dst: &mut SeriesBlock, sbs: &mut [SortCursor]) {
    loop {
        // Find the block with the smallest current timestamp (the first in
        // block order on ties) and the smallest current timestamp among the
        // remaining blocks.
        let mut min_i = usize::MAX;
        let mut min_ts = i64::MAX;
        let mut bound = i64::MAX;
        for (i, sb) in sbs.iter().enumerate() {
            if sb.pos >= sb.end {
                continue;
            }
            let ts = sb.db.timestamps[sb.pos];
            if min_i == usize::MAX || ts < min_ts {
                bound = min_ts;
                min_ts = ts;
                min_i = i;
            } else if ts < bound {
                bound = ts;
            }
        }
        if min_i == usize::MAX {
            return;
        }
        let sb = &mut sbs[min_i];
        let start = sb.pos;
        // All the samples with ts < bound precede every remaining sample of
        // the other blocks.
        let end = if bound == i64::MAX {
            sb.end
        } else {
            start + sb.db.timestamps[start..sb.end].partition_point(|&t| t < bound)
        };
        if end == start {
            // The head ties with another block: emit a single sample; the
            // other blocks' equal timestamps collapse via the last-timestamp
            // check below.
            if dst.timestamps.last() != Some(&min_ts) {
                dst.timestamps.push(min_ts);
                dst.values.push(sb.db.values[start]);
            }
            sb.pos = start + 1;
            continue;
        }
        append_run(
            dst,
            &sb.db.timestamps[start..end],
            &sb.db.values[start..end],
        );
        sb.pos = end;
    }
}

/// Appends one sorted (non-decreasing) run to `dst`, collapsing duplicate
/// timestamps (the first occurrence wins).
fn append_run(dst: &mut SeriesBlock, timestamps: &[i64], values: &[f64]) {
    // "Strictly sorted" means no intra-run duplicates.
    if timestamps.is_sorted_by(|a, b| a < b)
        && dst.timestamps.last().is_none_or(|&l| l < timestamps[0])
    {
        dst.timestamps.extend_from_slice(timestamps);
        dst.values.extend_from_slice(values);
        return;
    }
    for (&t, &v) in timestamps.iter().zip(values) {
        if dst.timestamps.last() == Some(&t) {
            continue;
        }
        dst.timestamps.push(t);
        dst.values.push(v);
    }
}

/// One parallel unpack request: the shared work descriptor claimed batch by
/// batch (via `cursor`) by the pool workers and the calling thread.
struct UnpackJob {
    series: Vec<SeriesRefs>,
    cursor: AtomicUsize,
    batch: usize,
    tr: TimeRange,
    dedup_interval: i64,
    /// Deadline in unix seconds ([`crate::index::NO_DEADLINE`] for none).
    deadline: u64,
    /// Set on the first error so the remaining series are skipped cheaply.
    must_stop: AtomicBool,
    /// Worker results: one message per worker-claimed series index. The
    /// calling thread stores its own results directly.
    tx: mpsc::Sender<(usize, Result<SeriesBlock, SearchError>)>,
}

fn deadline_exceeded(deadline: u64) -> bool {
    esm_common::fasttime::unix_timestamp() > deadline
}

/// The shared process-wide unpack worker pool: a queue of job handles plus
/// `available_parallelism` daemon threads, spawned on first use.
struct UnpackPool {
    queue: Mutex<VecDeque<Arc<UnpackJob>>>,
    cond: Condvar,
    workers: usize,
}

impl UnpackPool {
    /// Enqueues `copies` handles of `job`, so up to `copies` idle workers
    /// join in draining its cursor. A worker popping a handle of an
    /// already-finished job returns immediately, so over-submitting and
    /// mixing handles of concurrent queries is harmless.
    fn submit(&self, job: &Arc<UnpackJob>, copies: usize) {
        let mut queue = self.queue.lock();
        for _ in 0..copies {
            queue.push_back(Arc::clone(job));
        }
        drop(queue);
        self.cond.notify_all();
    }

    fn pop(&self) -> Arc<UnpackJob> {
        let mut queue = self.queue.lock();
        loop {
            if let Some(job) = queue.pop_front() {
                return job;
            }
            self.cond.wait(&mut queue);
        }
    }
}

fn unpack_pool() -> &'static UnpackPool {
    static POOL: OnceLock<UnpackPool> = OnceLock::new();
    POOL.get_or_init(|| {
        // `ESM_UNPACK_WORKERS` overrides the default (debug/benchmarking
        // knob); 1 disables parallel unpacking.
        let workers = std::env::var("ESM_UNPACK_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(std::num::NonZeroUsize::get)
                    .unwrap_or(1)
            });
        for worker_id in 0..workers {
            std::thread::Builder::new()
                .name(format!("esm-unpack-{worker_id}"))
                .spawn(|| {
                    // Blocks until POOL is initialized, then serves forever.
                    let pool = unpack_pool();
                    let mut scratch = Scratch::default();
                    loop {
                        let job = pool.pop();
                        run_job(&job, &mut scratch);
                    }
                })
                .expect("cannot spawn the unpack worker thread");
        }
        UnpackPool {
            queue: Mutex::new(VecDeque::new()),
            cond: Condvar::new(),
            workers,
        }
    })
}

/// Worker side: claim series batches off the job cursor until exhausted.
/// Exactly one message is sent per claimed index, so the caller's
/// outstanding-results accounting always terminates.
fn run_job(job: &UnpackJob, scratch: &mut Scratch) {
    let total = job.series.len();
    loop {
        let start = job.cursor.fetch_add(job.batch, Ordering::Relaxed);
        if start >= total {
            return;
        }
        let end = (start + job.batch).min(total);
        for i in start..end {
            let res = if job.must_stop.load(Ordering::Relaxed) {
                // A failure message for the triggering index is already in
                // flight; this placeholder only keeps the accounting exact.
                Ok(SeriesBlock::default())
            } else if deadline_exceeded(job.deadline) {
                Err(SearchError::DeadlineExceeded)
            } else {
                unpack_series(&job.series[i], job.tr, job.dedup_interval, scratch)
            };
            if res.is_err() {
                job.must_stop.store(true, Ordering::Relaxed);
            }
            if job.tx.send((i, res)).is_err() {
                // The caller has already returned (error path); the
                // remaining series are skipped via must_stop.
                job.must_stop.store(true, Ordering::Relaxed);
            }
        }
    }
}

/// Batch size and helper-worker count for a job of `total` series when a
/// single query may use at most `max_workers` threads (calling thread
/// included) of a `pool_workers`-thread pool. Go: `RunParallel(workers)`
/// with `workers = -search.maxWorkersPerQuery`.
fn plan_unpack(total: usize, pool_workers: usize, max_workers: usize) -> (usize, usize) {
    let workers = pool_workers.min(max_workers).max(1);
    let batch = (total / (workers * 4)).clamp(1, MAX_BATCH_SIZE);
    // The calling thread claims batches too, so the job needs at most
    // `workers - 1` helpers, and never more than the remaining batches.
    let helpers = (workers - 1).min(total.div_ceil(batch).saturating_sub(1));
    (batch, helpers)
}

/// Second pass: unpacks `series` into [`SeriesBlock`]s across the shared
/// pool, preserving order. Go: Results.RunParallel + unpackWorker.
fn unpack_series_parallel(
    series: Vec<SeriesRefs>,
    tr: TimeRange,
    deadline: u64,
) -> Result<Vec<SeriesBlock>, SearchError> {
    let total = series.len();
    if total == 0 {
        return Ok(Vec::new());
    }
    let dedup_interval = crate::dedup::get_dedup_interval();
    let pool = unpack_pool();
    let max_workers = esm_common::query_workers::max_workers();
    if pool.workers <= 1 || max_workers <= 1 || total <= MIN_PARALLEL_SERIES {
        // Fast path: unpack on the calling thread (Go: gomaxprocs == 1 or
        // a single series).
        let mut scratch = Scratch::default();
        let mut out = Vec::with_capacity(total);
        for sr in &series {
            if deadline_exceeded(deadline) {
                return Err(SearchError::DeadlineExceeded);
            }
            out.push(unpack_series(sr, tr, dedup_interval, &mut scratch)?);
        }
        return Ok(out);
    }

    let (batch, helpers) = plan_unpack(total, pool.workers, max_workers);
    let (tx, rx) = mpsc::channel();
    let job = Arc::new(UnpackJob {
        series,
        cursor: AtomicUsize::new(0),
        batch,
        tr,
        dedup_interval,
        deadline,
        must_stop: AtomicBool::new(false),
        tx,
    });
    if helpers > 0 {
        pool.submit(&job, helpers);
    }

    let mut out: Vec<Option<SeriesBlock>> = Vec::with_capacity(total);
    out.resize_with(total, || None);
    let mut outstanding = total;

    // Claim and unpack batches on the calling thread alongside the workers.
    let mut scratch = Scratch::default();
    loop {
        let start = job.cursor.fetch_add(batch, Ordering::Relaxed);
        if start >= total {
            break;
        }
        if deadline_exceeded(deadline) {
            job.must_stop.store(true, Ordering::Relaxed);
            return Err(SearchError::DeadlineExceeded);
        }
        for (i, sr) in job.series[start..(start + batch).min(total)]
            .iter()
            .enumerate()
        {
            match unpack_series(sr, tr, dedup_interval, &mut scratch) {
                Ok(sb) => {
                    out[start + i] = Some(sb);
                    outstanding -= 1;
                }
                Err(err) => {
                    job.must_stop.store(true, Ordering::Relaxed);
                    return Err(err);
                }
            }
        }
    }

    // Collect the worker-produced series. Every worker-claimed index sends
    // exactly one message, and an error (if any) is among them.
    while outstanding > 0 {
        let (i, res) = rx
            .recv()
            .map_err(|_| SearchError::Other("BUG: unpack workers disconnected".to_string()))?;
        match res {
            Ok(sb) => {
                out[i] = Some(sb);
                outstanding -= 1;
            }
            Err(err) => {
                job.must_stop.store(true, Ordering::Relaxed);
                return Err(err);
            }
        }
    }
    Ok(out
        .into_iter()
        .map(|sb| sb.expect("BUG: a series was left unpacked"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::plan_unpack;

    #[test]
    fn plan_unpack_respects_per_query_worker_cap() {
        // Cap 3 on an 8-worker pool: the calling thread plus at most
        // 2 helpers.
        let (_, helpers) = plan_unpack(1000, 8, 3);
        assert_eq!(helpers, 2);
        // Cap 1: no helpers — the query runs on the calling thread only.
        let (_, helpers) = plan_unpack(1000, 8, 1);
        assert_eq!(helpers, 0);
        // Uncapped (cap >= pool): full fan-out, caller + pool workers.
        let (batch, helpers) = plan_unpack(1000, 8, 32);
        assert_eq!(batch, 8);
        assert_eq!(helpers, 7);
    }

    #[test]
    fn plan_unpack_never_requests_more_helpers_than_batches() {
        // 4 series at batch 1: the caller plus at most 3 helpers.
        let (batch, helpers) = plan_unpack(4, 8, 8);
        assert_eq!(batch, 1);
        assert_eq!(helpers, 3);
        // Degenerate empty job (guarded by the caller) must not underflow.
        let (_, helpers) = plan_unpack(0, 8, 8);
        assert_eq!(helpers, 0);
    }
}
