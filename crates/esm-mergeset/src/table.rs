//! Port of `table.go`: the `Table` itself, background flushers and the
//! write path. The merge machinery lives in `table_merge.rs`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::block_stream_reader::BlockStreamReader;
use crate::block_stream_writer::BlockStreamWriter;
use crate::inmemory_block::InmemoryBlock;
use crate::inmemory_part::InmemoryPart;
use crate::merge::PrepareBlockCallback;
use crate::part_wrapper::{
    get_flush_to_disk_deadline, get_parts_for_optimal_merge, PartWrapper, DEFAULT_PARTS_TO_MERGE,
};
use crate::raw_items::{RawItemsShards, TOO_LONG_ITEMS_TOTAL};
use crate::table_merge::{get_max_inmemory_part_size, PartType};
use crate::table_parts::{must_open_parts, must_write_part_names};
use crate::util::{available_cpus, Sema, Shutdown, WaitCounter};

/// The maximum number of inmemory parts in the table.
///
/// This limit allows reducing CPU usage under high ingestion rate.
/// If this number is reached, then the data ingestion is paused until
/// background mergers reduce the number of parts below this number.
pub(crate) const MAX_INMEMORY_PARTS: usize = 30;

/// The interval for flushing buffered data to parts, so it becomes visible
/// to search.
pub(crate) const PENDING_ITEMS_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// The default interval for calling flush_callback when there is pending
/// data to flush.
///
/// It is set relatively high in order to improve the effectiveness of caches
/// reset by flush_callback.
const DEFAULT_FLUSH_CALLBACK_INTERVAL: Duration = Duration::from_secs(10);

/// Callback invoked every time a new batch of data becomes visible to search.
pub type FlushCallback = Arc<dyn Fn() + Send + Sync>;

pub(crate) fn inmemory_parts_concurrency() -> &'static Sema {
    // The concurrency for processing in-memory parts must equal the number of
    // CPU cores, since these operations are CPU-bound.
    static SEMA: OnceLock<Sema> = OnceLock::new();
    SEMA.get_or_init(|| Sema::new(available_cpus()))
}

pub(crate) fn file_parts_concurrency_cap() -> usize {
    // Allow at least 4 concurrent workers for file parts on systems with less
    // than 4 CPU cores in order to be able to make small file merges when big
    // file merges are in progress.
    available_cpus().max(4)
}

pub(crate) fn file_parts_concurrency() -> &'static Sema {
    static SEMA: OnceLock<Sema> = OnceLock::new();
    SEMA.get_or_init(|| Sema::new(file_parts_concurrency_cap()))
}

pub(crate) struct TableState {
    /// Inmemory parts, which are visible for search.
    pub inmemory_parts: Vec<Arc<PartWrapper>>,
    /// File-backed parts, which are visible for search.
    pub file_parts: Vec<Arc<PartWrapper>>,
    /// Set when the table is being closed. Guards against spawning workers
    /// after shutdown (Go's "wg.Add only under partsLock" discipline).
    pub stopped: bool,
}

pub(crate) struct TableInner {
    pub active_inmemory_merges: AtomicI64,
    pub active_file_merges: AtomicI64,

    pub inmemory_merges_count: AtomicU64,
    pub file_merges_count: AtomicU64,

    pub inmemory_items_merged: AtomicU64,
    pub file_items_merged: AtomicU64,

    pub items_added: AtomicU64,
    pub items_added_size_bytes: AtomicU64,

    pub inmemory_parts_limit_reached_count: AtomicU64,

    pub merge_idx: AtomicU64,

    pub path: PathBuf,

    /// The interval for guaranteed flush of recently ingested data from
    /// memory to on-disk parts, so they survive process crash.
    pub flush_interval: Duration,

    pub flush_callback: Option<FlushCallback>,
    pub flush_callback_interval: Duration,
    pub need_flush_callback_call: AtomicBool,

    pub prepare_block: Option<PrepareBlockCallback>,
    pub is_read_only: Arc<AtomicBool>,

    /// Recently added items that haven't been converted to parts yet and
    /// aren't visible for search.
    pub raw_items: RawItemsShards,

    /// Protects the parts lists and worker spawning.
    pub parts: Mutex<TableState>,

    /// Limits the number of inmemory parts to MAX_INMEMORY_PARTS in order to
    /// prevent from data ingestion slowdown: this is the backpressure.
    pub inmemory_parts_limit: Sema,

    /// Notifies all the background workers to stop.
    pub shutdown: Shutdown,

    /// Handles of the background workers.
    pub threads: Mutex<Vec<JoinHandle<()>>>,

    /// Tracks in-flight pending-item flushes (Go `flushPendingItemsWG`).
    pub flush_pending_items_wg: WaitCounter,
}

/// A mergeset table: an LSM-tree over sorted byte-string items.
pub struct Table {
    pub(crate) inner: Arc<TableInner>,
}

impl Table {
    /// Opens a table on the given path. The table is created if it doesn't
    /// exist yet.
    ///
    /// `flush_interval` is the interval for flushing pending in-memory data
    /// to disk.
    ///
    /// The optional `flush_callback` is called every time a new data batch is
    /// flushed to the underlying storage and becomes visible to search.
    ///
    /// `flush_callback_interval` is how often `flush_callback` is invoked
    /// when there is pending data to flush. Zero selects the default (10s).
    ///
    /// The optional `prepare_block` is called during merge before flushing
    /// the prepared block to persistent storage.
    pub fn must_open(
        path: impl AsRef<Path>,
        flush_interval: Duration,
        flush_callback: Option<FlushCallback>,
        flush_callback_interval: Duration,
        prepare_block: Option<PrepareBlockCallback>,
        is_read_only: Arc<AtomicBool>,
    ) -> Table {
        let path = path.as_ref().to_path_buf();

        // There is no sense in setting flush_interval to values smaller than
        // PENDING_ITEMS_FLUSH_INTERVAL, since pending rows unconditionally
        // remain in memory for up to PENDING_ITEMS_FLUSH_INTERVAL.
        let flush_interval = flush_interval.max(PENDING_ITEMS_FLUSH_INTERVAL);
        let flush_callback_interval = if flush_callback_interval.is_zero() {
            DEFAULT_FLUSH_CALLBACK_INTERVAL
        } else {
            flush_callback_interval
        };

        // Create a directory at the path if it doesn't exist yet.
        esm_common::fs::must_mkdir_if_not_exist(&path);

        // Open table parts.
        let pws = must_open_parts(&path);

        // Sync the path and the parent dir, so the path becomes visible in
        // the parent dir.
        esm_common::fs::must_sync_path_and_parent_dir(&path);

        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let inner = Arc::new(TableInner {
            active_inmemory_merges: AtomicI64::new(0),
            active_file_merges: AtomicI64::new(0),
            inmemory_merges_count: AtomicU64::new(0),
            file_merges_count: AtomicU64::new(0),
            inmemory_items_merged: AtomicU64::new(0),
            file_items_merged: AtomicU64::new(0),
            items_added: AtomicU64::new(0),
            items_added_size_bytes: AtomicU64::new(0),
            inmemory_parts_limit_reached_count: AtomicU64::new(0),
            merge_idx: AtomicU64::new(now_nanos),
            path,
            flush_interval,
            flush_callback,
            flush_callback_interval,
            need_flush_callback_call: AtomicBool::new(false),
            prepare_block,
            is_read_only,
            raw_items: RawItemsShards::new(),
            parts: Mutex::new(TableState {
                inmemory_parts: Vec::new(),
                file_parts: pws,
                stopped: false,
            }),
            inmemory_parts_limit: Sema::new(MAX_INMEMORY_PARTS),
            shutdown: Shutdown::new(),
            threads: Mutex::new(Vec::new()),
            flush_pending_items_wg: WaitCounter::new(),
        });

        inner.start_background_workers();

        Table { inner }
    }

    /// The path to the table on the filesystem.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Adds the given items to the table.
    ///
    /// The function ignores items with length exceeding
    /// `MAX_INMEMORY_BLOCK_SIZE`. It logs the ignored items, so users could
    /// notice and fix the issue.
    pub fn add_items(&self, items: &[&[u8]]) {
        self.inner.add_items(items);
    }

    /// Makes sure all the recently added data is visible to search.
    ///
    /// This function is for debugging and testing purposes only, since it may
    /// slow down data ingestion when used frequently.
    pub fn debug_flush(&self) {
        self.inner.flush_pending_items(true);
        // Wait for background flushers to finish.
        self.inner.flush_pending_items_wg.wait();
    }

    /// Notifies the table that it may be switched from read-only mode to
    /// read-write mode.
    pub fn notify_read_write_mode(&self) {
        self.inner.start_inmemory_parts_mergers();
        self.inner.start_file_parts_mergers();
    }

    /// Updates `m` with metrics from the table.
    pub fn update_metrics(&self, m: &mut TableMetrics) {
        self.inner.update_metrics(m);
    }

    /// Creates a table snapshot in the given destination directory using hard
    /// links.
    pub fn must_create_snapshot_at(&self, dst_dir: impl AsRef<Path>) {
        self.inner.must_create_snapshot_at(dst_dir.as_ref());
    }

    /// Closes the table.
    ///
    /// This must be called only when there are no threads using the table,
    /// such as ones that ingest or retrieve data.
    pub fn must_close(self) {
        let inner = &self.inner;

        // Notify the background workers to stop. `stopped` is set under the
        // parts lock in order to guarantee that no new workers are spawned
        // after the shutdown signal.
        {
            let mut state = inner.parts.lock();
            assert!(!state.stopped, "BUG: Table::must_close called twice");
            state.stopped = true;
        }
        inner.shutdown.signal();

        // Wait for the background workers to stop.
        loop {
            let handle = inner.threads.lock().pop();
            match handle {
                Some(h) => h.join().expect("BUG: table background worker panicked"),
                None => break,
            }
        }

        // Flush the remaining in-memory items to files.
        inner.flush_inmemory_items_to_files();

        // Remove references to the parts from the table, so they may be
        // eventually closed and deleted after all the searches are done.
        let file_parts = {
            let mut state = inner.parts.lock();

            let n = inner.raw_items.len();
            assert!(
                n == 0,
                "BUG: raw items must be empty at this stage; got {n} items"
            );

            let n = state.inmemory_parts.len();
            assert!(
                n == 0,
                "BUG: in-memory parts must be empty at this stage; got {n} parts"
            );

            std::mem::take(&mut state.file_parts)
        };

        for pw in file_parts {
            let refs = Arc::strong_count(&pw);
            assert!(
                refs == 1,
                "BUG: unexpected non-zero references to partWrapper when closing the table: {}",
                refs - 1
            );
            drop(pw);
        }

        // Wait for the scheduled part-directory removals, so the caller may
        // safely remove or re-open the table directory.
        esm_common::fs::remove_dir_async_drain();
    }
}

impl TableInner {
    fn start_background_workers(self: &Arc<Self>) {
        // Start file parts mergers, so they could start merging unmerged
        // parts if needed. There is no need in starting in-memory parts
        // mergers, since there are no in-memory parts yet.
        self.start_file_parts_mergers();

        let state = self.parts.lock();
        self.spawn_worker_locked(&state, |inner| inner.pending_items_flusher());
        self.spawn_worker_locked(&state, |inner| inner.inmemory_parts_flusher());
        if self.flush_callback.is_some() {
            self.spawn_worker_locked(&state, |inner| inner.flush_callback_worker());
        }
    }

    pub(crate) fn start_inmemory_parts_mergers(self: &Arc<Self>) {
        let state = self.parts.lock();
        for _ in 0..available_cpus() {
            self.start_inmemory_parts_merger_locked(&state);
        }
    }

    pub(crate) fn start_inmemory_parts_merger_locked(self: &Arc<Self>, state: &TableState) {
        self.spawn_worker_locked(state, |inner| inner.inmemory_parts_merger());
    }

    pub(crate) fn start_file_parts_mergers(self: &Arc<Self>) {
        let state = self.parts.lock();
        for _ in 0..file_parts_concurrency_cap() {
            self.start_file_parts_merger_locked(&state);
        }
    }

    pub(crate) fn start_file_parts_merger_locked(self: &Arc<Self>, state: &TableState) {
        self.spawn_worker_locked(state, |inner| inner.file_parts_merger());
    }

    fn spawn_worker_locked(
        self: &Arc<Self>,
        state: &TableState,
        f: impl FnOnce(&Arc<TableInner>) + Send + 'static,
    ) {
        if state.stopped {
            return;
        }
        let inner = Arc::clone(self);
        let handle = std::thread::spawn(move || f(&inner));
        let mut threads = self.threads.lock();
        // Reap already finished workers, so the handle list doesn't grow
        // unboundedly for lazily restarted mergers.
        let mut i = 0;
        while i < threads.len() {
            if threads[i].is_finished() {
                let h = threads.swap_remove(i);
                h.join().expect("BUG: table background worker panicked");
            } else {
                i += 1;
            }
        }
        threads.push(handle);
    }

    fn add_items(self: &Arc<Self>, items: &[&[u8]]) {
        let mut remaining = items;
        while !remaining.is_empty() {
            let (consumed, ibs_to_flush) = self.raw_items.add_items_to_shard(remaining);
            let ibs_to_merge = self.raw_items.add_ibs_to_flush(ibs_to_flush);
            self.flush_blocks_to_inmemory_parts(ibs_to_merge, false);
            remaining = &remaining[consumed..];
        }

        self.items_added
            .fetch_add(items.len() as u64, Ordering::Relaxed);
        let n: usize = items.iter().map(|item| item.len()).sum();
        self.items_added_size_bytes
            .fetch_add(n as u64, Ordering::Relaxed);
    }

    // --- background workers ---

    fn pending_items_flusher(self: &Arc<Self>) {
        // Do not add jitter in order to guarantee the flush interval.
        loop {
            if self.shutdown.wait_timeout(PENDING_ITEMS_FLUSH_INTERVAL) {
                return;
            }
            self.flush_pending_items(false);
        }
    }

    fn inmemory_parts_flusher(self: &Arc<Self>) {
        // Do not add jitter in order to guarantee the flush interval.
        loop {
            if self.shutdown.wait_timeout(self.flush_interval) {
                return;
            }
            self.flush_inmemory_parts_to_files(false);
        }
    }

    fn flush_callback_worker(self: &Arc<Self>) {
        let cb = self
            .flush_callback
            .as_ref()
            .expect("BUG: flush_callback must be set");
        // Call flush_callback at flush_callback_interval (with jitter) in
        // order to improve the effectiveness of caches which are reset by the
        // callback.
        let d = add_jitter_to_duration(self.flush_callback_interval);
        loop {
            if self.shutdown.wait_timeout(d) {
                cb();
                return;
            }
            if self
                .need_flush_callback_call
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                cb();
            }
        }
    }

    // --- flush paths ---

    pub(crate) fn flush_pending_items(self: &Arc<Self>, is_final: bool) {
        self.flush_pending_items_wg.add();
        let blocks = self.raw_items.take_blocks_to_flush(is_final);
        self.flush_blocks_to_inmemory_parts(blocks, is_final);
        self.flush_pending_items_wg.done();
    }

    pub(crate) fn flush_inmemory_items_to_files(self: &Arc<Self>) {
        self.flush_pending_items(true);
        self.flush_inmemory_parts_to_files(true);
    }

    pub(crate) fn flush_inmemory_parts_to_files(self: &Arc<Self>, is_final: bool) {
        let current_time = Instant::now();
        let pws: Vec<Arc<PartWrapper>> = {
            let state = self.parts.lock();
            state
                .inmemory_parts
                .iter()
                .filter(|pw| {
                    !pw.is_in_merge.load(Ordering::Relaxed)
                        && (is_final || pw.flush_to_disk_deadline < current_time)
                })
                .map(|pw| {
                    pw.is_in_merge.store(true, Ordering::Relaxed);
                    Arc::clone(pw)
                })
                .collect()
        };

        if let Err(e) = self.merge_inmemory_parts_to_files(pws) {
            panic!("FATAL: cannot merge in-memory parts to files: {e}");
        }
    }

    fn merge_inmemory_parts_to_files(
        self: &Arc<Self>,
        mut pws: Vec<Arc<PartWrapper>>,
    ) -> Result<(), String> {
        let pws_len = pws.len();
        let err_global: Mutex<Option<String>> = Mutex::new(None);

        std::thread::scope(|s| {
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = get_parts_for_optimal_merge(pws);
                pws = pws_remaining;
                inmemory_parts_concurrency().acquire();

                let err_global = &err_global;
                let this = self;
                s.spawn(move || {
                    // stop is None, so the merge cannot be forcibly stopped.
                    if let Err(err) = this.merge_parts(pws_to_merge, None, true) {
                        let mut e = err_global.lock();
                        if e.is_none() {
                            *e = Some(err.to_string());
                        }
                    }
                    inmemory_parts_concurrency().release();
                });
            }
        });

        match err_global.into_inner() {
            Some(e) => Err(format!("cannot optimally merge {pws_len} parts: {e}")),
            None => Ok(()),
        }
    }

    pub(crate) fn flush_blocks_to_inmemory_parts(
        self: &Arc<Self>,
        mut ibs: Vec<InmemoryBlock>,
        is_final: bool,
    ) {
        if ibs.is_empty() {
            return;
        }

        // Merge ibs into in-memory parts.
        let pws_shared: Mutex<Vec<Arc<PartWrapper>>> = Mutex::new(Vec::with_capacity(
            ibs.len().div_ceil(DEFAULT_PARTS_TO_MERGE),
        ));
        std::thread::scope(|s| {
            while !ibs.is_empty() {
                let n = DEFAULT_PARTS_TO_MERGE.min(ibs.len());
                let chunk: Vec<InmemoryBlock> = ibs.drain(..n).collect();
                inmemory_parts_concurrency().acquire();

                let pws_shared = &pws_shared;
                let this = &**self;
                s.spawn(move || {
                    if let Some(pw) = this.create_inmemory_part(chunk) {
                        pws_shared.lock().push(pw);
                    }
                    inmemory_parts_concurrency().release();
                });
            }
        });
        let mut pws = pws_shared.into_inner();

        // Merge pws into a single in-memory part.
        let max_part_size = get_max_inmemory_part_size();
        while pws.len() > 1 {
            pws = self.must_merge_inmemory_parts(pws);

            let mut pws_remaining = Vec::with_capacity(pws.len());
            for pw in pws {
                if pw.p.size >= max_part_size {
                    self.add_to_inmemory_parts(pw, is_final);
                } else {
                    pws_remaining.push(pw);
                }
            }
            pws = pws_remaining;
        }
        if let Some(pw) = pws.pop() {
            self.add_to_inmemory_parts(pw, is_final);
        }
    }

    fn add_to_inmemory_parts(self: &Arc<Self>, pw: Arc<PartWrapper>, is_final: bool) {
        // Wait until the number of in-memory parts goes below
        // MAX_INMEMORY_PARTS. This prevents from excess CPU usage during
        // search under high ingestion rate.
        if !self.inmemory_parts_limit.try_acquire() {
            self.inmemory_parts_limit_reached_count
                .fetch_add(1, Ordering::Relaxed);
            self.inmemory_parts_limit.acquire_or_stop(&self.shutdown);
        }

        {
            let mut state = self.parts.lock();
            state.inmemory_parts.push(pw);
            self.start_inmemory_parts_merger_locked(&state);
        }

        if let Some(cb) = &self.flush_callback {
            if is_final {
                cb();
            } else {
                // Use load in front of compare_exchange in order to avoid
                // slow inter-CPU synchronization when the flag is already set.
                if !self.need_flush_callback_call.load(Ordering::Acquire) {
                    let _ = self.need_flush_callback_call.compare_exchange(
                        false,
                        true,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
            }
        }
    }

    fn must_merge_inmemory_parts(
        self: &Arc<Self>,
        mut pws: Vec<Arc<PartWrapper>>,
    ) -> Vec<Arc<PartWrapper>> {
        let pws_result: Mutex<Vec<Arc<PartWrapper>>> = Mutex::new(Vec::new());
        std::thread::scope(|s| {
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = get_parts_for_optimal_merge(pws);
                pws = pws_remaining;
                inmemory_parts_concurrency().acquire();

                let pws_result = &pws_result;
                let this = &**self;
                s.spawn(move || {
                    let pw = this.must_merge_inmemory_parts_final(pws_to_merge);
                    pws_result.lock().push(pw);
                    inmemory_parts_concurrency().release();
                });
            }
        });
        pws_result.into_inner()
    }

    /// Merges the given in-memory part wrappers into a single new in-memory
    /// part wrapper. A single input wrapper is returned as is.
    pub(crate) fn must_merge_inmemory_parts_final(
        &self,
        mut pws: Vec<Arc<PartWrapper>>,
    ) -> Arc<PartWrapper> {
        assert!(
            !pws.is_empty(),
            "BUG: pws must contain at least a single item"
        );
        if pws.len() == 1 {
            // Nothing to merge.
            return pws.pop().expect("pws is non-empty");
        }

        let bsrs: Vec<BlockStreamReader> = pws
            .iter()
            .map(|pw| {
                let mp = pw.mp.as_ref().expect("BUG: unexpected file part");
                BlockStreamReader::from_inmemory_part(mp)
            })
            .collect();

        let flush_to_disk_deadline = get_flush_to_disk_deadline(&pws, self.flush_interval);
        // Dropping pws afterwards releases the references to the source parts.
        self.must_merge_into_inmemory_part(bsrs, flush_to_disk_deadline)
    }

    pub(crate) fn create_inmemory_part(&self, ibs: Vec<InmemoryBlock>) -> Option<Arc<PartWrapper>> {
        // Prepare blockStreamReaders for the source blocks.
        let mut bsrs: Vec<BlockStreamReader> = ibs
            .iter()
            .filter(|ib| !ib.items.is_empty())
            .map(BlockStreamReader::from_inmemory_block)
            .collect();
        if bsrs.is_empty() {
            return None;
        }

        let flush_to_disk_deadline = Instant::now() + self.flush_interval;
        if bsrs.len() == 1 {
            // Nothing to merge. Just return a single inmemory part.
            let mut bsr = bsrs.pop().expect("bsrs is non-empty");
            let mp = InmemoryPart::init(&mut bsr.block);
            return Some(PartWrapper::new_from_inmemory_part(
                mp,
                flush_to_disk_deadline,
            ));
        }

        Some(self.must_merge_into_inmemory_part(bsrs, flush_to_disk_deadline))
    }

    fn must_merge_into_inmemory_part(
        &self,
        mut bsrs: Vec<BlockStreamReader>,
        flush_to_disk_deadline: Instant,
    ) -> Arc<PartWrapper> {
        // Prepare the blockStreamWriter for the destination part.
        let out_items_count: u64 = bsrs.iter().map(|bsr| bsr.ph.items_count).sum();
        let compress_level = crate::table_merge::get_compress_level(out_items_count);
        let mut bsw = BlockStreamWriter::new_inmemory_part(compress_level);

        // Merge the parts. The merge shouldn't be interrupted, so pass no
        // stop signal.
        let res = self.merge_parts_internal(None, &mut bsw, &mut bsrs, PartType::Inmemory, None);
        for bsr in &mut bsrs {
            bsr.must_close();
        }
        let (ph, bufs) = res.unwrap_or_else(|e| panic!("FATAL: cannot merge inmemoryBlocks: {e}"));
        let mp =
            InmemoryPart::from_buffers(ph, bufs.expect("BUG: in-memory merge must return buffers"));

        PartWrapper::new_from_inmemory_part(mp, flush_to_disk_deadline)
    }

    // --- parts snapshotting for searches ---

    /// Returns a snapshot of the current parts. The parts stay alive for as
    /// long as the returned references are held.
    pub(crate) fn get_parts(&self) -> Vec<Arc<PartWrapper>> {
        let state = self.parts.lock();
        let mut dst = Vec::with_capacity(state.inmemory_parts.len() + state.file_parts.len());
        dst.extend(state.inmemory_parts.iter().cloned());
        dst.extend(state.file_parts.iter().cloned());
        dst
    }

    // --- snapshots ---

    fn must_create_snapshot_at(self: &Arc<Self>, dst_dir: &Path) {
        let src_dir = std::path::absolute(&self.path).unwrap_or_else(|e| {
            panic!("FATAL: cannot obtain absolute dir for {:?}: {e}", self.path)
        });
        let dst_dir = std::path::absolute(dst_dir)
            .unwrap_or_else(|e| panic!("FATAL: cannot obtain absolute dir for {dst_dir:?}: {e}"));
        assert!(
            !dst_dir.starts_with(&src_dir),
            "BUG: cannot create snapshot {dst_dir:?} inside the data dir {src_dir:?}"
        );

        // Flush inmemory items to disk.
        self.flush_inmemory_items_to_files();

        esm_common::fs::must_mkdir_fail_if_exist(&dst_dir);

        let pws = self.get_parts();

        // Create a file with the part names at dst_dir.
        must_write_part_names(&pws, &dst_dir);

        // Make hardlinks for the parts at dst_dir.
        for pw in &pws {
            if pw.mp.is_some() {
                // Skip in-memory parts.
                continue;
            }
            let src_part_path = &pw.p.path;
            let part_name = src_part_path
                .file_name()
                .unwrap_or_else(|| panic!("BUG: part path {src_part_path:?} has no base name"));
            let dst_part_path = dst_dir.join(part_name);
            esm_common::fs::must_hard_link_files(src_part_path, &dst_part_path);
        }

        esm_common::fs::must_sync_path_and_parent_dir(&dst_dir);
    }

    // --- metrics ---

    fn update_metrics(&self, m: &mut TableMetrics) {
        m.active_inmemory_merges += self.active_inmemory_merges.load(Ordering::Relaxed) as u64;
        m.active_file_merges += self.active_file_merges.load(Ordering::Relaxed) as u64;

        m.inmemory_merges_count += self.inmemory_merges_count.load(Ordering::Relaxed);
        m.file_merges_count += self.file_merges_count.load(Ordering::Relaxed);

        m.inmemory_items_merged += self.inmemory_items_merged.load(Ordering::Relaxed);
        m.file_items_merged += self.file_items_merged.load(Ordering::Relaxed);

        m.items_added += self.items_added.load(Ordering::Relaxed);
        m.items_added_size_bytes += self.items_added_size_bytes.load(Ordering::Relaxed);

        m.inmemory_parts_limit_reached_count += self
            .inmemory_parts_limit_reached_count
            .load(Ordering::Relaxed);

        m.pending_items += self.raw_items.len() as u64;

        let state = self.parts.lock();

        m.inmemory_parts_count += state.inmemory_parts.len() as u64;
        for pw in &state.inmemory_parts {
            m.inmemory_blocks_count += pw.p.ph.blocks_count;
            m.inmemory_items_count += pw.p.ph.items_count;
            m.inmemory_size_bytes += pw.p.size;
            m.parts_ref_count += Arc::strong_count(pw) as u64;
        }

        m.file_parts_count += state.file_parts.len() as u64;
        for pw in &state.file_parts {
            m.file_blocks_count += pw.p.ph.blocks_count;
            m.file_items_count += pw.p.ph.items_count;
            m.file_size_bytes += pw.p.size;
            m.parts_ref_count += Arc::strong_count(pw) as u64;
        }
        drop(state);

        m.too_long_items_dropped_total = TOO_LONG_ITEMS_TOTAL.load(Ordering::Relaxed);

        // The block-cache metrics are process-global (the caches are shared
        // by all the tables), so they are assigned rather than summed
        // (mirrors Go `Table.UpdateMetrics`).
        let c = crate::part::ib_cache();
        m.data_blocks_cache_size = c.len() as u64;
        m.data_blocks_cache_size_bytes = c.size_bytes() as u64;
        m.data_blocks_cache_size_max_bytes = c.size_max_bytes() as u64;
        m.data_blocks_cache_requests = c.requests();
        m.data_blocks_cache_misses = c.misses();

        let c = crate::part::ib_sparse_cache();
        m.data_blocks_sparse_cache_size = c.len() as u64;
        m.data_blocks_sparse_cache_size_bytes = c.size_bytes() as u64;
        m.data_blocks_sparse_cache_size_max_bytes = c.size_max_bytes() as u64;
        m.data_blocks_sparse_cache_requests = c.requests();
        m.data_blocks_sparse_cache_misses = c.misses();

        let c = crate::part::idxb_cache();
        m.index_blocks_cache_size = c.len() as u64;
        m.index_blocks_cache_size_bytes = c.size_bytes() as u64;
        m.index_blocks_cache_size_max_bytes = c.size_max_bytes() as u64;
        m.index_blocks_cache_requests = c.requests();
        m.index_blocks_cache_misses = c.misses();
    }
}

/// Essential metrics for a [`Table`].
#[derive(Debug, Default, Clone)]
pub struct TableMetrics {
    pub active_inmemory_merges: u64,
    pub active_file_merges: u64,

    pub inmemory_merges_count: u64,
    pub file_merges_count: u64,

    pub inmemory_items_merged: u64,
    pub file_items_merged: u64,

    pub items_added: u64,
    pub items_added_size_bytes: u64,

    pub inmemory_parts_limit_reached_count: u64,

    pub pending_items: u64,

    pub inmemory_parts_count: u64,
    pub file_parts_count: u64,

    pub inmemory_blocks_count: u64,
    pub file_blocks_count: u64,

    pub inmemory_items_count: u64,
    pub file_items_count: u64,

    pub inmemory_size_bytes: u64,
    pub file_size_bytes: u64,

    pub parts_ref_count: u64,

    pub too_long_items_dropped_total: u64,

    pub data_blocks_cache_size: u64,
    pub data_blocks_cache_size_bytes: u64,
    pub data_blocks_cache_size_max_bytes: u64,
    pub data_blocks_cache_requests: u64,
    pub data_blocks_cache_misses: u64,

    pub data_blocks_sparse_cache_size: u64,
    pub data_blocks_sparse_cache_size_bytes: u64,
    pub data_blocks_sparse_cache_size_max_bytes: u64,
    pub data_blocks_sparse_cache_requests: u64,
    pub data_blocks_sparse_cache_misses: u64,

    pub index_blocks_cache_size: u64,
    pub index_blocks_cache_size_bytes: u64,
    pub index_blocks_cache_size_max_bytes: u64,
    pub index_blocks_cache_requests: u64,
    pub index_blocks_cache_misses: u64,
}

impl TableMetrics {
    /// The total number of items in the table.
    pub fn total_items_count(&self) -> u64 {
        self.inmemory_items_count + self.file_items_count
    }
}

fn add_jitter_to_duration(d: Duration) -> Duration {
    // Add up to 10% jitter (upstream timeutil.AddJitterToDuration).
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|x| x.subsec_nanos() as u64)
        .unwrap_or(0);
    let p = d.as_nanos() as u64 / 10;
    if p == 0 {
        return d;
    }
    d + Duration::from_nanos(nanos % p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("esm-mergeset-table-unit-{name}"))
    }

    #[test]
    fn must_merge_inmemory_parts_final_ref_count() {
        let path = test_dir("merge-final-refcount");
        let _ = std::fs::remove_dir_all(&path);

        let tb = Table::must_open(
            &path,
            Duration::ZERO,
            None,
            Duration::ZERO,
            None,
            Arc::new(AtomicBool::new(false)),
        );

        let generate_part_wrappers = |n: usize| -> Vec<Arc<PartWrapper>> {
            (0..n)
                .map(|i| {
                    let mut ib = InmemoryBlock::default();
                    let items = vec![i as u8; 1024];
                    assert!(ib.add(&items));
                    tb.inner
                        .create_inmemory_part(vec![ib])
                        .expect("part must be created")
                })
                .collect()
        };

        let assert_ref_count = |pws: &[Arc<PartWrapper>], want: usize| {
            for pw in pws {
                assert_eq!(
                    Arc::strong_count(pw),
                    want,
                    "unexpected part wrapper ref count"
                );
            }
        };

        // Single source part wrapper: returned as is.
        let pws_src = generate_part_wrappers(1);
        assert_ref_count(&pws_src, 1);
        let pw_final = tb.inner.must_merge_inmemory_parts_final(pws_src.clone());
        assert!(Arc::ptr_eq(&pw_final, &pws_src[0]));
        assert_eq!(Arc::strong_count(&pw_final), 2); // pws_src + pw_final

        // Many source part wrappers: sources released after the merge.
        let pws_src = generate_part_wrappers(100);
        let pws_clones = pws_src.clone();
        assert_ref_count(&pws_src, 2);
        let pw_final = tb.inner.must_merge_inmemory_parts_final(pws_clones);
        assert_ref_count(&pws_src, 1); // only the test's own references remain
        assert_eq!(Arc::strong_count(&pw_final), 1);

        drop(pw_final);
        drop(pws_src);
        tb.must_close();
        let _ = std::fs::remove_dir_all(&path);
    }
}
