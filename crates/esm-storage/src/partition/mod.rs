//! Stage 4: port of the upstream VictoriaMetrics v1.146.0 lib/storage/partition.go —
//! a monthly partition holding inmemory/small/big part sets, the rawRows
//! ingestion shards with their flushers, merge scheduling and retention
//! enforcement. The merge machinery lives in [`merge`], the search side in
//! [`search`].
//!
//! PORT-SKIP (spec §8): read-only `NotifyReadWriteMode` re-start hooks (the
//! free-disk watcher is skipped at the storage level), partition metrics
//! beyond simple counters.

pub(crate) mod merge;
pub(crate) mod raw_rows;
pub(crate) mod search;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::index::IndexDb;
use crate::part::{InmemoryPart, Part};
use crate::raw_row::RawRow;
use crate::storage::StorageEnv;
use crate::sync_util::{add_jitter_to_duration, available_cpus, Sema, Shutdown};

use crate::time_range::{timestamp_to_partition_name, TimeRange};
use raw_rows::RawRowsShards;

/// The maximum size of big part.
pub(crate) const MAX_BIG_PART_SIZE: u64 = 1_000_000_000_000;

/// The maximum expected number of inmemory parts per partition.
pub(crate) const MAX_INMEMORY_PARTS: usize = 60;

/// Default number of parts to merge at once.
pub(crate) const DEFAULT_PARTS_TO_MERGE: usize = 15;

/// The interval for flushing buffered rows into parts, so they become
/// visible to search.
pub(crate) const PENDING_ROWS_FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// The interval for guaranteed flush of recently ingested data from memory
/// to on-disk parts, so they survive process crash.
pub(crate) const DATA_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// The maximum number of rawRow items in a rawRowsShard (8MiB of rows).
pub(crate) fn max_raw_rows_per_shard() -> usize {
    (8 << 20) / std::mem::size_of::<RawRow>()
}

/// The number of rawRows shards per partition.
fn raw_rows_shards_per_partition() -> usize {
    available_cpus()
}

// --- global merge concurrency semaphores (spec §4.5) ---

pub(crate) fn inmemory_parts_concurrency() -> &'static Sema {
    // The concurrency for processing in-memory parts must equal the number
    // of CPU cores, since these operations are CPU-bound.
    static SEMA: OnceLock<Sema> = OnceLock::new();
    SEMA.get_or_init(|| Sema::new(available_cpus()))
}

pub(crate) fn small_parts_concurrency() -> &'static Sema {
    // Allow at least 4 concurrent workers for small parts on systems with
    // fewer than 4 CPU cores, so smaller part merges can be made while
    // bigger part merges are in progress.
    static SEMA: OnceLock<Sema> = OnceLock::new();
    SEMA.get_or_init(|| Sema::new(available_cpus().max(4)))
}

pub(crate) fn big_parts_concurrency() -> &'static Sema {
    static SEMA: OnceLock<Sema> = OnceLock::new();
    SEMA.get_or_init(|| Sema::new(available_cpus().max(4)))
}

// --- partWrapper ---

/// A refcounted wrapper around a part; the `Arc` strong count plays the role
/// of Go's manual `refCount`. Go: partWrapper.
pub(crate) struct PartWrapper {
    /// The part itself. `Arc`ed separately so searches can outlive the
    /// wrapper (deletion of dropped parts happens when the last `Arc<Part>`
    /// is released).
    pub p: Arc<Part>,

    /// The in-memory part backing `p`, if any.
    pub mp: Option<InmemoryPart>,

    /// Marks the part directory for deletion once the wrapper is dropped.
    /// This field should be updated only after the wrapper was removed from
    /// the list of active parts.
    pub must_drop: AtomicBool,

    /// Whether the part takes part in a merge. Guarded by the partition
    /// parts lock (stored as an atomic only to keep `PartWrapper: Sync`).
    pub is_in_merge: AtomicBool,

    /// The deadline when the in-memory part must be flushed to disk.
    pub flush_to_disk_deadline: Instant,
}

impl PartWrapper {
    pub fn new_from_inmemory_part(
        mp: InmemoryPart,
        flush_to_disk_deadline: Instant,
    ) -> Arc<PartWrapper> {
        let p = Arc::new(mp.new_part());
        Arc::new(PartWrapper {
            p,
            mp: Some(mp),
            must_drop: AtomicBool::new(false),
            is_in_merge: AtomicBool::new(false),
            flush_to_disk_deadline,
        })
    }

    pub fn new_from_file_part(p: Part) -> Arc<PartWrapper> {
        Arc::new(PartWrapper {
            p: Arc::new(p),
            mp: None,
            must_drop: AtomicBool::new(false),
            is_in_merge: AtomicBool::new(false),
            flush_to_disk_deadline: Instant::now(),
        })
    }
}

impl Drop for PartWrapper {
    fn drop(&mut self) {
        if self.must_drop.load(Ordering::Acquire) && self.mp.is_none() {
            // The actual directory removal happens when the last Arc<Part>
            // reference (e.g. held by an in-flight search) is dropped.
            self.p.must_drop_on_release.store(true, Ordering::Release);
        }
    }
}

/// Returns the earliest flush-to-disk deadline among the in-memory `pws`.
/// Go: getFlushToDiskDeadline.
pub(crate) fn get_flush_to_disk_deadline(pws: &[Arc<PartWrapper>]) -> Instant {
    let mut d = Instant::now() + DATA_FLUSH_INTERVAL;
    for pw in pws {
        if pw.mp.is_some() && pw.flush_to_disk_deadline < d {
            d = pw.flush_to_disk_deadline;
        }
    }
    d
}

pub(crate) fn get_parts_size(pws: &[Arc<PartWrapper>]) -> u64 {
    pws.iter().map(|pw| pw.p.size).sum()
}

pub(crate) fn are_all_inmemory_parts(pws: &[Arc<PartWrapper>]) -> bool {
    pws.iter().all(|pw| pw.mp.is_some())
}

pub(crate) fn make_ptr_set(pws: &[Arc<PartWrapper>]) -> HashSet<*const PartWrapper> {
    let m: HashSet<*const PartWrapper> = pws.iter().map(Arc::as_ptr).collect();
    assert!(
        m.len() == pws.len(),
        "BUG: {} duplicate parts found in {} source parts",
        pws.len() - m.len(),
        pws.len()
    );
    m
}

/// Removes parts listed in `parts_to_remove` from `pws`. Returns the number
/// of removed parts. Go: removeParts.
pub(crate) fn remove_parts(
    pws: &mut Vec<Arc<PartWrapper>>,
    parts_to_remove: &HashSet<*const PartWrapper>,
) -> usize {
    let before = pws.len();
    pws.retain(|pw| !parts_to_remove.contains(&Arc::as_ptr(pw)));
    before - pws.len()
}

// --- partition ---

pub(crate) struct PartsState {
    /// Inmemory parts with recently ingested data, visible for search.
    pub inmemory_parts: Vec<Arc<PartWrapper>>,
    /// File-based parts with a small number of rows, visible for search.
    pub small_parts: Vec<Arc<PartWrapper>>,
    /// File-based parts with a big number of rows, visible for search.
    pub big_parts: Vec<Arc<PartWrapper>>,
    /// Set when the partition is being closed. Guards against spawning
    /// workers after shutdown (Go's "wg.Add only under partsLock"
    /// discipline).
    pub stopped: bool,
}

/// The partition internals shared between the [`Partition`] handle and the
/// background worker threads. Go: partition.
pub(crate) struct PtInner {
    pub inmemory_rows_merged: AtomicU64,
    pub small_rows_merged: AtomicU64,
    pub big_rows_merged: AtomicU64,

    pub inmemory_rows_deleted: AtomicU64,
    pub small_rows_deleted: AtomicU64,
    pub big_rows_deleted: AtomicU64,

    pub inmemory_merges_count: AtomicU64,
    pub small_merges_count: AtomicU64,
    pub big_merges_count: AtomicU64,

    pub is_dedup_scheduled: AtomicBool,

    pub merge_idx: AtomicU64,

    pub small_parts_path: PathBuf,
    pub big_parts_path: PathBuf,
    pub index_db_parts_path: PathBuf,

    /// The storage-level environment (Go: partition.s).
    pub env: Arc<StorageEnv>,

    /// The name of the partition in the form YYYY_MM.
    pub name: String,

    /// The time range for the partition (a whole month).
    pub tr: TimeRange,

    /// Recently added rows that haven't been converted into parts yet.
    /// They aren't visible for search for performance reasons.
    pub raw_rows: RawRowsShards,

    /// Protects the parts lists and worker spawning (Go: partsLock).
    pub parts: Mutex<PartsState>,

    /// The inverted index for the data stored in this partition.
    pub idb: IndexDb,

    /// Notifies all the background workers to stop (Go: stopCh).
    pub shutdown: Shutdown,

    /// Handles of the background workers (Go: wg).
    pub threads: Mutex<Vec<JoinHandle<()>>>,
}

/// A monthly partition. Go: partition (the handle side).
pub(crate) struct Partition {
    pub(crate) inner: Arc<PtInner>,
}

/// Creates a new partition for the given timestamp. Go: mustCreatePartition.
pub(crate) fn must_create_partition(
    timestamp: i64,
    small_partitions_path: &Path,
    big_partitions_path: &Path,
    index_db_path: &Path,
    env: Arc<StorageEnv>,
) -> Partition {
    let tr = TimeRange::from_partition_timestamp(timestamp);
    let name = timestamp_to_partition_name(timestamp);

    let small_parts_path = small_partitions_path.join(&name);
    let big_parts_path = big_partitions_path.join(&name);
    let index_db_parts_path = index_db_path.join(&name);
    log::info!(
        "creating a partition {name:?} with smallPartsPath={small_parts_path:?}, \
         bigPartsPath={big_parts_path:?}, indexDBPartsPath={index_db_parts_path:?}"
    );

    esm_common::fs::must_mkdir_fail_if_exist(&small_parts_path);
    esm_common::fs::must_mkdir_fail_if_exist(&big_parts_path);
    esm_common::fs::must_mkdir_fail_if_exist(&index_db_parts_path);

    // Create the parts.json file. Since we are creating a new partition,
    // there are no parts yet.
    merge::must_write_part_names(&[], &[], &small_parts_path);

    let pt = new_partition(
        &name,
        small_parts_path.clone(),
        big_parts_path.clone(),
        index_db_parts_path.clone(),
        tr,
        env,
    );

    esm_common::fs::must_sync_path_and_parent_dir(&small_parts_path);
    esm_common::fs::must_sync_path_and_parent_dir(&big_parts_path);
    esm_common::fs::must_sync_path_and_parent_dir(&index_db_parts_path);

    pt.inner.start_background_workers();

    log::info!("partition {name:?} has been created");
    pt
}

/// Opens the existing partition from the given paths.
/// Go: mustOpenPartition.
pub(crate) fn must_open_partition(
    small_parts_path: &Path,
    big_parts_path: &Path,
    index_db_parts_path: &Path,
    env: Arc<StorageEnv>,
) -> Partition {
    // Create paths to parts if they are missing.
    esm_common::fs::must_mkdir_if_not_exist(small_parts_path);
    esm_common::fs::must_mkdir_if_not_exist(big_parts_path);
    esm_common::fs::must_mkdir_if_not_exist(index_db_parts_path);

    let name = small_parts_path
        .file_name()
        .unwrap_or_else(|| panic!("BUG: smallPartsPath {small_parts_path:?} has no base name"))
        .to_string_lossy()
        .into_owned();
    let tr = TimeRange::from_partition_name(&name).unwrap_or_else(|err| {
        panic!(
            "FATAL: cannot obtain partition time range from smallPartsPath \
             {small_parts_path:?}: {err}"
        )
    });
    assert!(
        big_parts_path.ends_with(&name) && index_db_parts_path.ends_with(&name),
        "FATAL: partition name in bigPartsPath {big_parts_path:?} / indexDBPartsPath \
         {index_db_parts_path:?} doesn't match smallPartsPath {small_parts_path:?}; want {name:?}"
    );

    let parts_file = small_parts_path.join(merge::PARTS_FILENAME);
    let (part_names_small, part_names_big) =
        merge::must_read_part_names(&parts_file, small_parts_path, big_parts_path);

    let small_parts = merge::must_open_parts(&parts_file, small_parts_path, &part_names_small);
    let big_parts = merge::must_open_parts(&parts_file, big_parts_path, &part_names_big);

    if !esm_common::fs::is_path_exist(&parts_file) {
        // Create the parts.json file if it doesn't exist yet.
        merge::must_write_part_names(&small_parts, &big_parts, small_parts_path);
    }

    let pt = new_partition(
        &name,
        small_parts_path.to_path_buf(),
        big_parts_path.to_path_buf(),
        index_db_parts_path.to_path_buf(),
        tr,
        env,
    );
    {
        let mut state = pt.inner.parts.lock();
        state.small_parts = small_parts;
        state.big_parts = big_parts;
    }

    esm_common::fs::must_sync_path_and_parent_dir(small_parts_path);
    esm_common::fs::must_sync_path_and_parent_dir(big_parts_path);

    pt.inner.start_background_workers();
    pt
}

fn new_partition(
    name: &str,
    small_parts_path: PathBuf,
    big_parts_path: PathBuf,
    index_db_parts_path: PathBuf,
    tr: TimeRange,
    env: Arc<StorageEnv>,
) -> Partition {
    let id = tr.min_timestamp as u64;
    let idb = IndexDb::must_open(
        id,
        tr,
        name,
        &index_db_parts_path,
        Arc::clone(&env.idb_ctx),
        Arc::clone(&env.is_read_only),
        false,
    );

    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    Partition {
        inner: Arc::new(PtInner {
            inmemory_rows_merged: AtomicU64::new(0),
            small_rows_merged: AtomicU64::new(0),
            big_rows_merged: AtomicU64::new(0),
            inmemory_rows_deleted: AtomicU64::new(0),
            small_rows_deleted: AtomicU64::new(0),
            big_rows_deleted: AtomicU64::new(0),
            inmemory_merges_count: AtomicU64::new(0),
            small_merges_count: AtomicU64::new(0),
            big_merges_count: AtomicU64::new(0),
            is_dedup_scheduled: AtomicBool::new(false),
            merge_idx: AtomicU64::new(now_nanos),
            small_parts_path,
            big_parts_path,
            index_db_parts_path,
            env,
            name: name.to_string(),
            tr,
            raw_rows: RawRowsShards::new(raw_rows_shards_per_partition()),
            parts: Mutex::new(PartsState {
                inmemory_parts: Vec::new(),
                small_parts: Vec::new(),
                big_parts: Vec::new(),
                stopped: false,
            }),
            idb,
            shutdown: Shutdown::new(),
            threads: Mutex::new(Vec::new()),
        }),
    }
}

impl Partition {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn time_range(&self) -> TimeRange {
        self.inner.tr
    }

    pub fn idb(&self) -> &IndexDb {
        &self.inner.idb
    }

    /// Returns true if the partition contains the given timestamp.
    /// Go: partition.HasTimestamp.
    pub fn has_timestamp(&self, timestamp: i64) -> bool {
        self.inner.tr.contains(timestamp)
    }

    /// Adds the given rows to the partition. All the rows must fit the
    /// partition by timestamp range. Go: partition.AddRows.
    pub fn add_rows(&self, rows: &[RawRow]) {
        if rows.is_empty() {
            return;
        }
        self.inner.raw_rows.add_rows(&self.inner, rows);
    }

    /// Returns a snapshot of the partition parts. The parts are kept alive
    /// by the returned `Arc`s. Go: partition.GetParts + PutParts.
    pub fn get_parts(&self, add_in_memory: bool) -> Vec<Arc<PartWrapper>> {
        let state = self.inner.parts.lock();
        let mut dst = Vec::with_capacity(
            state.inmemory_parts.len() + state.small_parts.len() + state.big_parts.len(),
        );
        if add_in_memory {
            dst.extend(state.inmemory_parts.iter().cloned());
        }
        dst.extend(state.small_parts.iter().cloned());
        dst.extend(state.big_parts.iter().cloned());
        dst
    }

    /// Flushes pending raw index and data rows of this partition so they
    /// become visible to search. For debug purposes only.
    /// Go: partition.DebugFlush.
    pub fn debug_flush(&self) {
        self.inner.idb.debug_flush();
        self.inner.flush_pending_rows(true);
    }

    /// Creates a snapshot of the partition's file parts at the given
    /// destination dirs using hard links. Go: partition.MustCreateSnapshotAt.
    ///
    /// In-memory rows/parts are force-flushed to files first; parts that are
    /// still purely in-memory afterwards are skipped (matches upstream).
    pub(crate) fn must_create_snapshot_at(
        &self,
        small_dst: &Path,
        big_dst: &Path,
        indexdb_dst: &Path,
    ) {
        self.inner.flush_inmemory_rows_to_files();

        // Ref the current file parts under the parts lock so a concurrent
        // merge cannot drop their directories while we hard-link them
        // (PartWrapper::Drop removes dirs only when the last Arc goes away).
        let (pws_small, pws_big) = {
            let state = self.inner.parts.lock();
            (state.small_parts.clone(), state.big_parts.clone())
        };

        esm_common::fs::must_mkdir_fail_if_exist(small_dst);
        esm_common::fs::must_mkdir_fail_if_exist(big_dst);

        // parts.json lives in the small dir and lists both small and big
        // part names (matches must_create_partition / swap_src_with_dst_parts).
        merge::must_write_part_names(&pws_small, &pws_big, small_dst);

        Self::must_hard_link_parts(&pws_small, small_dst);
        Self::must_hard_link_parts(&pws_big, big_dst);

        esm_common::fs::must_sync_path_and_parent_dir(small_dst);
        esm_common::fs::must_sync_path_and_parent_dir(big_dst);

        // The per-partition inverted index is an esm-mergeset table, which
        // already knows how to snapshot itself.
        self.inner.idb.tb().must_create_snapshot_at(indexdb_dst);
    }

    fn must_hard_link_parts(pws: &[Arc<PartWrapper>], dst_dir: &Path) {
        for pw in pws {
            if pw.mp.is_some() {
                continue; // skip in-memory parts
            }
            let src_part_path = &pw.p.path;
            let part_name = src_part_path
                .file_name()
                .unwrap_or_else(|| panic!("BUG: part path {src_part_path:?} has no base name"));
            esm_common::fs::must_hard_link_files(src_part_path, dst_dir.join(part_name));
        }
    }

    /// Runs a merge for all the parts in the partition.
    /// Go: partition.ForceMergeAllParts.
    pub fn force_merge_all_parts(&self, stop: Option<&AtomicBool>) -> Result<(), String> {
        self.inner.force_merge_all_parts(stop)
    }

    /// Whether the final dedup pass must run for this partition.
    /// Go: partition.isFinalDedupNeeded.
    pub fn is_final_dedup_needed(&self) -> bool {
        let dedup_interval = crate::dedup::get_dedup_interval();
        let pws = self.get_parts(false);
        let min_dedup_interval = pws
            .iter()
            .map(|pw| pw.p.ph.min_dedup_interval)
            .min()
            .unwrap_or(0);
        dedup_interval > min_dedup_interval
    }

    pub fn set_dedup_scheduled(&self, v: bool) {
        self.inner.is_dedup_scheduled.store(v, Ordering::Relaxed);
    }

    /// Closes the partition. The partition must be detached from the table
    /// before calling this. Go: partition.MustClose.
    pub fn must_close(self) {
        // Notify the background workers to stop. `stopped` is set under the
        // parts lock to guarantee no new workers are spawned after the
        // shutdown signal (Go's wg.Add/stopCh discipline).
        {
            let mut state = self.inner.parts.lock();
            assert!(
                !state.stopped,
                "BUG: partition {:?} closed twice",
                self.inner.name
            );
            state.stopped = true;
        }
        self.inner.shutdown.signal();

        // Wait for the background workers to stop.
        loop {
            let handle = self.inner.threads.lock().pop();
            match handle {
                Some(h) => h.join().expect("BUG: partition background worker panicked"),
                None => break,
            }
        }

        // Flush the remaining in-memory rows to files.
        self.inner.flush_inmemory_rows_to_files();

        // Remove the references from the parts, so they may be eventually
        // closed after all the searches are done.
        let (small_parts, big_parts) = {
            let mut state = self.inner.parts.lock();

            let n = self.inner.raw_rows.len();
            assert!(
                n == 0,
                "BUG: raw rows must be empty at this stage; got {n} rows"
            );
            let n = state.inmemory_parts.len();
            assert!(
                n == 0,
                "BUG: in-memory parts must be empty at this stage; got {n} parts"
            );

            (
                std::mem::take(&mut state.small_parts),
                std::mem::take(&mut state.big_parts),
            )
        };
        for pw in small_parts.into_iter().chain(big_parts) {
            let refs = Arc::strong_count(&pw);
            assert!(
                refs == 1,
                "BUG: unexpected {} extra references to a part when closing partition {:?}",
                refs - 1,
                self.inner.name
            );
            drop(pw);
        }

        // Close the indexDB. At this point nothing else references the
        // partition internals.
        let inner = Arc::try_unwrap(self.inner)
            .ok()
            .expect("BUG: pending references to the partition internals on close");
        inner.idb.must_close();
    }

    /// Drops all the data of the partition on disk. The partition must be
    /// closed first. Go: partition.Drop.
    pub(crate) fn drop_paths(small: &Path, big: &Path, indexdb: &Path) {
        log::info!("dropping partition at {small:?}, {big:?}, {indexdb:?}");
        // Wait for scheduled part-directory removals, so the recursive
        // removals below don't race the background dir remover over
        // subdirectories of the partition.
        esm_common::fs::remove_dir_async_drain();
        esm_common::fs::must_remove_dir(small);
        esm_common::fs::must_remove_dir(big);
        esm_common::fs::must_remove_dir(indexdb);
    }
}

impl PtInner {
    fn start_background_workers(self: &Arc<Self>) {
        // Start file parts mergers, so they could start merging unmerged
        // parts if needed. There is no need in starting in-memory parts
        // mergers, since there are no in-memory parts yet.
        self.start_small_parts_mergers();
        self.start_big_parts_mergers();

        let state = self.parts.lock();
        self.spawn_worker_locked(&state, |inner| inner.pending_rows_flusher());
        self.spawn_worker_locked(&state, |inner| inner.inmemory_parts_flusher());
        self.spawn_worker_locked(&state, |inner| inner.stale_parts_remover());
    }

    pub(crate) fn start_inmemory_parts_merger_locked(self: &Arc<Self>, state: &PartsState) {
        self.spawn_worker_locked(state, |inner| inner.inmemory_parts_merger());
    }

    fn start_small_parts_mergers(self: &Arc<Self>) {
        let state = self.parts.lock();
        for _ in 0..small_parts_concurrency().capacity() {
            self.start_small_parts_merger_locked(&state);
        }
    }

    pub(crate) fn start_small_parts_merger_locked(self: &Arc<Self>, state: &PartsState) {
        self.spawn_worker_locked(state, |inner| inner.small_parts_merger());
    }

    fn start_big_parts_mergers(self: &Arc<Self>) {
        let state = self.parts.lock();
        for _ in 0..big_parts_concurrency().capacity() {
            self.start_big_parts_merger_locked(&state);
        }
    }

    pub(crate) fn start_big_parts_merger_locked(self: &Arc<Self>, state: &PartsState) {
        self.spawn_worker_locked(state, |inner| inner.big_parts_merger());
    }

    fn spawn_worker_locked(
        self: &Arc<Self>,
        state: &PartsState,
        f: impl FnOnce(&Arc<PtInner>) + Send + 'static,
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
                h.join().expect("BUG: partition background worker panicked");
            } else {
                i += 1;
            }
        }
        threads.push(handle);
    }

    // --- flushers (Go: pendingRowsFlusher / inmemoryPartsFlusher) ---

    fn pending_rows_flusher(self: &Arc<Self>) {
        // Do not add jitter in order to guarantee the flush interval.
        loop {
            if self.shutdown.wait_timeout(PENDING_ROWS_FLUSH_INTERVAL) {
                return;
            }
            self.flush_pending_rows(false);
        }
    }

    fn inmemory_parts_flusher(self: &Arc<Self>) {
        // Do not add jitter in order to guarantee the flush interval.
        loop {
            if self.shutdown.wait_timeout(DATA_FLUSH_INTERVAL) {
                return;
            }
            self.flush_inmemory_parts_to_files(false);
        }
    }

    fn stale_parts_remover(self: &Arc<Self>) {
        let d = add_jitter_to_duration(Duration::from_secs(7 * 60));
        loop {
            if self.shutdown.wait_timeout(d) {
                return;
            }
            self.remove_stale_parts();
        }
    }

    pub(crate) fn flush_pending_rows(self: &Arc<Self>, is_final: bool) {
        self.raw_rows.flush(self, is_final);
    }

    pub(crate) fn flush_inmemory_rows_to_files(self: &Arc<Self>) {
        self.flush_pending_rows(true);
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

        if let Err(err) = self.merge_parts_to_files(pws, None, inmemory_parts_concurrency()) {
            panic!("FATAL: cannot merge in-memory parts: {err}");
        }
    }

    // --- rawRows -> inmemory parts (Go: flushRowssToInmemoryParts) ---

    pub(crate) fn flush_rowss_to_inmemory_parts(self: &Arc<Self>, rowss: Vec<Vec<RawRow>>) {
        if rowss.is_empty() {
            return;
        }

        // Convert rowss into in-memory parts.
        let pws_lock: Mutex<Vec<Arc<PartWrapper>>> = Mutex::new(Vec::with_capacity(rowss.len()));
        std::thread::scope(|scope| {
            for mut rows in rowss {
                inmemory_parts_concurrency().acquire();
                let pws_lock = &pws_lock;
                scope.spawn(move || {
                    if let Some(pw) = Self::create_inmemory_part(&mut rows, self.tr) {
                        pws_lock.lock().push(pw);
                    }
                    inmemory_parts_concurrency().release();
                });
            }
        });
        let mut pws = pws_lock.into_inner();

        // Merge pws into a single in-memory part.
        let max_part_size = merge::get_max_inmemory_part_size();
        while pws.len() > 1 {
            pws = self.must_merge_inmemory_parts(pws);

            let mut pws_remaining = Vec::with_capacity(pws.len());
            for pw in pws {
                if pw.p.size >= max_part_size {
                    self.add_to_inmemory_parts(pw);
                } else {
                    pws_remaining.push(pw);
                }
            }
            pws = pws_remaining;
        }
        if let Some(pw) = pws.pop() {
            self.add_to_inmemory_parts(pw);
        }
    }

    fn add_to_inmemory_parts(self: &Arc<Self>, pw: Arc<PartWrapper>) {
        let mut state = self.parts.lock();
        state.inmemory_parts.push(pw);
        self.start_inmemory_parts_merger_locked(&state);
    }

    /// Repeatedly merges optimal groups of the given in-memory parts.
    /// Go: mustMergeInmemoryParts.
    fn must_merge_inmemory_parts(
        self: &Arc<Self>,
        mut pws: Vec<Arc<PartWrapper>>,
    ) -> Vec<Arc<PartWrapper>> {
        let result: Mutex<Vec<Arc<PartWrapper>>> = Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = merge::get_parts_for_optimal_merge(pws);
                pws = pws_remaining;
                inmemory_parts_concurrency().acquire();
                let result = &result;
                scope.spawn(move || {
                    if let Some(pw) = self.must_merge_inmemory_parts_final(pws_to_merge) {
                        result.lock().push(pw);
                    }
                    inmemory_parts_concurrency().release();
                });
            }
        });
        result.into_inner()
    }

    /// Merges the given in-memory parts into a single new in-memory part.
    /// Returns None if the merge results in an empty part.
    /// Go: mustMergeInmemoryPartsFinal.
    fn must_merge_inmemory_parts_final(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
    ) -> Option<Arc<PartWrapper>> {
        assert!(!pws.is_empty(), "BUG: pws must contain at least one item");
        if pws.len() == 1 {
            // Nothing to merge.
            return Some(pws.into_iter().next().unwrap());
        }

        let mut bsrs: Vec<crate::block_stream::BlockStreamReader> = pws
            .iter()
            .map(|pw| {
                let mp = pw
                    .mp
                    .as_ref()
                    .unwrap_or_else(|| panic!("BUG: unexpected file part"));
                crate::block_stream::BlockStreamReader::from_inmemory_part(mp)
            })
            .collect();

        // Determine flushToDiskDeadline before performing the actual merge,
        // in order to guarantee the correct deadline, since the merge may
        // take significant amounts of time.
        let flush_to_disk_deadline = get_flush_to_disk_deadline(&pws);

        let src_rows_count: u64 = pws.iter().map(|pw| pw.p.ph.rows_count).sum();
        let src_blocks_count: u64 = pws.iter().map(|pw| pw.p.ph.blocks_count).sum();
        let compress_level = crate::block_stream::get_compress_level(
            src_rows_count as f64 / src_blocks_count.max(1) as f64,
        );
        let mut bsw = crate::block_stream::BlockStreamWriter::new_inmemory_part(compress_level);

        // Merge the parts. The merge shouldn't be interrupted on shutdown,
        // so pass no stop flag.
        let (ph, bufs) = self
            .merge_parts_internal(
                None,
                &mut bsw,
                &mut bsrs,
                merge::PartType::Inmemory,
                None,
                crate::sync_util::now_unix_milli(),
            )
            .unwrap_or_else(|err| panic!("FATAL: cannot merge inmemory parts: {err}"));
        drop(bsrs);
        drop(pws);

        // The resulting part is empty; no need to create a part wrapper.
        if ph.blocks_count == 0 {
            return None;
        }

        let mp =
            InmemoryPart::from_buffers(ph, bufs.expect("BUG: in-memory merge must return buffers"));
        Some(PartWrapper::new_from_inmemory_part(
            mp,
            flush_to_disk_deadline,
        ))
    }

    /// Go: createInmemoryPart.
    fn create_inmemory_part(rows: &mut [RawRow], tr: TimeRange) -> Option<Arc<PartWrapper>> {
        if rows.is_empty() {
            return None;
        }
        let mp = InmemoryPart::init_from_rows(rows);

        // Make sure the part may be added.
        assert!(
            mp.ph.min_timestamp <= mp.ph.max_timestamp
                && mp.ph.min_timestamp >= tr.min_timestamp
                && mp.ph.max_timestamp <= tr.max_timestamp,
            "BUG: the part {} cannot be added to the partition with time range {tr}",
            mp.ph
        );
        let flush_to_disk_deadline = Instant::now() + DATA_FLUSH_INTERVAL;
        Some(PartWrapper::new_from_inmemory_part(
            mp,
            flush_to_disk_deadline,
        ))
    }
}
