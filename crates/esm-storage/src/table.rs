//! Stage 4: port of the upstream VictoriaMetrics v1.146.0 lib/storage/table.go — the
//! monthly partition set with refcounted partition wrappers, the retention
//! watcher and the final-dedup (historical merge) watcher.
//!
//! PORT-SKIP: snapshots, `NotifyReadWriteMode` (free-disk watcher is skipped
//! at the storage level).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex;

use crate::partition::{must_create_partition, must_open_partition, Partition};
use crate::raw_row::RawRow;
use crate::storage::StorageEnv;
use crate::sync_util::{add_jitter_to_duration, now_unix_milli, Shutdown, WaitCounter};
use crate::time_range::{timestamp_to_partition_name, TimeRange, MAX_UNIX_MILLI};

pub(crate) const SMALL_DIRNAME: &str = "small";
pub(crate) const BIG_DIRNAME: &str = "big";
pub(crate) const INDEXDB_DIRNAME: &str = "indexdb";
const SNAPSHOTS_DIRNAME: &str = "snapshots";

/// The interval for checking when the final deduplication process should
/// start. Go: finalDedupScheduleInterval.
const FINAL_DEDUP_SCHEDULE_INTERVAL: Duration = Duration::from_secs(3600);

/// Refcounting wrapper for a partition: the `Arc` strong count plays the
/// role of Go's manual `refCount`. When the last reference is dropped the
/// partition is closed and — if `must_drop` is set — its directories are
/// removed. Go: partitionWrapper.
pub(crate) struct PartitionWrapper {
    pt: Option<Partition>,
    /// When set, the partition data is dropped once the last reference is
    /// released.
    pub must_drop: AtomicBool,
}

impl PartitionWrapper {
    fn new(pt: Partition) -> Arc<PartitionWrapper> {
        Arc::new(PartitionWrapper {
            pt: Some(pt),
            must_drop: AtomicBool::new(false),
        })
    }

    pub fn pt(&self) -> &Partition {
        self.pt.as_ref().expect("BUG: partition already closed")
    }
}

impl Drop for PartitionWrapper {
    fn drop(&mut self) {
        let pt = self.pt.take().expect("BUG: partition closed twice");
        let must_drop = self.must_drop.load(Ordering::Acquire);
        let small = pt.inner.small_parts_path.clone();
        let big = pt.inner.big_parts_path.clone();
        let indexdb = pt.inner.index_db_parts_path.clone();
        pt.must_close();
        if must_drop {
            Partition::drop_paths(&small, &big, &indexdb);
        }
    }
}

struct PtwsState {
    ptws: Vec<Arc<PartitionWrapper>>,
}

pub(crate) struct TableInner {
    #[allow(dead_code)]
    path: PathBuf,
    small_partitions_path: PathBuf,
    big_partitions_path: PathBuf,
    index_db_path: PathBuf,

    pub(crate) env: Arc<StorageEnv>,

    state: Mutex<PtwsState>,

    shutdown: Shutdown,
    threads: Mutex<Vec<JoinHandle<()>>>,
    force_merge_wg: WaitCounter,
}

/// A single table with time series data. Go: table.
pub(crate) struct Table {
    pub(crate) inner: Arc<TableInner>,
}

/// Opens (creating if needed) a table on the given path.
/// Go: mustOpenTable.
pub(crate) fn must_open_table(path: &Path, env: Arc<StorageEnv>) -> Table {
    // Create a directory for the table if it doesn't exist yet.
    esm_common::fs::must_mkdir_if_not_exist(path);

    let small_partitions_path = path.join(SMALL_DIRNAME);
    esm_common::fs::must_mkdir_if_not_exist(&small_partitions_path);
    let big_partitions_path = path.join(BIG_DIRNAME);
    esm_common::fs::must_mkdir_if_not_exist(&big_partitions_path);
    let index_db_path = path.join(INDEXDB_DIRNAME);
    esm_common::fs::must_mkdir_if_not_exist(&index_db_path);

    // Open the partitions.
    let pts = must_open_partitions(
        &small_partitions_path,
        &big_partitions_path,
        &index_db_path,
        &env,
    );

    // Make sure all the directories inside the path are properly synced.
    esm_common::fs::must_sync_path_and_parent_dir(path);

    let inner = Arc::new(TableInner {
        path: path.to_path_buf(),
        small_partitions_path,
        big_partitions_path,
        index_db_path,
        env,
        state: Mutex::new(PtwsState {
            ptws: pts.into_iter().map(PartitionWrapper::new).collect(),
        }),
        shutdown: Shutdown::new(),
        threads: Mutex::new(Vec::new()),
        force_merge_wg: WaitCounter::new(),
    });

    inner.start_retention_watcher();
    inner.start_historical_merge_watcher();

    Table { inner }
}

impl Table {
    /// Closes the table. This must be called only when there are no threads
    /// using the table. Go: table.MustClose.
    pub fn must_close(self) {
        let inner = &self.inner;
        inner.shutdown.signal();
        loop {
            let handle = inner.threads.lock().pop();
            match handle {
                Some(h) => h.join().expect("BUG: table background worker panicked"),
                None => break,
            }
        }
        inner.force_merge_wg.wait();

        let ptws = std::mem::take(&mut inner.state.lock().ptws);
        for ptw in ptws {
            let refs = Arc::strong_count(&ptw);
            assert!(
                refs == 1,
                "BUG: unexpected refCount={refs} when closing the partition; \
                 probably there are pending searches"
            );
            drop(ptw);
        }
    }

    /// Flushes all pending raw index and data rows, so they become visible
    /// to search. For debug purposes only. Go: table.DebugFlush.
    pub fn debug_flush(&self) {
        for ptw in self.get_all_partitions() {
            ptw.pt().debug_flush();
        }
    }

    /// Adds the given rows to the table. Go: table.MustAddRows.
    pub fn must_add_rows(&self, rows: &[RawRow]) {
        if rows.is_empty() {
            return;
        }

        // Verify whether all the rows may be added to a single partition.
        let ptws = self.get_all_partitions();
        for (i, ptw) in ptws.iter().enumerate() {
            if !rows.iter().all(|r| ptw.pt().has_timestamp(r.timestamp)) {
                continue;
            }

            if i != 0 {
                // Move the partition with the matching rows to the front of
                // the partition list, so it is detected faster next time.
                let mut state = self.inner.state.lock();
                if let Some(j) = state.ptws.iter().position(|w| Arc::ptr_eq(w, ptw)) {
                    state.ptws.swap(0, j);
                }
            }

            // Fast path - add all the rows into the ptw.
            ptw.pt().add_rows(rows);
            return;
        }

        // Slower path - split rows into per-partition buckets.
        let mut pt_buckets: Vec<Vec<RawRow>> = vec![Vec::new(); ptws.len()];
        let mut missing_rows: Vec<RawRow> = Vec::new();
        for r in rows {
            match ptws
                .iter()
                .position(|ptw| ptw.pt().has_timestamp(r.timestamp))
            {
                Some(i) => pt_buckets[i].push(*r),
                None => missing_rows.push(*r),
            }
        }
        for (i, pt_rows) in pt_buckets.iter().enumerate() {
            ptws[i].pt().add_rows(pt_rows);
        }
        drop(ptws);
        if missing_rows.is_empty() {
            return;
        }

        // The slowest path - there are rows that don't fit any existing
        // partition. Create new partitions for these rows.
        let (min_timestamp, max_timestamp) = self.get_min_max_timestamps();
        let mut state = self.inner.state.lock();
        for r in &missing_rows {
            if r.timestamp < min_timestamp || r.timestamp > max_timestamp {
                // Silently skip rows outside the retention, since they
                // should be deleted anyway.
                continue;
            }

            // Make sure the partition for r hasn't been added by another
            // thread.
            if let Some(ptw) = state
                .ptws
                .iter()
                .find(|ptw| ptw.pt().has_timestamp(r.timestamp))
            {
                ptw.pt().add_rows(std::slice::from_ref(r));
                continue;
            }

            let pt = must_create_partition(
                r.timestamp,
                &self.inner.small_partitions_path,
                &self.inner.big_partitions_path,
                &self.inner.index_db_path,
                Arc::clone(&self.inner.env),
            );
            pt.add_rows(std::slice::from_ref(r));
            state.ptws.push(PartitionWrapper::new(pt));
        }
    }

    /// Go: table.getMinMaxTimestamps.
    pub fn get_min_max_timestamps(&self) -> (i64, i64) {
        let now = now_unix_milli();
        // Negative timestamps aren't supported by the storage.
        let min_timestamp = (now - self.inner.env.retention_msecs).max(0);
        let max_timestamp = MAX_UNIX_MILLI.min(now + self.inner.env.future_retention_msecs);
        (min_timestamp, max_timestamp)
    }

    /// Returns the partition that contains the given timestamp, creating it
    /// if needed. Go: table.MustGetPartition.
    pub fn must_get_partition(&self, timestamp: i64) -> Arc<PartitionWrapper> {
        let mut state = self.inner.state.lock();
        if let Some(ptw) = state
            .ptws
            .iter()
            .find(|ptw| ptw.pt().has_timestamp(timestamp))
        {
            return Arc::clone(ptw);
        }

        let pt = must_create_partition(
            timestamp,
            &self.inner.small_partitions_path,
            &self.inner.big_partitions_path,
            &self.inner.index_db_path,
            Arc::clone(&self.inner.env),
        );
        let ptw = PartitionWrapper::new(pt);
        state.ptws.push(Arc::clone(&ptw));
        ptw
    }

    /// Returns the id of the indexDB which contains the provided hour,
    /// creating the partition if needed. Go: table.MustGetIndexDBIDByHour.
    pub fn must_get_index_db_id_by_hour(&self, hour: u64) -> u64 {
        let ts = (hour * crate::time_range::MSEC_PER_HOUR as u64) as i64;
        self.must_get_partition(ts).pt().idb().id()
    }

    /// Returns a snapshot of all the partitions. Go: table.GetAllPartitions.
    pub fn get_all_partitions(&self) -> Vec<Arc<PartitionWrapper>> {
        self.inner.state.lock().ptws.clone()
    }

    /// Creates a snapshot of all partitions under `dst_data_dir`, producing
    /// `dst_data_dir/{small,big,indexdb}/<partition>/...` hard-link trees.
    /// Go: table.MustCreateSnapshot (layout adapted: no symlink indirection).
    ///
    /// Called by `Storage::must_create_snapshot`.
    pub(crate) fn must_create_snapshot_at(&self, dst_data_dir: &Path) {
        let dst_small = dst_data_dir.join(SMALL_DIRNAME);
        let dst_big = dst_data_dir.join(BIG_DIRNAME);
        let dst_indexdb = dst_data_dir.join(INDEXDB_DIRNAME);
        esm_common::fs::must_mkdir_fail_if_exist(dst_data_dir);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_small);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_big);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_indexdb);

        // Holding the Arc<PartitionWrapper>s keeps every partition alive
        // (retention/drop is deferred until the refs are released).
        let ptws = self.get_all_partitions();
        for ptw in &ptws {
            let pt = ptw.pt();
            let name = &pt.inner.name;
            pt.must_create_snapshot_at(
                &dst_small.join(name),
                &dst_big.join(name),
                &dst_indexdb.join(name),
            );
        }

        esm_common::fs::must_sync_path_and_parent_dir(&dst_small);
        esm_common::fs::must_sync_path_and_parent_dir(&dst_big);
        esm_common::fs::must_sync_path_and_parent_dir(&dst_indexdb);
    }

    /// Returns a snapshot of the partitions whose time ranges overlap with
    /// `tr`. Go: table.GetPartitions.
    pub fn get_partitions(&self, tr: TimeRange) -> Vec<Arc<PartitionWrapper>> {
        self.inner
            .state
            .lock()
            .ptws
            .iter()
            .filter(|ptw| ptw.pt().time_range().overlaps_with(tr))
            .cloned()
            .collect()
    }

    /// Force-merges partitions with names starting from the given prefix.
    /// Partitions are merged sequentially in order to reduce the load on the
    /// system. Go: table.ForceMergePartitions.
    pub fn force_merge_partitions(&self, partition_name_prefix: &str) -> Result<(), String> {
        let ptws = self.get_all_partitions();
        self.inner.force_merge_wg.add();
        let res = (|| {
            for ptw in &ptws {
                if !ptw.pt().name().starts_with(partition_name_prefix) {
                    continue;
                }
                log::info!("starting forced merge for partition {:?}", ptw.pt().name());
                ptw.pt()
                    .force_merge_all_parts(Some(self.inner.shutdown.flag()))
                    .map_err(|err| {
                        format!(
                            "cannot complete forced merge for partition {:?}: {err}",
                            ptw.pt().name()
                        )
                    })?;
            }
            Ok(())
        })();
        self.inner.force_merge_wg.done();
        res
    }

    /// Drops the partitions that are fully outside the retention. Called by
    /// the retention watcher; exposed for deterministic tests.
    pub fn enforce_retention(&self) {
        self.inner.enforce_retention();
    }
}

impl TableInner {
    fn start_retention_watcher(self: &Arc<Self>) {
        let inner = Arc::clone(self);
        let handle = std::thread::spawn(move || inner.retention_watcher());
        self.threads.lock().push(handle);
    }

    fn start_historical_merge_watcher(self: &Arc<Self>) {
        let inner = Arc::clone(self);
        let handle = std::thread::spawn(move || inner.historical_merge_watcher());
        self.threads.lock().push(handle);
    }

    /// Go: table.retentionWatcher.
    fn retention_watcher(self: &Arc<Self>) {
        let d = add_jitter_to_duration(Duration::from_secs(60));
        loop {
            if self.shutdown.wait_timeout(d) {
                return;
            }
            self.enforce_retention();
        }
    }

    fn enforce_retention(&self) {
        let now_msecs = now_unix_milli();
        let min_timestamp = now_msecs - self.env.retention_msecs;
        let max_timestamp = now_msecs + self.env.future_retention_msecs;
        let mut ptws_drop = Vec::new();
        {
            let mut state = self.state.lock();
            let ptws = std::mem::take(&mut state.ptws);
            for ptw in ptws {
                let tr = ptw.pt().time_range();
                if tr.max_timestamp < min_timestamp || tr.min_timestamp > max_timestamp {
                    ptws_drop.push(ptw);
                } else {
                    state.ptws.push(ptw);
                }
            }
        }

        // Drop the partitions outside the retention. The partitions are
        // closed and dropped once all the pending searches release their
        // references.
        for ptw in ptws_drop {
            ptw.must_drop.store(true, Ordering::Release);
            drop(ptw);
        }
    }

    /// Schedules the final dedup pass for historical partitions when the
    /// global dedup interval grows. Go: table.historicalMergeWatcher.
    fn historical_merge_watcher(self: &Arc<Self>) {
        if !crate::dedup::is_dedup_enabled() {
            // Deduplication is disabled: nothing to watch.
            return;
        }

        let d = add_jitter_to_duration(FINAL_DEDUP_SCHEDULE_INTERVAL);
        loop {
            if self.shutdown.wait_timeout(d) {
                return;
            }

            let ptws = self.state.lock().ptws.clone();
            let current_partition_name = timestamp_to_partition_name(now_unix_milli());

            let mut ptws_to_merge = Vec::new();
            for ptw in &ptws {
                if ptw.pt().name() == current_partition_name {
                    // Do not run the force merge for the current month: its
                    // samples are continuously deduplicated by the regular
                    // background merges.
                    continue;
                }
                if ptw.pt().is_final_dedup_needed() {
                    ptw.pt().set_dedup_scheduled(true);
                    ptws_to_merge.push(Arc::clone(ptw));
                }
            }
            for ptw in ptws_to_merge {
                log::info!(
                    "start removing duplicate samples for partition {:?}",
                    ptw.pt().name()
                );
                if let Err(err) = ptw.pt().force_merge_all_parts(Some(self.shutdown.flag())) {
                    log::error!(
                        "cannot remove duplicate samples for partition {:?}: {err}",
                        ptw.pt().name()
                    );
                }
                ptw.pt().set_dedup_scheduled(false);
            }
        }
    }
}

/// Go: mustOpenPartitions.
fn must_open_partitions(
    small_partitions_path: &Path,
    big_partitions_path: &Path,
    index_db_path: &Path,
    env: &Arc<StorageEnv>,
) -> Vec<Partition> {
    // Certain partition directories in either `big` or `small` dir may be
    // missing after restoring from backup, so populate the partition names
    // from all the dirs.
    let mut pt_names: Vec<String> = Vec::new();
    must_populate_partition_names(small_partitions_path, &mut pt_names);
    must_populate_partition_names(big_partitions_path, &mut pt_names);
    must_populate_partition_names(index_db_path, &mut pt_names);
    pt_names.sort();
    pt_names.dedup();

    pt_names
        .iter()
        .map(|pt_name| {
            must_open_partition(
                &small_partitions_path.join(pt_name),
                &big_partitions_path.join(pt_name),
                &index_db_path.join(pt_name),
                Arc::clone(env),
            )
        })
        .collect()
}

/// Go: mustPopulatePartitionNames.
fn must_populate_partition_names(partitions_path: &Path, pt_names: &mut Vec<String>) {
    if !esm_common::fs::is_path_exist(partitions_path) {
        return;
    }
    for de in esm_common::fs::must_read_dir(partitions_path) {
        if !esm_common::fs::is_dir_or_symlink(&de) {
            // Skip non-directories.
            continue;
        }
        let pt_name = de.file_name().to_string_lossy().into_owned();
        if pt_name == SNAPSHOTS_DIRNAME {
            // Skip the directory with snapshots.
            continue;
        }
        let pt_dir_path = partitions_path.join(&pt_name);
        if esm_common::fs::is_partially_removed_dir(&pt_dir_path) {
            // Finish the removal of partially deleted partition directories
            // left after unclean shutdown in the middle of a removal.
            esm_common::fs::must_remove_dir(&pt_dir_path);
            continue;
        }
        pt_names.push(pt_name);
    }
}
