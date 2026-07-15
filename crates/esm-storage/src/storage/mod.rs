//! Stage 4: port of the upstream VictoriaMetrics v1.146.0 lib/storage/storage.go — the
//! `Storage` struct: open/close lifecycle, the `add_rows` write path with
//! TSID resolution and per-day index registration, the hour/next-day
//! metricID caches and the search entry points.
//!
//! PORT-SKIP (spec §8): legacy indexDBs, snapshots, cardinality limiters,
//! metric-name stats tracker, metadata storage, the free-disk-space watcher
//! (`is_read_only` stays false) and on-disk persistence of the working-set
//! caches (tsidCache & friends are rebuilt from the index after restart).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use esm_common::uint64set::Set;
use esm_common::{decimal, fasttime};
use parking_lot::{Mutex, RwLock};

mod per_day;

pub(crate) use per_day::{HourMetricIds, NextDayMetricIds};

use crate::index::caches::WorkingSetCache;
use crate::index::{generate_tsid, IndexDbContext, SearchError, TagFilters, NO_DEADLINE};
use crate::metric_name::MetricName;
use crate::raw_row::RawRow;
use crate::sync_util::{add_jitter_to_duration, now_unix_milli, Shutdown};
use crate::table::{must_open_table, Table};
use crate::time_range::{
    is_first_hour_of_day, TimeRange, GLOBAL_INDEX_TIME_RANGE, MSEC_PER_DAY, MSEC_PER_HOUR,
};
use crate::tsid::{merge_sorted_tsids, Tsid};

const RETENTION_2_DAYS_MSECS: i64 = 2 * 24 * 3600 * 1000;
const RETENTION_31_DAYS_MSECS: i64 = 31 * 24 * 3600 * 1000;
/// The maximum retention (100 years). Go: retentionMax.
pub const RETENTION_MAX_MSECS: i64 = 100 * 12 * RETENTION_31_DAYS_MSECS;

/// AddRows chunk size. Go: maxMetricRowsPerBlock.
const MAX_METRIC_ROWS_PER_BLOCK: usize = 8000;

/// Once the time range is bigger than 40 days, searching using the per-day
/// index becomes slower than using the global index.
/// Go: maxDaysForPerDaySearch.
const MAX_DAYS_FOR_PER_DAY_SEARCH: u64 = 40;

/// The subdirectory holding snapshot trees. Go: snapshotsDirname.
const SNAPSHOTS_DIRNAME: &str = "snapshots";

/// MetricRow is a metric to insert into storage. Go: MetricRow.
#[derive(Debug, Clone, Default)]
pub struct MetricRow {
    /// The raw metric name (see [`crate::metric_name::marshal_metric_name_raw`]
    /// and [`MetricName::marshal_raw`]).
    pub metric_name_raw: Vec<u8>,
    /// Unix timestamp in milliseconds.
    pub timestamp: i64,
    /// The value.
    pub value: f64,
}

/// A [`MetricRow`] whose raw metric name borrows a caller-owned buffer.
/// The zero-copy seam for the ingestion path: `Storage.add` copies the name
/// bytes into its own caches/index items when needed, so batches can borrow
/// a parse arena directly instead of allocating one `Vec<u8>` per row.
#[derive(Debug, Clone, Copy)]
pub struct MetricRowRef<'a> {
    /// The raw metric name.
    pub metric_name_raw: &'a [u8],
    /// Unix timestamp in milliseconds.
    pub timestamp: i64,
    /// The value.
    pub value: f64,
}

/// Optional arguments for [`Storage::must_open`]. Go: OpenOptions.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Retention in milliseconds. `<= 0` or too big means the maximum
    /// retention (100 years).
    pub retention_msecs: i64,
    /// Future retention in milliseconds; defaults to 2 days.
    pub future_retention_msecs: i64,
    /// Whether queries outside the retention must be denied.
    pub deny_queries_outside_retention: bool,
    /// Disables the per-day index (Go: -disablePerDayIndex). PORT-NOTE: the
    /// green-field port always uses the per-day index by default.
    pub disable_per_day_index: bool,
    /// The start of the next-month indexdb prefill window, in seconds before
    /// the month rollover. Defaults to 1 hour.
    pub idb_prefill_start_seconds: i64,
}

/// Storage-level state shared by the table and the partitions
/// (in Go this role is played by `*Storage` back-references).
pub(crate) struct StorageEnv {
    pub retention_msecs: i64,
    pub future_retention_msecs: i64,
    /// Set when the storage is in read-only mode. PORT-NOTE: the free-disk
    /// watcher is skipped, so this stays false; the plumbing is kept for the
    /// mergers and the per-partition indexDBs.
    pub is_read_only: Arc<AtomicBool>,
    /// The shared context of all per-partition indexDBs (storage-level
    /// caches, self-healing state).
    pub idb_ctx: Arc<IndexDbContext>,
}

pub(crate) struct StorageInner {
    pub(crate) env: Arc<StorageEnv>,
    path: PathBuf,
    deny_queries_outside_retention: bool,
    idb_prefill_start_seconds: i64,

    /// Lock file for exclusive access to the storage on the given path.
    /// Released (unlocked) when the inner state is dropped.
    _flock_f: std::fs::File,

    /// Serializes snapshot creation. Go: Storage.snapshotLock.
    snapshot_lock: Mutex<()>,

    pub(crate) tb: Table,

    /// MetricNameRaw -> TSID cache (Go: tsidCache; the legacy 8-byte
    /// generation padding of the cached value is dropped in the port, and
    /// the TSID is stored unmarshaled since the cache is process-local).
    tsid_cache: WorkingSetCache<Tsid>,

    /// Striped locks serializing the registration of new series per metric
    /// name (see the slow path in `add`). Freshly created index entries are
    /// not searchable until the mergeset raw-items buffer flushes, so
    /// without this concurrent inserters of the same new name race between
    /// the tsidCache miss and the tsidCache put and register duplicate
    /// TSIDs (observed: 2214 shadow series on a 1000-series concurrent
    /// load), which bloat the index and slow every query whose time range
    /// covers their data. Deviation from Go, which tolerates the race.
    new_series_locks: Vec<Mutex<()>>,

    curr_hour_metric_ids: RwLock<Arc<HourMetricIds>>,
    prev_hour_metric_ids: RwLock<Arc<HourMetricIds>>,
    next_day_metric_ids: RwLock<Arc<NextDayMetricIds>>,

    pending_hour_entries: Mutex<Set>,
    pending_next_day_metric_ids: Mutex<Set>,

    shutdown: Shutdown,
    threads: Mutex<Vec<JoinHandle<()>>>,

    // Ingestion counters.
    pub rows_received_total: AtomicU64,
    pub rows_added_total: AtomicU64,
    pub too_small_timestamp_rows: AtomicU64,
    pub too_big_timestamp_rows: AtomicU64,
    pub invalid_raw_metric_names: AtomicU64,
    pub timeseries_repopulated: AtomicU64,
    pub timeseries_pre_created: AtomicU64,
    pub new_timeseries_created: AtomicU64,
    pub slow_row_inserts: AtomicU64,
    pub slow_per_day_index_inserts: AtomicU64,
}

/// TSDB storage. Go: Storage.
pub struct Storage {
    inner: Arc<StorageInner>,
}

/// Go: getTSIDCacheSize (0.37 × allowed memory).
fn get_tsid_cache_size() -> u64 {
    (esm_common::memory::allowed() as f64 * 0.37) as u64
}

/// Stripe count for [`StorageInner::new_series_locks`]; power of two.
const NEW_SERIES_LOCK_STRIPES: usize = 64;

/// FNV-1a over the raw metric name, for the new-series lock striping.
fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

impl Storage {
    /// Opens the storage at the given path with the given options.
    /// Go: MustOpenStorage.
    pub fn must_open(path: impl AsRef<Path>, opts: OpenOptions) -> Storage {
        let path = path.as_ref().to_path_buf();
        let mut retention_msecs = opts.retention_msecs;
        if retention_msecs <= 0 || retention_msecs > RETENTION_MAX_MSECS {
            retention_msecs = RETENTION_MAX_MSECS;
        }
        let future_retention_msecs = opts.future_retention_msecs.max(RETENTION_2_DAYS_MSECS);
        let idb_prefill_start_seconds = if opts.idb_prefill_start_seconds <= 0 {
            3600
        } else {
            opts.idb_prefill_start_seconds
        };

        esm_common::fs::must_mkdir_if_not_exist(&path);

        // Refuse to open a half-restored dataset. The esrestore tool creates
        // this file at restore start and removes it only on success.
        let restore_lock = path.join("restore-in-progress");
        if esm_common::fs::is_path_exist(&restore_lock) {
            panic!(
                "FATAL: incomplete restore run detected; run esrestore again \
                 or remove the lock file {restore_lock:?}"
            );
        }

        // Protect from concurrent opens.
        let flock_f = esm_common::fs::must_create_flock_file(&path);

        // The shared per-partition indexDB context. PORT-NOTE:
        // minTimestampForCompositeIndex stays 0 (green-field database), so
        // composite index searches are always enabled.
        let mut idb_ctx = IndexDbContext::new();
        idb_ctx.disable_per_day_index = opts.disable_per_day_index;
        idb_ctx.min_timestamp_for_composite_index = 0;

        let env = Arc::new(StorageEnv {
            retention_msecs,
            future_retention_msecs,
            is_read_only: Arc::new(AtomicBool::new(false)),
            idb_ctx: Arc::new(idb_ctx),
        });

        // Load the data.
        let table_path = path.join("data");
        let tb = must_open_table(&table_path, Arc::clone(&env));

        // Initialize the hour and next-day metricID caches after the table
        // is opened, since they require the partition index. PORT-NOTE: the
        // caches are not persisted across restarts (Go stores them under
        // `<path>/cache`) — after a restart the per-day registration falls
        // back to the (correct, slower) dateMetricIDCache/index probes.
        let hour = fasttime::unix_hour();
        let hm_curr = Arc::new(HourMetricIds {
            m: Set::default(),
            hour,
            idb_id: tb.must_get_index_db_id_by_hour(hour),
        });
        let hm_prev = Arc::new(HourMetricIds {
            m: Set::default(),
            hour: hour.saturating_sub(1),
            idb_id: tb.must_get_index_db_id_by_hour(hour.saturating_sub(1)),
        });
        let timestamp = fasttime::unix_timestamp();
        let mut date = timestamp / (24 * 3600);
        if is_first_hour_of_day(timestamp) {
            date = date.saturating_sub(1);
        }
        let next_day_idb_id = tb
            .must_get_partition((date + 1) as i64 * MSEC_PER_DAY)
            .pt()
            .idb()
            .id();
        let next_day = Arc::new(NextDayMetricIds {
            idb_id: next_day_idb_id,
            date,
            metric_ids: Set::default(),
        });

        let inner = Arc::new(StorageInner {
            env,
            path,
            deny_queries_outside_retention: opts.deny_queries_outside_retention,
            idb_prefill_start_seconds,
            _flock_f: flock_f,
            snapshot_lock: Mutex::new(()),
            tb,
            tsid_cache: WorkingSetCache::new(get_tsid_cache_size()),
            new_series_locks: (0..NEW_SERIES_LOCK_STRIPES)
                .map(|_| Mutex::new(()))
                .collect(),
            curr_hour_metric_ids: RwLock::new(hm_curr),
            prev_hour_metric_ids: RwLock::new(hm_prev),
            next_day_metric_ids: RwLock::new(next_day),
            pending_hour_entries: Mutex::new(Set::default()),
            pending_next_day_metric_ids: Mutex::new(Set::default()),
            shutdown: Shutdown::new(),
            threads: Mutex::new(Vec::new()),
            rows_received_total: AtomicU64::new(0),
            rows_added_total: AtomicU64::new(0),
            too_small_timestamp_rows: AtomicU64::new(0),
            too_big_timestamp_rows: AtomicU64::new(0),
            invalid_raw_metric_names: AtomicU64::new(0),
            timeseries_repopulated: AtomicU64::new(0),
            timeseries_pre_created: AtomicU64::new(0),
            new_timeseries_created: AtomicU64::new(0),
            slow_row_inserts: AtomicU64::new(0),
            slow_per_day_index_inserts: AtomicU64::new(0),
        });

        inner.start_curr_hour_metric_ids_updater();
        inner.start_next_day_metric_ids_updater();
        // PORT-SKIP: freeDiskSpaceWatcher (read-only mode on low disk).

        Storage { inner }
    }

    /// Closes the storage, joining all the background threads.
    /// Go: Storage.MustClose.
    pub fn must_close(self) {
        let inner = &self.inner;
        inner.shutdown.signal();
        loop {
            let handle = inner.threads.lock().pop();
            match handle {
                Some(h) => h.join().expect("BUG: storage background worker panicked"),
                None => break,
            }
        }

        let inner = Arc::try_unwrap(self.inner)
            .ok()
            .expect("BUG: pending references to the storage on close");
        inner.tb.must_close();
        // Wait for the scheduled part-directory removals, so the caller may
        // safely remove or re-open the storage directory.
        esm_common::fs::remove_dir_async_drain();
        // The flock file is released when `inner` is dropped here.
    }

    /// The configured retention in milliseconds.
    pub fn retention_msecs(&self) -> i64 {
        self.inner.env.retention_msecs
    }

    /// Flushes all the pending data, so it becomes visible to search.
    /// For tests and benchmarks only. Go: Storage.DebugFlush.
    pub fn force_flush(&self) {
        self.inner.tb.debug_flush();
    }

    /// Creates a new snapshot under `<path>/snapshots/<name>` and returns
    /// the name. Instant (hard links). Go: Storage.MustCreateSnapshot.
    pub fn must_create_snapshot(&self) -> String {
        let _guard = self.inner.snapshot_lock.lock();
        let started = std::time::Instant::now();
        let name = crate::snapshot::new_name();
        log::info!("creating storage snapshot {name:?}...");

        let snapshots_dir = self.inner.path.join(SNAPSHOTS_DIRNAME);
        esm_common::fs::must_mkdir_if_not_exist(&snapshots_dir);
        let dst_dir = snapshots_dir.join(&name);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_dir);

        self.inner.tb.must_create_snapshot_at(&dst_dir.join("data"));

        esm_common::fs::must_sync_path_and_parent_dir(&dst_dir);
        log::info!(
            "created storage snapshot {name:?} in {:.3} seconds",
            started.elapsed().as_secs_f64()
        );
        name
    }

    /// Returns the sorted list of existing snapshot names.
    /// Go: Storage.MustListSnapshots.
    pub fn must_list_snapshots(&self) -> Vec<String> {
        let snapshots_dir = self.inner.path.join(SNAPSHOTS_DIRNAME);
        if !esm_common::fs::is_path_exist(&snapshots_dir) {
            return Vec::new();
        }
        let mut names: Vec<String> = esm_common::fs::must_read_dir(&snapshots_dir)
            .iter()
            .filter_map(|e| e.file_name().to_str().map(str::to_owned))
            .filter(|name| crate::snapshot::validate_name(name).is_ok())
            .collect();
        names.sort();
        names
    }

    /// Deletes the snapshot with the given name. Go: Storage.DeleteSnapshot.
    pub fn delete_snapshot(&self, name: &str) -> Result<(), String> {
        crate::snapshot::validate_name(name)
            .map_err(|e| format!("invalid snapshotName {name:?}: {e}"))?;
        // Only delete names we actually listed (defense in depth against
        // path tricks; validate_name already excludes separators).
        if !self.must_list_snapshots().iter().any(|s| s == name) {
            return Err(format!("cannot find snapshot {name:?}"));
        }
        let started = std::time::Instant::now();
        log::info!("deleting snapshot {name:?}...");
        let snapshot_path = self.inner.path.join(SNAPSHOTS_DIRNAME).join(name);
        esm_common::fs::must_remove_dir(&snapshot_path);
        log::info!(
            "deleted snapshot {name:?} in {:.3} seconds",
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// Force-merges partitions with names starting from the given prefix.
    /// Go: Storage.ForceMergePartitions.
    pub fn force_merge_partitions(&self, partition_name_prefix: &str) -> Result<(), String> {
        self.inner.tb.force_merge_partitions(partition_name_prefix)
    }

    /// Drops the partitions that are fully outside the retention. This is
    /// the retention-watcher body, exposed for deterministic tests (the
    /// watcher itself runs on a ~1min jittered ticker).
    pub fn debug_enforce_retention(&self) {
        self.inner.tb.enforce_retention();
    }

    /// Marks as deleted all the series matching the given tag filters in all
    /// the partition indexDBs. Returns the number of deleted series.
    /// Go: Storage.DeleteSeries (minimal port).
    pub fn delete_series(
        &self,
        tfss: &[TagFilters],
        max_metrics: usize,
    ) -> Result<usize, SearchError> {
        let mut deleted = Set::default();
        for ptw in self.inner.tb.get_all_partitions() {
            let dmis = ptw.pt().idb().delete_series(tfss, max_metrics)?;
            deleted.union(&dmis);
        }
        if !deleted.is_empty() {
            // The tsidCache may contain deleted TSIDs.
            self.inner.tsid_cache.reset();
        }
        Ok(deleted.len())
    }

    // --- write path ---

    /// Adds the given rows to the storage. Go: Storage.AddRows.
    pub fn add_rows(&self, mrs: &[MetricRow], precision_bits: u8) {
        let refs: Vec<MetricRowRef<'_>> = mrs
            .iter()
            .map(|mr| MetricRowRef {
                metric_name_raw: &mr.metric_name_raw,
                timestamp: mr.timestamp,
                value: mr.value,
            })
            .collect();
        self.add_rows_ref(&refs, precision_bits);
    }

    /// Adds the given borrowed rows to the storage (the zero-copy ingestion
    /// entry point). Go: Storage.AddRows.
    pub fn add_rows_ref(&self, mrs: &[MetricRowRef<'_>], precision_bits: u8) {
        if mrs.is_empty() {
            return;
        }

        // Add rows to the storage in blocks with limited size in order to
        // reduce memory usage.
        for mrs_block in mrs.chunks(MAX_METRIC_ROWS_PER_BLOCK) {
            let rows_added = self.inner.add(mrs_block, precision_bits);
            self.inner
                .rows_added_total
                .fetch_add(rows_added as u64, Ordering::Relaxed);
            self.inner
                .rows_received_total
                .fetch_add(mrs_block.len() as u64, Ordering::Relaxed);
        }
    }

    // --- search entry points ---

    /// Searches the TSIDs corresponding to the given tag filters within the
    /// given time range. The returned TSIDs are sorted.
    /// Go: Storage.SearchTSIDs.
    pub fn search_tsids(
        &self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
        deadline: u64,
    ) -> Result<Vec<Tsid>, SearchError> {
        self.check_time_range(tr)?;
        let tsidss = self.inner.search_in_partition_idbs(tr, |idb, search_tr| {
            idb.search_tsids(tfss, search_tr, max_metrics, deadline)
        })?;
        Ok(merge_sorted_tsids(&tsidss))
    }

    /// Returns the (marshaled canonical) metric names matching the given tag
    /// filters within the given time range. Go: Storage.SearchMetricNames.
    pub fn search_metric_names(
        &self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
        deadline: u64,
    ) -> Result<Vec<Vec<u8>>, SearchError> {
        self.check_time_range(tr)?;
        let names_per_idb = self.inner.search_in_partition_idbs(tr, |idb, search_tr| {
            idb.search_metric_names(tfss, search_tr, max_metrics, deadline)
        })?;

        // Merge with deduplication.
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut all = Vec::new();
        for names in names_per_idb {
            for name in names {
                if seen.insert(name.clone()) {
                    all.push(name);
                }
            }
        }
        Ok(all)
    }

    /// Returns an error if the time range is outside the allowed retention
    /// window when `deny_queries_outside_retention` is set.
    /// Go: Storage.checkTimeRange.
    fn check_time_range(&self, tr: TimeRange) -> Result<(), SearchError> {
        if !self.inner.deny_queries_outside_retention {
            return Ok(());
        }
        let (min_timestamp, max_timestamp) = self.inner.tb.get_min_max_timestamps();
        if min_timestamp <= tr.min_timestamp && tr.max_timestamp <= max_timestamp {
            return Ok(());
        }
        Err(SearchError::Other(format!(
            "the given time range {tr} is outside the allowed retention according to \
             denyQueriesOutsideRetention"
        )))
    }

    pub(crate) fn inner(&self) -> &Arc<StorageInner> {
        &self.inner
    }
}

impl StorageInner {
    // --- caches ---

    fn get_tsid_by_metric_name_from_cache(&self, dst: &mut Tsid, metric_name_raw: &[u8]) -> bool {
        match self.tsid_cache.get(metric_name_raw) {
            Some(tsid) => {
                *dst = tsid;
                true
            }
            None => false,
        }
    }

    fn put_tsid_by_metric_name_to_cache(&self, tsid: &Tsid, metric_name_raw: &[u8]) {
        self.tsid_cache.set(metric_name_raw, *tsid);
    }

    /// Runs `search` on every partition indexDB overlapping `tr` (with the
    /// per-idb adjusted time range), in parallel when there is more than one
    /// partition. Go: searchAndMerge.
    fn search_in_partition_idbs<T: Send>(
        &self,
        tr: TimeRange,
        search: impl Fn(&crate::index::IndexDb, TimeRange) -> Result<T, SearchError> + Sync,
    ) -> Result<Vec<T>, SearchError> {
        let ptws = self.tb.get_partitions(tr);
        if ptws.is_empty() {
            return Ok(Vec::new());
        }
        if ptws.len() == 1 {
            // It is faster to process one indexDB without spawning threads.
            let idb = ptws[0].pt().idb();
            let search_tr = self.adjust_time_range(tr, idb.time_range());
            return Ok(vec![search(idb, search_tr)?]);
        }

        let mut results: Vec<Option<Result<T, SearchError>>> =
            (0..ptws.len()).map(|_| None).collect();
        std::thread::scope(|scope| {
            for (ptw, slot) in ptws.iter().zip(results.iter_mut()) {
                let search = &search;
                scope.spawn(move || {
                    let idb = ptw.pt().idb();
                    let search_tr = self.adjust_time_range(tr, idb.time_range());
                    *slot = Some(search(idb, search_tr));
                });
            }
        });

        results
            .into_iter()
            .map(|r| r.expect("BUG: missing search result"))
            .collect()
    }

    /// Decides whether to use the time range as is or the global index time
    /// range. Go: Storage.adjustTimeRange.
    fn adjust_time_range(&self, search_tr: TimeRange, idb_tr: TimeRange) -> TimeRange {
        // If the per-day index is disabled, unconditionally search the
        // global index.
        if self.env.idb_ctx.disable_per_day_index {
            return GLOBAL_INDEX_TIME_RANGE;
        }

        let mut tr = idb_tr;
        if idb_tr.contains(search_tr.min_timestamp) {
            tr.min_timestamp = search_tr.min_timestamp;
        }
        if idb_tr.contains(search_tr.max_timestamp) {
            tr.max_timestamp = search_tr.max_timestamp;
        }

        let (min_date, max_date) = tr.date_range();
        if max_date - min_date > MAX_DAYS_FOR_PER_DAY_SEARCH {
            return GLOBAL_INDEX_TIME_RANGE;
        }

        // If the final time range is still the same as the idb time range,
        // search the global index, since the entire indexDB needs to be
        // searched anyway.
        if tr == idb_tr {
            return GLOBAL_INDEX_TIME_RANGE;
        }

        tr
    }

    // --- add() (Go: Storage.add) ---

    fn add<'a>(self: &Arc<Self>, mrs: &[MetricRowRef<'a>], precision_bits: u8) -> usize {
        let hm_prev = Arc::clone(&self.prev_hour_metric_ids.read());
        let hm_curr = Arc::clone(&self.curr_hour_metric_ids.read());
        let mut pending_hour_entries: Vec<u64> = Vec::new();

        let mut mn = MetricName::default();
        // These are used for speeding up bulk imports of multiple adjacent
        // rows for the same metricName.
        let mut prev_tsid = Tsid::default();
        let mut prev_metric_name_raw: &[u8] = b"";
        let mut metric_name_buf: Vec<u8> = Vec::new();

        let mut slow_inserts_count = 0u64;
        let mut new_series_count = 0u64;
        let mut series_repopulated = 0u64;

        let (min_timestamp, max_timestamp) = self.tb.get_min_max_timestamps();

        // Log only the first error, since it has no sense in logging all
        // errors.
        let mut first_warn: Option<String> = None;

        let mut rows: Vec<RawRow> = Vec::with_capacity(mrs.len());
        let mut row_names: Vec<&'a [u8]> = Vec::with_capacity(mrs.len());

        let mut i = 0;
        'outer: while i < mrs.len() {
            // Find the next valid row in order to pick its partition.
            let first_ts = loop {
                if i >= mrs.len() {
                    break 'outer;
                }
                match self.validate_row(mrs[i], min_timestamp, max_timestamp, &mut first_warn) {
                    true => break mrs[i].timestamp,
                    false => i += 1,
                }
            };

            // Process the run of rows landing in this partition.
            let ptw = self.tb.must_get_partition(first_ts);
            let idb = ptw.pt().idb();
            let mut is = idb.get_index_search(NO_DEADLINE);
            let deleted_metric_ids = idb.get_deleted_metric_ids();

            // Batch-local memo of metricIDs known to exist in this indexDB.
            // "The metricID exists in the idb global index" is an immutable
            // fact, so positive `is.has_metric_id` answers can be reused for
            // the rest of the batch without touching the shared
            // metricIDCache again (its lock is contended when several
            // ingestion threads run; Go reads it lock-free instead).
            let mut verified_metric_ids = Set::default();
            let idb_has_metric_id =
                |is: &mut crate::index::IndexSearch<'_>, verified: &mut Set, metric_id: u64| {
                    if verified.has(metric_id) {
                        return true;
                    }
                    let ok = is.has_metric_id(metric_id);
                    if ok {
                        verified.add(metric_id);
                    }
                    ok
                };

            while i < mrs.len() {
                let mr = mrs[i];
                if !self.validate_row(mr, min_timestamp, max_timestamp, &mut first_warn) {
                    i += 1;
                    continue;
                }
                if !ptw.pt().has_timestamp(mr.timestamp) {
                    // Partition switch: restart the outer loop.
                    continue 'outer;
                }
                i += 1;

                let date = mr.timestamp as u64 / MSEC_PER_DAY as u64;
                let hour = mr.timestamp as u64 / MSEC_PER_HOUR as u64;
                let mut add_to_pending_hour = |metric_id: u64| {
                    if hour == hm_curr.hour && !hm_curr.m.has(metric_id) {
                        pending_hour_entries.push(metric_id);
                    }
                };

                let mut r = RawRow {
                    tsid: Tsid::default(),
                    timestamp: mr.timestamp,
                    value: mr.value,
                    precision_bits,
                };

                // Search for the TSID of the given metric_name_raw.
                if !prev_metric_name_raw.is_empty() && mr.metric_name_raw == prev_metric_name_raw {
                    // Fast path - the current mr contains the same metric
                    // name as the previous mr, so it contains the same TSID.
                    if !idb_has_metric_id(&mut is, &mut verified_metric_ids, prev_tsid.metric_id) {
                        // The found TSID is not present in the current
                        // indexDB. Create it there.
                        if !unmarshal_raw_sorted(&mut mn, mr.metric_name_raw, self, &mut first_warn)
                        {
                            continue;
                        }
                        idb.create_global_indexes(&prev_tsid, &mn);
                        verified_metric_ids.add(prev_tsid.metric_id);
                    }
                    r.tsid = prev_tsid;
                    rows.push(r);
                    row_names.push(mr.metric_name_raw);
                    continue;
                }

                // The tsidCache may contain TSIDs that were deleted from
                // some indexDBs but are still in use in other indexDBs, so
                // also check whether the TSID isn't deleted in the current
                // indexDB.
                let mut tsid = Tsid::default();
                if self.get_tsid_by_metric_name_from_cache(&mut tsid, mr.metric_name_raw)
                    && !deleted_metric_ids.has(tsid.metric_id)
                {
                    // Fast path - the TSID was found in the cache and isn't
                    // deleted.
                    if !idb_has_metric_id(&mut is, &mut verified_metric_ids, tsid.metric_id) {
                        // The found TSID is from another partition indexDB.
                        // Only create an entry in this partition's global
                        // index; the per-day entry is created in
                        // update_per_date_data().
                        if !unmarshal_raw_sorted(&mut mn, mr.metric_name_raw, self, &mut first_warn)
                        {
                            continue;
                        }
                        idb.create_global_indexes(&tsid, &mn);
                        verified_metric_ids.add(tsid.metric_id);
                        series_repopulated += 1;
                        slow_inserts_count += 1;
                    }
                    add_to_pending_hour(tsid.metric_id);
                    r.tsid = tsid;
                    prev_tsid = tsid;
                    prev_metric_name_raw = mr.metric_name_raw;
                    rows.push(r);
                    row_names.push(mr.metric_name_raw);
                    continue;
                }

                // Slow path - the TSID for the given metric_name_raw is
                // missing in the cache.
                slow_inserts_count += 1;

                // Construct the canonical metric name - it is used below.
                if !unmarshal_raw_sorted(&mut mn, mr.metric_name_raw, self, &mut first_warn) {
                    continue;
                }
                metric_name_buf.clear();
                mn.marshal(&mut metric_name_buf);

                // Serialize registration per metric name (striped by name
                // hash): freshly created index entries only become
                // searchable after the raw-items buffer flushes, so the
                // index lookup below cannot close the race between
                // concurrent inserters of the same new name — only the
                // tsidCache put does, and it must not race the miss above.
                // See the `new_series_locks` field docs.
                let _creation_guard = {
                    let h = fnv1a(mr.metric_name_raw);
                    self.new_series_locks[(h as usize) & (NEW_SERIES_LOCK_STRIPES - 1)].lock()
                };
                if self.get_tsid_by_metric_name_from_cache(&mut tsid, mr.metric_name_raw)
                    && !deleted_metric_ids.has(tsid.metric_id)
                {
                    // A concurrent inserter has just registered this name.
                    if !idb_has_metric_id(&mut is, &mut verified_metric_ids, tsid.metric_id) {
                        idb.create_global_indexes(&tsid, &mn);
                        verified_metric_ids.add(tsid.metric_id);
                        series_repopulated += 1;
                    }
                } else if is.get_tsid_by_metric_name(&mut tsid, &metric_name_buf, date) {
                    // Slower path - the TSID has been found in the indexdb.
                    self.put_tsid_by_metric_name_to_cache(&tsid, mr.metric_name_raw);
                } else {
                    // Slowest path - the TSID isn't found in the indexdb.
                    // Create it.
                    generate_tsid(&mut tsid, &mn);
                    idb.create_global_indexes(&tsid, &mn);
                    idb.create_per_day_indexes(date, &tsid, &mn);
                    self.put_tsid_by_metric_name_to_cache(&tsid, mr.metric_name_raw);
                    new_series_count += 1;
                }

                add_to_pending_hour(tsid.metric_id);
                r.tsid = tsid;
                prev_tsid = tsid;
                prev_metric_name_raw = mr.metric_name_raw;
                rows.push(r);
                row_names.push(mr.metric_name_raw);
            }
        }

        self.slow_row_inserts
            .fetch_add(slow_inserts_count, Ordering::Relaxed);
        self.new_timeseries_created
            .fetch_add(new_series_count, Ordering::Relaxed);
        self.timeseries_repopulated
            .fetch_add(series_repopulated, Ordering::Relaxed);

        if !pending_hour_entries.is_empty() {
            let mut pending = self.pending_hour_entries.lock();
            for metric_id in &pending_hour_entries {
                pending.add(*metric_id);
            }
        }

        if let Err(err) = self.prefill_next_index_db(&rows, &row_names) {
            if first_warn.is_none() {
                first_warn = Some(format!("cannot prefill next indexdb: {err}"));
            }
        }

        if let Err(err) = self.update_per_date_data(&rows, &row_names, &hm_prev, &hm_curr) {
            if first_warn.is_none() {
                first_warn = Some(format!("cannot update per-day index: {err}"));
            }
        }

        if let Some(warn) = first_warn {
            log::warn!("warn occurred during rows addition: {warn}");
        }

        self.tb.must_add_rows(&rows);
        rows.len()
    }

    /// Validates the value and the timestamp of the given row, updating the
    /// skip counters. Returns false when the row must be skipped.
    fn validate_row(
        &self,
        mr: MetricRowRef<'_>,
        min_timestamp: i64,
        max_timestamp: i64,
        first_warn: &mut Option<String>,
    ) -> bool {
        if mr.value.is_nan() && !decimal::is_stale_nan(mr.value) {
            // Skip NaNs other than the Prometheus staleness marker, since
            // the underlying encoding doesn't know how to work with them.
            return false;
        }
        if mr.timestamp < min_timestamp {
            // Skip rows with too small timestamps outside the retention.
            if first_warn.is_none() {
                *first_warn = Some(format!(
                    "cannot insert row with too small timestamp {} outside the retention; \
                     minimum allowed timestamp is {min_timestamp}",
                    mr.timestamp
                ));
            }
            self.too_small_timestamp_rows
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if mr.timestamp > max_timestamp {
            // Skip rows with too big timestamps significantly exceeding the
            // current time.
            if first_warn.is_none() {
                *first_warn = Some(format!(
                    "cannot insert row with too big timestamp {} exceeding the current time; \
                     maximum allowed timestamp is {max_timestamp}",
                    mr.timestamp
                ));
            }
            self.too_big_timestamp_rows.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }
}

/// Unmarshals `raw` into `mn` and sorts the tags. Returns false (and updates
/// the warn/counters) on failure.
fn unmarshal_raw_sorted(
    mn: &mut MetricName,
    raw: &[u8],
    inner: &StorageInner,
    first_warn: &mut Option<String>,
) -> bool {
    if let Err(err) = mn.unmarshal_raw(raw) {
        if first_warn.is_none() {
            *first_warn = Some(format!("cannot unmarshal MetricNameRaw {raw:?}: {err}"));
        }
        inner
            .invalid_raw_metric_names
            .fetch_add(1, Ordering::Relaxed);
        return false;
    }
    mn.sort_tags();
    true
}
