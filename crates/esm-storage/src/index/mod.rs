//! Stage 3: the inverted index (per-partition indexDB of the upstream VictoriaMetrics
//! v1.146.0 `index_db.go`) on top of the esm-mergeset LSM.
//!
//! In v1.146 each monthly partition owns its own [`IndexDb`] (mergeset table
//! at `data/indexdb/<YYYY_MM>`). This module keeps the struct
//! partition-agnostic: the caller (stage-4 `storage.rs`) supplies the path,
//! the id/time-range/name of the partition and a shared [`IndexDbContext`]
//! holding the storage-level caches and self-healing state, then wires
//! per-partition instances together.
//!
//! PORT-SKIP (per spec §8): legacy read-only indexDBs, Graphite paths/query
//! filters, TSDB status / series count / label-name-value search (stage 5
//! concern), cardinality limiters.

pub mod caches;
mod create;
mod planner;
mod row_merge;
mod search;
mod tag_filters;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use esm_common::uint64set::Set;
use esm_encoding::{marshal_uint64, marshal_var_uint64, unmarshal_var_uint64};
use esm_mergeset::{FlushCallback, Table};
use parking_lot::{Mutex, RwLock};

use crate::time_range::TimeRange;
use crate::tsid::Tsid;

use caches::{BytesCache, DateMetricIdCache, MetricIdCache, WorkingSetCache};

pub use create::{generate_tsid, generate_unique_metric_id};
pub use row_merge::{
    check_items_sorted, merge_tag_to_metric_ids_rows, merge_tag_to_metric_ids_rows_callback,
    remove_duplicate_metric_ids, TagToMetricIDsRowParser,
    INDEX_BLOCKS_WITH_METRIC_IDS_INCORRECT_ORDER, INDEX_BLOCKS_WITH_METRIC_IDS_PROCESSED,
    MAX_METRIC_IDS_PER_ROW,
};
pub use search::{match_tag_filters, IndexSearch};
pub use tag_filters::{
    convert_to_composite_tag_filterss, get_common_metric_name_for_tag_filterss, TagFilter,
    TagFilters,
};

// --- key namespaces (all index keys start with 1 prefix byte) ---

/// Prefix for MetricName->TSID entries. Only used when the per-day index is
/// disabled.
pub const NS_PREFIX_METRIC_NAME_TO_TSID: u8 = 0;
/// Prefix for Tag->MetricIDs entries.
pub const NS_PREFIX_TAG_TO_METRIC_IDS: u8 = 1;
/// Prefix for MetricID->TSID entries.
pub const NS_PREFIX_METRIC_ID_TO_TSID: u8 = 2;
/// Prefix for MetricID->MetricName entries.
pub const NS_PREFIX_METRIC_ID_TO_METRIC_NAME: u8 = 3;
/// Prefix for deleted MetricID entries.
pub const NS_PREFIX_DELETED_METRIC_ID: u8 = 4;
/// Prefix for Date->MetricID entries.
pub const NS_PREFIX_DATE_TO_METRIC_ID: u8 = 5;
/// Prefix for (Date,Tag)->MetricIDs entries.
pub const NS_PREFIX_DATE_TAG_TO_METRIC_IDS: u8 = 6;
/// Prefix for (Date,MetricName)->TSID entries.
pub const NS_PREFIX_DATE_METRIC_NAME_TO_TSID: u8 = 7;

/// The size of the common key prefix (1 namespace byte).
pub const COMMON_PREFIX_LEN: usize = 1;

/// The deadline value that disables deadline checks (unix seconds).
pub const NO_DEADLINE: u64 = u64::MAX;

/// The prefix for composite tag keys, which speed up searches for filter
/// sets containing a `{__name__="<metric_name>"}` filter.
///
/// It is expected that the given prefix isn't used by users.
pub const COMPOSITE_TAG_KEY_PREFIX: u8 = 0xfe;

/// The tag key for the reverse metric name (Graphite wildcard speedup).
///
/// It is expected that the given key isn't used by users.
pub const GRAPHITE_REVERSE_TAG_KEY: &[u8] = b"\xff";

/// The interval for flushing recently ingested index items to disk.
/// Go: dataFlushInterval (partition.go), 5s by default.
const DATA_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Appends the 1-byte namespace prefix to `dst`. Go: marshalCommonPrefix.
#[inline]
pub fn marshal_common_prefix(dst: &mut Vec<u8>, ns_prefix: u8) {
    dst.push(ns_prefix);
}

/// Strips the 1-byte namespace prefix from `src`.
/// Go: unmarshalCommonPrefix.
pub fn unmarshal_common_prefix(src: &[u8]) -> Result<(&[u8], u8), String> {
    if src.len() < COMMON_PREFIX_LEN {
        return Err(format!(
            "cannot unmarshal common prefix from {} bytes; need at least {COMMON_PREFIX_LEN} bytes",
            src.len()
        ));
    }
    Ok((&src[COMMON_PREFIX_LEN..], src[0]))
}

/// Appends the composite tag key `{0xfe, varuint(len(name)), name, key}` to
/// `dst`. Go: marshalCompositeTagKey.
pub fn marshal_composite_tag_key(dst: &mut Vec<u8>, name: &[u8], key: &[u8]) {
    dst.push(COMPOSITE_TAG_KEY_PREFIX);
    marshal_var_uint64(dst, name.len() as u64);
    dst.extend_from_slice(name);
    dst.extend_from_slice(key);
}

/// Splits a composite tag key back into (name, key).
/// Go: unmarshalCompositeTagKey.
pub fn unmarshal_composite_tag_key(src: &[u8]) -> Result<(&[u8], &[u8]), String> {
    if src.is_empty() {
        return Err("composite tag key cannot be empty".to_string());
    }
    if src[0] != COMPOSITE_TAG_KEY_PREFIX {
        return Err(format!("missing composite tag key prefix in {src:?}"));
    }
    let src = &src[1..];
    let (n, n_size) = unmarshal_var_uint64(src)
        .ok_or("cannot unmarshal metric name length from composite tag key")?;
    let src = &src[n_size..];
    if (src.len() as u64) < n {
        return Err(format!(
            "missing metric name with length {n} in composite tag key {src:?}"
        ));
    }
    let name = &src[..n as usize];
    let key = &src[n as usize..];
    Ok((name, key))
}

/// Returns true for artificially created tag keys (composite / reverse
/// Graphite). Go: isArtificialTagKey.
pub fn is_artificial_tag_key(key: &[u8]) -> bool {
    key == GRAPHITE_REVERSE_TAG_KEY || (!key.is_empty() && key[0] == COMPOSITE_TAG_KEY_PREFIX)
}

/// Appends the reversed `src` to `dst`. Go: reverseBytes.
pub(crate) fn reverse_bytes(dst: &mut Vec<u8>, src: &[u8]) {
    dst.extend(src.iter().rev());
}

// --- errors ---

/// Errors returned by index search operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchError {
    /// The search deadline was exceeded. Go: ErrDeadlineExceeded.
    DeadlineExceeded,
    /// The number of matching time series exceeds the given limit.
    /// Go: errTooManyTimeseries.
    TooManyTimeseries(usize),
    /// Too many loops are needed for applying a tag filter. This is internal
    /// planner control flow (Go: errTooManyLoops) — it never escapes the
    /// public search APIs.
    TooManyLoops,
    /// Any other (corruption/IO) error.
    Other(String),
}

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchError::DeadlineExceeded => write!(f, "deadline exceeded"),
            SearchError::TooManyTimeseries(max_metrics) => write!(
                f,
                "the number of matching timeseries exceeds {max_metrics}; \
                 either narrow down the search or increase the limit"
            ),
            SearchError::TooManyLoops => {
                write!(f, "too many loops is needed for applying this filter")
            }
            SearchError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SearchError {}

// --- IndexDbContext: the storage seam ---

#[derive(Default)]
struct MissingMetricIds {
    m: HashMap<u64, u64>,
    reset_deadline: u64,
}

/// State and configuration shared between all per-partition [`IndexDb`]
/// instances. In Go this role is played by `*Storage`; stage 4 creates one
/// context and passes it to every partition indexDB.
pub struct IndexDbContext {
    /// When set, the per-day index (ns 5/6/7) is not written and TSID
    /// lookups go through the global ns0 index. Go: -disablePerDayIndex.
    pub disable_per_day_index: bool,

    /// Composite filters are used for time ranges with
    /// `min_timestamp >= min_timestamp_for_composite_index`.
    /// A fresh database uses 0 (composite index always enabled).
    pub min_timestamp_for_composite_index: i64,

    /// Storage-level MetricID -> TSID cache (Go: Storage.metricIDCache).
    /// MetricIDs are globally unique, so the cache is shared across idbs.
    metric_id_to_tsid_cache: BytesCache,

    /// Storage-level MetricID -> MetricName cache
    /// (Go: Storage.metricNameCache).
    metric_name_cache: BytesCache,

    /// Self-healing state for missing metricIDs
    /// (Go: Storage.missingMetricIDs).
    missing_metric_ids: Mutex<MissingMetricIds>,
}

impl IndexDbContext {
    /// Creates a context with the default cache sizes
    /// (metricIDCache = mem/16, metricNameCache = mem/10, spec §6.2).
    pub fn new() -> IndexDbContext {
        let mem = esm_common::memory::allowed() as u64;
        Self::with_cache_sizes(mem / 16, mem / 10)
    }

    /// Creates a context with explicit cache sizes in bytes.
    pub fn with_cache_sizes(
        metric_id_cache_bytes: u64,
        metric_name_cache_bytes: u64,
    ) -> IndexDbContext {
        IndexDbContext {
            disable_per_day_index: false,
            min_timestamp_for_composite_index: 0,
            metric_id_to_tsid_cache: WorkingSetCache::new(metric_id_cache_bytes),
            metric_name_cache: WorkingSetCache::new(metric_name_cache_bytes),
            missing_metric_ids: Mutex::new(MissingMetricIds::default()),
        }
    }

    /// Looks up the TSID for the given metricID in the shared cache.
    /// Go: Storage.getTSIDByMetricIDFromCache.
    pub fn get_tsid_by_metric_id_from_cache(&self, dst: &mut Tsid, metric_id: u64) -> bool {
        let key = metric_id.to_be_bytes();
        match self.metric_id_to_tsid_cache.get(&key) {
            Some(v) if v.len() == crate::tsid::MARSHALED_TSID_SIZE => dst.unmarshal(&v).is_ok(),
            _ => false,
        }
    }

    /// Stores the TSID for the given metricID in the shared cache.
    /// Go: Storage.putTSIDByMetricIDToCache.
    pub fn put_tsid_by_metric_id_to_cache(&self, metric_id: u64, tsid: &Tsid) {
        let key = metric_id.to_be_bytes();
        let mut value = Vec::with_capacity(crate::tsid::MARSHALED_TSID_SIZE);
        tsid.marshal(&mut value);
        self.metric_id_to_tsid_cache
            .set(&key, value.into_boxed_slice());
    }

    /// Appends the cached metric name for the given metricID to `dst`.
    /// Returns false on a cache miss.
    /// Go: Storage.getMetricNameByMetricIDFromCache.
    pub fn get_metric_name_by_metric_id_from_cache(
        &self,
        dst: &mut Vec<u8>,
        metric_id: u64,
    ) -> bool {
        let key = metric_id.to_be_bytes();
        match self.metric_name_cache.get(&key) {
            Some(v) => {
                dst.extend_from_slice(&v);
                true
            }
            None => false,
        }
    }

    /// Stores the metric name for the given metricID in the shared cache.
    /// Go: Storage.putMetricNameByMetricIDToCache.
    pub fn put_metric_name_by_metric_id_to_cache(&self, metric_id: u64, metric_name: &[u8]) {
        let key = metric_id.to_be_bytes();
        self.metric_name_cache
            .set(&key, metric_name.to_vec().into_boxed_slice());
    }

    /// Returns true if the given metricID was already reported as missing
    /// more than 60 seconds ago. This gates the ns4 tombstoning of metricIDs
    /// whose MetricID->TSID/MetricName rows are missing (self-healing of a
    /// corrupted index after unclean shutdown).
    /// Go: Storage.wasMetricIDMissingBefore.
    pub fn was_metric_id_missing_before(&self, metric_id: u64) -> bool {
        let ct = esm_common::fasttime::unix_timestamp();
        let mut state = self.missing_metric_ids.lock();
        if ct > state.reset_deadline {
            state.m.clear();
            state.reset_deadline = ct + 2 * 60;
        }
        let delete_deadline = *state.m.entry(metric_id).or_insert(ct + 60);
        ct > delete_deadline
    }
}

impl Default for IndexDbContext {
    fn default() -> Self {
        Self::new()
    }
}

// --- IndexDb ---

/// Returns the default maximum size of `tagFiltersToMetricIDsCache`.
/// Go: getTagFiltersCacheSize (mem/32).
fn get_tag_filters_cache_size() -> u64 {
    esm_common::memory::allowed() as u64 / 32
}

/// A per-partition inverted index database. Go: indexDB.
pub struct IndexDb {
    /// The number of calls for date range searches.
    pub date_range_search_calls: AtomicU64,
    /// The number of hits for date range searches.
    pub date_range_search_hits: AtomicU64,
    /// The number of calls for global searches.
    pub global_search_calls: AtomicU64,
    /// The number of missing MetricID->TSID entries.
    pub missing_tsids_for_metric_id: AtomicU64,
    /// The number of missing MetricID->MetricName entries.
    pub missing_metric_names_for_metric_id: AtomicU64,

    /// Identifies the indexDB in various caches.
    id: u64,
    /// The time range covered by this indexDB (the partition month).
    tr: TimeRange,
    name: String,

    tb: Option<Table>,

    /// The shared storage-level state (Go: indexDB.s).
    pub(crate) ctx: Arc<IndexDbContext>,

    /// Whether the indexDB accepts registration of new series.
    no_register_new_series: AtomicBool,

    /// Cache for fast TagFilters -> MetricIDs lookups.
    pub(crate) tag_filters_to_metric_ids_cache: Arc<WorkingSetCache<Arc<Set>>>,

    /// Cache for (date, tagFilter) -> loopsCount, used for reducing the
    /// amount of work when matching a set of filters.
    pub(crate) loops_per_date_tag_filter_cache: BytesCache,

    /// A cache of metricIDs that have been added to this indexDB
    /// (ingestion-only).
    pub(crate) metric_id_cache: MetricIdCache,

    /// A (date, metricID) cache speeding up per-day index probes during
    /// ingestion.
    pub(crate) date_metric_id_cache: DateMetricIdCache,

    /// An in-memory set of deleted metricIDs (copy-on-write).
    deleted_metric_ids: RwLock<Arc<Set>>,
    deleted_metric_ids_update_lock: Mutex<()>,
}

impl IndexDb {
    /// Opens (creating if needed) an indexDB at the given path.
    /// Go: mustOpenIndexDB.
    pub fn must_open(
        id: u64,
        tr: TimeRange,
        name: &str,
        path: impl AsRef<Path>,
        ctx: Arc<IndexDbContext>,
        is_read_only: Arc<AtomicBool>,
        no_register_new_series: bool,
    ) -> IndexDb {
        let tfss_cache: Arc<WorkingSetCache<Arc<Set>>> =
            Arc::new(WorkingSetCache::new(get_tag_filters_cache_size()));
        // The tagFilters cache is invalidated whenever newly ingested items
        // become searchable.
        let cache_for_cb = Arc::clone(&tfss_cache);
        let flush_callback: FlushCallback = Arc::new(move || cache_for_cb.reset());
        let tb = Table::must_open(
            path,
            DATA_FLUSH_INTERVAL,
            Some(flush_callback),
            Duration::ZERO,
            Some(merge_tag_to_metric_ids_rows_callback()),
            is_read_only,
        );
        let db = IndexDb {
            date_range_search_calls: AtomicU64::new(0),
            date_range_search_hits: AtomicU64::new(0),
            global_search_calls: AtomicU64::new(0),
            missing_tsids_for_metric_id: AtomicU64::new(0),
            missing_metric_names_for_metric_id: AtomicU64::new(0),
            id,
            tr,
            name: name.to_string(),
            tb: Some(tb),
            ctx,
            no_register_new_series: AtomicBool::new(no_register_new_series),
            tag_filters_to_metric_ids_cache: tfss_cache,
            loops_per_date_tag_filter_cache: WorkingSetCache::new(
                esm_common::memory::allowed() as u64 / 128,
            ),
            metric_id_cache: MetricIdCache::new(),
            date_metric_id_cache: DateMetricIdCache::new(),
            deleted_metric_ids: RwLock::new(Arc::new(Set::default())),
            deleted_metric_ids_update_lock: Mutex::new(()),
        };
        db.must_load_deleted_metric_ids();
        db
    }

    /// Closes the indexDB. Go: indexDB.MustClose.
    pub fn must_close(mut self) {
        let tb = self.tb.take().expect("BUG: IndexDb closed twice");
        tb.must_close();
        // The caches are dropped together with self.
    }

    /// The id of the indexDB (used in caches shared across indexDBs).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The name of the indexDB (the partition name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The time range covered by this indexDB.
    pub fn time_range(&self) -> TimeRange {
        self.tr
    }

    /// The shared storage-level context.
    pub fn context(&self) -> &Arc<IndexDbContext> {
        &self.ctx
    }

    /// Whether registration of new series is disabled for this indexDB.
    pub fn no_register_new_series(&self) -> bool {
        self.no_register_new_series.load(Ordering::Relaxed)
    }

    /// Enables/disables registration of new series.
    pub fn set_no_register_new_series(&self, v: bool) {
        self.no_register_new_series.store(v, Ordering::Relaxed);
    }

    pub(crate) fn tb(&self) -> &Table {
        self.tb.as_ref().expect("BUG: IndexDb is closed")
    }

    /// Makes all the recently added index entries visible to search.
    /// For tests and debugging only. Go: mergeset.Table.DebugFlush.
    pub fn debug_flush(&self) {
        self.tb().debug_flush();
    }

    /// Updates `m` with the mergeset table metrics of the indexDB.
    pub fn update_table_metrics(&self, m: &mut esm_mergeset::TableMetrics) {
        self.tb().update_metrics(m);
    }

    /// Returns an index search cursor over the indexDB.
    /// Go: indexDB.getIndexSearch.
    pub fn get_index_search(&self, deadline: u64) -> IndexSearch<'_> {
        IndexSearch::new(self, deadline, false)
    }

    /// Returns an index search cursor bypassing the shared metric-name cache
    /// (Go: sparse cache mode for exports).
    pub fn get_index_search_internal(&self, deadline: u64, sparse: bool) -> IndexSearch<'_> {
        IndexSearch::new(self, deadline, sparse)
    }

    // --- deleted metricIDs ---

    /// A snapshot of the deleted metricIDs set.
    /// Go: indexDB.getDeletedMetricIDs.
    pub fn get_deleted_metric_ids(&self) -> Arc<Set> {
        Arc::clone(&self.deleted_metric_ids.read())
    }

    /// Replaces the deleted metricIDs set.
    /// Go: indexDB.setDeletedMetricIDs.
    pub fn set_deleted_metric_ids(&self, dmis: Arc<Set>) {
        *self.deleted_metric_ids.write() = dmis;
    }

    /// Atomically adds `metric_ids` to the deleted metricIDs set
    /// (copy-on-write). Go: indexDB.updateDeletedMetricIDs.
    pub fn update_deleted_metric_ids(&self, metric_ids: &Set) {
        let _guard = self.deleted_metric_ids_update_lock.lock();
        let mut dmis_new = (*self.get_deleted_metric_ids()).clone();
        dmis_new.union(metric_ids);
        self.set_deleted_metric_ids(Arc::new(dmis_new));
    }

    fn must_load_deleted_metric_ids(&self) {
        let mut is = self.get_index_search(NO_DEADLINE);
        let dmis = is.load_deleted_metric_ids().unwrap_or_else(|err| {
            panic!(
                "FATAL: cannot load deleted metricIDs for indexDB {:?}: {err}",
                self.name
            )
        });
        is.must_close();
        self.set_deleted_metric_ids(Arc::new(dmis));
    }

    /// Marks the series matching `tfss` (searched in the global index) as
    /// deleted and persists ns4 tombstones. Returns the deleted metricIDs.
    /// Go: indexDB.DeleteSeries.
    pub fn delete_series(
        &self,
        tfss: &[TagFilters],
        max_metrics: usize,
    ) -> Result<Arc<Set>, SearchError> {
        // Unconditionally search the global index, since a given day in the
        // per-day index may not contain the full set of metricIDs matching
        // the tfss.
        let mut is = self.get_index_search(NO_DEADLINE);
        let metric_ids = is.search_metric_ids(
            tfss,
            crate::time_range::GLOBAL_INDEX_TIME_RANGE,
            max_metrics,
        );
        is.must_close();
        let metric_ids = Arc::new(metric_ids?);
        self.save_deleted_metric_ids(&metric_ids);
        Ok(metric_ids)
    }

    /// Persists the deleted metricIDs as ns4 rows, after adding them to the
    /// in-memory set and resetting the tagFilters cache (in that order — see
    /// the Go source for the unclean-shutdown rationale).
    /// Go: indexDB.saveDeletedMetricIDs.
    pub fn save_deleted_metric_ids(&self, metric_ids: &Set) {
        if metric_ids.is_empty() {
            // Nothing to delete.
            return;
        }

        // Atomically add the deleted metricIDs to the in-memory set.
        self.update_deleted_metric_ids(metric_ids);

        // Reset the TagFilters -> metricIDs cache, since it may contain
        // deleted metricIDs.
        self.tag_filters_to_metric_ids_cache.reset();

        // Store the metricIDs as deleted.
        let mut items = create::IndexItems::default();
        metric_ids.for_each(|part| {
            for &metric_id in part {
                items.b.push(NS_PREFIX_DELETED_METRIC_ID);
                marshal_uint64(&mut items.b, metric_id);
                items.next();
            }
            true
        });
        items.add_to(self.tb());
    }

    // --- cached tag-filters search ---

    fn get_metric_ids_from_tag_filters_cache(&self, key: &[u8]) -> Option<Arc<Set>> {
        self.tag_filters_to_metric_ids_cache.get(key)
    }

    fn put_metric_ids_to_tag_filters_cache(&self, metric_ids: Arc<Set>, key: &[u8]) {
        self.tag_filters_to_metric_ids_cache.set(key, metric_ids);
    }

    /// Returns the metricIDs for the given tag filters and time range,
    /// memoized in `tagFiltersToMetricIDsCache`.
    /// Go: indexDB.searchMetricIDs.
    pub fn search_metric_ids(
        &self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
        deadline: u64,
    ) -> Result<Arc<Set>, SearchError> {
        if tfss.is_empty() {
            return Ok(Arc::new(Set::default()));
        }

        let mut tf_key = Vec::new();
        marshal_tag_filters_key(&mut tf_key, tfss, tr);
        if let Some(metric_ids) = self.get_metric_ids_from_tag_filters_cache(&tf_key) {
            // Fast path - the metricIDs are found in the cache.
            if metric_ids.len() > max_metrics {
                return Err(SearchError::TooManyTimeseries(max_metrics));
            }
            return Ok(metric_ids);
        }

        // Slow path - search for the metricIDs in the db.
        let mut is = self.get_index_search(deadline);
        let res = is.search_metric_ids(tfss, tr, max_metrics);
        is.must_close();
        let metric_ids = Arc::new(res?);

        // Store the metricIDs in the cache.
        self.put_metric_ids_to_tag_filters_cache(Arc::clone(&metric_ids), &tf_key);

        Ok(metric_ids)
    }

    /// Searches the TSIDs that correspond to the filters within the given
    /// time range. The returned TSIDs are sorted.
    /// Go: indexDB.SearchTSIDs.
    pub fn search_tsids(
        &self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
        deadline: u64,
    ) -> Result<Vec<Tsid>, SearchError> {
        let metric_ids = self.search_metric_ids(tfss, tr, max_metrics, deadline)?;
        if metric_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tsids: Vec<Tsid> = Vec::with_capacity(metric_ids.len());
        let mut metric_ids_to_delete = Set::default();
        let mut err: Option<SearchError> = None;
        let mut pace_limiter = 0usize;
        let mut is = self.get_index_search(deadline);
        metric_ids.for_each(|part| {
            for &metric_id in part {
                if pace_limiter & search::PACE_LIMITER_SLOW_ITERATIONS_MASK == 0 {
                    if let Err(e) = search::check_search_deadline_and_pace(deadline) {
                        err = Some(e);
                        return false;
                    }
                }
                pace_limiter += 1;

                // Try obtaining the TSID from the MetricID->TSID cache. This
                // is much faster than scanning the mergeset if it contains a
                // lot of metricIDs.
                let mut tsid = Tsid::default();
                if self
                    .ctx
                    .get_tsid_by_metric_id_from_cache(&mut tsid, metric_id)
                {
                    // Fast path - the tsid for the metricID is found in the
                    // cache.
                    tsids.push(tsid);
                    continue;
                }
                if !is.get_tsid_by_metric_id(&mut tsid, metric_id) {
                    // Cannot find the TSID for the given metricID.
                    // This may be the case on an incomplete indexDB due to
                    // unflushed entries. Mark the metricID as deleted, so it
                    // is created again when a new sample for the given time
                    // series is ingested next time.
                    if self.ctx.was_metric_id_missing_before(metric_id) {
                        self.missing_tsids_for_metric_id
                            .fetch_add(1, Ordering::Relaxed);
                        metric_ids_to_delete.add(metric_id);
                    }
                    continue;
                }
                self.ctx.put_tsid_by_metric_id_to_cache(metric_id, &tsid);
                tsids.push(tsid);
            }
            true
        });
        is.must_close();
        if let Some(e) = err {
            return Err(e);
        }

        // Sort the found tsids, since they must be passed to the TSID search
        // in the sorted order.
        tsids.sort_unstable();

        if !metric_ids_to_delete.is_empty() {
            self.save_deleted_metric_ids(&metric_ids_to_delete);
        }
        Ok(tsids)
    }

    /// Appends the metric name for the given metricID to `dst`, returning
    /// (dst, found). Go: indexDB.searchMetricName.
    pub fn search_metric_name(
        &self,
        dst: Vec<u8>,
        metric_id: u64,
        no_cache: bool,
    ) -> (Vec<u8>, bool) {
        let mut is = self.get_index_search_internal(NO_DEADLINE, no_cache);
        let res = is.search_metric_name(dst, metric_id);
        is.must_close();
        res
    }

    /// Returns the (marshaled canonical) metric names matching the given tag
    /// filters within the given time range.
    /// Go: indexDB.SearchMetricNames.
    pub fn search_metric_names(
        &self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
        deadline: u64,
    ) -> Result<Vec<Vec<u8>>, SearchError> {
        let metric_ids = self.search_metric_ids(tfss, tr, max_metrics, deadline)?;
        if metric_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut metric_names: Vec<Vec<u8>> = Vec::with_capacity(metric_ids.len());
        let mut metric_ids_to_delete = Set::default();
        let mut err: Option<SearchError> = None;
        let mut pace_limiter = 0usize;
        let mut is = self.get_index_search(deadline);
        metric_ids.for_each(|part| {
            for &metric_id in part {
                if pace_limiter & search::PACE_LIMITER_SLOW_ITERATIONS_MASK == 0 {
                    if let Err(e) = search::check_search_deadline_and_pace(deadline) {
                        err = Some(e);
                        return false;
                    }
                }
                pace_limiter += 1;

                let (metric_name, ok) = is.search_metric_name_with_cache(Vec::new(), metric_id);
                if !ok {
                    // Cannot find the metric name for the given metricID.
                    // Self-heal in the same way as search_tsids does.
                    if self.ctx.was_metric_id_missing_before(metric_id) {
                        self.missing_metric_names_for_metric_id
                            .fetch_add(1, Ordering::Relaxed);
                        metric_ids_to_delete.add(metric_id);
                    }
                    continue;
                }
                metric_names.push(metric_name);
            }
            true
        });
        is.must_close();
        if let Some(e) = err {
            return Err(e);
        }

        if !metric_ids_to_delete.is_empty() {
            self.save_deleted_metric_ids(&metric_ids_to_delete);
        }
        Ok(metric_names)
    }
}

/// Marshals the cache key for `tagFiltersToMetricIDsCache`:
/// `startDate u64be | endDate u64be | (0x00 | tf.Marshal()*)*`.
/// Go: marshalTagFiltersKey.
pub(crate) fn marshal_tag_filters_key(dst: &mut Vec<u8>, tfss: &[TagFilters], tr: TimeRange) {
    // Round the start and end times to per-day granularity according to the
    // per-day inverted index.
    let (start_date, end_date) = tr.date_range();
    marshal_uint64(dst, start_date);
    marshal_uint64(dst, end_date);
    for tfs in tfss {
        dst.push(0); // separator between tfs groups
        for tf in &tfs.tfs {
            tf.marshal(dst);
        }
    }
}
