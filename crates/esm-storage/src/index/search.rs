//! Port of the `indexSearch` methods from `index_db.go`: TSID/metric-name
//! lookups, the tag-filter planner with loops-count weighting, per-day and
//! global metricIDs search.

use esm_common::uint64set::Set;
use esm_encoding::{marshal_uint64, unmarshal_uint64, unmarshal_var_uint64};
use esm_mergeset::{Error as MergesetError, TableSearch};
use parking_lot::Mutex;

use crate::metric_name::{marshal_tag_value, unmarshal_tag_value, MetricName, KV_SEPARATOR_CHAR};
use crate::time_range::{TimeRange, GLOBAL_INDEX_DATE, GLOBAL_INDEX_TIME_RANGE, MSEC_PER_DAY};
use crate::tsid::Tsid;

use super::row_merge::TagToMetricIDsRowParser;
use super::tag_filters::{convert_to_composite_tag_filterss, TagFilter, TagFilters};
use super::{
    marshal_common_prefix, IndexDb, SearchError, COMPOSITE_TAG_KEY_PREFIX,
    GRAPHITE_REVERSE_TAG_KEY, NS_PREFIX_DATE_METRIC_NAME_TO_TSID, NS_PREFIX_DATE_TAG_TO_METRIC_IDS,
    NS_PREFIX_DATE_TO_METRIC_ID, NS_PREFIX_DELETED_METRIC_ID, NS_PREFIX_METRIC_ID_TO_METRIC_NAME,
    NS_PREFIX_METRIC_ID_TO_TSID, NS_PREFIX_METRIC_NAME_TO_TSID, NS_PREFIX_TAG_TO_METRIC_IDS,
};

// Pace limiter masks (Go: search.go). The deadline is checked every
// 2^16 / 2^14 / 2^12 iterations for fast / medium / slow iterations.
pub(crate) const PACE_LIMITER_FAST_ITERATIONS_MASK: usize = (1 << 16) - 1;
pub(crate) const PACE_LIMITER_MEDIUM_ITERATIONS_MASK: usize = (1 << 14) - 1;
pub(crate) const PACE_LIMITER_SLOW_ITERATIONS_MASK: usize = (1 << 12) - 1;

/// Returns DeadlineExceeded if the given deadline (unix seconds) is
/// exceeded. Go: checkSearchDeadlineAndPace.
pub(crate) fn check_search_deadline_and_pace(deadline: u64) -> Result<(), SearchError> {
    if esm_common::fasttime::unix_timestamp() > deadline {
        return Err(SearchError::DeadlineExceeded);
    }
    Ok(())
}

/// A search cursor over an [`IndexDb`]. Go: indexSearch.
pub struct IndexSearch<'a> {
    pub(crate) db: &'a IndexDb,
    pub(crate) ts: TableSearch,
    pub(crate) kb: Vec<u8>,
    pub(crate) mp: TagToMetricIDsRowParser,

    /// The deadline in unix seconds for the given search.
    pub(crate) deadline: u64,
}

impl<'a> IndexSearch<'a> {
    pub(crate) fn new(db: &'a IndexDb, deadline: u64, sparse: bool) -> IndexSearch<'a> {
        IndexSearch {
            db,
            ts: TableSearch::new(db.tb(), sparse),
            kb: Vec::new(),
            mp: TagToMetricIDsRowParser::default(),
            deadline,
        }
    }

    /// Closes the search cursor, releasing the table part references.
    pub fn must_close(self) {
        self.ts.must_close();
    }

    /// Fills `dst` with the TSID for the given marshaled canonical metric
    /// name at the given date, skipping deleted series. Returns false when
    /// nothing is found. Go: indexSearch.getTSIDByMetricName.
    pub fn get_tsid_by_metric_name(
        &mut self,
        dst: &mut Tsid,
        metric_name: &[u8],
        date: u64,
    ) -> bool {
        let dmis = self.db.get_deleted_metric_ids();
        let kb = &mut self.kb;
        kb.clear();
        if self.db.ctx.disable_per_day_index {
            marshal_common_prefix(kb, NS_PREFIX_METRIC_NAME_TO_TSID);
        } else {
            marshal_common_prefix(kb, NS_PREFIX_DATE_METRIC_NAME_TO_TSID);
            marshal_uint64(kb, date);
        }
        kb.extend_from_slice(metric_name);
        kb.push(KV_SEPARATOR_CHAR);
        self.ts.seek(kb);
        while self.ts.next_item() {
            let item = self.ts.item();
            if !item.starts_with(kb) {
                // Nothing found.
                return false;
            }
            let v = &item[kb.len()..];
            let tail = dst
                .unmarshal(v)
                .unwrap_or_else(|err| panic!("FATAL: cannot unmarshal TSID: {err}"));
            assert!(
                tail.is_empty(),
                "FATAL: unexpected non-empty tail left after unmarshaling TSID: {tail:X?}"
            );
            if dmis.has(dst.metric_id) {
                // The dst is deleted. Continue searching.
                continue;
            }
            // Found a valid dst.
            return true;
        }
        if let Some(err) = self.ts.error() {
            panic!("FATAL: error when searching TSID by metricName; searchPrefix {kb:?}: {err}");
        }
        // Nothing found.
        false
    }

    /// Appends the metric name for the given metricID to `dst`, consulting
    /// the shared metricName cache first.
    /// Go: indexSearch.searchMetricNameWithCache.
    pub fn search_metric_name_with_cache(
        &mut self,
        mut dst: Vec<u8>,
        metric_id: u64,
    ) -> (Vec<u8>, bool) {
        if self
            .db
            .ctx
            .get_metric_name_by_metric_id_from_cache(&mut dst, metric_id)
        {
            return (dst, true);
        }
        let (dst, ok) = self.search_metric_name(dst, metric_id);
        if ok {
            // There is no need in verifying whether the given metricID is
            // deleted, since the filtering must be performed before calling
            // this function.
            self.db
                .ctx
                .put_metric_name_by_metric_id_to_cache(metric_id, &dst);
            return (dst, true);
        }
        (dst, false)
    }

    /// Appends the metric name for the given metricID to `dst` (ns3 lookup,
    /// no caching). Go: indexSearch.searchMetricName.
    pub fn search_metric_name(&mut self, mut dst: Vec<u8>, metric_id: u64) -> (Vec<u8>, bool) {
        let kb = &mut self.kb;
        kb.clear();
        marshal_common_prefix(kb, NS_PREFIX_METRIC_ID_TO_METRIC_NAME);
        marshal_uint64(kb, metric_id);
        match self.ts.first_item_with_prefix(kb) {
            Ok(()) => {
                dst.extend_from_slice(&self.ts.item()[kb.len()..]);
                (dst, true)
            }
            Err(MergesetError::Eof) => (dst, false),
            Err(err) => panic!(
                "FATAL: error when searching metricName by metricID; searchPrefix {kb:?}: {err}"
            ),
        }
    }

    /// Fills `dst` with the TSID for the given metricID (ns2 lookup).
    /// Deleted metricIDs must be checked by the caller.
    /// Go: indexSearch.getTSIDByMetricID.
    pub fn get_tsid_by_metric_id(&mut self, dst: &mut Tsid, metric_id: u64) -> bool {
        let kb = &mut self.kb;
        kb.clear();
        marshal_common_prefix(kb, NS_PREFIX_METRIC_ID_TO_TSID);
        marshal_uint64(kb, metric_id);
        match self.ts.first_item_with_prefix(kb) {
            Ok(()) => {
                let v = &self.ts.item()[kb.len()..];
                let tail = dst.unmarshal(v).unwrap_or_else(|err| {
                    panic!("FATAL: cannot unmarshal the found TSID={v:X?} for metricID={metric_id}: {err}")
                });
                assert!(
                    tail.is_empty(),
                    "FATAL: unexpected non-zero tail left after unmarshaling TSID for metricID={metric_id}"
                );
                true
            }
            Err(MergesetError::Eof) => false,
            Err(err) => panic!(
                "FATAL: error when searching TSID by metricID={metric_id}; searchPrefix {kb:?}: {err}"
            ),
        }
    }

    /// Returns true if the given metricID is registered in this indexDB.
    /// Go: indexSearch.hasMetricID.
    pub fn has_metric_id(&mut self, metric_id: u64) -> bool {
        if self.db.metric_id_cache.has(metric_id) {
            return true;
        }
        let ok = self.has_metric_id_slow(metric_id);
        if ok {
            self.db.metric_id_cache.set(metric_id);
        }
        ok
    }

    fn has_metric_id_slow(&mut self, metric_id: u64) -> bool {
        let kb = &mut self.kb;
        kb.clear();
        marshal_common_prefix(kb, NS_PREFIX_METRIC_ID_TO_TSID);
        marshal_uint64(kb, metric_id);
        match self.ts.first_item_with_prefix(kb) {
            Ok(()) => true,
            Err(MergesetError::Eof) => false,
            Err(err) => panic!(
                "FATAL: error when searching for metricID={metric_id}; searchPrefix {kb:?}: {err}"
            ),
        }
    }

    /// Returns true if the (date, metricID) entry exists in the per-day
    /// index. Go: indexSearch.hasDateMetricID.
    pub fn has_date_metric_id(&mut self, date: u64, metric_id: u64) -> bool {
        if self.db.date_metric_id_cache.has(date, metric_id) {
            return true;
        }
        let ok = self.has_date_metric_id_slow(date, metric_id);
        if ok {
            self.db.date_metric_id_cache.set(date, metric_id);
        }
        ok
    }

    fn has_date_metric_id_slow(&mut self, date: u64, metric_id: u64) -> bool {
        let kb = &mut self.kb;
        kb.clear();
        marshal_common_prefix(kb, NS_PREFIX_DATE_TO_METRIC_ID);
        marshal_uint64(kb, date);
        marshal_uint64(kb, metric_id);
        match self.ts.first_item_with_prefix(kb) {
            Ok(()) => {
                assert!(
                    self.ts.item() == &kb[..],
                    "FATAL: unexpected entry for (date={date}, metricID={metric_id})"
                );
                true
            }
            Err(MergesetError::Eof) => false,
            Err(err) => panic!(
                "FATAL: unexpected error when searching for (date={date}, metricID={metric_id}) entry: {err}"
            ),
        }
    }

    /// Loads the ns4 deleted-metricID rows.
    /// Go: indexSearch.loadDeletedMetricIDs.
    pub(crate) fn load_deleted_metric_ids(&mut self) -> Result<Set, String> {
        let mut dmis = Set::default();
        let kb = &mut self.kb;
        kb.clear();
        kb.push(NS_PREFIX_DELETED_METRIC_ID);
        self.ts.seek(kb);
        while self.ts.next_item() {
            let item = self.ts.item();
            if !item.starts_with(kb) {
                break;
            }
            let item = &item[kb.len()..];
            if item.len() != 8 {
                return Err(format!(
                    "unexpected item len; got {} bytes; want 8 bytes",
                    item.len()
                ));
            }
            dmis.add(unmarshal_uint64(item));
        }
        if let Some(err) = self.ts.error() {
            return Err(err.to_string());
        }
        Ok(dmis)
    }

    // --- metricIDs search ---

    /// Returns the metricIDs (with deleted metricIDs filtered out) matching
    /// the given tag filters within the given time range.
    /// Go: indexSearch.searchMetricIDs.
    pub fn search_metric_ids(
        &mut self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
    ) -> Result<Set, SearchError> {
        let mut metric_ids = self.search_metric_ids_internal(tfss, tr, max_metrics)?;
        if metric_ids.is_empty() {
            // Nothing found.
            return Ok(metric_ids);
        }

        // Filter out deleted metricIDs.
        let dmis = self.db.get_deleted_metric_ids();
        metric_ids.subtract(&dmis);
        Ok(metric_ids)
    }

    /// Go: indexSearch.searchMetricIDsInternal.
    fn search_metric_ids_internal(
        &mut self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
    ) -> Result<Set, SearchError> {
        let mut metric_ids = Set::default();

        // PORT-SKIP: legacyContainsTimeRange — partition indexDBs may
        // receive data with bigger timestamps at any time.

        let converted;
        let tfss: &[TagFilters] =
            if tr.min_timestamp >= self.db.ctx.min_timestamp_for_composite_index {
                converted = convert_to_composite_tag_filterss(tfss);
                &converted
            } else {
                tfss
            };

        for tfs in tfss {
            let empty_tfs;
            let tfs = if tfs.filters().is_empty() {
                // An empty filter set must be equivalent to `{__name__!=""}`.
                let mut t = TagFilters::new();
                t.add(&[], &[], true, false)
                    .expect(r#"BUG: cannot add {__name__!=""} filter"#);
                empty_tfs = t;
                &empty_tfs
            } else {
                tfs
            };
            self.update_metric_ids_for_tag_filters(&mut metric_ids, tfs, tr, max_metrics + 1)?;
            if metric_ids.len() > max_metrics {
                return Err(SearchError::TooManyTimeseries(max_metrics));
            }
        }
        Ok(metric_ids)
    }

    /// Go: indexSearch.updateMetricIDsForTagFilters.
    fn update_metric_ids_for_tag_filters(
        &mut self,
        metric_ids: &mut Set,
        tfs: &TagFilters,
        tr: TimeRange,
        max_metrics: usize,
    ) -> Result<(), SearchError> {
        use std::sync::atomic::Ordering;
        if tr != GLOBAL_INDEX_TIME_RANGE {
            // Fast path - search the metricIDs by date range in the per-day
            // inverted index.
            self.db
                .date_range_search_calls
                .fetch_add(1, Ordering::Relaxed);
            let (min_date, max_date) = tr.date_range();
            return self.update_metric_ids_for_date_range(
                metric_ids,
                tfs,
                min_date,
                max_date,
                max_metrics,
            );
        }

        // Slow path - search the metricIDs in the global inverted index.
        self.db.global_search_calls.fetch_add(1, Ordering::Relaxed);
        let m = self.get_metric_ids_for_date_and_filters(GLOBAL_INDEX_DATE, tfs, max_metrics)?;
        metric_ids.union_may_own(&m);
        Ok(())
    }

    /// Go: indexSearch.updateMetricIDsForDateRange.
    fn update_metric_ids_for_date_range(
        &mut self,
        metric_ids: &mut Set,
        tfs: &TagFilters,
        min_date: u64,
        max_date: u64,
        max_metrics: usize,
    ) -> Result<(), SearchError> {
        use std::sync::atomic::Ordering;
        if min_date == max_date {
            // Fast path - query only a single date.
            let m = self.get_metric_ids_for_date_and_filters(min_date, tfs, max_metrics)?;
            metric_ids.union_may_own(&m);
            self.db
                .date_range_search_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Slower path - search for metricIDs for each day.
        //
        // Few-day ranges are searched sequentially: Go fans the days out over
        // goroutines, but spawning OS threads per query costs milliseconds on
        // Windows (~8ms per spawn+join with Defender active, observed as a
        // steady +16ms on every query whose lookbehind window crosses
        // midnight), which dwarfs a sequential per-day lookup.
        const MAX_SEQUENTIAL_DAYS: u64 = 4;
        if max_date - min_date < MAX_SEQUENTIAL_DAYS {
            let mut m = Set::default();
            for date in min_date..=max_date {
                let day = self.get_metric_ids_for_date_and_filters(date, tfs, max_metrics)?;
                if m.len() < max_metrics {
                    m.union_may_own(&day);
                }
            }
            metric_ids.union_may_own(&m);
            self.db
                .date_range_search_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Many days: fan out over threads like Go's per-day goroutines.
        let db = self.db;
        let deadline = self.deadline;
        let shared: Mutex<(Set, Option<SearchError>)> = Mutex::new((Set::default(), None));
        std::thread::scope(|s| {
            for date in min_date..=max_date {
                let shared = &shared;
                s.spawn(move || {
                    let mut is_local = db.get_index_search(deadline);
                    let res = is_local.get_metric_ids_for_date_and_filters(date, tfs, max_metrics);
                    is_local.must_close();
                    let mut guard = shared.lock();
                    if guard.1.is_some() {
                        return;
                    }
                    match res {
                        Ok(m) => {
                            if guard.0.len() < max_metrics {
                                guard.0.union_may_own(&m);
                            }
                        }
                        Err(err) => guard.1 = Some(err),
                    }
                });
            }
        });
        let (m, err) = shared.into_inner();
        if let Some(err) = err {
            return Err(err);
        }
        metric_ids.union_may_own(&m);
        self.db
            .date_range_search_hits
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Marshals the per-day (or global when date == 0) tag-rows prefix.
/// Go: indexSearch.marshalCommonPrefixForDate.
pub(crate) fn marshal_common_prefix_for_date(dst: &mut Vec<u8>, date: u64) {
    if date == GLOBAL_INDEX_DATE {
        // Global index.
        marshal_common_prefix(dst, NS_PREFIX_TAG_TO_METRIC_IDS);
    } else {
        // Per-day index.
        marshal_common_prefix(dst, NS_PREFIX_DATE_TAG_TO_METRIC_IDS);
        marshal_uint64(dst, date);
    }
}

/// Returns the time range covered by a single date of the per-day index
/// (used by the searches on a particular date).
#[allow(dead_code)]
pub(crate) fn time_range_for_date(date: u64) -> TimeRange {
    if date == GLOBAL_INDEX_DATE {
        return GLOBAL_INDEX_TIME_RANGE;
    }
    TimeRange {
        min_timestamp: date as i64 * MSEC_PER_DAY,
        max_timestamp: (date + 1) as i64 * MSEC_PER_DAY - 1,
    }
}

// --- composite filter unfolding and direct metric-name matching ---

/// Go: hasCompositeTagFilters.
fn has_composite_tag_filters(tfs: &[&TagFilter], prefix: &[u8]) -> bool {
    let mut tag_key = Vec::new();
    for tf in tfs {
        if !tf.prefix().starts_with(prefix) {
            continue;
        }
        let suffix = &tf.prefix()[prefix.len()..];
        tag_key.clear();
        unmarshal_tag_value(&mut tag_key, suffix).unwrap_or_else(|err| {
            panic!("BUG: cannot unmarshal tag key from suffix={suffix:?}: {err}")
        });
        if !tag_key.is_empty() && tag_key[0] == COMPOSITE_TAG_KEY_PREFIX {
            return true;
        }
    }
    false
}

/// Unfolds composite tag filters back into plain per-tag filters plus a
/// `{__name__="name"}` filter. Go: removeCompositeTagFilters.
pub(crate) fn remove_composite_tag_filters(tfs: &[&TagFilter], prefix: &[u8]) -> Vec<TagFilter> {
    if !has_composite_tag_filters(tfs, prefix) {
        return tfs.iter().map(|tf| (*tf).clone()).collect();
    }
    let mut tag_key = Vec::new();
    let mut name: Vec<u8> = Vec::new();
    let mut tfs_new: Vec<TagFilter> = Vec::with_capacity(tfs.len() + 1);
    for tf in tfs {
        if !tf.prefix().starts_with(prefix) {
            tfs_new.push((*tf).clone());
            continue;
        }
        let suffix = &tf.prefix()[prefix.len()..];
        tag_key.clear();
        unmarshal_tag_value(&mut tag_key, suffix).unwrap_or_else(|err| {
            panic!("BUG: cannot unmarshal tag key from suffix={suffix:?}: {err}")
        });
        if tag_key.is_empty() || tag_key[0] != COMPOSITE_TAG_KEY_PREFIX {
            tfs_new.push((*tf).clone());
            continue;
        }
        let tag_key_tail = &tag_key[1..];
        let (name_len, n_size) = unmarshal_var_uint64(tag_key_tail).unwrap_or_else(|| {
            panic!("BUG: cannot unmarshal nameLen from tagKey {tag_key_tail:?}")
        });
        let tag_key_tail = &tag_key_tail[n_size..];
        assert!(name_len > 0, "BUG: nameLen must be greater than 0");
        assert!(
            tag_key_tail.len() as u64 >= name_len,
            "BUG: expecting at least {name_len} bytes for the name in tagKey; got {} bytes",
            tag_key_tail.len()
        );
        name.clear();
        name.extend_from_slice(&tag_key_tail[..name_len as usize]);
        let plain_key = &tag_key_tail[name_len as usize..];
        let mut tf_new = TagFilter::default();
        tf_new
            .init(
                prefix,
                plain_key,
                tf.value(),
                tf.is_negative(),
                tf.is_regexp(),
            )
            .unwrap_or_else(|err| {
                panic!(
                    "BUG: cannot initialize {{{plain_key:?}={:?}}} filter: {err}",
                    tf.value()
                )
            });
        tfs_new.push(tf_new);
    }
    if !name.is_empty() {
        let mut tf_new = TagFilter::default();
        tf_new
            .init(prefix, &[], &name, false, false)
            .unwrap_or_else(|err| {
                panic!(
                    "BUG: unexpected error when initializing {{__name__={name:?}}} filter: {err}"
                )
            });
        tfs_new.push(tf_new);
    }
    tfs_new
}

/// Matches the given metric name against all the filters in `tfs`
/// (composite filters must be unfolded with `remove_composite_tag_filters`
/// first). A failed filter is moved to `tfs[0]` as a cheap MRU heuristic.
/// Go: matchTagFilters.
pub fn match_tag_filters(
    mn: &MetricName,
    tfs: &mut [TagFilter],
    kb: &mut Vec<u8>,
) -> Result<bool, String> {
    kb.clear();
    marshal_common_prefix(kb, NS_PREFIX_TAG_TO_METRIC_IDS);
    let prefix_len = kb.len();
    for i in 0..tfs.len() {
        enum Outcome {
            Matched,
            Failed,
        }
        let outcome = {
            let tf = &tfs[i];
            if tf.key() == GRAPHITE_REVERSE_TAG_KEY {
                // Skip artificial tag filters for Graphite-like metric names
                // with dots, since mn doesn't contain the corresponding tag.
                continue;
            }
            if tf.key().is_empty() || tf.key() == b"__graphite__" {
                // Match against mn.metric_group.
                kb.truncate(prefix_len);
                marshal_tag_value(kb, &[]);
                marshal_tag_value(kb, &mn.metric_group);
                let ok = tf.matches(kb).map_err(|err| {
                    format!(
                        "cannot match MetricGroup {:?} with tagFilter {tf}: {err}",
                        mn.metric_group
                    )
                })?;
                if ok {
                    Outcome::Matched
                } else {
                    Outcome::Failed
                }
            } else {
                // Search for the matching tag name.
                let mut tag_seen = false;
                let mut tag_matched = false;
                let mut failed = false;
                for tag in &mn.tags {
                    if tag.key != tf.key() {
                        continue;
                    }
                    // Found the matching tag name. Match the value.
                    tag_seen = true;
                    kb.truncate(prefix_len);
                    tag.marshal(kb);
                    let ok = tf.matches(kb).map_err(|err| {
                        format!("cannot match tag {tag:?} with tagFilter {tf}: {err}")
                    })?;
                    if !ok {
                        failed = true;
                    } else {
                        tag_matched = true;
                    }
                    break;
                }
                if failed {
                    Outcome::Failed
                } else if !tag_seen
                    && (!tf.is_negative() && tf.is_empty_match()
                        || tf.is_negative() && !tf.is_empty_match())
                {
                    // tf contains a positive empty-match filter for a
                    // non-existing tag key, i.e. {foo=~"bar|"}, OR tf
                    // contains a negative filter for a non-existing tag key
                    // that doesn't match an empty string, i.e.
                    // {non_existing_tag_key!="foobar"}. Such a filter
                    // matches anything.
                    //
                    // Note that the filter `{non_existing_tag_key!~"|foo"}`
                    // shouldn't match anything, since it is expected to
                    // match a non-empty `non_existing_tag_key`.
                    Outcome::Matched
                } else if tag_matched {
                    // tf matches mn. Go to the next tf.
                    Outcome::Matched
                } else {
                    // The matching tag name wasn't found.
                    Outcome::Failed
                }
            }
        };
        if let Outcome::Failed = outcome {
            // Move the failed tf to the start. This should reduce the
            // amount of useless work for the next mn.
            if i > 0 {
                tfs.swap(0, i);
            }
            return Ok(false);
        }
    }
    Ok(true)
}
