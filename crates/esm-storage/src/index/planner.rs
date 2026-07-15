//! Port of the tag-filter planner from `index_db.go`:
//! `getMetricIDsForDateAndFilters` with loops-count weighting, per-filter
//! seek-vs-scan strategies, negative-filter subtraction and the postponed
//! metric-name matching fallback.

use esm_common::uint64set::Set;
use esm_encoding::{marshal_int64, marshal_uint64, unmarshal_int64, unmarshal_uint64};

use crate::metric_name::{marshal_tag_value, MetricName, TAG_SEPARATOR_CHAR};

use super::row_merge::{find_tag_separator, MAX_METRIC_IDS_PER_ROW};
use super::search::{
    check_search_deadline_and_pace, marshal_common_prefix_for_date, match_tag_filters,
    remove_composite_tag_filters, IndexSearch, PACE_LIMITER_FAST_ITERATIONS_MASK,
    PACE_LIMITER_MEDIUM_ITERATIONS_MASK, PACE_LIMITER_SLOW_ITERATIONS_MASK,
};
use super::tag_filters::{TagFilter, TagFilters};
use super::{marshal_common_prefix, SearchError, NS_PREFIX_TAG_TO_METRIC_IDS};

/// The estimated number of index scan loops a single loop in
/// `update_metric_ids_by_metric_name_match` takes.
/// Go: loopsCountPerMetricNameMatch.
const LOOPS_COUNT_PER_METRIC_NAME_MATCH: i64 = 150;

impl IndexSearch<'_> {
    /// The core tag-filter planner: returns the metricIDs matching all the
    /// filters in `tfs` on the given date (0 = the global index), ordering
    /// the filters by their loops-count stats collected from previous
    /// queries. Go: indexSearch.getMetricIDsForDateAndFilters.
    pub(crate) fn get_metric_ids_for_date_and_filters(
        &mut self,
        date: u64,
        tfs: &TagFilters,
        max_metrics: usize,
    ) -> Result<Set, SearchError> {
        struct TagFilterWithWeight<'t> {
            tf: &'t TagFilter,
            loops_count: i64,
            filter_loops_count: i64,
        }

        // Sort tfs by the loopsCount needed for performing each filter.
        // These stats are usually collected from the previous queries.
        // This way we limit the amount of work below by applying the fast
        // filters first.
        let current_time = esm_common::fasttime::unix_timestamp();
        let mut tfws: Vec<TagFilterWithWeight<'_>> = Vec::with_capacity(tfs.filters().len());
        for tf in tfs.filters() {
            let (mut loops_count, mut filter_loops_count, timestamp) =
                self.get_loops_count_and_timestamp_for_date_filter(date, tf);
            if current_time > timestamp + 3600 {
                // Update the stats once per hour for relatively fast tag
                // filters. There is no need in spending CPU resources on
                // updating the stats for heavy tag filters.
                if loops_count <= 10_000_000 {
                    loops_count = 0;
                }
                if filter_loops_count <= 10_000_000 {
                    filter_loops_count = 0;
                }
            }
            tfws.push(TagFilterWithWeight {
                tf,
                loops_count,
                filter_loops_count,
            });
        }
        tfws.sort_by(|a, b| {
            a.loops_count
                .cmp(&b.loops_count)
                .then_with(|| match a.tf.less(b.tf) {
                    true => std::cmp::Ordering::Less,
                    false => std::cmp::Ordering::Greater,
                })
        });
        fn get_first_positive_loops_count(tfws: &[TagFilterWithWeight<'_>]) -> i64 {
            for tfw in tfws {
                if tfw.loops_count > 0 {
                    return tfw.loops_count;
                }
            }
            i64::MAX
        }
        fn get_first_positive_filter_loops_count(tfws: &[TagFilterWithWeight<'_>]) -> i64 {
            for tfw in tfws {
                if tfw.filter_loops_count > 0 {
                    return tfw.filter_loops_count;
                }
            }
            i64::MAX
        }

        // Populate metricIDs for the first non-negative filter with the
        // smallest cost.
        let mut metric_ids: Option<Set> = None;
        let mut tfws_remaining: Vec<TagFilterWithWeight<'_>> = Vec::with_capacity(tfws.len());
        let max_date_metrics = if max_metrics < usize::MAX / 50 {
            max_metrics * 50
        } else {
            usize::MAX
        };
        for i in 0..tfws.len() {
            let tf = tfws[i].tf;
            if tf.is_negative() || tf.is_empty_match() {
                let tfw = TagFilterWithWeight {
                    tf,
                    loops_count: tfws[i].loops_count,
                    filter_loops_count: tfws[i].filter_loops_count,
                };
                tfws_remaining.push(tfw);
                continue;
            }
            let max_loops_count = get_first_positive_loops_count(&tfws[i + 1..]);
            let (loops_count, res) = self.get_metric_ids_for_date_tag_filter(
                tf,
                date,
                &tfs.common_prefix,
                max_date_metrics,
                max_loops_count,
            );
            match res {
                Err(SearchError::TooManyLoops) => {
                    // The tf took too many loops compared to the next filter.
                    // Postpone applying this filter.
                    let new_loops_count = 2 * loops_count;
                    if new_loops_count != tfws[i].loops_count {
                        self.store_loops_count_for_date_filter(
                            date,
                            tf,
                            new_loops_count,
                            tfws[i].filter_loops_count,
                        );
                    }
                    tfws_remaining.push(TagFilterWithWeight {
                        tf,
                        loops_count: new_loops_count,
                        filter_loops_count: tfws[i].filter_loops_count,
                    });
                    continue;
                }
                Err(err) => {
                    // Move the failing filter to the end of the list.
                    self.store_loops_count_for_date_filter(
                        date,
                        tf,
                        i64::MAX,
                        tfws[i].filter_loops_count,
                    );
                    return Err(err);
                }
                Ok(m) => {
                    if m.len() >= max_date_metrics {
                        // Too many time series found by a single tag filter.
                        // Move the filter to the end of the list.
                        self.store_loops_count_for_date_filter(
                            date,
                            tf,
                            i64::MAX - 1,
                            tfws[i].filter_loops_count,
                        );
                        tfws_remaining.push(TagFilterWithWeight {
                            tf,
                            loops_count: i64::MAX - 1,
                            filter_loops_count: tfws[i].filter_loops_count,
                        });
                        continue;
                    }
                    if loops_count != tfws[i].loops_count {
                        self.store_loops_count_for_date_filter(
                            date,
                            tf,
                            loops_count,
                            tfws[i].filter_loops_count,
                        );
                    }
                    metric_ids = Some(m);
                    for tfw in tfws.drain(i + 1..) {
                        tfws_remaining.push(tfw);
                    }
                    break;
                }
            }
        }
        let mut tfws = tfws_remaining;

        let mut metric_ids = match metric_ids {
            Some(m) => m,
            None => {
                // All the filters in tfs are negative or match too many time
                // series. Populate all the metricIDs for the given date, so
                // they can be filtered out with the negative filters later.
                let m = self
                    .get_metric_ids_for_date(date, max_date_metrics)
                    .map_err(|err| match err {
                        SearchError::Other(msg) => {
                            SearchError::Other(format!("cannot obtain all the metricIDs: {msg}"))
                        }
                        other => other,
                    })?;
                if m.len() >= max_date_metrics {
                    // Too many time series found for the given date.
                    return Err(SearchError::TooManyTimeseries(max_date_metrics));
                }
                m
            }
        };

        tfws.sort_by(|a, b| {
            a.filter_loops_count
                .cmp(&b.filter_loops_count)
                .then_with(|| match a.tf.less(b.tf) {
                    true => std::cmp::Ordering::Less,
                    false => std::cmp::Ordering::Greater,
                })
        });

        // Intersect the metricIDs with the rest of the filters.
        //
        // Do not run these tag filters in parallel, since this may result in
        // CPU and RAM waste when the initial tag filters significantly
        // reduce the number of found metricIDs, so the remaining filters
        // could be performed via the much faster metric-name matching.
        let mut tfs_postponed: Vec<&TagFilter> = Vec::new();
        for i in 0..tfws.len() {
            let tf = tfws[i].tf;
            let metric_ids_len = metric_ids.len();
            if metric_ids_len == 0 {
                // There is no need in applying the remaining filters to an
                // empty set.
                break;
            }
            if tfws[i].filter_loops_count
                > metric_ids_len as i64 * LOOPS_COUNT_PER_METRIC_NAME_MATCH
            {
                // It should be faster performing the metric-name match on
                // the remaining filters instead of scanning a big number of
                // entries in the inverted index for these filters.
                for tfw in &tfws[i..] {
                    tfs_postponed.push(tfw.tf);
                }
                break;
            }
            let mut max_loops_count = get_first_positive_filter_loops_count(&tfws[i + 1..]);
            if max_loops_count == i64::MAX {
                max_loops_count = metric_ids_len as i64 * LOOPS_COUNT_PER_METRIC_NAME_MATCH;
            }
            let (filter_loops_count, res) = self.get_metric_ids_for_date_tag_filter(
                tf,
                date,
                &tfs.common_prefix,
                usize::MAX,
                max_loops_count,
            );
            match res {
                Err(SearchError::TooManyLoops) => {
                    // Postpone tf, since it took more loops than the next
                    // filter may need.
                    let new_flc = 2 * filter_loops_count;
                    if new_flc != tfws[i].filter_loops_count {
                        self.store_loops_count_for_date_filter(
                            date,
                            tf,
                            tfws[i].loops_count,
                            new_flc,
                        );
                    }
                    tfs_postponed.push(tf);
                    continue;
                }
                Err(err) => {
                    // Move the failing tf to the end of the filter list.
                    self.store_loops_count_for_date_filter(date, tf, tfws[i].loops_count, i64::MAX);
                    return Err(err);
                }
                Ok(m) => {
                    if filter_loops_count != tfws[i].filter_loops_count {
                        self.store_loops_count_for_date_filter(
                            date,
                            tf,
                            tfws[i].loops_count,
                            filter_loops_count,
                        );
                    }
                    if tf.is_negative() || tf.is_empty_match() {
                        metric_ids.subtract(&m);
                    } else {
                        metric_ids.intersect(&m);
                    }
                }
            }
        }
        if metric_ids.is_empty() {
            // There is no need in applying tfs_postponed, since the result
            // is empty.
            return Ok(Set::default());
        }
        if !tfs_postponed.is_empty() {
            // Apply the postponed filters via the metric-name match.
            let mut m = Set::default();
            self.update_metric_ids_by_metric_name_match(&mut m, &mut metric_ids, &tfs_postponed)?;
            return Ok(m);
        }
        Ok(metric_ids)
    }

    /// Runs one tag filter against the per-day (or global, date == 0) index
    /// with a rebased prefix. Returns (loopsCount, result); for positive
    /// empty-match filters the result is the complement within `key=~".+"`.
    /// Go: indexSearch.getMetricIDsForDateTagFilter.
    fn get_metric_ids_for_date_tag_filter(
        &mut self,
        tf: &TagFilter,
        date: u64,
        common_prefix: &[u8],
        max_metrics: usize,
        max_loops_count: i64,
    ) -> (i64, Result<Set, SearchError>) {
        assert!(
            tf.prefix().starts_with(common_prefix),
            "BUG: unexpected tf.prefix {:?}; must start with common_prefix {common_prefix:?}",
            tf.prefix()
        );

        let mut prefix = Vec::new();
        marshal_common_prefix_for_date(&mut prefix, date);
        let date_prefix_len = prefix.len();
        prefix.extend_from_slice(&tf.prefix()[common_prefix.len()..]);
        let mut tf_new = tf.clone();
        // is_negative for the original tf is handled by the caller.
        tf_new.is_negative = false;
        tf_new.prefix = prefix.clone();
        let (metric_ids, loops_count, err) = {
            let (lc, res) =
                self.get_metric_ids_for_tag_filter(&tf_new, max_metrics, max_loops_count);
            match res {
                Ok(m) => (m, lc, None),
                Err(e) => (Set::default(), lc, Some(e)),
            }
        };
        if let Some(err) = err {
            return (loops_count, Err(err));
        }
        if tf.is_negative() || !tf.is_empty_match() {
            return (loops_count, Ok(metric_ids));
        }
        // The tag filter matches empty labels such as {foo=~"bar|"}.
        // Convert it to the negative filter, which matches
        // {foo=~".+", foo!~"bar|"}, i.e. return
        // metricIDs(key=~".+") − metricIDs(tf).
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1601
        let max_loops_count = max_loops_count - loops_count;
        prefix.truncate(date_prefix_len);
        let mut tf_gross = TagFilter::default();
        if let Err(err) = tf_gross.init(&prefix, tf.key(), b".+", false, true) {
            panic!(
                "BUG: cannot init tag filter: {{{:?}=~\".+\"}}: {err}",
                tf.key()
            );
        }
        let (lc, res) = self.get_metric_ids_for_tag_filter(&tf_gross, max_metrics, max_loops_count);
        let loops_count = loops_count + lc;
        match res {
            Err(err) => (loops_count, Err(err)),
            Ok(mut m) => {
                m.subtract(&metric_ids);
                (loops_count, Ok(m))
            }
        }
    }

    /// Go: indexSearch.getMetricIDsForTagFilter.
    fn get_metric_ids_for_tag_filter(
        &mut self,
        tf: &TagFilter,
        max_metrics: usize,
        max_loops_count: i64,
    ) -> (i64, Result<Set, SearchError>) {
        assert!(!tf.is_negative(), "BUG: isNegative must be false");
        let mut metric_ids = Set::default();
        if !tf.or_suffixes().is_empty() {
            // Fast path for or_suffixes - seek for the rows for each value
            // from or_suffixes.
            let (loops_count, res) = self.update_metric_ids_for_or_suffixes(
                tf,
                &mut metric_ids,
                max_metrics,
                max_loops_count,
            );
            return (loops_count, res.map(|()| metric_ids));
        }
        // Slow path - scan all the rows with the given prefix.
        let (loops_count, res) =
            self.get_metric_ids_for_tag_filter_slow(tf, max_loops_count, &mut metric_ids);
        (loops_count, res.map(|()| metric_ids))
    }

    /// Go: indexSearch.getMetricIDsForTagFilterSlow.
    fn get_metric_ids_for_tag_filter_slow(
        &mut self,
        tf: &TagFilter,
        max_loops_count: i64,
        metric_ids: &mut Set,
    ) -> (i64, Result<(), SearchError>) {
        assert!(
            tf.or_suffixes().is_empty(),
            "BUG: getMetricIDsForTagFilterSlow must be called only for empty or_suffixes"
        );

        // Scan all the rows with tf.prefix and add the metricIDs on every
        // tf match.
        let ts = &mut self.ts;
        let kb = &mut self.kb;
        let mp = &mut self.mp;
        let mut prev_matching_suffix: Vec<u8> = Vec::new();
        let mut suffix_buf: Vec<u8> = Vec::new();
        let mut prev_match = false;
        let mut loops_count: i64 = 0;
        let mut loops_pace_limiter = 0usize;
        let prefix = tf.prefix();
        ts.seek(prefix);
        while ts.next_item() {
            if loops_pace_limiter & PACE_LIMITER_MEDIUM_ITERATIONS_MASK == 0 {
                if let Err(err) = check_search_deadline_and_pace(self.deadline) {
                    return (loops_count, Err(err));
                }
            }
            loops_pace_limiter += 1;
            let item = ts.item();
            if !item.starts_with(prefix) {
                return (loops_count, Ok(()));
            }
            let tail = &item[prefix.len()..];
            let Some(n) = find_tag_separator(tail) else {
                return (
                    loops_count,
                    Err(SearchError::Other(format!(
                        "invalid tag->metricIDs line {item:?}: cannot find tagSeparatorChar={TAG_SEPARATOR_CHAR}"
                    ))),
                );
            };
            suffix_buf.clear();
            suffix_buf.extend_from_slice(&tail[..n + 1]);
            let tail = &tail[n + 1..];
            if let Err(err) = mp.init_only_tail(tail) {
                return (loops_count, Err(SearchError::Other(err)));
            }
            mp.parse_metric_ids();
            loops_count += mp.metric_ids_len() as i64;
            if loops_count > max_loops_count {
                return (loops_count, Err(SearchError::TooManyLoops));
            }
            if prev_match && suffix_buf == prev_matching_suffix {
                // Fast path: the same tag value found. There is no need in
                // checking it again with the potentially slow
                // tf.match_suffix, which may call the regexp.
                metric_ids.add_multi(&mp.metric_ids);
                continue;
            }
            // Slow path: need a tf.match_suffix call.
            let ok = match tf.match_suffix(&suffix_buf) {
                Ok(ok) => ok,
                Err(err) => {
                    return (
                        loops_count,
                        Err(SearchError::Other(format!(
                            "error when matching {tf} against suffix {suffix_buf:?}: {err}"
                        ))),
                    );
                }
            };
            // Assume that a tf.match_suffix call needs 10x more time than a
            // single metric scan iteration.
            loops_count += 10 * tf.match_cost() as i64;
            if !ok {
                prev_match = false;
                if mp.metric_ids_len() < MAX_METRIC_IDS_PER_ROW / 2 {
                    // If the current row contains a non-full metricIDs list,
                    // then it is likely the next row contains the next tag
                    // value. So skip seeking to the next tag value, since it
                    // would be slower than just the ts.next_item() call.
                    continue;
                }
                // Optimization: skip all the metricIDs for the given tag
                // value.
                kb.clear();
                kb.extend_from_slice(&item[..item.len() - tail.len()]);
                // The last char in kb must be tagSeparatorChar.
                // Just increment it in order to jump to the next tag value.
                if kb.is_empty() || kb[kb.len() - 1] != TAG_SEPARATOR_CHAR {
                    return (
                        loops_count,
                        Err(SearchError::Other(format!(
                            "data corruption: the last char in k={kb:X?} must be {TAG_SEPARATOR_CHAR:X}"
                        ))),
                    );
                }
                let last = kb.len() - 1;
                kb[last] += 1;
                ts.seek(kb);
                // Assume that a seek cost is equivalent to 1000 ordinary
                // loops.
                loops_count += 1000;
                continue;
            }
            prev_match = true;
            std::mem::swap(&mut prev_matching_suffix, &mut suffix_buf);
            metric_ids.add_multi(&mp.metric_ids);
        }
        if let Some(err) = ts.error() {
            return (
                loops_count,
                Err(SearchError::Other(format!(
                    "error when searching for tag filter prefix {prefix:?}: {err}"
                ))),
            );
        }
        (loops_count, Ok(()))
    }

    /// Go: indexSearch.updateMetricIDsForOrSuffixes.
    fn update_metric_ids_for_or_suffixes(
        &mut self,
        tf: &TagFilter,
        metric_ids: &mut Set,
        max_metrics: usize,
        max_loops_count: i64,
    ) -> (i64, Result<(), SearchError>) {
        assert!(!tf.is_negative(), "BUG: isNegative must be false");
        let mut kb = Vec::new();
        let mut loops_count: i64 = 0;
        for or_suffix in tf.or_suffixes() {
            kb.clear();
            kb.extend_from_slice(tf.prefix());
            kb.extend_from_slice(or_suffix.as_bytes());
            kb.push(TAG_SEPARATOR_CHAR);
            let (lc, res) = self.update_metric_ids_for_or_suffix(
                &kb,
                metric_ids,
                max_metrics,
                max_loops_count - loops_count,
            );
            loops_count += lc;
            if let Err(err) = res {
                return (loops_count, Err(err));
            }
            if metric_ids.len() >= max_metrics {
                return (loops_count, Ok(()));
            }
        }
        (loops_count, Ok(()))
    }

    /// Go: indexSearch.updateMetricIDsForOrSuffix.
    fn update_metric_ids_for_or_suffix(
        &mut self,
        prefix: &[u8],
        metric_ids: &mut Set,
        max_metrics: usize,
        max_loops_count: i64,
    ) -> (i64, Result<(), SearchError>) {
        let ts = &mut self.ts;
        let mp = &mut self.mp;
        let mut loops_count: i64 = 0;
        let mut loops_pace_limiter = 0usize;
        ts.seek(prefix);
        while metric_ids.len() < max_metrics && ts.next_item() {
            if loops_pace_limiter & PACE_LIMITER_FAST_ITERATIONS_MASK == 0 {
                if let Err(err) = check_search_deadline_and_pace(self.deadline) {
                    return (loops_count, Err(err));
                }
            }
            loops_pace_limiter += 1;
            let item = ts.item();
            if !item.starts_with(prefix) {
                return (loops_count, Ok(()));
            }
            if let Err(err) = mp.init_only_tail(&item[prefix.len()..]) {
                return (loops_count, Err(SearchError::Other(err)));
            }
            loops_count += mp.metric_ids_len() as i64;
            if loops_count > max_loops_count {
                return (loops_count, Err(SearchError::TooManyLoops));
            }
            mp.parse_metric_ids();
            metric_ids.add_multi(&mp.metric_ids);
        }
        if let Some(err) = ts.error() {
            return (
                loops_count,
                Err(SearchError::Other(format!(
                    "error when searching for tag filter prefix {prefix:?}: {err}"
                ))),
            );
        }
        (loops_count, Ok(()))
    }

    /// Extracts all the metricIDs from the `(date, __name__=value)` rows.
    /// Go: indexSearch.getMetricIDsForDate.
    pub fn get_metric_ids_for_date(
        &mut self,
        date: u64,
        max_metrics: usize,
    ) -> Result<Set, SearchError> {
        let mut prefix = Vec::new();
        marshal_common_prefix_for_date(&mut prefix, date);
        marshal_tag_value(&mut prefix, &[]);
        let mut metric_ids = Set::default();
        self.update_metric_ids_for_prefix(&prefix, &mut metric_ids, max_metrics)?;
        Ok(metric_ids)
    }

    /// Go: indexSearch.updateMetricIDsForPrefix.
    fn update_metric_ids_for_prefix(
        &mut self,
        prefix: &[u8],
        metric_ids: &mut Set,
        max_metrics: usize,
    ) -> Result<(), SearchError> {
        let ts = &mut self.ts;
        let mp = &mut self.mp;
        let mut loops_pace_limiter = 0usize;
        ts.seek(prefix);
        while ts.next_item() {
            if loops_pace_limiter & PACE_LIMITER_FAST_ITERATIONS_MASK == 0 {
                check_search_deadline_and_pace(self.deadline)?;
            }
            loops_pace_limiter += 1;
            let item = ts.item();
            if !item.starts_with(prefix) {
                return Ok(());
            }
            let tail = &item[prefix.len()..];
            let Some(n) = find_tag_separator(tail) else {
                return Err(SearchError::Other(format!(
                    "invalid tag->metricIDs line {item:?}: cannot find tagSeparatorChar {TAG_SEPARATOR_CHAR}"
                )));
            };
            let tail = &tail[n + 1..];
            mp.init_only_tail(tail).map_err(SearchError::Other)?;
            mp.parse_metric_ids();
            metric_ids.add_multi(&mp.metric_ids);
            if metric_ids.len() >= max_metrics {
                return Ok(());
            }
        }
        if let Some(err) = ts.error() {
            return Err(SearchError::Other(format!(
                "error when searching for all metricIDs by prefix {prefix:?}: {err}"
            )));
        }
        Ok(())
    }

    // --- loops-count cache ---

    /// Go: indexSearch.getLoopsCountAndTimestampForDateFilter.
    fn get_loops_count_and_timestamp_for_date_filter(
        &mut self,
        date: u64,
        tf: &TagFilter,
    ) -> (i64, i64, u64) {
        self.kb.clear();
        append_date_tag_filter_cache_key(&mut self.kb, self.db.name(), date, tf);
        let Some(v) = self.db.loops_per_date_tag_filter_cache.get(&self.kb) else {
            return (0, 0, 0);
        };
        if v.len() != 3 * 8 {
            return (0, 0, 0);
        }
        let loops_count = unmarshal_int64(&v);
        let filter_loops_count = unmarshal_int64(&v[8..]);
        let timestamp = unmarshal_uint64(&v[16..]);
        (loops_count, filter_loops_count, timestamp)
    }

    /// Go: indexSearch.storeLoopsCountForDateFilter.
    fn store_loops_count_for_date_filter(
        &mut self,
        date: u64,
        tf: &TagFilter,
        loops_count: i64,
        filter_loops_count: i64,
    ) {
        let current_timestamp = esm_common::fasttime::unix_timestamp();
        self.kb.clear();
        append_date_tag_filter_cache_key(&mut self.kb, self.db.name(), date, tf);
        let mut v = Vec::with_capacity(24);
        marshal_int64(&mut v, loops_count);
        marshal_int64(&mut v, filter_loops_count);
        marshal_uint64(&mut v, current_timestamp);
        self.db
            .loops_per_date_tag_filter_cache
            .set(&self.kb, v.into_boxed_slice());
    }

    /// Matches the metric names of `src_metric_ids` against `tfs` and adds
    /// the matching metricIDs to `metric_ids`.
    /// Go: indexSearch.updateMetricIDsByMetricNameMatch.
    fn update_metric_ids_by_metric_name_match(
        &mut self,
        metric_ids: &mut Set,
        src_metric_ids: &mut Set,
        tfs: &[&TagFilter],
    ) -> Result<(), SearchError> {
        // Sort src_metric_ids in order to speed up the index seeks below.
        let sorted_metric_ids = src_metric_ids.append_to(Vec::new());

        let mut prefix = Vec::new();
        marshal_common_prefix(&mut prefix, NS_PREFIX_TAG_TO_METRIC_IDS);
        let mut tfs = remove_composite_tag_filters(tfs, &prefix);

        let mut metric_name: Vec<u8> = Vec::new();
        let mut kb = Vec::new();
        let mut mn = MetricName::default();
        for (loops_pace_limiter, &metric_id) in sorted_metric_ids.iter().enumerate() {
            if loops_pace_limiter & PACE_LIMITER_SLOW_ITERATIONS_MASK == 0 {
                check_search_deadline_and_pace(self.deadline)?;
            }
            metric_name.clear();
            let (name, ok) =
                self.search_metric_name_with_cache(std::mem::take(&mut metric_name), metric_id);
            metric_name = name;
            if !ok {
                // It is likely the metricID->metricName entry didn't
                // propagate to the inverted index yet. Skip this metricID
                // for now.
                continue;
            }
            mn.unmarshal(&metric_name).unwrap_or_else(|err| {
                panic!("FATAL: cannot unmarshal metricName {metric_name:?}: {err}")
            });

            // Match the mn against tfs.
            let ok = match_tag_filters(&mn, &mut tfs, &mut kb).map_err(|err| {
                SearchError::Other(format!(
                    "cannot match MetricName {mn:?} against tagFilters: {err}"
                ))
            })?;
            if !ok {
                continue;
            }
            metric_ids.add(metric_id);
        }
        Ok(())
    }
}

/// Go: appendDateTagFilterCacheKey.
fn append_date_tag_filter_cache_key(
    dst: &mut Vec<u8>,
    index_db_name: &str,
    date: u64,
    tf: &TagFilter,
) {
    dst.extend_from_slice(index_db_name.as_bytes());
    marshal_uint64(dst, date);
    tf.marshal(dst);
}
