//! Index entry creation for new time series: `createGlobalIndexes` /
//! `createPerDayIndexes` plus TSID generation (`generateTSID`,
//! `generateUniqueMetricID`) from `index_db.go`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use esm_encoding::marshal_uint64;
use esm_mergeset::Table;
use xxhash_rust::xxh64::xxh64;

use crate::metric_name::{marshal_tag_value, MetricName, KV_SEPARATOR_CHAR};
use crate::tsid::Tsid;

use super::{
    marshal_common_prefix, marshal_composite_tag_key, reverse_bytes, IndexDb,
    GRAPHITE_REVERSE_TAG_KEY, NS_PREFIX_DATE_METRIC_NAME_TO_TSID, NS_PREFIX_DATE_TAG_TO_METRIC_IDS,
    NS_PREFIX_DATE_TO_METRIC_ID, NS_PREFIX_METRIC_ID_TO_METRIC_NAME, NS_PREFIX_METRIC_ID_TO_TSID,
    NS_PREFIX_METRIC_NAME_TO_TSID, NS_PREFIX_TAG_TO_METRIC_IDS,
};

/// Returns a locally unique MetricID.
///
/// The counter is seeded with the current UnixNano at startup, so it must
/// not go backwards across restarts (do not change the server time between
/// restarts). Go: generateUniqueMetricID.
pub fn generate_unique_metric_id() -> u64 {
    // It is expected that metricIDs returned from this function are dense;
    // sparse metricIDs would hurt metric_ids intersection performance with
    // uint64set::Set.
    static NEXT_UNIQUE_METRIC_ID: OnceLock<AtomicU64> = OnceLock::new();
    NEXT_UNIQUE_METRIC_ID
        .get_or_init(|| {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            AtomicU64::new(nanos)
        })
        .fetch_add(1, Ordering::Relaxed)
        + 1
}

/// Fills `dst` with a new TSID for the given metric name.
///
/// `mn` must be canonicalized with [`MetricName::sort_tags`] first: the
/// job-like tag is assumed at `tags[0]` and the instance-like tag at
/// `tags[1]`, which groups data blocks for the same (job, instance) close to
/// each other on disk. Go: generateTSID.
pub fn generate_tsid(dst: &mut Tsid, mn: &MetricName) {
    dst.metric_group_id = xxh64(&mn.metric_group, 0);
    if !mn.tags.is_empty() {
        dst.job_id = xxh64(&mn.tags[0].value, 0) as u32;
    }
    if mn.tags.len() > 1 {
        dst.instance_id = xxh64(&mn.tags[1].value, 0) as u32;
    }
    dst.metric_id = generate_unique_metric_id();
}

/// A batch of index items sharing one backing buffer. Go: indexItems.
#[derive(Default)]
pub(crate) struct IndexItems {
    pub b: Vec<u8>,
    offsets: Vec<(usize, usize)>,
    start: usize,
}

impl IndexItems {
    /// Finalizes the current item. Go: indexItems.Next.
    pub fn next(&mut self) {
        self.offsets.push((self.start, self.b.len()));
        self.start = self.b.len();
    }

    /// Adds all the items to the mergeset table.
    pub fn add_to(&self, tb: &Table) {
        let items: Vec<&[u8]> = self
            .offsets
            .iter()
            .map(|&(start, end)| &self.b[start..end])
            .collect();
        tb.add_items(&items);
    }
}

impl IndexDb {
    /// Creates the global index entries for a new time series:
    /// metricID->metricName (ns3), metricID->TSID (ns2) and the global
    /// tag->metricIDs rows (ns1) including composite rows (and the ns0
    /// metricName->TSID row when the per-day index is disabled).
    ///
    /// `mn` must be sorted with [`MetricName::sort_tags`] before the call.
    /// Go: indexDB.createGlobalIndexes.
    pub fn create_global_indexes(&self, tsid: &Tsid, mn: &MetricName) {
        assert!(
            !self.no_register_new_series(),
            "BUG: registration of new series is disabled for indexDB {:?}",
            self.name()
        );

        // Add the new metricID to the cache.
        self.metric_id_cache.set(tsid.metric_id);

        let mut ii = IndexItems::default();

        if self.ctx.disable_per_day_index {
            // Create the metricName -> TSID entry. This index is used for
            // searching a TSID by metric name during data ingestion when the
            // per-day index is disabled.
            marshal_common_prefix(&mut ii.b, NS_PREFIX_METRIC_NAME_TO_TSID);
            mn.marshal(&mut ii.b);
            ii.b.push(KV_SEPARATOR_CHAR);
            tsid.marshal(&mut ii.b);
            ii.next();
        }

        // Create the metricID -> metricName entry.
        marshal_common_prefix(&mut ii.b, NS_PREFIX_METRIC_ID_TO_METRIC_NAME);
        marshal_uint64(&mut ii.b, tsid.metric_id);
        mn.marshal(&mut ii.b);
        ii.next();

        // Create the metricID -> TSID entry.
        marshal_common_prefix(&mut ii.b, NS_PREFIX_METRIC_ID_TO_TSID);
        marshal_uint64(&mut ii.b, tsid.metric_id);
        tsid.marshal(&mut ii.b);
        ii.next();

        // Create tag -> metricID entries for every tag in mn.
        let mut prefix = Vec::with_capacity(1);
        marshal_common_prefix(&mut prefix, NS_PREFIX_TAG_TO_METRIC_IDS);
        ii.register_tag_indexes(&prefix, mn, tsid.metric_id);

        ii.add_to(self.tb());
    }

    /// Creates the per-day index entries for the given (date, series):
    /// date->metricID (ns5), (date,metricName)->TSID (ns7) and the per-day
    /// tag->metricIDs rows (ns6). No-op when the per-day index is disabled.
    ///
    /// `mn` must be sorted with [`MetricName::sort_tags`] before the call.
    /// Go: indexDB.createPerDayIndexes.
    pub fn create_per_day_indexes(&self, date: u64, tsid: &Tsid, mn: &MetricName) {
        assert!(
            !self.no_register_new_series(),
            "BUG: registration of new series is disabled for indexDB {:?}",
            self.name()
        );

        if self.ctx.disable_per_day_index {
            return;
        }

        self.date_metric_id_cache.set(date, tsid.metric_id);

        let mut ii = IndexItems::default();

        // Create the date -> metricID entry.
        marshal_common_prefix(&mut ii.b, NS_PREFIX_DATE_TO_METRIC_ID);
        marshal_uint64(&mut ii.b, date);
        marshal_uint64(&mut ii.b, tsid.metric_id);
        ii.next();

        // Create the (date, metricName) -> TSID entry.
        marshal_common_prefix(&mut ii.b, NS_PREFIX_DATE_METRIC_NAME_TO_TSID);
        marshal_uint64(&mut ii.b, date);
        mn.marshal(&mut ii.b);
        ii.b.push(KV_SEPARATOR_CHAR);
        tsid.marshal(&mut ii.b);
        ii.next();

        // Create per-day tag -> metricID entries for every tag in mn.
        let mut prefix = Vec::with_capacity(9);
        marshal_common_prefix(&mut prefix, NS_PREFIX_DATE_TAG_TO_METRIC_IDS);
        marshal_uint64(&mut prefix, date);
        ii.register_tag_indexes(&prefix, mn, tsid.metric_id);

        ii.add_to(self.tb());
    }
}

impl IndexItems {
    /// Emits the tag->metricIDs rows for `mn` under the given row prefix:
    /// the MetricGroup row (empty key), one row per tag, plus composite
    /// (MetricGroup+tag) rows. Go: indexItems.registerTagIndexes.
    pub fn register_tag_indexes(&mut self, prefix: &[u8], mn: &MetricName, metric_id: u64) {
        // Add the MetricGroup -> metricID entry.
        self.b.extend_from_slice(prefix);
        marshal_tag_value(&mut self.b, &[]);
        marshal_tag_value(&mut self.b, &mn.metric_group);
        marshal_uint64(&mut self.b, metric_id);
        self.next();
        self.add_reverse_metric_group_if_needed(prefix, mn, metric_id);

        // Add tag -> metricID entries.
        for tag in &mn.tags {
            self.b.extend_from_slice(prefix);
            tag.marshal(&mut self.b);
            marshal_uint64(&mut self.b, metric_id);
            self.next();
        }

        // Add index entries for composite tags: MetricGroup+tag -> metricID.
        let mut composite_key = Vec::new();
        for tag in &mn.tags {
            composite_key.clear();
            marshal_composite_tag_key(&mut composite_key, &mn.metric_group, &tag.key);
            self.b.extend_from_slice(prefix);
            marshal_tag_value(&mut self.b, &composite_key);
            marshal_tag_value(&mut self.b, &tag.value);
            marshal_uint64(&mut self.b, metric_id);
            self.next();
        }
    }

    /// Emits the reverse-MetricGroup row (key 0xff) for Graphite-like metric
    /// names containing dots. Go: indexItems.addReverseMetricGroupIfNeeded.
    fn add_reverse_metric_group_if_needed(
        &mut self,
        prefix: &[u8],
        mn: &MetricName,
        metric_id: u64,
    ) {
        if !mn.metric_group.contains(&b'.') {
            // The reverse metric group is only needed for Graphite-like
            // metrics with points.
            return;
        }
        // This is most likely a Graphite metric like 'foo.bar.baz'.
        // Store the reverse metric name 'zab.rab.oof' in order to speed up
        // the search for '*.bar.baz' when the Graphite wildcard has a suffix
        // matching a small number of time series.
        self.b.extend_from_slice(prefix);
        marshal_tag_value(&mut self.b, GRAPHITE_REVERSE_TAG_KEY);
        let mut rev = Vec::with_capacity(mn.metric_group.len());
        reverse_bytes(&mut rev, &mn.metric_group);
        marshal_tag_value(&mut self.b, &rev);
        marshal_uint64(&mut self.b, metric_id);
        self.next();
    }
}
