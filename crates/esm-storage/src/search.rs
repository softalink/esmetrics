//! Stage 5: port of the upstream VictoriaMetrics v1.146.0 lib/storage/search.go and
//! metric_name_search.go — the block-level [`Search`] iterator plus a
//! higher-level per-series read API ([`Search::next_series`]) that the
//! esm-select layer builds a `MetricsProvider` on: per series it yields the
//! unmarshaled [`MetricName`] and the merged/trimmed/deduplicated
//! `(timestamps, values)` arrays.

use std::sync::Arc;

use esm_common::decimal;

use crate::block::Block;
use crate::index::{SearchError, TagFilters};
use crate::metric_name::MetricName;
use crate::part::BlockRef;
use crate::storage::Storage;
use crate::table::PartitionWrapper;
use crate::table_search::TableSearch;
use crate::time_range::TimeRange;

/// Pace-limiter mask for slow iterations (search.go).
const PACE_LIMITER_SLOW_ITERATIONS_MASK: usize = (1 << 12) - 1;

/// Searches the metric name of a metricID in the partition indexDBs
/// overlapping the search time range, memoized in the shared
/// metricID->metricName cache. Go: metricNameSearch.
pub(crate) struct MetricNameSearch {
    ptws: Vec<Arc<PartitionWrapper>>,
}

impl MetricNameSearch {
    pub fn new(storage: &Storage, tr: TimeRange) -> MetricNameSearch {
        MetricNameSearch {
            ptws: storage.inner().tb.get_partitions(tr),
        }
    }

    /// Appends the metric name of the given metricID to `dst`.
    /// Go: metricNameSearch.search.
    pub fn search(&self, mut dst: Vec<u8>, metric_id: u64) -> (Vec<u8>, bool) {
        // Fast path: the shared metricID->metricName cache. It is consulted
        // (and populated) inside IndexDb::search_metric_name via the shared
        // IndexDbContext.
        //
        // This is just one idb most of the time, since a typical time range
        // fits within a single month.
        if let Some(ptw) = self.ptws.first() {
            let ctx = ptw.pt().idb().context();
            if ctx.get_metric_name_by_metric_id_from_cache(&mut dst, metric_id) {
                return (dst, true);
            }
        }
        for ptw in &self.ptws {
            let (found_dst, found) = ptw.pt().idb().search_metric_name(dst, metric_id, false);
            dst = found_dst;
            if found {
                ptw.pt()
                    .idb()
                    .context()
                    .put_metric_name_by_metric_id_to_cache(metric_id, &dst);
                return (dst, true);
            }
        }
        // Not deleting the metricID when no metric name has been found,
        // because it is not known which indexDB the metricID belongs to
        // (see indexDB.SearchTSIDs / SearchMetricNames for the self-healing
        // path).
        (dst, false)
    }
}

/// A search for time series data blocks. Go: Search.
///
/// Use [`Storage::search`] to create one. [`Search::next_metric_block`]
/// iterates `(metric name, block ref)` pairs ordered by
/// `(TSID, MinTimestamp)`; [`Search::next_series`] is the higher-level
/// per-series API.
pub struct Search<'a> {
    mns: MetricNameSearch,

    /// Used for filtering out blocks fully outside the configured retention.
    retention_deadline: i64,

    ts: TableSearch,

    /// The (original) time range used in the search; `next_series` trims
    /// samples to it.
    tr: TimeRange,

    /// The deadline in unix seconds for the search.
    deadline: u64,

    err: Option<SearchError>,
    eof: bool,

    loops: usize,
    prev_metric_id: u64,

    /// The marshaled canonical metric name of the current block.
    metric_name: Vec<u8>,

    /// Set when `next_series` has already consumed a block belonging to the
    /// next series via `next_metric_block`.
    have_pending_block: bool,

    /// Scratch block for `next_series`.
    block: Block,

    _lifetime: std::marker::PhantomData<&'a Storage>,
}

/// The result buffer for [`Search::next_series`]: one complete series with
/// its samples merged across blocks/parts/partitions, trimmed to the search
/// time range and deduplicated.
#[derive(Default)]
pub struct SeriesBlock {
    /// The unmarshaled metric name of the series.
    pub metric_name: MetricName,
    /// Sample timestamps (milliseconds), ascending.
    pub timestamps: Vec<i64>,
    /// Sample values.
    pub values: Vec<f64>,
}

impl Storage {
    /// Starts a search for the time series matching the given tag filters
    /// within the given time range. Go: Search.Init.
    ///
    /// The search holds references to the storage parts until dropped.
    pub fn search(
        &self,
        tfss: &[TagFilters],
        tr: TimeRange,
        max_metrics: usize,
        deadline: u64,
    ) -> Result<Search<'_>, SearchError> {
        let retention_deadline =
            esm_common::fasttime::unix_timestamp() as i64 * 1000 - self.retention_msecs();

        let mns = MetricNameSearch::new(self, tr);
        let tsids = self.search_tsids(tfss, tr, max_metrics, deadline)?;
        let ts = TableSearch::new(&self.inner().tb, tsids, tr);

        Ok(Search {
            mns,
            retention_deadline,
            ts,
            tr,
            deadline,
            err: None,
            eof: false,
            loops: 0,
            prev_metric_id: 0,
            metric_name: Vec::new(),
            have_pending_block: false,
            block: Block::default(),
            _lifetime: std::marker::PhantomData,
        })
    }
}

impl Search<'_> {
    /// Proceeds to the next metric block. Go: Search.NextMetricBlock.
    pub fn next_metric_block(&mut self) -> bool {
        if self.err.is_some() || self.eof {
            return false;
        }
        while self.ts.next_block() {
            if self.loops & PACE_LIMITER_SLOW_ITERATIONS_MASK == 0
                && esm_common::fasttime::unix_timestamp() > self.deadline
            {
                self.err = Some(SearchError::DeadlineExceeded);
                return false;
            }
            self.loops += 1;
            let bh = *self.ts.block_ref().header();
            if bh.tsid.metric_id != self.prev_metric_id {
                if bh.max_timestamp < self.retention_deadline {
                    // Skip the block, since it contains only data outside
                    // the configured retention.
                    continue;
                }
                self.metric_name.clear();
                let (metric_name, ok) = self
                    .mns
                    .search(std::mem::take(&mut self.metric_name), bh.tsid.metric_id);
                self.metric_name = metric_name;
                if !ok {
                    // Skip missing metricName for the metricID. It should be
                    // automatically fixed — see the self-healing path in
                    // IndexDb::search_tsids.
                    continue;
                }
                self.prev_metric_id = bh.tsid.metric_id;
            }
            return true;
        }
        if let Some(err) = self.ts.error() {
            self.err = Some(SearchError::Other(err));
            return false;
        }
        self.eof = true;
        false
    }

    /// The marshaled canonical metric name of the current block.
    pub fn metric_name(&self) -> &[u8] {
        &self.metric_name
    }

    /// The reference to the current block.
    pub fn block_ref(&self) -> &BlockRef {
        self.ts.block_ref()
    }

    /// Returns the search error, if any. Go: Search.Error.
    pub fn error(&self) -> Option<&SearchError> {
        self.err.as_ref()
    }

    /// Reads the next series into `dst`: all the blocks of one TSID are
    /// merged, the samples are sorted by timestamp with duplicate timestamps
    /// collapsed, trimmed to the search time range and deduplicated with the
    /// global dedup interval (spec §7 "dedup on read"). Returns `Ok(false)`
    /// when there are no more series.
    pub fn next_series(&mut self, dst: &mut SeriesBlock) -> Result<bool, SearchError> {
        if !self.have_pending_block && !self.next_metric_block() {
            return match self.err.clone() {
                Some(err) => Err(err),
                None => Ok(false),
            };
        }
        self.have_pending_block = false;

        let cur_metric_id = self.ts.block_ref().header().tsid.metric_id;
        dst.metric_name
            .unmarshal(&self.metric_name)
            .map_err(|err| SearchError::Other(format!("cannot unmarshal metricName: {err}")))?;

        // Collect the samples of all the blocks of the current series.
        let mut samples: Vec<(i64, f64)> = Vec::new();
        loop {
            self.read_current_block_samples(&mut samples)?;

            if !self.next_metric_block() {
                if let Some(err) = self.err.clone() {
                    return Err(err);
                }
                break;
            }
            if self.ts.block_ref().header().tsid.metric_id != cur_metric_id {
                // The block belongs to the next series; keep it pending.
                self.have_pending_block = true;
                break;
            }
        }

        // Merge the (possibly overlapping) blocks: sort by timestamp and
        // collapse duplicate timestamps, keeping the first occurrence in the
        // (TSID, MinTimestamp) block order — this mirrors vmselect's
        // mergeSortBlocks behavior.
        samples.sort_by_key(|(ts, _)| *ts);
        dst.timestamps.clear();
        dst.values.clear();
        for (ts, v) in samples {
            if dst.timestamps.last() == Some(&ts) {
                continue;
            }
            dst.timestamps.push(ts);
            dst.values.push(v);
        }

        // Query-time deduplication (spec §7).
        let dedup_interval = crate::dedup::get_dedup_interval();
        if dedup_interval > 0 {
            crate::dedup::deduplicate_samples(&mut dst.timestamps, &mut dst.values, dedup_interval);
        }
        Ok(true)
    }

    /// Reads the current block, converting the decimal values to f64 and
    /// trimming the samples to the search time range (Go: MustReadBlock +
    /// Block.AppendRowsWithTimeRangeFilter on the caller side).
    fn read_current_block_samples(
        &mut self,
        samples: &mut Vec<(i64, f64)>,
    ) -> Result<(), SearchError> {
        self.ts
            .block_ref()
            .read_block(&mut self.block)
            .map_err(SearchError::Other)?;
        let scale = self.block.header().scale;
        let mut values = Vec::with_capacity(self.block.values().len());
        decimal::append_decimal_to_float(&mut values, self.block.values(), scale);
        for (&ts, v) in self.block.timestamps().iter().zip(values) {
            if ts < self.tr.min_timestamp || ts > self.tr.max_timestamp {
                continue;
            }
            samples.push((ts, v));
        }
        Ok(())
    }
}
