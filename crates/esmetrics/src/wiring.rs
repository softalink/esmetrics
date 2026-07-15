//! Adapters connecting the storage engine to the ingestion and query layers.
//!
//! Mirrors the glue in Go `app/victoria-metrics/main.go` +
//! `app/vmselect/netstorage`: [`StorageSink`] feeds `/write` rows into
//! [`esm_storage::Storage`]; [`StorageProvider`] serves the promql
//! evaluator's searches from it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use esm_promql::provider::{Deadline, MetricsProvider, SearchQuery, Series};
use esm_storage::{marshal_metric_name_raw, MetricName, Storage, TagFilters, TimeRange};
use esm_streamaggr::{Aggregators, Label, Sample as AggSample, TimeSeries};

/// Default precision bits for ingested values (Go `-precisionBits` default).
const DEFAULT_PRECISION_BITS: u8 = 64;

/// Optional global stream-aggregation stage for the single-node insert path
/// (`-streamAggr.config`). Aggregated output is written to storage; input
/// consumed by an aggregator is dropped from the direct write path unless
/// `keep_input` is set.
pub struct StreamAggSink {
    pub aggregators: Arc<Aggregators>,
    pub keep_input: bool,
}

/// [`esm_insert::RowSink`] implementation over [`Storage`], optionally
/// aggregating via [`StreamAggSink`] first.
pub struct StorageSink {
    pub storage: Arc<Storage>,
    /// `None` when `-streamAggr.config` is unset (direct-write fast path).
    pub stream_agg: Option<StreamAggSink>,
}

impl esm_insert::RowSink for StorageSink {
    fn add_rows(&self, rows: &[esm_insert::MetricRow<'_>]) -> Result<(), String> {
        let Some(sa) = &self.stream_agg else {
            // Fast path: write straight to storage. esm-insert's MetricRow
            // borrows a batch arena; storage's MetricRowRef borrows too, so
            // the conversion is a per-row struct copy without touching the
            // name bytes.
            write_rows_direct(&self.storage, rows.iter().map(row_ref));
            return Ok(());
        };

        // Group rows sharing a `metric_name_raw` into one aggregator input
        // series; the aggregated output flows to storage via the aggregators'
        // push callback (set in `build_stream_agg`).
        let mut index: HashMap<&[u8], usize> = HashMap::new();
        let mut names: Vec<&[u8]> = Vec::new();
        let mut series: Vec<TimeSeries> = Vec::new();
        for row in rows {
            let sample = AggSample {
                timestamp: row.timestamp,
                value: row.value,
            };
            if let Some(&i) = index.get(row.metric_name_raw) {
                series[i].samples.push(sample);
                continue;
            }
            let Ok(labels) = decode_labels(row.metric_name_raw) else {
                continue;
            };
            index.insert(row.metric_name_raw, series.len());
            names.push(row.metric_name_raw);
            series.push(TimeSeries {
                labels,
                samples: vec![sample],
            });
        }

        let mut match_idxs = Vec::new();
        sa.aggregators.push(&series, &mut match_idxs);

        if sa.keep_input {
            write_rows_direct(&self.storage, rows.iter().map(row_ref));
        } else {
            // Write only rows whose series no aggregator consumed.
            let matched: HashSet<&[u8]> = names
                .iter()
                .zip(match_idxs.iter())
                .filter(|(_, &m)| m == 1)
                .map(|(n, _)| *n)
                .collect();
            write_rows_direct(
                &self.storage,
                rows.iter()
                    .filter(|r| !matched.contains(r.metric_name_raw))
                    .map(row_ref),
            );
        }
        Ok(())
    }
}

fn row_ref<'a>(r: &esm_insert::MetricRow<'a>) -> esm_storage::MetricRowRef<'a> {
    esm_storage::MetricRowRef {
        metric_name_raw: r.metric_name_raw,
        timestamp: r.timestamp,
        value: r.value,
    }
}

fn write_rows_direct<'a>(
    storage: &Storage,
    rows: impl Iterator<Item = esm_storage::MetricRowRef<'a>>,
) {
    let mrs: Vec<esm_storage::MetricRowRef<'_>> = rows.collect();
    if !mrs.is_empty() {
        storage.add_rows_ref(&mrs, DEFAULT_PRECISION_BITS);
    }
}

/// Decodes `metric_name_raw` into a relabel-ready label set (`__name__` +
/// tags), for stream-aggregation input.
fn decode_labels(metric_name_raw: &[u8]) -> Result<Vec<Label>, String> {
    let mut mn = MetricName::default();
    mn.unmarshal_raw(metric_name_raw)?;
    let mut labels = Vec::with_capacity(mn.tags.len() + 1);
    labels.push(Label {
        name: "__name__".to_string(),
        value: String::from_utf8_lossy(&mn.metric_group).into_owned(),
    });
    for tag in &mn.tags {
        labels.push(Label {
            name: String::from_utf8_lossy(&tag.key).into_owned(),
            value: String::from_utf8_lossy(&tag.value).into_owned(),
        });
    }
    Ok(labels)
}

/// Writes the aggregators' output series to `storage` (the stream-agg push
/// callback). Each series' labels are re-marshaled to a raw metric name.
pub fn write_aggregated_to_storage(storage: &Storage, tss: &[TimeSeries]) {
    let mut raws: Vec<Vec<u8>> = Vec::with_capacity(tss.len());
    // (raw index, timestamp, value) tuples, resolved to MetricRowRef after
    // all raws are allocated so the borrows stay valid.
    let mut rows: Vec<(usize, i64, f64)> = Vec::new();
    for ts in tss {
        let mut group: &[u8] = b"";
        let mut tags: Vec<(&[u8], &[u8])> = Vec::with_capacity(ts.labels.len());
        for l in &ts.labels {
            if l.name == "__name__" {
                group = l.value.as_bytes();
            } else {
                tags.push((l.name.as_bytes(), l.value.as_bytes()));
            }
        }
        // marshal_metric_name_raw takes the metric group as the empty-key
        // pair, followed by the (already name-sorted) tags.
        let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity(tags.len() + 1);
        pairs.push((b"", group));
        pairs.extend_from_slice(&tags);
        let mut raw = Vec::new();
        marshal_metric_name_raw(&mut raw, &pairs);
        let raw_idx = raws.len();
        raws.push(raw);
        for s in &ts.samples {
            rows.push((raw_idx, s.timestamp, s.value));
        }
    }
    let mrs: Vec<esm_storage::MetricRowRef<'_>> = rows
        .iter()
        .map(|(i, t, v)| esm_storage::MetricRowRef {
            metric_name_raw: &raws[*i],
            timestamp: *t,
            value: *v,
        })
        .collect();
    if !mrs.is_empty() {
        storage.add_rows_ref(&mrs, DEFAULT_PRECISION_BITS);
    }
}

/// [`MetricsProvider`] implementation over [`Storage`] (the seam Go fills
/// with `netstorage.ProcessSearchQuery` + `RunParallel`).
pub struct StorageProvider {
    pub storage: Arc<Storage>,
}

impl MetricsProvider for StorageProvider {
    fn search(&self, sq: &SearchQuery, deadline: Deadline) -> esm_promql::Result<Vec<Series>> {
        let mut tfss = Vec::with_capacity(sq.tag_filterss.len());
        for lfs in &sq.tag_filterss {
            let mut tfs = TagFilters::new();
            for lf in lfs {
                let key: &[u8] = if lf.label == "__name__" {
                    b""
                } else {
                    lf.label.as_bytes()
                };
                tfs.add(key, lf.value.as_bytes(), lf.is_negative, lf.is_regexp)
                    .map_err(|e| esm_promql::Error::new(format!("cannot parse tag filter: {e}")))?;
            }
            tfss.push(tfs);
        }
        let tr = TimeRange {
            min_timestamp: sq.start,
            max_timestamp: sq.end,
        };
        let deadline_secs = deadline_unix_secs(deadline);
        // Two-pass parallel read (Go: ProcessSearchQuery + RunParallel):
        // block refs are collected on this thread, then decoded/merged
        // across the shared unpack worker pool.
        let series_blocks = self
            .storage
            .search_series_parallel(
                &tfss,
                tr,
                effective_max_metrics(sq.max_metrics),
                deadline_secs,
            )
            .map_err(|e| esm_promql::Error::new(format!("search error: {e}")))?;
        Ok(series_blocks
            .into_iter()
            .map(|sb| Series {
                metric_name: sb.metric_name,
                timestamps: Arc::new(sb.timestamps),
                values: sb.values,
            })
            .collect())
    }
}

/// Storage treats `max_metrics` as a hard limit; promql passes 0 for
/// "unlimited", which storage does not accept. the upstream's default `-search.maxUniqueTimeseries`.
fn effective_max_metrics(max_metrics: usize) -> usize {
    if max_metrics == 0 {
        300_000
    } else {
        max_metrics
    }
}

/// promql deadlines are unix-ms wrappers; storage takes unix seconds
/// (0 = none, mirrored by [`esm_storage::NO_DEADLINE`]).
fn deadline_unix_secs(d: Deadline) -> u64 {
    let ms = d.deadline_unix_ms();
    if ms <= 0 {
        esm_storage::NO_DEADLINE
    } else {
        (ms as u64).div_ceil(1000)
    }
}
