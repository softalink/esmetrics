//! Endpoint handlers. Ports of `app/vmselect/prometheus/prometheus.go`
//! `QueryHandler`, `QueryRangeHandler`, `SeriesHandler`, `LabelsHandler`,
//! `LabelValuesHandler` and `ExportHandler`.
//!
//! Series/labels design note: [`esm_promql::MetricsProvider`] exposes a
//! single `search()` returning fully unpacked series, while Go uses
//! dedicated index-level APIs (`SearchMetricNames`, `LabelNames`,
//! `LabelValues`). This port evaluates the matchers through the same
//! `search()` call over the requested time range and derives metric names /
//! label names / label values from the returned series. The visible data is
//! equivalent (names of the series matching the selector on the range);
//! only the storage-side efficiency differs, which can be recovered later
//! by growing the provider trait without touching these handlers.

use crate::json::{
    write_export_json_line, write_export_prom_api_footer, write_export_prom_api_header,
    write_export_prom_api_line, write_export_prometheus_line, write_query_range_response,
    write_query_response, write_series_response, write_string_list_response, Stats,
};
use crate::params::Params;
use crate::searchutil::{
    get_bool, get_duration, get_extra_tag_filters, get_int, get_time, get_timeout_ms,
    join_tag_filterss, now_unix_ms, tag_filterss_from_matches, unescape_prometheus_label_name,
    DEFAULT_STEP, MAX_TIME_MSECS,
};
use crate::SelectHandlers;
use esm_http::ResponseWriter;
use esm_metricsql::{DurationExpr, Expr, LabelFilter};
use esm_promql::provider::{Deadline, MetricsProvider, SearchQuery, Series};
use esm_promql::timeseries::{metric_name_group_key, metric_name_less, sort_metric_tags};
use esm_promql::{EvalConfig, QueryResult};
use esm_storage::metric_name::MetricName;
use std::collections::{BTreeSet, HashSet};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Common `/api/v1/*` parameters. Port of `commonParams`.
struct CommonParams {
    deadline: Deadline,
    start: i64,
    end: i64,
    filterss: Vec<Vec<LabelFilter>>,
}

/// Wraps the storage provider and counts fetched series for the
/// `seriesFetched` stat (`promql.QueryStats.SeriesFetched` analog).
struct CountingProvider<'a> {
    inner: &'a dyn MetricsProvider,
    count: AtomicU64,
    /// Total time spent in storage searches, in microseconds
    /// (for the `ESM_LOG_SLOW_MS` debug breakdown).
    search_micros: AtomicU64,
}

impl MetricsProvider for CountingProvider<'_> {
    fn search(&self, sq: &SearchQuery, deadline: Deadline) -> esm_promql::Result<Vec<Series>> {
        let started = Instant::now();
        let series = self.inner.search(sq, deadline)?;
        self.search_micros
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.count.fetch_add(series.len() as u64, Ordering::Relaxed);
        Ok(series)
    }
}

impl<P: MetricsProvider> SelectHandlers<P> {
    /// Port of `QueryRangeHandler` (/api/v1/query_range).
    pub(crate) fn handle_query_range(
        &self,
        params: &Params,
        w: &mut ResponseWriter<'_>,
    ) -> Result<(), String> {
        let ct = now_unix_ms();
        let query = params
            .get("query")
            .filter(|q| !q.is_empty())
            .ok_or("missing `query` arg")?
            .to_string();
        let start = get_time(params, "start", ct - DEFAULT_STEP)?;
        let end = get_time(params, "end", ct)?;
        let step = get_duration(params, "step", DEFAULT_STEP)?;
        let etfs = get_extra_tag_filters(params)?;
        self.query_range_inner(params, w, &query, start, end, step, ct, etfs)
            .map_err(|e| {
                format!(
                    "error when executing query={query:?} on the time range \
                     (start={start}, end={end}, step={step}): {e}"
                )
            })
    }

    /// Port of `queryRangeHandler`.
    #[allow(clippy::too_many_arguments)]
    fn query_range_inner(
        &self,
        params: &Params,
        w: &mut ResponseWriter<'_>,
        query: &str,
        mut start: i64,
        mut end: i64,
        step: i64,
        ct: i64,
        etfs: Vec<Vec<LabelFilter>>,
    ) -> Result<(), String> {
        let cfg = &self.config;
        let deadline = self.query_deadline(params);
        let may_cache = !get_bool(params, "nocache");
        let lookback_delta = get_max_lookback(params)?;
        if query.len() > cfg.max_query_len {
            return Err(format!(
                "too long query; got {} bytes; mustn't exceed `-search.maxQueryLen={}` bytes",
                query.len(),
                cfg.max_query_len
            ));
        }
        if start > end {
            end = start + DEFAULT_STEP;
        }
        esm_promql::timeseries::validate_max_points_per_series(
            start,
            end,
            step,
            cfg.max_points_per_timeseries,
        )
        .map_err(|e| format!("{e}; (see -search.maxPointsPerTimeseries command-line flag)"))?;
        if may_cache {
            (start, end) = esm_promql::eval::adjust_start_end(start, end, step);
        }

        let mut ec = EvalConfig::new(start, end, step);
        ec.max_points_per_series = cfg.max_points_per_timeseries;
        ec.deadline = deadline;
        ec.may_cache = may_cache;
        ec.lookback_delta = lookback_delta;
        ec.round_digits = get_round_digits(params);
        ec.enforced_tag_filterss = etfs;

        let counting = CountingProvider {
            inner: &self.provider,
            count: AtomicU64::new(0),
            search_micros: AtomicU64::new(0),
        };
        let started = Instant::now();
        let mut result =
            esm_promql::exec(&counting, &ec, query).map_err(|e| e.message().to_string())?;
        let execution_time_msec = started.elapsed().as_millis() as i64;
        log_slow_query(
            query,
            start,
            end,
            execution_time_msec,
            counting.search_micros.load(Ordering::Relaxed),
        );

        if step < cfg.max_step_for_points_adjustment_ms {
            let query_offset = get_latency_offset(params, cfg.latency_offset_ms)?;
            if ct - query_offset < end {
                adjust_last_points(&mut result, ct - query_offset, ct + step);
            }
        }
        // Remove NaN values as Prometheus does.
        remove_empty_values_and_timeseries(&mut result);

        let mut body = Vec::with_capacity(4096);
        write_query_range_response(
            &mut body,
            &result,
            &Stats {
                series_fetched: counting.count.load(Ordering::Relaxed),
                execution_time_msec,
            },
        );
        send_json(w, body);
        Ok(())
    }

    /// Port of `QueryHandler` (/api/v1/query).
    pub(crate) fn handle_query(
        &self,
        params: &Params,
        w: &mut ResponseWriter<'_>,
    ) -> Result<(), String> {
        let cfg = &self.config;
        let ct = now_unix_ms();
        let deadline = self.query_deadline(params);
        let may_cache = !get_bool(params, "nocache");
        let query = params
            .get("query")
            .filter(|q| !q.is_empty())
            .ok_or("missing `query` arg")?
            .to_string();
        let mut start = get_time(params, "time", ct)?;
        let lookback_delta = get_max_lookback(params)?;
        let mut step = get_duration(params, "step", lookback_delta)?;
        if step <= 0 {
            step = DEFAULT_STEP;
        }
        if query.len() > cfg.max_query_len {
            return Err(format!(
                "too long query; got {} bytes; mustn't exceed `-search.maxQueryLen={}` bytes",
                query.len(),
                cfg.max_query_len
            ));
        }
        let etfs = get_extra_tag_filters(params)?;

        // Rewrite `selector[d]` into a raw-sample export in promapi format.
        if let Some((child_query, window_expr, offset_expr)) =
            is_metric_selector_with_rollup(&query)
        {
            let window = window_expr.non_negative_duration(step).map_err(|e| {
                format!("cannot parse lookbehind window in square brackets at {query}: {e}")
            })?;
            let offset = offset_expr.map_or(0, |o| o.duration(step));
            start -= offset;
            let mut end = start;
            // Do not include the sample matching the lower window boundary,
            // as Prometheus does.
            start = end - window + 1;
            if end < start {
                end = start;
            }
            let tag_filterss = tag_filterss_from_matches(std::slice::from_ref(&child_query))?;
            let filterss = join_tag_filterss(tag_filterss, &etfs);
            let cp = CommonParams {
                deadline,
                start,
                end,
                filterss,
            };
            return self.export_inner(w, &cp, "promapi", 0).map_err(|e| {
                format!(
                    "error when exporting data for query={child_query:?} on the time range \
                     (start={start}, end={end}): {e}"
                )
            });
        }
        // Rewrite `expr[w:step]` into a range query.
        if let Some((child_query, window_expr, step_expr, offset_expr)) = is_rollup(&query) {
            let new_step = match &step_expr {
                Some(se) => se
                    .non_negative_duration(step)
                    .map_err(|e| format!("cannot parse step in square brackets at {query}: {e}"))?,
                None => 0,
            };
            if new_step > 0 {
                step = new_step;
            }
            let window = window_expr.non_negative_duration(step).map_err(|e| {
                format!("cannot parse lookbehind window in square brackets at {query}: {e}")
            })?;
            let offset = offset_expr.map_or(0, |o| o.duration(step));
            start -= offset;
            let end = start;
            let start = end - window;
            return self
                .query_range_inner(params, w, &child_query, start, end, step, ct, etfs)
                .map_err(|e| {
                    format!(
                        "error when executing query={child_query:?} on the time range \
                         (start={start}, end={end}, step={step}): {e}"
                    )
                });
        }

        let mut query_offset = get_latency_offset(params, cfg.latency_offset_ms)?;
        if !get_bool(params, "nocache") && ct - start < query_offset && start - ct < query_offset {
            // Adjust start time only if `nocache` arg isn't set.
            let start_prev = start;
            start = ct - query_offset;
            query_offset = start_prev - start;
        } else {
            query_offset = 0;
        }

        let mut ec = EvalConfig::new(start, start, step);
        ec.max_points_per_series = cfg.max_points_per_timeseries;
        ec.deadline = deadline;
        ec.may_cache = may_cache;
        ec.lookback_delta = lookback_delta;
        ec.round_digits = get_round_digits(params);
        ec.enforced_tag_filterss = etfs;

        let counting = CountingProvider {
            inner: &self.provider,
            count: AtomicU64::new(0),
            search_micros: AtomicU64::new(0),
        };
        let started = Instant::now();
        let mut result = esm_promql::exec(&counting, &ec, &query).map_err(|e| {
            format!("error when executing query={query:?} for (time={start}, step={step}): {e}")
        })?;
        let execution_time_msec = started.elapsed().as_millis() as i64;
        log_slow_query(
            &query,
            start,
            start,
            execution_time_msec,
            counting.search_micros.load(Ordering::Relaxed),
        );

        if query_offset > 0 {
            for r in result.iter_mut() {
                // Timestamps may be shared among series; copy before shifting.
                let shifted: Vec<i64> = r.timestamps.iter().map(|ts| ts + query_offset).collect();
                r.timestamps = Arc::new(shifted);
            }
        }
        result.retain(|r| !r.values.is_empty());

        let mut body = Vec::with_capacity(2048);
        write_query_response(
            &mut body,
            &result,
            &Stats {
                series_fetched: counting.count.load(Ordering::Relaxed),
                execution_time_msec,
            },
        );
        send_json(w, body);
        Ok(())
    }

    /// Port of `SeriesHandler` (/api/v1/series).
    pub(crate) fn handle_series(
        &self,
        params: &Params,
        w: &mut ResponseWriter<'_>,
    ) -> Result<(), String> {
        let cp = self.common_params(params, true, true)?;
        let limit = get_int(params, "limit")?;
        let sq = SearchQuery {
            start: cp.start,
            end: cp.end,
            tag_filterss: cp.filterss,
            max_metrics: self.config.max_series,
        };
        let series = self
            .provider
            .search(&sq, cp.deadline)
            .map_err(|e| format!("cannot fetch time series: {e}"))?;

        let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(series.len());
        let mut metric_names: Vec<MetricName> = Vec::with_capacity(series.len());
        for s in series {
            let mut mn = s.metric_name;
            if seen.insert(metric_name_group_key(&mut mn)) {
                metric_names.push(mn);
            }
        }
        for mn in metric_names.iter_mut() {
            sort_metric_tags(mn);
        }
        metric_names.sort_by(|a, b| {
            if metric_name_less(a, b) {
                std::cmp::Ordering::Less
            } else if metric_name_less(b, a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        if limit > 0 && (limit as usize) < metric_names.len() {
            metric_names.truncate(limit as usize);
        }

        let mut body = Vec::with_capacity(1024);
        write_series_response(&mut body, &metric_names);
        send_json(w, body);
        Ok(())
    }

    /// Port of `LabelsHandler` (/api/v1/labels).
    pub(crate) fn handle_labels(
        &self,
        params: &Params,
        w: &mut ResponseWriter<'_>,
    ) -> Result<(), String> {
        let cp = self.common_params(params, false, true)?;
        let limit = get_int(params, "limit")?;
        let series = self.search_labels_api(cp)?;
        let mut names: BTreeSet<String> = BTreeSet::new();
        for s in &series {
            if !s.metric_name.metric_group.is_empty() {
                names.insert("__name__".to_string());
            }
            for tag in &s.metric_name.tags {
                names.insert(String::from_utf8_lossy(&tag.key).into_owned());
            }
        }
        let labels = truncate_sorted(names, limit);
        let mut body = Vec::with_capacity(512);
        write_string_list_response(&mut body, &labels);
        send_json(w, body);
        Ok(())
    }

    /// Port of `LabelValuesHandler` (/api/v1/label/<name>/values).
    pub(crate) fn handle_label_values(
        &self,
        label_name: &str,
        params: &Params,
        w: &mut ResponseWriter<'_>,
    ) -> Result<(), String> {
        let cp = self.common_params(params, false, true)?;
        let limit = get_int(params, "limit")?;
        let label_name = if label_name.starts_with("U__") {
            unescape_prometheus_label_name(label_name)
        } else {
            label_name.to_string()
        };
        let series = self.search_labels_api(cp)?;
        let mut values: BTreeSet<String> = BTreeSet::new();
        for s in &series {
            if label_name == "__name__" {
                if !s.metric_name.metric_group.is_empty() {
                    values
                        .insert(String::from_utf8_lossy(&s.metric_name.metric_group).into_owned());
                }
                continue;
            }
            if let Some(v) = s.metric_name.get_tag_value(&label_name) {
                if !v.is_empty() {
                    values.insert(String::from_utf8_lossy(v).into_owned());
                }
            }
        }
        let values = truncate_sorted(values, limit);
        let mut body = Vec::with_capacity(512);
        write_string_list_response(&mut body, &values);
        send_json(w, body);
        Ok(())
    }

    fn search_labels_api(&self, cp: CommonParams) -> Result<Vec<Series>, String> {
        let sq = SearchQuery {
            start: cp.start,
            end: cp.end,
            tag_filterss: cp.filterss,
            max_metrics: self.config.max_labels_api_series,
        };
        self.provider
            .search(&sq, cp.deadline)
            .map_err(|e| format!("error during search on time range: {e}"))
    }

    /// Port of `ExportHandler` (/api/v1/export).
    pub(crate) fn handle_export(
        &self,
        params: &Params,
        w: &mut ResponseWriter<'_>,
    ) -> Result<(), String> {
        let mut cp = self.common_params(params, true, false)?;
        // Export requests get the (much larger) export deadline.
        let timeout_ms = get_timeout_ms(params, self.config.max_export_duration_ms);
        cp.deadline = Deadline::from_timeout(Duration::from_millis(timeout_ms as u64));
        let format = params.get("format").unwrap_or("").to_string();
        // fastfloat.ParseInt64BestEffort semantics: 0 on parse failure.
        let max_rows_per_line = params
            .get("max_rows_per_line")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
            .max(0) as usize;
        let (start, end) = (cp.start, cp.end);
        self.export_inner(w, &cp, &format, max_rows_per_line)
            .map_err(|e| {
                format!(
                    "error when exporting data on the time range (start={start}, end={end}): {e}"
                )
            })
    }

    /// Port of `exportHandler`: streams the matching raw series in the
    /// requested format. All fallible work happens before streaming starts.
    fn export_inner(
        &self,
        w: &mut ResponseWriter<'_>,
        cp: &CommonParams,
        format: &str,
        max_rows_per_line: usize,
    ) -> Result<(), String> {
        let sq = SearchQuery {
            start: cp.start,
            end: cp.end,
            tag_filterss: cp.filterss.clone(),
            max_metrics: self.config.max_export_series,
        };
        let mut series = self
            .provider
            .search(&sq, cp.deadline)
            .map_err(|e| format!("cannot fetch data: {e}"))?;
        for s in series.iter_mut() {
            sort_metric_tags(&mut s.metric_name);
        }

        let content_type = match format {
            "prometheus" => "text/plain; charset=utf-8",
            _ => "application/stream+json; charset=utf-8",
        };
        w.set_content_type(content_type);
        if w.begin_stream().is_err() {
            return Ok(()); // client went away; nothing else to report
        }
        let mut out = Vec::with_capacity(16 * 1024);
        let mut first_line = true;
        if format == "promapi" {
            write_export_prom_api_header(&mut out);
        }
        for s in &series {
            if s.timestamps.is_empty() {
                continue; // RunParallel skips series without samples
            }
            for (values, timestamps) in RowChunks::new(&s.values, &s.timestamps, max_rows_per_line)
            {
                match format {
                    "promapi" => {
                        if !first_line {
                            out.push(b',');
                        }
                        write_export_prom_api_line(&mut out, &s.metric_name, values, timestamps);
                    }
                    "prometheus" => {
                        write_export_prometheus_line(&mut out, &s.metric_name, values, timestamps);
                    }
                    _ => {
                        write_export_json_line(&mut out, &s.metric_name, values, timestamps);
                    }
                }
                first_line = false;
                if out.len() >= 1024 * 1024 {
                    if w.write_all(&out).is_err() {
                        return Ok(());
                    }
                    out.clear();
                }
            }
        }
        if format == "promapi" {
            write_export_prom_api_footer(&mut out);
        }
        let _ = w.write_all(&out);
        Ok(())
    }

    fn query_deadline(&self, params: &Params) -> Deadline {
        let ms = get_timeout_ms(params, self.config.max_query_duration_ms);
        Deadline::from_timeout(Duration::from_millis(ms as u64))
    }

    /// Port of `getCommonParamsInternal` (+ the labels-API adjustments from
    /// `getCommonParamsForLabelsAPI`).
    fn common_params(
        &self,
        params: &Params,
        require_non_empty_match: bool,
        is_labels_api: bool,
    ) -> Result<CommonParams, String> {
        let ct = now_unix_ms();
        let deadline_max = if is_labels_api {
            self.config.max_labels_api_duration_ms
        } else {
            self.config.max_query_duration_ms
        };
        let deadline = Deadline::from_timeout(Duration::from_millis(get_timeout_ms(
            params,
            deadline_max,
        ) as u64));
        let mut start = get_time(params, "start", 0)?;
        let mut end = get_time(params, "end", ct)?;
        if end > MAX_TIME_MSECS {
            end = MAX_TIME_MSECS;
        }
        if end < start {
            end = start;
        }
        let matches = params.matches();
        if require_non_empty_match && matches.is_empty() {
            return Err("missing `match[]` arg".to_string());
        }
        let filterss = tag_filterss_from_matches(&matches)?;
        let etfs = get_extra_tag_filters(params)?;
        let filterss = join_tag_filterss(filterss, &etfs);
        if is_labels_api && start == 0 {
            // Avoid scanning the whole storage by default; see
            // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/91
            start = end - DEFAULT_STEP;
        }
        Ok(CommonParams {
            deadline,
            start,
            end,
            filterss,
        })
    }
}

/// Debug knob: when `ESM_LOG_SLOW_MS` is set, queries whose evaluation takes
/// at least that many milliseconds are logged with their duration.
fn log_slow_query(query: &str, start: i64, end: i64, execution_time_msec: i64, search_micros: u64) {
    static THRESHOLD: std::sync::OnceLock<Option<i64>> = std::sync::OnceLock::new();
    let Some(threshold) = *THRESHOLD.get_or_init(|| {
        std::env::var("ESM_LOG_SLOW_MS")
            .ok()
            .and_then(|v| v.parse().ok())
    }) else {
        return;
    };
    if execution_time_msec >= threshold {
        let search_ms = search_micros as f64 / 1000.0;
        log::warn!(
            "slow query: {execution_time_msec}ms (search {search_ms:.1}ms)              start={start} end={end} {query}"
        );
    }
}

fn send_json(w: &mut ResponseWriter<'_>, body: Vec<u8>) {
    w.set_content_type("application/json");
    w.write_body(&body);
}

fn truncate_sorted(set: BTreeSet<String>, limit: i64) -> Vec<String> {
    let mut items: Vec<String> = set.into_iter().collect();
    if limit > 0 && (limit as usize) < items.len() {
        items.truncate(limit as usize);
    }
    items
}

/// Port of `getMaxLookback`: `-search.maxLookback` and
/// `-search.maxStalenessInterval` both default to 0 (auto).
fn get_max_lookback(params: &Params) -> Result<i64, String> {
    get_duration(params, "max_lookback", 0)
}

/// Port of `getRoundDigits`: invalid/missing → 100 (disabled).
fn get_round_digits(params: &Params) -> i32 {
    params
        .get("round_digits")
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(100)
}

/// Port of `getLatencyOffsetMilliseconds` (`-search.latencyOffset`,
/// overridable via the `latency_offset` arg).
fn get_latency_offset(params: &Params, default_ms: i64) -> Result<i64, String> {
    get_duration(params, "latency_offset", default_ms.max(0))
}

/// Port of `promql.IsMetricSelectorWithRollup`.
fn is_metric_selector_with_rollup(q: &str) -> Option<(String, DurationExpr, Option<DurationExpr>)> {
    let expr = esm_promql::exec::parse_promql_with_cache(q).ok()?;
    let Expr::Rollup(re) = expr.as_ref() else {
        return None;
    };
    let window = re.window.clone()?;
    if re.step.is_some() {
        return None;
    }
    let Expr::Metric(me) = re.expr.as_ref() else {
        return None;
    };
    if me.label_filterss.is_empty() {
        return None;
    }
    let mut child = String::new();
    re.expr.append_string(&mut child);
    Some((child, window, re.offset.clone()))
}

/// Port of `promql.IsRollup`.
type RollupParts = (
    String,
    DurationExpr,
    Option<DurationExpr>,
    Option<DurationExpr>,
);
fn is_rollup(q: &str) -> Option<RollupParts> {
    let expr = esm_promql::exec::parse_promql_with_cache(q).ok()?;
    let Expr::Rollup(re) = expr.as_ref() else {
        return None;
    };
    let window = re.window.clone()?;
    let mut child = String::new();
    re.expr.append_string(&mut child);
    Some((child, window, re.step.clone(), re.offset.clone()))
}

/// Port of `removeEmptyValuesAndTimeseries`: strips NaN points and drops
/// series left with no points.
fn remove_empty_values_and_timeseries(tss: &mut Vec<QueryResult>) {
    tss.retain_mut(|ts| {
        if !ts.values.iter().any(|v| v.is_nan()) {
            return !ts.values.is_empty();
        }
        let mut values = Vec::with_capacity(ts.values.len());
        // ts.timestamps may be shared among series — build a fresh vec.
        let mut timestamps = Vec::with_capacity(ts.timestamps.len());
        for (j, &v) in ts.values.iter().enumerate() {
            if v.is_nan() {
                continue;
            }
            values.push(v);
            timestamps.push(ts.timestamps[j]);
        }
        ts.values = values;
        ts.timestamps = Arc::new(timestamps);
        !ts.values.is_empty()
    });
}

/// Port of `adjustLastPoints`: substitutes point values on `(start..end]`
/// with the previous point value, since those points may be incomplete.
fn adjust_last_points(tss: &mut [QueryResult], start: i64, end: i64) {
    for ts in tss.iter_mut() {
        let timestamps = &ts.timestamps;
        if timestamps.last().is_some_and(|&last| last > end) {
            // `offset` in the query shifted the range beyond `end`; leave
            // the series as is.
            continue;
        }
        let mut j = timestamps.len();
        while j > 0 && timestamps[j - 1] > start {
            j -= 1;
        }
        let last_value = if j > 0 { ts.values[j - 1] } else { f64::NAN };
        while j < timestamps.len() && timestamps[j] <= end {
            ts.values[j] = last_value;
            j += 1;
        }
    }
}

/// Splits parallel value/timestamp slices into chunks of at most
/// `max_rows` rows (0 = single chunk), for `max_rows_per_line`.
struct RowChunks<'a> {
    values: &'a [f64],
    timestamps: &'a [i64],
    max_rows: usize,
}

impl<'a> RowChunks<'a> {
    fn new(values: &'a [f64], timestamps: &'a [i64], max_rows: usize) -> RowChunks<'a> {
        RowChunks {
            values,
            timestamps,
            max_rows,
        }
    }
}

impl<'a> Iterator for RowChunks<'a> {
    type Item = (&'a [f64], &'a [i64]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            return None;
        }
        let n = if self.max_rows == 0 {
            self.values.len()
        } else {
            self.max_rows.min(self.values.len())
        };
        let item = (&self.values[..n], &self.timestamps[..n]);
        self.values = &self.values[n..];
        self.timestamps = &self.timestamps[n..];
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(timestamps: Vec<i64>, values: Vec<f64>) -> QueryResult {
        QueryResult {
            metric_name: MetricName::default(),
            values,
            timestamps: Arc::new(timestamps),
        }
    }

    fn assert_values(got: &[f64], want: &[f64]) {
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(want) {
            if w.is_nan() {
                assert!(g.is_nan(), "got {g}, want NaN");
            } else {
                assert_eq!(g, w);
            }
        }
    }

    /// Port of `TestAdjustLastPoints` (prometheus_test.go).
    #[test]
    fn adjust_last_points_cases() {
        let nan = f64::NAN;
        let ts5 = || vec![100i64, 200, 300, 400, 500];

        let mut tss = vec![
            result(ts5(), vec![1.0, 2.0, 3.0, 4.0, nan]),
            result(ts5(), vec![1.0, 2.0, 3.0, nan, nan]),
        ];
        adjust_last_points(&mut tss, 400, 500);
        assert_values(&tss[0].values, &[1.0, 2.0, 3.0, 4.0, 4.0]);
        assert_values(&tss[1].values, &[1.0, 2.0, 3.0, nan, nan]);

        let mut tss = vec![
            result(ts5(), vec![1.0, 2.0, 3.0, nan, nan]),
            result(ts5(), vec![1.0, 2.0, nan, nan, nan]),
        ];
        adjust_last_points(&mut tss, 300, 500);
        assert_values(&tss[0].values, &[1.0, 2.0, 3.0, 3.0, 3.0]);
        assert_values(&tss[1].values, &[1.0, 2.0, nan, nan, nan]);

        // start > end: nothing to adjust.
        let mut tss = vec![
            result(ts5(), vec![1.0, 2.0, nan, nan, nan]),
            result(ts5(), vec![1.0, nan, nan, nan, nan]),
        ];
        adjust_last_points(&mut tss, 500, 300);
        assert_values(&tss[0].values, &[1.0, 2.0, nan, nan, nan]);
        assert_values(&tss[1].values, &[1.0, nan, nan, nan, nan]);

        let mut tss = vec![
            result(ts5(), vec![1.0, 2.0, 3.0, 4.0, nan]),
            result(vec![100, 200, 300, 400], vec![1.0, 2.0, 3.0, 4.0]),
        ];
        adjust_last_points(&mut tss, 400, 500);
        assert_values(&tss[0].values, &[1.0, 2.0, 3.0, 4.0, 4.0]);
        assert_values(&tss[1].values, &[1.0, 2.0, 3.0, 4.0]);

        let mut tss = vec![
            result(ts5(), vec![1.0, 2.0, 3.0, nan, nan]),
            result(vec![100, 200, 300], vec![1.0, 2.0, nan]),
        ];
        adjust_last_points(&mut tss, 300, 600);
        assert_values(&tss[0].values, &[1.0, 2.0, 3.0, 3.0, 3.0]);
        assert_values(&tss[1].values, &[1.0, 2.0, nan]);
    }

    #[test]
    fn remove_empty_values() {
        let nan = f64::NAN;
        let mut tss = vec![
            result(vec![100, 200, 300], vec![1.0, nan, 3.0]),
            result(vec![100, 200], vec![nan, nan]),
            result(vec![100], vec![5.0]),
            result(vec![], vec![]),
        ];
        remove_empty_values_and_timeseries(&mut tss);
        assert_eq!(tss.len(), 2);
        assert_eq!(tss[0].values, vec![1.0, 3.0]);
        assert_eq!(*tss[0].timestamps, vec![100, 300]);
        assert_eq!(tss[1].values, vec![5.0]);
    }

    #[test]
    fn rollup_query_detection() {
        let (child, window, offset) = is_metric_selector_with_rollup("foo{a=\"b\"}[5m]").unwrap();
        assert_eq!(child, "foo{a=\"b\"}");
        assert_eq!(window.duration(0), 300_000);
        assert!(offset.is_none());

        assert!(is_metric_selector_with_rollup("rate(foo[5m])").is_none());
        assert!(is_metric_selector_with_rollup("foo").is_none());
        // Subquery step present → not a plain selector-with-rollup.
        assert!(is_metric_selector_with_rollup("foo[5m:1m]").is_none());

        let (child, window, step, offset) = is_rollup("foo[1h:5m] offset 10m").unwrap();
        assert_eq!(child, "foo");
        assert_eq!(window.duration(0), 3_600_000);
        assert_eq!(step.unwrap().duration(0), 300_000);
        assert_eq!(offset.unwrap().duration(0), 600_000);
    }

    #[test]
    fn row_chunks_split() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let timestamps = [1i64, 2, 3, 4, 5];
        let chunks: Vec<_> = RowChunks::new(&values, &timestamps, 2).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].0, &[1.0, 2.0]);
        assert_eq!(chunks[2].0, &[5.0]);
        let chunks: Vec<_> = RowChunks::new(&values, &timestamps, 0).collect();
        assert_eq!(chunks.len(), 1);
    }
}
