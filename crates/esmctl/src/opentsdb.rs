//! OpenTSDB migration source: metric/series discovery, retention→query-range
//! planning, data retrieval, and Prometheus-data-model normalization. Ports
//! `app/vmctl/opentsdb/{opentsdb,parser}.go` and `app/vmctl/opentsdb.go`.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use reqwest::blocking::Client;

use crate::importer::{Importer, Series};

const MS_PER_SEC: i64 = 1000;
const MS_PER_HOUR: i64 = 3600 * MS_PER_SEC;

/// A single time range to query (offsets from the start time, in the
/// configured time unit). Ports `opentsdb.TimeRange`.
#[derive(Clone, Copy)]
pub(crate) struct TimeRange {
    pub(crate) start: i64,
    pub(crate) end: i64,
}

/// A retention's aggregation policy plus the query ranges it expands to.
/// Ports `opentsdb.Retention`.
pub(crate) struct Retention {
    pub(crate) first_order: String,
    pub(crate) second_order: String,
    pub(crate) agg_time: String,
    pub(crate) query_ranges: Vec<TimeRange>,
}

/// A discovered series (metric + tag set). Ports `opentsdb.Meta`.
#[derive(Clone, serde::Deserialize)]
pub(crate) struct Meta {
    pub(crate) metric: String,
    #[serde(default)]
    pub(crate) tags: HashMap<String, String>,
}

#[derive(serde::Deserialize)]
struct MetaResults {
    #[serde(default)]
    results: Vec<Meta>,
}

#[derive(serde::Deserialize)]
struct OtsdbMetric {
    metric: String,
    #[serde(default)]
    tags: HashMap<String, String>,
    #[serde(default, rename = "aggregateTags")]
    aggregate_tags: Vec<String>,
    #[serde(default)]
    dps: HashMap<String, f64>,
}

/// Client configuration. Ports `opentsdb.Config`.
pub(crate) struct Config {
    pub(crate) addr: String,
    pub(crate) limit: i64,
    pub(crate) offset_days: i64,
    pub(crate) hard_ts: i64,
    pub(crate) retentions: Vec<String>,
    pub(crate) filters: Vec<String>,
    pub(crate) normalize: bool,
    pub(crate) msecs_time: bool,
}

/// OpenTSDB HTTP client. Ports `opentsdb.Client`.
pub(crate) struct OtsdbClient {
    pub(crate) addr: String,
    pub(crate) limit: i64,
    pub(crate) retentions: Vec<Retention>,
    pub(crate) filters: Vec<String>,
    pub(crate) normalize: bool,
    pub(crate) hard_ts: i64,
    pub(crate) msecs_time: bool,
    http: Client,
}

impl OtsdbClient {
    /// Ports `opentsdb.NewClient`.
    pub(crate) fn new(cfg: Config, http: Client) -> Result<OtsdbClient, String> {
        let mut offset_secs = cfg.offset_days * 24 * 60 * 60;
        if cfg.msecs_time {
            offset_secs *= 1000;
        }
        let mut retentions = Vec::new();
        for r in &cfg.retentions {
            retentions.push(
                convert_retention(r, offset_secs, cfg.msecs_time)
                    .map_err(|e| format!("couldn't parse retention {r:?}: {e}"))?,
            );
        }
        Ok(OtsdbClient {
            addr: cfg.addr.trim_matches('/').to_string(),
            limit: cfg.limit,
            retentions,
            filters: cfg.filters,
            normalize: cfg.normalize,
            hard_ts: cfg.hard_ts,
            msecs_time: cfg.msecs_time,
            http,
        })
    }

    /// Ports `Client.FindMetrics` (GET /api/suggest).
    fn find_metrics(&self, url: &str) -> Result<Vec<String>, String> {
        let resp = self
            .http
            .get(url)
            .send()
            .map_err(|e| format!("failed to GET {url:?}: {e}"))?;
        if resp.status().as_u16() != 200 {
            return Err(format!("bad return from OpenTSDB: {}", resp.status()));
        }
        let body = resp.text().map_err(|e| format!("read error: {e}"))?;
        serde_json::from_str(&body)
            .map_err(|e| format!("failed to parse metrics from {url:?}: {e}"))
    }

    /// Ports `Client.FindSeries` (GET /api/search/lookup).
    fn find_series(&self, metric: &str) -> Result<Vec<Meta>, String> {
        let url = format!(
            "{}/api/search/lookup?m={}&limit={}",
            self.addr, metric, self.limit
        );
        let resp = self
            .http
            .get(&url)
            .send()
            .map_err(|e| format!("failed to GET {url:?}: {e}"))?;
        if resp.status().as_u16() != 200 {
            return Err(format!("bad return from OpenTSDB: {}", resp.status()));
        }
        let body = resp.text().map_err(|e| format!("read error: {e}"))?;
        let results: MetaResults = serde_json::from_str(&body)
            .map_err(|e| format!("failed to parse series from {url:?}: {e}"))?;
        Ok(results.results)
    }

    /// Ports `Client.GetData` (GET /api/query). Non-fatal server/parse errors
    /// yield an empty metric rather than aborting the migration.
    fn get_data(
        &self,
        series: &Meta,
        rt: &RetentionMeta,
        start: i64,
        end: i64,
    ) -> Result<Metric, String> {
        let mut tag_str = String::new();
        for (k, v) in &series.tags {
            tag_str.push_str(&format!("{k}={v},"));
        }
        let tag_str = tag_str.trim_end_matches(',');
        let agg_pol = format!(
            "{}:{}-{}-none",
            rt.first_order, rt.agg_time, rt.second_order
        );
        let url = format!(
            "{}/api/query?start={}&end={}&m={}:{}{{{}}}",
            self.addr, start, end, agg_pol, series.metric, tag_str
        );
        let resp = match self.http.get(&url).send() {
            Ok(r) => r,
            Err(_) => return Ok(Metric::default()),
        };
        if resp.status().as_u16() != 200 {
            log::warn!(
                "bad response code from OpenTSDB query {} for {url:?}...skipping",
                resp.status()
            );
            return Ok(Metric::default());
        }
        let body = match resp.text() {
            Ok(b) => b,
            Err(_) => return Ok(Metric::default()),
        };
        let output: Vec<OtsdbMetric> = match serde_json::from_str(&body) {
            Ok(o) => o,
            Err(_) => return Ok(Metric::default()),
        };
        if output.is_empty() {
            return Ok(Metric::default());
        }
        if output.len() > 1 {
            return Err(format!(
                "unexpected number of series returned: {} for {url:?}",
                output.len()
            ));
        }
        if !output[0].aggregate_tags.is_empty() {
            return Err(format!(
                "aggregate tags {:?} present in response for {url:?}; series may be suppressed",
                output[0].aggregate_tags
            ));
        }
        let mut data = modify_data(&output[0].metric, &output[0].tags, self.normalize)?;
        for (ts_str, val) in &output[0].dps {
            let ts: i64 = ts_str
                .parse()
                .map_err(|_| format!("bad dps timestamp {ts_str:?}"))?;
            data.timestamps
                .push(if self.msecs_time { ts } else { ts * 1000 });
            data.values.push(*val);
        }
        Ok(data)
    }
}

/// A per-query subset of a [`Retention`]. Ports `opentsdb.RetentionMeta`.
struct RetentionMeta {
    first_order: String,
    second_order: String,
    agg_time: String,
}

/// Time series data in VM format. Ports `opentsdb.Metric`.
#[derive(Default)]
pub(crate) struct Metric {
    pub(crate) metric: String,
    pub(crate) tags: HashMap<String, String>,
    pub(crate) timestamps: Vec<i64>,
    pub(crate) values: Vec<f64>,
}

/// Converts an OpenTSDB/Java-style duration (`y`/`w`/`d`/`h`/`m`/`s`/`ms`) to
/// milliseconds. Ports `convertDuration`.
fn convert_duration(duration: &str) -> Result<i64, String> {
    let bad = || format!("invalid time range: {duration:?}");
    if let Some(n) = duration.strip_suffix('y') {
        let v: i64 = n.parse().map_err(|_| bad())?;
        Ok(v * 365 * 24 * MS_PER_HOUR)
    } else if let Some(n) = duration.strip_suffix('w') {
        let v: i64 = n.parse().map_err(|_| bad())?;
        Ok(v * 7 * 24 * MS_PER_HOUR)
    } else if let Some(n) = duration.strip_suffix('d') {
        let v: i64 = n.parse().map_err(|_| bad())?;
        Ok(v * 24 * MS_PER_HOUR)
    } else if let Some(n) = duration.strip_suffix("ms") {
        let v: f64 = n.parse().map_err(|_| bad())?;
        Ok(v as i64)
    } else if let Some(n) = duration.strip_suffix('h') {
        let v: f64 = n.parse().map_err(|_| bad())?;
        Ok((v * MS_PER_HOUR as f64) as i64)
    } else if let Some(n) = duration.strip_suffix('m') {
        let v: f64 = n.parse().map_err(|_| bad())?;
        Ok((v * 60.0 * MS_PER_SEC as f64) as i64)
    } else if let Some(n) = duration.strip_suffix('s') {
        let v: f64 = n.parse().map_err(|_| bad())?;
        Ok((v * MS_PER_SEC as f64) as i64)
    } else {
        Err(format!("invalid time duration string: {duration:?}"))
    }
}

/// Parses a retention string `agg-aggtime-agg2:rowlen:ttl` into a
/// [`Retention`] with expanded query ranges. Ports `convertRetention`.
fn convert_retention(retention: &str, offset: i64, msec_time: bool) -> Result<Retention, String> {
    let chunks: Vec<&str> = retention.split(':').collect();
    if chunks.len() != 3 {
        return Err(format!("invalid retention string: {retention:?}"));
    }
    let scale = |ms: i64| if msec_time { ms } else { ms / 1000 };

    let mut query_length = scale(convert_duration(chunks[2])?);
    if query_length <= 0 {
        return Err(format!(
            "ttl {:?} resolves to non-positive query range",
            chunks[2]
        ));
    }
    let query_range = query_length;
    query_length += offset;

    let aggregates: Vec<&str> = chunks[0].split('-').collect();
    if aggregates.len() != 3 {
        return Err(format!("invalid aggregation string: {:?}", chunks[0]));
    }
    let agg_time = scale(convert_duration(aggregates[1])?);
    let row_length = scale(convert_duration(chunks[1])?);

    let divisor_base = if row_length > agg_time {
        row_length
    } else {
        agg_time
    };
    let divisor = query_range / (divisor_base * 4);
    let query_size = if divisor == 0 {
        query_range
    } else {
        query_range / divisor
    };
    if query_size <= 0 {
        return Err(format!(
            "computed non-positive query size for retention {retention:?}"
        ));
    }

    let mut time_chunks = Vec::new();
    let mut i = offset;
    while i <= query_length {
        time_chunks.push(TimeRange {
            start: i + query_size,
            end: i,
        });
        i += query_size;
    }

    Ok(Retention {
        first_order: aggregates[0].to_string(),
        second_order: aggregates[2].to_string(),
        agg_time: aggregates[1].to_string(),
        query_ranges: time_chunks,
    })
}

/// Normalizes a metric+tags to the Prometheus data model (sanitize names,
/// optionally lowercase, drop `__`-prefixed tags). Ports `modifyData`.
fn modify_data(
    metric: &str,
    tags: &HashMap<String, String>,
    normalize: bool,
) -> Result<Metric, String> {
    if !metric
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
    {
        return Err(format!("{metric} has a bad first character"));
    }
    let name = if normalize {
        metric.to_lowercase()
    } else {
        metric.to_string()
    };
    let mut out = Metric {
        metric: sanitize_metric_name(&name),
        ..Default::default()
    };
    for (key, value) in tags {
        let (mut key, value) = if normalize {
            (key.to_lowercase(), value.to_lowercase())
        } else {
            (key.clone(), value.clone())
        };
        key = sanitize_label_name(&key);
        if !key.starts_with("__") {
            out.tags.insert(key, value);
        }
    }
    Ok(out)
}

/// Ports `promrelabel.SanitizeMetricName` (`[^a-zA-Z0-9_:]` → `_`).
fn sanitize_metric_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Ports `promrelabel.SanitizeLabelName` (`[^a-zA-Z0-9_]` → `_`).
fn sanitize_label_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Runs the OpenTSDB → import migration. Ports `otsdbProcessor.run`.
pub(crate) fn run(
    client: &OtsdbClient,
    importer: Importer,
    concurrency: usize,
    assume_yes: bool,
) -> Result<(), String> {
    log::info!(
        "Loading all metrics from OpenTSDB for filters: {:?}",
        client.filters
    );
    let mut metrics: Vec<String> = Vec::new();
    for filter in &client.filters {
        let url = format!(
            "{}/api/suggest?type=metrics&q={}&max={}",
            client.addr, filter, client.limit
        );
        metrics.extend(client.find_metrics(&url)?);
    }
    if metrics.is_empty() {
        return Err(format!(
            "found no timeseries to import with filters {:?}",
            client.filters
        ));
    }
    if !crate::prompt(
        assume_yes,
        &format!("Found {} metrics to import. Continue?", metrics.len()),
    ) {
        return Ok(());
    }

    let start_time = if client.hard_ts != 0 {
        client.hard_ts
    } else {
        crate::timeparse::now_ms() / 1000
    };

    let importer = Arc::new(importer);
    for metric in &metrics {
        log::info!("Starting work on {metric}");
        let series_list = client.find_series(metric)?;
        let mut queue: VecDeque<(Meta, usize, TimeRange)> = VecDeque::new();
        for series in &series_list {
            for (ri, rt) in client.retentions.iter().enumerate() {
                for tr in &rt.query_ranges {
                    queue.push_back((series.clone(), ri, *tr));
                }
            }
        }
        run_metric_workers(client, &importer, concurrency, queue, start_time)?;
    }

    // Close the importer (flush + join) and surface any import error.
    match Arc::try_unwrap(importer) {
        Ok(im) => im.close(),
        Err(_) => Err("importer still referenced at shutdown".to_string()),
    }?;
    log::info!("Import finished!");
    Ok(())
}

fn run_metric_workers(
    client: &OtsdbClient,
    importer: &Arc<Importer>,
    concurrency: usize,
    queue: VecDeque<(Meta, usize, TimeRange)>,
    start_time: i64,
) -> Result<(), String> {
    let cc = concurrency.max(1);
    let queue = Arc::new(Mutex::new(queue));
    let first_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let stop = Arc::new(AtomicBool::new(false));

    std::thread::scope(|scope| {
        for _ in 0..cc {
            let queue = Arc::clone(&queue);
            let first_error = Arc::clone(&first_error);
            let stop = Arc::clone(&stop);
            let importer = Arc::clone(importer);
            scope.spawn(move || loop {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let item = { queue.lock().unwrap().pop_front() };
                let Some((series, ri, tr)) = item else { return };
                let rt = &client.retentions[ri];
                let meta = RetentionMeta {
                    first_order: rt.first_order.clone(),
                    second_order: rt.second_order.clone(),
                    agg_time: rt.agg_time.clone(),
                };
                let start = start_time - tr.start;
                let end = start_time - tr.end;
                let res = client
                    .get_data(&series, &meta, start, end)
                    .and_then(|data| {
                        if data.timestamps.is_empty() || data.values.is_empty() {
                            return Ok(());
                        }
                        let labels = data.tags.into_iter().collect();
                        importer.input(Series {
                            name: data.metric,
                            labels,
                            timestamps: data.timestamps,
                            values: data.values,
                        })
                    });
                if let Err(e) = res {
                    let mut slot = first_error.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                    stop.store(true, Ordering::SeqCst);
                    return;
                }
            });
        }
    });

    let err = first_error.lock().unwrap().take();
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_duration_units() {
        assert_eq!(convert_duration("1h").unwrap(), 3_600_000);
        assert_eq!(convert_duration("1m").unwrap(), 60_000);
        assert_eq!(convert_duration("1s").unwrap(), 1_000);
        assert_eq!(convert_duration("500ms").unwrap(), 500);
        assert_eq!(convert_duration("1d").unwrap(), 86_400_000);
        assert_eq!(convert_duration("1w").unwrap(), 7 * 86_400_000);
        assert_eq!(convert_duration("1y").unwrap(), 365 * 86_400_000);
        assert!(convert_duration("1x").is_err());
    }

    #[test]
    fn convert_retention_splits_ranges() {
        // sum-1m-avg : 1h : 1d, second-resolution (msec_time=false), offset 0.
        let r = convert_retention("sum-1m-avg:1h:1d", 0, false).unwrap();
        assert_eq!(r.first_order, "sum");
        assert_eq!(r.agg_time, "1m");
        assert_eq!(r.second_order, "avg");
        assert!(!r.query_ranges.is_empty());
    }

    #[test]
    fn convert_retention_rejects_bad() {
        assert!(convert_retention("bad", 0, false).is_err());
        assert!(convert_retention("a-b:c:d", 0, false).is_err());
    }

    #[test]
    fn sanitize_replaces_invalid_chars() {
        assert_eq!(sanitize_metric_name("sys.cpu.user"), "sys_cpu_user");
        assert_eq!(sanitize_metric_name("ns:metric_1"), "ns:metric_1");
        assert_eq!(sanitize_label_name("has.dot"), "has_dot");
        assert_eq!(sanitize_label_name("a:b"), "a_b");
    }

    #[test]
    fn modify_data_drops_dunder_and_sanitizes() {
        let mut tags = HashMap::new();
        tags.insert("host.name".to_string(), "h1".to_string());
        tags.insert("__internal".to_string(), "x".to_string());
        let m = modify_data("sys.cpu", &tags, false).unwrap();
        assert_eq!(m.metric, "sys_cpu");
        assert_eq!(m.tags.get("host_name"), Some(&"h1".to_string()));
        assert!(!m.tags.contains_key("__internal"));
    }

    #[test]
    fn modify_data_rejects_bad_first_char() {
        assert!(modify_data("1metric", &HashMap::new(), false).is_err());
    }
}
