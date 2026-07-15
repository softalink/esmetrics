//! InfluxDB migration source. Ports `app/vmctl/influx/{influx,parser}.go` and
//! `app/vmctl/influx.go`, reimplementing the small slice of the InfluxDB HTTP
//! `/query` API that vmctl uses (schema exploration via `show field/tag
//! keys`/`show series`, then a `select` per series) instead of depending on
//! the `influxdata/influxdb/client/v2` Go library.
//!
//! **Deviation:** queries are issued non-chunked and the full JSON response is
//! read into memory (upstream streams chunked NDJSON purely to bound memory).
//! Results are identical; only peak memory per query differs.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use reqwest::blocking::Client;

use crate::importer::{Importer, Series as ImportSeries};
use crate::timeparse::parse_time_msec;

/// A tag key/value pair on a series.
#[derive(Clone)]
pub(crate) struct LabelPair {
    pub(crate) name: String,
    pub(crate) value: String,
}

/// A concrete (measurement, field, tags) series to fetch. Ports
/// `influx.Series`.
#[derive(Clone)]
pub(crate) struct Series {
    measurement: String,
    field: String,
    label_pairs: Vec<LabelPair>,
    empty_tags: Vec<String>,
}

/// InfluxDB client configuration.
pub(crate) struct Config {
    pub(crate) addr: String,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) retention: String,
    pub(crate) filter_series: String,
    pub(crate) filter_time_start: String,
    pub(crate) filter_time_end: String,
    pub(crate) http: Client,
}

/// InfluxDB HTTP client. Ports `influx.Client`.
pub(crate) struct InfluxClient {
    addr: String,
    user: String,
    password: String,
    database: String,
    retention: String,
    filter_series: String,
    filter_time: String,
    http: Client,
}

/// A decoded query result row-set: a measurement name plus its columns
/// (`column -> values`). Ports `queryValues`.
struct QueryValues {
    name: String,
    values: HashMap<String, Vec<serde_json::Value>>,
}

impl InfluxClient {
    /// Ports `influx.NewClient` (incl. the `/ping` check).
    pub(crate) fn new(cfg: Config) -> Result<InfluxClient, String> {
        let c = InfluxClient {
            addr: cfg.addr.trim_end_matches('/').to_string(),
            user: cfg.user,
            password: cfg.password,
            database: cfg.database,
            retention: cfg.retention,
            filter_series: cfg.filter_series,
            filter_time: time_filter(&cfg.filter_time_start, &cfg.filter_time_end),
            http: cfg.http,
        };
        c.ping()?;
        Ok(c)
    }

    pub(crate) fn database(&self) -> &str {
        &self.database
    }

    fn ping(&self) -> Result<(), String> {
        let rb = self.auth(self.http.get(format!("{}/ping", self.addr)));
        let resp = rb.send().map_err(|e| format!("ping failed: {e}"))?;
        let s = resp.status().as_u16();
        if s != 204 && s != 200 {
            return Err(format!("ping failed: bad status {s}"));
        }
        Ok(())
    }

    fn auth(&self, rb: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
        if self.user.is_empty() {
            rb
        } else {
            rb.basic_auth(&self.user, Some(&self.password))
        }
    }

    /// Runs an InfluxQL command and returns its parsed result rows. Ports
    /// `Client.do` + `parseResult`.
    fn query(&self, command: &str) -> Result<Vec<QueryValues>, String> {
        let rb = self.auth(self.http.get(format!("{}/query", self.addr)).query(&[
            ("q", command),
            ("db", self.database.as_str()),
            ("rp", self.retention.as_str()),
        ]));
        let resp = rb.send().map_err(|e| format!("query error: {e}"))?;
        if resp.status().as_u16() != 200 {
            let body = resp.text().unwrap_or_default();
            return Err(format!("query {command:?} failed: {body}"));
        }
        let body = resp.text().map_err(|e| format!("read error: {e}"))?;
        parse_response(&body, command)
    }

    /// Explores the schema, returning every concrete series to import. Ports
    /// `Client.Explore`.
    pub(crate) fn explore(&self) -> Result<Vec<Series>, String> {
        log::info!("Exploring scheme for database {:?}", self.database);
        let m_fields = self.fields_by_measurement()?;
        if m_fields.is_empty() {
            return Err(format!(
                "found no numeric fields for import in database {:?}",
                self.database
            ));
        }
        let measurement_tags = self.measurement_tags()?;
        let series = self.get_series()?;

        let mut out = Vec::new();
        for s in &series {
            let Some(fields) = m_fields.get(&s.measurement) else {
                log::info!(
                    "skip measurement {:?} since it has no fields",
                    s.measurement
                );
                continue;
            };
            let empty = empty_tags(measurement_tags.get(&s.measurement), &s.label_pairs);
            for field in fields {
                out.push(Series {
                    measurement: s.measurement.clone(),
                    field: field.clone(),
                    label_pairs: s.label_pairs.clone(),
                    empty_tags: empty.clone(),
                });
            }
        }
        Ok(out)
    }

    /// `show field keys` → measurement → non-string field names.
    fn fields_by_measurement(&self) -> Result<HashMap<String, Vec<String>>, String> {
        let qvs = self.query("show field keys")?;
        let mut result = HashMap::new();
        for qv in qvs {
            let types = qv.values.get("fieldType");
            let mut fields = Vec::new();
            if let Some(keys) = qv.values.get("fieldKey") {
                for (i, key) in keys.iter().enumerate() {
                    let ty = types.and_then(|t| t.get(i)).and_then(|v| v.as_str());
                    if ty == Some("string") {
                        continue; // skip non-numeric fields
                    }
                    if let Some(name) = key.as_str() {
                        fields.push(name.to_string());
                    }
                }
            }
            result.insert(qv.name, fields);
        }
        Ok(result)
    }

    /// `show tag keys` → measurement → tag-key set.
    fn measurement_tags(&self) -> Result<HashMap<String, HashSet<String>>, String> {
        let qvs = self.query("show tag keys")?;
        let mut result: HashMap<String, HashSet<String>> = HashMap::new();
        for qv in qvs {
            let entry = result.entry(qv.name).or_default();
            if let Some(keys) = qv.values.get("tagKey") {
                for k in keys {
                    if let Some(k) = k.as_str() {
                        entry.insert(k.to_string());
                    }
                }
            }
        }
        Ok(result)
    }

    /// `show series [filter] [where time…]` → series list.
    fn get_series(&self) -> Result<Vec<Series>, String> {
        let qvs = self.query(&self.series_command())?;
        let mut result = Vec::new();
        for qv in qvs {
            if let Some(keys) = qv.values.get("key") {
                for v in keys {
                    if let Some(s) = v.as_str() {
                        result.push(unmarshal_series(s)?);
                    }
                }
            }
        }
        log::info!("found {} series", result.len());
        Ok(result)
    }

    fn series_command(&self) -> String {
        let mut com = "show series".to_string();
        if !self.filter_series.is_empty() {
            com = format!("{com} {}", self.filter_series);
        }
        if !self.filter_time.is_empty() {
            let join = if com.to_lowercase().contains(" where ") {
                " AND "
            } else {
                " where "
            };
            com = format!("{com}{join}{}", self.filter_time);
        }
        com
    }

    /// Fetches the datapoints for one series. Ports `FetchDataPoints` +
    /// `ChunkedResponse.Next` (non-chunked).
    fn fetch_data_points(&self, s: &Series) -> Result<(Vec<i64>, Vec<f64>), String> {
        let qvs = self.query(&fetch_query(s, &self.filter_time))?;
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for qv in &qvs {
            let times = qv.values.get("time");
            let field_vals = qv.values.get(&s.field);
            let (Some(times), Some(field_vals)) = (times, field_vals) else {
                continue;
            };
            for t in times {
                let ts = t.as_str().ok_or("time value is not a string")?;
                timestamps.push(
                    parse_time_msec(ts).map_err(|e| format!("cannot parse time {ts:?}: {e}"))?,
                );
            }
            for fv in field_vals {
                values.push(to_float64(fv)?);
            }
        }
        Ok((timestamps, values))
    }
}

/// Ports `parseResult` over a (possibly multi-line NDJSON) response body.
fn parse_response(body: &str, command: &str) -> Result<Vec<QueryValues>, String> {
    let mut out = Vec::new();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let resp: InfluxResponse = serde_json::from_str(line)
            .map_err(|e| format!("cannot decode response for {command:?}: {e}"))?;
        for result in resp.results {
            if let Some(err) = result.error {
                return Err(format!("result error for {command:?}: {err}"));
            }
            for row in result.series {
                let mut values: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
                for value_row in &row.values {
                    for (idx, v) in value_row.iter().enumerate() {
                        if let Some(col) = row.columns.get(idx) {
                            values.entry(col.clone()).or_default().push(v.clone());
                        }
                    }
                }
                out.push(QueryValues {
                    name: row.name,
                    values,
                });
            }
        }
    }
    Ok(out)
}

#[derive(serde::Deserialize)]
struct InfluxResponse {
    #[serde(default)]
    results: Vec<InfluxResult>,
}

#[derive(serde::Deserialize)]
struct InfluxResult {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    series: Vec<InfluxSeriesJson>,
}

#[derive(serde::Deserialize)]
struct InfluxSeriesJson {
    #[serde(default)]
    name: String,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    values: Vec<Vec<serde_json::Value>>,
}

/// Ports `toFloat64`.
fn to_float64(v: &serde_json::Value) -> Result<f64, String> {
    match v {
        serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| "bad number".to_string()),
        serde_json::Value::String(s) => s
            .parse()
            .map_err(|_| format!("cannot parse {s:?} as float")),
        serde_json::Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        other => Err(format!("unexpected value type {other}")),
    }
}

/// Ports `timeFilter`.
fn time_filter(start: &str, end: &str) -> String {
    if start.is_empty() && end.is_empty() {
        return String::new();
    }
    let mut tf = String::new();
    if !start.is_empty() {
        tf = format!("time >= '{start}'");
    }
    if !end.is_empty() {
        if !tf.is_empty() {
            tf.push_str(" and ");
        }
        tf.push_str(&format!("time <= '{end}'"));
    }
    tf
}

/// Ports `Series.fetchQuery`.
fn fetch_query(s: &Series, time_filter: &str) -> String {
    let mut conditions: Vec<String> = Vec::new();
    for pair in &s.label_pairs {
        conditions.push(format!(
            "\"{}\"::tag='{}'",
            pair.name,
            escape_value(&pair.value)
        ));
    }
    for label in &s.empty_tags {
        conditions.push(format!("\"{label}\"::tag=''"));
    }
    if !time_filter.is_empty() {
        conditions.push(time_filter.to_string());
    }
    let mut q = format!("select \"{}\" from \"{}\"", s.field, s.measurement);
    if !conditions.is_empty() {
        q.push_str(&format!(" where {}", conditions.join(" and ")));
    }
    q
}

/// Ports `valueEscaper` (`\` → `\\`, `'` → `\'`).
fn escape_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Ports `getEmptyTags`.
fn empty_tags(tags: Option<&HashSet<String>>, label_pairs: &[LabelPair]) -> Vec<String> {
    let Some(tags) = tags else { return Vec::new() };
    if tags.is_empty() {
        return Vec::new();
    }
    let present: HashSet<&str> = label_pairs.iter().map(|p| p.name.as_str()).collect();
    tags.iter()
        .filter(|t| !present.contains(t.as_str()))
        .cloned()
        .collect()
}

// ---- series key unmarshaling (ports parser.go) ----

/// Parses an Influx series key (`measurement,tag=v,tag2=v2`). Ports
/// `Series.unmarshal`.
fn unmarshal_series(v: &str) -> Result<Series, String> {
    let no_escape = !v.contains('\\');
    match next_unescaped_char(v, b',', no_escape) {
        None => Ok(Series {
            measurement: unescape_tag_value(v, no_escape),
            field: String::new(),
            label_pairs: Vec::new(),
            empty_tags: Vec::new(),
        }),
        Some(n) => Ok(Series {
            measurement: unescape_tag_value(&v[..n], no_escape),
            field: String::new(),
            label_pairs: unmarshal_tags(&v[n + 1..], no_escape)?,
            empty_tags: Vec::new(),
        }),
    }
}

fn unmarshal_tags(mut s: &str, no_escape: bool) -> Result<Vec<LabelPair>, String> {
    let mut result = Vec::new();
    loop {
        match next_unescaped_char(s, b',', no_escape) {
            None => {
                let lp = unmarshal_tag(s, no_escape)?;
                if lp.name.is_empty() || lp.value.is_empty() {
                    return Ok(Vec::new());
                }
                result.push(lp);
                return Ok(result);
            }
            Some(n) => {
                let lp = unmarshal_tag(&s[..n], no_escape)?;
                s = &s[n + 1..];
                if lp.name.is_empty() || lp.value.is_empty() {
                    continue;
                }
                result.push(lp);
            }
        }
    }
}

fn unmarshal_tag(s: &str, no_escape: bool) -> Result<LabelPair, String> {
    match next_unescaped_char(s, b'=', no_escape) {
        None => Err(format!("missing tag value for {s:?}")),
        Some(n) => Ok(LabelPair {
            name: unescape_tag_value(&s[..n], no_escape),
            value: unescape_tag_value(&s[n + 1..], no_escape),
        }),
    }
}

fn unescape_tag_value(s: &str, no_escape: bool) -> String {
    if no_escape || !s.contains('\\') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut dst: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            if i + 1 >= bytes.len() {
                dst.push(b'\\');
                break;
            }
            let ch = bytes[i + 1];
            if ch != b' ' && ch != b',' && ch != b'=' && ch != b'\\' {
                dst.push(b'\\');
            }
            dst.push(ch);
            i += 2;
        } else {
            dst.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&dst).into_owned()
}

/// Ports `nextUnescapedChar` — index of the next `ch` not preceded by an odd
/// run of backslashes.
fn next_unescaped_char(s: &str, ch: u8, no_escape: bool) -> Option<usize> {
    let b = s.as_bytes();
    if no_escape {
        return b.iter().position(|&c| c == ch);
    }
    let mut start = 0;
    loop {
        let rel = b[start..].iter().position(|&c| c == ch)?;
        let n = start + rel;
        if n == 0 {
            return Some(n);
        }
        // Count preceding backslashes.
        let mut slashes = 0;
        let mut k = n;
        while k > 0 && b[k - 1] == b'\\' {
            slashes += 1;
            k -= 1;
        }
        if slashes % 2 == 0 {
            return Some(n);
        }
        start = n + 1;
    }
}

// ---- processor (ports influx.go) ----

const DB_LABEL: &str = "db";
const NAME_LABEL: &str = "__name__";
const VALUE_FIELD: &str = "value";

/// Configuration for the influx migration.
pub(crate) struct InfluxProcessorConfig {
    pub(crate) client: Arc<InfluxClient>,
    pub(crate) concurrency: usize,
    pub(crate) separator: String,
    pub(crate) skip_db_label: bool,
    pub(crate) prom_mode: bool,
    pub(crate) assume_yes: bool,
}

/// Runs the influx migration. Ports `influxProcessor.run`.
pub(crate) fn run(cfg: &InfluxProcessorConfig, importer: Importer) -> Result<(), String> {
    let series = cfg.client.explore()?;
    if series.is_empty() {
        return Err("found no timeseries to import".to_string());
    }
    if !crate::prompt(
        cfg.assume_yes,
        &format!("Found {} timeseries to import. Continue?", series.len()),
    ) {
        return Ok(());
    }

    let importer = Arc::new(importer);
    let queue = Arc::new(Mutex::new(
        series
            .into_iter()
            .collect::<std::collections::VecDeque<_>>(),
    ));
    let first_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let stop = Arc::new(AtomicBool::new(false));
    let cc = cfg.concurrency.max(1);

    std::thread::scope(|scope| {
        for _ in 0..cc {
            let queue = Arc::clone(&queue);
            let first_error = Arc::clone(&first_error);
            let stop = Arc::clone(&stop);
            let importer = Arc::clone(&importer);
            let client = Arc::clone(&cfg.client);
            let separator = cfg.separator.clone();
            let skip_db_label = cfg.skip_db_label;
            let prom_mode = cfg.prom_mode;
            scope.spawn(move || loop {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let s = { queue.lock().unwrap().pop_front() };
                let Some(s) = s else { return };
                let res =
                    process_series(&client, &importer, &s, &separator, skip_db_label, prom_mode);
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

    if let Some(e) = first_error.lock().unwrap().take() {
        return Err(format!("import process failed: {e}"));
    }

    match Arc::try_unwrap(importer) {
        Ok(im) => im.close(),
        Err(_) => Err("importer still referenced at shutdown".to_string()),
    }?;
    log::info!("Import finished!");
    Ok(())
}

/// Ports `influxProcessor.do`.
fn process_series(
    client: &InfluxClient,
    importer: &Importer,
    s: &Series,
    separator: &str,
    skip_db_label: bool,
    prom_mode: bool,
) -> Result<(), String> {
    let (timestamps, values) = client.fetch_data_points(s)?;

    let mut name = if s.measurement.is_empty() {
        s.field.clone()
    } else {
        format!("{}{}{}", s.measurement, separator, s.field)
    };

    let mut labels: Vec<(String, String)> = Vec::with_capacity(s.label_pairs.len());
    let mut contains_db = false;
    for lp in &s.label_pairs {
        if lp.name == DB_LABEL {
            contains_db = true;
        } else if lp.name == NAME_LABEL && s.field == VALUE_FIELD && prom_mode {
            name = lp.value.clone();
        }
        labels.push((lp.name.clone(), lp.value.clone()));
    }
    if !contains_db && !skip_db_label {
        labels.push((DB_LABEL.to_string(), client.database().to_string()));
    }

    if timestamps.is_empty() {
        return Ok(());
    }
    importer.input(ImportSeries {
        name,
        labels,
        timestamps,
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_series_key() {
        let s = unmarshal_series("cpu,host=web01,region=us").unwrap();
        assert_eq!(s.measurement, "cpu");
        assert_eq!(s.label_pairs.len(), 2);
        assert_eq!(s.label_pairs[0].name, "host");
        assert_eq!(s.label_pairs[0].value, "web01");
    }

    #[test]
    fn parses_measurement_only_key() {
        let s = unmarshal_series("uptime").unwrap();
        assert_eq!(s.measurement, "uptime");
        assert!(s.label_pairs.is_empty());
    }

    #[test]
    fn handles_escaped_commas() {
        let s = unmarshal_series(r"m\,x,host=a").unwrap();
        assert_eq!(s.measurement, "m,x");
        assert_eq!(s.label_pairs[0].name, "host");
    }

    #[test]
    fn fetch_query_builds_select() {
        let s = Series {
            measurement: "cpu".into(),
            field: "usage".into(),
            label_pairs: vec![LabelPair {
                name: "host".into(),
                value: "a".into(),
            }],
            empty_tags: vec!["rack".into()],
        };
        let q = fetch_query(&s, "time >= '2024-01-01T00:00:00Z'");
        assert_eq!(
            q,
            "select \"usage\" from \"cpu\" where \"host\"::tag='a' and \"rack\"::tag='' and time >= '2024-01-01T00:00:00Z'"
        );
    }

    #[test]
    fn time_filter_combines_bounds() {
        assert_eq!(time_filter("", ""), "");
        assert_eq!(time_filter("A", ""), "time >= 'A'");
        assert_eq!(time_filter("A", "B"), "time >= 'A' and time <= 'B'");
    }

    #[test]
    fn escapes_value() {
        assert_eq!(escape_value(r"a'b\c"), r"a\'b\\c");
    }

    #[test]
    fn to_float_handles_types() {
        assert_eq!(to_float64(&serde_json::json!(3.5)).unwrap(), 3.5);
        assert_eq!(to_float64(&serde_json::json!("7")).unwrap(), 7.0);
        assert_eq!(to_float64(&serde_json::json!(true)).unwrap(), 1.0);
    }

    #[test]
    fn parses_show_field_keys_response() {
        let body = r#"{"results":[{"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["usage","float"],["note","string"]]}]}]}"#;
        let qvs = parse_response(body, "show field keys").unwrap();
        assert_eq!(qvs.len(), 1);
        assert_eq!(qvs[0].name, "cpu");
        assert_eq!(qvs[0].values.get("fieldKey").unwrap().len(), 2);
    }
}
