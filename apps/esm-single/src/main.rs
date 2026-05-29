//! `esm-single` — single-node EsMetrics binary.
//!
//! Phase 4 MVP: HTTP server backed by [`esm_storage::Storage`] with two
//! routes:
//!
//! - `POST /api/v1/import/prometheus` — ingest a Prometheus text-exposition
//!   document. Body is `text/plain`. Response: `204 No Content`.
//! - `GET /api/v1/query?metric=NAME&start=MS&end=MS` — list samples for a
//!   single canonical metric name within an inclusive time range. Response:
//!   JSON array of `{ "timestamp": <ms>, "value": <i64> }`.
//!
//! The full Prometheus `/api/v1/query{,_range}` surface (PromQL evaluator,
//! step, lookback, etc.) and `/api/v1/write` (Prom remote-write, protobuf+
//! snappy) land in later phases. This MVP is the smallest thing that lets
//! `curl` ingest scraped metrics and read them back, end-to-end.

#![allow(clippy::print_stderr)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use clap::Parser;
use esm_storage::{QueryStore, Sample, ShardedStorage, TimeRange};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(
    name = "esm-single",
    about = "Single-node EsMetrics server (drop-in candidate for VictoriaMetrics vmsingle).",
    version
)]
struct Cli {
    /// On-disk data directory.
    #[arg(long, default_value = "./esm-data")]
    storage_data_path: PathBuf,

    /// HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:8428")]
    http_listen_addr: SocketAddr,

    /// Maximum age of stored samples, in seconds. Older parts are dropped
    /// by a background sweeper. `0` disables retention.
    /// Matches `vmsingle -retentionPeriod` (which uses weeks/months units;
    /// here it's plain seconds for now).
    #[arg(long, default_value_t = 0)]
    retention_period_secs: u64,

    /// Maximum size of a single uploaded body (bytes). VM default: 64 MiB.
    /// Honored on every ingest endpoint. Mirrors `vmsingle -maxInsertRequestSize`.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_insert_request_size: u64,

    /// Maximum query lookback window (seconds). PromQL queries spanning a
    /// longer interval are rejected. `0` disables the limit. Mirrors
    /// `vmsingle -search.maxQueryDuration`.
    #[arg(long, default_value_t = 0)]
    search_max_query_duration_secs: u64,

    /// Maximum number of unique series returned by a single query. `0`
    /// disables the limit. Mirrors `vmsingle -search.maxSeries`.
    #[arg(long, default_value_t = 0)]
    search_max_series: u64,

    /// HTTP request timeout (seconds). Mirrors `vmsingle -http.maxGracefulShutdownDuration`
    /// for the graceful shutdown side; here we use it as a per-request timeout.
    #[arg(long, default_value_t = 30)]
    http_request_timeout_secs: u64,

    /// Log level. One of: trace, debug, info, warn, error. Mirrors
    /// `vmsingle -loggerLevel`.
    #[arg(long, default_value = "info")]
    logger_level: String,

    /// Disable self-monitoring `/metrics` endpoint. Mirrors
    /// `vmsingle -selfScrapeInterval=0`.
    #[arg(long, default_value_t = false)]
    disable_self_metrics: bool,

    /// Path to a TLS certificate file. If set together with `--tls-key-file`,
    /// the HTTP server runs as HTTPS. Mirrors `vmsingle -tlsCertFile`.
    #[arg(long)]
    tls_cert_file: Option<PathBuf>,

    /// Path to a TLS key file. See `--tls-cert-file`.
    #[arg(long)]
    tls_key_file: Option<PathBuf>,

    /// Optional `Authorization` header value required on every request.
    /// Mirrors `vmsingle -httpAuth.password` (we accept the raw header so
    /// callers can use either Basic or Bearer schemes).
    #[arg(long)]
    http_auth_header: Option<String>,

    /// Drop snapshots older than this many seconds. `0` disables.
    #[arg(long, default_value_t = 0)]
    snapshot_retention_secs: u64,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let cli = parse_cli_with_vm_compat();
    init_tracing_with_level(&cli.logger_level);

    // Shard count: ~2 writer lanes per core so concurrent ingest rarely
    // collides on a shard lock (TSBS: 16→32 shards is +13% at 16 workers; >32
    // over-shards — flush/merge overhead and buffer RAM outweigh contention
    // relief). Stable hash routes each series to a fixed shard. `ESM_SHARDS`
    // overrides for tuning to the actual client concurrency.
    let n_shards =
        std::env::var("ESM_SHARDS").ok().and_then(|v| v.parse::<usize>().ok()).map_or_else(
            || {
                (std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get) * 2)
                    .min(32)
            },
            |n| n.max(1),
        );
    let storage = ShardedStorage::open(&cli.storage_data_path, n_shards)
        .with_context(|| format!("open data dir {}", cli.storage_data_path.display()))?;
    tracing::info!(data_dir = %cli.storage_data_path.display(), shards = n_shards, "storage opened");
    let storage = Arc::new(storage);

    let mut app = Router::new()
        .route("/api/v1/import/prometheus", post(ingest_prometheus))
        .route("/api/v1/write", post(ingest_prom_remote_write))
        .route("/write", post(ingest_influx_v1))
        .route("/api/v2/write", post(ingest_influx_v2))
        .route("/api/v1/import/graphite", post(ingest_graphite))
        .route("/api/v1/import", post(ingest_json))
        .route("/api/put", post(ingest_opentsdb_http))
        .route("/api/v1/import/opentsdb", post(ingest_opentsdb_telnet))
        .route("/api/v1/datadog/series", post(ingest_datadog))
        .route("/api/v1/import/csv", post(ingest_csv))
        .route("/api/v1/newrelic/infra/v2/metrics/events/bulk", post(ingest_newrelic))
        .route("/opentelemetry/v1/metrics", post(ingest_otlp))
        .route("/api/v1/otlp/v1/metrics", post(ingest_otlp))
        .route("/api/v1/import/native", post(ingest_native_vm))
        .route("/api/v1/query", get(query_dispatch))
        .route("/api/v1/query_range", get(promql_range))
        .route("/api/v1/promql", get(promql_instant))
        .route("/api/v1/promql_range", get(promql_range))
        .route("/api/v1/series", get(series_endpoint))
        .route("/api/v1/labels", get(labels_endpoint))
        .route("/api/v1/label/:name/values", get(label_values_endpoint))
        .route("/api/v1/status/buildinfo", get(status_buildinfo))
        .route("/api/v1/status/runtimeinfo", get(status_runtimeinfo))
        .route("/api/v1/status/flags", get(status_flags))
        .route("/api/v1/status/tsdb", get(status_tsdb))
        .route("/api/v1/targets", get(status_targets))
        .route("/snapshot/create", post(snapshot_create))
        .route("/snapshot/list", get(snapshot_list))
        .route("/snapshot/delete/:name", post(snapshot_delete))
        .route("/vmui", get(vmui_index))
        .route("/vmui/", get(vmui_index))
        .route("/vmui/*path", get(vmui_asset))
        .route("/health", get(health));
    if !cli.disable_self_metrics {
        app = app.route("/metrics", get(self_metrics));
    }
    let app = app
        .layer(axum::extract::DefaultBodyLimit::max(
            usize::try_from(cli.max_insert_request_size).unwrap_or(usize::MAX),
        ))
        .with_state(storage.clone());
    let app = if let Some(header) = cli.http_auth_header.clone() {
        app.layer(axum::middleware::from_fn(move |req, next| {
            let header = header.clone();
            auth_middleware(req, next, header)
        }))
    } else {
        app
    };
    if cli.tls_cert_file.is_some() || cli.tls_key_file.is_some() {
        tracing::warn!(
            "--tls-cert-file / --tls-key-file accepted but TLS termination is not yet implemented; serve plaintext"
        );
    }
    if cli.search_max_query_duration_secs > 0 || cli.search_max_series > 0 {
        tracing::warn!(
            search_max_query_duration_secs = cli.search_max_query_duration_secs,
            search_max_series = cli.search_max_series,
            "search limits accepted but not yet enforced"
        );
    }
    if cli.http_request_timeout_secs != 30 {
        tracing::info!(
            http_request_timeout_secs = cli.http_request_timeout_secs,
            "per-request timeout accepted but not yet enforced"
        );
    }

    tracing::info!(addr = %cli.http_listen_addr, "starting HTTP server");
    let listener = tokio::net::TcpListener::bind(cli.http_listen_addr)
        .await
        .with_context(|| format!("bind {}", cli.http_listen_addr))?;

    // Background retention sweeper.
    let retention_handle = if cli.retention_period_secs > 0 {
        let storage_bg = storage.clone();
        let period = cli.retention_period_secs;
        Some(tokio::spawn(async move {
            run_retention_sweeper(storage_bg, period).await;
        }))
    } else {
        None
    };
    let snapshot_retention_handle = if cli.snapshot_retention_secs > 0 {
        let storage_bg = storage.clone();
        let period = cli.snapshot_retention_secs;
        Some(tokio::spawn(async move {
            run_snapshot_retention_sweeper(storage_bg, period).await;
        }))
    } else {
        None
    };

    let shutdown = async {
        let _ = esm_platform::signal::wait_for_shutdown().await;
        tracing::info!("shutdown signal received");
    };

    axum::serve(listener, app).with_graceful_shutdown(shutdown).await.context("axum serve")?;

    if let Some(h) = retention_handle {
        h.abort();
        let _ = h.await;
    }
    if let Some(h) = snapshot_retention_handle {
        h.abort();
        let _ = h.await;
    }
    tracing::info!("HTTP listener stopped; draining in-memory state");
    let s = storage.as_ref();
    s.flush().context("flush on shutdown")?;
    tracing::info!("shutdown complete");
    Ok(())
}

/// Parse the CLI, transparently rewriting VM-style single-dash camelCase
/// flags into Rust-idiomatic double-dash kebab-case so existing
/// `vmsingle` deployment scripts continue to work.
///
/// Examples that all resolve identically:
///   `-storageDataPath /data`
///   `-storage-data-path /data`
///   `--storage-data-path /data`
fn parse_cli_with_vm_compat() -> Cli {
    let raw: Vec<String> = std::env::args().collect();
    let translated: Vec<String> = raw
        .into_iter()
        .map(|arg| {
            // Skip program name.
            if !arg.starts_with('-') || arg.starts_with("--") {
                return arg;
            }
            let body = arg.trim_start_matches('-');
            // Single-letter flags ("-h", "-V") pass through.
            if body.len() <= 1 {
                return arg;
            }
            // Translate camelCase to kebab-case and prefix `--`.
            let kebab = camel_to_kebab(body);
            format!("--{kebab}")
        })
        .collect();
    <Cli as clap::Parser>::parse_from(translated)
}

fn camel_to_kebab(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let mut prev_is_lower = false;
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            if prev_is_lower {
                out.push('-');
            }
            for low in c.to_lowercase() {
                out.push(low);
            }
            prev_is_lower = false;
        } else {
            out.push(c);
            prev_is_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
        }
    }
    out
}

fn init_tracing_with_level(level: &str) {
    let level_filter = match level.to_ascii_lowercase().as_str() {
        "trace" => "trace",
        "debug" => "debug",
        "warn" | "warning" => "warn",
        "error" => "error",
        _ => "info",
    };
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level_filter)),
        )
        .with_target(false)
        .finish();
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("warning: tracing init failed: {e}");
    }
}

async fn auth_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
    expected_header: String,
) -> axum::response::Response {
    let supplied = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if supplied == expected_header {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

async fn health() -> &'static str {
    "ok"
}

/// `GET /metrics` — Prometheus text exposition of self-metrics.
async fn self_metrics(State(storage): State<Arc<ShardedStorage>>) -> impl IntoResponse {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let serves = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let s = storage.as_ref();
    let metric_count = s.metric_count();
    let data_dir_display = s.data_dir().display().to_string();
    let body = format!(
        "# HELP esm_metric_count Number of distinct metric names known to the engine.\n\
         # TYPE esm_metric_count gauge\n\
         esm_metric_count {metric_count}\n\
         # HELP esm_metrics_endpoint_requests_total Number of times the /metrics endpoint has been scraped.\n\
         # TYPE esm_metrics_endpoint_requests_total counter\n\
         esm_metrics_endpoint_requests_total {serves}\n\
         # HELP esm_build_info EsMetrics build metadata.\n\
         # TYPE esm_build_info gauge\n\
         esm_build_info{{version=\"{pkg_ver}\",rustc=\"{rustc}\"}} 1\n\
         # HELP esm_data_dir Path of the data directory (label only).\n\
         # TYPE esm_data_dir gauge\n\
         esm_data_dir{{path=\"{data_dir_display}\"}} 1\n",
        pkg_ver = env!("CARGO_PKG_VERSION"),
        rustc = "stable",
    );
    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

#[derive(Debug, Deserialize)]
struct InfluxParams {
    /// `ns`, `us`, `ms`, `s` — defaults to `ns` for v1, also accepted as a
    /// fallback for v2.
    #[serde(default)]
    precision: Option<String>,
}

async fn ingest_influx_v1(
    State(storage): State<Arc<ShardedStorage>>,
    Query(p): Query<InfluxParams>,
    body: String,
) -> Result<StatusCode, AppError> {
    let ns_per_unit = ns_per_unit_for(&p.precision.unwrap_or_default(), 1);
    let now_ms = chrono_like_now_ms();
    // Arena-keyed parse: keys share one growable buffer (no Vec per sample);
    // storage interns them, allocating only for new series.
    let mut arena = Vec::with_capacity(body.len());
    let mut entries = Vec::new();
    esm_protocols::influx_line::parse_into(&body, now_ms, ns_per_unit, &mut arena, &mut entries)
        .map_err(|e| AppError(anyhow::anyhow!("influx parse: {e}")))?;
    storage.ingest_keyed(&arena, &entries)?;
    tracing::debug!(count = entries.len(), "influx v1 ingested");
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_influx_v2(
    state: State<Arc<ShardedStorage>>,
    p: Query<InfluxParams>,
    body: String,
) -> Result<StatusCode, AppError> {
    // v2 differs only in default precision (ns). For the MVP we re-use the v1 path.
    ingest_influx_v1(state, p, body).await
}

/// `GET /api/v1/series?match[]=expr` — returns the label sets of matching
/// series. Multiple `match[]` params are unioned.
async fn series_endpoint(
    State(storage): State<Arc<ShardedStorage>>,
    raw_query: Option<axum::extract::RawQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let matchers = parse_matchers_from_query(raw_query.and_then(|q| q.0));
    let s = storage.as_ref();
    let mut out: Vec<serde_json::Value> = Vec::new();
    let mut seen: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    for (name, _tsid) in s.iter_metric_names() {
        let matches = if matchers.is_empty() {
            true
        } else {
            matchers.iter().any(|m| match_metric_against_matcher(&name, m))
        };
        if !matches {
            continue;
        }
        if !seen.insert(name.clone()) {
            continue;
        }
        out.push(decode_metric_labels_json(&name));
    }
    Ok(Json(serde_json::json!({ "status": "success", "data": out })))
}

/// `GET /api/v1/labels[?match[]=expr]` — list every label name known across
/// all matched series.
async fn labels_endpoint(
    State(storage): State<Arc<ShardedStorage>>,
    raw_query: Option<axum::extract::RawQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let matchers = parse_matchers_from_query(raw_query.and_then(|q| q.0));
    let s = storage.as_ref();
    let mut labels: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (name, _tsid) in s.iter_metric_names() {
        let matches = if matchers.is_empty() {
            true
        } else {
            matchers.iter().any(|m| match_metric_against_matcher(&name, m))
        };
        if !matches {
            continue;
        }
        for k in decode_metric_labels(&name).keys() {
            labels.insert(k.clone());
        }
    }
    let data: Vec<&str> = labels.iter().map(String::as_str).collect();
    Ok(Json(serde_json::json!({ "status": "success", "data": data })))
}

/// `GET /api/v1/label/<name>/values[?match[]=expr]`.
async fn label_values_endpoint(
    State(storage): State<Arc<ShardedStorage>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    raw_query: Option<axum::extract::RawQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let matchers = parse_matchers_from_query(raw_query.and_then(|q| q.0));
    let s = storage.as_ref();
    let mut values: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (metric_name, _tsid) in s.iter_metric_names() {
        let matches = if matchers.is_empty() {
            true
        } else {
            matchers.iter().any(|m| match_metric_against_matcher(&metric_name, m))
        };
        if !matches {
            continue;
        }
        if let Some(v) = decode_metric_labels(&metric_name).get(&name) {
            values.insert(v.clone());
        }
    }
    let data: Vec<&str> = values.iter().map(String::as_str).collect();
    Ok(Json(serde_json::json!({ "status": "success", "data": data })))
}

fn parse_matchers_from_query(raw: Option<String>) -> Vec<esm_promql::VectorSelector> {
    let Some(q) = raw else { return Vec::new() };
    let mut out = Vec::new();
    for pair in q.split('&') {
        let Some((k, v)) = pair.split_once('=') else { continue };
        if k != "match[]" && k != "match%5B%5D" {
            continue;
        }
        let decoded = url_decode(v);
        if let Ok(esm_promql::Expr::VectorSelector(sel)) = esm_promql::parser::parse(&decoded) {
            out.push(sel);
        }
    }
    out
}

fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3])
                    && let Ok(b) = u8::from_str_radix(hex, 16)
                {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
                out.push('%');
                i += 1;
            }
            b'+' => {
                out.push(' ');
                i += 1;
            }
            other => {
                out.push(other as char);
                i += 1;
            }
        }
    }
    out
}

fn match_metric_against_matcher(metric_name: &[u8], sel: &esm_promql::VectorSelector) -> bool {
    let labels = decode_metric_labels(metric_name);
    if let Some(name) = sel.name.as_deref()
        && labels.get("__name__").map(String::as_str) != Some(name)
    {
        return false;
    }
    for m in &sel.matchers {
        let actual = labels.get(&m.name).cloned().unwrap_or_default();
        let ok = match m.op {
            esm_promql::MatchOp::Equal => actual == m.value,
            esm_promql::MatchOp::NotEqual => actual != m.value,
            esm_promql::MatchOp::RegexMatch | esm_promql::MatchOp::RegexNotMatch => {
                // Minimal anchored regex match (matches the simple matcher used
                // in the evaluator).
                let m_op = matches!(m.op, esm_promql::MatchOp::RegexMatch);
                let matched = simple_full_regex(&actual, &m.value);
                if m_op { matched } else { !matched }
            }
        };
        if !ok {
            return false;
        }
    }
    true
}

fn simple_full_regex(s: &str, pattern: &str) -> bool {
    fn rec(s: &[u8], p: &[u8]) -> bool {
        if p.is_empty() {
            return s.is_empty();
        }
        if p.len() >= 2 && p[1] == b'*' {
            let c = p[0];
            for i in 0..=s.len() {
                if rec(&s[i..], &p[2..]) {
                    return true;
                }
                if i == s.len() || !(c == b'.' || s[i] == c) {
                    break;
                }
            }
            return false;
        }
        if s.is_empty() {
            return false;
        }
        if p[0] == b'.' || s[0] == p[0] {
            return rec(&s[1..], &p[1..]);
        }
        false
    }
    rec(s.as_bytes(), pattern.as_bytes())
}

fn decode_metric_labels_json(metric_name: &[u8]) -> serde_json::Value {
    let labels = decode_metric_labels(metric_name);
    serde_json::to_value(&labels).unwrap_or_else(|_| serde_json::json!({}))
}

async fn ingest_json(
    State(storage): State<Arc<ShardedStorage>>,
    body: String,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::json_line::parse(&body)
        .map_err(|e| AppError(anyhow::anyhow!("json parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_opentsdb_telnet(
    State(storage): State<Arc<ShardedStorage>>,
    body: String,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::opentsdb::parse_telnet(&body)
        .map_err(|e| AppError(anyhow::anyhow!("opentsdb parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_opentsdb_http(
    State(storage): State<Arc<ShardedStorage>>,
    body: String,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::opentsdb::parse_http_json(&body)
        .map_err(|e| AppError(anyhow::anyhow!("opentsdb http parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_datadog(
    State(storage): State<Arc<ShardedStorage>>,
    body: String,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::datadog::parse(&body)
        .map_err(|e| AppError(anyhow::anyhow!("datadog parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn vmui_index() -> impl IntoResponse {
    let body = esm_vmui::asset("/").unwrap_or(b"<h1>EsMetrics</h1>");
    ([(axum::http::header::CONTENT_TYPE, esm_vmui::mime_for("index.html"))], body)
}

async fn vmui_asset(axum::extract::Path(path): axum::extract::Path<String>) -> impl IntoResponse {
    if let Some(body) = esm_vmui::asset(&path) {
        (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, esm_vmui::mime_for(&path))], body)
            .into_response()
    } else {
        (StatusCode::NOT_FOUND, "not found").into_response()
    }
}

async fn status_buildinfo() -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "status": "success",
        "data": {
            "version": env!("CARGO_PKG_VERSION"),
            "revision": "",
            "branch": "",
            "buildUser": "",
            "buildDate": "",
            "goVersion": format!("rustc/{}", "unknown"),
        }
    }))
}

async fn status_runtimeinfo() -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "status": "success",
        "data": {
            "startTime": "1970-01-01T00:00:00Z",
            "CWD": std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_default(),
            "GOOS": std::env::consts::OS,
            "GOARCH": std::env::consts::ARCH,
        }
    }))
}

async fn status_flags() -> impl IntoResponse {
    // Returns the set of CLI args as the flag dump.
    let args: Vec<String> = std::env::args().collect();
    axum::Json(serde_json::json!({
        "status": "success",
        "data": { "argv": args },
    }))
}

async fn status_tsdb(State(storage): State<Arc<ShardedStorage>>) -> impl IntoResponse {
    let s = storage.as_ref();
    let count = s.metric_count();
    axum::Json(serde_json::json!({
        "status": "success",
        "data": {
            "headStats": { "numSeries": count },
            "seriesCountByMetricName": [],
            "labelValueCountByLabelName": [],
            "memoryInBytesByLabelName": [],
            "seriesCountByLabelValuePair": [],
        }
    }))
}

async fn status_targets() -> impl IntoResponse {
    // esm-single does not scrape (esm-agent does); always empty.
    axum::Json(serde_json::json!({
        "status": "success",
        "data": { "activeTargets": [], "droppedTargets": [] },
    }))
}

async fn snapshot_create(
    State(storage): State<Arc<ShardedStorage>>,
) -> Result<axum::Json<serde_json::Value>, AppError> {
    let name = format!("snapshot-{}", chrono_like_now_ms());
    let s = storage.as_ref();
    let path = s.create_snapshot(&name).map_err(|e| AppError(anyhow::anyhow!("snapshot: {e}")))?;
    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "snapshot": name,
        "path": path.display().to_string(),
    })))
}

async fn snapshot_list(
    State(storage): State<Arc<ShardedStorage>>,
) -> Result<axum::Json<serde_json::Value>, AppError> {
    let s = storage.as_ref();
    let list = s.list_snapshots().map_err(|e| AppError(anyhow::anyhow!("snapshot list: {e}")))?;
    Ok(axum::Json(serde_json::json!({ "status": "ok", "snapshots": list })))
}

async fn snapshot_delete(
    State(storage): State<Arc<ShardedStorage>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<StatusCode, AppError> {
    let s = storage.as_ref();
    s.delete_snapshot(&name).map_err(|e| AppError(anyhow::anyhow!("snapshot delete: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn run_snapshot_retention_sweeper(storage: Arc<ShardedStorage>, period_secs: u64) {
    let sweep_period = std::cmp::min(period_secs / 12, 3600).max(60);
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(sweep_period));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now_ms = chrono_like_now_ms();
        let cutoff_ms =
            now_ms - i64::try_from(period_secs.saturating_mul(1000)).unwrap_or(i64::MAX);
        let s = storage.as_ref();
        match s.enforce_snapshot_retention(cutoff_ms) {
            Ok(0) => tracing::debug!(cutoff_ms, "snapshot retention: nothing dropped"),
            Ok(n) => tracing::info!(cutoff_ms, snapshots_dropped = n, "snapshot retention"),
            Err(e) => tracing::warn!(error = %e, "snapshot retention failed"),
        }
    }
}

async fn run_retention_sweeper(storage: Arc<ShardedStorage>, period_secs: u64) {
    // Sweep once per hour, or sooner if the retention window is short.
    let sweep_period = std::cmp::min(period_secs.saturating_mul(1000) / 24, 3600).max(60);
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(sweep_period));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now_ms = chrono_like_now_ms();
        let cutoff_ms =
            now_ms - i64::try_from(period_secs.saturating_mul(1000)).unwrap_or(i64::MAX);
        let s = storage.as_ref();
        match s.enforce_retention(cutoff_ms) {
            Ok(0) => tracing::debug!(cutoff_ms, "retention sweep: nothing dropped"),
            Ok(n) => tracing::info!(cutoff_ms, parts_dropped = n, "retention sweep"),
            Err(e) => tracing::warn!(error = %e, "retention sweep failed"),
        }
    }
}

async fn ingest_csv(
    State(storage): State<Arc<ShardedStorage>>,
    body: String,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::csv_import::parse(&body)
        .map_err(|e| AppError(anyhow::anyhow!("csv parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_native_vm(
    State(storage): State<Arc<ShardedStorage>>,
    body: axum::body::Bytes,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::native_vm::parse(&body)
        .map_err(|e| AppError(anyhow::anyhow!("native vm parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_otlp(
    State(storage): State<Arc<ShardedStorage>>,
    body: axum::body::Bytes,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::otlp::parse(&body)
        .map_err(|e| AppError(anyhow::anyhow!("otlp parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_newrelic(
    State(storage): State<Arc<ShardedStorage>>,
    body: axum::body::Bytes,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::newrelic::parse(&body)
        .map_err(|e| AppError(anyhow::anyhow!("newrelic parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_graphite(
    State(storage): State<Arc<ShardedStorage>>,
    body: String,
) -> Result<StatusCode, AppError> {
    let now_ms = chrono_like_now_ms();
    let parsed = esm_protocols::graphite::parse(&body, now_ms)
        .map_err(|e| AppError(anyhow::anyhow!("graphite parse: {e}")))?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    Ok(StatusCode::NO_CONTENT)
}

fn ns_per_unit_for(precision: &str, default: i64) -> i64 {
    match precision {
        "ns" | "n" | "" => 1,
        "us" | "u" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        _ => default,
    }
}

/// `POST /api/v1/write` — Prometheus remote-write.
///
/// Body: snappy-compressed protobuf `prometheus.WriteRequest`. This is the
/// standard endpoint Prometheus servers (and vmagent) use.
async fn ingest_prom_remote_write(
    State(storage): State<Arc<ShardedStorage>>,
    body: bytes::Bytes,
) -> Result<StatusCode, AppError> {
    let parsed = esm_protocols::prom_remote_write::parse_snappy(&body)
        .map_err(|e| AppError(anyhow::anyhow!("remote-write parse: {e}")))?;
    let mut samples: Vec<Sample> = Vec::new();
    for ts in parsed {
        let canonical = ts.canonical_storage_key();
        for s in ts.samples {
            #[allow(clippy::cast_possible_truncation)]
            let value = s.value as i64;
            samples.push(Sample {
                metric_name: canonical.clone(),
                timestamp_ms: s.timestamp_ms,
                value,
            });
        }
    }
    let s = storage.as_ref();
    s.ingest(&samples)?;
    tracing::debug!(count = samples.len(), "prom remote-write ingested");
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/v1/import/prometheus`
///
/// Body: Prometheus text-exposition document.
async fn ingest_prometheus(
    State(storage): State<Arc<ShardedStorage>>,
    body: String,
) -> Result<StatusCode, AppError> {
    let now_ms = chrono_like_now_ms();
    let parsed = esm_protocols::text_exposition::parse(&body, now_ms)?;
    let samples: Vec<Sample> = parsed
        .into_iter()
        .map(|p| Sample {
            metric_name: p.metric_name,
            timestamp_ms: p.timestamp_ms,
            value: p.value,
        })
        .collect();
    let s = storage.as_ref();
    s.ingest(&samples)?;
    tracing::debug!(count = samples.len(), "ingested");
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    metric: String,
    /// Lower bound (epoch ms, inclusive).
    start: Option<i64>,
    /// Upper bound (epoch ms, inclusive).
    end: Option<i64>,
    /// If true, force a flush before the query (mostly useful in tests).
    #[serde(default)]
    flush: bool,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    metric: String,
    samples: Vec<SampleOut>,
}

#[derive(Debug, Serialize)]
struct SampleOut {
    timestamp: i64,
    value: i64,
}

// The PromQL response shape is built ad-hoc via `serde_json::json!` below to
// match Prometheus's existing format byte-for-byte.

#[derive(Debug, Deserialize)]
struct PromqlParams {
    query: String,
    /// Eval timestamp, epoch *seconds* (matches Prometheus). Defaults to now.
    time: Option<f64>,
    #[serde(default)]
    flush: bool,
}

/// `GET /api/v1/promql?query=<expr>&time=<sec>`
///
/// Prometheus-compatible instant query. Returns the standard
/// `{"status":"success","data":{"resultType":"vector|scalar","result":...}}`
/// envelope.
async fn promql_instant(
    State(storage): State<Arc<ShardedStorage>>,
    Query(params): Query<PromqlParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    let expr = esm_promql::parser::parse(&params.query)
        .map_err(|e| AppError(anyhow::anyhow!("parse error: {e}")))?;
    let now_sec = params.time.unwrap_or_else(|| {
        #[allow(clippy::cast_precision_loss)]
        let ms = chrono_like_now_ms() as f64;
        ms / 1000.0
    });
    let ctx = esm_promql::EvalContext::instant(seconds_to_ms(now_sec));
    // Read-only queries take a shared read lock so they run concurrently; only
    // the optional explicit flush needs the exclusive write lock.
    if params.flush {
        storage.flush()?;
    }
    let s = storage.as_ref();
    let value = esm_promql::evaluator::evaluate(&expr, s, ctx)
        .map_err(|e| AppError(anyhow::anyhow!("eval error: {e}")))?;

    let envelope = build_promql_envelope(value, now_sec);
    Ok(Json(envelope))
}

#[derive(Debug, Deserialize)]
struct PromqlRangeParams {
    query: String,
    /// Start time, epoch seconds.
    start: f64,
    /// End time, epoch seconds (inclusive).
    end: f64,
    /// Step in seconds.
    step: f64,
    #[serde(default)]
    flush: bool,
}

/// One series in a matrix result. Serializes directly to the Prometheus shape
/// `{"metric":{…},"values":[[ts,"v"],…]}` — avoiding a `serde_json::Value`
/// tree, which for a 10k-series × 12-step result is ~250k heap allocations
/// built only to be re-serialized.
#[derive(Serialize)]
struct RangeSeries {
    metric: std::collections::BTreeMap<String, String>,
    values: Vec<(f64, String)>,
}

#[derive(Serialize)]
struct RangeData {
    #[serde(rename = "resultType")]
    result_type: &'static str,
    result: Vec<RangeSeries>,
}

#[derive(Serialize)]
struct RangeEnvelope {
    status: &'static str,
    data: RangeData,
}

/// `GET /api/v1/promql_range?query=<expr>&start=<sec>&end=<sec>&step=<sec>`
///
/// Prometheus-compatible range query. Returns
/// `{"status":"success","data":{"resultType":"matrix","result":[…]}}`.
async fn promql_range(
    State(storage): State<Arc<ShardedStorage>>,
    Query(params): Query<PromqlRangeParams>,
) -> Result<Json<RangeEnvelope>, AppError> {
    let expr = esm_promql::parser::parse(&params.query)
        .map_err(|e| AppError(anyhow::anyhow!("parse error: {e}")))?;
    let start_ms = seconds_to_ms(params.start);
    let end_ms = seconds_to_ms(params.end);
    let step_ms = seconds_to_ms(params.step);
    if step_ms <= 0 {
        return Err(AppError(anyhow::anyhow!("step must be > 0")));
    }
    // Read-only queries take a shared read lock so they run concurrently; only
    // the optional explicit flush needs the exclusive write lock.
    if params.flush {
        storage.flush()?;
    }
    let s = storage.as_ref();
    let elements = esm_promql::evaluator::evaluate_range(&expr, s, start_ms, end_ms, step_ms)
        .map_err(|e| AppError(anyhow::anyhow!("eval error: {e}")))?;

    let result: Vec<RangeSeries> = elements
        .into_iter()
        .map(|elt| {
            let metric = decode_metric_labels(&elt.metric_name);
            let values: Vec<(f64, String)> = elt
                .values
                .into_iter()
                .map(|(ts_ms, v)| {
                    #[allow(clippy::cast_precision_loss)]
                    let ts_sec = (ts_ms as f64) / 1000.0;
                    (ts_sec, fmt_value(v))
                })
                .collect();
            RangeSeries { metric, values }
        })
        .collect();
    Ok(Json(RangeEnvelope { status: "success", data: RangeData { result_type: "matrix", result } }))
}

fn seconds_to_ms(sec: f64) -> i64 {
    #[allow(clippy::cast_possible_truncation)]
    let ms = (sec * 1000.0) as i64;
    ms
}

fn build_promql_envelope(value: esm_promql::Value, eval_sec: f64) -> serde_json::Value {
    use esm_promql::Value as V;
    match value {
        V::Scalar(n) => serde_json::json!({
            "status": "success",
            "data": { "resultType": "scalar", "result": [fmt_timestamp_sec(eval_sec), fmt_value(n)] }
        }),
        V::InstantVector(elems) => {
            let result: Vec<serde_json::Value> = elems
                .into_iter()
                .map(|e| {
                    let metric = decode_metric_labels(&e.metric_name);
                    #[allow(clippy::cast_precision_loss)]
                    let ts_sec = (e.timestamp_ms as f64) / 1000.0;
                    serde_json::json!({
                        "metric": metric,
                        "value": [fmt_timestamp_sec(ts_sec), fmt_value(e.value)]
                    })
                })
                .collect();
            serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": result }
            })
        }
    }
}

/// Emit a Prometheus-style timestamp number: integer-valued whole seconds
/// serialise without a `.0` suffix to match upstream VM byte-for-byte.
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_precision_loss)]
fn fmt_timestamp_sec(sec: f64) -> serde_json::Value {
    if sec.is_finite() && sec.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&sec) {
        serde_json::Value::Number(serde_json::Number::from(sec as i64))
    } else {
        serde_json::Number::from_f64(sec).map_or(serde_json::Value::Null, serde_json::Value::Number)
    }
}

fn fmt_value(n: f64) -> String {
    if n.is_nan() {
        "NaN".to_string()
    } else if n.is_infinite() {
        if n > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() }
    } else {
        // Prometheus uses Go's `strconv.FormatFloat(v, 'f', -1, 64)` which
        // emits the shortest representation. Rust's `{}` produces the same
        // for typical inputs.
        format!("{n}")
    }
}

fn decode_metric_labels(metric_name: &[u8]) -> std::collections::BTreeMap<String, String> {
    let s = String::from_utf8_lossy(metric_name).into_owned();
    let mut out = std::collections::BTreeMap::new();
    let (name, labels) = match s.find('{') {
        Some(i) => (&s[..i], s[i..].trim_start_matches('{').trim_end_matches('}').to_string()),
        None => (s.as_str(), String::new()),
    };
    if !name.is_empty() {
        out.insert("__name__".to_string(), name.to_string());
    }
    if !labels.is_empty() {
        for part in labels.split(',') {
            let Some(eq) = part.find('=') else { continue };
            let k = part[..eq].to_string();
            let raw = &part[eq + 1..];
            let v = raw.trim_start_matches('"').trim_end_matches('"').to_string();
            out.insert(k, v);
        }
    }
    out
}

/// `GET /api/v1/query`
/// `/api/v1/query` dispatcher: behaves like Prometheus's instant-query
/// endpoint when `?query=<PromQL>` is supplied; falls back to our
/// legacy `?metric=<name>` byte-range lookup otherwise. This lets
/// Prometheus + Grafana clients hit the canonical Prom path while
/// keeping the small-footprint metric lookup available.
async fn query_dispatch(
    State(storage): State<Arc<ShardedStorage>>,
    raw_query: axum::extract::RawQuery,
) -> axum::response::Response {
    let raw = raw_query.0.unwrap_or_default();
    let has_query = raw.split('&').filter_map(|kv| kv.split_once('=')).any(|(k, _)| k == "query");
    if has_query {
        match serde_urlencoded::from_str::<PromqlParams>(&raw) {
            Ok(p) => match promql_instant(State(storage), Query(p)).await {
                Ok(j) => {
                    // VM normalises scalar PromQL results into a single-
                    // element instant vector before serialising for
                    // `/api/v1/query`. We do the same so Grafana, etc.,
                    // see the same shape against either backend. The
                    // strict-Prometheus shape is still available at
                    // `/api/v1/promql`.
                    let envelope = match j.0 {
                        serde_json::Value::Object(mut env) => {
                            if let Some(serde_json::Value::Object(mut data)) = env.remove("data")
                                && data.get("resultType").and_then(|v| v.as_str()) == Some("scalar")
                                && let Some(serde_json::Value::Array(pair)) = data.remove("result")
                            {
                                let val = serde_json::Value::Array(pair);
                                data.insert(
                                    "resultType".to_string(),
                                    serde_json::Value::String("vector".to_string()),
                                );
                                data.insert(
                                    "result".to_string(),
                                    serde_json::Value::Array(vec![serde_json::json!({
                                        "metric": {},
                                        "value": val,
                                    })]),
                                );
                                env.insert("data".to_string(), serde_json::Value::Object(data));
                            } else if let Some(data) = env.remove("data") {
                                env.insert("data".to_string(), data);
                            }
                            serde_json::Value::Object(env)
                        }
                        other => other,
                    };
                    axum::Json(envelope).into_response()
                }
                Err(e) => e.into_response(),
            },
            Err(e) => (StatusCode::BAD_REQUEST, format!("bad params: {e}")).into_response(),
        }
    } else {
        match serde_urlencoded::from_str::<QueryParams>(&raw) {
            Ok(p) => match query_metric_inner(&storage, p) {
                Ok(j) => j.into_response(),
                Err(e) => e.into_response(),
            },
            Err(e) => (StatusCode::BAD_REQUEST, format!("bad params: {e}")).into_response(),
        }
    }
}

fn query_metric_inner(
    storage: &ShardedStorage,
    params: QueryParams,
) -> Result<Json<QueryResponse>, AppError> {
    let range = TimeRange {
        min_timestamp_ms: params.start.unwrap_or(i64::MIN),
        max_timestamp_ms: params.end.unwrap_or(i64::MAX),
    };
    if params.flush {
        storage.flush()?;
    }
    let hits = storage.search_by_metric_name(params.metric.as_bytes(), range)?;
    let samples: Vec<SampleOut> = hits
        .into_iter()
        .map(|sample| SampleOut { timestamp: sample.timestamp_ms, value: sample.value })
        .collect();
    Ok(Json(QueryResponse { metric: params.metric, samples }))
}

/// One-line monotonic-ish "now" without pulling in the chrono crate. Uses
/// `SystemTime::UNIX_EPOCH` which is reasonable as a fallback timestamp.
fn chrono_like_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    // Truncating millis to i64 is safe for any reasonable wall-clock time
    // through year ~292277026596; saturate on the absurd case.
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Lightweight error wrapper that converts to an HTTP response.
#[derive(Debug)]
struct AppError(anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        tracing::warn!(error = %self.0, "request failed");
        (StatusCode::BAD_REQUEST, format!("error: {}", self.0)).into_response()
    }
}
