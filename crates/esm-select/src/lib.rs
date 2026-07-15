//! Query-side HTTP API for esmetrics. Rust port of the upstream VictoriaMetrics
//! v1.146.0 vmselect HTTP layer (`app/vmselect/main.go` routing +
//! `app/vmselect/prometheus` handlers), Stage 1.
//!
//! Endpoints:
//! - `/api/v1/query_range`, `/api/v1/query` (including the bare
//!   `selector[d]` → promapi export and `expr[w:step]` → range-query
//!   rewrites),
//! - `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/<name>/values`,
//! - `/api/v1/export` (JSON lines / `prometheus` / `promapi` formats),
//! - static Grafana-compat stubs: `/api/v1/status/buildinfo`,
//!   `/api/v1/rules`, `/api/v1/alerts`, `/api/v1/notifiers`,
//!   `/api/v1/query_exemplars` (same literal bodies as the upstream).
//!
//! Everything else (including the remaining `/api/v1/status/*` endpoints)
//! is left to the caller — [`SelectHandlers::handle`] returns `false`.
//!
//! Error mapping follows `httpserver.SendPrometheusError`: handler errors
//! become HTTP 422 with `{"status":"error","errorType":"422","error":...}`
//! (the upstream uses the status code as the `errorType`, and answers
//! 422 even for missing/invalid params). `/api/v1/export` errors use the
//! plain-text `httpserver.Errorf` path (HTTP 400), and a concurrency-limit
//! queue timeout answers plain-text HTTP 429 with `Retry-After: 10`,
//! exactly like `vmselect/main.go`.

mod handlers;
mod json;
mod limiter;
mod params;
mod searchutil;

use esm_http::{Request, ResponseWriter};
use esm_promql::MetricsProvider;
use limiter::ConcurrencyLimiter;
use params::Params;
use std::time::Duration;

/// Tunables mirroring the vmselect command-line flags (defaults match the upstream
/// VictoriaMetrics v1.146.0).
#[derive(Debug, Clone)]
pub struct SelectConfig {
    /// `-search.maxConcurrentRequests`; 0 → `min(2 * cpus, 16)`.
    pub max_concurrent_requests: usize,
    /// `-search.maxQueueDuration` (ms): how long a request may wait for a
    /// free concurrency slot.
    pub max_queue_duration_ms: i64,
    /// `-search.maxQueryDuration` (ms); the per-request `timeout` arg can
    /// only lower it.
    pub max_query_duration_ms: i64,
    /// `-search.maxLabelsAPIDuration` (ms) for /series, /labels and
    /// /label/.../values.
    pub max_labels_api_duration_ms: i64,
    /// `-search.maxExportDuration` (ms).
    pub max_export_duration_ms: i64,
    /// `-search.latencyOffset` (ms).
    pub latency_offset_ms: i64,
    /// `-search.maxStepForPointsAdjustment` (ms).
    pub max_step_for_points_adjustment_ms: i64,
    /// `-search.maxPointsPerTimeseries`.
    pub max_points_per_timeseries: usize,
    /// `-search.maxSeries` (for /api/v1/series).
    pub max_series: usize,
    /// `-search.maxExportSeries`.
    pub max_export_series: usize,
    /// `-search.maxLabelsAPISeries`.
    pub max_labels_api_series: usize,
    /// `-search.maxQueryLen` (bytes).
    pub max_query_len: usize,
}

impl Default for SelectConfig {
    fn default() -> Self {
        SelectConfig {
            max_concurrent_requests: 0,
            max_queue_duration_ms: 10_000,
            max_query_duration_ms: 30_000,
            max_labels_api_duration_ms: 5_000,
            max_export_duration_ms: 30 * 24 * 3_600_000,
            latency_offset_ms: 30_000,
            max_step_for_points_adjustment_ms: 60_000,
            max_points_per_timeseries: 30_000,
            max_series: 30_000,
            max_export_series: 10_000_000,
            max_labels_api_series: 1_000_000,
            max_query_len: 16 * 1024,
        }
    }
}

/// `getDefaultMaxConcurrentRequests` from app/victoria-metrics/main.go:
/// `min(2 × cpus, 16)`. Pub so the binary's usage text can display the
/// computed default the way Go's flag package does.
pub fn default_max_concurrent_requests() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    (cpus * 2).min(16)
}

/// HTTP request router for the select (query) API. Port of the vmselect
/// `RequestHandler`. Generic over the storage-backed
/// [`MetricsProvider`]; tests use in-memory fakes.
pub struct SelectHandlers<P: MetricsProvider> {
    pub(crate) provider: P,
    pub(crate) config: SelectConfig,
    limiter: ConcurrencyLimiter,
}

impl<P: MetricsProvider> SelectHandlers<P> {
    pub fn new(provider: P) -> SelectHandlers<P> {
        SelectHandlers::with_config(provider, SelectConfig::default())
    }

    pub fn with_config(provider: P, config: SelectConfig) -> SelectHandlers<P> {
        let capacity = if config.max_concurrent_requests > 0 {
            config.max_concurrent_requests
        } else {
            default_max_concurrent_requests()
        };
        SelectHandlers {
            provider,
            config,
            limiter: ConcurrencyLimiter::new(capacity),
        }
    }

    /// Routes a select request. Returns `false` when the path is not
    /// handled by this crate (the caller answers 404 or dispatches
    /// elsewhere).
    pub fn handle(&self, req: &mut Request<'_>, w: &mut ResponseWriter<'_>) -> bool {
        let decoded = esm_http::percent_decode(req.path()).into_owned();
        // Path normalization from vmselect RequestHandler: collapse `//`,
        // strip the /prometheus and /graphite path prefixes.
        let mut path = decoded.replace("//", "/");
        if let Some(rest) = path.strip_prefix("/prometheus/") {
            path = format!("/{rest}");
        } else if let Some(rest) = path.strip_prefix("/graphite/") {
            path = format!("/{rest}");
        }

        // Static responses (handleStaticAndSimpleRequests): served without
        // touching the concurrency limiter, byte-identical to the upstream.
        let static_body: Option<&str> = match path.as_str() {
            "/api/v1/status/buildinfo" => {
                Some(r#"{"status":"success","data":{"version":"2.24.0"}}"#)
            }
            "/api/v1/rules" | "/rules" => Some(r#"{"status":"success","data":{"groups":[]}}"#),
            "/api/v1/alerts" | "/alerts" => Some(r#"{"status":"success","data":{"alerts":[]}}"#),
            "/api/v1/notifiers" | "/notifiers" => {
                Some(r#"{"status":"success","data":{"notifiers":[]}}"#)
            }
            "/api/v1/query_exemplars" => Some(r#"{"status":"success","data":[]}"#),
            _ => None,
        };
        if let Some(body) = static_body {
            w.write_json(200, body);
            return true;
        }

        let label_values_name = path
            .strip_prefix("/api/v1/label/")
            .and_then(|s| s.strip_suffix("/values"))
            .map(str::to_string);
        let is_select_path = label_values_name.is_some()
            || matches!(
                path.as_str(),
                "/api/v1/query"
                    | "/api/v1/query_range"
                    | "/api/v1/series"
                    | "/api/v1/labels"
                    | "/api/v1/export"
            );
        if !is_select_path {
            return false;
        }

        let request_params = Params::from_request(req);

        // Concurrency limiter: wait up to min(query timeout, queue duration)
        // for a slot, then give up with 429 + Retry-After (vmselect
        // RequestHandler behavior).
        let wait_ms =
            searchutil::get_timeout_ms(&request_params, self.config.max_query_duration_ms)
                .min(self.config.max_queue_duration_ms)
                .max(0);
        let Some(_slot) = self.limiter.acquire(Duration::from_millis(wait_ms as u64)) else {
            let capacity = if self.config.max_concurrent_requests > 0 {
                self.config.max_concurrent_requests
            } else {
                default_max_concurrent_requests()
            };
            // Upstream also suggests -search.maxQueueDuration and
            // -search.maxQueryDuration here; the port doesn't define those
            // flags yet, so only reference the ones an operator can set.
            let msg = format!(
                "couldn't start executing the request in {:.3} seconds, since \
                 -search.maxConcurrentRequests={capacity} concurrent requests are executed. \
                 Possible solutions: to reduce query load; to add more compute resources to the \
                 server; to increase -search.maxConcurrentRequests",
                wait_ms as f64 / 1e3,
            );
            w.set_header("Retry-After", "10");
            send_plain_error(w, 429, &msg);
            return true;
        };

        let result = match path.as_str() {
            "/api/v1/query" => self.handle_query(&request_params, w),
            "/api/v1/query_range" => self.handle_query_range(&request_params, w),
            "/api/v1/series" => self.handle_series(&request_params, w),
            "/api/v1/labels" => self.handle_labels(&request_params, w),
            "/api/v1/export" => {
                if let Err(err) = self.handle_export(&request_params, w) {
                    // Export uses httpserver.Errorf: plain-text 400.
                    log::warn!("esm-select: {path}: {err}");
                    send_plain_error(w, 400, &err);
                }
                return true;
            }
            _ => {
                let name = label_values_name.expect("checked above");
                self.handle_label_values(&name, &request_params, w)
            }
        };
        if let Err(err) = result {
            log::warn!("esm-select: {path}: {err}");
            send_prometheus_error(w, 422, &err);
        }
        true
    }
}

/// Port of `httpserver.SendPrometheusError`: JSON error envelope; the
/// status code doubles as the `errorType`.
fn send_prometheus_error(w: &mut ResponseWriter<'_>, status: u16, msg: &str) {
    if w.is_streaming() {
        return; // headers already sent; nothing sane to report
    }
    let mut body = Vec::with_capacity(msg.len() + 64);
    json::write_prometheus_error_response(&mut body, status, msg);
    w.set_status(status);
    w.set_content_type("application/json");
    w.write_body(&body);
}

/// Port of Go `http.Error` as used by `httpserver.Errorf`.
fn send_plain_error(w: &mut ResponseWriter<'_>, status: u16, msg: &str) {
    if w.is_streaming() {
        return;
    }
    w.set_status(status);
    w.set_content_type("text/plain; charset=utf-8");
    w.set_header("X-Content-Type-Options", "nosniff");
    w.write_body(msg.as_bytes());
    w.write_body(b"\n");
}
