//! esm-insert: ingestion handlers for esmetrics.
//!
//! Rust port of the upstream VictoriaMetrics v1.146.0 `app/vminsert` (influx `/write`
//! path only) plus `lib/writeconcurrencylimiter`.
//!
//! # Storage seam
//!
//! `esm-storage`'s stage-4 `storage` module (Storage/AddRows) is developed
//! concurrently, so this crate is decoupled from it via [`RowSink`] and a
//! local [`MetricRow`] that mirrors Go `storage.MetricRow`:
//! `MetricNameRaw` bytes (as produced by
//! `esm_storage::marshal_metric_name_raw`), timestamp in milliseconds and an
//! f64 value. `metric_name_raw` borrows a per-batch arena, so a sink that
//! needs to keep the rows must copy the bytes (Go `Storage.add` copies them
//! into its own buffers anyway). Once `esm_storage::MetricRow` lands, either
//! implement [`RowSink`] for the storage handle by converting at the
//! boundary, or swap this struct for a re-export if the shapes match.

mod common;
mod convert_ctx;
mod limiter;

pub mod csvimport;
pub mod datadog;
pub mod graphite;
pub mod influx;
pub mod ingestserver;
pub mod opentelemetry;
pub mod opentsdb;
pub mod opentsdbhttp;
pub mod prometheusimport;
pub mod promremotewrite;
pub mod vmimport;

pub use limiter::ConcurrencyLimiter;

use std::sync::Arc;
use std::time::Duration;

use esm_http::{Request, ResponseWriter};

/// A single ingested data point, mirroring Go `storage.MetricRow`.
///
/// `metric_name_raw` is in the MetricNameRaw wire form produced by
/// [`esm_storage::marshal_metric_name_raw`] and can be decoded with
/// [`esm_storage::MetricName::unmarshal_raw`]. It borrows a batch-scoped
/// arena owned by the caller of [`RowSink::add_rows`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricRow<'a> {
    /// MetricNameRaw-encoded metric name (labels + metric group).
    pub metric_name_raw: &'a [u8],
    /// Timestamp in milliseconds.
    pub timestamp: i64,
    /// Data point value.
    pub value: f64,
}

/// Destination for ingested rows (the seam towards `esm-storage`).
pub trait RowSink: Send + Sync {
    /// Adds the given rows to the underlying storage.
    ///
    /// The rows (and the arena their `metric_name_raw` borrows) are only
    /// valid for the duration of the call.
    fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String>;
}

impl<S: RowSink + ?Sized> RowSink for Arc<S> {
    fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
        (**self).add_rows(rows)
    }
}

/// An ingestion error carrying the HTTP status code to report, mirroring Go
/// `httpserver.ErrorWithStatusCode` (plain errors map to 400 like
/// `httpserver.Errorf`).
#[derive(Debug)]
pub struct InsertError {
    pub status_code: u16,
    pub message: String,
}

impl InsertError {
    pub(crate) fn bad_request(message: String) -> InsertError {
        InsertError {
            status_code: 400,
            message,
        }
    }

    pub(crate) fn unavailable(message: String) -> InsertError {
        InsertError {
            status_code: 503,
            message,
        }
    }
}

/// HTTP ingestion request router. Port of the vminsert `RequestHandler`
/// (influx paths only).
pub struct InsertHandlers<S: RowSink> {
    sink: S,
    limiter: ConcurrencyLimiter,
}

impl<S: RowSink> InsertHandlers<S> {
    /// Creates handlers with the default concurrency limits:
    /// `-maxConcurrentInserts` = 2 x available CPUs and
    /// `-insert.maxQueueDuration` = 1 minute.
    pub fn new(sink: S) -> InsertHandlers<S> {
        InsertHandlers {
            sink,
            limiter: ConcurrencyLimiter::default(),
        }
    }

    /// Creates handlers with explicit concurrency limits
    /// (`-maxConcurrentInserts` / `-insert.maxQueueDuration`).
    pub fn with_limits(
        sink: S,
        max_concurrent_inserts: usize,
        max_queue_duration: Duration,
    ) -> InsertHandlers<S> {
        InsertHandlers {
            sink,
            limiter: ConcurrencyLimiter::new(max_concurrent_inserts, max_queue_duration),
        }
    }

    /// Handles an ingestion request. Returns `false` if `req.path()` is not
    /// an ingestion path, so the caller can route it elsewhere.
    ///
    /// Go: `app/vminsert/main.go` `RequestHandler`.
    pub fn handle(&self, req: &mut Request<'_>, w: &mut ResponseWriter<'_>) -> bool {
        // Go: `path := strings.ReplaceAll(r.URL.Path, "//", "/")` (main.go)
        // before all route matching. Note this normalized path is used for
        // routing and status selection only; label extraction inside the
        // handlers reads `req.path()` directly, exactly like Go's
        // `GetExtraLabels(req)` reads the un-normalized `req.URL.Path`.
        let path = normalize_path(req.path());
        if let Some(path_suffix) = prometheus_import_path_suffix(&path) {
            // Extracted before the handler call: `path` borrows `req`
            // (paths decode lazily), and the handler needs `req` mutably.
            let is_pushgateway_path = path_suffix.starts_with("/metrics/job/");
            match prometheusimport::insert_handler(&self.sink, &self.limiter, req) {
                Ok(()) => {
                    // Go: main.go returns 200 (not 204) for Pushgateway-style
                    // requests specifically, to satisfy Pushgateway clients.
                    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3636
                    let status = if is_pushgateway_path { 200 } else { 204 };
                    w.write_status(status);
                }
                Err(err) => {
                    w.set_status(err.status_code);
                    w.set_content_type("text/plain; charset=utf-8");
                    w.write_body(err.message.as_bytes());
                    w.write_body(b"\n");
                }
            }
            return true;
        }
        // Go: `if strings.HasPrefix(path, "/datadog/") { path =
        // strings.TrimSuffix(path, "/") }` — legacy DataDog agents append a
        // trailing slash; this trims at most one, applied only to
        // `/datadog/`-prefixed paths, right before the switch.
        let path = trim_datadog_trailing_slash(path);
        match path.as_ref() {
            "/influx/write" | "/influx/api/v2/write" | "/write" | "/api/v2/write" => {
                // Some clients expect an InfluxDB version header.
                // Go: addInfluxResponseHeaders.
                w.set_header("X-Influxdb-Version", "1.8.0");
                match influx::insert_handler_for_http(&self.sink, &self.limiter, req) {
                    Ok(()) => w.write_status(204),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
                true
            }
            "/api/v1/write"
            | "/prometheus/api/v1/write"
            | "/api/v1/push"
            | "/prometheus/api/v1/push" => {
                match promremotewrite::insert_handler(&self.sink, &self.limiter, req) {
                    Ok(()) => w.write_status(204),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
                true
            }
            "/api/v1/import" | "/prometheus/api/v1/import" => {
                match vmimport::insert_handler(&self.sink, &self.limiter, req) {
                    Ok(()) => w.write_status(204),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
                true
            }
            "/api/v1/import/csv" | "/prometheus/api/v1/import/csv" => {
                match csvimport::insert_handler(&self.sink, &self.limiter, req) {
                    Ok(()) => w.write_status(204),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
                true
            }
            "/opentelemetry/api/v1/push" | "/opentelemetry/v1/metrics" => {
                match opentelemetry::insert_handler(&self.sink, &self.limiter, req) {
                    // Go: `firehose.WriteSuccessResponse` — 200 with an empty
                    // body for plain (non-AWS-Firehose) OTLP requests, not
                    // 204. See `opentelemetry.rs`'s module doc.
                    Ok(()) => w.write_status(200),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
                true
            }
            "/datadog/api/v1/series" => {
                match datadog::insert_handler_v1(&self.sink, &self.limiter, req) {
                    // Go: `app/vminsert/main.go`'s `/datadog/api/v1/series`
                    // case — 202 with a JSON `{"status":"ok"}` body, not a
                    // bare 204 (copied from main.go, not guessed; see
                    // `crate::datadog`'s module doc).
                    Ok(()) => w.write_json(202, "{\"status\":\"ok\"}"),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
                true
            }
            "/datadog/api/v2/series" => {
                match datadog::insert_handler_v2(&self.sink, &self.limiter, req) {
                    // Go: `app/vminsert/main.go`'s `/datadog/api/v2/series`
                    // case — same 202 + `{"status":"ok"}` shape as v1.
                    Ok(()) => w.write_json(202, "{\"status\":\"ok\"}"),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
                true
            }
            // Fixed-response DataDog agent stub endpoints. Go:
            // `app/vminsert/main.go`'s `/datadog/api/v1/validate`,
            // `/datadog/api/v1/check_run`, `/datadog/intake`,
            // `/datadog/api/v1/metadata` cases — statuses/bodies copied
            // exactly (none of these call `w.WriteHeader` unless noted, so
            // Go's default status of 200 applies where no explicit status
            // is shown below; do not trust guesses here, only the read
            // source).
            "/datadog/api/v1/validate" => {
                w.write_json(200, "{\"valid\":true}");
                true
            }
            "/datadog/api/v1/check_run" => {
                w.write_json(202, "{\"status\":\"ok\"}");
                true
            }
            "/datadog/intake" => {
                w.write_json(200, "{}");
                true
            }
            "/datadog/api/v1/metadata" => {
                w.write_json(200, "{}");
                true
            }
            _ => false,
        }
    }
}

/// Collapses doubled slashes in the request path before route matching.
/// Go: `path := strings.ReplaceAll(r.URL.Path, "//", "/")` at the top of
/// `app/vminsert/main.go` `RequestHandler`.
///
/// Exactly one non-overlapping left-to-right replacement pass (which is what
/// both Go `strings.ReplaceAll` and Rust `str::replace` do), deliberately
/// NOT applied repeatedly: `"///"` normalizes to `"//"`, not `"/"`, staying
/// byte-faithful to upstream. Zero-alloc on the common no-`"//"` path.
fn normalize_path(path: &str) -> std::borrow::Cow<'_, str> {
    if path.contains("//") {
        std::borrow::Cow::Owned(path.replace("//", "/"))
    } else {
        std::borrow::Cow::Borrowed(path)
    }
}

/// Trims a single trailing `/` from `path` if it starts with `/datadog/`.
/// Go: `app/vminsert/main.go`'s `if strings.HasPrefix(path, "/datadog/") {
/// path = strings.TrimSuffix(path, "/") }`, applied after [`normalize_path`]
/// and before route matching, "in order to support legacy DataDog agent"
/// (upstream comment, referencing
/// <https://github.com/VictoriaMetrics/VictoriaMetrics/pull/2670>).
/// `strings.TrimSuffix` removes at most one occurrence of the suffix, so
/// only one trailing slash is ever stripped (`"/datadog/api/v1/series//"`
/// normalizes to `"/datadog/api/v1/series/"` via `normalize_path`'s `//` ->
/// `/` collapse first, then loses just its final `/` here).
fn trim_datadog_trailing_slash(path: std::borrow::Cow<'_, str>) -> std::borrow::Cow<'_, str> {
    if path.starts_with("/datadog/") && path.ends_with('/') {
        let mut owned = path.into_owned();
        owned.pop();
        return std::borrow::Cow::Owned(owned);
    }
    path
}

/// Matches `/api/v1/import/prometheus` and its `/prometheus`-prefixed twin,
/// returning the path remainder after the matched prefix (e.g. `""` or
/// `/metrics/job/backup/instance/host1`). Go: the `strings.HasPrefix`
/// checks in `app/vminsert/main.go` `RequestHandler`. `path` must already
/// be normalized via [`normalize_path`].
fn prometheus_import_path_suffix(path: &str) -> Option<&str> {
    for prefix in [
        "/prometheus/api/v1/import/prometheus",
        "/api/v1/import/prometheus",
    ] {
        if let Some(suffix) = path.strip_prefix(prefix) {
            return Some(suffix);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_path_collapses_doubled_slashes_in_one_pass() {
        // No "//": borrowed passthrough.
        assert!(matches!(
            normalize_path("/api/v1/write"),
            std::borrow::Cow::Borrowed("/api/v1/write")
        ));
        // Doubled slashes collapse anywhere in the path.
        assert_eq!(normalize_path("//write"), "/write");
        assert_eq!(
            normalize_path("//api/v1/import/prometheus//metrics/job/x"),
            "/api/v1/import/prometheus/metrics/job/x"
        );
        // Single-pass semantics, byte-faithful to Go's one
        // strings.ReplaceAll("//", "/") call: "///" -> "//", not "/".
        assert_eq!(normalize_path("///write"), "//write");
        assert_eq!(normalize_path("////write"), "//write");
    }

    #[test]
    fn triple_slash_write_does_not_match_routes_like_upstream() {
        // "///write" normalizes to "//write" (single pass), which matches no
        // route arm — same 404-shaped outcome as upstream vminsert.
        let path = normalize_path("///write");
        assert_eq!(path, "//write");
        assert!(prometheus_import_path_suffix(&path).is_none());
        assert!(!matches!(
            path.as_ref(),
            "/influx/write" | "/influx/api/v2/write" | "/write" | "/api/v2/write"
        ));
    }

    #[test]
    fn prometheus_import_suffix_matches_normalized_doubled_slash_paths() {
        let path = normalize_path("//api/v1/import/prometheus/metrics/job/x");
        assert_eq!(prometheus_import_path_suffix(&path), Some("/metrics/job/x"));
        let path = normalize_path("//prometheus//api/v1/import/prometheus");
        assert_eq!(prometheus_import_path_suffix(&path), Some(""));
    }
}
