//! OpenTSDB HTTP `/api/put` insert handler.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/opentsdbhttp/request_handler.go` plus
//! `lib/ingestserver/opentsdbhttp/server.go`'s `newRequestHandler`.
//!
//! # A dedicated listener, not a route on the main HTTP port
//!
//! Unlike every other handler in this crate, `/api/put` is **not** wired
//! into [`crate::InsertHandlers`] (the main `-httpListenAddr` router).
//! Upstream serves OpenTSDB HTTP `put` requests on their own listener
//! (`-opentsdbHTTPListenAddr`, a distinct `net/http.Server` — see
//! `lib/ingestserver/opentsdbhttp/server.go`), confirmed by grepping
//! `app/vminsert/main.go`'s `RequestHandler`: it has no `/api/put` case at
//! all. [`OpentsdbHttpHandlers`] is meant to be the *sole* handler on that
//! second `esm_http::Server` (wired in `esmetrics::run`), which is why
//! [`OpentsdbHttpHandlers::handle`] — unlike [`crate::InsertHandlers::handle`]
//! — always writes a response instead of returning `bool` for the caller to
//! fall through: Go's dedicated server has no further routing to fall
//! through to either (`newRequestHandler`'s only branches are success, an
//! `InsertHandler` error, and failed basic auth).
//!
//! Both `/api/put` and `/opentsdb/api/put` are accepted, matching Go
//! `InsertHandler`'s `switch path`. Any other path gets the same HTTP 400
//! upstream produces for its `default` switch arm (`fmt.Errorf("unexpected
//! path requested on HTTP OpenTSDB server: %q", path)`, which
//! `httpserver.Errorf` maps to `http.StatusBadRequest` since it isn't an
//! `ErrorWithStatusCode`) — not 404, since this dedicated server has no
//! concept of "not my route, try elsewhere".
//!
//! No path normalization (`strings.ReplaceAll(path, "//", "/")`) is applied
//! here: that normalization lives in `app/vminsert/main.go`'s `RequestHandler`
//! for the shared/main port, not in the standalone
//! `lib/ingestserver/opentsdbhttp` package this handler mirrors.
//!
//! No HTTP Basic Auth (`httpserver.CheckBasicAuth`) — out of scope, matching
//! how no other handler in this crate implements the shared
//! `-httpAuth.*` flags either.
//!
//! # Label order
//!
//! Go's `insertRows` calls `ctx.AddLabel("", r.Metric)` *before* the tag
//! loop, then appends `extraLabels` last:
//! ```go
//! ctx.AddLabel("", r.Metric)
//! for j := range r.Tags { ctx.AddLabel(tag.Key, tag.Value) }
//! for j := range extraLabels { ctx.AddLabel(label.Name, label.Value) }
//! ```
//! So the metric group is marshaled *first* here (matching
//! `crate::opentsdb`'s telnet converter — same upstream label-construction
//! logic), then tags in input order, then extra labels last. This is the
//! opposite convention from `crate::vmimport`/`crate::prometheusimport`
//! (metric group *last*), which is why extra-label placement differs too:
//! those group-last converters append extra labels right after the input
//! tags and before the trailing group pair, whereas here extra labels come
//! after everything else, exactly mirroring Go's call order.
//!
//! `extra_label` query-arg support (`protoparserutil.GetExtraLabels`) is
//! real here, unlike `crate::opentsdb`'s telnet path (no query string to
//! read on a raw TCP/UDP connection) — this is genuine HTTP with a query
//! string, and upstream's `InsertHandler` calls `GetExtraLabels(req)`
//! explicitly.
//!
//! # Metrics
//!
//! `esm_rows_inserted_total{type="opentsdbhttp"}` is ported (see
//! [`ROWS_INSERTED`]); `vm_rows_per_insert{type="opentsdbhttp"}` (a
//! histogram) is not — this crate only ports counters (see
//! `esm_common::metrics`'s module doc).

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::{Request, ResponseWriter};
use esm_protoparser::opentsdbhttp::{self, Row};
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `vm_rows_inserted_total{type="opentsdbhttp"}`
/// (`app/vminsert/opentsdbhttp/request_handler.go:17`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="opentsdbhttp"}"#));

/// Routes `/api/put` and `/opentsdb/api/put` requests to the OpenTSDB HTTP
/// insert handler. Intended to be the sole handler on a dedicated
/// `-opentsdbHTTPListenAddr` `esm_http::Server` — see the module doc.
pub struct OpentsdbHttpHandlers<S: RowSink> {
    sink: S,
    limiter: ConcurrencyLimiter,
}

impl<S: RowSink> OpentsdbHttpHandlers<S> {
    /// Creates handlers with the default concurrency limits, matching
    /// [`crate::InsertHandlers::new`].
    pub fn new(sink: S) -> OpentsdbHttpHandlers<S> {
        OpentsdbHttpHandlers {
            sink,
            limiter: ConcurrencyLimiter::default(),
        }
    }

    /// Handles one request on the dedicated OpenTSDB HTTP listener. Always
    /// writes a response (200/204 success or an error status) — see the
    /// module doc for why this doesn't return `bool` like
    /// [`crate::InsertHandlers::handle`].
    pub fn handle(&self, req: &mut Request<'_>, w: &mut ResponseWriter<'_>) {
        match req.path() {
            "/api/put" | "/opentsdb/api/put" => {
                match insert_handler(&self.sink, &self.limiter, req) {
                    Ok(()) => w.write_status(204),
                    Err(err) => {
                        w.set_status(err.status_code);
                        w.set_content_type("text/plain; charset=utf-8");
                        w.write_body(err.message.as_bytes());
                        w.write_body(b"\n");
                    }
                }
            }
            other => {
                w.set_status(400);
                w.set_content_type("text/plain; charset=utf-8");
                w.write_body(
                    format!("unexpected path requested on HTTP OpenTSDB server: {other:?}\n")
                        .as_bytes(),
                );
            }
        }
    }
}

/// Processes an OpenTSDB HTTP `/api/put` request.
/// Go: `opentsdbhttp.InsertHandler`.
fn insert_handler<S: RowSink>(
    sink: &S,
    limiter: &ConcurrencyLimiter,
    req: &mut Request<'_>,
) -> Result<(), InsertError> {
    let extra_labels = common::get_extra_labels(req)?;

    // Backpressure: writeconcurrencylimiter. Held for the whole request.
    let _permit = limiter
        .acquire()
        .map_err(|err| InsertError::unavailable(err.to_string()))?;

    let encoding = req.content_encoding_str();
    let result = with_ctx(|ctx| {
        opentsdbhttp::parse_stream(
            req.body(),
            encoding,
            |_msg| { /* no logging framework wired up yet; see esm-insert's crate doc */ },
            |rows| convert_and_add(ctx, sink, rows, &extra_labels).map_err(Into::into),
        )
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        opentsdbhttp::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable/unparseable request data maps to 400
        // (httpserver.Errorf).
        _ => InsertError::bad_request(err.to_string()),
    })
}

/// Converts one parsed request's rows to `MetricRow`s and pushes them to the
/// sink. Port of Go `insertRows` (no relabeling) — see the module doc's
/// "Label order" section for why the metric group is marshaled first here.
fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    rows: &[Row],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for r in rows {
        let offset = ctx.arena.len();
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", r.metric.as_bytes())]);
        for tag in &r.tags {
            marshal_metric_name_raw(
                &mut ctx.arena,
                &[(tag.key.as_bytes(), tag.value.as_bytes())],
            );
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        ctx.entries.push(Entry {
            offset,
            len: ctx.arena.len() - offset,
            timestamp: r.timestamp,
            value: r.value,
        });
    }

    // Go: `rowsInserted.Add(len(rows))` before `ctx.FlushBufs()`
    // (`request_handler.go:64-66`) — incremented even if the flush below
    // fails. OpenTSDB HTTP rows have no nested values, so `len(rows)` there
    // is `ctx.entries.len()` here (one entry per row).
    ROWS_INSERTED.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
    use esm_protoparser::opentsdbhttp::Tag;
    use esm_storage::MetricName;
    use std::sync::Mutex;

    #[derive(Debug, PartialEq)]
    struct GotRow {
        metric_group: String,
        tags: Vec<(String, String)>,
        timestamp: i64,
        value: f64,
    }

    #[derive(Default)]
    struct CollectSink {
        rows: Mutex<Vec<GotRow>>,
        raw: Mutex<Vec<Vec<u8>>>,
    }

    impl RowSink for CollectSink {
        fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
            let mut got = self.rows.lock().unwrap();
            let mut raw = self.raw.lock().unwrap();
            for row in rows {
                let mut mn = MetricName::default();
                mn.unmarshal_raw(row.metric_name_raw)
                    .expect("valid metric_name_raw");
                got.push(GotRow {
                    metric_group: String::from_utf8(mn.metric_group.clone()).unwrap(),
                    tags: mn
                        .tags
                        .iter()
                        .map(|t| {
                            (
                                String::from_utf8(t.key.clone()).unwrap(),
                                String::from_utf8(t.value.clone()).unwrap(),
                            )
                        })
                        .collect(),
                    timestamp: row.timestamp,
                    value: row.value,
                });
                raw.push(row.metric_name_raw.to_vec());
            }
            Ok(())
        }
    }

    fn row(metric: &str, tags: &[(&str, &str)], value: f64, timestamp: i64) -> Row {
        Row {
            metric: metric.to_owned(),
            tags: tags
                .iter()
                .map(|&(k, v)| Tag {
                    key: k.to_owned(),
                    value: v.to_owned(),
                })
                .collect(),
            value,
            timestamp,
        }
    }

    fn convert(rows: &[Row], extra_labels: &[(String, String)]) -> (Vec<GotRow>, Vec<Vec<u8>>) {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        convert_and_add(&mut ctx, &sink, rows, extra_labels).unwrap();
        (
            sink.rows.into_inner().unwrap(),
            sink.raw.into_inner().unwrap(),
        )
    }

    fn got(metric_group: &str, tags: &[(&str, &str)], timestamp: i64, value: f64) -> GotRow {
        GotRow {
            metric_group: metric_group.to_owned(),
            tags: tags
                .iter()
                .map(|&(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            timestamp,
            value,
        }
    }

    #[test]
    fn maps_metric_and_tags_to_metric_row() {
        let rows = [row(
            "sys.cpu",
            &[("host", "h1"), ("cpu", "0")],
            42.0,
            1_727_879_909_000,
        )];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(
            converted,
            vec![got(
                "sys.cpu",
                &[("host", "h1"), ("cpu", "0")],
                1_727_879_909_000,
                42.0,
            )]
        );
    }

    #[test]
    fn zero_tags_is_accepted() {
        let rows = [row("sys.cpu", &[], 1.0, 100)];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(converted, vec![got("sys.cpu", &[], 100, 1.0)]);
    }

    #[test]
    fn extra_labels_are_appended_last() {
        let rows = [row("sys.cpu", &[("host", "h1")], 1.0, 1)];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let (converted, _) = convert(&rows, &extra);
        assert_eq!(
            converted,
            vec![got("sys.cpu", &[("host", "h1"), ("env", "prod")], 1, 1.0)]
        );
    }

    #[test]
    fn raw_encoding_has_metric_group_before_tags_and_extra_labels_last() {
        let rows = [row("sys.cpu", &[("host", "h1")], 3.0, 1)];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let (_, raw) = convert(&rows, &extra);
        let mut expected = Vec::new();
        marshal_metric_name_raw(
            &mut expected,
            &[(b"", b"sys.cpu"), (b"host", b"h1"), (b"env", b"prod")],
        );
        assert_eq!(raw, vec![expected]);
    }

    #[test]
    fn multiple_rows_are_all_converted() {
        let rows = [row("a", &[("t", "1")], 1.0, 10), row("b", &[], 2.0, 20)];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(
            converted,
            vec![got("a", &[("t", "1")], 10, 1.0), got("b", &[], 20, 2.0)]
        );
    }

    #[test]
    fn buffers_are_reused_across_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let rows = [row("m", &[("t", "v")], 1.0, 9)];
        convert_and_add(&mut ctx, &sink, &rows, &[]).unwrap();
        let arena_cap = ctx.arena.capacity();
        let rows_cap = ctx.rows_capacity();
        assert!(rows_cap >= 1);
        convert_and_add(&mut ctx, &sink, &rows, &[]).unwrap();
        assert_eq!(ctx.arena.capacity(), arena_cap, "arena must be reused");
        assert_eq!(ctx.rows_capacity(), rows_cap, "row vec must be recycled");
        let converted = sink.rows.into_inner().unwrap();
        assert_eq!(converted.len(), 2);
        assert_eq!(converted[0], converted[1]);
    }

    #[test]
    fn sink_error_is_propagated() {
        struct FailSink;
        impl RowSink for FailSink {
            fn add_rows(&self, _rows: &[MetricRow<'_>]) -> Result<(), String> {
                Err("storage full".to_owned())
            }
        }
        let mut ctx = ConvertCtx::default();
        let rows = [row("m", &[], 1.0, 1)];
        let err = convert_and_add(&mut ctx, &FailSink, &rows, &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }
}
