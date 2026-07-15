//! OTLP metrics `/opentelemetry/v1/metrics` (+ `/opentelemetry/api/v1/push`)
//! handler.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/opentelemetry/request_handler.go`.
//!
//! ## Success response — corrected from the task brief
//!
//! The brief speculated upstream might reply with a marshaled
//! `ExportMetricsServiceResponse` protobuf body. Checking
//! `app/vminsert/main.go`'s route (`case "/opentelemetry/api/v1/push",
//! "/opentelemetry/v1/metrics":`) shows it calls
//! `firehose.WriteSuccessResponse(w, r)` on success, whose actual body
//! (`lib/protoparser/opentelemetry/firehose/http.go`) is:
//!
//! ```go
//! func WriteSuccessResponse(w http.ResponseWriter, r *http.Request) {
//!     requestID := r.Header.Get("X-Amz-Firehose-Request-Id")
//!     if requestID == "" {
//!         // This isn't a AWS firehose request - just return an empty response.
//!         w.WriteHeader(http.StatusOK)
//!         return
//!     }
//!     // ... build and write an AWS-Firehose-specific JSON ack body ...
//! }
//! ```
//!
//! Plain OTLP requests (no `X-Amz-Firehose-Request-Id` header) — the only
//! kind this crate accepts, see below — always take the first branch: HTTP
//! **200 with an empty body**, no protobuf response at all.
//!
//! ## JSON rejection
//!
//! Upstream's `InsertHandler` only accepts `application/json` bodies when
//! they carry an `X-Amz-Firehose-Protocol-Version` header (AWS Firehose's
//! JSON-wrapped-protobuf-records envelope, decoded by a separate
//! `firehose.ProcessRequestBody` before the real protobuf decode). Task 11
//! already established that plain OTLP/JSON is rejected outright at this
//! upstream version. Firehose ingestion itself is out of scope for this
//! port (no controller-supplied collector binary emits it, and it is a
//! distinct request format, not the metrics protobuf this crate decodes) —
//! so this handler rejects **any** `application/json` request body with
//! upstream's exact message, regardless of the Firehose header.
//!
//! ## Deviations from the Go original
//!
//! - No relabeling (`relabel.HasRelabeling`) and no metric-metadata handling
//!   (`prommetadata`) — out of scope for this port, matching how
//!   `influx.rs`/`promremotewrite.rs`/`prometheusimport.rs` already omit
//!   them (see `esm_protoparser::opentelemetry::convert`'s module doc for
//!   why `MetricMetadata` specifically has zero effect at this port's fixed
//!   naming defaults anyway).
//! - `esm_rows_inserted_total{type="opentelemetry"}` is ported (see
//!   [`ROWS_INSERTED`]); `vm_rows_per_insert{type="opentelemetry"}` (a
//!   histogram), `vm_metadata_rows_inserted_total` and
//!   `vm_http_requests_total{...}` are not — this crate only ports counters
//!   for row/request accounting, not per-endpoint HTTP request counters (see
//!   `esm_common::metrics`'s module doc).
//! - AWS Firehose ingestion (`X-Amz-Firehose-Protocol-Version` /
//!   `firehose.ProcessRequestBody`) is not implemented — see above.
//!
//! # Buffer strategy
//!
//! Same thread-local [`ConvertCtx`] arena pattern as
//! [`crate::prometheusimport`] (the 6th duplication of this pattern in the
//! crate — see the module doc on `crate::influx` for the rationale; still
//! worth extracting into a shared helper at some point). Per parsed
//! request, every row's `MetricNameRaw` bytes are marshaled once into a
//! reused arena — tags in input order, then extra labels, then the trailing
//! `("", metric)` pair.

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::Request;
use esm_protoparser::opentelemetry::{self, Row};
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `vm_rows_inserted_total{type="opentelemetry"}`
/// (`app/vminsert/opentelemetry/request_handler.go:18`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="opentelemetry"}"#));

/// Processes an OpenTelemetry metrics protobuf request.
/// Go: `opentelemetry.InsertHandler`.
pub(crate) fn insert_handler<S: RowSink>(
    sink: &S,
    limiter: &ConcurrencyLimiter,
    req: &mut Request<'_>,
) -> Result<(), InsertError> {
    let extra_labels = common::get_extra_labels(req)?;

    // Go: `if req.Header.Get("Content-Type") == "application/json" { ... }`
    // — see the module doc for why this port always rejects it (Firehose's
    // JSON-wrapped envelope is the only accepted exception upstream, and
    // Firehose ingestion is out of scope here).
    if req.content_type() == Some("application/json") {
        return Err(InsertError::bad_request(
            "json encoding isn't supported for opentelemetry format. Use protobuf encoding"
                .to_string(),
        ));
    }

    // Backpressure: writeconcurrencylimiter. Held for the whole request.
    let _permit = limiter
        .acquire()
        .map_err(|err| InsertError::unavailable(err.to_string()))?;

    let encoding = req.content_encoding_str();
    let result = with_ctx(|ctx| {
        opentelemetry::parse_stream(req.body(), encoding, |rows| {
            convert_and_add(ctx, sink, rows, &extra_labels).map_err(Into::into)
        })
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        opentelemetry::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable request data maps to 400 (httpserver.Errorf).
        _ => InsertError::bad_request(err.to_string()),
    })
}

/// Converts one parsed block of OTLP-derived rows to `MetricRow`s and pushes
/// them to the sink. Port of Go `insertRows` (no relabeling, no metadata).
fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    rows: &[Row],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for r in rows {
        let offset = ctx.arena.len();
        for (name, value) in &r.tags {
            marshal_metric_name_raw(&mut ctx.arena, &[(name.as_bytes(), value.as_bytes())]);
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        // The metric name goes last, encoded as an empty-key pair — same
        // layout convention as the other ingestion handlers in this crate.
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", r.metric.as_bytes())]);
        ctx.entries.push(Entry {
            offset,
            len: ctx.arena.len() - offset,
            timestamp: r.timestamp,
            value: r.value,
        });
    }

    // Go: `rowsInserted.Add(rowsTotal)` before `ctx.FlushBufs()`
    // (`request_handler.go:83-85`) — incremented even if the flush below
    // fails. `rowsTotal` there sums `len(ts.Samples)` per series, i.e.
    // `ctx.entries.len()` here (one entry per sample, not per series).
    ROWS_INSERTED.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
    use esm_storage::MetricName;
    use std::sync::Mutex;

    /// Decoded, owned form of a converted row for assertions.
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
    }

    impl RowSink for CollectSink {
        fn add_rows(&self, rows: &[MetricRow<'_>]) -> Result<(), String> {
            let mut got = self.rows.lock().unwrap();
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
            }
            Ok(())
        }
    }

    fn row(metric: &str, tags: &[(&str, &str)], timestamp: i64, value: f64) -> Row {
        Row {
            metric: metric.to_string(),
            tags: tags
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            timestamp,
            value,
        }
    }

    fn convert(rows: &[Row], extra_labels: &[(String, String)]) -> Vec<GotRow> {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        convert_and_add(&mut ctx, &sink, rows, extra_labels).unwrap();
        sink.rows.into_inner().unwrap()
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
    fn metric_name_becomes_group_tags_preserved() {
        let rows = [row(
            "cpu_usage",
            &[("host", "h1"), ("region", "us")],
            1000,
            42.5,
        )];
        let converted = convert(&rows, &[]);
        assert_eq!(
            converted,
            vec![got(
                "cpu_usage",
                &[("host", "h1"), ("region", "us")],
                1000,
                42.5
            )]
        );
    }

    #[test]
    fn tags_keep_input_order_without_sorting() {
        let rows = [row("m", &[("z", "1"), ("a", "2")], 1, 1.0)];
        let converted = convert(&rows, &[]);
        assert_eq!(
            converted[0].tags,
            vec![
                ("z".to_owned(), "1".to_owned()),
                ("a".to_owned(), "2".to_owned())
            ]
        );
    }

    #[test]
    fn extra_labels_are_appended_after_tags() {
        let rows = [row("m", &[("job", "x")], 1, 1.0)];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let converted = convert(&rows, &extra);
        assert_eq!(
            converted,
            vec![got("m", &[("job", "x"), ("env", "prod")], 1, 1.0)]
        );
    }

    #[test]
    fn no_tags_row_still_ingests() {
        let rows = [row("m", &[], 7, 3.0)];
        let converted = convert(&rows, &[]);
        assert_eq!(converted, vec![got("m", &[], 7, 3.0)]);
    }

    #[test]
    fn raw_encoding_matches_marshal_metric_name_raw_with_group_last() {
        let rows = [row("cpu", &[("host", "h1")], 1, 3.0)];
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        convert_and_add(&mut ctx, &sink, &rows, &[]).unwrap();
        let mut expected = Vec::new();
        marshal_metric_name_raw(
            &mut expected,
            &[(b"host", b"h1"), (b"", b"cpu")], // metric group is encoded last
        );
        assert_eq!(ctx.arena, expected);
    }

    #[test]
    fn buffers_are_reused_across_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let rows = [row("m", &[("t", "v")], 9, 1.0), row("m2", &[], 10, 2.0)];
        convert_and_add(&mut ctx, &sink, &rows, &[]).unwrap();
        let arena_cap = ctx.arena.capacity();
        let rows_cap = ctx.rows_capacity();
        assert!(rows_cap >= 2);
        convert_and_add(&mut ctx, &sink, &rows, &[]).unwrap();
        assert_eq!(ctx.arena.capacity(), arena_cap, "arena must be reused");
        assert_eq!(ctx.rows_capacity(), rows_cap, "row vec must be recycled");
        let converted = sink.rows.into_inner().unwrap();
        assert_eq!(converted.len(), 4);
        assert_eq!(converted[0], converted[2]);
        assert_eq!(converted[1], converted[3]);
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
        let rows = [row("m", &[], 1, 1.0)];
        let err = convert_and_add(&mut ctx, &FailSink, &rows, &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }
}
