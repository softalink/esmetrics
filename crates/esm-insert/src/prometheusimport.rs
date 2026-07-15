//! Prometheus exposition-text `/api/v1/import/prometheus` handler.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/prometheusimport/request_handler.go`.
//!
//! Deviations from the Go original:
//! - No relabeling (`relabel.HasRelabeling`) and no metric-metadata handling
//!   (`prommetadata`) — out of scope for this port, matching how
//!   `influx.rs`/`promremotewrite.rs` already omit them.
//! - `esm_rows_inserted_total{type="prometheus"}` is ported (see
//!   [`ROWS_INSERTED`]); `vm_rows_per_insert{type="prometheus"}` (a
//!   histogram) and `vm_metadata_rows_inserted_total` are not — this crate only
//!   ports counters (see `esm_common::metrics`'s module doc).
//! - Line-parse errors (the `errLogger` callback) are discarded rather than
//!   logged through an `httpserver.LogError`-equivalent — no logging
//!   framework is wired up yet in this crate (same gap as `influx.rs`'s
//!   `stream::parse_stream` caller).
//!
//! # Buffer strategy
//!
//! Same thread-local [`ConvertCtx`] arena pattern as [`crate::influx`] and
//! [`crate::promremotewrite`] (shared via `crate::convert_ctx`). Per parsed
//! block, every row's `MetricNameRaw` bytes are marshaled once into a reused
//! arena — tags in input order, then extra labels (Pushgateway path labels
//! first, then `extra_label` query args), then the trailing
//! `("", metric_group)` pair — matching the convention already established
//! by `influx.rs`/`promremotewrite.rs` (not Go's literal `ctx.AddLabel("",
//! r.Metric)`-first order, which doesn't affect Go's output either: Go's
//! `InsertCtx` restructures the label list into a `MetricName` struct before
//! the real canonical marshal, so input order there is irrelevant).

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::Request;
use esm_protoparser::prometheus::{self, Row};
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `vm_rows_inserted_total{type="prometheus"}`
/// (`app/vminsert/prometheusimport/request_handler.go:19`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="prometheus"}"#));

/// Processes a Prometheus exposition-text import request.
/// Go: `prometheusimport.InsertHandler`.
pub(crate) fn insert_handler<S: RowSink>(
    sink: &S,
    limiter: &ConcurrencyLimiter,
    req: &mut Request<'_>,
) -> Result<(), InsertError> {
    let extra_labels = common::get_extra_labels(req)?;
    let default_timestamp = common::get_timestamp(req)?;

    // Backpressure: writeconcurrencylimiter. Held for the whole request.
    let _permit = limiter
        .acquire()
        .map_err(|err| InsertError::unavailable(err.to_string()))?;

    let encoding = req.content_encoding_str();
    let result = with_ctx(|ctx| {
        prometheus::parse_stream(
            req.body(),
            encoding,
            default_timestamp,
            |_msg| { /* no logging framework wired up yet; see module doc */ },
            |rows| convert_and_add(ctx, sink, rows, &extra_labels).map_err(Into::into),
        )
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        prometheus::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable request data maps to 400 (httpserver.Errorf).
        _ => InsertError::bad_request(err.to_string()),
    })
}

/// Converts one parsed block of Prometheus rows to `MetricRow`s and pushes
/// them to the sink. Port of Go `insertRows` (no relabeling, no metadata).
fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    rows: &[Row<'_>],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for r in rows {
        let offset = ctx.arena.len();
        for tag in &r.tags {
            marshal_metric_name_raw(
                &mut ctx.arena,
                &[(tag.key.as_bytes(), tag.value.as_bytes())],
            );
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        // The metric name goes last, encoded as an empty-key pair — same
        // layout convention as `influx.rs`/`promremotewrite.rs`.
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", r.metric.as_bytes())]);
        ctx.entries.push(Entry {
            offset,
            len: ctx.arena.len() - offset,
            timestamp: r.timestamp,
            value: r.value,
        });
    }

    // Go: `rowsInserted.Add(len(rows))` before `ctx.FlushBufs()`
    // (`request_handler.go:67-69`) — incremented even if the flush below
    // fails. Prometheus-exposition rows have no nested values, so
    // `len(rows)` there is `ctx.entries.len()` here (one entry per row).
    ROWS_INSERTED.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
    use esm_protoparser::prometheus::Tag;
    use esm_storage::MetricName;
    use std::borrow::Cow;
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

    fn row(
        metric: &'static str,
        tags: &[(&'static str, &'static str)],
        value: f64,
        timestamp: i64,
    ) -> Row<'static> {
        Row {
            metric,
            tags: tags
                .iter()
                .map(|&(k, v)| Tag {
                    key: k,
                    value: Cow::Borrowed(v),
                })
                .collect(),
            value,
            timestamp,
        }
    }

    fn convert(rows: &[Row<'_>], extra_labels: &[(String, String)]) -> (Vec<GotRow>, Vec<Vec<u8>>) {
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
    fn metric_name_becomes_group_tags_preserved() {
        let rows = [row(
            "cpu_usage",
            &[("host", "h1"), ("region", "us")],
            42.5,
            1000,
        )];
        let (converted, _) = convert(&rows, &[]);
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
        let rows = [row("m", &[("z", "1"), ("a", "2")], 1.0, 1)];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(
            converted[0].tags,
            vec![
                ("z".to_owned(), "1".to_owned()),
                ("a".to_owned(), "2".to_owned()),
            ]
        );
    }

    #[test]
    fn extra_labels_are_appended_after_tags() {
        let rows = [row("m", &[("job", "x")], 1.0, 1)];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let (converted, _) = convert(&rows, &extra);
        assert_eq!(
            converted,
            vec![got("m", &[("job", "x"), ("env", "prod")], 1, 1.0)]
        );
    }

    #[test]
    fn no_tags_row_still_ingests() {
        let rows = [row("m", &[], 3.0, 7)];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(converted, vec![got("m", &[], 7, 3.0)]);
    }

    #[test]
    fn raw_encoding_matches_marshal_metric_name_raw_with_group_last() {
        let rows = [row("cpu", &[("host", "h1")], 3.0, 1)];
        let (_, raw) = convert(&rows, &[]);
        let mut expected = Vec::new();
        marshal_metric_name_raw(
            &mut expected,
            &[
                (b"host", b"h1"),
                (b"", b"cpu"), // metric group is encoded last
            ],
        );
        assert_eq!(raw, vec![expected]);
    }

    #[test]
    fn buffers_are_reused_across_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let rows = [row("m", &[("t", "v")], 1.0, 9), row("m2", &[], 2.0, 10)];
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
        let rows = [row("m", &[], 1.0, 1)];
        let err = convert_and_add(&mut ctx, &FailSink, &rows, &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }
}
