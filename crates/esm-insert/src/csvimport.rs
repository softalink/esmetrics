//! CSV import handler: `/api/v1/import/csv`.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/csvimport/request_handler.go`.
//!
//! Deviations from the Go original:
//! - No relabeling (`relabel.HasRelabeling`) — out of scope, matching every
//!   other handler in this crate.
//! - `esm_rows_inserted_total{type="csvimport"}` is ported (see
//!   [`ROWS_INSERTED`]); `vm_rows_per_insert{type="csvimport"}` (a histogram)
//!   is not — this crate only ports counters (see `esm_common::metrics`'s
//!   module doc).
//! - Line-parse errors (the `errLogger` callback) are discarded rather than
//!   logged — no logging framework wired up yet (same gap as every other
//!   handler here).
//!
//! # `format` query arg
//!
//! Required: Go's `csvimport.ParseColumnDescriptors("")` already fails on a
//! missing/empty `format` (no `metric` column can be found in an empty
//! string), surfacing as a 400 either way; this handler just gives that case
//! its own clearer message before ever calling the parser.
//!
//! # Label ordering
//!
//! `esm_protoparser::csvimport::Row::tags` is column-scan order (Go:
//! `parseRows`'s per-line loop appends to `tags` in the same order), so
//! marshaling tags in that order is already input-order, no descriptor
//! lookup needed. Metric name is marshaled last as the trailing `("",
//! metric)` pair, matching the layout convention already established by
//! `crate::influx`/`crate::prometheusimport`/`crate::vmimport` (not Go's
//! literal `ctx.AddLabel("", r.Metric)`-first order, which is immaterial to
//! Go's own output either, per those modules' docs).
//!
//! # Buffer strategy
//!
//! Same thread-local [`ConvertCtx`] arena pattern as `crate::influx`,
//! `crate::promremotewrite`, `crate::prometheusimport`, and `crate::vmimport`
//! — shared via `crate::convert_ctx`.

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::Request;
use esm_protoparser::csvimport::{self, Row};
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `vm_rows_inserted_total{type="csvimport"}`
/// (`app/vminsert/csvimport/request_handler.go:16`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="csvimport"}"#));

/// Processes a `/api/v1/import/csv` request. Go: `csvimport.InsertHandler`.
pub(crate) fn insert_handler<S: RowSink>(
    sink: &S,
    limiter: &ConcurrencyLimiter,
    req: &mut Request<'_>,
) -> Result<(), InsertError> {
    let format = common::query_param(req, "format")
        .ok_or_else(|| InsertError::bad_request("missing `format` query arg".to_owned()))?;
    let cds = csvimport::parse_column_descriptors(&format).map_err(|err| {
        InsertError::bad_request(format!("cannot parse the provided csv format: {err}"))
    })?;
    let extra_labels = common::get_extra_labels(req)?;

    // Backpressure: writeconcurrencylimiter. Held for the whole request.
    let _permit = limiter
        .acquire()
        .map_err(|err| InsertError::unavailable(err.to_string()))?;

    let encoding = req.content_encoding_str();
    let result = with_ctx(|ctx| {
        csvimport::parse_stream(
            req.body(),
            encoding,
            &cds,
            |_msg| { /* no logging framework wired up yet; see module doc */ },
            |rows| convert_and_add(ctx, sink, rows, &extra_labels).map_err(Into::into),
        )
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        csvimport::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable request data, or a bad `format`, maps to
        // 400 (httpserver.Errorf).
        _ => InsertError::bad_request(err.to_string()),
    })
}

/// Converts one parsed block of csv rows to `MetricRow`s and pushes them to
/// the sink. Port of Go `insertRows` (no relabeling).
fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    rows: &[Row],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for r in rows {
        let offset = ctx.arena.len();
        for (key, value) in &r.tags {
            marshal_metric_name_raw(&mut ctx.arena, &[(key.as_bytes(), value.as_bytes())]);
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", r.metric.as_bytes())]);
        ctx.entries.push(Entry {
            offset,
            len: ctx.arena.len() - offset,
            timestamp: r.timestamp,
            value: r.value,
        });
    }

    // Go: `rowsInserted.Add(len(rows))` before `ctx.FlushBufs()`
    // (`request_handler.go:56-58`) — incremented even if the flush below
    // fails. CSV rows have no nested values, so `len(rows)` there is
    // `ctx.entries.len()` here (one entry per row).
    ROWS_INSERTED.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
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

    fn row(metric: &str, tags: &[(&str, &str)], value: f64, timestamp: i64) -> Row {
        Row {
            metric: metric.to_owned(),
            tags: tags
                .iter()
                .map(|&(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            value,
            timestamp,
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
    fn metric_column_becomes_group_labels_in_column_order() {
        let rows = [row(
            "temperature",
            &[("device", "sensor-1")],
            23.5,
            1_447_116_400_000,
        )];
        let converted = convert(&rows, &[]);
        assert_eq!(
            converted,
            vec![got(
                "temperature",
                &[("device", "sensor-1")],
                1_447_116_400_000,
                23.5
            )]
        );
    }

    #[test]
    fn multiple_metric_columns_share_tags_and_timestamp() {
        let rows = [
            row("bid", &[("symbol", "AUDCAD")], 0.9725, 1000),
            row("ask", &[("symbol", "AUDCAD")], 0.97273, 1000),
        ];
        let converted = convert(&rows, &[]);
        assert_eq!(
            converted,
            vec![
                got("bid", &[("symbol", "AUDCAD")], 1000, 0.9725),
                got("ask", &[("symbol", "AUDCAD")], 1000, 0.97273),
            ]
        );
    }

    #[test]
    fn extra_labels_are_appended_after_tags() {
        let rows = [row("m", &[("job", "x")], 1.0, 1)];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let converted = convert(&rows, &extra);
        assert_eq!(
            converted,
            vec![got("m", &[("job", "x"), ("env", "prod")], 1, 1.0)]
        );
    }

    #[test]
    fn no_tags_row_still_ingests() {
        let rows = [row("m", &[], 3.0, 7)];
        assert_eq!(convert(&rows, &[]), vec![got("m", &[], 7, 3.0)]);
    }

    #[test]
    fn raw_encoding_matches_marshal_metric_name_raw_with_group_last() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let rows = [row("cpu", &[("host", "h1")], 3.0, 1)];
        convert_and_add(&mut ctx, &sink, &rows, &[]).unwrap();
        let mut expected = Vec::new();
        marshal_metric_name_raw(&mut expected, &[(b"host", b"h1"), (b"", b"cpu")]);
        assert_eq!(ctx.arena, expected);
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
        assert_eq!(sink.rows.into_inner().unwrap().len(), 4);
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
