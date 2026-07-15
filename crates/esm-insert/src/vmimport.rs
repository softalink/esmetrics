//! `/api/v1/import` VictoriaMetrics JSON-lines import handler.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/vmimport/request_handler.go`.
//!
//! Deviations from the Go original:
//! - No relabeling (`relabel.HasRelabeling`) — out of scope for this port,
//!   matching how `influx.rs`/`promremotewrite.rs`/`prometheusimport.rs`
//!   already omit it.
//! - `esm_rows_inserted_total{type="vmimport"}` is ported (see
//!   [`ROWS_INSERTED`]); `vm_rows_per_insert{type="vmimport"}` (a histogram)
//!   is not — this crate only ports counters (see `esm_common::metrics`'s
//!   module doc).
//! - Line-parse errors (the `errLogger` callback) are discarded rather than
//!   logged — no logging framework wired up yet in this crate (same gap as
//!   every other handler in this crate).
//!
//! # `__name__` handling
//!
//! Unlike `crate::influx` (distinct `measurement`/`field` types) or
//! `crate::prometheusimport` (a distinct `Row::metric` field),
//! `esm_protoparser::vmimport::Row::tags` carries `__name__` as an ordinary
//! tag, exactly like Go's `insertRows`, which loops over `r.Tags` and calls
//! `ic.AddLabelBytes(tag.Key, tag.Value)` uniformly (no special case for
//! `__name__` in the Go handler at all — that extraction happens later,
//! inside `storage.MetricName`/`InsertCtx`, which this Rust port's
//! `esm_storage::MetricName` mirrors). So this handler special-cases
//! `__name__` itself: it is pulled out of the tag list and marshaled last
//! as the trailing `("", metric_group)` pair, matching the layout
//! convention already established by `crate::promremotewrite` (whose
//! `prompb::Label` list has the same "`__name__` is just another label"
//! shape).
//!
//! # Buffer strategy
//!
//! Same thread-local [`ConvertCtx`] arena pattern as `crate::influx`,
//! `crate::promremotewrite`, and `crate::prometheusimport` (shared via
//! `crate::convert_ctx`). Per parsed
//! block, every row's `MetricNameRaw` bytes are marshaled once into a
//! reused arena — non-`__name__` tags in input order, then extra labels
//! (`extra_label` query args / Pushgateway path labels), then the trailing
//! `("", metric_group)` pair — and every (value, timestamp) pair of that
//! row shares the resulting arena slice, matching Go's `insertRows` inner
//! loop over `r.Values`/`r.Timestamps` with a single, row-scoped
//! `metricNameBuf`.

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::Request;
use esm_protoparser::vmimport::{self, Row};
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `vm_rows_inserted_total{type="vmimport"}`
/// (`app/vminsert/vmimport/request_handler.go:19`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="vmimport"}"#));

/// Processes a `/api/v1/import` request.
/// Go: `vmimport.InsertHandler`.
pub(crate) fn insert_handler<S: RowSink>(
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
        vmimport::parse_stream(
            req.body(),
            encoding,
            |_msg| { /* no logging framework wired up yet; see module doc */ },
            |rows| convert_and_add(ctx, sink, rows, &extra_labels).map_err(Into::into),
        )
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        vmimport::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable request data maps to 400 (httpserver.Errorf).
        _ => InsertError::bad_request(err.to_string()),
    })
}

/// Converts one parsed block of vmimport rows to `MetricRow`s and pushes
/// them to the sink. Port of Go `insertRows` (no relabeling).
fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    rows: &[Row],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for r in rows {
        // Marshal the labels of this row once: every tag except `__name__`
        // in input order, then extra labels, then the metric group (the
        // `__name__` value, or empty if absent) as the trailing empty-key
        // pair. See the module doc's "`__name__` handling" section.
        let base_start = ctx.arena.len();
        let mut metric_group: &[u8] = b"";
        for (key, value) in &r.tags {
            if key.as_slice() == b"__name__" {
                metric_group = value.as_slice();
            } else {
                marshal_metric_name_raw(&mut ctx.arena, &[(key.as_slice(), value.as_slice())]);
            }
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", metric_group)]);
        let base_end = ctx.arena.len();

        // Every (value, timestamp) pair of this row shares the same
        // metric_name_raw slice.
        for (&value, &timestamp) in r.values.iter().zip(r.timestamps.iter()) {
            ctx.entries.push(Entry {
                offset: base_start,
                len: base_end - base_start,
                timestamp,
                value,
            });
        }
    }

    // Go: `rowsInserted.Add(rowsTotal)` before `ic.FlushBufs()`
    // (`request_handler.go:77-79`) — incremented even if the flush below
    // fails. `rowsTotal` there sums `len(r.Values)` per row, i.e.
    // `ctx.entries.len()` here (one entry per value/timestamp pair).
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

    fn row(tags: &[(&str, &str)], values: &[f64], timestamps: &[i64]) -> Row {
        Row {
            tags: tags
                .iter()
                .map(|&(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .collect(),
            values: values.to_vec(),
            timestamps: timestamps.to_vec(),
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
    fn name_tag_becomes_metric_group_other_tags_preserved() {
        let rows = [row(
            &[
                ("__name__", "up"),
                ("job", "node_exporter"),
                ("instance", "localhost:9100"),
            ],
            &[0.0, 1.0],
            &[100, 200],
        )];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(
            converted,
            vec![
                got(
                    "up",
                    &[("job", "node_exporter"), ("instance", "localhost:9100")],
                    100,
                    0.0
                ),
                got(
                    "up",
                    &[("job", "node_exporter"), ("instance", "localhost:9100")],
                    200,
                    1.0
                ),
            ]
        );
    }

    #[test]
    fn one_metric_row_per_value_timestamp_pair() {
        let rows = [row(&[("__name__", "m")], &[1.0, 2.0, 3.0], &[10, 20, 30])];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(converted.len(), 3);
        assert_eq!(converted[0], got("m", &[], 10, 1.0));
        assert_eq!(converted[1], got("m", &[], 20, 2.0));
        assert_eq!(converted[2], got("m", &[], 30, 3.0));
    }

    #[test]
    fn missing_name_tag_yields_empty_metric_group() {
        let rows = [row(&[("foo", "bar")], &[1.0], &[1])];
        let (converted, _) = convert(&rows, &[]);
        assert_eq!(converted, vec![got("", &[("foo", "bar")], 1, 1.0)]);
    }

    #[test]
    fn tags_keep_input_order_without_sorting() {
        let rows = [row(&[("z", "1"), ("a", "2")], &[1.0], &[1])];
        let (converted, _) = convert(&rows, &[]);
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
        let rows = [row(&[("__name__", "m"), ("job", "x")], &[1.0], &[1])];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let (converted, _) = convert(&rows, &extra);
        assert_eq!(
            converted,
            vec![got("m", &[("job", "x"), ("env", "prod")], 1, 1.0)]
        );
    }

    #[test]
    fn raw_encoding_matches_marshal_metric_name_raw_with_group_last() {
        let rows = [row(&[("__name__", "cpu"), ("host", "h1")], &[3.0], &[1])];
        let (_, raw) = convert(&rows, &[]);
        let mut expected = Vec::new();
        marshal_metric_name_raw(&mut expected, &[(b"host", b"h1"), (b"", b"cpu")]);
        assert_eq!(raw, vec![expected]);
    }

    #[test]
    fn buffers_are_reused_across_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let rows = [
            row(&[("__name__", "m"), ("t", "v")], &[1.0], &[9]),
            row(&[("__name__", "m2")], &[2.0], &[10]),
        ];
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
        let rows = [row(&[("__name__", "m")], &[1.0], &[1])];
        let err = convert_and_add(&mut ctx, &FailSink, &rows, &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }
}
