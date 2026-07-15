//! Influx line-protocol `/write` handler.
//!
//! Port of the upstream VictoriaMetrics v1.146.0 `app/vminsert/influx/request_handler.go`
//! with the command-line flags fixed at their defaults:
//!
//! - `-influxMeasurementFieldSeparator` = `"_"`
//! - `-influxSkipSingleField` = `false` (the field key is always appended)
//! - `-influxSkipMeasurement` = `false`
//! - `-influxDBLabel` = `"db"`
//! - `-sortLabels` = `false` (labels are marshaled in input order; canonical
//!   tag sorting happens later in storage, exactly like Go)
//!
//! Name mapping: metric name = `{measurement}_{field_key}`; if the
//! measurement is empty the metric name is just `{field_key}`. There is no
//! special case for a field named `value` in v1.146.0 with default flags.
//!
//! # Metrics
//!
//! `esm_rows_inserted_total{type="influx"}` is ported (see [`ROWS_INSERTED`]).
//! `vm_rows_per_insert{type="influx"}` (a histogram) is not — out of scope,
//! this crate only ports counters (see `esm_common::metrics`'s module doc).
//!
//! # Buffer strategy
//!
//! Conversion state lives in a thread-local [`ConvertCtx`] (the server is
//! thread-per-connection, so this is the Rust analogue of Go's
//! `pushCtxPool`). Per parse-stream block, every metric name is appended to
//! a single reused byte arena (Go `pushCtx.metricNameBuf`): the per-row base
//! (tags + `db` label + extra labels) is marshaled once and memcpy'd from
//! within the arena for each field, then the `("", metric_group)` pair is
//! appended last — byte-identical layout to Go's
//! `WriteDataPoint(metricNameBuf, labels[len-1:])`. [`MetricRow`]s borrow
//! the arena; the row Vec's allocation is recycled across blocks via
//! [`ConvertCtx::flush_to`](crate::convert_ctx::ConvertCtx::flush_to).
//! Steady-state, a request allocates nothing once the buffers have grown.

use std::cell::RefCell;
use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::Request;
use esm_protoparser::influx::Row;
use esm_protoparser::stream;
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `-influxMeasurementFieldSeparator` default.
const MEASUREMENT_FIELD_SEPARATOR: &[u8] = b"_";
/// Go: `-influxDBLabel` default.
const DB_LABEL: &str = "db";

/// Go: `vm_rows_inserted_total{type="influx"}`
/// (`app/vminsert/influx/request_handler.go:29`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="influx"}"#));

/// Processes an influx line-protocol write request.
/// Go: `influx.InsertHandlerForHTTP`.
pub(crate) fn insert_handler_for_http<S: RowSink>(
    sink: &S,
    limiter: &ConcurrencyLimiter,
    req: &mut Request<'_>,
) -> Result<(), InsertError> {
    let db = common::query_param(req, "db").unwrap_or_default();
    let precision = common::query_param(req, "precision").unwrap_or_default();
    let extra_labels = common::get_extra_labels(req)?;

    // Backpressure: writeconcurrencylimiter. Held for the whole request.
    let _permit = limiter
        .acquire()
        .map_err(|err| InsertError::unavailable(err.to_string()))?;

    // parse_stream does its own gunzipping, so hand it the raw framed body.
    let is_gzipped = req.is_gzipped();
    let result = with_ctx(|ctx| {
        GROUP_BUF.with(|cell| {
            let group_buf = &mut *cell.borrow_mut();
            stream::parse_stream(req.body(), is_gzipped, &precision, &db, |rows| {
                convert_and_add(ctx, group_buf, sink, rows, &db, &extra_labels).map_err(Into::into)
            })
        })
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        stream::Error::Callback { .. } => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable request data maps to 400 (httpserver.Errorf).
        other => InsertError::bad_request(other.to_string()),
    })
}

// `{measurement}{separator}{field_key}` scratch buffer, kept as a file-local
// thread-local (rather than a field on the shared `ConvertCtx`) since it is
// specific to influx's group-last marshaling. Go: `pushCtx.metricGroupBuf`.
thread_local! {
    static GROUP_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Converts one parsed block of influx rows to `MetricRow`s and pushes them
/// to the sink. Port of Go `insertRows` (no relabeling, no series limits).
fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    group_buf: &mut Vec<u8>,
    sink: &S,
    rows: &[Row<'_>],
    db: &str,
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for r in rows {
        // Marshal the per-row base labels once: tags in input order, then
        // the db label (unless a `db` tag is already present), then extra
        // labels. marshal_metric_name_raw skips empty values, matching
        // InsertCtx.AddLabel + MarshalMetricNameRaw.
        let base_start = ctx.arena.len();
        let mut has_db_key = false;
        for tag in &r.tags {
            if tag.key.as_ref() == DB_LABEL {
                has_db_key = true;
            }
            marshal_metric_name_raw(
                &mut ctx.arena,
                &[(tag.key.as_bytes(), tag.value.as_bytes())],
            );
        }
        if !has_db_key {
            marshal_metric_name_raw(&mut ctx.arena, &[(DB_LABEL.as_bytes(), db.as_bytes())]);
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        let base_end = ctx.arena.len();

        // `{measurement}{separator}` metric-group prefix, shared by all the
        // fields of the row.
        group_buf.clear();
        group_buf.extend_from_slice(r.measurement.as_bytes());
        if !group_buf.is_empty() {
            group_buf.extend_from_slice(MEASUREMENT_FIELD_SEPARATOR);
        }
        let group_prefix_len = group_buf.len();

        for f in &r.fields {
            group_buf.truncate(group_prefix_len);
            group_buf.extend_from_slice(f.key.as_bytes());

            let offset = ctx.arena.len();
            ctx.arena.extend_from_within(base_start..base_end);
            // The metric group goes last, encoded as an empty-key pair —
            // same layout as Go WriteDataPoint(prefix, labels[len-1:]).
            marshal_metric_name_raw(&mut ctx.arena, &[(b"", group_buf.as_slice())]);
            ctx.entries.push(Entry {
                offset,
                len: ctx.arena.len() - offset,
                timestamp: r.timestamp,
                value: f.value,
            });
        }
    }

    // Go: `rowsInserted.Add(rowsTotal)` before `ic.FlushBufs()`
    // (`request_handler.go:151-153`) — incremented even if the flush below
    // fails, so this mirrors that ordering exactly. `rowsTotal` there is a
    // running sum of `len(r.Fields)` per row, i.e. `ctx.entries.len()` here
    // (one entry per data point, not per row).
    ROWS_INSERTED.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
    use esm_protoparser::influx::{Field, Row, Tag};
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
        measurement: &'static str,
        tags: &[(&'static str, &'static str)],
        fields: &[(&'static str, f64)],
        timestamp: i64,
    ) -> Row<'static> {
        Row {
            measurement: Cow::Borrowed(measurement),
            tags: tags
                .iter()
                .map(|&(k, v)| Tag {
                    key: Cow::Borrowed(k),
                    value: Cow::Borrowed(v),
                })
                .collect(),
            fields: fields
                .iter()
                .map(|&(k, v)| Field {
                    key: Cow::Borrowed(k),
                    value: v,
                })
                .collect(),
            timestamp,
        }
    }

    fn convert(
        rows: &[Row<'_>],
        db: &str,
        extra_labels: &[(String, String)],
    ) -> (Vec<GotRow>, Vec<Vec<u8>>) {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let mut group_buf = Vec::new();
        convert_and_add(&mut ctx, &mut group_buf, &sink, rows, db, extra_labels).unwrap();
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
    fn maps_measurement_and_field_to_metric_name_per_field() {
        let rows = [row(
            "cpu",
            &[("hostname", "host_0")],
            &[("usage_user", 58.0), ("usage_system", 2.0)],
            1451606400000,
        )];
        let (converted, _) = convert(&rows, "benchmark", &[]);
        assert_eq!(
            converted,
            vec![
                got(
                    "cpu_usage_user",
                    &[("hostname", "host_0"), ("db", "benchmark")],
                    1451606400000,
                    58.0,
                ),
                got(
                    "cpu_usage_system",
                    &[("hostname", "host_0"), ("db", "benchmark")],
                    1451606400000,
                    2.0,
                ),
            ]
        );
    }

    #[test]
    fn field_named_value_has_no_special_case() {
        // v1.146.0 default flags: the separator + field key are always
        // appended, even for a single field named "value".
        let rows = [row("cpu", &[], &[("value", 1.5)], 42)];
        let (converted, _) = convert(&rows, "", &[]);
        assert_eq!(converted, vec![got("cpu_value", &[], 42, 1.5)]);
    }

    #[test]
    fn empty_measurement_uses_field_key_without_separator() {
        let rows = [row("", &[], &[("baz", 123.0)], 7)];
        let (converted, _) = convert(&rows, "", &[]);
        assert_eq!(converted, vec![got("baz", &[], 7, 123.0)]);
    }

    #[test]
    fn db_param_becomes_label_unless_db_tag_present() {
        let rows = [
            row("m", &[("a", "b")], &[("f", 1.0)], 1),
            row("m", &[("db", "own")], &[("f", 2.0)], 2),
        ];
        let (converted, _) = convert(&rows, "fromquery", &[]);
        assert_eq!(
            converted,
            vec![
                got("m_f", &[("a", "b"), ("db", "fromquery")], 1, 1.0),
                got("m_f", &[("db", "own")], 2, 2.0),
            ]
        );
    }

    #[test]
    fn empty_db_param_adds_no_label() {
        let rows = [row("m", &[("a", "b")], &[("f", 1.0)], 1)];
        let (converted, _) = convert(&rows, "", &[]);
        assert_eq!(converted, vec![got("m_f", &[("a", "b")], 1, 1.0)]);
    }

    #[test]
    fn extra_labels_are_appended_after_db_label() {
        let rows = [row("m", &[("t", "v")], &[("f", 1.0)], 1)];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let (converted, _) = convert(&rows, "d", &extra);
        assert_eq!(
            converted,
            vec![got(
                "m_f",
                &[("t", "v"), ("db", "d"), ("env", "prod")],
                1,
                1.0
            )]
        );
    }

    #[test]
    fn tags_keep_input_order_without_sorting() {
        // -sortLabels defaults to false: labels are marshaled in input
        // order; canonical sorting happens later in storage.
        let rows = [row("m", &[("z", "1"), ("a", "2")], &[("f", 1.0)], 1)];
        let (converted, _) = convert(&rows, "", &[]);
        assert_eq!(
            converted[0].tags,
            vec![
                ("z".to_owned(), "1".to_owned()),
                ("a".to_owned(), "2".to_owned()),
            ]
        );
    }

    #[test]
    fn raw_encoding_matches_marshal_metric_name_raw_with_group_last() {
        let rows = [row("cpu", &[("host", "h1")], &[("usage", 3.0)], 1)];
        let (_, raw) = convert(&rows, "db0", &[]);
        let mut expected = Vec::new();
        marshal_metric_name_raw(
            &mut expected,
            &[
                (b"host", b"h1"),
                (b"db", b"db0"),
                (b"", b"cpu_usage"), // metric group is encoded last
            ],
        );
        assert_eq!(raw, vec![expected]);
    }

    #[test]
    fn buffers_are_reused_across_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let mut group_buf = Vec::new();
        let rows = [row("m", &[("t", "v")], &[("f", 1.0), ("g", 2.0)], 9)];
        convert_and_add(&mut ctx, &mut group_buf, &sink, &rows, "d", &[]).unwrap();
        let arena_cap = ctx.arena.capacity();
        let rows_cap = ctx.rows_capacity();
        assert!(rows_cap >= 2);
        convert_and_add(&mut ctx, &mut group_buf, &sink, &rows, "d", &[]).unwrap();
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
        let mut group_buf = Vec::new();
        let rows = [row("m", &[], &[("f", 1.0)], 1)];
        let err = convert_and_add(&mut ctx, &mut group_buf, &FailSink, &rows, "", &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }

    /// Counter is process-global (shared with every other test in the
    /// binary), so assert on the delta, not an absolute value.
    #[test]
    fn rows_inserted_counter_increments_by_field_count() {
        let before = ROWS_INSERTED.get();
        let rows = [row(
            "cpu",
            &[("hostname", "host_0")],
            &[("usage_user", 58.0), ("usage_system", 2.0)],
            1451606400000,
        )];
        let (converted, _) = convert(&rows, "benchmark", &[]);
        assert_eq!(converted.len(), 2, "one entry per field, not per row");
        // `>=`, not `==`: other tests in this file increment the same
        // process-global counter concurrently (parallel test execution).
        assert!(ROWS_INSERTED.get() >= before + 2);
    }
}
