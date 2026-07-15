//! Prometheus remote-write `/api/v1/write` handler.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/promremotewrite/request_handler.go`.
//!
//! Deviations from the Go original:
//! - No relabeling (`relabel.HasRelabeling`) and no metric-metadata handling
//!   (`prommetadata`) — out of scope for this port, matching how `influx.rs`
//!   already omits them.
//! - `esm_rows_inserted_total{type="promremotewrite"}` is ported (see
//!   [`ROWS_INSERTED`]); `vm_rows_per_insert{type="promremotewrite"}` (a
//!   histogram) and `vm_metadata_rows_inserted_total` are not — this crate only
//!   ports counters (see `esm_common::metrics`'s module doc).
//!
//! # Buffer strategy
//!
//! Same thread-local [`ConvertCtx`] arena pattern as [`crate::influx`]
//! (shared via `crate::convert_ctx`):
//! per parsed block, every series' `MetricNameRaw` bytes are marshaled once
//! into a reused arena — non-`__name__` labels in input order, then extra
//! labels, then the trailing `("", metric_group)` pair — and every sample of
//! that series shares the resulting arena slice (unlike influx, where each
//! field of a row gets its own metric name).

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::Request;
use esm_protoparser::prompb::TimeSeries;
use esm_protoparser::promremotewrite as remotewrite;
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `vm_rows_inserted_total{type="promremotewrite"}`
/// (`app/vminsert/promremotewrite/request_handler.go:17`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="promremotewrite"}"#));

/// Processes a Prometheus remote-write request.
/// Go: `promremotewrite.InsertHandler`.
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

    // Go: `req.Header.Get("Content-Encoding") == "zstd"` decides the
    // decompression fallback order inside `stream.Parse`.
    let encoding = req.content_encoding_str();
    let result = with_ctx(|ctx| {
        remotewrite::parse(req.body(), encoding, |tss| {
            convert_and_add(ctx, sink, tss, &extra_labels).map_err(Into::into)
        })
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        remotewrite::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable request data maps to 400 (httpserver.Errorf).
        other => InsertError::bad_request(other.to_string()),
    })
}

/// Converts one parsed `WriteRequest`'s timeseries to `MetricRow`s and
/// pushes them to the sink. Port of Go `insertRows` (no relabeling, no
/// metadata).
fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    timeseries: &[TimeSeries<'_>],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for ts in timeseries {
        // Marshal the labels of this series once: every label except
        // `__name__` in input order, then extra labels, then the metric
        // group (the `__name__` value, or empty if absent) as the trailing
        // empty-key pair — same layout as Go
        // `WriteDataPointExt(nil, labels, ...)` with `labels[len-1]` holding
        // `__name__`.
        let base_start = ctx.arena.len();
        let mut metric_group: &[u8] = b"";
        for src_label in &ts.labels {
            if src_label.name == b"__name__" {
                metric_group = src_label.value;
            } else {
                marshal_metric_name_raw(&mut ctx.arena, &[(src_label.name, src_label.value)]);
            }
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", metric_group)]);
        let base_end = ctx.arena.len();

        // Every sample of the series shares the same metric_name_raw slice.
        for sample in &ts.samples {
            ctx.entries.push(Entry {
                offset: base_start,
                len: base_end - base_start,
                timestamp: sample.timestamp,
                value: sample.value,
            });
        }
    }

    // Go: `rowsInserted.Add(rowsTotal)` before `ctx.FlushBufs()`
    // (`request_handler.go:72-75`) — incremented even if the flush below
    // fails. `rowsTotal` there sums `len(ts.Samples)` per series, i.e.
    // `ctx.entries.len()` here (one entry per sample, not per series).
    ROWS_INSERTED.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
    use esm_protoparser::prompb::{Label, Sample};
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

    fn label(name: &'static str, value: &'static str) -> Label<'static> {
        Label {
            name: name.as_bytes(),
            value: value.as_bytes(),
        }
    }

    fn series(
        labels: &[(&'static str, &'static str)],
        samples: &[(f64, i64)],
    ) -> TimeSeries<'static> {
        TimeSeries {
            labels: labels.iter().map(|&(n, v)| label(n, v)).collect(),
            samples: samples
                .iter()
                .map(|&(value, timestamp)| Sample { value, timestamp })
                .collect(),
        }
    }

    fn convert(
        timeseries: &[TimeSeries<'_>],
        extra_labels: &[(String, String)],
    ) -> (Vec<GotRow>, Vec<Vec<u8>>) {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        convert_and_add(&mut ctx, &sink, timeseries, extra_labels).unwrap();
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
    fn name_label_becomes_metric_group() {
        let tss = [series(
            &[("__name__", "foo"), ("job", "x")],
            &[(42.5, 1000)],
        )];
        let (converted, _) = convert(&tss, &[]);
        assert_eq!(converted, vec![got("foo", &[("job", "x")], 1000, 42.5)]);
    }

    #[test]
    fn label_order_is_preserved() {
        // No -sortLabels equivalent here: non-name labels marshal in input
        // order; canonical sorting happens later in storage.
        let tss = [series(
            &[("__name__", "m"), ("z", "1"), ("a", "2")],
            &[(1.0, 1)],
        )];
        let (converted, _) = convert(&tss, &[]);
        assert_eq!(
            converted[0].tags,
            vec![
                ("z".to_owned(), "1".to_owned()),
                ("a".to_owned(), "2".to_owned())
            ]
        );
    }

    #[test]
    fn one_row_per_sample() {
        let tss = [series(
            &[("__name__", "m")],
            &[(1.0, 100), (2.0, 200), (3.0, 300)],
        )];
        let (converted, raw) = convert(&tss, &[]);
        assert_eq!(
            converted,
            vec![
                got("m", &[], 100, 1.0),
                got("m", &[], 200, 2.0),
                got("m", &[], 300, 3.0),
            ]
        );
        // All three samples of the same series share the same metric name.
        assert_eq!(raw[0], raw[1]);
        assert_eq!(raw[1], raw[2]);
    }

    #[test]
    fn extra_labels_appended_last() {
        let tss = [series(&[("__name__", "m"), ("job", "x")], &[(1.0, 1)])];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let (converted, _) = convert(&tss, &extra);
        assert_eq!(
            converted,
            vec![got("m", &[("job", "x"), ("env", "prod")], 1, 1.0)]
        );
    }

    #[test]
    fn series_without_name_label_still_ingests() {
        // Upstream does not reject a series lacking `__name__`; the metric
        // group is simply empty.
        let tss = [series(&[("job", "x")], &[(1.0, 1)])];
        let (converted, _) = convert(&tss, &[]);
        assert_eq!(converted, vec![got("", &[("job", "x")], 1, 1.0)]);
    }

    #[test]
    fn all_empty_label_series_is_dropped() {
        // Every label has an empty value and there is no metric group, so Go's
        // InsertCtx.TryPrepareLabels early-skips the series
        // (`len(ctx.Labels) == 0`). The samples are still counted by
        // rowsInserted upstream, so the counter must move even though nothing
        // is stored.
        let before = ROWS_INSERTED.get();
        let tss = [series(
            &[("job", ""), ("__name__", "")],
            &[(1.0, 1), (2.0, 2)],
        )];
        let (converted, raw) = convert(&tss, &[]);
        assert!(converted.is_empty(), "degenerate series must not be stored");
        assert!(raw.is_empty(), "degenerate series must not be stored");
        assert!(
            ROWS_INSERTED.get() >= before + 2,
            "skipped samples are still counted, matching Go rowsTotal"
        );
    }

    #[test]
    fn raw_encoding_matches_marshal_metric_name_raw() {
        let tss = [series(
            &[("__name__", "cpu_usage"), ("host", "h1")],
            &[(3.0, 1)],
        )];
        let (_, raw) = convert(&tss, &[]);
        let mut expected = Vec::new();
        marshal_metric_name_raw(
            &mut expected,
            &[
                (b"host", b"h1"),
                (b"", b"cpu_usage"), // metric group is encoded last
            ],
        );
        assert_eq!(raw, vec![expected]);
    }

    #[test]
    fn buffers_are_reused_across_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let tss = [series(&[("__name__", "m")], &[(1.0, 9), (2.0, 10)])];
        convert_and_add(&mut ctx, &sink, &tss, &[]).unwrap();
        let arena_cap = ctx.arena.capacity();
        let rows_cap = ctx.rows_capacity();
        assert!(rows_cap >= 2);
        convert_and_add(&mut ctx, &sink, &tss, &[]).unwrap();
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
        let tss = [series(&[("__name__", "m")], &[(1.0, 1)])];
        let err = convert_and_add(&mut ctx, &FailSink, &tss, &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }

    /// Counter is process-global (shared with every other test in the
    /// binary), so assert on the delta, not an absolute value.
    #[test]
    fn rows_inserted_counter_increments_by_sample_count() {
        let before = ROWS_INSERTED.get();
        let tss = [series(
            &[("__name__", "m")],
            &[(1.0, 100), (2.0, 200), (3.0, 300)],
        )];
        let (converted, _) = convert(&tss, &[]);
        assert_eq!(converted.len(), 3, "one entry per sample, not per series");
        // `>=`, not `==`: other tests in this file increment the same
        // process-global counter concurrently (parallel test execution).
        assert!(ROWS_INSERTED.get() >= before + 3);
    }
}
