//! DataDog `/api/v1/series` and `/api/v2/series` insert handlers.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/datadogv1/request_handler.go` and
//! `app/vminsert/datadogv2/request_handler.go`. The fixed-response DataDog
//! agent stub endpoints (`/datadog/api/v1/validate`,
//! `/datadog/api/v1/check_run`, `/datadog/intake`,
//! `/datadog/api/v1/metadata`) and the `/datadog/` path-normalization step
//! live in `crate::lib`'s `InsertHandlers::handle`, not here — see that
//! module's doc for the exact status codes/bodies (copied from
//! `app/vminsert/main.go`, not guessed).
//!
//! # Label order (verified against upstream, not assumed)
//!
//! `app/vminsert/datadogv1/request_handler.go`'s `insertRows`:
//! ```go
//! ctx.AddLabel("", ss.Metric)
//! if ss.Host != "" { ctx.AddLabel("host", ss.Host) }
//! if ss.Device != "" { ctx.AddLabel("device", ss.Device) }
//! for _, tag := range ss.Tags {
//!     name, value := datadogutil.SplitTag(tag)
//!     if name == "host" { name = "exported_host" }
//!     ctx.AddLabel(name, value)
//! }
//! for extraLabels { ctx.AddLabel(label.Name, label.Value) }
//! ```
//! So the metric group is marshaled **first** (matching `crate::opentsdb`/
//! `crate::opentsdbhttp`'s group-first convention, not
//! `crate::vmimport`/`crate::promremotewrite`'s group-last one), then
//! `host`/`device` (if non-empty), then tags (renaming a tag literally named
//! `host` to `exported_host` so it never collides with the `host` label
//! above), then extra labels last.
//!
//! `app/vminsert/datadogv2/request_handler.go`'s `insertRows` is the same
//! shape with `resources`/`source_type_name` standing in for
//! `host`/`device`:
//! ```go
//! ctx.AddLabel("", ss.Metric)
//! for _, rs := range ss.Resources { ctx.AddLabel(rs.Type, rs.Name) }
//! for _, tag := range ss.Tags { /* same SplitTag + host->exported_host rename */ }
//! if ss.SourceTypeName != "" { ctx.AddLabel("source_type_name", ss.SourceTypeName) }
//! for extraLabels { ctx.AddLabel(label.Name, label.Value) }
//! ```
//! Each `Resource{Type, Name}` becomes a label named `rs.Type` with value
//! `rs.Name` (e.g. `{"type":"host","name":"h1"}` -> `host="h1"`).
//!
//! # Timestamp units (verified against upstream, not assumed)
//!
//! v1 `Point` is `[timestamp_seconds, value]`; the request handler calls
//! `pt.Timestamp()`, which is `int64(pt[0] * 1000)` — multiply-then-truncate
//! milliseconds, computed here at conversion time (not at parse time; the
//! parser only fills in *missing* seconds-granularity timestamps — see
//! `esm_protoparser::datadog` module doc). v2 `Point.Timestamp` is already
//! an `int64` in seconds; the handler computes `pt.Timestamp * 1000`
//! (integer multiplication, no truncation concern).
//!
//! # Buffer strategy
//!
//! Same thread-local [`ConvertCtx`] arena pattern as
//! `crate::promremotewrite`/`crate::opentsdbhttp`: labels for one series are
//! marshaled once, and every point of that series shares the resulting
//! arena slice (shared via `crate::convert_ctx`).
//!
//! # Metrics
//!
//! `esm_rows_inserted_total{type="datadogv1"}` and
//! `esm_rows_inserted_total{type="datadogv2"}` are ported (see
//! [`ROWS_INSERTED_V1`]/[`ROWS_INSERTED_V2`]); the corresponding
//! `vm_rows_per_insert{type="datadogv1"|"datadogv2"}` histograms are not —
//! this crate only ports counters (see `esm_common::metrics`'s module doc).

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_http::Request;
use esm_protoparser::datadog::{split_tag, v1, v2};
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::{common, ConcurrencyLimiter, InsertError, RowSink};

/// Go: `vm_rows_inserted_total{type="datadogv1"}`
/// (`app/vminsert/datadogv1/request_handler.go:17`).
static ROWS_INSERTED_V1: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="datadogv1"}"#));

/// Go: `vm_rows_inserted_total{type="datadogv2"}`
/// (`app/vminsert/datadogv2/request_handler.go:17`).
static ROWS_INSERTED_V2: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="datadogv2"}"#));

/// Processes a `/datadog/api/v1/series` request.
/// Go: `datadogv1.InsertHandlerForHTTP`.
pub(crate) fn insert_handler_v1<S: RowSink>(
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
        v1::parse_stream(req.body(), encoding, |series| {
            convert_and_add_v1(ctx, sink, series, &extra_labels).map_err(Into::into)
        })
    });
    result.map_err(|err| match err {
        // Sink failures map to 503, like Go InsertCtx.FlushBufs.
        esm_protoparser::datadog::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        // Unreadable/undecodable/unparseable request data maps to 400
        // (httpserver.Errorf).
        _ => InsertError::bad_request(err.to_string()),
    })
}

/// Processes a `/datadog/api/v2/series` request.
/// Go: `datadogv2.InsertHandlerForHTTP`.
pub(crate) fn insert_handler_v2<S: RowSink>(
    sink: &S,
    limiter: &ConcurrencyLimiter,
    req: &mut Request<'_>,
) -> Result<(), InsertError> {
    let extra_labels = common::get_extra_labels(req)?;

    let _permit = limiter
        .acquire()
        .map_err(|err| InsertError::unavailable(err.to_string()))?;

    let encoding = req.content_encoding_str();
    let content_type = req.content_type().unwrap_or("");
    let result = with_ctx(|ctx| {
        v2::parse_stream(req.body(), encoding, content_type, |series| {
            convert_and_add_v2(ctx, sink, series, &extra_labels).map_err(Into::into)
        })
    });
    result.map_err(|err| match err {
        esm_protoparser::datadog::Error::Callback(_) => InsertError::unavailable(err.to_string()),
        _ => InsertError::bad_request(err.to_string()),
    })
}

/// Renames a split tag's name to `exported_host` if it collides with the
/// `host`/resource-type label added earlier, matching both Go handlers'
/// identical `if name == "host" { name = "exported_host" }` step.
fn tag_label_name(name: &str) -> &str {
    if name == "host" {
        "exported_host"
    } else {
        name
    }
}

/// Port of Go `datadogv1.insertRows` (no relabeling) — see the module doc's
/// "Label order" section.
fn convert_and_add_v1<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    series: &[v1::Series],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for s in series {
        let offset = ctx.arena.len();
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", s.metric.as_bytes())]);
        if !s.host.is_empty() {
            marshal_metric_name_raw(&mut ctx.arena, &[(b"host", s.host.as_bytes())]);
        }
        if !s.device.is_empty() {
            marshal_metric_name_raw(&mut ctx.arena, &[(b"device", s.device.as_bytes())]);
        }
        for tag in &s.tags {
            let (name, value) = split_tag(tag);
            let name = tag_label_name(name);
            marshal_metric_name_raw(&mut ctx.arena, &[(name.as_bytes(), value.as_bytes())]);
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        let len = ctx.arena.len() - offset;
        for pt in &s.points {
            // Go: `pt.Timestamp()` = `int64(pt[0] * 1000)`.
            let timestamp = (pt.timestamp_seconds * 1000.0) as i64;
            ctx.entries.push(Entry {
                offset,
                len,
                timestamp,
                value: pt.value,
            });
        }
    }
    // Go: `rowsInserted.Add(rowsTotal)` before `ctx.FlushBufs()`
    // (`app/vminsert/datadogv1/request_handler.go:80-82`) — incremented even
    // if the flush below fails. `rowsTotal` there sums `len(ss.Points)` per
    // series, i.e. `ctx.entries.len()` here (one entry per point).
    ROWS_INSERTED_V1.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

/// Port of Go `datadogv2.insertRows` (no relabeling) — see the module doc's
/// "Label order" section.
fn convert_and_add_v2<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    series: &[v2::Series],
    extra_labels: &[(String, String)],
) -> Result<(), String> {
    ctx.begin();
    for s in series {
        let offset = ctx.arena.len();
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", s.metric.as_bytes())]);
        for rs in &s.resources {
            marshal_metric_name_raw(
                &mut ctx.arena,
                &[(rs.r#type.as_bytes(), rs.name.as_bytes())],
            );
        }
        for tag in &s.tags {
            let (name, value) = split_tag(tag);
            let name = tag_label_name(name);
            marshal_metric_name_raw(&mut ctx.arena, &[(name.as_bytes(), value.as_bytes())]);
        }
        if !s.source_type_name.is_empty() {
            marshal_metric_name_raw(
                &mut ctx.arena,
                &[(b"source_type_name", s.source_type_name.as_bytes())],
            );
        }
        common::append_extra_labels(&mut ctx.arena, extra_labels);
        let len = ctx.arena.len() - offset;
        for pt in &s.points {
            // Go: `pt.Timestamp * 1000` (int64 * int64, no truncation
            // concern since both sides are already integers).
            let timestamp = pt.timestamp * 1000;
            ctx.entries.push(Entry {
                offset,
                len,
                timestamp,
                value: pt.value,
            });
        }
    }
    // Go: `rowsInserted.Add(rowsTotal)` before `ctx.FlushBufs()`
    // (`app/vminsert/datadogv2/request_handler.go:83-85`) — incremented even
    // if the flush below fails. `rowsTotal` there sums `len(ss.Points)` per
    // series, i.e. `ctx.entries.len()` here (one entry per point).
    ROWS_INSERTED_V2.inc_by(ctx.entries.len() as u64);
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

    fn v1_series(
        metric: &str,
        host: &str,
        device: &str,
        tags: &[&str],
        points: &[(f64, f64)],
    ) -> v1::Series {
        v1::Series {
            metric: metric.to_owned(),
            host: host.to_owned(),
            device: device.to_owned(),
            points: points
                .iter()
                .map(|&(ts, value)| v1::Point {
                    timestamp_seconds: ts,
                    value,
                })
                .collect(),
            tags: tags.iter().map(|&t| t.to_owned()).collect(),
        }
    }

    fn convert_v1(series: &[v1::Series], extra: &[(String, String)]) -> Vec<GotRow> {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        convert_and_add_v1(&mut ctx, &sink, series, extra).unwrap();
        sink.rows.into_inner().unwrap()
    }

    #[test]
    fn v1_label_order_is_metric_host_device_tags_extra() {
        let series = [v1_series(
            "sys.cpu",
            "h1",
            "/dev/sda",
            &["region:eu"],
            &[(1_000.0, 42.0)],
        )];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let converted = convert_v1(&series, &extra);
        assert_eq!(
            converted,
            vec![got(
                "sys.cpu",
                &[
                    ("host", "h1"),
                    ("device", "/dev/sda"),
                    ("region", "eu"),
                    ("env", "prod"),
                ],
                1_000_000,
                42.0,
            )]
        );
    }

    #[test]
    fn v1_timestamp_converts_seconds_to_milliseconds() {
        let series = [v1_series("m", "", "", &[], &[(1_727_879_909.0, 1.0)])];
        let converted = convert_v1(&series, &[]);
        assert_eq!(converted[0].timestamp, 1_727_879_909_000);
    }

    #[test]
    fn v1_empty_host_and_device_are_omitted() {
        let series = [v1_series("m", "", "", &[], &[(1.0, 1.0)])];
        let converted = convert_v1(&series, &[]);
        assert_eq!(converted[0].tags, vec![]);
    }

    #[test]
    fn v1_tag_named_host_is_renamed_to_exported_host() {
        let series = [v1_series(
            "m",
            "h1",
            "",
            &["host:agent-host"],
            &[(1.0, 1.0)],
        )];
        let converted = convert_v1(&series, &[]);
        assert_eq!(
            converted[0].tags,
            vec![
                ("host".to_owned(), "h1".to_owned()),
                ("exported_host".to_owned(), "agent-host".to_owned()),
            ]
        );
    }

    #[test]
    fn v1_tag_without_colon_gets_no_label_value() {
        let series = [v1_series("m", "", "", &["standalone"], &[(1.0, 1.0)])];
        let converted = convert_v1(&series, &[]);
        assert_eq!(
            converted[0].tags,
            vec![("standalone".to_owned(), "no_label_value".to_owned())]
        );
    }

    #[test]
    fn v1_multiple_points_share_the_same_labels() {
        let series = [v1_series(
            "m",
            "h1",
            "",
            &[],
            &[(1.0, 10.0), (2.0, 20.0), (3.0, 30.0)],
        )];
        let converted = convert_v1(&series, &[]);
        assert_eq!(converted.len(), 3);
        for row in &converted {
            assert_eq!(row.metric_group, "m");
            assert_eq!(row.tags, vec![("host".to_owned(), "h1".to_owned())]);
        }
        assert_eq!(converted[0].value, 10.0);
        assert_eq!(converted[2].timestamp, 3000);
    }

    #[test]
    fn v1_sink_error_is_propagated() {
        struct FailSink;
        impl RowSink for FailSink {
            fn add_rows(&self, _rows: &[MetricRow<'_>]) -> Result<(), String> {
                Err("storage full".to_owned())
            }
        }
        let series = [v1_series("m", "", "", &[], &[(1.0, 1.0)])];
        let mut ctx = ConvertCtx::default();
        let err = convert_and_add_v1(&mut ctx, &FailSink, &series, &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }

    /// `resources` is `&[(type, name)]`, matching Go `Resource{Type, Name}`'s
    /// field order.
    fn v2_series(
        metric: &str,
        resources: &[(&str, &str)],
        tags: &[&str],
        source_type_name: &str,
        points: &[(i64, f64)],
    ) -> v2::Series {
        v2::Series {
            metric: metric.to_owned(),
            points: points
                .iter()
                .map(|&(timestamp, value)| v2::Point { timestamp, value })
                .collect(),
            resources: resources
                .iter()
                .map(|&(r#type, name)| v2::Resource {
                    name: name.to_owned(),
                    r#type: r#type.to_owned(),
                })
                .collect(),
            source_type_name: source_type_name.to_owned(),
            tags: tags.iter().map(|&t| t.to_owned()).collect(),
        }
    }

    fn convert_v2(series: &[v2::Series], extra: &[(String, String)]) -> Vec<GotRow> {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        convert_and_add_v2(&mut ctx, &sink, series, extra).unwrap();
        sink.rows.into_inner().unwrap()
    }

    #[test]
    fn v2_label_order_is_metric_resources_tags_source_type_extra() {
        let series = [v2_series(
            "sys.load",
            &[("host", "dummyhost")],
            &["environment:test"],
            "kubernetes",
            &[(1_636_629_071, 0.7)],
        )];
        let extra = vec![("env".to_owned(), "prod".to_owned())];
        let converted = convert_v2(&series, &extra);
        assert_eq!(
            converted,
            vec![got(
                "sys.load",
                &[
                    ("host", "dummyhost"),
                    ("environment", "test"),
                    ("source_type_name", "kubernetes"),
                    ("env", "prod"),
                ],
                1_636_629_071_000,
                0.7,
            )]
        );
    }

    #[test]
    fn v2_resource_type_becomes_label_name() {
        let series = [v2_series(
            "m",
            &[("host", "h1"), ("region", "eu")],
            &[],
            "",
            &[(1, 1.0)],
        )];
        let converted = convert_v2(&series, &[]);
        assert_eq!(
            converted[0].tags,
            vec![
                ("host".to_owned(), "h1".to_owned()),
                ("region".to_owned(), "eu".to_owned()),
            ]
        );
    }

    #[test]
    fn v2_tag_named_host_is_renamed_to_exported_host() {
        let series = [v2_series(
            "m",
            &[("host", "h1")],
            &["host:agent-host"],
            "",
            &[(1, 1.0)],
        )];
        let converted = convert_v2(&series, &[]);
        assert_eq!(
            converted[0].tags,
            vec![
                ("host".to_owned(), "h1".to_owned()),
                ("exported_host".to_owned(), "agent-host".to_owned()),
            ]
        );
    }

    #[test]
    fn v2_empty_source_type_name_is_omitted() {
        let series = [v2_series("m", &[], &[], "", &[(1, 1.0)])];
        let converted = convert_v2(&series, &[]);
        assert_eq!(converted[0].tags, vec![]);
    }

    #[test]
    fn v2_timestamp_converts_seconds_to_milliseconds() {
        let series = [v2_series("m", &[], &[], "", &[(2, 5.0)])];
        let converted = convert_v2(&series, &[]);
        assert_eq!(converted[0].timestamp, 2000);
    }

    #[test]
    fn v2_multiple_points_share_the_same_labels() {
        let series = [v2_series("m", &[], &[], "", &[(1, 1.0), (2, 2.0)])];
        let converted = convert_v2(&series, &[]);
        assert_eq!(converted.len(), 2);
        assert_eq!(converted[0].metric_group, "m");
        assert_eq!(converted[1].metric_group, "m");
    }

    #[test]
    fn v2_sink_error_is_propagated() {
        struct FailSink;
        impl RowSink for FailSink {
            fn add_rows(&self, _rows: &[MetricRow<'_>]) -> Result<(), String> {
                Err("storage full".to_owned())
            }
        }
        let series = [v2_series("m", &[], &[], "", &[(1, 1.0)])];
        let mut ctx = ConvertCtx::default();
        let err = convert_and_add_v2(&mut ctx, &FailSink, &series, &[]).unwrap_err();
        assert_eq!(err, "storage full");
    }

    #[test]
    fn buffers_are_reused_across_v1_and_v2_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let v1s = [v1_series("m", "h1", "", &[], &[(1.0, 1.0)])];
        convert_and_add_v1(&mut ctx, &sink, &v1s, &[]).unwrap();
        let rows_cap = ctx.rows_capacity();
        assert!(rows_cap >= 1, "rows Vec must have allocated capacity");
        let v2s = [v2_series("m2", &[], &[], "", &[(2, 2.0)])];
        convert_and_add_v2(&mut ctx, &sink, &v2s, &[]).unwrap();
        assert_eq!(
            ctx.rows_capacity(),
            rows_cap,
            "rows Vec allocation must be recycled, not reallocated"
        );
        assert_eq!(sink.rows.into_inner().unwrap().len(), 2);
    }
}
