//! OpenTSDB telnet `put` row -> `MetricRow` conversion, shared by the TCP and
//! UDP listeners in [`crate::ingestserver`].
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `app/vminsert/opentsdb/request_handler.go` `insertRows`, which is
//! byte-for-byte the same label-construction logic as
//! `app/vminsert/graphite/request_handler.go` `insertRows` (both call
//! `ctx.AddLabel("", r.Metric)` before the tag loop) — see `crate::graphite`'s
//! module doc for the full explanation of why the metric group is marshaled
//! *first* here, unlike `crate::influx`'s group-last layout.
//!
//! No relabeling and no timeseries-limit check are ported, matching the
//! scope cuts already made throughout this crate. The telnet/UDP transports
//! carry no query string, so there is nothing to add beyond the metric and
//! its tags — confirmed by `insertRows`'s signature (`rows []parser.Row`, no
//! extra params).
//!
//! # Metrics
//!
//! `esm_rows_inserted_total{type="opentsdb"}` is ported (see
//! [`ROWS_INSERTED`]); `vm_rows_per_insert{type="opentsdb"}` (a histogram) is
//! not — this crate only ports counters (see `esm_common::metrics`'s module
//! doc). Per-connection/per-datagram `vm_ingestserver_requests_total`
//! counters live in `crate::ingestserver`, not here.

use std::sync::LazyLock;

use esm_common::metrics::{get_or_create_counter, Counter};
use esm_protoparser::opentsdb::Row;
use esm_storage::marshal_metric_name_raw;

use crate::convert_ctx::{with_ctx, ConvertCtx, Entry};
use crate::RowSink;

/// Go: `vm_rows_inserted_total{type="opentsdb"}`
/// (`app/vminsert/opentsdb/request_handler.go:14`).
static ROWS_INSERTED: LazyLock<&'static Counter> =
    LazyLock::new(|| get_or_create_counter(r#"esm_rows_inserted_total{type="opentsdb"}"#));

/// Converts one parsed block of OpenTSDB rows to `MetricRow`s and pushes
/// them to the sink. Port of Go `insertRows`.
pub fn insert_rows<S: RowSink>(sink: &S, rows: &[Row<'_>]) -> Result<(), String> {
    with_ctx(|ctx| convert_and_add(ctx, sink, rows))
}

fn convert_and_add<S: RowSink>(
    ctx: &mut ConvertCtx,
    sink: &S,
    rows: &[Row<'_>],
) -> Result<(), String> {
    ctx.begin();
    for r in rows {
        let offset = ctx.arena.len();
        // Metric group first, then tags in input order (see module doc).
        marshal_metric_name_raw(&mut ctx.arena, &[(b"", r.metric.as_bytes())]);
        for tag in &r.tags {
            marshal_metric_name_raw(
                &mut ctx.arena,
                &[(tag.key.as_bytes(), tag.value.as_bytes())],
            );
        }
        ctx.entries.push(Entry {
            offset,
            len: ctx.arena.len() - offset,
            timestamp: r.timestamp,
            value: r.value,
        });
    }

    // Go: `rowsInserted.Add(len(rows))` before `ctx.FlushBufs()`
    // (`request_handler.go:46-48`) — incremented even if the flush below
    // fails. OpenTSDB rows have no nested values, so `len(rows)` there is
    // `ctx.entries.len()` here (one entry per row).
    ROWS_INSERTED.inc_by(ctx.entries.len() as u64);
    ctx.flush_to(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricRow;
    use esm_protoparser::opentsdb::Tag;
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

    fn row<'a>(
        metric: &'a str,
        tags: &[(&'a str, &'a str)],
        value: f64,
        timestamp: i64,
    ) -> Row<'a> {
        Row {
            metric,
            tags: tags
                .iter()
                .map(|&(k, v)| Tag { key: k, value: v })
                .collect(),
            value,
            timestamp,
        }
    }

    fn convert(rows: &[Row<'_>]) -> (Vec<GotRow>, Vec<Vec<u8>>) {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        convert_and_add(&mut ctx, &sink, rows).unwrap();
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
        let (converted, _) = convert(&rows);
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
        // OpenTSDB's parser itself allows a put line with no tags (VM issue
        // #3290, already ported in `esm_protoparser::opentsdb`); the
        // converter has no additional tag-count requirement.
        let rows = [row("sys.cpu", &[], 1.0, 100)];
        let (converted, _) = convert(&rows);
        assert_eq!(converted, vec![got("sys.cpu", &[], 100, 1.0)]);
    }

    #[test]
    fn multiple_rows_are_all_converted() {
        let rows = [row("a", &[("t", "1")], 1.0, 10), row("b", &[], 2.0, 20)];
        let (converted, _) = convert(&rows);
        assert_eq!(
            converted,
            vec![got("a", &[("t", "1")], 10, 1.0), got("b", &[], 20, 2.0)]
        );
    }

    #[test]
    fn raw_encoding_has_metric_group_before_tags() {
        let rows = [row("sys.cpu", &[("host", "h1")], 3.0, 1)];
        let (_, raw) = convert(&rows);
        let mut expected = Vec::new();
        marshal_metric_name_raw(&mut expected, &[(b"", b"sys.cpu"), (b"host", b"h1")]);
        assert_eq!(raw, vec![expected]);
    }

    #[test]
    fn buffers_are_reused_across_batches() {
        let sink = CollectSink::default();
        let mut ctx = ConvertCtx::default();
        let rows = [row("m", &[("t", "v")], 1.0, 9)];
        convert_and_add(&mut ctx, &sink, &rows).unwrap();
        let arena_cap = ctx.arena.capacity();
        let rows_cap = ctx.rows_capacity();
        assert!(rows_cap >= 1);
        convert_and_add(&mut ctx, &sink, &rows).unwrap();
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
        let err = convert_and_add(&mut ctx, &FailSink, &rows).unwrap_err();
        assert_eq!(err, "storage full");
    }
}
