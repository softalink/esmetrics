//! `metricsql_expr_test` sample comparison for `esmalert-tool`.
//!
//! Port of VictoriaMetrics `app/vmalert-tool/unittest/recording.go`
//! (`checkMetricsqlCase` + `expSample` label/value comparison). Split out of
//! `runner.rs` to keep that file under the module size cap; `runner.rs`'s
//! post-loop `metricsql_expr_test` pass calls [`check_metricsql_expr_test_case`].

// Scaffold stage: not wired into `main()` yet — only used from `runner.rs`
// and this module's callers' tests until the CLI lands (a later task).
#![allow(dead_code)]

use esm_metricsql::Expr;

use esmalert::datasource::Datasource;

use crate::schema::MetricsqlTestCase;
use crate::ToolError;

/// Epsilon used to compare a `metricsql_expr_test` sample's actual value
/// against its `exp_samples` expectation. Deliberate divergence from
/// upstream's exact `reflect.DeepEqual` float comparison
/// (`unittest/recording.go:89`), to tolerate floating-point noise in the
/// query round-trip.
const SAMPLE_VALUE_EPSILON: f64 = 1e-9;

/// Parses an `ExpSample.labels` string (e.g. `up{job="x"}`, `{a="b"}`, or
/// `""`) into a label set, via the same metricsql selector parse
/// `input::expand_series` uses for `input_series`. An empty string parses to
/// an empty label set (matching upstream's `if s.Labels != ""` guard,
/// `unittest/recording.go:53`) rather than being sent through the parser.
///
/// Port of the `exp_samples`-parsing half of `checkMetricsqlCase`
/// (`unittest/recording.go:52-73`).
fn parse_labels_selector(s: &str) -> Result<Vec<(String, String)>, ToolError> {
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let expr = esm_metricsql::parse(s)
        .map_err(|e| ToolError::new(format!("failed to parse labels {s:?}: {e}")))?;
    let Expr::Metric(metric_expr) = expr else {
        return Err(ToolError::new(format!("got invalid exp_samples: {s:?}")));
    };
    if metric_expr.label_filterss.len() > 1 {
        return Err(ToolError::new(format!("got invalid exp_samples: {s:?}")));
    }
    let Some(lfs) = metric_expr.label_filterss.first() else {
        return Ok(Vec::new());
    };
    Ok(lfs
        .iter()
        .map(|f| (f.label.clone(), f.value.clone()))
        .collect())
}

/// Renders a label set as its canonical `name{k="v",...}` selector string,
/// e.g. `[("__name__","up"),("job","x")]` -> `up{job="x"}`. Used only for
/// human-readable diff messages — the actual comparison in
/// [`check_metricsql_expr_test_case`] is by parsed label-set equality, not
/// by this string form, so formatting quirks here never cause a false diff.
fn labels_to_selector_string(labels: &[(String, String)]) -> String {
    let mut name = "";
    let mut kvs = Vec::new();
    for (k, v) in labels {
        if k == "__name__" {
            name = v;
        } else {
            kvs.push(format!("{k}={v:?}"));
        }
    }
    if kvs.is_empty() {
        name.to_string()
    } else {
        format!("{name}{{{}}}", kvs.join(","))
    }
}

/// Checks one [`MetricsqlTestCase`] by running `case.expr` as an instant
/// query against `ds` at `ts_ms`, and set-matching (order-insensitive) the
/// returned samples against `case.exp_samples` by parsed label-set equality
/// plus a [`SAMPLE_VALUE_EPSILON`]-tolerant value comparison. Appends a
/// human-readable diff to `diffs` on any mismatch (query error, unparsable
/// `exp_samples.labels`, or missing/extra/differing sample).
///
/// Port of `checkMetricsqlCase`'s per-case body (`unittest/recording.go:33-94`).
pub(crate) fn check_metricsql_expr_test_case(
    ds: &Datasource,
    test_group_name: &str,
    case: &MetricsqlTestCase,
    ts_ms: i64,
    diffs: &mut Vec<String>,
) {
    // Upstream's `checkMetricsqlCase` sets `nocache=1` and
    // `latency_offset=1ms` query params on this instant query
    // (`recording.go:31`); this port omits them — against the in-process
    // harness the exact-`eval_time` query returns correct results without
    // them (a deliberate simplification, like the epsilon comparison above).
    let result = match ds.query(&case.expr, ts_ms) {
        Ok(r) => r,
        Err(e) => {
            diffs.push(format!(
                "testGroupName: {test_group_name}, expr: {:?}, time: {:?}, err: {e}",
                case.expr, case.eval_time
            ));
            return;
        }
    };

    let mut got: Vec<(Vec<(String, String)>, f64)> = result
        .data
        .iter()
        .map(|m| {
            let mut labels = m.labels.clone();
            labels.sort();
            (labels, m.values.first().copied().unwrap_or(f64::NAN))
        })
        .collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));

    let mut exp: Vec<(Vec<(String, String)>, f64)> = Vec::with_capacity(case.exp_samples.len());
    for s in &case.exp_samples {
        let mut labels = match parse_labels_selector(&s.labels) {
            Ok(labels) => labels,
            Err(e) => {
                diffs.push(format!(
                    "testGroupName: {test_group_name}, expr: {:?}, time: {:?}, err: {e}",
                    case.expr, case.eval_time
                ));
                return;
            }
        };
        labels.sort();
        exp.push((labels, s.value));
    }
    exp.sort_by(|a, b| a.0.cmp(&b.0));

    let matches = exp.len() == got.len()
        && exp
            .iter()
            .zip(got.iter())
            .all(|(e, g)| e.0 == g.0 && (e.1 - g.1).abs() < SAMPLE_VALUE_EPSILON);

    if !matches {
        let render = |samples: &[(Vec<(String, String)>, f64)]| -> String {
            samples
                .iter()
                .map(|(labels, v)| format!("{} => {v}", labels_to_selector_string(labels)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        diffs.push(format!(
            "testGroupName: {test_group_name}, expr: {:?}, time: {:?}\n    exp: {}\n    got: {}",
            case.expr,
            case.eval_time,
            render(&exp),
            render(&got),
        ));
    }
}
