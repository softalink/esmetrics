//! Behavioral and validation tests. Ports `TestAggregatorsFailure` and
//! `TestAggregatorsEqual` from `streamaggr_test.go`, plus direct push→flush
//! aggregation checks (the upstream behavioral coverage lives in a
//! `synctest` virtual-clock harness that is not ported; these drive the same
//! aggregation logic deterministically via a synchronous flush).

use std::time::{SystemTime, UNIX_EPOCH};

use crate::aggregator::Aggregators;
use crate::config::Options;
use crate::{Label, Sample, TimeSeries};

fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
    TimeSeries {
        labels: labels
            .iter()
            .map(|(n, v)| Label {
                name: (*n).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
        samples: samples
            .iter()
            .map(|(t, v)| Sample {
                timestamp: *t,
                value: *v,
            })
            .collect(),
    }
}

fn labels_to_string(labels: &[Label]) -> String {
    let mut sorted: Vec<&Label> = labels.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let inner: Vec<String> = sorted
        .iter()
        .map(|l| format!("{}={:?}", l.name, l.value))
        .collect();
    format!("{{{}}}", inner.join(","))
}

/// Renders a push→flush result the way upstream's `timeSeriessToString` does:
/// one `labels value` line per output series, sorted by label string.
fn aggregate(config: &str, tss: &[TimeSeries]) -> String {
    let opts = Options::default();
    let a = Aggregators::load_without_flusher(config, &opts).expect("load config");
    let out = a.push_and_flush(tss);
    let mut lines: Vec<String> = out
        .iter()
        .map(|ts| {
            assert_eq!(ts.samples.len(), 1, "expected 1 sample per output series");
            format!("{} {}\n", labels_to_string(&ts.labels), ts.samples[0].value)
        })
        .collect();
    lines.sort();
    lines.concat()
}

fn expect_failure(config: &str) {
    let opts = Options::default();
    let r = Aggregators::load_without_flusher(config, &opts);
    assert!(r.is_err(), "expected error for config:\n{config}");
}

#[test]
fn config_failures() {
    // Ports TestAggregatorsFailure.
    expect_failure("foobar");
    expect_failure("\n- interval: 1m\n  outputs: [total]\n  foobar: baz\n");
    expect_failure("\n- outputs: [total]\n");
    expect_failure("\n- interval: 1m\n");
    expect_failure("\n- interval: 1foo\n  outputs: [total]\n");
    expect_failure("\n- interval: 1m\n  outputs: [foobar]\n");
    expect_failure("\n- outputs: [total]\n  interval: -5m\n");
    expect_failure("\n- outputs: [total]\n  interval: 10ms\n");
    expect_failure("\n- interval: 1m\n  dedup_interval: 1foo\n  outputs: [\"quantiles(0.5)\"]\n");
    expect_failure("\n- interval: 1m\n  dedup_interval: 35s\n  outputs: [\"quantiles(0.5)\"]\n");
    expect_failure("\n- interval: 1m\n  dedup_interval: 1h\n  outputs: [\"quantiles(0.5)\"]\n");
    expect_failure(
        "\n- interval: 1m\n  staleness_interval: 1foo\n  outputs: [\"quantiles(0.5)\"]\n",
    );
    expect_failure(
        "\n- interval: 1m\n  staleness_interval: 30s\n  outputs: [\"quantiles(0.5)\"]\n",
    );
    expect_failure(
        "\n- interval: 1m\n  keep_metric_names: true\n  outputs: [\"total\", \"increase\"]\n",
    );
    expect_failure(
        "\n- interval: 1m\n  keep_metric_names: true\n  outputs: [\"histogram_bucket\"]\n",
    );
    expect_failure(
        "\n- interval: 1m\n  outputs: [total]\n  input_relabel_configs:\n  - action: replace\n",
    );
    expect_failure(
        "\n- interval: 1m\n  outputs: [total]\n  output_relabel_configs:\n  - action: replace\n",
    );
    expect_failure("\n- interval: 1m\n  outputs: [total]\n  by: [foo]\n  without: [bar]\n");
    expect_failure("\n- interval: 1m\n  outputs: [\"quantiles(\"]\n");
    expect_failure("\n- interval: 1m\n  outputs: [\"quantiles()\"]\n");
    expect_failure("\n- interval: 1m\n  outputs: [\"quantiles(foo)\"]\n");
    expect_failure("\n- interval: 1m\n  outputs: [\"quantiles(-0.5)\"]\n");
    expect_failure("\n- interval: 1m\n  outputs: [\"quantiles(1.5)\"]\n");
    expect_failure("\n- interval: 1m\n  outputs: [total, total]\n");
    expect_failure("\n- interval: 1m\n  outputs: [\"quantiles(0.5)\", \"quantiles(0.9)\"]\n");
    // bare "quantiles" (no phis) is not a valid output name
    expect_failure("\n- interval: 1m\n  outputs: [\"quantiles\"]\n");
}

#[test]
fn aggregators_equal() {
    // Ports TestAggregatorsEqual.
    fn check(a: &str, b: &str, expected: bool) {
        let opts = Options::default();
        let aa = Aggregators::load_without_flusher(a, &opts).unwrap();
        let ab = Aggregators::load_without_flusher(b, &opts).unwrap();
        assert_eq!(aa.equal(&ab), expected, "a={a:?} b={b:?}");
    }
    check("", "", true);
    check("\n- outputs: [total]\n  interval: 5m\n", "", false);
    check(
        "\n- outputs: [total]\n  interval: 5m\n",
        "\n- outputs: [total]\n  interval: 5m\n",
        true,
    );
    check(
        "\n- outputs: [total]\n  interval: 3m\n",
        "\n- outputs: [total]\n  interval: 5m\n",
        false,
    );
    check(
        "\n- outputs: [total]\n  interval: 5m\n  flush_on_shutdown: true\n",
        "\n- outputs: [total]\n  interval: 5m\n  flush_on_shutdown: false\n",
        false,
    );
    check(
        "\n- outputs: [total]\n  interval: 5m\n  ignore_first_intervals: 2\n",
        "\n- outputs: [total]\n  interval: 5m\n  ignore_first_intervals: 4\n",
        false,
    );
}

#[test]
fn sum_and_count_samples() {
    let cfg = "\n- interval: 1m\n  outputs: [sum_samples, count_samples]\n";
    let tss = vec![
        series(&[("__name__", "foo")], &[(0, 10.0)]),
        series(&[("__name__", "foo")], &[(0, 20.0)]),
        series(&[("__name__", "bar")], &[(0, 100.0)]),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(
        got,
        "{__name__=\"bar:1m_count_samples\"} 1\n\
         {__name__=\"bar:1m_sum_samples\"} 100\n\
         {__name__=\"foo:1m_count_samples\"} 2\n\
         {__name__=\"foo:1m_sum_samples\"} 30\n"
    );
}

#[test]
fn avg_min_max_last() {
    let cfg = "\n- interval: 1m\n  outputs: [avg, min, max, last]\n";
    let tss = vec![
        series(&[("__name__", "x")], &[(1, 4.0)]),
        series(&[("__name__", "x")], &[(2, 8.0)]),
        series(&[("__name__", "x")], &[(3, 6.0)]),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(
        got,
        "{__name__=\"x:1m_avg\"} 6\n\
         {__name__=\"x:1m_last\"} 6\n\
         {__name__=\"x:1m_max\"} 8\n\
         {__name__=\"x:1m_min\"} 4\n"
    );
}

#[test]
fn total_counts_delta() {
    // Two increasing samples of the same counter → delta 20, independent of
    // keep_first_sample behaviour.
    let cfg = "\n- interval: 1m\n  outputs: [total]\n";
    let tss = vec![
        series(&[("__name__", "c"), ("job", "a")], &[(1000, 10.0)]),
        series(&[("__name__", "c"), ("job", "a")], &[(2000, 30.0)]),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(got, "{__name__=\"c:1m_total\",job=\"a\"} 20\n");
}

#[test]
fn windowed_total_plumbs_through() {
    // Exercises the enable_windows config path end to end: the aggregator
    // builds the shared-state (blue+green) output map, routes both fresh
    // samples to the blue window, and flushes the same 20 delta as the plain
    // `total` output. `no_align_flush_to_interval` sets min_time to the
    // aggregator's construction time — which is a hair AFTER the `now` captured
    // here. Both samples are placed a few seconds ahead of `now` so they sit
    // safely above min_deadline (never dropped as "old" due to sub-ms
    // construction jitter) and below max_deadline (min_time + 1m), keeping the
    // test deterministic regardless of wall-clock position.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let cfg = "\n- interval: 1m\n  enable_windows: true\n  no_align_flush_to_interval: true\n  outputs: [total]\n";
    let tss = vec![
        series(&[("__name__", "c"), ("job", "a")], &[(now + 5_000, 10.0)]),
        series(&[("__name__", "c"), ("job", "a")], &[(now + 6_000, 30.0)]),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(got, "{__name__=\"c:1m_total\",job=\"a\"} 20\n");
}

#[test]
fn count_series_groups_by_labels() {
    let cfg = "\n- interval: 1m\n  by: [job]\n  outputs: [count_series]\n";
    let tss = vec![
        series(
            &[("__name__", "m"), ("job", "a"), ("inst", "1")],
            &[(0, 1.0)],
        ),
        series(
            &[("__name__", "m"), ("job", "a"), ("inst", "2")],
            &[(0, 1.0)],
        ),
        series(
            &[("__name__", "m"), ("job", "b"), ("inst", "1")],
            &[(0, 1.0)],
        ),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(
        got,
        "{__name__=\"m:1m_by_job_count_series\",job=\"a\"} 2\n\
         {__name__=\"m:1m_by_job_count_series\",job=\"b\"} 1\n"
    );
}

#[test]
fn without_grouping_drops_labels() {
    let cfg = "\n- interval: 1m\n  without: [inst]\n  outputs: [sum_samples]\n";
    let tss = vec![
        series(
            &[("__name__", "m"), ("job", "a"), ("inst", "1")],
            &[(0, 3.0)],
        ),
        series(
            &[("__name__", "m"), ("job", "a"), ("inst", "2")],
            &[(0, 4.0)],
        ),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(
        got,
        "{__name__=\"m:1m_without_inst_sum_samples\",job=\"a\"} 7\n"
    );
}

#[test]
fn keep_metric_names_leaves_name() {
    let cfg = "\n- interval: 1m\n  keep_metric_names: true\n  outputs: [sum_samples]\n";
    let tss = vec![
        series(&[("__name__", "foo")], &[(0, 10.0)]),
        series(&[("__name__", "foo")], &[(0, 5.0)]),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(got, "{__name__=\"foo\"} 15\n");
}

#[test]
fn match_filter_selects_series() {
    let cfg = "\n- interval: 1m\n  match: '{__name__=\"keep\"}'\n  outputs: [count_samples]\n";
    let tss = vec![
        series(&[("__name__", "keep")], &[(0, 1.0)]),
        series(&[("__name__", "drop")], &[(0, 1.0)]),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(got, "{__name__=\"keep:1m_count_samples\"} 1\n");
}

#[test]
fn quantiles_output() {
    let cfg = "\n- interval: 1m\n  outputs: [\"quantiles(0.5, 1.0)\"]\n";
    let tss = vec![
        series(&[("__name__", "q")], &[(0, 1.0)]),
        series(&[("__name__", "q")], &[(0, 2.0)]),
        series(&[("__name__", "q")], &[(0, 3.0)]),
    ];
    let got = aggregate(cfg, &tss);
    assert_eq!(
        got,
        "{__name__=\"q:1m_quantiles\",quantile=\"0.5\"} 2\n\
         {__name__=\"q:1m_quantiles\",quantile=\"1\"} 3\n"
    );
}

#[test]
fn output_relabel_applies() {
    let cfg = "\n- interval: 1m\n  outputs: [count_samples]\n  output_relabel_configs:\n  - target_label: env\n    replacement: prod\n";
    let tss = vec![series(&[("__name__", "m")], &[(0, 1.0)])];
    let got = aggregate(cfg, &tss);
    assert_eq!(got, "{__name__=\"m:1m_count_samples\",env=\"prod\"} 1\n");
}
