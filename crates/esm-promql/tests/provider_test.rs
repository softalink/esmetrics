//! Series-backed evaluation tests using an in-memory `MetricsProvider`:
//! rollups over synthetic series (incl. the TSBS query shapes), incremental
//! vs general aggregation equivalence, staleness handling and offsets.

use esm_common::decimal::STALE_NAN;
use esm_promql::provider::{Deadline, MetricsProvider, SearchQuery, Series};
use esm_promql::{exec, EvalConfig, QueryResult};
use esm_storage::metric_name::MetricName;
use std::sync::Arc;

const START: i64 = 1_000_000;
const END: i64 = 2_000_000;
const STEP: i64 = 200_000;
const NAN: f64 = f64::NAN;

/// In-memory provider matching label filters against stored series.
struct FakeProvider {
    series: Vec<Series>,
}

impl FakeProvider {
    fn new() -> Self {
        FakeProvider { series: Vec::new() }
    }

    fn add(&mut self, name: &str, tags: &[(&str, &str)], samples: &[(i64, f64)]) {
        let mut mn = MetricName {
            metric_group: name.as_bytes().to_vec(),
            ..Default::default()
        };
        for &(k, v) in tags {
            mn.add_tag(k, v);
        }
        self.series.push(Series {
            metric_name: mn,
            timestamps: Arc::new(samples.iter().map(|&(t, _)| t).collect()),
            values: samples.iter().map(|&(_, v)| v).collect(),
        });
    }
}

fn filter_matches(lf: &esm_metricsql::LabelFilter, mn: &MetricName) -> bool {
    let value: &[u8] = if lf.label == "__name__" {
        &mn.metric_group
    } else {
        mn.get_tag_value(&lf.label).unwrap_or(b"")
    };
    let value = String::from_utf8_lossy(value);
    let matches = if lf.is_regexp {
        regex::Regex::new(&format!("^(?:{})$", lf.value))
            .map(|re| re.is_match(&value))
            .unwrap_or(false)
    } else {
        value == lf.value
    };
    matches != lf.is_negative
}

impl MetricsProvider for FakeProvider {
    fn search(&self, sq: &SearchQuery, _deadline: Deadline) -> esm_promql::Result<Vec<Series>> {
        let mut result = Vec::new();
        for s in &self.series {
            let matched = sq
                .tag_filterss
                .iter()
                .any(|tfs| tfs.iter().all(|lf| filter_matches(lf, &s.metric_name)));
            if !matched {
                continue;
            }
            // Restrict samples to [sq.start .. sq.end].
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            for (i, &ts) in s.timestamps.iter().enumerate() {
                if ts >= sq.start && ts <= sq.end {
                    timestamps.push(ts);
                    values.push(s.values[i]);
                }
            }
            if timestamps.is_empty() {
                continue;
            }
            result.push(Series {
                metric_name: s.metric_name.clone(),
                timestamps: Arc::new(timestamps),
                values,
            });
        }
        Ok(result)
    }
}

/// The standard TSBS-ish data set: cpu_usage_user{hostname="hN"} sampled
/// every 10s on [START-10m .. END]; value = ts_secs/10 + N*1000.
fn tsbs_provider(hosts: usize) -> FakeProvider {
    let mut p = FakeProvider::new();
    for n in 0..hosts {
        let samples: Vec<(i64, f64)> = ((START - 600_000) / 10_000..=END / 10_000)
            .map(|i| {
                let ts = i * 10_000;
                (ts, ts as f64 / 10_000.0 + n as f64 * 1000.0)
            })
            .collect();
        p.add(
            "cpu_usage_user",
            &[("hostname", &format!("h{n}"))],
            &samples,
        );
    }
    p
}

fn eval_config() -> EvalConfig {
    let mut ec = EvalConfig::new(START, END, STEP);
    ec.max_points_per_series = 10_000;
    ec.max_series = 1000;
    ec
}

fn run(p: &dyn MetricsProvider, q: &str) -> Vec<QueryResult> {
    exec(p, &eval_config(), q).unwrap_or_else(|err| panic!("unexpected error for {q:?}: {err}"))
}

#[track_caller]
fn assert_values(got: &[f64], want: &[f64], ctx: &str) {
    assert_eq!(got.len(), want.len(), "{ctx}: length mismatch");
    for (j, (&g, &w)) in got.iter().zip(want).enumerate() {
        if w.is_nan() {
            assert!(g.is_nan(), "{ctx}: value #{j}: got {g}; want NaN");
        } else {
            assert!(
                !g.is_nan() && (g - w).abs() / w.abs().max(f64::MIN_POSITIVE) <= 1e-13,
                "{ctx}: value #{j}: got {g}; want {w}"
            );
        }
    }
}

#[test]
fn default_rollup_over_series() {
    let p = tsbs_provider(2);
    let result = run(&p, "cpu_usage_user");
    assert_eq!(result.len(), 2);
    // Samples land exactly on grid points, so default_rollup returns the
    // sample value at every grid timestamp; metric names are kept.
    for (n, r) in result.iter().enumerate() {
        assert_eq!(r.metric_name.metric_group, b"cpu_usage_user");
        assert_eq!(
            r.metric_name.get_tag_value("hostname"),
            Some(format!("h{n}").as_bytes())
        );
        let want: Vec<f64> = r
            .timestamps
            .iter()
            .map(|&ts| ts as f64 / 10_000.0 + n as f64 * 1000.0)
            .collect();
        assert_values(&r.values, &want, "default_rollup");
    }
}

#[test]
fn regex_selector() {
    let p = tsbs_provider(4);
    let result = run(&p, r#"cpu_usage_user{hostname=~'h1|h2'}"#);
    assert_eq!(result.len(), 2);
    let result = run(&p, r#"cpu_usage_user{hostname!="h0"}"#);
    assert_eq!(result.len(), 3);
    let result = run(&p, r#"{__name__=~'cpu_.*'}"#);
    assert_eq!(result.len(), 4);
}

#[test]
fn max_over_time_window() {
    let p = tsbs_provider(1);
    let result = run(&p, "max_over_time(cpu_usage_user[1m])");
    assert_eq!(result.len(), 1);
    // Window is (t-60s, t]: the max is the sample at t itself.
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| ts as f64 / 10_000.0)
        .collect();
    assert_values(&result[0].values, &want, "max_over_time");

    let result = run(&p, "min_over_time(cpu_usage_user[1m])");
    // Min over (t-60s, t] with 10s samples = value at t-50s.
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| (ts - 50_000) as f64 / 10_000.0)
        .collect();
    assert_values(&result[0].values, &want, "min_over_time");

    let result = run(&p, "count_over_time(cpu_usage_user[1m])");
    assert_values(&result[0].values, &[6.0; 6], "count_over_time");

    let result = run(&p, "avg_over_time(cpu_usage_user[1m])");
    // Average of the 6 samples in (t-60, t]: value(t) - 2.5.
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| ts as f64 / 10_000.0 - 2.5)
        .collect();
    assert_values(&result[0].values, &want, "avg_over_time");

    let result = run(&p, "sum_over_time(cpu_usage_user[1m])");
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| 6.0 * (ts as f64 / 10_000.0) - 15.0)
        .collect();
    assert_values(&result[0].values, &want, "sum_over_time");
}

#[test]
fn tsbs_query_shape_max_by_name() {
    // The primary TSBS query shape:
    // max(max_over_time(m[1m])) by (__name__) over multiple hosts.
    let p = tsbs_provider(4);
    let result = run(
        &p,
        r#"max(max_over_time(cpu_usage_user{hostname=~'h1|h2'}[1m])) by (__name__)"#,
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].metric_name.metric_group, b"cpu_usage_user");
    assert_eq!(result[0].metric_name.tags.len(), 0);
    // h2 dominates: max = value(t) for host 2.
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| ts as f64 / 10_000.0 + 2000.0)
        .collect();
    assert_values(&result[0].values, &want, "tsbs max");
}

#[test]
fn tsbs_query_shape_avg_by_name_hostname() {
    let p = tsbs_provider(3);
    let result = run(
        &p,
        r#"avg(avg_over_time({__name__=~'cpu_(usage_user|b)'}[1h])) by (__name__, hostname)"#,
    );
    assert_eq!(result.len(), 3);
    for r in &result {
        assert_eq!(r.metric_name.metric_group, b"cpu_usage_user");
        assert!(r.metric_name.get_tag_value("hostname").is_some());
    }
}

#[test]
fn rate_and_increase_over_counter() {
    // Counter increasing by 1 every 10s (value = ts/10000).
    let p = tsbs_provider(1);
    let result = run(&p, "rate(cpu_usage_user[1m])");
    assert_eq!(result.len(), 1);
    // rate = 1 unit / 10s = 0.1 per second.
    assert_values(&result[0].values, &[0.1; 6], "rate");

    let result = run(&p, "increase(cpu_usage_user[1m])");
    // increase over (t-60, t] with prev sample at t-60 = 6 units.
    assert_values(&result[0].values, &[6.0; 6], "increase");

    let result = run(&p, "delta(cpu_usage_user[1m])");
    assert_values(&result[0].values, &[6.0; 6], "delta");
}

#[test]
fn rate_with_counter_reset() {
    let mut p = FakeProvider::new();
    // A counter which resets at t=1,500,000: 0,10,20,...
    let samples: Vec<(i64, f64)> = ((START - 600_000) / 10_000..=END / 10_000)
        .map(|i| {
            let ts = i * 10_000;
            let v = if ts < 1_500_000 {
                (ts / 1000) as f64
            } else {
                ((ts - 1_500_000) / 1000) as f64
            };
            (ts, v)
        })
        .collect();
    p.add("counter_with_reset", &[], &samples);
    let result = run(&p, "rate(counter_with_reset[1m])");
    assert_eq!(result.len(), 1);
    // The counter grows 1 unit/s before and after the reset;
    // removeCounterResets makes the rate a constant 1.0.
    assert_values(&result[0].values, &[1.0; 6], "rate with reset");
}

#[test]
fn staleness_markers() {
    let mut p = FakeProvider::new();
    // Samples every 10s; stale markers on [1,390,000 .. 1,600,000].
    let samples: Vec<(i64, f64)> = ((START - 600_000) / 10_000..=END / 10_000)
        .map(|i| {
            let ts = i * 10_000;
            let v = if (1_390_000..=1_600_000).contains(&ts) {
                STALE_NAN
            } else {
                1.0
            };
            (ts, v)
        })
        .collect();
    p.add("m_with_stale", &[], &samples);

    // default_rollup keeps stale markers -> gaps at grid points whose last
    // window sample is a stale marker.
    let result = run(&p, "m_with_stale");
    assert_eq!(result.len(), 1);
    assert_values(
        &result[0].values,
        &[1.0, 1.0, NAN, NAN, 1.0, 1.0],
        "default_rollup staleness",
    );

    // max_over_time drops stale markers before the rollup: the window
    // (t-200s, t] at t=1.4e6/1.6e6 still contains non-stale samples.
    let result = run(&p, "max_over_time(m_with_stale[200s])");
    assert_eq!(result.len(), 1);
    assert_values(
        &result[0].values,
        &[1.0, 1.0, 1.0, NAN, 1.0, 1.0],
        "max_over_time staleness",
    );
}

#[test]
fn offset_modifier() {
    let p = tsbs_provider(1);
    let result = run(&p, "cpu_usage_user offset 100s");
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].timestamps.as_slice(),
        &[
            START,
            START + STEP,
            START + 2 * STEP,
            START + 3 * STEP,
            START + 4 * STEP,
            END
        ]
    );
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| (ts - 100_000) as f64 / 10_000.0)
        .collect();
    assert_values(&result[0].values, &want, "offset");
}

#[test]
fn quantile_over_time_scalar_arg() {
    let p = tsbs_provider(1);
    let result = run(&p, "quantile_over_time(0, cpu_usage_user[1m])");
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| (ts - 50_000) as f64 / 10_000.0)
        .collect();
    assert_values(&result[0].values, &want, "quantile_over_time(0)");
    let result = run(&p, "quantile_over_time(1, cpu_usage_user[1m])");
    let want: Vec<f64> = result[0]
        .timestamps
        .iter()
        .map(|&ts| ts as f64 / 10_000.0)
        .collect();
    assert_values(&result[0].values, &want, "quantile_over_time(1)");
}

#[test]
fn incremental_vs_general_equivalence() {
    // The incremental fast path (aggr over rollup over metricExpr) must
    // produce the same results as the general path. `union(...)` forces the
    // general path while passing series through unchanged.
    let p = tsbs_provider(5);
    for aggr in [
        "sum", "min", "max", "avg", "count", "group", "sum2", "geomean",
    ] {
        for modifier in ["", "by (hostname)", "by (__name__)", "without (hostname)"] {
            for rollup in ["max_over_time", "avg_over_time"] {
                let q_incr = format!("{aggr}({rollup}(cpu_usage_user[1m])) {modifier}");
                let q_gen = format!("{aggr}(union({rollup}(cpu_usage_user[1m]))) {modifier}");
                let mut r_incr = exec(&p, &eval_config(), &q_incr).unwrap();
                let mut r_gen = exec(&p, &eval_config(), &q_gen).unwrap();
                assert_eq!(
                    r_incr.len(),
                    r_gen.len(),
                    "series count mismatch for {q_incr:?}"
                );
                r_incr.sort_by(|a, b| {
                    format!("{}", a.metric_name).cmp(&format!("{}", b.metric_name))
                });
                r_gen.sort_by(|a, b| {
                    format!("{}", a.metric_name).cmp(&format!("{}", b.metric_name))
                });
                for (a, b) in r_incr.iter().zip(&r_gen) {
                    assert_eq!(
                        format!("{}", a.metric_name),
                        format!("{}", b.metric_name),
                        "metric name mismatch for {q_incr:?}"
                    );
                    assert_values(&a.values, &b.values, &format!("{q_incr:?}"));
                }
            }
        }
    }
}

#[test]
fn incremental_equivalence_multi_worker() {
    // Repeat the equivalence check with several worker splits to exercise
    // the per-worker map merge.
    let p = tsbs_provider(7);
    let reference = exec(
        &p,
        &eval_config(),
        "sum(avg_over_time(cpu_usage_user[1m])) by (__name__)",
    )
    .unwrap();
    assert_eq!(reference.len(), 1);
    for workers in [1usize, 2, 3, 8, 32] {
        let mut ec = eval_config();
        ec.max_workers = workers;
        let result = exec(
            &p,
            &ec,
            "sum(avg_over_time(cpu_usage_user[1m])) by (__name__)",
        )
        .unwrap();
        assert_eq!(result.len(), 1, "workers={workers}");
        assert_values(
            &result[0].values,
            &reference[0].values,
            &format!("workers={workers}"),
        );
    }
}

#[test]
fn aggr_limit_on_incremental_path() {
    let p = tsbs_provider(4);
    let result = run(&p, "sum(cpu_usage_user) by (hostname) limit 2");
    assert_eq!(result.len(), 2);
}

#[test]
fn instant_query() {
    let p = tsbs_provider(2);
    let mut ec = eval_config();
    ec.start = END;
    ec.end = END;
    let result = exec(&p, &ec, "max_over_time(cpu_usage_user[1m])").unwrap();
    assert_eq!(result.len(), 2);
    for (n, r) in result.iter().enumerate() {
        assert_eq!(r.timestamps.as_slice(), &[END]);
        assert_values(
            &r.values,
            &[END as f64 / 10_000.0 + n as f64 * 1000.0],
            "instant",
        );
    }
}

#[test]
fn binary_op_between_selectors() {
    let p = tsbs_provider(2);
    let result = run(
        &p,
        r#"cpu_usage_user{hostname="h1"} - on(hostname) cpu_usage_user{hostname="h1"}"#,
    );
    assert_eq!(result.len(), 1);
    assert_values(&result[0].values, &[0.0; 6], "self subtraction");
}

#[test]
fn deadline_exceeded() {
    let p = tsbs_provider(1);
    let mut ec = eval_config();
    ec.deadline = Deadline::from_timeout(std::time::Duration::from_millis(1));
    std::thread::sleep(std::time::Duration::from_millis(20));
    let result = exec(&p, &ec, "max_over_time(cpu_usage_user[1m])");
    assert!(result.is_err(), "expecting deadline error");
    assert!(
        result.unwrap_err().message().contains("deadline"),
        "expecting a deadline error message"
    );
}

#[test]
fn search_query_time_range() {
    // Verify the fetch range: [start - max(window, step) - silence, end]
    // for default_rollup and [start - max(window, step), end] for
    // max_over_time.
    struct RangeCheck {
        expected_start: i64,
        inner: FakeProvider,
    }
    impl MetricsProvider for RangeCheck {
        fn search(&self, sq: &SearchQuery, deadline: Deadline) -> esm_promql::Result<Vec<Series>> {
            assert_eq!(sq.start, self.expected_start, "unexpected search start");
            assert_eq!(sq.end, END);
            self.inner.search(sq, deadline)
        }
    }
    // default_rollup: silence interval 5m + step 200s lookbehind.
    let p = RangeCheck {
        expected_start: START - 300_000 - STEP,
        inner: tsbs_provider(1),
    };
    run(&p, "cpu_usage_user");
    // max_over_time[10m]: no silence interval; window 600s > step.
    let p = RangeCheck {
        expected_start: START - 600_000,
        inner: tsbs_provider(1),
    };
    run(&p, "max_over_time(cpu_usage_user[10m])");
}

// --- Worker pool + rollup result cache ------------------------------------

/// Provider wrapper counting `search` calls (to observe cache hits).
struct CountingProvider {
    inner: FakeProvider,
    searches: std::sync::atomic::AtomicUsize,
    last_start: std::sync::atomic::AtomicI64,
}

impl CountingProvider {
    fn new(inner: FakeProvider) -> Self {
        CountingProvider {
            inner,
            searches: std::sync::atomic::AtomicUsize::new(0),
            last_start: std::sync::atomic::AtomicI64::new(0),
        }
    }

    fn search_count(&self) -> usize {
        self.searches.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn last_search_start(&self) -> i64 {
        self.last_start.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl MetricsProvider for CountingProvider {
    fn search(&self, sq: &SearchQuery, deadline: Deadline) -> esm_promql::Result<Vec<Series>> {
        self.searches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.last_start
            .store(sq.start, std::sync::atomic::Ordering::SeqCst);
        self.inner.search(sq, deadline)
    }
}

#[track_caller]
fn assert_results_equal(got: &[QueryResult], want: &[QueryResult], ctx: &str) {
    assert_eq!(got.len(), want.len(), "{ctx}: result count mismatch");
    for (g, w) in got.iter().zip(want) {
        assert_eq!(
            g.metric_name.metric_group, w.metric_name.metric_group,
            "{ctx}"
        );
        assert_eq!(g.timestamps.as_slice(), w.timestamps.as_slice(), "{ctx}");
        assert_values(&g.values, &w.values, ctx);
    }
}

#[test]
fn pool_worker_split_equivalence() {
    // The persistent-pool fan-out must produce results equivalent to the
    // single-threaded reference for every worker split, including the
    // incremental-aggregation path where per-worker states are merged.
    let p = tsbs_provider(13);
    let queries = [
        "max(max_over_time(cpu_usage_user[1m])) by (__name__)",
        "avg(avg_over_time(cpu_usage_user[1m])) by (hostname)",
        "sum(rate(cpu_usage_user[1m]))",
        "count(cpu_usage_user)",
        "max_over_time(cpu_usage_user[1m])",
    ];
    for q in queries {
        let mut ec = eval_config();
        ec.max_workers = 1;
        let reference = exec(&p, &ec, q).unwrap();
        for workers in [2usize, 3, 5, 13, 32] {
            let mut ec = eval_config();
            ec.max_workers = workers;
            let got = exec(&p, &ec, q).unwrap();
            assert_results_equal(&got, &reference, &format!("{q} with {workers} workers"));
        }
    }
}

#[test]
fn rollup_cache_full_hit_serves_from_cache() {
    // A repeated identical query is served entirely from the cache without
    // touching the storage provider (Go: rollupResultCacheFullHits).
    let p = CountingProvider::new(tsbs_provider(2));
    let mut ec = eval_config();
    ec.may_cache = true;
    let q = r#"max_over_time(cpu_usage_user{hostname="h0"}[1m])"#;
    let r1 = exec(&p, &ec, q).unwrap();
    let n1 = p.search_count();
    assert!(n1 >= 1);
    assert_eq!(p.last_search_start(), START - STEP);
    let r2 = exec(&p, &ec, q).unwrap();
    assert_eq!(p.search_count(), n1, "a full cache hit must not search");
    assert_results_equal(&r2, &r1, "cached vs fresh");
}

#[test]
fn rollup_cache_partial_hit_merges_tail() {
    let p = CountingProvider::new(tsbs_provider(2));
    let q = r#"avg_over_time(cpu_usage_user{hostname="h1"}[1m])"#;

    // Populate the cache for the shorter range [START .. END - 2*STEP].
    let mut ec_short = eval_config();
    ec_short.end = END - 2 * STEP;
    ec_short.may_cache = true;
    exec(&p, &ec_short, q).unwrap();
    let n1 = p.search_count();

    // The longer range must evaluate only the tail and merge.
    let mut ec_full = eval_config();
    ec_full.may_cache = true;
    let merged = exec(&p, &ec_full, q).unwrap();
    assert!(
        p.search_count() > n1,
        "the tail past the cached range must still be evaluated"
    );

    // The merged result must match a fully uncached evaluation.
    let p2 = CountingProvider::new(tsbs_provider(2));
    let mut ec_nocache = eval_config();
    ec_nocache.may_cache = false;
    let fresh = exec(&p2, &ec_nocache, q).unwrap();
    assert_results_equal(&merged, &fresh, "partial-merge vs full eval");
}

#[test]
fn rollup_cache_incremental_aggregate_path() {
    // The iafc fast path caches the final aggregated series keyed by the
    // whole aggregate expression.
    let p = CountingProvider::new(tsbs_provider(3));
    let mut ec = eval_config();
    ec.may_cache = true;
    let q = "sum(sum_over_time(cpu_usage_user[1m])) by (__name__)";
    let r1 = exec(&p, &ec, q).unwrap();
    let n1 = p.search_count();
    let r2 = exec(&p, &ec, q).unwrap();
    assert_eq!(
        p.search_count(),
        n1,
        "the cached aggregate must be served without searching"
    );
    assert_results_equal(&r2, &r1, "cached aggregate");
}

#[test]
fn rollup_cache_respects_nocache_flag() {
    let p = CountingProvider::new(tsbs_provider(1));
    let mut ec = eval_config();
    ec.may_cache = false;
    let q = r#"min_over_time(cpu_usage_user{hostname="h0"}[1m])"#;
    let r1 = exec(&p, &ec, q).unwrap();
    let n1 = p.search_count();
    let r2 = exec(&p, &ec, q).unwrap();
    assert_eq!(
        p.search_count(),
        2 * n1,
        "nocache queries must never be served from the cache"
    );
    assert_results_equal(&r2, &r1, "nocache repeat");
}

#[test]
fn rollup_cache_skips_unaligned_ranges() {
    // may_cache requires start/end alignment on the step grid.
    let p = CountingProvider::new(tsbs_provider(1));
    let mut ec = eval_config();
    ec.start += 1;
    ec.end += 1;
    ec.may_cache = true;
    let q = r#"sum_over_time(cpu_usage_user{hostname="h0"}[1m])"#;
    let r1 = exec(&p, &ec, q).unwrap();
    let n1 = p.search_count();
    let r2 = exec(&p, &ec, q).unwrap();
    assert_eq!(
        p.search_count(),
        2 * n1,
        "unaligned ranges must not be cached"
    );
    assert_results_equal(&r2, &r1, "unaligned repeat");
}
