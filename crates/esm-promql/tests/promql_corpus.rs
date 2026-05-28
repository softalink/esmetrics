//! A small curated PromQL corpus.
//!
//! Inspired by Prometheus's `promqltest` format but simpler: each case is
//! `(setup samples, query, expected results)`. Each result is a series
//! identified by its canonical metric-name bytes and its scalar value at
//! query time.
//!
//! Goal: a regression net that catches semantic drift in the evaluator
//! without needing the full Prometheus corpus parser. Today we cover the
//! shapes most often used in alert rules and Grafana dashboards.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::manual_assert)]

use esm_promql::EvalContext;
use esm_promql::evaluator::{Value, evaluate};
use esm_promql::parser::parse;
use esm_storage::{Sample, Storage};

const NOW_MS: i64 = 1_700_000_000_000;

struct Case {
    name: &'static str,
    samples: &'static [(&'static str, &'static [(i64, i64)])],
    query: &'static str,
    expect: Expect,
}

enum Expect {
    Scalar(f64),
    /// Set of (metric_name_substring, value) pairs. We compare as a set so
    /// element order doesn't matter. Use a substring match against
    /// canonical metric-name bytes to keep tests robust to label-ordering
    /// changes.
    Vector(&'static [(&'static str, f64)]),
}

fn load(storage: &mut Storage, samples: &[(&str, &[(i64, i64)])]) {
    let mut batch = Vec::new();
    for (name, points) in samples {
        for (ts, v) in *points {
            batch.push(Sample {
                metric_name: (*name).as_bytes().to_vec(),
                timestamp_ms: *ts,
                value: *v,
            });
        }
    }
    storage.ingest(&batch).expect("ingest");
    storage.flush().expect("flush");
}

fn run_case(c: &Case) -> Result<(), String> {
    let tmp = tempfile::tempdir().map_err(|e| format!("{}: tmp: {e}", c.name))?;
    let mut storage =
        Storage::open(tmp.path().join("d")).map_err(|e| format!("{}: open: {e}", c.name))?;
    load(&mut storage, c.samples);
    let expr = parse(c.query).map_err(|e| format!("{}: parse: {e}", c.name))?;
    let value = evaluate(&expr, &storage, EvalContext::instant(NOW_MS))
        .map_err(|e| format!("{}: eval: {e}", c.name))?;
    match (&c.expect, value) {
        (Expect::Scalar(want), Value::Scalar(got)) => {
            if (got - want).abs() > 1e-9 * want.abs().max(1.0) {
                return Err(format!("{}: scalar mismatch: want {want}, got {got}", c.name));
            }
        }
        (Expect::Vector(want), Value::InstantVector(elems)) => {
            let got: std::collections::BTreeSet<(String, i64)> = elems
                .iter()
                .map(|e| {
                    (
                        String::from_utf8_lossy(&e.metric_name).to_string(),
                        // Quantize to i64 to dodge f64 round-trip noise.
                        e.value.round() as i64,
                    )
                })
                .collect();
            for (substr, want_val) in *want {
                let want_int = want_val.round() as i64;
                let found =
                    got.iter().any(|(name, val)| name.contains(*substr) && *val == want_int);
                if !found {
                    return Err(format!(
                        "{}: expected ({substr}, {want_val}) not in {got:?}",
                        c.name
                    ));
                }
            }
            if got.len() != want.len() {
                return Err(format!(
                    "{}: cardinality mismatch: want {} elements, got {}: {got:?}",
                    c.name,
                    want.len(),
                    got.len()
                ));
            }
        }
        (Expect::Scalar(_), Value::InstantVector(v)) => {
            return Err(format!("{}: expected scalar, got vector: {v:?}", c.name));
        }
        (Expect::Vector(_), Value::Scalar(s)) => {
            return Err(format!("{}: expected vector, got scalar: {s}", c.name));
        }
    }
    Ok(())
}

#[test]
fn corpus() {
    // All cases share the same "now" timestamp of NOW_MS so samples are
    // placed in the recent lookback window (5 min default).
    let cases = [
        Case {
            name: "instant_selector",
            samples: &[("up", &[(NOW_MS - 1000, 1)])],
            query: "up",
            expect: Expect::Vector(&[("up", 1.0)]),
        },
        Case { name: "scalar_literal", samples: &[], query: "42", expect: Expect::Scalar(42.0) },
        Case {
            name: "arithmetic_scalar",
            samples: &[],
            query: "2 + 3 * 4",
            expect: Expect::Scalar(14.0),
        },
        Case {
            name: "sum_aggregation",
            samples: &[
                ("requests{job=\"a\"}", &[(NOW_MS - 1000, 3)]),
                ("requests{job=\"b\"}", &[(NOW_MS - 1000, 5)]),
            ],
            query: "sum(requests)",
            expect: Expect::Vector(&[("", 8.0)]),
        },
        Case {
            name: "sum_by_label",
            samples: &[
                ("requests{job=\"a\",code=\"200\"}", &[(NOW_MS - 1000, 3)]),
                ("requests{job=\"a\",code=\"500\"}", &[(NOW_MS - 1000, 1)]),
                ("requests{job=\"b\",code=\"200\"}", &[(NOW_MS - 1000, 7)]),
            ],
            query: "sum by (job) (requests)",
            expect: Expect::Vector(&[("job=\"a\"", 4.0), ("job=\"b\"", 7.0)]),
        },
        Case {
            name: "count_simple",
            samples: &[
                ("up{job=\"a\"}", &[(NOW_MS - 1000, 1)]),
                ("up{job=\"b\"}", &[(NOW_MS - 1000, 1)]),
                ("up{job=\"c\"}", &[(NOW_MS - 1000, 0)]),
            ],
            query: "count(up)",
            expect: Expect::Vector(&[("", 3.0)]),
        },
        Case {
            name: "topk_2",
            samples: &[
                ("m{i=\"1\"}", &[(NOW_MS - 1000, 10)]),
                ("m{i=\"2\"}", &[(NOW_MS - 1000, 20)]),
                ("m{i=\"3\"}", &[(NOW_MS - 1000, 5)]),
            ],
            query: "topk(2, m)",
            expect: Expect::Vector(&[("i=\"1\"", 10.0), ("i=\"2\"", 20.0)]),
        },
        Case {
            name: "abs_negative",
            samples: &[("temp", &[(NOW_MS - 1000, -7)])],
            query: "abs(temp)",
            expect: Expect::Vector(&[("temp", 7.0)]),
        },
        Case {
            name: "vector_constant",
            samples: &[],
            query: "vector(5)",
            expect: Expect::Vector(&[("", 5.0)]),
        },
        Case {
            name: "binary_comparison_bool",
            samples: &[("v", &[(NOW_MS - 1000, 10)])],
            query: "v > bool 5",
            expect: Expect::Vector(&[("v", 1.0)]),
        },
        Case {
            name: "binary_filter_drops_nonmatching",
            samples: &[
                ("v{i=\"1\"}", &[(NOW_MS - 1000, 10)]),
                ("v{i=\"2\"}", &[(NOW_MS - 1000, 1)]),
            ],
            query: "v > 5",
            expect: Expect::Vector(&[("i=\"1\"", 10.0)]),
        },
        Case {
            name: "min_aggregation",
            samples: &[
                ("v{i=\"1\"}", &[(NOW_MS - 1000, 7)]),
                ("v{i=\"2\"}", &[(NOW_MS - 1000, 3)]),
                ("v{i=\"3\"}", &[(NOW_MS - 1000, 5)]),
            ],
            query: "min(v)",
            expect: Expect::Vector(&[("", 3.0)]),
        },
        Case {
            name: "max_aggregation",
            samples: &[
                ("v{i=\"1\"}", &[(NOW_MS - 1000, 7)]),
                ("v{i=\"2\"}", &[(NOW_MS - 1000, 3)]),
            ],
            query: "max(v)",
            expect: Expect::Vector(&[("", 7.0)]),
        },
        Case {
            name: "avg_aggregation",
            samples: &[
                ("v{i=\"1\"}", &[(NOW_MS - 1000, 10)]),
                ("v{i=\"2\"}", &[(NOW_MS - 1000, 20)]),
            ],
            query: "avg(v)",
            expect: Expect::Vector(&[("", 15.0)]),
        },
        Case {
            name: "label_replace_replaces",
            samples: &[("svc{name=\"foo\"}", &[(NOW_MS - 1000, 1)])],
            query: r#"label_replace(svc, "tier", "bar", "name", "foo")"#,
            expect: Expect::Vector(&[("tier=\"bar\"", 1.0)]),
        },
        Case {
            name: "histogram_sum_classical",
            samples: &[
                ("http_request_duration_sum{job=\"api\"}", &[(NOW_MS - 1000, 42)]),
                ("http_request_duration_count{job=\"api\"}", &[(NOW_MS - 1000, 7)]),
                ("http_request_duration_bucket{job=\"api\",le=\"+Inf\"}", &[(NOW_MS - 1000, 7)]),
            ],
            query: r#"histogram_sum(http_request_duration_bucket{job="api",le="+Inf"})"#,
            expect: Expect::Vector(&[("http_request_duration_sum", 42.0)]),
        },
        Case {
            name: "histogram_count_classical",
            samples: &[
                ("http_request_duration_count{job=\"api\"}", &[(NOW_MS - 1000, 7)]),
                ("http_request_duration_bucket{job=\"api\",le=\"+Inf\"}", &[(NOW_MS - 1000, 7)]),
            ],
            query: r#"histogram_count(http_request_duration_bucket{job="api",le="+Inf"})"#,
            expect: Expect::Vector(&[("http_request_duration_count", 7.0)]),
        },
        Case {
            name: "histogram_avg_classical",
            samples: &[
                ("http_request_duration_sum{job=\"api\"}", &[(NOW_MS - 1000, 42)]),
                ("http_request_duration_count{job=\"api\"}", &[(NOW_MS - 1000, 7)]),
                ("http_request_duration_bucket{job=\"api\",le=\"+Inf\"}", &[(NOW_MS - 1000, 7)]),
            ],
            query: r#"histogram_avg(http_request_duration_bucket{job="api",le="+Inf"})"#,
            expect: Expect::Vector(&[("http_request_duration_sum", 6.0)]), // 42/7=6
        },
    ];

    let mut failures = Vec::new();
    for case in &cases {
        if let Err(e) = run_case(case) {
            failures.push(e);
        }
    }
    if !failures.is_empty() {
        panic!(
            "{} of {} corpus cases failed:\n  {}",
            failures.len(),
            cases.len(),
            failures.join("\n  ")
        );
    }
}
