//! Unit tests for [`super`] (`opentelemetry::convert`), split into their own
//! file to keep `convert.rs` under the project's 800-line-per-file
//! guideline. Wired up via `#[path]` from `convert.rs`, so this is still the
//! ordinary `convert::tests` child module with access to the parent's
//! private items (`convert_request`, `push_attribute_label`,
//! `format_float_go_g`, `format_vmrange`, ...) — no visibility changes were
//! needed.

use super::*;

// --- Go 'g'-format port: side-by-side vectors against real go1.26
// `strconv.FormatFloat(v, 'g', -1, 64)` output. ---

#[test]
// This is a literal Go-verified test vector (`3.14159265358979`, one
// digit short of full `f64` precision), not an attempt to name a
// mathematical constant — `clippy::approx_constant` doesn't apply.
#[allow(clippy::approx_constant)]
fn format_float_go_g_matches_go_vectors() {
    let cases: &[(f64, &str)] = &[
        (123456789012345.0, "1.23456789012345e+14"),
        (1e-7, "1e-07"),
        (1e21, "1e+21"),
        (0.1, "0.1"),
        (100.0, "100"),
        (1.5e300, "1.5e+300"),
        (5e-324, "5e-324"),
        (0.0, "0"),
        (1e20, "1e+20"),
        (-1e-7, "-1e-07"),
        (0.0001, "0.0001"),
        (0.00001, "1e-05"),
        (3.14159265358979, "3.14159265358979"),
        (-123.456, "-123.456"),
        (1e-300, "1e-300"),
        (1e300, "1e+300"),
        (20.0, "20"),
        (2e21, "2e+21"),
        (1.9999999999999998e20, "1.9999999999999997e+20"),
        (100000.0, "100000"),
        (1000000.0, "1e+06"),
        (123456.0, "123456"),
        (1234567.0, "1.234567e+06"),
        (999999.0, "999999"),
        (12345.6789, "12345.6789"),
        (999999999999999999.0, "1e+18"),
        (42.0, "42"),
        (-42.0, "-42"),
        (0.5, "0.5"),
        (-0.5, "-0.5"),
        (7.0, "7"),
        (0.0078125, "0.0078125"),
        (3.0, "3"),
        (1234.5, "1234.5"),
    ];
    for &(v, want) in cases {
        assert_eq!(format_float_go_g(v), want, "v={v:e}");
    }
}

#[test]
fn format_float_go_g_handles_nan_and_infinities() {
    assert_eq!(format_float_go_g(f64::NAN), "NaN");
    assert_eq!(format_float_go_g(f64::INFINITY), "+Inf");
    assert_eq!(format_float_go_g(f64::NEG_INFINITY), "-Inf");
}

// --- vmrange (fixed 'e',3 format) port: side-by-side against real
// go1.26 `strconv.AppendFloat(_, v, 'e', 3, 64)` output. ---

#[test]
fn format_vmrange_matches_go_vectors() {
    assert_eq!(format_vmrange(-0.001, 0.001), "-1.000e-03...1.000e-03");
    assert_eq!(format_vmrange(1.0, 2.0), "1.000e+00...2.000e+00");
    assert_eq!(format_vmrange(100.0, 200.0), "1.000e+02...2.000e+02");
    assert_eq!(format_vmrange(-200.0, -100.0), "-2.000e+02...-1.000e+02");
    assert_eq!(
        format_vmrange(0.0009765625, 0.001953125),
        "9.766e-04...1.953e-03"
    );
    assert_eq!(format_vmrange(1e-300, 2e-300), "1.000e-300...2.000e-300");
    assert_eq!(format_vmrange(1.5e10, 3e10), "1.500e+10...3.000e+10");
    // Runtime negative zero (not constant-folded) keeps its sign, like Go.
    let zero_threshold = 0.0_f64;
    assert_eq!(
        format_vmrange(-zero_threshold, zero_threshold),
        "-0.000e+00...0.000e+00"
    );
}

// --- JSON rendering for array/kvlist-nested attribute values. ---

#[test]
fn any_value_to_json_renders_array_of_scalars() {
    let arr = pb::AnyValue::Array(pb::ArrayValue {
        values: vec![
            pb::AnyValue::Int(1),
            pb::AnyValue::String("two".to_string()),
            pb::AnyValue::Bool(true),
        ],
    });
    assert_eq!(any_value_to_json(&arr), r#"[1,"two",true]"#);
}

#[test]
fn any_value_to_json_renders_kvlist_nested_in_array_as_object() {
    let arr = pb::AnyValue::Array(pb::ArrayValue {
        values: vec![pb::AnyValue::KeyValueList(pb::KeyValueList {
            values: vec![pb::KeyValue {
                key: "a".to_string(),
                value: pb::AnyValue::Double(123456789012345.0),
            }],
        })],
    });
    // The nested double uses 'g' format (scientific, exp=14 >= 6),
    // not the 'f' format used for top-level scalar attribute values.
    assert_eq!(any_value_to_json(&arr), r#"[{"a":1.23456789012345e+14}]"#);
}

// --- Attribute flattening: top-level KeyValueList dotted-prefix labels
// vs. array JSON-encoding. ---

#[test]
fn push_attribute_label_flattens_top_level_kvlist_recursively() {
    let mut labels = Vec::new();
    let value = pb::AnyValue::KeyValueList(pb::KeyValueList {
        values: vec![
            pb::KeyValue {
                key: "a".to_string(),
                value: pb::AnyValue::String("1".to_string()),
            },
            pb::KeyValue {
                key: "b".to_string(),
                value: pb::AnyValue::KeyValueList(pb::KeyValueList {
                    values: vec![pb::KeyValue {
                        key: "c".to_string(),
                        value: pb::AnyValue::String("2".to_string()),
                    }],
                }),
            },
        ],
    });
    push_attribute_label(&mut labels, "outer", &value);
    assert_eq!(
        labels,
        vec![
            ("outer.a".to_string(), "1".to_string()),
            ("outer.b.c".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn push_attribute_label_skips_present_but_empty_any_value() {
    // Go: `decodeAnyValue` over a present-but-empty AnyValue message never
    // reaches `ls.Add`, so no label is emitted. `AnyValue::Unset` is the
    // port's decoded form of that empty message — it must add nothing.
    let mut labels = Vec::new();
    push_attribute_label(&mut labels, "empty", &pb::AnyValue::Unset);
    assert!(labels.is_empty());
}

#[test]
fn push_attribute_label_skips_empty_any_value_nested_in_kvlist() {
    // A kvlist entry whose value is an empty AnyValue must likewise drop the
    // label, matching `decodeKeyValueList` -> `decodeKeyValue` ->
    // `decodeAnyValue` over empty bytes.
    let mut labels = Vec::new();
    let value = pb::AnyValue::KeyValueList(pb::KeyValueList {
        values: vec![
            pb::KeyValue {
                key: "a".to_string(),
                value: pb::AnyValue::String("1".to_string()),
            },
            pb::KeyValue {
                key: "b".to_string(),
                value: pb::AnyValue::Unset,
            },
        ],
    });
    push_attribute_label(&mut labels, "outer", &value);
    assert_eq!(labels, vec![("outer.a".to_string(), "1".to_string())]);
}

#[test]
fn push_attribute_label_json_encodes_array_as_single_label() {
    let mut labels = Vec::new();
    let value = pb::AnyValue::Array(pb::ArrayValue {
        values: vec![pb::AnyValue::Int(1), pb::AnyValue::Int(2)],
    });
    push_attribute_label(&mut labels, "arr", &value);
    assert_eq!(labels, vec![("arr".to_string(), "[1,2]".to_string())]);
}

// --- Golden conversion cases, ported from
// lib/protoparser/opentelemetry/pb/pb_test.go's TestDecodeScopeMetrics
// and stream/streamparser_test.go's TestParseStream generators. ---

fn string_kv(k: &str, v: &str) -> pb::KeyValue {
    pb::KeyValue {
        key: k.to_string(),
        value: pb::AnyValue::String(v.to_string()),
    }
}

fn labels_map(row: &Row) -> std::collections::BTreeMap<String, String> {
    row.tags.iter().cloned().collect()
}

/// Go: `pb_test.go`'s `TestDecodeScopeMetrics`, first `f(...)` case
/// (`DisableScopeMetadata: false, DisableResourceAttributes: false`) —
/// exactly this port's fixed defaults.
#[test]
fn resource_and_scope_metadata_are_promoted_to_labels() {
    let req = pb::ExportMetricsServiceRequest {
        resource_metrics: vec![pb::ResourceMetrics {
            resource: Some(pb::Resource {
                attributes: vec![string_kv("job", "vm"), string_kv("region", "us-east-1")],
            }),
            scope_metrics: vec![pb::ScopeMetrics {
                scope: Some(pb::InstrumentationScope {
                    name: Some("my-scope".to_string()),
                    version: Some("v1.0".to_string()),
                    attributes: vec![string_kv("env", "prod")],
                }),
                metrics: vec![pb::Metric {
                    name: "my-gauge".to_string(),
                    description: "a test gauge".to_string(),
                    gauge: Some(pb::Gauge {
                        data_points: vec![pb::NumberDataPoint {
                            attributes: vec![string_kv("label1", "value1")],
                            int_value: Some(1),
                            time_unix_nano: 1000,
                            ..Default::default()
                        }],
                    }),
                    ..Default::default()
                }],
            }],
        }],
    };
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric, "my-gauge");
    assert_eq!(
        labels_map(&rows[0]),
        [
            ("job", "vm"),
            ("region", "us-east-1"),
            ("scope.name", "my-scope"),
            ("scope.version", "v1.0"),
            ("scope.attributes.env", "prod"),
            ("label1", "value1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    );
    assert_eq!(rows[0].value, 1.0);
    assert_eq!(rows[0].timestamp, 0); // 1000ns / 1e6 truncates to 0ms.
}

/// Go: `streamparser_test.go`'s `generateGauge` + expectation
/// `newTimeSeries("my-gauge", 15000, 15.0, ...)`.
#[test]
fn gauge_data_point_converts_to_plain_sample() {
    let req = single_metric_request(pb::Metric {
        name: "my-gauge".to_string(),
        gauge: Some(pb::Gauge {
            data_points: vec![pb::NumberDataPoint {
                attributes: vec![string_kv("label1", "value1")],
                int_value: Some(15),
                time_unix_nano: 15_000_000_000,
                ..Default::default()
            }],
        }),
        ..Default::default()
    });
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric, "my-gauge");
    assert_eq!(rows[0].timestamp, 15000);
    assert_eq!(rows[0].value, 15.0);
}

/// Go: `streamparser_test.go`'s `generateSum(..., isMonotonic: false)` +
/// expectation `newTimeSeries("my-sum", 150000, 15.5, ...)`.
#[test]
fn sum_data_point_converts_to_plain_sample_no_suffix() {
    let req = single_metric_request(pb::Metric {
        name: "my-sum".to_string(),
        sum: Some(pb::Sum {
            data_points: vec![pb::NumberDataPoint {
                attributes: vec![string_kv("label5", "value5")],
                double_value: Some(15.5),
                time_unix_nano: 150_000_000_000,
                ..Default::default()
            }],
            is_monotonic: false,
        }),
        ..Default::default()
    });
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metric, "my-sum");
    assert_eq!(rows[0].timestamp, 150000);
    assert_eq!(rows[0].value, 15.5);
}

/// Go: `streamparser_test.go`'s `generateHistogram(..., hasSum: true)` +
/// its expected `_count`/`_sum`/`_bucket` (cumulative, `+Inf`-terminated)
/// time series.
#[test]
fn histogram_data_point_cumulates_buckets_and_has_inf_bucket() {
    let req = single_metric_request(pb::Metric {
        name: "my-histogram".to_string(),
        histogram: Some(pb::Histogram {
            data_points: vec![pb::HistogramDataPoint {
                attributes: vec![string_kv("label2", "value2")],
                time_unix_nano: 30_000_000_000,
                count: 15,
                sum: Some(30.0),
                explicit_bounds: vec![0.1, 0.5, 1.0, 5.0],
                bucket_counts: vec![0, 5, 10, 0, 0],
                flags: 0,
            }],
        }),
        ..Default::default()
    });
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);

    let by_metric = |m: &str| -> Vec<&Row> { rows.iter().filter(|r| r.metric == m).collect() };

    let count = by_metric("my-histogram_count");
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].value, 15.0);
    assert_eq!(count[0].timestamp, 30000);

    let sum = by_metric("my-histogram_sum");
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].value, 30.0);

    let buckets = by_metric("my-histogram_bucket");
    assert_eq!(buckets.len(), 5);
    let bucket_le = |le: &str| -> f64 {
        buckets
            .iter()
            .find(|r| r.tags.iter().any(|(k, v)| k == "le" && v == le))
            .unwrap_or_else(|| panic!("no bucket with le={le}"))
            .value
    };
    assert_eq!(bucket_le("0.1"), 0.0);
    assert_eq!(bucket_le("0.5"), 5.0);
    assert_eq!(bucket_le("1"), 15.0);
    assert_eq!(bucket_le("5"), 15.0);
    assert_eq!(bucket_le("+Inf"), 15.0);
}

/// Go: `streamparser_test.go`'s `generateHistogram(..., hasSum: false)` —
/// `_sum` must be entirely absent, not a zero-valued row.
#[test]
fn sumless_histogram_omits_sum_row() {
    let req = single_metric_request(pb::Metric {
        name: "my-sumless-histogram".to_string(),
        histogram: Some(pb::Histogram {
            data_points: vec![pb::HistogramDataPoint {
                time_unix_nano: 30_000_000_000,
                count: 15,
                sum: None,
                explicit_bounds: vec![0.1, 0.5, 1.0, 5.0],
                bucket_counts: vec![0, 5, 10, 0, 0],
                ..Default::default()
            }],
        }),
        ..Default::default()
    });
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);
    assert!(!rows.iter().any(|r| r.metric == "my-sumless-histogram_sum"));
    assert!(rows
        .iter()
        .any(|r| r.metric == "my-sumless-histogram_count"));
}

/// Go: `streamparser_test.go`'s `generateSummary` + its expected
/// `_sum`/`_count`/quantile-labeled time series.
#[test]
fn summary_data_point_emits_count_sum_and_quantile_rows() {
    let req = single_metric_request(pb::Metric {
        name: "my-summary".to_string(),
        summary: Some(pb::Summary {
            data_points: vec![pb::SummaryDataPoint {
                attributes: vec![string_kv("label6", "value6")],
                time_unix_nano: 35_000_000_000,
                count: 5,
                sum: 32.5,
                quantile_values: vec![
                    pb::ValueAtQuantile {
                        quantile: 0.1,
                        value: 7.5,
                    },
                    pb::ValueAtQuantile {
                        quantile: 0.5,
                        value: 10.0,
                    },
                    pb::ValueAtQuantile {
                        quantile: 1.0,
                        value: 15.0,
                    },
                ],
                flags: 0,
            }],
        }),
        ..Default::default()
    });
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);

    let count: Vec<_> = rows
        .iter()
        .filter(|r| r.metric == "my-summary_count")
        .collect();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].value, 5.0);

    let sum: Vec<_> = rows
        .iter()
        .filter(|r| r.metric == "my-summary_sum")
        .collect();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].value, 32.5);

    let quantiles: Vec<_> = rows.iter().filter(|r| r.metric == "my-summary").collect();
    assert_eq!(quantiles.len(), 3);
    let quantile_value = |q: &str| -> f64 {
        quantiles
            .iter()
            .find(|r| r.tags.iter().any(|(k, v)| k == "quantile" && v == q))
            .unwrap_or_else(|| panic!("no quantile row for {q}"))
            .value
    };
    assert_eq!(quantile_value("0.1"), 7.5);
    assert_eq!(quantile_value("0.5"), 10.0);
    assert_eq!(quantile_value("1"), 15.0);
}

/// Go: `streamparser_test.go`'s `generateExpHistogram`. Verifies the
/// zero bucket, positive-side and negative-side `vmrange` cumulative
/// bucket expansion, and that zero-count buckets are skipped.
#[test]
fn exponential_histogram_emits_vmrange_buckets() {
    let req = single_metric_request(pb::Metric {
        name: "my-exp-histogram".to_string(),
        exponential_histogram: Some(pb::ExponentialHistogram {
            data_points: vec![pb::ExponentialHistogramDataPoint {
                attributes: vec![string_kv("label1", "value1")],
                time_unix_nano: 15_000_000_000,
                count: 31,
                sum: Some(588.0),
                scale: 0,
                zero_count: 0,
                positive: Some(pb::Buckets {
                    offset: 2,
                    bucket_counts: vec![1, 2, 3, 4, 5, 0, 0, 1],
                }),
                negative: Some(pb::Buckets {
                    offset: 2,
                    bucket_counts: vec![1, 2, 3, 4, 5],
                }),
                ..Default::default()
            }],
        }),
        ..Default::default()
    });
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);

    assert_eq!(
        rows.iter()
            .filter(|r| r.metric == "my-exp-histogram_count")
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r.metric == "my-exp-histogram_sum")
            .count(),
        1
    );
    let buckets: Vec<_> = rows
        .iter()
        .filter(|r| r.metric == "my-exp-histogram_bucket")
        .collect();
    // 6 nonzero positive buckets (index 5,6 are zero-count and skipped)
    // + 5 nonzero negative buckets = 11. No zero bucket (zero_count=0).
    assert_eq!(buckets.len(), 11);
    // scale=0 => ratio=1, base=2; positive offset=2 => positiveBound=4.
    // index 0 nonzero bucket: lower=4*2^0=4, upper=8.
    assert!(buckets.iter().any(|r| r
        .tags
        .iter()
        .any(|(k, v)| k == "vmrange" && v == "4.000e+00...8.000e+00")));
}

/// Go: `pb.go`'s `PushSample`/`streamparser.go`'s
/// `if flags&1 != 0 { value = decimal.StaleNaN }` (the OTLP
/// `NoRecordedValue` / staleness marker bit).
#[test]
fn stale_flag_bit_replaces_value_with_stale_nan() {
    let req = single_metric_request(pb::Metric {
        name: "my-gauge".to_string(),
        gauge: Some(pb::Gauge {
            data_points: vec![pb::NumberDataPoint {
                double_value: Some(42.0),
                time_unix_nano: 1_000_000_000,
                flags: 1, // NoRecordedValue bit set.
                ..Default::default()
            }],
        }),
        ..Default::default()
    });
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);
    assert_eq!(rows.len(), 1);
    assert!(esm_common::decimal::is_stale_nan(rows[0].value));
}

#[test]
fn missing_scope_submessage_adds_no_scope_labels() {
    let req = pb::ExportMetricsServiceRequest {
        resource_metrics: vec![pb::ResourceMetrics {
            resource: None,
            scope_metrics: vec![pb::ScopeMetrics {
                scope: None, // No Scope submessage at all.
                metrics: vec![pb::Metric {
                    name: "m".to_string(),
                    gauge: Some(pb::Gauge {
                        data_points: vec![pb::NumberDataPoint {
                            double_value: Some(1.0),
                            ..Default::default()
                        }],
                    }),
                    ..Default::default()
                }],
            }],
        }],
    };
    let mut rows = Vec::new();
    convert_request(&req, &mut rows);
    assert_eq!(rows.len(), 1);
    assert!(rows[0].tags.is_empty());
}

/// Helper: wraps a single metric in a minimal resource/scope tree with
/// the same `job=vm` / `scope{name:foo,version:bar}` shape as
/// `streamparser_test.go`'s `generateOTLPSamples`, but without its
/// `scope.attributes.abc=qwe` (kept minimal; the scope-label promotion
/// itself is covered by `resource_and_scope_metadata_are_promoted_to_labels`).
fn single_metric_request(metric: pb::Metric) -> pb::ExportMetricsServiceRequest {
    pb::ExportMetricsServiceRequest {
        resource_metrics: vec![pb::ResourceMetrics {
            resource: None,
            scope_metrics: vec![pb::ScopeMetrics {
                scope: None,
                metrics: vec![metric],
            }],
        }],
    }
}
