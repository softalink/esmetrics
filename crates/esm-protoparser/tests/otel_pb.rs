//! Integration tests for `esm_protoparser::opentelemetry::pb`, the
//! decode-only port of upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/opentelemetry/pb/pb.go`.
//!
//! Payloads are built with a tiny protobuf wire-writer (no protobuf
//! dependency), following the same pattern as `src/prompb.rs`'s inline
//! tests, extended with fixed64/sfixed64/sint32/packed-repeated writers for
//! the OTLP-specific wire types.

use esm_protoparser::opentelemetry::pb::{
    AnyValue, ArrayValue, Buckets, ExportMetricsServiceRequest, HistogramDataPoint, KeyValue,
    KeyValueList, Metric, NumberDataPoint, Resource, ScopeMetrics, ValueAtQuantile, WireError,
};

// --- tiny protobuf wire-writer test helpers (no protobuf dependency) ---

fn append_varint(dst: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            dst.push(byte);
            break;
        }
        dst.push(byte | 0x80);
    }
}

fn append_tag(dst: &mut Vec<u8>, field_num: u32, wire_type: u8) {
    append_varint(dst, (u64::from(field_num) << 3) | u64::from(wire_type));
}

/// Wire type 2: length-delimited (bytes, string, embedded message).
fn append_bytes_field(dst: &mut Vec<u8>, field_num: u32, data: &[u8]) {
    append_tag(dst, field_num, 2);
    append_varint(dst, data.len() as u64);
    dst.extend_from_slice(data);
}

/// Wire type 0: varint (used directly for `uint32`/`int64`/`bool` fields;
/// `sint32` fields go through [`append_sint32_field`] instead).
fn append_varint_field(dst: &mut Vec<u8>, field_num: u32, v: u64) {
    append_tag(dst, field_num, 0);
    append_varint(dst, v);
}

fn append_bool_field(dst: &mut Vec<u8>, field_num: u32, v: bool) {
    append_varint_field(dst, field_num, u64::from(v));
}

/// Wire type 0, zigzag-encoded: `sint32`.
fn append_sint32_field(dst: &mut Vec<u8>, field_num: u32, v: i32) {
    let zigzag = ((v << 1) ^ (v >> 31)) as u32;
    append_varint_field(dst, field_num, u64::from(zigzag));
}

/// Wire type 1: `fixed64`/`sfixed64`/`double`.
fn append_fixed64_field(dst: &mut Vec<u8>, field_num: u32, bits: u64) {
    append_tag(dst, field_num, 1);
    dst.extend_from_slice(&bits.to_le_bytes());
}

fn append_double_field(dst: &mut Vec<u8>, field_num: u32, v: f64) {
    append_fixed64_field(dst, field_num, v.to_bits());
}

fn append_sfixed64_field(dst: &mut Vec<u8>, field_num: u32, v: i64) {
    append_fixed64_field(dst, field_num, v as u64);
}

/// Packed `repeated fixed64`/`repeated double`: one length-delimited field
/// containing the concatenated 8-byte little-endian values.
fn append_packed_fixed64s_field(dst: &mut Vec<u8>, field_num: u32, values: &[u64]) {
    let mut payload = Vec::with_capacity(values.len() * 8);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    append_bytes_field(dst, field_num, &payload);
}

fn append_packed_doubles_field(dst: &mut Vec<u8>, field_num: u32, values: &[f64]) {
    append_packed_fixed64s_field(
        dst,
        field_num,
        &values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
    );
}

/// Packed `repeated uint64`: one length-delimited field containing the
/// concatenated varints.
fn append_packed_uint64s_field(dst: &mut Vec<u8>, field_num: u32, values: &[u64]) {
    let mut payload = Vec::new();
    for v in values {
        append_varint(&mut payload, *v);
    }
    append_bytes_field(dst, field_num, &payload);
}

// --- AnyValue / KeyValue encoders ---

fn encode_any_value_string(s: &str) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, s.as_bytes());
    dst
}

fn encode_any_value_bool(v: bool) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bool_field(&mut dst, 2, v);
    dst
}

fn encode_any_value_int(v: i64) -> Vec<u8> {
    let mut dst = Vec::new();
    append_varint_field(&mut dst, 3, v as u64);
    dst
}

fn encode_any_value_double(v: f64) -> Vec<u8> {
    let mut dst = Vec::new();
    append_double_field(&mut dst, 4, v);
    dst
}

fn encode_any_value_array(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut array_dst = Vec::new();
    for el in elements {
        append_bytes_field(&mut array_dst, 1, el);
    }
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 5, &array_dst);
    dst
}

fn encode_any_value_kvlist(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut kvl_dst = Vec::new();
    for e in entries {
        append_bytes_field(&mut kvl_dst, 1, e);
    }
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 6, &kvl_dst);
    dst
}

fn encode_any_value_bytes(b: &[u8]) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 7, b);
    dst
}

/// Encodes a `KeyValue{key, value}` with `value` present (possibly empty).
fn encode_key_value(key: &str, any_value: &[u8]) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, key.as_bytes());
    append_bytes_field(&mut dst, 2, any_value);
    dst
}

/// Encodes a `KeyValue` with the `value` sub-message field (2) entirely
/// absent from the wire — exercises the upstream drop-the-entry rule.
fn encode_key_value_no_value_field(key: &str) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, key.as_bytes());
    dst
}

// --- data-point-family encoders ---

fn encode_number_data_point_double(attrs: &[Vec<u8>], ts: u64, value: f64, flags: u32) -> Vec<u8> {
    let mut dst = Vec::new();
    for a in attrs {
        append_bytes_field(&mut dst, 7, a);
    }
    append_fixed64_field(&mut dst, 3, ts);
    append_double_field(&mut dst, 4, value);
    append_varint_field(&mut dst, 8, u64::from(flags));
    dst
}

fn encode_number_data_point_int(ts: u64, value: i64, flags: u32) -> Vec<u8> {
    let mut dst = Vec::new();
    append_fixed64_field(&mut dst, 3, ts);
    append_sfixed64_field(&mut dst, 6, value);
    append_varint_field(&mut dst, 8, u64::from(flags));
    dst
}

fn encode_gauge(data_points: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for dp in data_points {
        append_bytes_field(&mut dst, 1, dp);
    }
    dst
}

fn encode_sum(data_points: &[Vec<u8>], is_monotonic: bool) -> Vec<u8> {
    let mut dst = Vec::new();
    for dp in data_points {
        append_bytes_field(&mut dst, 1, dp);
    }
    append_bool_field(&mut dst, 3, is_monotonic);
    dst
}

#[allow(clippy::too_many_arguments)]
fn encode_histogram_data_point(
    attrs: &[Vec<u8>],
    ts: u64,
    count: u64,
    sum: Option<f64>,
    bucket_counts: &[u64],
    explicit_bounds: &[f64],
    flags: u32,
) -> Vec<u8> {
    let mut dst = Vec::new();
    for a in attrs {
        append_bytes_field(&mut dst, 9, a);
    }
    append_fixed64_field(&mut dst, 3, ts);
    append_fixed64_field(&mut dst, 4, count);
    if let Some(sum) = sum {
        append_double_field(&mut dst, 5, sum);
    }
    append_packed_fixed64s_field(&mut dst, 6, bucket_counts);
    append_packed_doubles_field(&mut dst, 7, explicit_bounds);
    append_varint_field(&mut dst, 10, u64::from(flags));
    dst
}

fn encode_histogram(data_points: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for dp in data_points {
        append_bytes_field(&mut dst, 1, dp);
    }
    dst
}

fn encode_buckets(offset: i32, bucket_counts: &[u64]) -> Vec<u8> {
    let mut dst = Vec::new();
    append_sint32_field(&mut dst, 1, offset);
    append_packed_uint64s_field(&mut dst, 2, bucket_counts);
    dst
}

#[allow(clippy::too_many_arguments)]
fn encode_exponential_histogram_data_point(
    ts: u64,
    count: u64,
    sum: Option<f64>,
    scale: i32,
    zero_count: u64,
    positive: Option<&[u8]>,
    negative: Option<&[u8]>,
    flags: u32,
) -> Vec<u8> {
    let mut dst = Vec::new();
    append_fixed64_field(&mut dst, 3, ts);
    append_fixed64_field(&mut dst, 4, count);
    if let Some(sum) = sum {
        append_double_field(&mut dst, 5, sum);
    }
    append_sint32_field(&mut dst, 6, scale);
    append_fixed64_field(&mut dst, 7, zero_count);
    if let Some(p) = positive {
        append_bytes_field(&mut dst, 8, p);
    }
    if let Some(n) = negative {
        append_bytes_field(&mut dst, 9, n);
    }
    append_varint_field(&mut dst, 10, u64::from(flags));
    dst
}

fn encode_exponential_histogram(data_points: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for dp in data_points {
        append_bytes_field(&mut dst, 1, dp);
    }
    dst
}

fn encode_value_at_quantile(quantile: f64, value: f64) -> Vec<u8> {
    let mut dst = Vec::new();
    append_double_field(&mut dst, 1, quantile);
    append_double_field(&mut dst, 2, value);
    dst
}

fn encode_summary_data_point(
    attrs: &[Vec<u8>],
    ts: u64,
    count: u64,
    sum: f64,
    quantiles: &[Vec<u8>],
    flags: u32,
) -> Vec<u8> {
    let mut dst = Vec::new();
    for a in attrs {
        append_bytes_field(&mut dst, 7, a);
    }
    append_fixed64_field(&mut dst, 3, ts);
    append_fixed64_field(&mut dst, 4, count);
    append_double_field(&mut dst, 5, sum);
    for q in quantiles {
        append_bytes_field(&mut dst, 6, q);
    }
    append_varint_field(&mut dst, 8, u64::from(flags));
    dst
}

fn encode_summary(data_points: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for dp in data_points {
        append_bytes_field(&mut dst, 1, dp);
    }
    dst
}

// --- envelope encoders ---

struct MetricDataField {
    field_num: u32,
    bytes: Vec<u8>,
}

fn encode_metric(
    name: &str,
    description: &str,
    unit: &str,
    data: Option<MetricDataField>,
    metadata: &[Vec<u8>],
) -> Vec<u8> {
    let mut dst = Vec::new();
    append_bytes_field(&mut dst, 1, name.as_bytes());
    if !description.is_empty() {
        append_bytes_field(&mut dst, 2, description.as_bytes());
    }
    if !unit.is_empty() {
        append_bytes_field(&mut dst, 3, unit.as_bytes());
    }
    if let Some(d) = data {
        append_bytes_field(&mut dst, d.field_num, &d.bytes);
    }
    for md in metadata {
        append_bytes_field(&mut dst, 12, md);
    }
    dst
}

fn encode_instrumentation_scope(
    name: Option<&str>,
    version: Option<&str>,
    attrs: &[Vec<u8>],
) -> Vec<u8> {
    let mut dst = Vec::new();
    if let Some(name) = name {
        append_bytes_field(&mut dst, 1, name.as_bytes());
    }
    if let Some(version) = version {
        append_bytes_field(&mut dst, 2, version.as_bytes());
    }
    for a in attrs {
        append_bytes_field(&mut dst, 3, a);
    }
    dst
}

fn encode_scope_metrics(scope: Option<&[u8]>, metrics: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    if let Some(scope) = scope {
        append_bytes_field(&mut dst, 1, scope);
    }
    for m in metrics {
        append_bytes_field(&mut dst, 2, m);
    }
    dst
}

fn encode_resource(attrs: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for a in attrs {
        append_bytes_field(&mut dst, 1, a);
    }
    dst
}

fn encode_resource_metrics(resource: Option<&[u8]>, scope_metrics: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    if let Some(resource) = resource {
        append_bytes_field(&mut dst, 1, resource);
    }
    for sm in scope_metrics {
        append_bytes_field(&mut dst, 2, sm);
    }
    dst
}

fn encode_export_request(resource_metrics: &[Vec<u8>]) -> Vec<u8> {
    let mut dst = Vec::new();
    for rm in resource_metrics {
        append_bytes_field(&mut dst, 1, rm);
    }
    dst
}

// =======================================================================
// The task brief's six named tests
// =======================================================================

#[test]
fn gauge_double_datapoint() {
    let dp = encode_number_data_point_double(&[], 1_700_000_000_000_000_000, 42.5, 0);
    let gauge = encode_gauge(&[dp]);
    let metric = encode_metric(
        "cpu.usage",
        "",
        "",
        Some(MetricDataField {
            field_num: 5,
            bytes: gauge,
        }),
        &[],
    );
    let sm = encode_scope_metrics(None, &[metric]);
    let rm = encode_resource_metrics(None, &[sm]);
    let src = encode_export_request(&[rm]);

    let req = ExportMetricsServiceRequest::unmarshal(&src).unwrap();

    assert_eq!(req.resource_metrics.len(), 1);
    let metric = &req.resource_metrics[0].scope_metrics[0].metrics[0];
    assert_eq!(metric.name, "cpu.usage");
    let gauge = metric.gauge.as_ref().expect("gauge must be set");
    assert_eq!(gauge.data_points.len(), 1);
    let dp = &gauge.data_points[0];
    assert_eq!(dp.time_unix_nano, 1_700_000_000_000_000_000);
    assert_eq!(dp.double_value, Some(42.5));
    assert_eq!(dp.int_value, None);
    assert_eq!(dp.flags, 0);
}

#[test]
fn sum_with_attributes() {
    let attr_str = encode_key_value("service", encode_any_value_string("checkout").as_slice());
    let attr_int = encode_key_value("retries", encode_any_value_int(42).as_slice());
    let dp = encode_number_data_point_double(&[attr_str, attr_int], 5_000, 3.0, 0);
    let sum = encode_sum(&[dp], true);
    let metric = encode_metric(
        "requests.total",
        "",
        "",
        Some(MetricDataField {
            field_num: 7,
            bytes: sum,
        }),
        &[],
    );
    let sm = encode_scope_metrics(None, &[metric]);
    let rm = encode_resource_metrics(None, &[sm]);
    let src = encode_export_request(&[rm]);

    let req = ExportMetricsServiceRequest::unmarshal(&src).unwrap();

    let metric = &req.resource_metrics[0].scope_metrics[0].metrics[0];
    let sum = metric.sum.as_ref().expect("sum must be set");
    assert!(sum.is_monotonic);
    let dp = &sum.data_points[0];
    assert_eq!(dp.attributes.len(), 2);
    assert_eq!(dp.attributes[0].key, "service");
    assert_eq!(dp.attributes[0].value.format_string(), "checkout");
    assert_eq!(dp.attributes[1].key, "retries");
    assert_eq!(dp.attributes[1].value, AnyValue::Int(42));
    assert_eq!(dp.attributes[1].value.format_string(), "42");
}

#[test]
fn histogram_buckets_and_bounds() {
    let dp = encode_histogram_data_point(
        &[],
        9_000,
        4,
        Some(10.5),
        &[1, 2, 3, 4],
        &[1.0, 2.0, 3.0],
        0,
    );
    let histogram = encode_histogram(&[dp]);
    let metric = encode_metric(
        "latency",
        "",
        "",
        Some(MetricDataField {
            field_num: 9,
            bytes: histogram,
        }),
        &[],
    );
    let sm = encode_scope_metrics(None, &[metric]);
    let rm = encode_resource_metrics(None, &[sm]);
    let src = encode_export_request(&[rm]);

    let req = ExportMetricsServiceRequest::unmarshal(&src).unwrap();

    let metric = &req.resource_metrics[0].scope_metrics[0].metrics[0];
    let histogram = metric.histogram.as_ref().expect("histogram must be set");
    let dp = &histogram.data_points[0];
    assert_eq!(dp.time_unix_nano, 9_000);
    assert_eq!(dp.count, 4);
    assert_eq!(dp.sum, Some(10.5));
    assert_eq!(dp.bucket_counts, vec![1, 2, 3, 4]);
    assert_eq!(dp.explicit_bounds, vec![1.0, 2.0, 3.0]);
}

#[test]
fn summary_quantiles() {
    let q50 = encode_value_at_quantile(0.5, 12.0);
    let q99 = encode_value_at_quantile(0.99, 42.0);
    let dp = encode_summary_data_point(&[], 7_000, 100, 543.0, &[q50, q99], 0);
    let summary = encode_summary(&[dp]);
    let metric = encode_metric(
        "request.duration",
        "",
        "",
        Some(MetricDataField {
            field_num: 11,
            bytes: summary,
        }),
        &[],
    );
    let sm = encode_scope_metrics(None, &[metric]);
    let rm = encode_resource_metrics(None, &[sm]);
    let src = encode_export_request(&[rm]);

    let req = ExportMetricsServiceRequest::unmarshal(&src).unwrap();

    let metric = &req.resource_metrics[0].scope_metrics[0].metrics[0];
    let summary = metric.summary.as_ref().expect("summary must be set");
    let dp = &summary.data_points[0];
    assert_eq!(dp.count, 100);
    assert_eq!(dp.sum, 543.0);
    assert_eq!(
        dp.quantile_values,
        vec![
            ValueAtQuantile {
                quantile: 0.5,
                value: 12.0
            },
            ValueAtQuantile {
                quantile: 0.99,
                value: 42.0
            },
        ]
    );
}

#[test]
fn resource_attributes_decoded() {
    let attr = encode_key_value(
        "service.name",
        encode_any_value_string("frontend").as_slice(),
    );
    let resource = encode_resource(&[attr]);
    let sm = encode_scope_metrics(None, &[]);
    let rm = encode_resource_metrics(Some(&resource), &[sm]);
    let src = encode_export_request(&[rm]);

    let req = ExportMetricsServiceRequest::unmarshal(&src).unwrap();

    let resource = req.resource_metrics[0]
        .resource
        .as_ref()
        .expect("resource must be set");
    assert_eq!(resource.attributes.len(), 1);
    assert_eq!(resource.attributes[0].key, "service.name");
    assert_eq!(resource.attributes[0].value.format_string(), "frontend");
}

#[test]
fn unknown_fields_skipped() {
    let dp = encode_number_data_point_double(&[], 1, 9.0, 0);
    let gauge = encode_gauge(&[dp]);
    let metric = encode_metric(
        "m",
        "",
        "",
        Some(MetricDataField {
            field_num: 5,
            bytes: gauge,
        }),
        &[],
    );

    // Unknown field 999 (length-delimited) at the ScopeMetrics level, and
    // unknown field 42 (varint) at the ExportMetricsServiceRequest level:
    // both must be skipped without disturbing decode of the real fields.
    let mut sm = Vec::new();
    append_bytes_field(&mut sm, 999, b"unexpected-junk");
    append_bytes_field(&mut sm, 2, &metric);

    let rm = encode_resource_metrics(None, &[sm]);

    let mut src = Vec::new();
    append_varint_field(&mut src, 42, 12345);
    append_bytes_field(&mut src, 1, &rm);

    let req = ExportMetricsServiceRequest::unmarshal(&src).unwrap();

    assert_eq!(req.resource_metrics.len(), 1);
    let metric = &req.resource_metrics[0].scope_metrics[0].metrics[0];
    assert_eq!(metric.name, "m");
    assert_eq!(
        metric.gauge.as_ref().unwrap().data_points[0].double_value,
        Some(9.0)
    );
}

// =======================================================================
// Additional representative coverage per message family
// =======================================================================

#[test]
fn number_data_point_sfixed64_int_value() {
    let dp = encode_number_data_point_int(2_000, -7, 1);
    assert_eq!(
        NumberDataPoint::unmarshal(&dp).unwrap(),
        NumberDataPoint {
            attributes: vec![],
            time_unix_nano: 2_000,
            double_value: None,
            int_value: Some(-7),
            flags: 1,
        }
    );
}

#[test]
fn exponential_histogram_positive_and_negative_buckets() {
    let positive = encode_buckets(1, &[1, 2, 3]);
    let negative = encode_buckets(-2, &[4, 5]);
    let dp = encode_exponential_histogram_data_point(
        3_000,
        10,
        Some(99.5),
        3,
        2,
        Some(&positive),
        Some(&negative),
        0,
    );
    let eh = encode_exponential_histogram(&[dp]);
    let metric = encode_metric(
        "eh_metric",
        "",
        "",
        Some(MetricDataField {
            field_num: 10,
            bytes: eh,
        }),
        &[],
    );
    let sm = encode_scope_metrics(None, &[metric]);
    let rm = encode_resource_metrics(None, &[sm]);
    let src = encode_export_request(&[rm]);

    let req = ExportMetricsServiceRequest::unmarshal(&src).unwrap();
    let metric = &req.resource_metrics[0].scope_metrics[0].metrics[0];
    let eh = metric.exponential_histogram.as_ref().unwrap();
    let dp = &eh.data_points[0];
    assert_eq!(dp.count, 10);
    assert_eq!(dp.sum, Some(99.5));
    assert_eq!(dp.scale, 3);
    assert_eq!(dp.zero_count, 2);
    assert_eq!(
        dp.positive,
        Some(Buckets {
            offset: 1,
            bucket_counts: vec![1, 2, 3],
        })
    );
    assert_eq!(
        dp.negative,
        Some(Buckets {
            offset: -2,
            bucket_counts: vec![4, 5],
        })
    );
}

#[test]
fn bucket_counts_accept_both_packed_and_unpacked_encodings() {
    // Packed: single length-delimited field 6 occurrence.
    let dp_packed = encode_histogram_data_point(&[], 1, 3, None, &[10, 20, 30], &[1.0, 2.0], 0);

    // Unpacked (legacy): one wire-type-1 occurrence per bucket count.
    let mut dp_unpacked = Vec::new();
    append_fixed64_field(&mut dp_unpacked, 3, 1);
    append_fixed64_field(&mut dp_unpacked, 4, 3);
    for count in [10u64, 20, 30] {
        append_fixed64_field(&mut dp_unpacked, 6, count);
    }
    for bound in [1.0f64, 2.0] {
        append_double_field(&mut dp_unpacked, 7, bound);
    }

    let packed = HistogramDataPoint::unmarshal(&dp_packed).unwrap();
    let unpacked = HistogramDataPoint::unmarshal(&dp_unpacked).unwrap();
    assert_eq!(packed.bucket_counts, unpacked.bucket_counts);
    assert_eq!(packed.explicit_bounds, unpacked.explicit_bounds);
    assert_eq!(packed.bucket_counts, vec![10, 20, 30]);
}

#[test]
fn buckets_bucket_counts_accept_both_packed_and_unpacked_uint64_encodings() {
    let packed = encode_buckets(0, &[7, 8, 9]);

    let mut unpacked = Vec::new();
    for count in [7u64, 8, 9] {
        append_varint_field(&mut unpacked, 2, count);
    }

    assert_eq!(
        Buckets::unmarshal(&packed).unwrap(),
        Buckets::unmarshal(&unpacked).unwrap()
    );
}

#[test]
fn any_value_scalars_format_as_expected() {
    assert_eq!(
        AnyValue::unmarshal(&encode_any_value_string("hello"))
            .unwrap()
            .format_string(),
        "hello"
    );
    assert_eq!(
        AnyValue::unmarshal(&encode_any_value_bool(true))
            .unwrap()
            .format_string(),
        "true"
    );
    assert_eq!(
        AnyValue::unmarshal(&encode_any_value_int(-5))
            .unwrap()
            .format_string(),
        "-5"
    );
    assert_eq!(
        AnyValue::unmarshal(&encode_any_value_double(3.5))
            .unwrap()
            .format_string(),
        "3.5"
    );
    assert_eq!(
        AnyValue::unmarshal(&encode_any_value_bytes(b"foo"))
            .unwrap()
            .format_string(),
        "Zm9v"
    );
}

#[test]
fn any_value_array_formats_as_json() {
    let arr = encode_any_value_array(&[
        encode_any_value_int(1),
        encode_any_value_string("x"),
        encode_any_value_bool(false),
    ]);
    let decoded = AnyValue::unmarshal(&arr).unwrap();
    assert_eq!(
        decoded,
        AnyValue::Array(ArrayValue {
            values: vec![
                AnyValue::Int(1),
                AnyValue::String("x".to_string()),
                AnyValue::Bool(false),
            ],
        })
    );
    assert_eq!(decoded.format_string(), r#"[1,"x",false]"#);
}

#[test]
fn any_value_kvlist_formats_as_json_object() {
    let kvl = encode_any_value_kvlist(&[encode_key_value(
        "inner",
        encode_any_value_int(7).as_slice(),
    )]);
    let decoded = AnyValue::unmarshal(&kvl).unwrap();
    assert_eq!(
        decoded,
        AnyValue::KeyValueList(KeyValueList {
            values: vec![KeyValue {
                key: "inner".to_string(),
                value: AnyValue::Int(7),
            }],
        })
    );
    assert_eq!(decoded.format_string(), r#"{"inner":7}"#);
}

#[test]
fn any_value_unset_when_no_oneof_case_present() {
    assert_eq!(AnyValue::unmarshal(&[]).unwrap(), AnyValue::Unset);
    assert_eq!(AnyValue::unmarshal(&[]).unwrap().format_string(), "");
}

#[test]
fn key_value_entry_dropped_when_key_is_missing() {
    // A KeyValue whose only field is `value` (field 2) — no key at all.
    let mut kv_no_key = Vec::new();
    append_bytes_field(&mut kv_no_key, 2, &encode_any_value_string("orphan"));

    let mut resource = Vec::new();
    append_bytes_field(&mut resource, 1, &kv_no_key);

    let decoded = Resource::unmarshal(&resource).unwrap();
    assert_eq!(decoded.attributes, vec![]);
}

#[test]
fn key_value_entry_dropped_when_value_submessage_is_structurally_absent() {
    let kv = encode_key_value_no_value_field("orphan.key");
    let mut resource = Vec::new();
    append_bytes_field(&mut resource, 1, &kv);

    let decoded = Resource::unmarshal(&resource).unwrap();
    assert_eq!(decoded.attributes, vec![]);
}

#[test]
fn key_value_entry_kept_with_unset_value_when_value_submessage_is_present_but_empty() {
    // Value sub-message field 2 present, but zero-length (no oneof case).
    let kv = encode_key_value("present.but.empty", &[]);
    let mut resource = Vec::new();
    append_bytes_field(&mut resource, 1, &kv);

    let decoded = Resource::unmarshal(&resource).unwrap();
    assert_eq!(
        decoded.attributes,
        vec![KeyValue {
            key: "present.but.empty".to_string(),
            value: AnyValue::Unset,
        }]
    );
}

#[test]
fn instrumentation_scope_and_scope_metrics_decode() {
    let attr = encode_key_value("scope.attr", encode_any_value_string("v").as_slice());
    let scope = encode_instrumentation_scope(Some("my-scope"), Some("1.2.3"), &[attr]);
    let sm_bytes = encode_scope_metrics(Some(&scope), &[]);

    let decoded = ScopeMetrics::unmarshal(&sm_bytes).unwrap();
    let scope = decoded.scope.expect("scope must decode");
    assert_eq!(scope.name.as_deref(), Some("my-scope"));
    assert_eq!(scope.version.as_deref(), Some("1.2.3"));
    assert_eq!(scope.attributes.len(), 1);
    assert_eq!(scope.attributes[0].key, "scope.attr");
}

#[test]
fn metric_metadata_attributes_decode() {
    let md = encode_key_value(
        "prometheus.type",
        encode_any_value_string("counter").as_slice(),
    );
    let metric_bytes = encode_metric("m", "desc", "unit", None, &[md]);

    let metric = Metric::unmarshal(&metric_bytes).unwrap();
    assert_eq!(metric.name, "m");
    assert_eq!(metric.description, "desc");
    assert_eq!(metric.unit, "unit");
    assert_eq!(metric.metadata.len(), 1);
    assert_eq!(metric.metadata[0].key, "prometheus.type");
    assert_eq!(metric.metadata[0].value.format_string(), "counter");
    assert!(metric.gauge.is_none());
}

#[test]
fn truncated_input_errors() {
    let dp = encode_number_data_point_double(&[], 1, 1.0, 0);
    let gauge = encode_gauge(&[dp]);
    let metric = encode_metric(
        "m",
        "",
        "",
        Some(MetricDataField {
            field_num: 5,
            bytes: gauge,
        }),
        &[],
    );
    let sm = encode_scope_metrics(None, &[metric]);
    let rm = encode_resource_metrics(None, &[sm]);
    let src = encode_export_request(&[rm]);

    let truncated = &src[..src.len() - 3];
    let err = ExportMetricsServiceRequest::unmarshal(truncated).unwrap_err();
    assert!(matches!(
        err,
        WireError::LengthOutOfRange | WireError::UnexpectedEof
    ));
}
