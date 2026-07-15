//! OTLP metrics → flattened Prometheus-shaped row conversion.
//!
//! Port of the *conversion* logic that upstream VictoriaMetrics v1.146.0
//! actually implements in `lib/protoparser/opentelemetry/pb/pb.go`'s
//! `decoderContext` (`decodeResource`/`decodeScopeMetrics`/`decodeMetric`/
//! `decodeGauge`/`decodeSum`/`decodeHistogram`/`decodeExponentialHistogram`/
//! `decodeSummary` and their `*DataPointContext.pushSamples` methods), not
//! `lib/protoparser/opentelemetry/stream/streamparser.go` as the task brief
//! assumed — **finding**: `streamparser.go`'s `writeRequestContext` only
//! implements the `pb.MetricPusher` callback interface (name sanitization +
//! copying labels/timestamp/value into `prompb.TimeSeries`); the actual
//! per-metric-type conversion (which suffixes to emit, cumulative bucket
//! counts, resource/scope label promotion) lives entirely in `pb.go`, which
//! Task 11 explicitly deferred ("Flattening the tree into Prometheus
//! samples/labels ... is deferred to a downstream conversion task") since it
//! built an AST instead of a decode-to-labels streamer. This module is that
//! downstream conversion, operating on the AST instead of raw wire bytes.
//!
//! Fixed flag defaults assumed throughout (matching upstream's actual
//! defaults, not the brief's fixed set):
//! - `-opentelemetry.usePrometheusNaming=false` (`sanitize` module is a
//!   no-op at this default).
//! - `-opentelemetry.promoteScopeMetadata=true` (default `true`) — scope
//!   metadata labels are added whenever a `Scope` submessage is present.
//! - `-opentelemetry.promoteAllResourceAttributes=true` (default `true`) —
//!   all resource attributes are promoted to labels; the
//!   `ignoreResourceAttributes`/`promoteResourceAttributes` filter lists are
//!   out of scope (empty by default anyway).
//!
//! Also out of scope, matching the established precedent in
//! `esm-insert`'s `prometheusimport.rs`/`promremotewrite.rs` (which already
//! drop `prommetadata`): `Metric.metadata`'s `"prometheus.type"` override and
//! `PushMetricMetadata`/`MetricMetadata` push entirely. At this port's fixed
//! naming defaults, `MetricMetadata.Type` has **zero** observable effect on
//! the emitted rows (`sanitizeMetricName`'s `_total`/`_ratio` suffix logic,
//! the only place `Type` is consulted outside the metadata-query API, is
//! itself unreachable at `usePrometheusNaming=false`) — so tracking it would
//! be pure dead weight for a metadata API this crate does not expose.

use std::fmt;
use std::io::{self, Read};

use crate::wire::WireError;

use super::pb;
use super::sanitize;
use crate::util::{self, UtilError};

/// Default maximum size in bytes of a single OpenTelemetry request. Go:
/// `-opentelemetry.maxRequestSize` flag default — a dedicated, larger cap
/// than the shared `-maxInsertRequestSize` (32MiB, [`crate::util::MAX_INSERT_REQUEST_SIZE`])
/// used by the other ingestion paths.
pub const MAX_OTLP_REQUEST_SIZE: usize = 64 * 1024 * 1024;

/// One flattened Prometheus-shaped row produced from an OTLP metric data
/// point. Mirrors [`crate::prometheus::Row`]'s shape (metric name kept
/// separate from the label set) so `esm-insert`'s handler can reuse the same
/// `ConvertCtx` / `marshal_metric_name_raw` conversion pattern already
/// established for `prometheusimport`/`promremotewrite`.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub metric: String,
    pub tags: Vec<(String, String)>,
    pub timestamp: i64,
    pub value: f64,
}

/// Error returned by [`parse_stream`].
#[derive(Debug)]
pub enum Error {
    /// I/O error while reading the request body.
    Io(io::Error),
    /// The `Content-Encoding` value is not recognized.
    UnsupportedEncoding(String),
    /// The (decompressed) body exceeds `MAX_OTLP_REQUEST_SIZE` bytes.
    TooBig { limit: usize },
    /// The compressed body could not be decoded.
    Decompress(String),
    /// The decompressed body could not be unmarshaled as an
    /// `ExportMetricsServiceRequest`.
    Unmarshal { len: usize, source: WireError },
    /// The caller-supplied callback returned an error.
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => {
                write!(
                    f,
                    "cannot read OpenTelemetry protocol data from client: {err}"
                )
            }
            Error::UnsupportedEncoding(enc) => {
                write!(f, "unsupported Content-Encoding: {enc:?}")
            }
            Error::TooBig { limit } => write!(
                f,
                "too big unpacked OpenTelemetry request; mustn't exceed {limit} bytes"
            ),
            Error::Decompress(msg) => {
                write!(f, "cannot decompress OpenTelemetry request: {msg}")
            }
            Error::Unmarshal { len, source } => {
                write!(f, "cannot unmarshal request from {len} bytes: {source}")
            }
            Error::Callback(err) => write!(f, "errors happened during parsing: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Callback(err) => Some(err.as_ref()),
            Error::UnsupportedEncoding(_) | Error::TooBig { .. } | Error::Decompress(_) => None,
            Error::Unmarshal { source, .. } => Some(source),
        }
    }
}

impl From<UtilError> for Error {
    fn from(err: UtilError) -> Self {
        match err {
            UtilError::Io(e) => Error::Io(e),
            UtilError::UnsupportedEncoding(enc) => Error::UnsupportedEncoding(enc),
            UtilError::TooBig { limit } => Error::TooBig { limit },
            UtilError::Decompress(msg) => Error::Decompress(msg),
            // `parse_stream`'s `read_uncompressed_data` closure below never
            // returns `Err` itself (unmarshal failures are captured
            // out-of-band via `unmarshal_result` instead, to keep them a
            // distinct `Error::Unmarshal` variant, matching the precedent in
            // `crate::promremotewrite`) — unreachable in practice, handled
            // without panicking per this crate's error-handling rules.
            UtilError::Callback(err) => Error::Decompress(err.to_string()),
        }
    }
}

/// Parses OTLP metrics protobuf data from `r` and calls `callback` once with
/// every row converted from the request.
///
/// Go: `stream.ParseStream` + `pb.DecodeMetricsData` (see the module doc for
/// why both are ported here rather than just `streamparser.go`).
///
/// Deviation: upstream flushes `callback` incrementally (every 4MiB of
/// buffered samples) to bound memory on huge requests; this port builds the
/// full `Vec<Row>` for the request in memory and calls `callback` exactly
/// once, matching how Task 11 already built a full in-memory AST rather than
/// streaming the decode.
pub fn parse_stream<R: Read>(
    r: R,
    encoding: &str,
    mut callback: impl FnMut(&[Row]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(), Error> {
    let mut unmarshal_result: Option<(usize, Result<Vec<Row>, WireError>)> = None;
    util::read_uncompressed_data(r, encoding, MAX_OTLP_REQUEST_SIZE, |data| {
        let len = data.len();
        let result = pb::ExportMetricsServiceRequest::unmarshal(data).map(|req| {
            let mut rows = Vec::new();
            convert_request(&req, &mut rows);
            rows
        });
        unmarshal_result = Some((len, result));
        Ok(())
    })?;

    match unmarshal_result {
        None => Ok(()),
        Some((_, Ok(rows))) => callback(&rows).map_err(Error::Callback),
        Some((len, Err(source))) => Err(Error::Unmarshal { len, source }),
    }
}

/// Walks the decoded request tree, appending one [`Row`] per emitted sample.
/// Go: `pb.DecodeMetricsData` + `decoderContext.decodeResourceMetrics`.
fn convert_request(req: &pb::ExportMetricsServiceRequest, rows: &mut Vec<Row>) {
    for rm in &req.resource_metrics {
        // Go: `decodeResource` — `promoteAllResourceAttributes=true` (fixed
        // default) means every resource attribute is promoted, unprefixed.
        let mut resource_labels: Vec<(String, String)> = Vec::new();
        if let Some(resource) = &rm.resource {
            for kv in &resource.attributes {
                push_attribute_label(&mut resource_labels, &kv.key, &kv.value);
            }
        }

        for sm in &rm.scope_metrics {
            let mut scope_labels = resource_labels.clone();
            // Go: `decodeScopeMetrics` — `if !disableScopeMetadata { ... }`
            // (`promoteScopeMetadata=true` fixed default). Note this whole
            // block is skipped when the `Scope` submessage itself is absent
            // (`ok` false on the `easyproto.GetMessageData` call), *not*
            // just when name/version happen to be empty — the "unknown"
            // fallback only applies to the name/version *fields* once we
            // know a `Scope` submessage exists.
            if let Some(scope) = &sm.scope {
                let name = scope.name.clone().unwrap_or_else(|| "unknown".to_string());
                let version = scope
                    .version
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                scope_labels.push(("scope.name".to_string(), name));
                scope_labels.push(("scope.version".to_string(), version));
                for kv in &scope.attributes {
                    let key = format!("scope.attributes.{}", kv.key);
                    push_attribute_label(&mut scope_labels, &key, &kv.value);
                }
            }

            for metric in &sm.metrics {
                convert_metric(metric, &scope_labels, rows);
            }
        }
    }
}

/// Go: `decoderContext.decodeMetric` dispatching into
/// `decodeGauge`/`decodeSum`/`decodeHistogram`/
/// `decodeExponentialHistogram`/`decodeSummary`. If more than one `oneof
/// data` case is set on the wire (malformed input), all of them are
/// converted independently, mirroring how `Metric` decode itself (Task 11)
/// keeps them as independent `Option`s rather than enforcing mutual
/// exclusion.
fn convert_metric(metric: &pb::Metric, base_labels: &[(String, String)], rows: &mut Vec<Row>) {
    let name = sanitize::sanitize_metric_name(&metric.name);

    if let Some(gauge) = &metric.gauge {
        for dp in &gauge.data_points {
            push_number_sample(dp, name, base_labels, rows);
        }
    }
    if let Some(sum) = &metric.sum {
        for dp in &sum.data_points {
            push_number_sample(dp, name, base_labels, rows);
        }
    }
    if let Some(histogram) = &metric.histogram {
        for dp in &histogram.data_points {
            push_histogram_sample(dp, name, base_labels, rows);
        }
    }
    if let Some(eh) = &metric.exponential_histogram {
        for dp in &eh.data_points {
            push_exponential_histogram_sample(dp, name, base_labels, rows);
        }
    }
    if let Some(summary) = &metric.summary {
        for dp in &summary.data_points {
            push_summary_sample(dp, name, base_labels, rows);
        }
    }
}

/// Go: `decoderContext.decodeNumberDataPoint` +
/// `dctx.mp.PushSample(&dctx.mm, "", ...)` — used for both `Gauge` and `Sum`
/// data points (the suffix is always empty for these; `Sum`'s
/// monotonic-vs-gauge `MetricMetadata.Type` distinction has no effect on the
/// emitted row at this port's fixed naming defaults — see the module doc).
fn push_number_sample(
    dp: &pb::NumberDataPoint,
    metric_name: &str,
    base_labels: &[(String, String)],
    rows: &mut Vec<Row>,
) {
    let mut labels = base_labels.to_vec();
    for kv in &dp.attributes {
        push_attribute_label(&mut labels, &kv.key, &kv.value);
    }
    rows.push(Row {
        metric: metric_name.to_string(),
        tags: labels,
        timestamp: nanos_to_millis(dp.time_unix_nano),
        value: apply_stale_flag(number_data_point_value(dp), dp.flags),
    });
}

/// Go: the `NumberDataPoint` local `value` variable in
/// `decodeNumberDataPoint`, which starts at `0.0` and is overwritten by
/// whichever `oneof` case (`as_double`/`as_int`) is present on the wire; if
/// neither is present it stays `0.0`. If both are present (malformed —
/// `oneof` violation), this prefers `double_value`, whereas Go would keep
/// whichever field appeared *last* on the wire; both are independent
/// `Option`s here (Task 11's decode deviation), so exact wire-order
/// last-wins isn't reconstructable without re-reading raw bytes.
fn number_data_point_value(dp: &pb::NumberDataPoint) -> f64 {
    if let Some(d) = dp.double_value {
        d
    } else if let Some(i) = dp.int_value {
        i as f64
    } else {
        0.0
    }
}

/// Go: `histogramDataPointContext.pushSamples`.
fn push_histogram_sample(
    dp: &pb::HistogramDataPoint,
    metric_name: &str,
    base_labels: &[(String, String)],
    rows: &mut Vec<Row>,
) {
    if dp.bucket_counts.is_empty() {
        // Go: `if len(hctx.bucketCounts) == 0 { skippedSampleLogger.Warnf(...); return }`.
        // No logging framework is wired up in this crate yet (same gap as
        // `prometheusimport.rs`'s `err_logger`), so this just skips.
        return;
    }
    if dp.bucket_counts.len() != dp.explicit_bounds.len() + 1 {
        // Go: same warn+skip, for a malformed bucket/bound count mismatch.
        return;
    }

    let mut labels = base_labels.to_vec();
    for kv in &dp.attributes {
        push_attribute_label(&mut labels, &kv.key, &kv.value);
    }
    let timestamp = nanos_to_millis(dp.time_unix_nano);
    let flags = dp.flags;

    rows.push(Row {
        metric: format!("{metric_name}_count"),
        tags: labels.clone(),
        timestamp,
        value: apply_stale_flag(dp.count as f64, flags),
    });
    // Go: `sum` is optional — absent when negative events were recorded.
    if let Some(sum) = dp.sum {
        rows.push(Row {
            metric: format!("{metric_name}_sum"),
            tags: labels.clone(),
            timestamp,
            value: apply_stale_flag(sum, flags),
        });
    }

    let mut cumulative: u64 = 0;
    for (i, bound) in dp.explicit_bounds.iter().enumerate() {
        cumulative += dp.bucket_counts[i];
        let mut bucket_labels = labels.clone();
        bucket_labels.push(("le".to_string(), pb::format_float(*bound)));
        rows.push(Row {
            metric: format!("{metric_name}_bucket"),
            tags: bucket_labels,
            timestamp,
            value: apply_stale_flag(cumulative as f64, flags),
        });
    }
    cumulative += dp.bucket_counts[dp.bucket_counts.len() - 1];
    let mut inf_labels = labels;
    inf_labels.push(("le".to_string(), "+Inf".to_string()));
    rows.push(Row {
        metric: format!("{metric_name}_bucket"),
        tags: inf_labels,
        timestamp,
        value: apply_stale_flag(cumulative as f64, flags),
    });
}

/// Go: `exponentialHistogramDataPointContext.pushSamples`. Unlike the plain
/// histogram path, `_count`/`_sum` are always pushed (no empty-buckets
/// skip), and buckets are VictoriaMetrics' own `vmrange`-labeled exponential
/// buckets rather than Prometheus `le` buckets.
fn push_exponential_histogram_sample(
    dp: &pb::ExponentialHistogramDataPoint,
    metric_name: &str,
    base_labels: &[(String, String)],
    rows: &mut Vec<Row>,
) {
    let mut labels = base_labels.to_vec();
    for kv in &dp.attributes {
        push_attribute_label(&mut labels, &kv.key, &kv.value);
    }
    let timestamp = nanos_to_millis(dp.time_unix_nano);
    let flags = dp.flags;

    rows.push(Row {
        metric: format!("{metric_name}_count"),
        tags: labels.clone(),
        timestamp,
        value: apply_stale_flag(dp.count as f64, flags),
    });
    if let Some(sum) = dp.sum {
        rows.push(Row {
            metric: format!("{metric_name}_sum"),
            tags: labels.clone(),
            timestamp,
            value: apply_stale_flag(sum, flags),
        });
    }

    if dp.zero_count > 0 {
        let mut zero_labels = labels.clone();
        zero_labels.push((
            "vmrange".to_string(),
            format_vmrange(-dp.zero_threshold, dp.zero_threshold),
        ));
        rows.push(Row {
            metric: format!("{metric_name}_bucket"),
            tags: zero_labels,
            timestamp,
            value: apply_stale_flag(dp.zero_count as f64, flags),
        });
    }

    // Go: `ratio := math.Pow(2, -float64(scale)); base := math.Pow(2, ratio)`.
    let ratio = 2f64.powf(-f64::from(dp.scale));
    let base = 2f64.powf(ratio);

    if let Some(positive) = &dp.positive {
        let positive_bound = 2f64.powf(f64::from(positive.offset) * ratio);
        for (i, &count) in positive.bucket_counts.iter().enumerate() {
            if count == 0 {
                // Go: `if count <= 0` — `bucketCounts` is `uint64`, so the
                // only way to satisfy that is `count == 0`.
                continue;
            }
            let lower = positive_bound * base.powi(i as i32);
            let upper = lower * base;
            let mut bucket_labels = labels.clone();
            bucket_labels.push(("vmrange".to_string(), format_vmrange(lower, upper)));
            rows.push(Row {
                metric: format!("{metric_name}_bucket"),
                tags: bucket_labels,
                timestamp,
                value: apply_stale_flag(count as f64, flags),
            });
        }
    }
    if let Some(negative) = &dp.negative {
        let negative_bound = 2f64.powf(f64::from(negative.offset) * ratio);
        for (i, &count) in negative.bucket_counts.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let lower = negative_bound * base.powi(i as i32);
            let upper = lower * base;
            let mut bucket_labels = labels.clone();
            bucket_labels.push(("vmrange".to_string(), format_vmrange(-upper, -lower)));
            rows.push(Row {
                metric: format!("{metric_name}_bucket"),
                tags: bucket_labels,
                timestamp,
                value: apply_stale_flag(count as f64, flags),
            });
        }
    }
}

/// Go: `summaryDataPointContext.pushSamples`.
fn push_summary_sample(
    dp: &pb::SummaryDataPoint,
    metric_name: &str,
    base_labels: &[(String, String)],
    rows: &mut Vec<Row>,
) {
    let mut labels = base_labels.to_vec();
    for kv in &dp.attributes {
        push_attribute_label(&mut labels, &kv.key, &kv.value);
    }
    let timestamp = nanos_to_millis(dp.time_unix_nano);
    let flags = dp.flags;

    rows.push(Row {
        metric: format!("{metric_name}_count"),
        tags: labels.clone(),
        timestamp,
        value: apply_stale_flag(dp.count as f64, flags),
    });
    rows.push(Row {
        metric: format!("{metric_name}_sum"),
        tags: labels.clone(),
        timestamp,
        value: apply_stale_flag(dp.sum, flags),
    });
    for qv in &dp.quantile_values {
        let mut q_labels = labels.clone();
        q_labels.push(("quantile".to_string(), pb::format_float(qv.quantile)));
        rows.push(Row {
            metric: metric_name.to_string(),
            tags: q_labels,
            timestamp,
            value: apply_stale_flag(qv.value, flags),
        });
    }
}

/// Go: `timestamp := int64(timestampNsecs / 1e6)` (plain integer division —
/// `1e6` is exactly representable, so this is not a floating-point op in
/// Go either; truncates towards zero, matching `u64`'s unsigned floor
/// division here).
fn nanos_to_millis(nanos: u64) -> i64 {
    (nanos / 1_000_000) as i64
}

/// Go: `if flags&1 != 0 { value = decimal.StaleNaN }`
/// (`prompb`'s `NoRecordedValue`/staleness marker bit, per the
/// opentelemetry-proto `DataPointFlags` doc).
fn apply_stale_flag(value: f64, flags: u32) -> f64 {
    if flags & 1 != 0 {
        esm_common::decimal::STALE_NAN
    } else {
        value
    }
}

/// Flattens `key`'s `AnyValue` into zero-or-more `(name, value)` label pairs,
/// pushed onto `labels`. Mirrors upstream's dual rendering rule (`pb.go`'s
/// `decodeAnyValue`/`decodeKeyValue`/`decodeKeyValueList`): scalar values
/// (string/bool/int/double/bytes) and arrays become exactly one label each
/// (arrays JSON-encoded, see [`any_value_to_json`]); a `KeyValueList`
/// recursively flattens into dotted-prefix labels (`key.subkey`) instead,
/// all the way down for nested kvlists — it never becomes a single
/// JSON-object label at the top level (only a kvlist nested *inside* an
/// array renders as a JSON object).
fn push_attribute_label(labels: &mut Vec<(String, String)>, key: &str, value: &pb::AnyValue) {
    // Go: `decodeAnyValue` over a present-but-empty `AnyValue` message loops
    // over zero bytes and never calls `ls.Add`, so no label is emitted.
    // `AnyValue::Unset` is this port's decoded form of that empty message —
    // skip it so no spurious empty-valued label is produced.
    if matches!(value, pb::AnyValue::Unset) {
        return;
    }
    if let pb::AnyValue::KeyValueList(kvl) = value {
        for kv in &kvl.values {
            let sub_key = format!("{key}.{}", kv.key);
            push_attribute_label(labels, &sub_key, &kv.value);
        }
        return;
    }
    // Go: `wctx.PushSample` runs every label name through
    // `sctx.sanitizeLabelName` — identity at this port's fixed
    // `usePrometheusNaming=false` default (see the `sanitize` module doc).
    let name = sanitize::sanitize_label_name(key).to_string();
    labels.push((name, format_label_value(value)));
}

/// Renders a single top-level attribute value to its label-value string. Go:
/// `pb.go`'s `decodeAnyValue` (label path) — bool via `strconv.FormatBool`,
/// int via `strconv.AppendInt(_, 10)`, double via
/// `strconv.AppendFloat(_, 'f', -1, 64)` (fixed notation — [`pb::format_float`]
/// reused here byte-for-byte), bytes via `base64.StdEncoding`
/// ([`pb::base64_encode`] reused here), arrays JSON-encoded through
/// `pb_json.go`'s `decodeArrayValueToJSON` ([`any_value_to_json`]).
fn format_label_value(value: &pb::AnyValue) -> String {
    match value {
        pb::AnyValue::Unset => String::new(),
        pb::AnyValue::String(s) => s.clone(),
        pb::AnyValue::Bool(b) => b.to_string(),
        pb::AnyValue::Int(i) => i.to_string(),
        pb::AnyValue::Double(d) => pb::format_float(*d),
        pb::AnyValue::Bytes(b) => pb::base64_encode(b),
        // Only reachable for `Array` in practice, since `push_attribute_label`
        // diverts a top-level `KeyValueList` to the flattening branch before
        // this function is ever called for one; kept exhaustive (rather than
        // `unreachable!`) so a `KeyValueList` reached here some other way
        // still degrades gracefully to the same JSON rendering `pb_json.go`
        // uses for one nested inside an array.
        pb::AnyValue::Array(_) | pb::AnyValue::KeyValueList(_) => any_value_to_json(value),
    }
}

/// Renders an `AnyValue` tree to JSON text, matching `pb_json.go`'s
/// `decodeArrayValueToJSON`/`decodeAnyValueToJSON`/`decodeKeyValueListToJSON`
/// applied to an already-decoded `AnyValue` (rather than raw wire bytes).
/// Reachable only for `Array` values and anything nested inside one
/// (including a `KeyValueList` nested inside an array, which *does* become a
/// JSON object — unlike a *top-level* `KeyValueList` attribute value, which
/// flattens instead; see [`push_attribute_label`]).
///
/// Numeric rendering matches upstream exactly:
/// - Integers: `strconv.AppendInt(_, 10)` (`fastjson.NewNumberInt`, only
///   reachable when the `int64` fits `int` — always true on the 64-bit
///   targets this crate builds for).
/// - Doubles: `strconv.AppendFloat(_, 'g', -1, 64)` (`fastjson.NewNumberFloat64`,
///   `vendor/github.com/valyala/fastjson/arena.go:82`) — [`format_float_go_g`]
///   ports this exactly (verified against real `go1.26` `strconv.FormatFloat`
///   output across 39 vectors spanning the `-4`/`6` exponent-threshold
///   boundary, negative/zero/subnormal/huge values; see its doc comment).
///   This is the fix for the divergence Task 11's `AnyValue::format_string`
///   documented (it used `serde_json`'s ryu-based shortest form instead,
///   which disagrees with Go's `'g'` verb on the fixed-vs-scientific
///   notation choice at extreme magnitudes).
///
/// String escaping/JSON structure reuses `serde_json` (matches Task 11's
/// precedent for the same reason — not a flagged fidelity item, unlike
/// numeric formatting).
fn any_value_to_json(value: &pb::AnyValue) -> String {
    match value {
        pb::AnyValue::Unset => "null".to_string(),
        pb::AnyValue::String(s) => serde_json::to_string(s).expect("string always serializes"),
        pb::AnyValue::Bool(b) => b.to_string(),
        pb::AnyValue::Int(i) => i.to_string(),
        pb::AnyValue::Double(d) => format_float_go_g(*d),
        pb::AnyValue::Bytes(b) => {
            serde_json::to_string(&pb::base64_encode(b)).expect("string always serializes")
        }
        pb::AnyValue::Array(arr) => {
            let items: Vec<String> = arr.values.iter().map(any_value_to_json).collect();
            format!("[{}]", items.join(","))
        }
        pb::AnyValue::KeyValueList(kvl) => {
            let items: Vec<String> = kvl
                .values
                .iter()
                .map(|kv| {
                    let key = serde_json::to_string(&kv.key).expect("string always serializes");
                    format!("{key}:{}", any_value_to_json(&kv.value))
                })
                .collect();
            format!("{{{}}}", items.join(","))
        }
    }
}

/// Go: `strconv.FormatFloat(v, 'g', -1, 64)`.
///
/// Ground-truthed against `go1.26`'s `internal/strconv/ftoa.go`: contrary to
/// this task's brief (which speculated a `>= 21` exponent threshold), the
/// actual decision in `formatDigits`'s `'g'`/`'G'` case is `eprec = 6` for
/// the shortest-precision path (`ftoa.go:239-241`,
/// `"if shortest { eprec = 6 }"`), so scientific notation is used when
/// `exp < -4 || exp >= 6`, not `>= 21`. Verified with 39 side-by-side
/// vectors against real `go1.26` `strconv.FormatFloat` output, including
/// `123456789012345.0` (→ scientific, since `exp=14 >= 6`), the `-4`/`6`
/// boundary itself (`1e-4`/`1e-5`/`100000.0`/`1e6`), `1e21`, `1e-7`, and
/// negative/huge/subnormal values.
///
/// Implementation: Rust's `{:e}` formatting produces the same shortest
/// round-trip decimal digit sequence as Go's `strconv` shortest algorithm
/// (both implement the same well-defined "shortest decimal that round-trips"
/// spec), and — critically — its exponent is exactly Go's `digs.dp - 1`
/// (the decision variable `exp` in `ftoa.go`), so the `-4`/`6` threshold
/// check can be applied directly to it. When in the fixed-notation range,
/// Rust's plain `Display` (`{v}`) already matches Go's `'f'`,`-1`,`64`
/// output exactly (see [`pb::format_float`]'s doc comment) — the two verbs'
/// shortest-form fixed-notation digit generation is identical, only the
/// notation *choice* differs between `'f'` (always fixed) and `'g'`
/// (fixed only within this threshold).
fn format_float_go_g(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    let sci = format!("{v:e}");
    let (sign, rest) = match sci.strip_prefix('-') {
        Some(r) => ("-", r),
        None => ("", sci.as_str()),
    };
    let e_pos = rest
        .find('e')
        .expect("Rust `{:e}` output always contains an 'e'");
    let mantissa = &rest[..e_pos];
    let exp: i32 = rest[e_pos + 1..]
        .parse()
        .expect("Rust `{:e}` exponent is always a valid integer");
    let digits: String = mantissa.chars().filter(|&c| c != '.').collect();

    if !(-4..6).contains(&exp) {
        let mut out = String::new();
        out.push_str(sign);
        out.push(digits.as_bytes()[0] as char);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        let abs_exp = exp.unsigned_abs();
        if abs_exp < 10 {
            out.push('0');
        }
        out.push_str(&abs_exp.to_string());
        out
    } else {
        format!("{v}")
    }
}

/// Go: `fmtBuffer.formatVmrange` — `strconv.AppendFloat(_, start, 'e', 3,
/// 64) + "..." + strconv.AppendFloat(_, end, 'e', 3, 64)`: fixed 3-digit
/// scientific notation (*not* shortest form), for exponential-histogram
/// `vmrange` bucket labels.
fn format_vmrange(start: f64, end: f64) -> String {
    format!("{}...{}", format_e3(start), format_e3(end))
}

/// Go: `strconv.AppendFloat(_, v, 'e', 3, 64)` — always scientific notation,
/// exactly 3 digits after the decimal point, exponent zero-padded to at
/// least 2 digits with an explicit sign. Verified against real `go1.26`
/// output across 9 vectors including negative bounds and runtime (not
/// constant-folded) negative zero (`-0.0`, which Go's `strconv` *does*
/// render as `"-0.000e+00"` for a runtime value, unlike a compile-time `-0.0`
/// literal, which Go's constant arithmetic folds to positive zero before it
/// ever reaches `strconv` — not a factor here since `zero_threshold` is
/// always a decoded runtime `f64`).
///
/// Implementation: Rust's `{:.3e}` already produces the correctly-rounded
/// 3-fractional-digit mantissa (fixed precision, so no ambiguity about
/// shortest-form digit count the way [`format_float_go_g`] has to handle);
/// only the exponent's sign-and-padding presentation needs reformatting to
/// match Go's `AppendFloat` (Rust's `{:e}` omits the `+` and zero-padding).
fn format_e3(v: f64) -> String {
    let s = format!("{v:.3e}");
    let (mantissa, exp_str) = s
        .split_once('e')
        .expect("Rust `{:.3e}` output always contains an 'e'");
    let exp: i32 = exp_str
        .parse()
        .expect("Rust `{:.3e}` exponent is always a valid integer");
    let sign_char = if exp < 0 { '-' } else { '+' };
    let abs_exp = exp.unsigned_abs();
    if abs_exp < 10 {
        format!("{mantissa}e{sign_char}0{abs_exp}")
    } else {
        format!("{mantissa}e{sign_char}{abs_exp}")
    }
}

// Unit tests live in `convert/tests.rs` (a `#[path]`-wired child module, so
// they keep access to this file's private items) — split out to keep this
// file under the 800-line-per-file guideline.
#[cfg(test)]
#[path = "convert/tests.rs"]
mod tests;
