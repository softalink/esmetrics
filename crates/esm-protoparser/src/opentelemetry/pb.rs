//! OTLP metrics protobuf message types and decode.
//!
//! Port of the decode direction of upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/opentelemetry/pb/pb.go` (`UnmarshalProtobuf`-equivalent
//! methods only — `marshalProtobuf` is vmagent-only and is skipped), using
//! [`crate::wire::WireReader`] in place of `easyproto`.
//!
//! Field numbers below are transcribed from the `case` arms and struct-doc
//! `.proto` snippets in `pb.go`, not guessed. See each `unmarshal` function's
//! doc comment for the field map it implements.
//!
//! ## Deviations from upstream
//!
//! - Owned `String`/`Vec` fields throughout (matching the task's scope: this
//!   is a decode-to-AST port, not a decode-to-labels port; see the
//!   module-level doc on [`super`]).
//! - String fields are decoded via [`String::from_utf8_lossy`], not an
//!   unchecked byte reinterpretation like Go's `unsafeBytesToString` — Rust
//!   `String` must be valid UTF-8, and untrusted network input should never
//!   be blindly reinterpreted.
//! - Duplicate (repeated-by-mistake) *singular* fields (e.g. two `Key`
//!   fields in one `KeyValue`) resolve to the *last* occurrence seen, not
//!   necessarily upstream's `easyproto.Get*` (first-match) semantics — an
//!   intentional, documented simplification for a case that does not occur
//!   in well-formed input.
//! - `KeyValue`/`AnyValue` decode mirrors the *exact* upstream skip rule
//!   (`pb.go`'s `decodeKeyValue` and `pb_json.go`'s `decodeKeyValueToJSON`
//!   agree on it): an entry is dropped entirely when its `key` field is
//!   absent **or** its `value` sub-message field is structurally absent from
//!   the wire; it is *kept* (rendering as empty string / JSON `null`) when
//!   the `value` sub-message is present but none of its `oneof` cases match.
//! - [`AnyValue::format_string`] does not correspond to a single upstream
//!   function — see its doc comment.
//! - JSON rendering (for `AnyValue::Array`/`AnyValue::KeyValueList`) reuses
//!   `serde_json::Value` (already a workspace dependency, `preserve_order`
//!   feature enabled to match `fastjson.Object`'s insertion-order iteration)
//!   instead of hand-rolling a JSON encoder. Nested float formatting uses
//!   `serde_json`'s ryu-based shortest representation rather than Go's
//!   `strconv.AppendFloat(_, 'g', -1, 64)`; the two agree for ordinary
//!   metric-attribute values but do not agree byte-for-byte on the choice of
//!   fixed vs. scientific notation at extreme magnitudes.

pub use crate::wire::WireError;
use crate::wire::WireReader;

// ---------------------------------------------------------------------
// Top-level request
// ---------------------------------------------------------------------

/// The top-level OTLP metrics export request.
///
/// Go: has no direct analog in `pb.go` — the closest sibling there is
/// `MetricsData` (same wire shape: `{1: repeated ResourceMetrics}`), which
/// `DecodeMetricsData` decodes directly. `ExportMetricsServiceRequest` is
/// the name used by the OTLP/gRPC+HTTP `MetricsService` collector proto for
/// the same message; this crate decodes the HTTP/protobuf export request
/// body, so that name is used here instead.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ExportMetricsServiceRequest {
    pub resource_metrics: Vec<ResourceMetrics>,
}

impl ExportMetricsServiceRequest {
    /// Field map: `{1: repeated ResourceMetrics}`.
    ///
    /// Go: `DecodeMetricsData`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut req = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    req.resource_metrics.push(ResourceMetrics::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(req)
    }
}

/// Go: `ResourceMetrics`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ResourceMetrics {
    pub resource: Option<Resource>,
    pub scope_metrics: Vec<ScopeMetrics>,
}

impl ResourceMetrics {
    /// Field map: `{1: Resource, 2: repeated ScopeMetrics}`.
    ///
    /// Go: `decoderContext.decodeResourceMetrics`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut rm = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    rm.resource = Some(Resource::unmarshal(data)?);
                }
                2 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    rm.scope_metrics.push(ScopeMetrics::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(rm)
    }
}

/// Go: `Resource`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Resource {
    pub attributes: Vec<KeyValue>,
}

impl Resource {
    /// Field map: `{1: repeated KeyValue}`.
    ///
    /// Go: `decoderContext.decodeResource` (the resource-attribute-filtering
    /// options that method also accepts, `DisableResourceAttributes` /
    /// `ResourceAttributesList`, are downstream conversion policy — out of
    /// scope for decode).
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut resource = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    if let Some(kv) = decode_key_value(data)? {
                        resource.attributes.push(kv);
                    }
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(resource)
    }
}

// ---------------------------------------------------------------------
// KeyValue / AnyValue
// ---------------------------------------------------------------------

/// Go: `KeyValue`.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub value: AnyValue,
}

/// Decodes a `KeyValue` message, applying upstream's entry-skip rule.
///
/// Returns `Ok(None)` when `key` (field 1) is absent, or when `value`
/// (field 2) is structurally absent from the wire — both `pb.go`'s
/// `decodeKeyValue` (label path) and `pb_json.go`'s `decodeKeyValueToJSON`
/// (JSON path) drop the entry entirely in either case. Returns
/// `Ok(Some(..))` with `value: AnyValue::Unset` when the `value` sub-message
/// is present but empty (no `oneof` case matched).
///
/// Go: `decodeKeyValue` / `decodeKeyValueToJSON`.
fn decode_key_value(src: &[u8]) -> Result<Option<KeyValue>, WireError> {
    let mut key: Option<String> = None;
    let mut value_data: Option<&[u8]> = None;
    let mut r = WireReader::new(src);
    while !r.is_eof() {
        let (field_num, wire_type) = r.read_tag()?;
        match field_num {
            1 => {
                if wire_type != 2 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                let raw = r.read_len_delim()?;
                key = Some(String::from_utf8_lossy(raw).into_owned());
            }
            2 => {
                if wire_type != 2 {
                    return Err(WireError::InvalidWireType(wire_type));
                }
                value_data = Some(r.read_len_delim()?);
            }
            _ => r.skip(wire_type)?,
        }
    }
    let (Some(key), Some(value_data)) = (key, value_data) else {
        return Ok(None);
    };
    let value = AnyValue::unmarshal(value_data)?;
    Ok(Some(KeyValue { key, value }))
}

/// Decodes a `repeated KeyValue` list, dropping entries per
/// [`decode_key_value`]'s skip rule.
fn decode_key_value_list(src: &[u8], dst: &mut Vec<KeyValue>) -> Result<(), WireError> {
    if let Some(kv) = decode_key_value(src)? {
        dst.push(kv);
    }
    Ok(())
}

/// Go: `AnyValue`, a protobuf `oneof`.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum AnyValue {
    /// No `oneof` case was set (an empty `AnyValue` message).
    #[default]
    Unset,
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Array(ArrayValue),
    KeyValueList(KeyValueList),
    Bytes(Vec<u8>),
}

impl AnyValue {
    /// Field map: `oneof value { 1: string, 2: bool, 3: int64, 4: double,
    /// 5: ArrayValue, 6: KeyValueList, 7: bytes }`.
    ///
    /// If more than one case is present on the wire (malformed input), the
    /// *last* one encountered wins — see the module-level deviations note.
    ///
    /// Go: `decodeAnyValue` (label path). Structurally identical to
    /// `pb_json.go`'s `decodeAnyValueToJSON`, modulo where each case's
    /// result is stored.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut value = AnyValue::Unset;
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let raw = r.read_len_delim()?;
                    value = AnyValue::String(String::from_utf8_lossy(raw).into_owned());
                }
                2 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    value = AnyValue::Bool(r.read_bool()?);
                }
                3 => {
                    if wire_type != 0 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    value = AnyValue::Int(r.read_int64()?);
                }
                4 => {
                    if wire_type != 1 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    value = AnyValue::Double(r.read_double()?);
                }
                5 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    value = AnyValue::Array(ArrayValue::unmarshal(data)?);
                }
                6 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    value = AnyValue::KeyValueList(KeyValueList::unmarshal(data)?);
                }
                7 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    value = AnyValue::Bytes(r.read_len_delim()?.to_vec());
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(value)
    }

    /// Renders this value to a single `String`, for use as an attribute /
    /// label value.
    ///
    /// This does **not** correspond to one single upstream function:
    /// - `Unset`/`String`/`Bool`/`Int`/`Double`/`Bytes`/`Array` match
    ///   `pb.go`'s `decodeAnyValue` exactly: bool via `strconv.FormatBool`,
    ///   int via `strconv.AppendInt(_, 10)`, double via
    ///   `strconv.AppendFloat(_, 'f', -1, 64)`, bytes via
    ///   `base64.StdEncoding`, arrays JSON-encoded through
    ///   `pb_json.go`'s `decodeArrayValueToJSON`.
    /// - `KeyValueList` has **no** single-string upstream equivalent: at the
    ///   top level (an attribute's value being exactly a `KeyValueList`),
    ///   `pb.go` flattens it into multiple dotted-prefix labels
    ///   (`decodeKeyValueList`) rather than one string. Since this task
    ///   produces an AST rather than flattened labels, this method instead
    ///   renders it the same way `pb_json.go` renders a `KeyValueList`
    ///   *nested inside an array* — as a JSON object. Flattening to
    ///   multiple labels is deferred to the downstream conversion task.
    pub fn format_string(&self) -> String {
        match self {
            AnyValue::Unset => String::new(),
            AnyValue::String(s) => s.clone(),
            AnyValue::Bool(b) => b.to_string(),
            AnyValue::Int(i) => i.to_string(),
            AnyValue::Double(d) => format_float(*d),
            AnyValue::Bytes(b) => base64_encode(b),
            AnyValue::Array(_) | AnyValue::KeyValueList(_) => {
                serde_json::Value::from(self).to_string()
            }
        }
    }
}

/// Go: `strconv.AppendFloat(_, v, 'f', -1, 64)` — fixed-point notation with
/// the shortest digit sequence that round-trips, no trailing `.0` for
/// whole numbers. Rust's `f64` `Display` produces the same shortest
/// round-trip fixed-notation digits for finite values; `NaN`/`Inf` are
/// special-cased to match Go's textual spelling.
///
/// `pub(super)` so `opentelemetry::convert` (this task's label-value
/// formatter, which needs the exact same 'f'-format for top-level scalar
/// attribute values / `le` / `quantile` labels) can reuse it instead of
/// duplicating it.
pub(super) fn format_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    }
}

/// Hand-rolled standard-alphabet (RFC 4648 `base64.StdEncoding`), padded
/// base64 encoder — matches Go's `base64.StdEncoding.AppendEncode` used by
/// `fmtBuffer.formatBase64`. Not reused from `esm-insert::common`, which
/// implements the unpadded *URL-safe* alphabet (`base64.RawURLEncoding`)
/// for a different purpose (decoding Pushgateway-style label segments).
///
/// `pub(super)` so `opentelemetry::convert` can reuse it for `bytes_value`
/// attribute label rendering.
pub(super) fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

impl From<&AnyValue> for serde_json::Value {
    /// Go: `pb_json.go`'s `decodeAnyValueToJSON` (applied to an
    /// already-decoded [`AnyValue`] rather than raw wire bytes).
    fn from(v: &AnyValue) -> Self {
        match v {
            AnyValue::Unset => serde_json::Value::Null,
            AnyValue::String(s) => serde_json::Value::String(s.clone()),
            AnyValue::Bool(b) => serde_json::Value::Bool(*b),
            AnyValue::Int(i) => serde_json::Value::Number((*i).into()),
            AnyValue::Double(d) => serde_json::Number::from_f64(*d)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            AnyValue::Bytes(b) => serde_json::Value::String(base64_encode(b)),
            AnyValue::Array(arr) => {
                serde_json::Value::Array(arr.values.iter().map(serde_json::Value::from).collect())
            }
            AnyValue::KeyValueList(kvl) => serde_json::Value::Object(
                kvl.values
                    .iter()
                    .map(|kv| (kv.key.clone(), serde_json::Value::from(&kv.value)))
                    .collect(),
            ),
        }
    }
}

/// Go: `ArrayValue`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ArrayValue {
    pub values: Vec<AnyValue>,
}

impl ArrayValue {
    /// Field map: `{1: repeated AnyValue}`. Every element is kept, even an
    /// empty `AnyValue` (renders as JSON `null` via
    /// [`AnyValue::format_string`]'s `Unset` case) — matches
    /// `decodeArrayValueToJSON`, which always appends one JSON value per
    /// wire occurrence of field 1.
    ///
    /// Go: `decodeArrayValueToJSON`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut arr = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    arr.values.push(AnyValue::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(arr)
    }
}

/// Go: `KeyValueList`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct KeyValueList {
    pub values: Vec<KeyValue>,
}

impl KeyValueList {
    /// Field map: `{1: repeated KeyValue}`, entries dropped per
    /// [`decode_key_value`]'s skip rule.
    ///
    /// Go: `decodeKeyValueList` / `decodeKeyValueListToJSON`.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut kvl = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    decode_key_value_list(data, &mut kvl.values)?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(kvl)
    }
}

// ---------------------------------------------------------------------
// Scope
// ---------------------------------------------------------------------

/// Go: `ScopeMetrics`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ScopeMetrics {
    pub scope: Option<InstrumentationScope>,
    pub metrics: Vec<Metric>,
}

impl ScopeMetrics {
    /// Field map: `{1: InstrumentationScope, 2: repeated Metric}`.
    ///
    /// Go: `decoderContext.decodeScopeMetrics` (the `DisableScopeMetadata`
    /// option that method also accepts is downstream conversion policy).
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut sm = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    sm.scope = Some(InstrumentationScope::unmarshal(data)?);
                }
                2 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    sm.metrics.push(Metric::unmarshal(data)?);
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(sm)
    }
}

/// Go: `InstrumentationScope`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct InstrumentationScope {
    pub name: Option<String>,
    pub version: Option<String>,
    pub attributes: Vec<KeyValue>,
}

impl InstrumentationScope {
    /// Field map: `{1: string name, 2: string version, 3: repeated
    /// KeyValue attributes}`.
    ///
    /// Go: `decoderContext.decodeInstrumentationScope` (the `"unknown"`
    /// fallback and `scope.name`/`scope.version` label emission there are
    /// downstream conversion behavior; decode only captures presence).
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut is = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let raw = r.read_len_delim()?;
                    is.name = Some(String::from_utf8_lossy(raw).into_owned());
                }
                2 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let raw = r.read_len_delim()?;
                    is.version = Some(String::from_utf8_lossy(raw).into_owned());
                }
                3 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    decode_key_value_list(data, &mut is.attributes)?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(is)
    }
}

// ---------------------------------------------------------------------
// Metric
// ---------------------------------------------------------------------

/// Go: `Metric`. Exactly one of `gauge`/`sum`/`histogram`/
/// `exponential_histogram`/`summary` is expected to be set (protobuf
/// `oneof data`); all are modeled as independent `Option`s, matching the Go
/// struct's independent pointer fields (if more than one is present on the
/// wire — malformed input — all of them decode; this mirrors `pb.go`, whose
/// decode loop runs each `case` independently rather than enforcing
/// mutual exclusion).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Metric {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub gauge: Option<Gauge>,
    pub sum: Option<Sum>,
    pub histogram: Option<Histogram>,
    pub exponential_histogram: Option<ExponentialHistogram>,
    pub summary: Option<Summary>,
    pub metadata: Vec<KeyValue>,
}

impl Metric {
    /// Field map: `{1: string name, 2: string description, 3: string unit,
    /// 5: Gauge, 7: Sum, 9: Histogram, 10: ExponentialHistogram,
    /// 11: Summary, 12: repeated KeyValue metadata}`.
    ///
    /// Go: `decoderContext.decodeMetric`. Upstream treats a missing `name`
    /// as an error (`missing metric name`); this port does not — an absent
    /// name simply decodes to `""`, since surfacing that as a hard error is
    /// downstream ingestion policy, not a wire-decode concern.
    pub fn unmarshal(src: &[u8]) -> Result<Self, WireError> {
        let mut m = Self::default();
        let mut r = WireReader::new(src);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag()?;
            match field_num {
                1 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let raw = r.read_len_delim()?;
                    m.name = String::from_utf8_lossy(raw).into_owned();
                }
                2 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let raw = r.read_len_delim()?;
                    m.description = String::from_utf8_lossy(raw).into_owned();
                }
                3 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let raw = r.read_len_delim()?;
                    m.unit = String::from_utf8_lossy(raw).into_owned();
                }
                5 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    m.gauge = Some(Gauge::unmarshal(data)?);
                }
                7 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    m.sum = Some(Sum::unmarshal(data)?);
                }
                9 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    m.histogram = Some(Histogram::unmarshal(data)?);
                }
                10 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    m.exponential_histogram = Some(ExponentialHistogram::unmarshal(data)?);
                }
                11 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    m.summary = Some(Summary::unmarshal(data)?);
                }
                12 => {
                    if wire_type != 2 {
                        return Err(WireError::InvalidWireType(wire_type));
                    }
                    let data = r.read_len_delim()?;
                    decode_key_value_list(data, &mut m.metadata)?;
                }
                _ => r.skip(wire_type)?,
            }
        }
        Ok(m)
    }
}

// `Gauge`/`Sum`/`NumberDataPoint` and the `Histogram`/`ExponentialHistogram`/
// `Summary` message families live in `pb_datapoints.rs` — this file alone
// would exceed the project's 800-line-per-file guideline. `pb_datapoints`
// is a child module of this one (declared below) so it can reach this
// file's private `decode_key_value_list` helper via `super::`; its types
// are re-exported so callers still see them as `pb::Gauge`, `pb::Summary`,
// etc.
mod pb_datapoints;
pub use pb_datapoints::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_encode_matches_known_vectors() {
        // https://datatracker.ietf.org/doc/html/rfc4648#section-10
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn format_float_matches_go_fixed_notation() {
        assert_eq!(format_float(5.0), "5");
        assert_eq!(format_float(3.25), "3.25");
        assert_eq!(format_float(-0.5), "-0.5");
        assert_eq!(format_float(f64::NAN), "NaN");
        assert_eq!(format_float(f64::INFINITY), "+Inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
    }
}
