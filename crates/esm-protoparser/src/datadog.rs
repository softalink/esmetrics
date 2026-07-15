//! DataDog `/api/v1/series` and `/api/v2/series` ingestion protocols.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/datadogutil/datadogutil.go`,
//! `lib/protoparser/datadogv1/{parser.go,stream/streamparser.go}`, and
//! `lib/protoparser/datadogv2/{parser.go,stream/streamparser.go}`.
//!
//! # v2 protobuf support (verified against upstream, not assumed)
//!
//! `lib/protoparser/datadogv2/stream/streamparser.go`'s `parseData` switches
//! on the request's `Content-Type` header: `"application/x-protobuf"` calls
//! `datadogv2.UnmarshalProtobuf`, anything else (including absent)  falls to
//! `datadogv2.UnmarshalJSON`. So v2 **does** support protobuf bodies at
//! v1.146.0 — this is ported (see [`v2::Request::unmarshal_protobuf`], which
//! reuses [`crate::wire::WireReader`], the same primitive `lib/prompb`
//! decoding uses). `datadogv1` has no `UnmarshalProtobuf` at all
//! (`lib/protoparser/datadogv1/parser.go` only defines JSON `Unmarshal`), so
//! v1 stays JSON-only, matching upstream exactly.
//!
//! # `-datadog.sanitizeMetricName` (default `true`, verified)
//!
//! `lib/protoparser/datadogutil/datadogutil.go` declares
//! `SanitizeMetricName = flag.Bool("datadog.sanitizeMetricName", true, ...)`
//! — unlike `-graphite.sanitizeMetricName` (default `false`, deliberately
//! *not* ported in `crate::graphite` since skipping it is a no-op at the
//! default), this flag defaults to `true`, so real DataDog traffic is
//! sanitized on every request. [`sanitize_name`] is therefore applied
//! unconditionally by both [`v1::parse_stream`] and [`v2::parse_stream`];
//! only the flag itself (to *disable* sanitizing) is not wired up, matching
//! how other single-purpose toggle flags are handled elsewhere in this
//! crate.
//!
//! # `-datadog.maxInsertRequestSize` (64 MiB, verified)
//!
//! `datadogutil.go`: `flagutil.NewBytes("datadog.maxInsertRequestSize",
//! 64*1024*1024, ...)` — distinct from the shared
//! `-maxInsertRequestSize` 32 MiB default
//! ([`crate::util::MAX_INSERT_REQUEST_SIZE`]) used by vmimport/OTLP/etc.;
//! see [`MAX_INSERT_REQUEST_SIZE`].
//!
//! # Reset-before-parse (`&mut self` API)
//!
//! Go pools `*datadogv1.Request`/`*datadogv2.Request` values
//! (`sync.Pool`-backed `getRequest`/`putRequest` in each `stream` package)
//! and calls `req.reset()` before every `Unmarshal`, specifically to avoid
//! leaking a previous request's field values into the next when a field is
//! absent from the new JSON (see the `TestRequestUnmarshalMissingHost`
//! regression test in both `parser_test.go`s, linked from
//! <https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3432>).
//! [`v1::Request::unmarshal`]/[`v2::Request::unmarshal_json`]/
//! [`v2::Request::unmarshal_protobuf`] all take `&mut self` and clear
//! `series` first, porting that same reset-before-parse behavior (and its
//! test) faithfully, even though this crate doesn't otherwise pool `Request`
//! values across calls (a fresh one is created per [`v1::parse_stream`]/
//! [`v2::parse_stream`] call).
//!
//! # Deviations from the Go original
//!
//! - No object pooling, no `vm_rows_inserted_total`/
//!   `vm_protoparser_rows_read_total`-style metrics counters (same gaps as
//!   the rest of this crate).
//! - Unlike `crate::opentsdbhttp`/`crate::vmimport` (lenient: a
//!   syntactically-valid-but-wrong-shaped value, or an invalid entry, is
//!   logged and skipped without failing the whole request), DataDog's JSON
//!   parsing here mirrors Go's `encoding/json`, which is strict for
//!   genuinely wrong-typed values: **any** non-null shape mismatch anywhere
//!   in the body (wrong top-level type, `series` a string, a non-object
//!   series/point entry, a wrongly-typed field, ...) fails the *entire*
//!   request, matching `json.Unmarshal`'s all-or-nothing behavior — there is
//!   no lenient per-entry skip tier here.
//! - A JSON `null` is the one value `encoding/json` never treats as an
//!   error: it is a no-op at **every** nesting level, not just the top-level
//!   body. `null` into a slice/map/pointer/interface becomes nil; `null`
//!   into a struct field or scalar leaves the zero value. This port matches
//!   that everywhere — a `null` body, `"series":null`, a `null` series/point/
//!   resource entry, and `null` metric/host/device/tags/points/resources/
//!   source_type_name/timestamp/value/point-element fields all parse to the
//!   corresponding zero value without error (see [`expect_string`],
//!   [`expect_f64`], [`expect_i64`], [`expect_array`], and the per-entry
//!   parsers). Only non-null wrong-typed values are rejected.
//! - Error messages are not byte-matched against Go's (upstream's own tests
//!   only assert `err != nil`, never wording), so [`ParseError`] carries a
//!   free-form diagnostic string rather than a byte-identical translation.

use std::fmt;
use std::io::{self, Read};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use regex::Regex;
use serde_json::Value;

use crate::util::{self, UtilError};
use crate::wire::{WireError, WireReader};

/// Go: `-datadog.maxInsertRequestSize` flag default (64 MiB) — see the
/// module doc.
pub const MAX_INSERT_REQUEST_SIZE: usize = 64 * 1024 * 1024;

/// Splits a DataDog tag into `(name, value)`.
///
/// Go: `datadogutil.SplitTag`. A tag without a `:` has no value at all
/// (returns the literal string `"no_label_value"`, not an empty string); a
/// tag starting with `:` has an empty name.
pub fn split_tag(tag: &str) -> (&str, &str) {
    match tag.find(':') {
        Some(n) => (&tag[..n], &tag[n + 1..]),
        None => (tag, "no_label_value"),
    }
}

/// Sanitizes a metric name per DataDog's custom-metric naming rules.
///
/// Go: `datadogutil.SanitizeName`. Applied unconditionally here — see the
/// module doc's `-datadog.sanitizeMetricName` note. Order matters: replace
/// unsupported characters with `_` first, then collapse consecutive `_`,
/// then drop a single `_` immediately before/after a `.`.
pub fn sanitize_name(name: &str) -> String {
    static UNSUPPORTED: OnceLock<Regex> = OnceLock::new();
    static MULTI_UNDERSCORES: OnceLock<Regex> = OnceLock::new();
    static UNDERSCORES_WITH_DOTS: OnceLock<Regex> = OnceLock::new();
    let unsupported = UNSUPPORTED.get_or_init(|| Regex::new(r"[^0-9a-zA-Z_.]+").unwrap());
    let multi_underscores = MULTI_UNDERSCORES.get_or_init(|| Regex::new(r"_+").unwrap());
    let underscores_with_dots =
        UNDERSCORES_WITH_DOTS.get_or_init(|| Regex::new(r"_?\._?").unwrap());
    let s = unsupported.replace_all(name, "_");
    let s = multi_underscores.replace_all(&s, "_");
    underscores_with_dots.replace_all(&s, ".").into_owned()
}

/// Parse error for a malformed DataDog request body. Never byte-matched
/// against Go's wording — see the module doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(String);

impl ParseError {
    fn new(msg: impl Into<String>) -> ParseError {
        ParseError(msg.into())
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseError {}

type Result<T> = std::result::Result<T, ParseError>;

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// Go's `encoding/json` treats a JSON `null` as a no-op for every destination
// type — nil for a slice/map/pointer/interface, the zero value for a struct
// field or scalar — never an error, at any nesting depth (see the module
// doc's null note). The `expect_*` helpers therefore map `null` to the
// destination's zero value, while still strictly rejecting genuinely
// wrong-typed (non-null) values.

fn expect_string(v: &Value, field: &str) -> Result<String> {
    if v.is_null() {
        return Ok(String::new());
    }
    v.as_str()
        .map(str::to_owned)
        .ok_or_else(|| ParseError::new(format!("`{field}` must be a string; got {v}")))
}

fn expect_f64(v: &Value, field: &str) -> Result<f64> {
    if v.is_null() {
        return Ok(0.0);
    }
    v.as_f64()
        .ok_or_else(|| ParseError::new(format!("`{field}` must be a number; got {v}")))
}

fn expect_i64(v: &Value, field: &str) -> Result<i64> {
    if v.is_null() {
        return Ok(0);
    }
    v.as_i64()
        .ok_or_else(|| ParseError::new(format!("`{field}` must be an integer; got {v}")))
}

/// Returns `v`'s array elements, or an empty slice when `v` is JSON `null`
/// (Go's `encoding/json` unmarshals `null` into a slice as nil — a no-op).
/// Errors only for a genuinely non-null, non-array value.
fn expect_array<'a>(v: &'a Value, field: &str) -> Result<&'a [Value]> {
    if v.is_null() {
        return Ok(&[]);
    }
    v.as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| ParseError::new(format!("`{field}` must be an array; got {v}")))
}

fn wire_err(err: WireError) -> ParseError {
    ParseError::new(format!("cannot unmarshal protobuf: {err}"))
}

/// Go: `fasttime.UnixTimestamp()` — a cached, once-per-second clock;
/// [`SystemTime`] is used here instead, matching the other stream ports in
/// this crate.
fn current_time_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Error returned by [`v1::parse_stream`]/[`v2::parse_stream`].
#[derive(Debug)]
pub enum Error {
    /// I/O error while reading from the underlying reader.
    Io(io::Error),
    /// The `Content-Encoding` value is not recognized.
    UnsupportedEncoding(String),
    /// The (decompressed) body exceeds [`MAX_INSERT_REQUEST_SIZE`] bytes.
    TooBig { limit: usize },
    /// The compressed body could not be decoded.
    Decompress(String),
    /// The body could not be unmarshaled (JSON syntax error, wrong
    /// top-level shape, or a protobuf decode failure) — a genuine
    /// request-level error, unlike the lenient shape-mismatch handling in
    /// `crate::opentsdbhttp`/`crate::vmimport` (see the module doc).
    Unmarshal(ParseError),
    /// The row callback returned an error.
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "cannot read DataDog request data from client: {err}"),
            Error::UnsupportedEncoding(enc) => {
                write!(f, "unsupported Content-Encoding: {enc:?}")
            }
            Error::TooBig { limit } => write!(
                f,
                "too big unpacked DataDog request; mustn't exceed {limit} bytes"
            ),
            Error::Decompress(msg) => write!(f, "cannot decompress DataDog request: {msg}"),
            Error::Unmarshal(err) => write!(f, "cannot unmarshal DataDog request: {err}"),
            Error::Callback(err) => write!(f, "error when processing imported data: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Unmarshal(err) => Some(err),
            Error::Callback(err) => Some(err.as_ref()),
            Error::UnsupportedEncoding(_) | Error::TooBig { .. } | Error::Decompress(_) => None,
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
            // The `read_uncompressed_data` closures in `v1::parse_stream`/
            // `v2::parse_stream` never return `Err` themselves (unmarshal
            // failures are captured out-of-band as a distinct
            // `Error::Unmarshal` instead, to keep the two error kinds
            // apart) — unreachable in practice, handled without panicking
            // per this crate's error-handling rules (same precedent as
            // `crate::opentsdbhttp`'s identical `From<UtilError>`).
            UtilError::Callback(err) => Error::Decompress(err.to_string()),
        }
    }
}

/// Result of the per-request row callback.
pub type CallbackResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// `/api/v1/series` types. Go: `lib/protoparser/datadogv1`.
pub mod v1 {
    use super::*;

    /// A single `[timestamp_seconds, value]` point.
    ///
    /// Go: `datadogv1.Point`, a `[2]float64` array — `pt[0]` is the
    /// timestamp in **seconds** (converted to milliseconds only at
    /// conversion/insert time, via `pt.Timestamp()`, not here).
    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    pub struct Point {
        pub timestamp_seconds: f64,
        pub value: f64,
    }

    /// A single series item. Go: `datadogv1.Series`.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Series {
        pub metric: String,
        pub host: String,
        pub device: String,
        pub points: Vec<Point>,
        pub tags: Vec<String>,
    }

    /// A `/api/v1/series` POST request body. Go: `datadogv1.Request`.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Request {
        pub series: Vec<Series>,
    }

    impl Request {
        /// Unmarshals `data` into `self`, clearing any previous contents
        /// first — see the module doc's "Reset-before-parse" section. Point
        /// timestamps that are missing or `<= 0` are filled with the
        /// current unix time in seconds (Go: `Request.Unmarshal`'s "Set
        /// missing timestamps to the current time" step).
        pub fn unmarshal(&mut self, data: &[u8]) -> Result<()> {
            self.series.clear();
            let v: Value = serde_json::from_slice(data)
                .map_err(|err| ParseError::new(format!("cannot unmarshal request body: {err}")))?;
            match v {
                // A JSON `null` body is a no-op, matching Go's
                // `json.Unmarshal(null, *Request)` semantics — see the
                // module doc.
                Value::Null => {}
                Value::Object(map) => {
                    if let Some(series_v) = map.get("series") {
                        for item in expect_array(series_v, "series")? {
                            self.series.push(parse_series(item)?);
                        }
                    }
                }
                other => {
                    return Err(ParseError::new(format!(
                        "expected a JSON object for the request body; got {}",
                        json_type_name(&other)
                    )));
                }
            }
            let current = current_time_seconds() as f64;
            for s in &mut self.series {
                for pt in &mut s.points {
                    if pt.timestamp_seconds <= 0.0 {
                        pt.timestamp_seconds = current;
                    }
                }
            }
            Ok(())
        }
    }

    fn parse_series(v: &Value) -> Result<Series> {
        // A `null` series entry is a no-op (zero-value `Series`), matching
        // `encoding/json` unmarshaling `null` into a struct element.
        if v.is_null() {
            return Ok(Series::default());
        }
        let obj = v
            .as_object()
            .ok_or_else(|| ParseError::new(format!("series entry must be an object; got {v}")))?;
        let mut s = Series::default();
        if let Some(m) = obj.get("metric") {
            s.metric = expect_string(m, "metric")?;
        }
        if let Some(h) = obj.get("host") {
            s.host = expect_string(h, "host")?;
        }
        if let Some(d) = obj.get("device") {
            s.device = expect_string(d, "device")?;
        }
        if let Some(p) = obj.get("points") {
            for item in expect_array(p, "points")? {
                s.points.push(parse_point(item)?);
            }
        }
        if let Some(t) = obj.get("tags") {
            for item in expect_array(t, "tags")? {
                s.tags.push(expect_string(item, "tags[]")?);
            }
        }
        Ok(s)
    }

    /// Go: `[2]float64` array decoding — a shorter array leaves the missing
    /// trailing element(s) at their zero value; a longer array's extra
    /// elements are discarded.
    fn parse_point(v: &Value) -> Result<Point> {
        // A `null` point element is a no-op (zero-value `Point`), matching
        // `encoding/json` unmarshaling `null` into a `[2]float64`.
        if v.is_null() {
            return Ok(Point::default());
        }
        let arr = v
            .as_array()
            .ok_or_else(|| ParseError::new(format!("point must be a 2-element array; got {v}")))?;
        let timestamp_seconds = match arr.first() {
            Some(x) => expect_f64(x, "points[][0]")?,
            None => 0.0,
        };
        let value = match arr.get(1) {
            Some(x) => expect_f64(x, "points[][1]")?,
            None => 0.0,
        };
        Ok(Point {
            timestamp_seconds,
            value,
        })
    }

    /// Parses a DataDog `/api/v1/series` request body from `r` and calls
    /// `callback` once with the parsed series (whole-body JSON, unlike the
    /// line-oriented streaming parsers elsewhere in this crate — matching
    /// upstream, which reads and unmarshals the full request in one shot).
    ///
    /// Go: `stream.Parse` + `parseData`. Sanitizes every series' metric name
    /// unconditionally — see the module doc's `-datadog.sanitizeMetricName`
    /// note.
    pub fn parse_stream<R: Read>(
        r: R,
        encoding: &str,
        mut callback: impl FnMut(&[Series]) -> CallbackResult,
    ) -> std::result::Result<(), super::Error> {
        let mut req = Request::default();
        let mut unmarshal_error = None;
        util::read_uncompressed_data(r, encoding, super::MAX_INSERT_REQUEST_SIZE, |data| {
            if let Err(err) = req.unmarshal(data) {
                unmarshal_error = Some(err);
            }
            Ok(())
        })?;
        if let Some(err) = unmarshal_error {
            return Err(super::Error::Unmarshal(err));
        }
        for s in &mut req.series {
            s.metric = super::sanitize_name(&s.metric);
        }
        callback(&req.series).map_err(super::Error::Callback)
    }
}

/// `/api/v2/series` types. Go: `lib/protoparser/datadogv2`.
pub mod v2 {
    use super::*;

    /// A single point. Go: `datadogv2.Point` — `Timestamp` is in
    /// **seconds** (converted to milliseconds only at conversion/insert
    /// time, via `pt.Timestamp * 1000`, not here).
    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    pub struct Point {
        pub timestamp: i64,
        pub value: f64,
    }

    /// A resource attached to a series (e.g. `{"name":"host1","type":"host"}`).
    /// Go: `datadogv2.Resource`.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Resource {
        pub name: String,
        pub r#type: String,
    }

    /// A single series item. Go: `datadogv2.Series`.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Series {
        pub metric: String,
        pub points: Vec<Point>,
        pub resources: Vec<Resource>,
        pub source_type_name: String,
        pub tags: Vec<String>,
    }

    /// A `/api/v2/series` POST request body. Go: `datadogv2.Request`.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Request {
        pub series: Vec<Series>,
    }

    impl Request {
        /// Unmarshals JSON `data` into `self`. Go: `datadogv2.UnmarshalJSON`
        /// — see [`super::v1::Request::unmarshal`]'s doc for the shared
        /// reset-before-parse and timestamp-fixup semantics (identical
        /// here, just with an `int64`-seconds timestamp instead of a
        /// `float64` one).
        pub fn unmarshal_json(&mut self, data: &[u8]) -> Result<()> {
            self.series.clear();
            let v: Value = serde_json::from_slice(data)
                .map_err(|err| ParseError::new(format!("cannot unmarshal request body: {err}")))?;
            match v {
                Value::Null => {}
                Value::Object(map) => {
                    if let Some(series_v) = map.get("series") {
                        for item in expect_array(series_v, "series")? {
                            self.series.push(parse_series_json(item)?);
                        }
                    }
                }
                other => {
                    return Err(ParseError::new(format!(
                        "expected a JSON object for the request body; got {}",
                        json_type_name(&other)
                    )));
                }
            }
            self.fixup_timestamps();
            Ok(())
        }

        /// Unmarshals a protobuf `Request` message from `data`. Go:
        /// `datadogv2.UnmarshalProtobuf` + `Request.unmarshalProtobuf`.
        ///
        /// ```text
        /// message Request { repeated Series series = 1; }
        /// message Series {
        ///   repeated Resource resources = 1;
        ///   string metric = 2;
        ///   repeated string tags = 3;
        ///   repeated Point points = 4;
        ///   string source_type_name = 7;
        /// }
        /// message Point { double value = 1; int64 timestamp = 2; }
        /// message Resource { string type = 1; string name = 2; }
        /// ```
        /// See <https://github.com/DataDog/agent-payload/blob/d7c5dcc63970d0e19678a342e7718448dd777062/proto/metrics/agent_payload.proto>.
        /// Unknown fields are skipped by wire type, matching easyproto.
        pub fn unmarshal_protobuf(&mut self, data: &[u8]) -> Result<()> {
            self.series.clear();
            let mut r = WireReader::new(data);
            while !r.is_eof() {
                let (field_num, wire_type) = r.read_tag().map_err(wire_err)?;
                match field_num {
                    1 => {
                        if wire_type != 2 {
                            return Err(wire_err(WireError::InvalidWireType(wire_type)));
                        }
                        let bytes = r.read_len_delim().map_err(wire_err)?;
                        self.series.push(parse_series_protobuf(bytes)?);
                    }
                    _ => r.skip(wire_type).map_err(wire_err)?,
                }
            }
            self.fixup_timestamps();
            Ok(())
        }

        fn fixup_timestamps(&mut self) {
            let current = current_time_seconds();
            for s in &mut self.series {
                for pt in &mut s.points {
                    if pt.timestamp <= 0 {
                        pt.timestamp = current;
                    }
                }
            }
        }
    }

    fn parse_series_json(v: &Value) -> Result<Series> {
        // A `null` series entry is a no-op (zero-value `Series`).
        if v.is_null() {
            return Ok(Series::default());
        }
        let obj = v
            .as_object()
            .ok_or_else(|| ParseError::new(format!("series entry must be an object; got {v}")))?;
        let mut s = Series::default();
        if let Some(m) = obj.get("metric") {
            s.metric = expect_string(m, "metric")?;
        }
        if let Some(p) = obj.get("points") {
            for item in expect_array(p, "points")? {
                s.points.push(parse_point_json(item)?);
            }
        }
        if let Some(r) = obj.get("resources") {
            for item in expect_array(r, "resources")? {
                s.resources.push(parse_resource_json(item)?);
            }
        }
        if let Some(stn) = obj.get("source_type_name") {
            s.source_type_name = expect_string(stn, "source_type_name")?;
        }
        if let Some(t) = obj.get("tags") {
            for item in expect_array(t, "tags")? {
                s.tags.push(expect_string(item, "tags[]")?);
            }
        }
        Ok(s)
    }

    fn parse_point_json(v: &Value) -> Result<Point> {
        // A `null` point element is a no-op (zero-value `Point`).
        if v.is_null() {
            return Ok(Point::default());
        }
        let obj = v
            .as_object()
            .ok_or_else(|| ParseError::new(format!("point must be an object; got {v}")))?;
        let timestamp = match obj.get("timestamp") {
            Some(x) => expect_i64(x, "timestamp")?,
            None => 0,
        };
        let value = match obj.get("value") {
            Some(x) => expect_f64(x, "value")?,
            None => 0.0,
        };
        Ok(Point { timestamp, value })
    }

    fn parse_resource_json(v: &Value) -> Result<Resource> {
        // A `null` resource entry is a no-op (zero-value `Resource`).
        if v.is_null() {
            return Ok(Resource::default());
        }
        let obj = v
            .as_object()
            .ok_or_else(|| ParseError::new(format!("resource must be an object; got {v}")))?;
        let mut r = Resource::default();
        if let Some(n) = obj.get("name") {
            r.name = expect_string(n, "name")?;
        }
        if let Some(t) = obj.get("type") {
            r.r#type = expect_string(t, "type")?;
        }
        Ok(r)
    }

    fn parse_series_protobuf(data: &[u8]) -> Result<Series> {
        let mut s = Series::default();
        let mut r = WireReader::new(data);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag().map_err(wire_err)?;
            match field_num {
                2 => s.metric = r.read_len_delim().map_err(wire_err).and_then(to_utf8)?,
                4 => {
                    if wire_type != 2 {
                        return Err(wire_err(WireError::InvalidWireType(wire_type)));
                    }
                    let bytes = r.read_len_delim().map_err(wire_err)?;
                    s.points.push(parse_point_protobuf(bytes)?);
                }
                1 => {
                    if wire_type != 2 {
                        return Err(wire_err(WireError::InvalidWireType(wire_type)));
                    }
                    let bytes = r.read_len_delim().map_err(wire_err)?;
                    s.resources.push(parse_resource_protobuf(bytes)?);
                }
                7 => s.source_type_name = r.read_len_delim().map_err(wire_err).and_then(to_utf8)?,
                3 => s
                    .tags
                    .push(r.read_len_delim().map_err(wire_err).and_then(to_utf8)?),
                _ => r.skip(wire_type).map_err(wire_err)?,
            }
        }
        Ok(s)
    }

    fn parse_point_protobuf(data: &[u8]) -> Result<Point> {
        let mut pt = Point::default();
        let mut r = WireReader::new(data);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag().map_err(wire_err)?;
            match field_num {
                1 => pt.value = r.read_double().map_err(wire_err)?,
                2 => pt.timestamp = r.read_int64().map_err(wire_err)?,
                _ => r.skip(wire_type).map_err(wire_err)?,
            }
        }
        Ok(pt)
    }

    fn parse_resource_protobuf(data: &[u8]) -> Result<Resource> {
        let mut res = Resource::default();
        let mut r = WireReader::new(data);
        while !r.is_eof() {
            let (field_num, wire_type) = r.read_tag().map_err(wire_err)?;
            match field_num {
                1 => res.r#type = r.read_len_delim().map_err(wire_err).and_then(to_utf8)?,
                2 => res.name = r.read_len_delim().map_err(wire_err).and_then(to_utf8)?,
                _ => r.skip(wire_type).map_err(wire_err)?,
            }
        }
        Ok(res)
    }

    /// Go's easyproto `FieldContext.String()` does no UTF-8 validation
    /// (`unsafe` byte-to-string cast, same as `crate::prompb`'s doc notes);
    /// a lossy conversion is the closest safe equivalent here.
    fn to_utf8(bytes: &[u8]) -> Result<String> {
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }

    /// Parses a DataDog `/api/v2/series` request body from `r` and calls
    /// `callback` once with the parsed series. `content_type` selects the
    /// wire format: `"application/x-protobuf"` decodes protobuf, anything
    /// else (including absent, i.e. `""`) decodes JSON — matching Go
    /// `stream.parseData`'s `switch contentType` (see the module doc's
    /// protobuf-support note).
    ///
    /// Go: `stream.Parse` + `parseData`. Sanitizes every series' metric name
    /// unconditionally — see the module doc's `-datadog.sanitizeMetricName`
    /// note.
    pub fn parse_stream<R: Read>(
        r: R,
        encoding: &str,
        content_type: &str,
        mut callback: impl FnMut(&[Series]) -> CallbackResult,
    ) -> std::result::Result<(), super::Error> {
        let mut req = Request::default();
        let mut unmarshal_error = None;
        util::read_uncompressed_data(r, encoding, super::MAX_INSERT_REQUEST_SIZE, |data| {
            let result = if content_type == "application/x-protobuf" {
                req.unmarshal_protobuf(data)
            } else {
                req.unmarshal_json(data)
            };
            if let Err(err) = result {
                unmarshal_error = Some(err);
            }
            Ok(())
        })?;
        if let Some(err) = unmarshal_error {
            return Err(super::Error::Unmarshal(err));
        }
        for s in &mut req.series {
            s.metric = super::sanitize_name(&s.metric);
        }
        callback(&req.series).map_err(super::Error::Callback)
    }
}

// Unit tests live in `datadog/tests.rs` (a `#[path]`-wired child module, so
// they keep access to this file's private items) — split out from the start
// to keep this file under the 800-line-per-file guideline, following the
// same pattern as `opentsdbhttp.rs`/`opentelemetry/convert.rs`.
#[cfg(test)]
#[path = "datadog/tests.rs"]
mod tests;
