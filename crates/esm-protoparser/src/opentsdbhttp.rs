//! OpenTSDB HTTP `/api/put` JSON protocol parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/opentsdbhttp/parser.go` plus
//! `lib/protoparser/opentsdbhttp/stream/streamparser.go`.
//!
//! See <http://opentsdb.net/docs/build/html/api_http/put.html>. The request
//! body is a single JSON object or a JSON array of objects, each shaped
//! `{"metric": "...", "timestamp": ..., "value": ..., "tags": {...}}`.
//! Unlike [`crate::opentsdb`] (telnet `put` lines, zero-copy `&str`
//! borrows), rows here are fully owned (`String`/`Vec<Tag>`): JSON parsing
//! via `serde_json::Value` allocates anyway, exactly like [`crate::vmimport`].
//!
//! # Deviations from the Go original
//!
//! - Go's `fastjson` parser is deliberately lenient about numeric/string
//!   value coercion but still strict about JSON *syntax*; `serde_json` is
//!   fully standards-compliant, so there is no `serde_json` analogue of the
//!   bare `Inf`/`NaN` literal-token quirk documented in `crate::vmimport`
//!   (OpenTSDB HTTP values/timestamps never use those spellings — special
//!   float values aren't part of this protocol's semantics at all).
//! - Two distinct failure tiers are ported faithfully, matching upstream
//!   exactly (see `parser_test.go`'s `TestRowsUnmarshalFailure` fixtures
//!   `f("1")`/`f("null")`/`f(`"foo"`)`): a **top-level JSON syntax error**
//!   (`serde_json::from_slice` itself fails) is a real request-level error
//!   ([`Error::Unmarshal`], mapped to HTTP 400 by the insert handler); a
//!   **syntactically valid JSON value that isn't an object or array** (e.g.
//!   a bare number, string, or `null`) is *not* an error at all — it is
//!   logged via `err_logger` and yields zero rows, exactly like Go's
//!   `unmarshalRows`'s `default` switch arm, which only increments
//!   `invalidLines` and returns the unchanged (possibly empty) row slice
//!   without an error the HTTP layer ever sees.
//! - `-opentsdbhttpTrimTimestamp` defaults to `1ms`, and the Go trimming
//!   loop only fires when the flag exceeds `1ms`, so it is a no-op at the
//!   default value; not ported (same reasoning as `crate::opentsdb_stream`'s
//!   `-opentsdbTrimTimestamp` cut, just a different default/threshold).
//! - The current time is taken from [`std::time::SystemTime`] instead of the
//!   Go `fasttime` cached clock (same deviation as the other stream ports in
//!   this crate).
//! - `opentsdbhttp.maxInsertRequestSize` defaults to 32 MiB — the same value
//!   as [`crate::util::MAX_INSERT_REQUEST_SIZE`] (the shared
//!   `-maxInsertRequestSize` default used by `vmimport`/OTLP/etc.), so that
//!   constant is reused directly rather than redeclared under a new name.
//! - No `vm_rows_invalid_total`/`vm_protoparser_rows_read_total` metrics
//!   counters, and no `Rows`/tag-pool object pooling across requests (same
//!   gaps as the other ported parsers in this crate).

use std::fmt;
use std::io::{self, Read};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::util::{self, UtilError, MAX_INSERT_REQUEST_SIZE};

/// Parse error for a single JSON object. Never surfaced publicly:
/// [`Rows::unmarshal`] only passes the formatted message to `err_logger` and
/// skips the entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParseError(String);

impl ParseError {
    fn new(msg: impl Into<String>) -> Self {
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

/// An OpenTSDB HTTP tag (a key/value pair from the `tags` JSON object).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// A single OpenTSDB HTTP row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    pub metric: String,
    pub tags: Vec<Tag>,
    pub value: f64,
    pub timestamp: i64,
}

impl Row {
    fn reset(&mut self) {
        self.metric.clear();
        self.tags.clear();
        self.value = 0.0;
        self.timestamp = 0;
    }
}

/// Parsed OpenTSDB HTTP rows.
///
/// Reusable across [`Rows::unmarshal`] calls: row structs (and their `tags`
/// `Vec`s) keep their capacity across calls, mirroring the Go
/// `tagsPool`/`Rows.Rows` reuse (in spirit, not the exact slab-allocation
/// shape).
#[derive(Debug, Default)]
pub struct Rows {
    // Rows beyond `len` are retained for reuse.
    rows: Vec<Row>,
    len: usize,
}

impl Rows {
    /// Returns the parsed rows.
    pub fn rows(&self) -> &[Row] {
        &self.rows[..self.len]
    }

    /// Returns the parsed rows, mutably (used by [`parse_stream`] to fill in
    /// default/sentinel timestamps after unmarshaling).
    pub fn rows_mut(&mut self) -> &mut [Row] {
        &mut self.rows[..self.len]
    }

    /// Resets `self`.
    pub fn reset(&mut self) {
        self.len = 0;
    }

    /// Unmarshals OpenTSDB HTTP rows from `v`.
    ///
    /// See <http://opentsdb.net/docs/build/html/api_http/put.html>
    ///
    /// `v` must be a JSON object (a single row) or a JSON array (of rows);
    /// any other JSON value is logged via `err_logger` and yields zero rows
    /// (see the module doc's "Deviations" section — this is *not* an error).
    /// Invalid entries within an object/array are skipped individually; the
    /// formatted error is passed to `err_logger` for each one.
    pub fn unmarshal(&mut self, v: &serde_json::Value, mut err_logger: impl FnMut(&str)) {
        self.reset();
        match v {
            serde_json::Value::Object(_) => self.unmarshal_row(v, &mut err_logger),
            serde_json::Value::Array(items) => {
                for item in items {
                    self.unmarshal_row(item, &mut err_logger);
                }
            }
            other => {
                err_logger(&format!(
                    "OpenTSDB JSON must be either object or array; got {}; body={other}",
                    json_type_name(other)
                ));
            }
        }
    }

    fn unmarshal_row(&mut self, o: &serde_json::Value, err_logger: &mut impl FnMut(&str)) {
        if self.len < self.rows.len() {
            self.rows[self.len].reset();
        } else {
            self.rows.push(Row::default());
        }
        self.len += 1;
        let idx = self.len - 1;
        if let Err(err) = unmarshal_single_row(&mut self.rows[idx], o) {
            self.len -= 1;
            err_logger(&format!("cannot unmarshal OpenTSDB object {o}: {err}"));
        }
    }
}

fn unmarshal_single_row(r: &mut Row, o: &serde_json::Value) -> Result<()> {
    r.reset();
    let metric = o.get("metric").and_then(|v| v.as_str()).unwrap_or("");
    if metric.is_empty() {
        return Err(ParseError(format!("missing `metric` in {o}")));
    }
    r.metric = metric.to_owned();

    match o.get("timestamp") {
        None => {
            // Allow missing timestamp. It is automatically populated with
            // the current time by `parse_stream`'s `fixup_timestamps`.
            r.timestamp = 0;
        }
        Some(raw_ts) => {
            let ts = get_float64(raw_ts)
                .map_err(|err| ParseError(format!("invalid `timestamp` in {o}: {err}")))?;
            r.timestamp = ts as i64;
        }
    }

    let raw_v = o
        .get("value")
        .ok_or_else(|| ParseError(format!("missing `value` in {o}")))?;
    r.value =
        get_float64(raw_v).map_err(|err| ParseError(format!("invalid `value` in {o}: {err}")))?;

    if let Some(vt) = o.get("tags") {
        let obj = vt
            .as_object()
            .ok_or_else(|| ParseError(format!("invalid `tags` in {o}: not an object")))?;
        for (key, val) in obj {
            let value = val.as_str().ok_or_else(|| {
                ParseError(format!(
                    "cannot parse tags {vt}: tag value must be string; got {val}"
                ))
            })?;
            if key.is_empty() || value.is_empty() {
                // Skip empty tags, matching upstream `unmarshalTags`.
                continue;
            }
            r.tags.push(Tag {
                key: key.clone(),
                value: value.to_owned(),
            });
        }
    }
    Ok(())
}

/// Parses a `value`/`timestamp` JSON field to `f64`, matching Go
/// `getFloat64`: a JSON number parses via its `f64` representation; a JSON
/// string is parsed via [`crate::fastfloat::parse`] (the same strict
/// `fastjson/fastfloat.Parse` port `crate::opentsdb`'s telnet parser uses);
/// any other JSON type (bool, null, array, object) is an error.
fn get_float64(v: &serde_json::Value) -> Result<f64> {
    match v {
        serde_json::Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| ParseError::new("cannot represent JSON number as f64")),
        serde_json::Value::String(s) => crate::fastfloat::parse(s).map_err(ParseError),
        other => Err(ParseError(format!(
            "value doesn't contain float64; it contains {}",
            json_type_name(other)
        ))),
    }
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// Streaming parser. Port of `lib/protoparser/opentsdbhttp/stream/streamparser.go`.
// ---------------------------------------------------------------------------

/// Go: `net/opentsdb/core.Const.SECOND_MASK` - if none of these bits are
/// set, the timestamp is in seconds and needs to be converted to
/// milliseconds; otherwise it is already in milliseconds.
/// See <http://opentsdb.net/docs/javadoc/net/opentsdb/core/Const.html#SECOND_MASK>.
const SECOND_MASK: i64 = 0x7FFF_FFFF_0000_0000;

/// Error returned by [`parse_stream`].
#[derive(Debug)]
pub enum Error {
    /// Cannot read OpenTSDB HTTP request data.
    Io(io::Error),
    /// The `Content-Encoding` value is not recognized.
    UnsupportedEncoding(String),
    /// The (decompressed) body exceeds [`MAX_INSERT_REQUEST_SIZE`] bytes.
    TooBig { limit: usize },
    /// The compressed body could not be decoded.
    Decompress(String),
    /// The body is not valid JSON at all (a real request-level error, unlike
    /// a syntactically valid non-object/array value — see the module doc's
    /// "Deviations" section).
    Unmarshal {
        len: usize,
        source: serde_json::Error,
    },
    /// The row callback returned an error.
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "cannot read OpenTSDB HTTP data from client: {err}"),
            Error::UnsupportedEncoding(enc) => {
                write!(f, "unsupported Content-Encoding: {enc:?}")
            }
            Error::TooBig { limit } => write!(
                f,
                "too big unpacked OpenTSDB HTTP request; mustn't exceed {limit} bytes"
            ),
            Error::Decompress(msg) => {
                write!(f, "cannot decompress OpenTSDB HTTP request: {msg}")
            }
            Error::Unmarshal { len, source } => {
                write!(
                    f,
                    "cannot parse HTTP OpenTSDB json from {len} bytes: {source}"
                )
            }
            Error::Callback(err) => write!(f, "error when processing imported data: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Unmarshal { source, .. } => Some(source),
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
            // `parse_stream`'s `read_uncompressed_data` closure below never
            // returns `Err` itself (JSON-syntax failures are captured
            // out-of-band via `top_level_error` instead, to keep them a
            // distinct `Error::Unmarshal` variant) — unreachable in
            // practice, handled without panicking per this crate's
            // error-handling rules (same precedent as
            // `crate::opentelemetry::convert`'s identical `From<UtilError>`).
            UtilError::Callback(err) => Error::Decompress(err.to_string()),
        }
    }
}

/// Result of the per-request row callback.
pub type CallbackResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Parses OpenTSDB HTTP `/api/put` JSON data from `r` and calls `callback`
/// once with every parsed row.
///
/// Go: `stream.Parse` + `parseData`. The whole (decompressed) body is parsed
/// as a single JSON document, unlike the line-oriented streaming parsers
/// elsewhere in this crate — matching upstream, which reads the full request
/// via `protoparserutil.ReadUncompressedData` before calling `fastjson`'s
/// `ParseBytes` once.
///
/// Row timestamps of `0` (no explicit timestamp) are filled with the current
/// unix time in seconds; timestamps that don't have any `SECOND_MASK` bits
/// set are then converted from seconds to milliseconds — Go: `parseData`'s
/// "Fill in missing timestamps" + "Convert timestamps in seconds to
/// milliseconds if needed" steps.
///
/// The callback must not hold on to the rows after returning; they borrow
/// state that is reused on the next call.
pub fn parse_stream<R: Read>(
    r: R,
    encoding: &str,
    mut err_logger: impl FnMut(&str),
    mut callback: impl FnMut(&[Row]) -> CallbackResult,
) -> std::result::Result<(), Error> {
    let mut rows = Rows::default();
    let mut top_level_error: Option<(usize, serde_json::Error)> = None;
    util::read_uncompressed_data(r, encoding, MAX_INSERT_REQUEST_SIZE, |data| {
        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(v) => rows.unmarshal(&v, |msg| err_logger(msg)),
            Err(err) => top_level_error = Some((data.len(), err)),
        }
        Ok(())
    })?;

    if let Some((len, source)) = top_level_error {
        return Err(Error::Unmarshal { len, source });
    }

    fixup_timestamps(rows.rows_mut());
    if let Err(source) = callback(rows.rows()) {
        return Err(Error::Callback(source));
    }
    Ok(())
}

/// Fills `0` (absent) timestamps with the current unix time in seconds,
/// then converts seconds-granularity timestamps to milliseconds.
fn fixup_timestamps(rows: &mut [Row]) {
    let current_timestamp = current_time_seconds();
    for row in rows.iter_mut() {
        if row.timestamp == 0 {
            row.timestamp = current_timestamp;
        }
    }
    for row in rows.iter_mut() {
        if row.timestamp & SECOND_MASK == 0 {
            row.timestamp *= 1_000;
        }
    }
}

fn current_time_seconds() -> i64 {
    // NOTE: Go uses `fasttime.UnixTimestamp()` (a cached clock updated once
    // per second); SystemTime is used here, matching the other stream ports
    // in this crate.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// Unit tests live in `opentsdbhttp/tests.rs` (a `#[path]`-wired child
// module, so they keep access to this file's private items) — split out to
// keep this file under the 800-line-per-file guideline, following the same
// pattern as `opentelemetry/convert.rs`.
#[cfg(test)]
#[path = "opentsdbhttp/tests.rs"]
mod tests;
