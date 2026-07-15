//! `/api/v1/import` VictoriaMetrics JSON-lines parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0 `lib/protoparser/vmimport/parser.go`
//! plus `lib/protoparser/vmimport/stream/streamparser.go`.
//!
//! Each line is `{"metric":{...},"values":[...],"timestamps":[...]}`. Unlike
//! `crate::influx`/`crate::prometheus` (which hold zero-copy `&str`/`Cow`
//! slices into the input buffer), rows here are fully owned
//! (`Vec<(Vec<u8>, Vec<u8>)>` tags, `Vec<f64>` values, `Vec<i64>`
//! timestamps): JSON parsing allocates anyway (`serde_json::Value` owns its
//! strings), so there is no zero-copy path to preserve.
//!
//! # Deviations from the Go original
//!
//! - Go parses each line with `valyala/fastjson`, which is deliberately
//!   lenient: it accepts bare (unquoted) `Inf`/`-Inf`/`NaN`/`null` tokens as
//!   JSON literals in addition to standard JSON. `serde_json` is strict
//!   standards-compliant JSON and rejects bare `Inf`/`NaN` tokens outright
//!   (invalid JSON syntax), so a line using them fails to parse at the JSON
//!   level here and the whole row is skipped/logged, whereas upstream would
//!   only reject that specific *value* inside an otherwise-valid line if the
//!   token were unrecognized. The **quoted-string** forms (`"Infinity"`,
//!   `"-Infinity"`, `"NaN"`, `"null"`) and the JSON literal `null` are valid
//!   JSON and are supported identically to upstream (see
//!   [`get_special_float64_from_string`]). This only affects hand-crafted
//!   bodies using fastjson's non-standard literal extension; VictoriaMetrics'
//!   own `/api/v1/export` output always quotes these (see the values test
//!   fixtures), so real exported data round-trips unaffected.
//! - `tagsUnmarshaler.unmarshalTags` in Go has a dead condition
//!   (`err != nil && tu.err != nil`, where `tu.err` is reset to `nil` right
//!   before the loop) that can never evaluate true on the first error, so a
//!   non-string tag value in the `metric` object never actually surfaces a
//!   parse error upstream — it silently becomes an empty-bytes tag value.
//!   Ported verbatim (see [`unmarshal_single_row`]) for behavioral fidelity,
//!   even though it looks like a bug.
//! - `import.maxLineLen` (10 MiB) is a fixed constant here
//!   ([`MAX_LINE_LEN`]) rather than a runtime flag.
//! - No `vm_rows_invalid_total`/`vm_protoparser_rows_read_total` metrics
//!   counters (same gap as the other ported parsers in this crate).
//! - The current time / concurrent unmarshal-work scheduling from Go's
//!   `sync.Pool`-based `unmarshalWork` is not ported; each block is
//!   unmarshaled inline (same simplification as `crate::stream` and
//!   `crate::prometheus_stream`).

use std::fmt;
use std::io::{self, Read};

use crate::util::{self, LinesReadError, UtilError};

/// Go: `-import.maxLineLen` flag default (`10*1024*1024`).
pub const MAX_LINE_LEN: usize = 10 * 1024 * 1024;

/// Parse error for a single line. Never surfaced publicly: [`Rows::unmarshal`]
/// only passes the formatted message to `err_logger` and skips the line.
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

/// A single, fully-owned row parsed from one `/api/v1/import` JSON line.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    /// `(key, value)` tag pairs, in JSON object key order (including
    /// `__name__` if present, as an ordinary tag — the insert handler is
    /// responsible for special-casing it into the metric group, matching
    /// how Go's `InsertCtx`/`storage.MetricName` extract it later rather
    /// than the parser doing so).
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
    pub values: Vec<f64>,
    pub timestamps: Vec<i64>,
}

impl Row {
    fn reset(&mut self) {
        self.tags.clear();
        self.values.clear();
        self.timestamps.clear();
    }
}

/// Parsed vmimport rows.
///
/// Reusable across [`Rows::unmarshal`] calls: row structs (and their tag/
/// value/timestamp `Vec`s) keep their capacity across calls, mirroring the
/// Go `tagsPool` reuse (in spirit, not in the exact slab-allocation shape).
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

    /// Resets `self`.
    pub fn reset(&mut self) {
        self.len = 0;
    }

    /// Unmarshals `/api/v1/import` JSON-lines rows from `s`.
    ///
    /// See <https://docs.victoriametrics.com/#how-to-import-time-series-data>
    ///
    /// Invalid lines are skipped; the formatted error is passed to
    /// `err_logger` for each one (Go: `unmarshalRow`'s `logger.Errorf` +
    /// `invalidLines.Inc()`). Empty lines are skipped silently, with no
    /// callback (matching Go: only a truly-empty line, after trimming a
    /// trailing `\r`, is skipped without invoking the JSON parser at all).
    pub fn unmarshal(&mut self, s: &str, mut err_logger: impl FnMut(&str)) {
        self.reset();
        let mut rest = s;
        loop {
            match rest.as_bytes().iter().position(|&b| b == b'\n') {
                None => {
                    self.unmarshal_row(rest, &mut err_logger);
                    break;
                }
                Some(n) => {
                    self.unmarshal_row(&rest[..n], &mut err_logger);
                    rest = &rest[n + 1..];
                }
            }
        }
    }

    fn unmarshal_row(&mut self, s: &str, err_logger: &mut impl FnMut(&str)) {
        let s = s.strip_suffix('\r').unwrap_or(s);
        if s.is_empty() {
            // Skip empty line, silently (no error logged).
            return;
        }

        if self.len < self.rows.len() {
            self.rows[self.len].reset();
        } else {
            self.rows.push(Row::default());
        }
        self.len += 1;
        let idx = self.len - 1;
        if let Err(err) = unmarshal_single_row(&mut self.rows[idx], s) {
            self.len -= 1;
            err_logger(&format!("skipping json line {s:?} because of error: {err}"));
        }
    }
}

fn unmarshal_single_row(r: &mut Row, s: &str) -> Result<()> {
    let v: serde_json::Value = serde_json::from_str(s)
        .map_err(|err| ParseError(format!("cannot parse json line: {err}")))?;

    // Unmarshal tags from the `metric` object.
    let metric = v.get("metric").filter(|m| m.is_object());
    let Some(metric) = metric else {
        return Err(ParseError::new("missing `metric` object"));
    };
    let metric_obj = metric.as_object().expect("checked is_object above");
    for (key, val) in metric_obj {
        // Go's `tagsUnmarshaler.unmarshalTags` has a dead-code error guard
        // (see module doc): a non-string value never surfaces an error and
        // silently becomes an empty-bytes tag value. Ported verbatim.
        let value_bytes: Vec<u8> = match val.as_str() {
            Some(s) => s.as_bytes().to_vec(),
            None => Vec::new(),
        };
        r.tags.push((key.as_bytes().to_vec(), value_bytes));
    }
    if r.tags.is_empty() {
        return Err(ParseError::new("missing tags"));
    }

    // Unmarshal the `values` array.
    let values = v
        .get("values")
        .and_then(|x| x.as_array())
        .filter(|a| !a.is_empty());
    let Some(values) = values else {
        return Err(ParseError::new("missing `values` array"));
    };
    for (i, item) in values.iter().enumerate() {
        let f = get_value_float64(item)
            .map_err(|err| ParseError(format!("cannot unmarshal value at position {i}: {err}")))?;
        r.values.push(f);
    }

    // Unmarshal the `timestamps` array.
    let timestamps = v
        .get("timestamps")
        .and_then(|x| x.as_array())
        .filter(|a| !a.is_empty());
    let Some(timestamps) = timestamps else {
        return Err(ParseError::new("missing `timestamps` array"));
    };
    for (i, item) in timestamps.iter().enumerate() {
        let ts = item.as_i64().ok_or_else(|| {
            ParseError(format!(
                "cannot unmarshal timestamp at position {i}: not an integer"
            ))
        })?;
        r.timestamps.push(ts);
    }

    if r.timestamps.len() != r.values.len() {
        return Err(ParseError(format!(
            "`timestamps` array size must match `values` array size; got {}; want {}",
            r.timestamps.len(),
            r.values.len()
        )));
    }
    Ok(())
}

/// Parses one `values[]` array entry to `f64`, matching Go
/// `getSpecialFloat64`: a JSON number always parses; a JSON `null` becomes
/// `NaN`; a JSON string is parsed via [`get_special_float64_from_string`];
/// any other JSON type (bool, array, object) is an error.
fn get_value_float64(v: &serde_json::Value) -> Result<f64> {
    match v {
        serde_json::Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| ParseError::new("cannot represent JSON number as f64")),
        serde_json::Value::Null => Ok(f64::NAN),
        serde_json::Value::String(s) => get_special_float64_from_string(s),
        other => Err(ParseError(format!("unsupported value type: {other:?}"))),
    }
}

/// Parses a special float string, matching Go `getSpecialFloat64FromString`
/// exactly: a leading `-` is stripped (tracked, not required to apply to
/// every case — see the `nan`/`null` arm below, which ignores it just like
/// upstream), then the remainder is compared case-sensitively against a
/// fixed set of accepted spellings.
fn get_special_float64_from_string(s: &str) -> Result<f64> {
    let (minus, rest) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    match rest {
        "infinity" | "Infinity" | "Inf" | "inf" => Ok(if minus {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }),
        // Note: the minus sign (if any) is intentionally dropped here,
        // matching upstream: "-nan" and "-null" both parse to plain NaN.
        "null" | "Null" | "nan" | "NaN" => Ok(f64::NAN),
        _ => Err(ParseError(format!("unsupported string: {s:?}"))),
    }
}

// ---------------------------------------------------------------------------
// Streaming parser. Port of `lib/protoparser/vmimport/stream/streamparser.go`.
// ---------------------------------------------------------------------------

/// Error returned by [`parse_stream`].
#[derive(Debug)]
pub enum Error {
    /// Cannot read vmimport data.
    Io(io::Error),
    /// A single line exceeds the maximum allowed size ([`MAX_LINE_LEN`]).
    TooLongLine { max_line_len: usize },
    /// The request body is not valid UTF-8.
    Utf8(std::str::Utf8Error),
    /// The body could not be decoded per its `Content-Encoding`.
    Decode(String),
    /// The row callback returned an error.
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "cannot read vmimport data: {err}"),
            Error::TooLongLine { max_line_len } => {
                write!(f, "too long line: more than {max_line_len} bytes")
            }
            Error::Utf8(err) => write!(f, "vmimport data is not valid UTF-8: {err}"),
            Error::Decode(msg) => write!(f, "cannot decode vmimport data: {msg}"),
            Error::Callback(err) => write!(f, "error when processing imported data: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Utf8(err) => Some(err),
            Error::Callback(err) => Some(err.as_ref()),
            Error::TooLongLine { .. } | Error::Decode(_) => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<LinesReadError> for Error {
    fn from(err: LinesReadError) -> Self {
        match err {
            LinesReadError::Io(err) => Error::Io(err),
            LinesReadError::TooLongLine { max_line_len } => Error::TooLongLine { max_line_len },
        }
    }
}

/// Result of the per-batch row callback.
pub type CallbackResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Parses `/api/v1/import` JSON-lines data from `r` in stream mode and calls
/// `callback` for each parsed block of rows.
///
/// Go: `stream.Parse`. Invalid lines are skipped (stream-mode semantics);
/// each skipped line's formatted error is passed to `err_logger`.
///
/// Unlike `crate::influx`/`crate::prometheus`, the callback's rows do not
/// borrow the read buffer (vmimport rows are fully owned), so there is no
/// lifetime constraint tying the callback to the current block.
pub fn parse_stream<R: Read>(
    r: R,
    encoding: &str,
    mut err_logger: impl FnMut(&str),
    mut callback: impl FnMut(&[Row]) -> CallbackResult,
) -> std::result::Result<(), Error> {
    let reader = util::uncompressed_reader(r, encoding).map_err(|err| match err {
        UtilError::UnsupportedEncoding(enc) => {
            Error::Decode(format!("unsupported Content-Encoding: {enc:?}"))
        }
        other => Error::Decode(other.to_string()),
    })?;
    parse_stream_internal(reader, &mut err_logger, &mut callback)
}

fn parse_stream_internal<R, F>(
    mut r: R,
    err_logger: &mut impl FnMut(&str),
    callback: &mut F,
) -> std::result::Result<(), Error>
where
    R: Read,
    F: FnMut(&[Row]) -> CallbackResult,
{
    let mut req_buf: Vec<u8> = Vec::new();
    let mut tail_buf: Vec<u8> = Vec::new();
    while util::read_lines_block(&mut r, &mut req_buf, &mut tail_buf, MAX_LINE_LEN)? {
        let block = std::str::from_utf8(&req_buf).map_err(Error::Utf8)?;
        let mut rows = Rows::default();
        rows.unmarshal(block, |msg| err_logger(msg));
        if let Err(source) = callback(rows.rows()) {
            return Err(Error::Callback(source));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(tags: &[(&str, &str)], values: &[f64], timestamps: &[i64]) -> Row {
        Row {
            tags: tags
                .iter()
                .map(|&(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .collect(),
            values: values.to_vec(),
            timestamps: timestamps.to_vec(),
        }
    }

    fn assert_values_eq(got: &[f64], want: &[f64]) {
        assert_eq!(
            got.len(),
            want.len(),
            "value count mismatch: {got:?} vs {want:?}"
        );
        for (i, (&g, &w)) in got.iter().zip(want.iter()).enumerate() {
            if w.is_nan() {
                assert!(g.is_nan(), "position {i}: expected NaN, got {g}");
            } else {
                assert_eq!(g, w, "position {i}");
            }
        }
    }

    fn unmarshal_ok(s: &str, expected: &[Row]) {
        let mut rows = Rows::default();
        rows.unmarshal(s, |msg| panic!("unexpected error for {s:?}: {msg}"));
        assert_eq!(rows.rows().len(), expected.len(), "row count for {s:?}");
        for (got, want) in rows.rows().iter().zip(expected.iter()) {
            assert_eq!(got.tags, want.tags, "tags for {s:?}");
            assert_eq!(got.timestamps, want.timestamps, "timestamps for {s:?}");
            assert_values_eq(&got.values, &want.values);
        }

        // Try unmarshaling again: rows/buffers must be reusable.
        rows.unmarshal(s, |msg| {
            panic!("unexpected error for {s:?} (2nd pass): {msg}")
        });
        assert_eq!(
            rows.rows().len(),
            expected.len(),
            "row count for {s:?} (2nd pass)"
        );

        rows.reset();
        assert!(rows.rows().is_empty());
    }

    fn unmarshal_invalid(s: &str) {
        let mut rows = Rows::default();
        let mut errs = 0;
        rows.unmarshal(s, |_| errs += 1);
        assert!(rows.rows().is_empty(), "expected no rows for {s:?}");
        assert!(errs > 0, "expected an error to be logged for {s:?}");
    }

    #[test]
    fn empty_lines_skipped_silently() {
        // Truly-empty lines are skipped without logging any error.
        let mut rows = Rows::default();
        let mut errs = 0;
        rows.unmarshal("", |_| errs += 1);
        assert!(rows.rows().is_empty());
        assert_eq!(errs, 0);

        rows.unmarshal("\n\n", |_| errs += 1);
        assert!(rows.rows().is_empty());
        assert_eq!(errs, 0);

        rows.unmarshal("\n\r\n", |_| errs += 1);
        assert!(rows.rows().is_empty());
        assert_eq!(errs, 0);
    }

    #[test]
    fn single_line_single_tag() {
        unmarshal_ok(
            r#"{"metric":{"foo":"bar"},"values":[1.23],"timestamps":[456]}"#,
            &[row(&[("foo", "bar")], &[1.23], &[456])],
        );
    }

    #[test]
    fn multiple_tags_preserve_input_order() {
        unmarshal_ok(
            r#"{"metric":{"foo":"bar","baz":"xx"},"values":[1.23, -3.21],"timestamps" : [456,789]}"#,
            &[row(
                &[("foo", "bar"), ("baz", "xx")],
                &[1.23, -3.21],
                &[456, 789],
            )],
        );
    }

    #[test]
    fn name_tag_is_an_ordinary_tag_in_input_order() {
        unmarshal_ok(
            r#"{"metric":{"__name__":"xx"},"values":[34],"timestamps" : [11]}"#,
            &[row(&[("__name__", "xx")], &[34.0], &[11])],
        );
    }

    #[test]
    fn multiple_lines() {
        let s = r#"{"metric":{"foo":"bar","baz":"xx"},"values":[1.23, -3.21],"timestamps" : [456,789]}
{"metric":{"__name__":"xx"},"values":[34],"timestamps" : [11]}
"#;
        unmarshal_ok(
            s,
            &[
                row(
                    &[("foo", "bar"), ("baz", "xx")],
                    &[1.23, -3.21],
                    &[456, 789],
                ),
                row(&[("__name__", "xx")], &[34.0], &[11]),
            ],
        );
    }

    #[test]
    fn no_trailing_newline_on_last_line() {
        let s = r#"{"metric":{"foo":"bar"},"values":[1],"timestamps":[1]}
{"metric":{"__name__":"xx"},"values":[34],"timestamps":[11]}"#;
        unmarshal_ok(
            s,
            &[
                row(&[("foo", "bar")], &[1.0], &[1]),
                row(&[("__name__", "xx")], &[34.0], &[11]),
            ],
        );
    }

    #[test]
    fn invalid_line_in_the_middle_is_skipped_others_kept() {
        let s = r#"{"metric":{"xfoo":"bar","baz":"xx"},"values":[1.232, -3.21],"timestamps" : [456,7890]}
garbage here
{"metric":{"__name__":"xxy"},"values":[34],"timestamps" : [111]}"#;
        let mut rows = Rows::default();
        let mut errs = 0;
        rows.unmarshal(s, |_| errs += 1);
        assert_eq!(errs, 1, "expected exactly one skipped line");

        let expected = [
            row(
                &[("xfoo", "bar"), ("baz", "xx")],
                &[1.232, -3.21],
                &[456, 7890],
            ),
            row(&[("__name__", "xxy")], &[34.0], &[111]),
        ];
        assert_eq!(rows.rows().len(), expected.len());
        for (got, want) in rows.rows().iter().zip(expected.iter()) {
            assert_eq!(got.tags, want.tags);
            assert_eq!(got.timestamps, want.timestamps);
            assert_values_eq(&got.values, &want.values);
        }
    }

    #[test]
    fn special_float_values() {
        // Bare (unquoted) `Inf`/`NaN` fastjson literal tokens are not valid
        // JSON and cannot be parsed with `serde_json`; this test uses only
        // the quoted-string and JSON-`null` forms, all of which are valid
        // JSON and behave identically to upstream (see module doc).
        unmarshal_ok(
            r#"{"metric":{"foo":"bar"},"values":["Infinity", "-Infinity", "NaN", null, "null", 1.2],"timestamps":[456, 789, 123, 2, 3, 7]}"#,
            &[row(
                &[("foo", "bar")],
                &[
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::NAN,
                    f64::NAN,
                    f64::NAN,
                    1.2,
                ],
                &[456, 789, 123, 2, 3, 7],
            )],
        );
    }

    #[test]
    fn negative_special_strings() {
        unmarshal_ok(
            r#"{"metric":{"foo":"bar"},"values":["-Inf", "-inf", "-nan"],"timestamps":[1,2,3]}"#,
            &[row(
                &[("foo", "bar")],
                &[f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NAN],
                &[1, 2, 3],
            )],
        );
    }

    #[test]
    fn failures() {
        // Invalid json line.
        unmarshal_invalid("foo");
        unmarshal_invalid("123");
        unmarshal_invalid("[1,3]");
        unmarshal_invalid("{}");
        unmarshal_invalid("[]");
        unmarshal_invalid(r#"{"foo":"bar"}"#);

        // Invalid metric.
        unmarshal_invalid(r#"{"metric":123,"values":[1,2],"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":[123],"values":[1,2],"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":[],"values":[1,2],"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{},"values":[1,2],"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":null,"values":[1,2],"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"values":[1,2],"timestamps":[3,4]}"#);

        // Invalid values.
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":1,"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":{"x":1},"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":null,"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":["foo"],"timestamps":[3]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":"null","timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":"NaN","timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[["NaN"]],"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[true],"timestamps":[3]}"#);

        // Invalid timestamps.
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[1,2],"timestamps":3}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[1,2],"timestamps":false}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[1,2],"timestamps":{}}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[1,2]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[1,2],"timestamps":[1,"foo"]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[1,2],"timestamps":[1,1.5]}"#);

        // values/timestamps count mismatch.
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[],"timestamps":[]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[],"timestamps":[1]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[2],"timestamps":[]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[2],"timestamps":[3,4]}"#);
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[2,3],"timestamps":[4]}"#);

        // Garbage after the line (no newline, so it's all one "line" and
        // must fail to parse as a single JSON value).
        unmarshal_invalid(r#"{"metric":{"foo":"bar"},"values":[2],"timestamps":[4]}{}"#);
    }

    // -----------------------------------------------------------------
    // Streaming parser tests.
    // -----------------------------------------------------------------

    #[derive(Debug, PartialEq)]
    struct OwnedRow {
        tags: Vec<(String, String)>,
        values: Vec<f64>,
        timestamps: Vec<i64>,
    }

    fn collect_rows(rows: &[Row], dst: &mut Vec<OwnedRow>) {
        for r in rows {
            dst.push(OwnedRow {
                tags: r
                    .tags
                    .iter()
                    .map(|(k, v)| {
                        (
                            String::from_utf8(k.clone()).unwrap(),
                            String::from_utf8(v.clone()).unwrap(),
                        )
                    })
                    .collect(),
                values: r.values.clone(),
                timestamps: r.timestamps.clone(),
            });
        }
    }

    fn owned_row(tags: &[(&str, &str)], values: &[f64], timestamps: &[i64]) -> OwnedRow {
        OwnedRow {
            tags: tags
                .iter()
                .map(|&(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            values: values.to_vec(),
            timestamps: timestamps.to_vec(),
        }
    }

    const GOOD_DATA: &str = "{\"metric\":{\"__name__\":\"up\",\"job\":\"node\"},\"values\":[0,1],\"timestamps\":[100,200]}\n\
{\"metric\":{\"__name__\":\"temp\"},\"values\":[21.5],\"timestamps\":[300]}";

    fn good_data_parsed() -> Vec<OwnedRow> {
        vec![
            owned_row(
                &[("__name__", "up"), ("job", "node")],
                &[0.0, 1.0],
                &[100, 200],
            ),
            owned_row(&[("__name__", "temp")], &[21.5], &[300]),
        ]
    }

    #[test]
    fn parse_stream_good_data() {
        let mut rows = Vec::new();
        parse_stream(
            GOOD_DATA.as_bytes(),
            "",
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows, good_data_parsed());
    }

    #[test]
    fn parse_stream_invalid_lines_are_skipped() {
        let bad_data = "{\"metric\":{\"a\":\"b\"},\"values\":[1],\"timestamps\":[1]}\n\
garbage\n\
{\"metric\":{\"a\":\"c\"},\"values\":[2],\"timestamps\":[2]}";
        let mut rows = Vec::new();
        let mut errs = 0;
        parse_stream(
            bad_data.as_bytes(),
            "",
            |_| errs += 1,
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![
                owned_row(&[("a", "b")], &[1.0], &[1]),
                owned_row(&[("a", "c")], &[2.0], &[2]),
            ]
        );
        assert_eq!(errs, 1);
    }

    #[test]
    fn parse_stream_gzip_roundtrip() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(GOOD_DATA.as_bytes()).unwrap();
        let gzipped = encoder.finish().unwrap();

        let mut rows = Vec::new();
        parse_stream(
            &gzipped[..],
            "gzip",
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows, good_data_parsed());
    }

    #[test]
    fn parse_stream_unsupported_encoding_errors() {
        let err = parse_stream(b"foo".as_slice(), "br", |_| {}, |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, Error::Decode(ref msg) if msg.contains("br")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_stream_too_long_line_errors() {
        let data = vec![b'a'; MAX_LINE_LEN + 1024];
        let err = parse_stream(&data[..], "", |_| {}, |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, Error::TooLongLine { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_stream_callback_error_propagates() {
        let err =
            parse_stream(GOOD_DATA.as_bytes(), "", |_| {}, |_| Err("boom".into())).unwrap_err();
        match err {
            Error::Callback(source) => assert_eq!(source.to_string(), "boom"),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn parse_stream_empty_input_calls_callback_zero_times() {
        let mut calls = 0;
        parse_stream(
            "".as_bytes(),
            "",
            |_| {},
            |_| {
                calls += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(calls, 0);
    }
}
