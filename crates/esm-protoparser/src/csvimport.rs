//! CSV import parser: `/api/v1/import/csv`. Port of upstream VictoriaMetrics
//! v1.146.0 `lib/protoparser/csvimport/{parser,column_descriptor,scanner}.go`
//! plus `.../stream/streamparser.go`, folded into one file per the task.
//!
//! Deviations from upstream (details at the named item): [`TimeFormat`]
//! replaces a Go closure field with a plain enum; `time:custom:<Go layout>`
//! columns are rejected, see [`parse_time_format`]; RFC3339 parsing is
//! hand-rolled (no `chrono`), see [`parse_rfc3339`]/[`days_from_civil`];
//! metric values use `str::parse::<f64>`, see [`parse_metric_value`]; no Go
//! `sync.Pool`-style pooling across HTTP blocks (same simplification as
//! `crate::vmimport`); `-csvTrimTimestamp` is not ported (its default value
//! disables it upstream too, `if tsTrim > 1`); missing timestamps default to
//! [`std::time::SystemTime`], not the Go `fasttime` cached clock.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read};

use crate::util::{self, LinesReadError, UtilError};

/// Go: `csvimport.maxColumnsPerRow`.
pub const MAX_COLUMNS_PER_ROW: usize = 64 * 1024;

/// Go: `protoparserutil.maxLineSize` (the non-`Ext` `ReadLinesBlock` call).
pub const MAX_LINE_LEN: usize = 256 * 1024;

// ---------------------------------------------------------------------------
// Column descriptors. Port of `column_descriptor.go`.
// ---------------------------------------------------------------------------

/// A recognized `time:<fmt>` column format. See the module doc for the
/// `custom:` narrowing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeFormat {
    UnixSeconds,
    UnixMillis,
    UnixNanos,
    Rfc3339,
}

impl TimeFormat {
    /// Go: `parseUnixTimestamp{Seconds,Milliseconds,Nanoseconds}`/`parseRFC3339`.
    fn parse(self, s: &str) -> Result<i64, String> {
        if self == TimeFormat::Rfc3339 {
            return parse_rfc3339(s);
        }
        let n: i64 = s
            .parse()
            .map_err(|err| format!("cannot parse timestamp from {s:?}: {err}"))?;
        Ok(match self {
            TimeFormat::UnixSeconds => {
                const MAX_SECONDS: i64 = i64::MAX / 1000;
                if n > MAX_SECONDS {
                    return Err(format!("too big unix timestamp in seconds: {n}; must be smaller than {MAX_SECONDS}"));
                }
                n * 1000
            }
            TimeFormat::UnixMillis => n,
            TimeFormat::UnixNanos => n / 1_000_000,
            TimeFormat::Rfc3339 => unreachable!(),
        })
    }
}

/// Parsing rules for a single csv column: transformed into a timestamp, a
/// label, or a metric value depending on which field is set, or ignored if
/// all are unset. Go: `ColumnDescriptor`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnDescriptor {
    pub parse_timestamp: Option<TimeFormat>,
    pub tag_name: String,
    pub metric_name: String,
}

impl ColumnDescriptor {
    fn is_empty(&self) -> bool {
        self.parse_timestamp.is_none() && self.tag_name.is_empty() && self.metric_name.is_empty()
    }
}

/// Parses column descriptors from `s`, the `format=` query arg: a
/// comma-separated `<column_pos>:<column_type>:<extension>` list.
/// `<column_pos>` is 1-based; `<column_type>` is `time` (extension
/// `unix_s`/`unix_ms`/`unix_ns`/`rfc3339`), `label` (extension is the label
/// name), or `metric` (extension is the metric name); at least one `metric`
/// and at most one `time` column are required. Go: `ParseColumnDescriptors`.
pub fn parse_column_descriptors(s: &str) -> Result<Vec<ColumnDescriptor>, String> {
    fn non_empty(entry: usize, col: &str, kind: &str, val: &str) -> Result<String, String> {
        if val.is_empty() {
            return Err(format!(
                "{kind} name cannot be empty in the entry #{entry} {col:?}"
            ));
        }
        Ok(val.to_owned())
    }

    let mut by_pos: HashMap<usize, ColumnDescriptor> = HashMap::new();
    let mut has_value_col = false;
    let mut has_time_col = false;
    let mut max_pos = 0usize;

    for (i, col) in s.split(',').enumerate() {
        let entry = i + 1;
        let parts: Vec<&str> = col.splitn(3, ':').collect();
        let [pos_str, typ, ext] = parts[..] else {
            return Err(format!(
                "entry #{entry} must have the following form: <column_pos>:<column_type>:<extension>; got {col:?}"
            ));
        };
        let pos: i64 = pos_str.parse().map_err(|err| {
            format!("cannot parse <column_pos> part from the entry #{entry} {col:?}: {err}")
        })?;
        if pos <= 0 {
            return Err(format!(
                "<column_pos> cannot be smaller than 1; got {pos} for entry #{entry} {col:?}"
            ));
        }
        if pos as usize > MAX_COLUMNS_PER_ROW {
            return Err(format!(
                "<column_pos> cannot be bigger than {MAX_COLUMNS_PER_ROW}; got {pos} for entry #{entry} {col:?}"
            ));
        }
        let pos = pos as usize;
        max_pos = max_pos.max(pos);

        let mut cd = ColumnDescriptor::default();
        match typ {
            "time" => {
                if has_time_col {
                    return Err(format!(
                        "duplicate time column has been found at entry #{entry} {col:?} for {s:?}"
                    ));
                }
                cd.parse_timestamp = Some(parse_time_format(ext).map_err(|err| {
                    format!("cannot parse time format from the entry #{entry} {col:?}: {err}")
                })?);
                has_time_col = true;
            }
            "label" => cd.tag_name = non_empty(entry, col, "label", ext)?,
            "metric" => {
                cd.metric_name = non_empty(entry, col, "metric", ext)?;
                has_value_col = true;
            }
            other => {
                return Err(format!(
                    "unknown <column_type>: {other:?}; allowed values: time, metric, label"
                ));
            }
        }

        let idx = pos - 1;
        if by_pos.contains_key(&idx) {
            return Err(format!(
                "duplicate <column_pos> {idx} for the entry #{entry} {col:?}"
            ));
        }
        by_pos.insert(idx, cd);
    }
    if !has_value_col {
        return Err(format!("missing 'metric' column in {s:?}"));
    }

    let mut cds = vec![ColumnDescriptor::default(); max_pos];
    for (pos, cd) in by_pos {
        cds[pos] = cd;
    }
    Ok(cds)
}

const SUPPORTED_TIME_FORMATS: &str = "unix_s, unix_ms, unix_ns, rfc3339";

fn parse_time_format(format: &str) -> Result<TimeFormat, String> {
    match format {
        "unix_s" => Ok(TimeFormat::UnixSeconds),
        "unix_ms" => Ok(TimeFormat::UnixMillis),
        "unix_ns" => Ok(TimeFormat::UnixNanos),
        "rfc3339" => Ok(TimeFormat::Rfc3339),
        _ if format.starts_with("custom:") => Err(format!(
            "custom time formats (Go time layouts) are not supported by this port; got {format:?}; supported formats: {SUPPORTED_TIME_FORMATS}"
        )),
        _ => Err(format!(
            "unknown format for time parsing: {format:?}; supported formats: {SUPPORTED_TIME_FORMATS}"
        )),
    }
}

/// Minimal RFC3339 parser, unix-ms output: optional fractional seconds
/// (truncated to ms), literal `Z` or numeric `±HH:MM` offset; no named
/// timezones/leap seconds. Hand-rolled; see the module doc for why.
fn parse_rfc3339(s: &str) -> Result<i64, String> {
    let err = || format!("cannot parse time in RFC3339 from {s:?}");
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return Err(err());
    }
    let sep_ok = bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':';
    if !sep_ok {
        return Err(err());
    }
    let digits = |range: std::ops::Range<usize>| -> Result<i64, String> {
        s.get(range)
            .filter(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|part| part.parse().ok())
            .ok_or_else(err)
    };
    let year = digits(0..4)?;
    let month = digits(5..7)?;
    let day = digits(8..10)?;
    let hour = digits(11..13)?;
    let minute = digits(14..16)?;
    let second = digits(17..19)?;
    // Reject invalid calendar dates and ":60" seconds, like Go time.Parse.
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        2 => 28 + i64::from(leap),
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    let valid = (1..=12).contains(&month)
        && (1..=max_day).contains(&day)
        && hour <= 23
        && minute <= 59
        && second <= 59;
    if !valid {
        return Err(err());
    }

    let mut idx = 19;
    let mut frac_millis: i64 = 0;
    if bytes.get(idx) == Some(&b'.') {
        idx += 1;
        let start = idx;
        while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        if idx == start {
            return Err(err());
        }
        let mut ms_digits = [b'0'; 3];
        for (slot, b) in ms_digits.iter_mut().zip(s[start..idx].bytes()) {
            *slot = b;
        }
        frac_millis = std::str::from_utf8(&ms_digits).unwrap().parse().unwrap();
    }

    let offset_secs: i64 = match bytes.get(idx) {
        Some(b'Z') if idx + 1 == bytes.len() => 0,
        Some(&sign @ (b'+' | b'-')) if bytes.len() == idx + 6 && bytes[idx + 3] == b':' => {
            let total = digits(idx + 1..idx + 3)? * 3600 + digits(idx + 4..idx + 6)? * 60;
            if sign == b'-' {
                -total
            } else {
                total
            }
        }
        _ => return Err(err()),
    };

    let days = days_from_civil(year, month as u32, day as u32);
    let secs_of_day = hour * 3600 + minute * 60 + second;
    let unix_secs = days * 86_400 + secs_of_day - offset_secs;
    Ok(unix_secs * 1000 + frac_millis)
}

/// Days since the Unix epoch for a civil date: Howard Hinnant's public-domain
/// `days_from_civil`, inverse of `esm_backup::timeutil`'s `civil_from_unix`.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m as i64 - 3 } else { m as i64 + 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

// ---------------------------------------------------------------------------
// Scanner. Port of `scanner.go`.
// ---------------------------------------------------------------------------

struct Scanner<'a> {
    line: &'a str,
    rest: &'a str,
    is_last_column: bool,
    error: Option<String>,
}

impl<'a> Scanner<'a> {
    fn new(s: &'a str) -> Self {
        Scanner {
            line: "",
            rest: s,
            is_last_column: false,
            error: None,
        }
    }

    fn next_line(&mut self) -> bool {
        self.line = "";
        self.error = None;
        self.is_last_column = false;
        let mut s = self.rest;
        while !s.is_empty() {
            let (line, tail) = match s.as_bytes().iter().position(|&b| b == b'\n') {
                Some(n) => (trim_trailing_cr(&s[..n]), &s[n + 1..]),
                None => (trim_trailing_cr(s), ""),
            };
            s = tail;
            if !line.is_empty() {
                self.line = line;
                self.rest = s;
                return true;
            }
        }
        self.rest = "";
        false
    }

    fn next_column(&mut self) -> Option<Cow<'a, str>> {
        if self.is_last_column || self.error.is_some() {
            return None;
        }
        let s = self.line;
        if s.starts_with('"') || s.starts_with('\'') {
            return match read_quoted_field(s) {
                Ok((field, tail)) => {
                    if tail.is_empty() {
                        self.is_last_column = true;
                    } else if let Some(rest) = tail.strip_prefix(',') {
                        self.line = rest;
                        return Some(field);
                    } else {
                        self.error = Some(format!("missing comma after quoted field in {s:?}"));
                        return None;
                    }
                    self.line = tail;
                    Some(field)
                }
                Err(err) => {
                    self.error = Some(err);
                    None
                }
            };
        }
        match s.find(',') {
            Some(n) => {
                self.line = &s[n + 1..];
                Some(Cow::Borrowed(&s[..n]))
            }
            None => {
                self.line = "";
                self.is_last_column = true;
                Some(Cow::Borrowed(s))
            }
        }
    }
}

fn trim_trailing_cr(s: &str) -> &str {
    s.strip_suffix('\r').unwrap_or(s)
}

fn read_quoted_field(s: &str) -> Result<(Cow<'_, str>, &str), String> {
    let err = || format!("missing closing quote for {s:?}");
    let bytes = s.as_bytes();
    let quote = bytes[0];
    let mut offset = 1usize;
    let n = bytes[offset..]
        .iter()
        .position(|&b| b == quote)
        .ok_or_else(err)?;
    offset += n + 1;
    if offset >= s.len() || bytes[offset] != quote {
        // Fast path: no escaped quotes.
        return Ok((Cow::Borrowed(&s[1..offset - 1]), &s[offset..]));
    }
    // Slow path: the quoted string contains an escaped (doubled) quote.
    let mut buf = String::with_capacity(s.len().saturating_sub(2));
    buf.push_str(&s[1..offset]);
    loop {
        offset += 1;
        let n = bytes[offset..]
            .iter()
            .position(|&b| b == quote)
            .ok_or_else(err)?;
        buf.push_str(&s[offset..offset + n]);
        offset += n + 1;
        if offset < s.len() && bytes[offset] == quote {
            buf.push(quote as char);
            continue;
        }
        return Ok((Cow::Owned(buf), &s[offset..]));
    }
}

// ---------------------------------------------------------------------------
// Row parser. Port of `parser.go`.
// ---------------------------------------------------------------------------

/// One parsed metric sample; a line with N `metric` columns produces N
/// `Row`s sharing tags/timestamp. Fully owned (like `crate::vmimport::Row`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    pub metric: String,
    /// `(key, value)` pairs, in column order.
    pub tags: Vec<(String, String)>,
    pub value: f64,
    pub timestamp: i64,
}

/// Parsed csv rows. Go: `Rows`.
#[derive(Debug, Default)]
pub struct Rows {
    rows: Vec<Row>,
}

impl Rows {
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn rows_mut(&mut self) -> &mut [Row] {
        &mut self.rows
    }

    pub fn reset(&mut self) {
        self.rows.clear();
    }

    /// Unmarshals csv lines from `s` according to `cds`. Go: `Rows.Unmarshal`.
    pub fn unmarshal(
        &mut self,
        s: &str,
        cds: &[ColumnDescriptor],
        mut err_logger: impl FnMut(&str),
    ) {
        self.rows.clear();
        let mut sc = Scanner::new(s);
        parse_rows(&mut sc, &mut self.rows, cds, false, &mut err_logger);
    }

    /// Like [`Self::unmarshal`], but skips the first row if it looks like a
    /// CSV header. Must only be called for the first data block in a
    /// stream. Go: `Rows.UnmarshalDetectHeader`.
    pub fn unmarshal_detect_header(
        &mut self,
        s: &str,
        cds: &[ColumnDescriptor],
        mut err_logger: impl FnMut(&str),
    ) {
        self.rows.clear();
        let mut sc = Scanner::new(s);
        parse_rows(&mut sc, &mut self.rows, cds, true, &mut err_logger);
    }
}

/// Next non-superfluous, non-empty `(descriptor, value)` pair, skipping
/// columns that don't qualify. Shared by [`is_header_row`]/[`parse_rows`].
fn next_used_column<'v, 'x>(
    sc: &mut Scanner<'v>,
    cds: &'x [ColumnDescriptor],
    col: &mut usize,
) -> Option<(&'x ColumnDescriptor, Cow<'v, str>)> {
    loop {
        let value = sc.next_column()?;
        if *col >= cds.len() {
            continue; // Superfluous column.
        }
        let cd = &cds[*col];
        *col += 1;
        if cd.is_empty() || value.is_empty() {
            continue;
        }
        return Some((cd, value));
    }
}

fn is_header_row(sc: &mut Scanner<'_>, cds: &[ColumnDescriptor]) -> bool {
    let mut is_header = false;
    let mut col = 0usize;
    while let Some((cd, value)) = next_used_column(sc, cds, &mut col) {
        if let Some(fmt) = cd.parse_timestamp {
            is_header |= fmt.parse(&value).is_err();
        }
        is_header |= !cd.metric_name.is_empty() && parse_metric_value(&value).is_err();
    }
    is_header
}

fn parse_rows(
    sc: &mut Scanner<'_>,
    dst: &mut Vec<Row>,
    cds: &[ColumnDescriptor],
    mut skip_header: bool,
    err_logger: &mut impl FnMut(&str),
) {
    while sc.next_line() {
        if skip_header {
            skip_header = false;
            let saved_line = sc.line;
            let is_header = is_header_row(sc, cds);
            sc.line = saved_line;
            sc.is_last_column = false;
            sc.error = None;
            if is_header {
                continue;
            }
        }

        let line = sc.line;
        let mut col = 0usize;
        // (metric_name, value) pairs, one metric column at a time.
        let mut metrics: Vec<(&str, f64)> = Vec::new();
        let mut tags: Vec<(String, String)> = Vec::new();
        let mut timestamp: i64 = 0;

        while let Some((cd, value)) = next_used_column(sc, cds, &mut col) {
            if let Some(fmt) = cd.parse_timestamp {
                match fmt.parse(&value) {
                    Ok(ts) => timestamp = ts,
                    Err(err) => {
                        sc.error = Some(format!("cannot parse timestamp from {value:?}: {err}"));
                        break;
                    }
                }
                continue;
            }
            if !cd.tag_name.is_empty() {
                tags.push((cd.tag_name.clone(), value.into_owned()));
                continue;
            }
            match parse_metric_value(&value) {
                Ok(v) => metrics.push((&cd.metric_name, v)),
                Err(err) => {
                    let name = &cd.metric_name;
                    sc.error = Some(format!(
                        "cannot parse metric value for {name:?} from {value:?}: {err}"
                    ));
                    break;
                }
            }
        }
        if sc.error.is_none() && col < cds.len() {
            let want = cds.len();
            sc.error = Some(format!(
                "missing columns in {line:?}: got {col}, want at least {want}"
            ));
        }
        if let Some(err) = sc.error.take() {
            err_logger(&format!(
                "error when parsing csv line {line:?}: {err}; skipping this line"
            ));
            continue;
        }
        if metrics.is_empty() {
            continue;
        }
        let last = metrics.len() - 1;
        for (i, (name, value)) in metrics.into_iter().enumerate() {
            dst.push(Row {
                metric: name.to_owned(),
                tags: if i == last {
                    std::mem::take(&mut tags)
                } else {
                    tags.clone()
                },
                value,
                timestamp,
            });
        }
    }
}

/// Parses a metric column value. See the module doc for the divergence from
/// upstream's `fastfloat.Parse`.
fn parse_metric_value(s: &str) -> Result<f64, std::num::ParseFloatError> {
    s.parse::<f64>()
}

// ---------------------------------------------------------------------------
// Streaming parser. Port of `stream/streamparser.go`.
// ---------------------------------------------------------------------------

/// Error returned by [`parse_stream`].
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    TooLongLine { max_line_len: usize },
    Utf8(std::str::Utf8Error),
    Decode(String),
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "cannot read csv data: {err}"),
            Error::TooLongLine { max_line_len } => {
                write!(f, "too long line: more than {max_line_len} bytes")
            }
            Error::Utf8(err) => write!(f, "csv data is not valid UTF-8: {err}"),
            Error::Decode(msg) => write!(f, "cannot decode csv data: {msg}"),
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

/// Parses csv data from `r` in stream mode, calling `callback` per parsed
/// block. Only the first block gets header autodetection; missing
/// timestamps are filled with the current time. Go: `stream.Parse`.
pub fn parse_stream<R: Read>(
    r: R,
    encoding: &str,
    cds: &[ColumnDescriptor],
    mut err_logger: impl FnMut(&str),
    mut callback: impl FnMut(&[Row]) -> CallbackResult,
) -> Result<(), Error> {
    let reader = util::uncompressed_reader(r, encoding).map_err(|err| match err {
        UtilError::UnsupportedEncoding(enc) => {
            Error::Decode(format!("unsupported Content-Encoding: {enc:?}"))
        }
        other => Error::Decode(other.to_string()),
    })?;
    parse_stream_internal(reader, cds, &mut err_logger, &mut callback)
}

fn parse_stream_internal<R>(
    mut r: R,
    cds: &[ColumnDescriptor],
    err_logger: &mut impl FnMut(&str),
    callback: &mut impl FnMut(&[Row]) -> CallbackResult,
) -> Result<(), Error>
where
    R: Read,
{
    let mut req_buf: Vec<u8> = Vec::new();
    let mut tail_buf: Vec<u8> = Vec::new();
    let mut first_block = true;
    while util::read_lines_block(&mut r, &mut req_buf, &mut tail_buf, MAX_LINE_LEN)? {
        let block = std::str::from_utf8(&req_buf).map_err(Error::Utf8)?;
        let mut rows = Rows::default();
        if first_block {
            rows.unmarshal_detect_header(block, cds, |msg| err_logger(msg));
        } else {
            rows.unmarshal(block, cds, |msg| err_logger(msg));
        }
        first_block = false;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        for row in rows.rows_mut() {
            if row.timestamp == 0 {
                row.timestamp = now;
            }
        }

        if let Err(source) = callback(rows.rows()) {
            return Err(Error::Callback(source));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The `pub` API is exercised from `tests/csvimport_parser.rs` (file-size
    // guideline); only tests touching *private* items live below.

    #[test]
    fn timestamp_helpers_match_upstream_fastfloat_semantics() {
        use TimeFormat::*;
        for (fmt, s, want) in [
            (UnixSeconds, "0", 0),
            (UnixSeconds, "123", 123_000),
            (UnixSeconds, "-123", -123_000),
            (UnixMillis, "123", 123),
            (UnixMillis, "-123", -123),
            (UnixNanos, "0", 0),
            (UnixNanos, "123", 0),
            (UnixNanos, "12343567", 12),
            (UnixNanos, "-12343567", -12),
        ] {
            assert_eq!(fmt.parse(s).unwrap(), want, "{fmt:?} {s:?}");
        }
        assert!(UnixSeconds.parse("12345678901234567").is_err());
    }

    #[test]
    fn rfc3339_matches_upstream_golden_values() {
        let cases = [
            ("2006-01-02T15:04:05Z", 1_136_214_245_000),
            ("2020-03-11T18:23:46Z", 1_583_951_026_000),
            ("2022-12-25T16:57:12+01:00", 1_671_983_832_000), // numeric offset, no fraction
            ("2022-12-25T16:57:12.000+01:00", 1_671_983_832_000), // zero-ms fraction, VM #5837
            ("2015-08-10T20:04:40.123Z", 1_439_237_080_123),  // fractional seconds
            ("2015-08-10T00:00:01.000Z", 1_439_164_801_000),
        ];
        for (s, want) in cases {
            assert_eq!(parse_rfc3339(s).unwrap(), want, "input {s:?}");
        }
    }

    #[test]
    fn rfc3339_rejects_garbage() {
        for s in [
            "",
            "not-a-date",
            "2020-03-11 18:23:46Z",
            "2020-03-11T18:23:46",
            "2020-13-01T00:00:00Z",
        ] {
            assert!(parse_rfc3339(s).is_err(), "expected error for {s:?}");
        }
    }

    // -- Scanner ----------------------------------------------------------

    fn scan_all(s: &str) -> (Vec<Vec<String>>, bool) {
        let mut sc = Scanner::new(s);
        let mut rows = Vec::new();
        while sc.next_line() {
            let mut row = Vec::new();
            while let Some(col) = sc.next_column() {
                row.push(col.into_owned());
            }
            rows.push(row);
            if sc.error.is_some() {
                // `next_line()` clears the error, so it must be observed here.
                return (rows, true);
            }
        }
        (rows, false)
    }

    #[test]
    fn scanner_success() {
        assert_eq!(scan_all("").0, Vec::<Vec<String>>::new());
        assert_eq!(
            scan_all("foo,bar\n\"aa,\"\"bb\",\"\"").0,
            vec![
                vec!["foo".to_owned(), "bar".to_owned()],
                vec!["aa,\"bb".to_owned(), "".to_owned()],
            ]
        );
        // Mixed double/single quoting, escaped-quote unescaping.
        let want = ["fo\"bar", "baz'a", "bc\"de", "g'e"]
            .map(str::to_owned)
            .to_vec();
        assert_eq!(scan_all(r#"fo"bar,baz'a,"bc""de",'g''e'"#).0, vec![want]);
    }

    #[test]
    fn scanner_failure() {
        // Unclosed quotes: no closing quote at all, and a closing quote not
        // followed by a comma.
        for s in ["foo\r\n\"bar,", "foo,\"bar\",\"\"a"] {
            let (_, had_error) = scan_all(s);
            assert!(had_error, "expected scanner error for {s:?}");
        }
    }
}
