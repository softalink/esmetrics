//! Streaming OpenTSDB telnet `put` protocol parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/opentsdb/stream/streamparser.go`, adapted to a
//! synchronous Rust API.
//!
//! This is a sibling module of [`crate::opentsdb`] (rather than folded into
//! it) purely to stay under the file-size guideline. Its public items are
//! re-exported from `crate::opentsdb`.
//!
//! # Deviations from the Go original
//!
//! - Unlike [`crate::graphite_stream`], the real Go `opentsdb/stream.Parse`
//!   does not accept a `Content-Encoding`/`encoding` parameter at all - the
//!   OpenTSDB telnet protocol is a raw TCP stream, not HTTP - so this port
//!   likewise has no `encoding` parameter and no `Decode` error variant.
//! - The current time is taken from [`std::time::SystemTime`] instead of the
//!   Go `fasttime` cached clock (same deviation as the other stream ports in
//!   this crate).
//! - `-opentsdbTrimTimestamp` defaults to `1s`, and the Go trimming loop
//!   only fires when the flag exceeds `1s`, so it is a no-op at the default
//!   value; not ported (same reasoning as `crate::graphite_stream`).
//! - No `Rows`/`unmarshalWork` object pooling across blocks, and no
//!   `vm_protoparser_rows_read_total` / `vm_rows_invalid_total` metrics
//!   counters (same gaps as the other ported parsers in this crate).

use std::fmt;
use std::io::{self, Read};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::opentsdb::{Row, Rows};
use crate::util::{self, LinesReadError};

/// Go: `protoparserutil.maxLineSize` (used by the plain, non-`Ext`
/// `ReadLinesBlock` call in the real `streamContext.Read`).
const MAX_LINE_SIZE: usize = 256 * 1024;

/// Go: `net/opentsdb/core.Const.SECOND_MASK` - if none of these bits are
/// set, the timestamp is in seconds and needs to be converted to
/// milliseconds; otherwise it is already in milliseconds.
/// See <http://opentsdb.net/docs/javadoc/net/opentsdb/core/Const.html#SECOND_MASK>.
const SECOND_MASK: i64 = 0x7FFF_FFFF_0000_0000;

/// Error returned by [`parse_stream`].
#[derive(Debug)]
pub enum Error {
    /// Cannot read OpenTSDB put protocol data.
    Io(io::Error),
    /// A single line exceeds the maximum allowed size.
    TooLongLine { max_line_len: usize },
    /// The request body is not valid UTF-8.
    Utf8(std::str::Utf8Error),
    /// The row callback returned an error.
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "cannot read OpenTSDB put protocol data: {err}"),
            Error::TooLongLine { max_line_len } => {
                write!(f, "too long line: more than {max_line_len} bytes")
            }
            Error::Utf8(err) => {
                write!(f, "OpenTSDB put protocol data is not valid UTF-8: {err}")
            }
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
            Error::TooLongLine { .. } => None,
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

/// Parses OpenTSDB `put` protocol data from `r` in stream mode and calls
/// `callback` for each parsed block of rows.
///
/// Go: `stream.Parse`. Invalid lines are skipped; each skipped line's
/// formatted error is passed to `err_logger`. Row timestamps of `0` (no
/// explicit timestamp) are filled with the current unix time in seconds;
/// timestamps that don't have any `SECOND_MASK` bits set are then converted
/// from seconds to milliseconds - Go: `unmarshalWork.Unmarshal`'s "Fill in
/// missing timestamps" + "Convert timestamps in seconds to milliseconds if
/// needed" steps.
///
/// The callback must not hold on to the rows after returning; they borrow
/// the internal read buffer, which is reused for the next block.
pub fn parse_stream<R: Read>(
    mut r: R,
    mut err_logger: impl FnMut(&str),
    mut callback: impl FnMut(&[Row<'_>]) -> CallbackResult,
) -> Result<(), Error> {
    let mut req_buf: Vec<u8> = Vec::new();
    let mut tail_buf: Vec<u8> = Vec::new();
    while util::read_lines_block(&mut r, &mut req_buf, &mut tail_buf, MAX_LINE_SIZE)? {
        let block = std::str::from_utf8(&req_buf).map_err(Error::Utf8)?;
        // TODO: pool `Rows` across blocks (Go uses sync.Pool via
        // `unmarshalWork`); a lifetime-erased pool is needed for that. The
        // parsed strings themselves are zero-copy borrows of `req_buf`.
        let mut rows = Rows::default();
        rows.unmarshal(block, |msg| err_logger(msg));
        fixup_timestamps(rows.rows_mut());
        if let Err(source) = callback(rows.rows()) {
            return Err(Error::Callback(source));
        }
    }
    Ok(())
}

/// Fills `0` (absent) timestamps with the current unix time in seconds,
/// then converts seconds-granularity timestamps to milliseconds.
fn fixup_timestamps(rows: &mut [Row<'_>]) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct OwnedRow {
        metric: String,
        tags: Vec<(String, String)>,
        value: f64,
        timestamp: i64,
    }

    fn collect_rows(rows: &[Row<'_>], dst: &mut Vec<OwnedRow>) {
        for r in rows {
            dst.push(OwnedRow {
                metric: r.metric.to_string(),
                tags: r
                    .tags
                    .iter()
                    .map(|t| (t.key.to_string(), t.value.to_string()))
                    .collect(),
                value: r.value,
                timestamp: r.timestamp,
            });
        }
    }

    fn owned_row(metric: &str, tags: &[(&str, &str)], value: f64, timestamp: i64) -> OwnedRow {
        OwnedRow {
            metric: metric.to_string(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value,
            timestamp,
        }
    }

    #[test]
    fn parse_stream_good_data() {
        let data = "put foo.bar 1727879909 123.456 tag1=v1\nput foo.baz 1727879910 1 a=b\n";
        let mut rows = Vec::new();
        parse_stream(
            data.as_bytes(),
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![
                owned_row("foo.bar", &[("tag1", "v1")], 123.456, 1_727_879_909_000),
                owned_row("foo.baz", &[("a", "b")], 1.0, 1_727_879_910_000),
            ]
        );
    }

    #[test]
    fn invalid_lines_are_skipped() {
        let bad_data = "put foo 100 1 a=b\naaa bbb\nput bar 300 3 a=b\n";
        let mut rows = Vec::new();
        let mut errs = 0;
        parse_stream(
            bad_data.as_bytes(),
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
                owned_row("foo", &[("a", "b")], 1.0, 100_000),
                owned_row("bar", &[("a", "b")], 3.0, 300_000)
            ]
        );
        assert_eq!(errs, 1);
    }

    #[test]
    fn missing_timestamp_fills_now() {
        let before = current_time_seconds() * 1000;
        let mut rows = Vec::new();
        parse_stream(
            "put foo 0 1 a=b\n".as_bytes(),
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        let after = current_time_seconds() * 1000;
        assert_eq!(rows.len(), 1);
        let ts = rows[0].timestamp;
        assert!(
            ts >= before && ts <= after,
            "timestamp {ts} not in [{before}, {after}]"
        );
    }

    #[test]
    fn already_millisecond_timestamp_is_not_rescaled() {
        // A timestamp with any SECOND_MASK bit set is treated as already
        // being in milliseconds and passed through unchanged.
        let ts_millis: i64 = 1_700_000_000_000;
        assert_ne!(ts_millis & SECOND_MASK, 0, "test fixture sanity check");
        let mut rows = Vec::new();
        parse_stream(
            format!("put foo {ts_millis} 1 a=b\n").as_bytes(),
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, ts_millis);
    }

    #[test]
    fn too_long_line_errors() {
        let data = vec![b'a'; MAX_LINE_SIZE + 1024];
        let err = parse_stream(&data[..], |_| {}, |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, Error::TooLongLine { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn callback_error_propagates() {
        let err = parse_stream(
            "put foo 100 1 a=b\n".as_bytes(),
            |_| {},
            |_| Err("boom".into()),
        )
        .unwrap_err();
        match err {
            Error::Callback(source) => assert_eq!(source.to_string(), "boom"),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn empty_input_calls_callback_zero_times() {
        let mut calls = 0;
        parse_stream(
            "".as_bytes(),
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
