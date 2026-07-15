//! Streaming influx line-protocol parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/influx/stream/streamparser.go` plus the needed pieces of
//! `lib/protoparser/protoparserutil` (`ReadLinesBlockExt`), adapted to a
//! synchronous Rust API.
//!
//! Deviations from the Go original:
//! - Only stream mode is ported (chunked reads, invalid lines skipped).
//!   Batch mode ("read whole request, fail on first bad line") is a separate
//!   code path in Go gated behind `-influx.forceStreamMode`.
//! - The current time is taken from [`std::time::SystemTime`] instead of the
//!   Go `fasttime` cached clock.
//! - `-influxTrimTimestamp` defaults to 1ms in Go, which makes the trimming
//!   loop a no-op; the flag-driven trimming is not ported.
//! - The input must be valid UTF-8 (Go casts raw bytes to string without
//!   validation; Rust `&str` requires validation).

use std::fmt;
use std::io::{self, Read};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::read::GzDecoder;

use crate::influx::{Row, Rows};
use crate::util::{self, LinesReadError};

/// The maximum size in bytes for a single InfluxDB line during parsing.
/// Go: `-influx.maxLineSize` flag, default 256KiB.
pub const MAX_LINE_SIZE: usize = 256 * 1024;

/// Error returned by [`parse_stream`].
#[derive(Debug)]
pub enum Error {
    /// Cannot read influx line protocol data.
    Io(io::Error),
    /// A single line exceeds the maximum allowed size.
    TooLongLine { max_line_len: usize },
    /// The request body is not valid UTF-8.
    Utf8(std::str::Utf8Error),
    /// The row callback returned an error.
    Callback {
        db: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "cannot read influx line protocol data: {err}"),
            Error::TooLongLine { max_line_len } => {
                write!(f, "too long line: more than {max_line_len} bytes")
            }
            Error::Utf8(err) => {
                write!(f, "influx line protocol data is not valid UTF-8: {err}")
            }
            Error::Callback { db, source } => {
                write!(
                    f,
                    "error when processing imported data (db={db:?}): {source}"
                )
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Utf8(err) => Some(err),
            Error::Callback { source, .. } => Some(source.as_ref()),
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

/// Parses influx line-protocol data from `r` in stream mode and calls
/// `callback` for each parsed block of rows.
///
/// Invalid lines are skipped (Go stream-mode semantics). Row timestamps are
/// adjusted to milliseconds according to `precision` (`"ns"`, `"u"`/`"us"`/
/// `"µ"`, `"ms"`, `"s"`, `"m"`, `"h"`, or empty for auto-detection), with
/// missing timestamps filled with the current time.
///
/// `db` is not interpreted by the parser: Go forwards it verbatim to the
/// callback (multi-tenant routing happens there); here it is only used to
/// annotate callback errors. Capture it in the closure if you need it.
///
/// The callback must not hold on to the rows after returning; they borrow the
/// internal read buffer, which is reused for the next block.
pub fn parse_stream<R: Read>(
    r: R,
    is_gzipped: bool,
    precision: &str,
    db: &str,
    mut callback: impl FnMut(&[Row<'_>]) -> CallbackResult,
) -> Result<(), Error> {
    let ts_multiplier = get_timestamp_multiplier(precision);
    if is_gzipped {
        parse_stream_internal(GzDecoder::new(r), ts_multiplier, db, &mut callback)
    } else {
        parse_stream_internal(r, ts_multiplier, db, &mut callback)
    }
}

fn parse_stream_internal<R, F>(
    mut r: R,
    ts_multiplier: i64,
    db: &str,
    callback: &mut F,
) -> Result<(), Error>
where
    R: Read,
    F: FnMut(&[Row<'_>]) -> CallbackResult,
{
    // TODO: Go hands each block to a shared unmarshal-work scheduler
    // (`protoparserutil.ScheduleUnmarshalWork`), swapping `reqBuf` between
    // the stream context and pooled `unmarshalWork` objects so that blocks
    // are unmarshaled concurrently. A shared scheduler would plug in here;
    // this sync port unmarshals blocks inline.
    let mut req_buf: Vec<u8> = Vec::new();
    let mut tail_buf: Vec<u8> = Vec::new();
    while util::read_lines_block(&mut r, &mut req_buf, &mut tail_buf, MAX_LINE_SIZE)? {
        let block = std::str::from_utf8(&req_buf).map_err(Error::Utf8)?;
        // TODO: pool `Rows` across blocks (Go uses sync.Pool via
        // `unmarshalWork`); a lifetime-erased pool is needed for that. The
        // parsed strings themselves are zero-copy borrows of `req_buf`.
        let mut rows = Rows::default();
        rows.unmarshal(block, true)
            .expect("BUG: unexpected non-nil error when rows must be ignored");
        adjust_timestamps(rows.rows_mut(), ts_multiplier, current_time_millis());
        if let Err(source) = callback(rows.rows()) {
            return Err(Error::Callback {
                db: db.to_string(),
                source,
            });
        }
    }
    Ok(())
}

fn get_timestamp_multiplier(precision: &str) -> i64 {
    match precision {
        "ns" => 1_000_000,
        "u" | "us" | "µ" => 1_000,
        "ms" => 1,
        "s" => -1_000,
        "m" => -1_000 * 60,
        "h" => -1_000 * 3600,
        _ => 0,
    }
}

/// Adjusts row timestamps to milliseconds according to `ts_multiplier`.
/// Port of the timestamp-adjustment part of Go `unmarshal`.
fn adjust_timestamps(rows: &mut [Row<'_>], ts_multiplier: i64, current_ts: i64) {
    if ts_multiplier == 0 {
        // Default precision is 'ns'. But it can be in ns, us, ms or s
        // depending on the number of digits in practice.
        for row in rows {
            row.timestamp = detect_timestamp(row.timestamp, current_ts);
        }
    } else if ts_multiplier >= 1 {
        for row in rows {
            if row.timestamp == 0 {
                row.timestamp = current_ts;
            } else {
                row.timestamp /= ts_multiplier;
            }
        }
    } else {
        let ts_multiplier = -ts_multiplier;
        let current_ts = current_ts - current_ts % ts_multiplier;
        for row in rows {
            if row.timestamp == 0 {
                row.timestamp = current_ts;
            } else {
                row.timestamp *= ts_multiplier;
            }
        }
    }
    // Go additionally trims timestamps to `-influxTrimTimestamp`, which
    // defaults to 1ms and is therefore a no-op; not ported.
}

fn detect_timestamp(ts: i64, current_ts: i64) -> i64 {
    if ts == 0 {
        return current_ts;
    }
    if ts >= 100_000_000_000_000_000 {
        // convert nanoseconds to milliseconds
        return ts / 1_000_000;
    }
    if ts >= 100_000_000_000_000 {
        // convert microseconds to milliseconds
        return ts / 1_000;
    }
    if ts >= 100_000_000_000 {
        // the ts is in milliseconds
        return ts;
    }
    // convert seconds to milliseconds
    ts * 1_000
}

fn current_time_millis() -> i64 {
    // NOTE: Go uses `time.Now().UnixNano() / 1e6`; a cached clock (fasttime)
    // is used elsewhere in the upstream. SystemTime is used here.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_detect_timestamp() {
        let ts_default: i64 = 123;
        let f = |ts: i64, ts_expected: i64| {
            let ts_result = detect_timestamp(ts, ts_default);
            assert_eq!(
                ts_result, ts_expected,
                "unexpected timestamp for detect_timestamp({ts}, {ts_default})"
            );
        };
        f(0, ts_default);
        f(1, 1_000);
        f(10_000_000, 10_000_000_000);
        f(100_000_000, 100_000_000_000);
        f(1_000_000_000, 1_000_000_000_000);
        f(10_000_000_000, 10_000_000_000_000);
        f(100_000_000_000, 100_000_000_000);
        f(1_000_000_000_000, 1_000_000_000_000);
        f(10_000_000_000_000, 10_000_000_000_000);
        f(100_000_000_000_000, 100_000_000_000);
        f(1_000_000_000_000_000, 1_000_000_000_000);
        f(10_000_000_000_000_000, 10_000_000_000_000);
        f(100_000_000_000_000_000, 100_000_000_000);
        f(1_000_000_000_000_000_000, 1_000_000_000_000);
    }

    #[derive(Debug, PartialEq)]
    struct OwnedRow {
        measurement: String,
        tags: Vec<(String, String)>,
        fields: Vec<(String, f64)>,
        timestamp: i64,
    }

    fn collect_rows(rows: &[Row<'_>], dst: &mut Vec<OwnedRow>) {
        for r in rows {
            dst.push(OwnedRow {
                measurement: r.measurement.to_string(),
                tags: r
                    .tags
                    .iter()
                    .map(|t| (t.key.to_string(), t.value.to_string()))
                    .collect(),
                fields: r
                    .fields
                    .iter()
                    .map(|f| (f.key.to_string(), f.value))
                    .collect(),
                timestamp: r.timestamp,
            });
        }
    }

    fn owned_row(
        measurement: &str,
        tags: &[(&str, &str)],
        fields: &[(&str, f64)],
        timestamp: i64,
    ) -> OwnedRow {
        OwnedRow {
            measurement: measurement.to_string(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            fields: fields.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            timestamp,
        }
    }

    const GOOD_DATA: &str = "foo1,location=us-midwest1 temperature=81 1727879909390000000\n\
foo2,location=us-midwest2 temperature=82 1727879909390000000\n\
foo3,location=us-midwest3 temperature=83 1727879909390000000";

    fn good_data_parsed() -> Vec<OwnedRow> {
        vec![
            owned_row(
                "foo1",
                &[("location", "us-midwest1")],
                &[("temperature", 81.0)],
                1727879909390,
            ),
            owned_row(
                "foo2",
                &[("location", "us-midwest2")],
                &[("temperature", 82.0)],
                1727879909390,
            ),
            owned_row(
                "foo3",
                &[("location", "us-midwest3")],
                &[("temperature", 83.0)],
                1727879909390,
            ),
        ]
    }

    #[test]
    fn test_parse_stream_good_data() {
        let mut rows = Vec::new();
        parse_stream(GOOD_DATA.as_bytes(), false, "ns", "test", |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, good_data_parsed());
    }

    #[test]
    fn test_parse_stream_bad_data_skips_invalid_lines() {
        let bad_data = "foo1,location=us-midwest1 temperature=81 1727879909390000000\n\
foo2, ,location=us-midwest2 temperature=82 1727879909390000000\n\
foo3,location=us-midwest3 temperature=83 1727879909390000000";
        let expected = vec![
            owned_row(
                "foo1",
                &[("location", "us-midwest1")],
                &[("temperature", 81.0)],
                1727879909390,
            ),
            owned_row(
                "foo3",
                &[("location", "us-midwest3")],
                &[("temperature", 83.0)],
                1727879909390,
            ),
        ];

        let mut rows = Vec::new();
        parse_stream(bad_data.as_bytes(), false, "ns", "test", |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, expected);
    }

    #[test]
    fn test_parse_stream_gzip_roundtrip() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(GOOD_DATA.as_bytes()).unwrap();
        let gzipped = encoder.finish().unwrap();

        let mut rows = Vec::new();
        parse_stream(&gzipped[..], true, "ns", "test", |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, good_data_parsed());
    }

    /// Reader yielding tiny chunks to exercise the tail-carry logic of
    /// `read_lines_block`.
    struct ChunkReader<'a> {
        data: &'a [u8],
        chunk: usize,
    }

    impl Read for ChunkReader<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.data.len().min(self.chunk).min(buf.len());
            buf[..n].copy_from_slice(&self.data[..n]);
            self.data = &self.data[n..];
            Ok(n)
        }
    }

    #[test]
    fn test_parse_stream_chunked_reads_carry_tail() {
        let mut data = String::new();
        let mut expected = Vec::new();
        for i in 0..500 {
            data.push_str(&format!("m,host=h{i} v={i} {}\n", 1727879909390i64 + i));
            expected.push(owned_row(
                "m",
                &[("host", &format!("h{i}"))],
                &[("v", i as f64)],
                1727879909390 + i,
            ));
        }
        let r = ChunkReader {
            data: data.as_bytes(),
            chunk: 7,
        };
        let mut rows = Vec::new();
        parse_stream(r, false, "ms", "test", |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, expected);
    }

    #[test]
    fn test_parse_stream_fills_missing_timestamp_with_now() {
        let before = current_time_millis();
        let mut rows = Vec::new();
        parse_stream("foo bar=1\n".as_bytes(), false, "ms", "test", |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        })
        .unwrap();
        let after = current_time_millis();
        assert_eq!(rows.len(), 1);
        let ts = rows[0].timestamp;
        assert!(
            ts >= before && ts <= after,
            "timestamp {ts} not in [{before}, {after}]"
        );
    }

    #[test]
    fn test_parse_stream_seconds_precision() {
        let mut rows = Vec::new();
        parse_stream("foo bar=1 5\n".as_bytes(), false, "s", "test", |rs| {
            collect_rows(rs, &mut rows);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, 5_000);
    }

    #[test]
    fn test_parse_stream_too_long_line() {
        let data = vec![b'a'; MAX_LINE_SIZE + 1024];
        let err = parse_stream(&data[..], false, "ns", "test", |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, Error::TooLongLine { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_stream_callback_error_propagates() {
        let err = parse_stream(GOOD_DATA.as_bytes(), false, "ns", "testdb", |_| {
            Err("boom".into())
        })
        .unwrap_err();
        match err {
            Error::Callback { db, source } => {
                assert_eq!(db, "testdb");
                assert_eq!(source.to_string(), "boom");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_parse_stream_empty_input() {
        let mut calls = 0;
        parse_stream("".as_bytes(), false, "ns", "test", |_| {
            calls += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(calls, 0);
    }
}
