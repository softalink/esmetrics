//! Streaming Prometheus exposition-text parser.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/prometheus/stream/streamparser.go`, plus the needed
//! pieces of `lib/protoparser/protoparserutil` (`GetUncompressedReader`,
//! `ReadLinesBlock`), adapted to a synchronous Rust API.
//!
//! This is a sibling module of [`crate::prometheus`] (rather than folded
//! into it) purely to stay under the file-size guideline — `prometheus.rs`
//! is already ~770 lines. Its public items are re-exported from
//! `crate::prometheus`, so callers use `esm_protoparser::prometheus::parse_stream`
//! like every other symbol in this parser.
//!
//! Deviations from the Go original:
//! - Only the row callback is ported; OpenMetrics metadata
//!   (`UnmarshalWithMetadata` / `prommetadata`) is out of scope, matching the
//!   scope cut already made in `crate::prometheus`.
//! - The current time is taken from [`std::time::SystemTime`] instead of the
//!   Go `fasttime` cached clock (same deviation as `crate::stream`).
//! - Unlike `crate::stream` (influx), which only decompresses gzip inline,
//!   `encoding` here is dispatched through [`crate::util::uncompressed_reader`],
//!   so gzip/zstd/deflate/identity are all supported, matching upstream
//!   `protoparserutil.GetUncompressedReader` (what the real `stream.Parse`
//!   calls).
//! - No `Rows`/`unmarshalWork` object pooling across blocks (Go pools both
//!   via `sync.Pool`); a fresh `Rows` is used for each block, same
//!   simplification as `crate::stream`.

use std::fmt;
use std::io::{self, Read};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::prometheus::{Row, Rows};
use crate::util::{self, LinesReadError, UtilError};

/// Go: `protoparserutil.maxLineSize` (used by the `ReadLinesBlock` — not
/// `Ext` — call in the real `streamContext.Read`).
const MAX_LINE_SIZE: usize = 256 * 1024;

/// Error returned by [`parse_stream`].
#[derive(Debug)]
pub enum Error {
    /// Cannot read Prometheus exposition data.
    Io(io::Error),
    /// A single line exceeds the maximum allowed size.
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
            Error::Io(err) => write!(f, "cannot read Prometheus exposition data: {err}"),
            Error::TooLongLine { max_line_len } => {
                write!(f, "too long line: more than {max_line_len} bytes")
            }
            Error::Utf8(err) => {
                write!(f, "Prometheus exposition data is not valid UTF-8: {err}")
            }
            Error::Decode(msg) => {
                write!(f, "cannot decode Prometheus text exposition data: {msg}")
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

/// Parses Prometheus exposition-text data from `r` in stream mode and calls
/// `callback` for each parsed block of rows.
///
/// Go: `stream.Parse`. Invalid lines are skipped (stream-mode semantics);
/// each skipped line's formatted error is passed to `err_logger`. Row
/// timestamps left at `0` by the parser (no explicit timestamp in the input)
/// are filled with `default_timestamp` if positive, or the current time
/// otherwise — Go: `unmarshalWork.Unmarshal`'s "Fill missing timestamps"
/// step.
///
/// The callback must not hold on to the rows after returning; they borrow
/// the internal read buffer, which is reused for the next block.
pub fn parse_stream<R: Read>(
    r: R,
    encoding: &str,
    default_timestamp: i64,
    mut err_logger: impl FnMut(&str),
    mut callback: impl FnMut(&[Row<'_>]) -> CallbackResult,
) -> Result<(), Error> {
    let reader = util::uncompressed_reader(r, encoding).map_err(|err| match err {
        UtilError::UnsupportedEncoding(enc) => {
            Error::Decode(format!("unsupported Content-Encoding: {enc:?}"))
        }
        other => Error::Decode(other.to_string()),
    })?;
    parse_stream_internal(reader, default_timestamp, &mut err_logger, &mut callback)
}

fn parse_stream_internal<R, F>(
    mut r: R,
    default_timestamp: i64,
    err_logger: &mut impl FnMut(&str),
    callback: &mut F,
) -> Result<(), Error>
where
    R: Read,
    F: FnMut(&[Row<'_>]) -> CallbackResult,
{
    let mut req_buf: Vec<u8> = Vec::new();
    let mut tail_buf: Vec<u8> = Vec::new();
    while util::read_lines_block(&mut r, &mut req_buf, &mut tail_buf, MAX_LINE_SIZE)? {
        let block = std::str::from_utf8(&req_buf).map_err(Error::Utf8)?;
        // TODO: pool `Rows` across blocks (Go uses sync.Pool via
        // `unmarshalWork`); a lifetime-erased pool is needed for that. The
        // parsed strings themselves are zero-copy borrows of `req_buf`.
        let mut rows = Rows::default();
        rows.unmarshal(block, |msg| err_logger(msg));
        fill_missing_timestamps(rows.rows_mut(), default_timestamp);
        if let Err(source) = callback(rows.rows()) {
            return Err(Error::Callback(source));
        }
    }
    Ok(())
}

/// Fills rows whose timestamp is `0` (no timestamp in the input) with
/// `default_timestamp`, or the current time if `default_timestamp <= 0`.
/// Port of the "Fill missing timestamps" step in Go `unmarshalWork.Unmarshal`.
fn fill_missing_timestamps(rows: &mut [Row<'_>], default_timestamp: i64) {
    let default_timestamp = if default_timestamp <= 0 {
        current_time_millis()
    } else {
        default_timestamp
    };
    for row in rows {
        if row.timestamp == 0 {
            row.timestamp = default_timestamp;
        }
    }
}

fn current_time_millis() -> i64 {
    // NOTE: Go uses `time.Now().UnixNano() / 1e6`; a cached clock (fasttime)
    // is used elsewhere in the upstream. SystemTime is used here, matching
    // `crate::stream`.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

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

    const GOOD_DATA: &str = "foo1{location=\"us-midwest1\"} 81 1727879909390\n\
foo2{location=\"us-midwest2\"} 82 1727879909390\n\
foo3{location=\"us-midwest3\"} 83 1727879909390";

    fn good_data_parsed() -> Vec<OwnedRow> {
        vec![
            owned_row("foo1", &[("location", "us-midwest1")], 81.0, 1727879909390),
            owned_row("foo2", &[("location", "us-midwest2")], 82.0, 1727879909390),
            owned_row("foo3", &[("location", "us-midwest3")], 83.0, 1727879909390),
        ]
    }

    #[test]
    fn parse_stream_good_data() {
        let mut rows = Vec::new();
        parse_stream(
            GOOD_DATA.as_bytes(),
            "",
            0,
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
    fn invalid_lines_are_skipped() {
        let bad_data = "foo1 1 100\n\
{missing_metric=\"x\"} 2\n\
foo3 3 300";
        let mut rows = Vec::new();
        let mut errs = 0;
        parse_stream(
            bad_data.as_bytes(),
            "",
            0,
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
                owned_row("foo1", &[], 1.0, 100_000),
                owned_row("foo3", &[], 3.0, 300_000)
            ]
        );
        assert_eq!(errs, 1);
    }

    #[test]
    fn gzip_roundtrip() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(GOOD_DATA.as_bytes()).unwrap();
        let gzipped = encoder.finish().unwrap();

        let mut rows = Vec::new();
        parse_stream(
            &gzipped[..],
            "gzip",
            0,
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
    fn zstd_roundtrip() {
        let compressed = zstd::bulk::compress(GOOD_DATA.as_bytes(), 1).unwrap();
        let mut rows = Vec::new();
        parse_stream(
            compressed.as_slice(),
            "zstd",
            0,
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
    fn unsupported_encoding_errors() {
        let err = parse_stream(b"foo 1\n".as_slice(), "br", 0, |_| {}, |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, Error::Decode(ref msg) if msg.contains("br")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn explicit_default_timestamp_fills_zero_rows() {
        let mut rows = Vec::new();
        parse_stream(
            "foo 1\n".as_bytes(),
            "",
            1_700_000_000_000,
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, 1_700_000_000_000);
    }

    #[test]
    fn missing_default_timestamp_fills_zero_rows_with_now() {
        let before = current_time_millis();
        let mut rows = Vec::new();
        parse_stream(
            "foo 1\n".as_bytes(),
            "",
            0,
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
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
    fn explicit_row_timestamp_is_not_overwritten() {
        let mut rows = Vec::new();
        parse_stream(
            "foo 1 12345\n".as_bytes(),
            "",
            1_700_000_000_000,
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, 12_345_000);
    }

    #[test]
    fn too_long_line_errors() {
        let data = vec![b'a'; MAX_LINE_SIZE + 1024];
        let err = parse_stream(&data[..], "", 0, |_| {}, |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, Error::TooLongLine { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn callback_error_propagates() {
        let err =
            parse_stream(GOOD_DATA.as_bytes(), "", 0, |_| {}, |_| Err("boom".into())).unwrap_err();
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
            "",
            0,
            |_| {},
            |_| {
                calls += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(calls, 0);
    }

    /// Reader yielding tiny chunks to exercise the tail-carry logic of
    /// `util::read_lines_block`.
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
    fn chunked_reads_carry_tail() {
        let mut data = String::new();
        let mut expected = Vec::new();
        for i in 0..500 {
            data.push_str(&format!(
                "m{{host=\"h{i}\"}} {i} {}\n",
                1727879909390i64 + i
            ));
            expected.push(owned_row(
                "m",
                &[("host", &format!("h{i}"))],
                i as f64,
                1727879909390 + i,
            ));
        }
        let r = ChunkReader {
            data: data.as_bytes(),
            chunk: 7,
        };
        let mut rows = Vec::new();
        parse_stream(
            r,
            "",
            0,
            |msg| panic!("{msg}"),
            |rs| {
                collect_rows(rs, &mut rows);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows, expected);
    }
}
