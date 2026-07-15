//! Prometheus remote-write stream decode.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/promremotewrite/stream/streamparser.go`.
//!
//! Deviations from the Go original:
//! - `encoding` (the `Content-Encoding` header value) replaces
//!   `isVMRemoteWrite bool`. Upstream computes `isVMRemoteWrite` as
//!   `req.Header.Get("Content-Encoding") == "zstd"` at the HTTP handler
//!   (`app/vminsert/promremotewrite/request_handler.go`), so `encoding ==
//!   "zstd"` selects the zstd-then-snappy fallback order here and any other
//!   value selects snappy-then-zstd — the same decision, just made inside
//!   this function instead of by the caller.
//! - The raw body is read via `util::read_capped` (made `pub(crate)` for
//!   this) rather than through `util::read_uncompressed_data`: upstream's
//!   `Parse` reads the raw body with `contentType=""` (identity, no
//!   decompression) and only `parseRequestBody` performs the snappy/zstd
//!   fallback dance, so the two phases are kept separate here too.
//! - `WriteRequest.Metadata` is out of scope (see `prompb` module
//!   deviations): the callback only receives timeseries, not metadata, and
//!   there is no `vm_protoparser_metadata_read_total` counter.
//! - No metrics counters (`vm_protoparser_*`) and no object pooling
//!   (`bodyBufferPool`, `WriteRequestUnmarshaler` pooling) — out of scope for
//!   this port; plain `Vec<u8>` allocations are used instead.
//! - `Error::Unmarshal` is an added variant beyond the four sketched in the
//!   task brief (Io/Decompress/TooBig/Callback): upstream has a distinct
//!   `cannot unmarshal prompb.WriteRequest ...` failure path, separate from
//!   the decompress-fallback failure, so folding it into `Decompress` would
//!   conflate two unrelated causes.

use std::fmt;
use std::io::{self, Read};

use crate::prompb::{self, TimeSeries, WireError};
use crate::util::{self, UtilError, MAX_INSERT_REQUEST_SIZE};

/// Error returned by [`parse`].
#[derive(Debug)]
pub enum Error {
    /// I/O error while reading the request body.
    Io(io::Error),
    /// The body exceeds `MAX_INSERT_REQUEST_SIZE` bytes. `actual` is `None`
    /// when the *raw* body tripped the read cap (its true size is unknown;
    /// reading stops at `limit + 1` bytes, Go `readFull`), or `Some(n)` when
    /// the *decompressed* body of `n` bytes tripped the post-decompress
    /// re-check (Go `parseRequestBody`).
    TooBig { limit: usize, actual: Option<usize> },
    /// Both snappy and zstd decompression failed; carries the error from the
    /// *first* attempted codec, matching upstream `parseRequestBody` (the
    /// fallback codec's error is discarded, only used to decide whether the
    /// fallback itself succeeded).
    Decompress {
        encoding: &'static str,
        len: usize,
        source: String,
    },
    /// The decompressed body could not be unmarshaled as a
    /// `prompb::WriteRequest`.
    Unmarshal { len: usize, source: WireError },
    /// The caller-supplied callback returned an error.
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => {
                write!(f, "cannot read prometheus remote_write data from client: {err}")
            }
            Error::TooBig {
                limit,
                actual: Some(actual),
            } => write!(
                f,
                "too big unpacked request; mustn't exceed `-maxInsertRequestSize={limit}` bytes; got {actual} bytes"
            ),
            Error::TooBig {
                limit,
                actual: None,
            } => write!(
                f,
                "too big data size exceeding `-maxInsertRequestSize={limit}` bytes"
            ),
            Error::Decompress {
                encoding,
                len,
                source,
            } => write!(
                f,
                "cannot decompress {encoding}-encoded request with length {len}: {source}"
            ),
            Error::Unmarshal { len, source } => write!(
                f,
                "cannot unmarshal prompb.WriteRequest with size {len} bytes: {source}"
            ),
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
            Error::TooBig { .. } | Error::Decompress { .. } => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

/// Parses a Prometheus remote_write message from `r` and calls `callback`
/// with the parsed timeseries.
///
/// Go: `stream.Parse`. `encoding` is the `Content-Encoding` header value:
/// `"zstd"` tries zstd-then-snappy; anything else tries snappy-then-zstd
/// (vmagent persistent-queue compatibility, see upstream issues #5301 and
/// #8650 — a `vmagent` that queued a message before a header-format change
/// may resend it compressed with the "wrong" codec for its header).
///
/// The callback must not hold on to the series after returning; they borrow
/// the decompressed buffer, which is dropped once `parse` returns.
pub fn parse<R: Read>(
    r: R,
    encoding: &str,
    callback: impl FnOnce(&[TimeSeries<'_>]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(), Error> {
    let raw = util::read_capped(r, MAX_INSERT_REQUEST_SIZE).map_err(|err| match err {
        UtilError::Io(err) => Error::Io(err),
        UtilError::TooBig { limit } => Error::TooBig {
            limit,
            actual: None,
        },
        UtilError::UnsupportedEncoding(_) | UtilError::Decompress(_) | UtilError::Callback(_) => {
            unreachable!("read_capped returns only Io or TooBig")
        }
    })?;
    let decompressed = decompress_with_fallback(&raw, encoding)?;
    if decompressed.len() > MAX_INSERT_REQUEST_SIZE {
        // Unreachable in practice: both decode helpers already enforce the
        // cap on their output. Kept because Go re-checks `len(bb.B)` here
        // too, and the helpers' cap is their contract, not this module's.
        return Err(Error::TooBig {
            limit: MAX_INSERT_REQUEST_SIZE,
            actual: Some(decompressed.len()),
        });
    }
    let wr = prompb::unmarshal_write_request(&decompressed).map_err(|source| Error::Unmarshal {
        len: decompressed.len(),
        source,
    })?;
    callback(&wr.timeseries).map_err(Error::Callback)
}

/// Go: `parseRequestBody`'s snappy/zstd mutual-fallback logic. Reports the
/// *first* attempted codec's error when both fail.
fn decompress_with_fallback(data: &[u8], encoding: &str) -> Result<Vec<u8>, Error> {
    if encoding == "zstd" {
        match util::zstd_decode(data, MAX_INSERT_REQUEST_SIZE) {
            Ok(decoded) => Ok(decoded),
            Err(zstd_err) => {
                util::snappy_block_decode(data, MAX_INSERT_REQUEST_SIZE).map_err(|_| {
                    Error::Decompress {
                        encoding: "zstd",
                        len: data.len(),
                        source: zstd_err.to_string(),
                    }
                })
            }
        }
    } else {
        match util::snappy_block_decode(data, MAX_INSERT_REQUEST_SIZE) {
            Ok(decoded) => Ok(decoded),
            Err(snappy_err) => {
                util::zstd_decode(data, MAX_INSERT_REQUEST_SIZE).map_err(|_| Error::Decompress {
                    encoding: "snappy",
                    len: data.len(),
                    source: snappy_err.to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- tiny protobuf wire-writer test helpers (no protobuf dependency) ---
    // Replicated locally from `prompb::tests`, which are not `pub` across
    // modules.

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

    fn append_bytes_field(dst: &mut Vec<u8>, field_num: u32, data: &[u8]) {
        append_tag(dst, field_num, 2);
        append_varint(dst, data.len() as u64);
        dst.extend_from_slice(data);
    }

    fn append_double_field(dst: &mut Vec<u8>, field_num: u32, v: f64) {
        append_tag(dst, field_num, 1);
        dst.extend_from_slice(&v.to_le_bytes());
    }

    fn append_varint_field(dst: &mut Vec<u8>, field_num: u32, v: i64) {
        append_tag(dst, field_num, 0);
        append_varint(dst, v as u64);
    }

    fn encode_label(name: &[u8], value: &[u8]) -> Vec<u8> {
        let mut dst = Vec::new();
        append_bytes_field(&mut dst, 1, name);
        append_bytes_field(&mut dst, 2, value);
        dst
    }

    fn encode_sample(value: f64, timestamp: i64) -> Vec<u8> {
        let mut dst = Vec::new();
        append_double_field(&mut dst, 1, value);
        append_varint_field(&mut dst, 2, timestamp);
        dst
    }

    fn encode_time_series(labels: &[(&[u8], &[u8])], samples: &[(f64, i64)]) -> Vec<u8> {
        let mut dst = Vec::new();
        for (name, value) in labels {
            append_bytes_field(&mut dst, 1, &encode_label(name, value));
        }
        for (value, ts) in samples {
            append_bytes_field(&mut dst, 2, &encode_sample(*value, *ts));
        }
        dst
    }

    fn encode_write_request(timeseries: &[Vec<u8>]) -> Vec<u8> {
        let mut dst = Vec::new();
        for ts in timeseries {
            append_bytes_field(&mut dst, 1, ts);
        }
        dst
    }

    /// One-series `WriteRequest` used across the tests below.
    fn one_series_write_request() -> Vec<u8> {
        let ts = encode_time_series(
            &[(b"__name__", b"foo"), (b"job", b"x")],
            &[(42.5, 1727879909390)],
        );
        encode_write_request(&[ts])
    }

    fn assert_one_series(tss: &[TimeSeries<'_>]) {
        assert_eq!(tss.len(), 1);
        assert_eq!(
            tss[0].labels,
            vec![
                prompb::Label {
                    name: b"__name__",
                    value: b"foo"
                },
                prompb::Label {
                    name: b"job",
                    value: b"x"
                },
            ]
        );
        assert_eq!(
            tss[0].samples,
            vec![prompb::Sample {
                value: 42.5,
                timestamp: 1727879909390,
            }]
        );
    }

    #[test]
    fn parses_snappy_body() {
        let wr = one_series_write_request();
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(&wr).unwrap();

        let mut seen = 0;
        parse(compressed.as_slice(), "", |tss| {
            assert_one_series(tss);
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn parses_zstd_body_with_zstd_encoding() {
        let wr = one_series_write_request();
        let compressed = zstd::bulk::compress(&wr, 1).unwrap();

        let mut seen = 0;
        parse(compressed.as_slice(), "zstd", |tss| {
            assert_one_series(tss);
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn snappy_body_with_zstd_header_falls_back() {
        // Upstream issue #5301: a vmagent persistent queue may hold a
        // snappy-compressed body tagged with a stale 'Content-Encoding:
        // zstd' header from before a vmagent restart. zstd decode must fail
        // and fall back to snappy.
        let wr = one_series_write_request();
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(&wr).unwrap();

        let mut seen = 0;
        parse(compressed.as_slice(), "zstd", |tss| {
            assert_one_series(tss);
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn zstd_body_without_zstd_header_falls_back() {
        // The mirror image of #5301 (see upstream issue comment referenced
        // in streamparser.go): a zstd body arrives without the
        // 'Content-Encoding: zstd' header. Snappy decode must fail and fall
        // back to zstd.
        let wr = one_series_write_request();
        let compressed = zstd::bulk::compress(&wr, 1).unwrap();

        let mut seen = 0;
        parse(compressed.as_slice(), "", |tss| {
            assert_one_series(tss);
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn decompressed_size_limit_enforced() {
        // A snappy block whose header claims a huge decoded length trips the
        // cap deterministically before any real decompression work (see
        // util::snappy_size_cap_checked_before_alloc). zstd_decode may
        // surface an over-limit body as Decompress rather than TooBig
        // (documented on util::zstd_decode), so this is asserted against
        // snappy where the boundary is deterministic.
        let (body, snappy_err, _zstd_err) = body_failing_both_codecs();
        assert!(
            snappy_err.contains("too big"),
            "expected the snappy cap error, got: {snappy_err}"
        );

        let err = parse(body.as_slice(), "", |_| {
            panic!("callback must not run when the body is too big")
        })
        .unwrap_err();
        match err {
            Error::Decompress {
                encoding, source, ..
            } => {
                assert_eq!(encoding, "snappy");
                assert_eq!(source, snappy_err);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn garbage_body_errors() {
        let err = parse(
            b"this is not a valid snappy or zstd frame".as_slice(),
            "",
            |_| panic!("callback must not run for garbage input"),
        )
        .unwrap_err();
        match err {
            Error::Decompress { encoding, .. } => assert_eq!(encoding, "snappy"),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn unmarshal_error_reported_separately_from_decompress() {
        // Valid snappy compression of bytes that are not a valid
        // WriteRequest protobuf (a length-delimited field 1 whose declared
        // length runs past the end of the buffer) must surface as
        // Error::Unmarshal, not Error::Decompress.
        let mut garbage = Vec::new();
        append_tag(&mut garbage, 1, 2);
        append_varint(&mut garbage, 100); // claims 100 bytes but none follow
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(&garbage).unwrap();

        let err = parse(compressed.as_slice(), "", |_| {
            panic!("callback must not run when unmarshal fails")
        })
        .unwrap_err();
        assert!(
            matches!(err, Error::Unmarshal { .. }),
            "unexpected error: {err}"
        );
    }

    /// A body that fails BOTH codecs, with distinguishable per-codec errors
    /// (snappy: length-header cap trip; zstd: invalid frame).
    fn body_failing_both_codecs() -> (Vec<u8>, String, String) {
        let huge_len: u64 = 4_000_000_000; // ~4GB, far above MAX_INSERT_REQUEST_SIZE.
        let mut body = Vec::new();
        let mut n = huge_len;
        loop {
            let mut byte = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                byte |= 0x80;
            }
            body.push(byte);
            if n == 0 {
                break;
            }
        }
        let snappy_err = util::snappy_block_decode(&body, MAX_INSERT_REQUEST_SIZE)
            .unwrap_err()
            .to_string();
        let zstd_err = util::zstd_decode(&body, MAX_INSERT_REQUEST_SIZE)
            .unwrap_err()
            .to_string();
        // Sanity: the two codecs must fail differently, or the precedence
        // assertions below prove nothing.
        assert_ne!(snappy_err, zstd_err);
        (body, snappy_err, zstd_err)
    }

    #[test]
    fn zstd_header_both_codecs_fail_reports_zstd_error() {
        // encoding == "zstd": zstd is attempted first, so when the snappy
        // fallback also fails, the surfaced error must be zstd's (upstream
        // parseRequestBody keeps `zstdErr` and discards the fallback error).
        let (body, _snappy_err, zstd_err) = body_failing_both_codecs();

        let err = parse(body.as_slice(), "zstd", |_| {
            panic!("callback must not run when both codecs fail")
        })
        .unwrap_err();
        match err {
            Error::Decompress {
                encoding,
                len,
                source,
            } => {
                assert_eq!(encoding, "zstd");
                assert_eq!(len, body.len());
                assert_eq!(
                    source, zstd_err,
                    "must carry the first-attempted codec's error"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn no_header_both_codecs_fail_reports_snappy_error() {
        // No zstd header: snappy is attempted first, so when the zstd
        // fallback also fails, the surfaced error must be snappy's (upstream
        // parseRequestBody keeps `snappyErr` and discards the fallback error).
        let (body, snappy_err, _zstd_err) = body_failing_both_codecs();

        let err = parse(body.as_slice(), "", |_| {
            panic!("callback must not run when both codecs fail")
        })
        .unwrap_err();
        match err {
            Error::Decompress {
                encoding,
                len,
                source,
            } => {
                assert_eq!(encoding, "snappy");
                assert_eq!(len, body.len());
                assert_eq!(
                    source, snappy_err,
                    "must carry the first-attempted codec's error"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn read_capped_allows_body_exactly_at_cap() {
        let body = [7u8; 8];
        let got = util::read_capped(body.as_slice(), 8).unwrap();
        assert_eq!(got, body);
    }

    #[test]
    fn read_capped_rejects_body_over_cap() {
        let body = [7u8; 9];
        let err = util::read_capped(body.as_slice(), 8).unwrap_err();
        assert!(
            matches!(err, util::UtilError::TooBig { limit: 8 }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn raw_body_over_insert_cap_errors() {
        // parse's raw-read cap is fixed at MAX_INSERT_REQUEST_SIZE; a body
        // one byte over it must trip Error::TooBig without any decompression
        // attempt (the boundary itself is covered with a small synthetic cap
        // by the read_capped_* tests above).
        let body = vec![0u8; MAX_INSERT_REQUEST_SIZE + 1];
        let err = parse(body.as_slice(), "", |_| {
            panic!("callback must not run when the raw body is too big")
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::TooBig {
                    limit: MAX_INSERT_REQUEST_SIZE,
                    actual: None,
                }
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_body_decodes_to_zero_series() {
        // An empty body fails snappy ("corrupt input (empty)") but the zstd
        // fallback decodes it as zero frames → empty output, matching Go's
        // klauspost behavior (verified against upstream lib/encoding/zstd):
        // the callback runs with an empty WriteRequest.
        let mut calls = 0;
        parse(b"".as_slice(), "", |tss| {
            calls += 1;
            assert!(tss.is_empty(), "empty body must decode to zero series");
            Ok(())
        })
        .unwrap();
        assert_eq!(calls, 1);
    }

    #[test]
    fn callback_error_propagates() {
        let wr = one_series_write_request();
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(&wr).unwrap();

        let err = parse(compressed.as_slice(), "", |_| Err("boom".into())).unwrap_err();
        match err {
            Error::Callback(source) => assert_eq!(source.to_string(), "boom"),
            other => panic!("unexpected error: {other}"),
        }
    }
}
