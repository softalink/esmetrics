//! Whole-body and streaming decompression helpers for HTTP ingestion paths.
//!
//! Port of the upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/protoparserutil/compress_reader.go`.
//!
//! Deviations from the Go original:
//! - No reader/decoder pooling (Go pools decoders and buffers via
//!   `sync.Pool` for its shared-worker-pool architecture; this port targets
//!   a thread-per-connection model, matching the influx stream port).
//! - `esm-protoparser` must not depend on `esm-http`; callers pass the
//!   `Content-Encoding` value as a plain `&str` (see
//!   `esm_http::ContentEncoding::as_str()`), rather than a shared enum.

use std::fmt;
use std::io::{self, Read};

/// Default maximum size in bytes of a single insert request.
/// Go: `-maxInsertRequestSize` flag default.
pub const MAX_INSERT_REQUEST_SIZE: usize = 32 * 1024 * 1024;

/// Error returned by the decompression helpers in this module.
#[derive(Debug)]
pub enum UtilError {
    /// I/O error while reading from the underlying reader.
    Io(io::Error),
    /// The `Content-Encoding` value is not recognized.
    UnsupportedEncoding(String),
    /// The data (decompressed, unless noted otherwise) exceeds `limit`.
    TooBig { limit: usize },
    /// The compressed data could not be decoded.
    Decompress(String),
    /// The caller-supplied callback returned an error.
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for UtilError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UtilError::Io(err) => write!(f, "I/O error: {err}"),
            UtilError::UnsupportedEncoding(enc) => {
                write!(f, "unsupported Content-Encoding: {enc:?}")
            }
            UtilError::TooBig { limit } => {
                write!(f, "too big data size exceeding {limit} bytes")
            }
            UtilError::Decompress(msg) => write!(f, "cannot decompress data: {msg}"),
            UtilError::Callback(err) => write!(f, "error when processing request data: {err}"),
        }
    }
}

impl std::error::Error for UtilError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UtilError::Io(err) => Some(err),
            UtilError::Callback(err) => Some(err.as_ref()),
            UtilError::UnsupportedEncoding(_)
            | UtilError::TooBig { .. }
            | UtilError::Decompress(_) => None,
        }
    }
}

impl From<io::Error> for UtilError {
    fn from(err: io::Error) -> Self {
        UtilError::Io(err)
    }
}

/// Go: protoparserutil.ReadUncompressedData. Reads the whole body,
/// decompresses per `encoding` ("", "none", "identity", "gzip", "zstd",
/// "deflate", "snappy"), enforces `max_data_size` on the *decompressed*
/// size, then hands the bytes to `callback`.
///
/// For `zstd`/`snappy` the compressed body is read in full (also capped at
/// `max_data_size + 1` bytes) and block-decoded in one shot, matching the
/// upstream fast path. For the other encodings the raw body is streamed
/// through a decompressing reader and only the decompressed output is
/// capped, matching the upstream slow path.
///
/// Note: an over-limit `zstd` body may surface as [`UtilError::Decompress`]
/// rather than [`UtilError::TooBig`] (see [`zstd_decode`]); the other
/// encodings report the decompressed-size cap as `TooBig`.
pub fn read_uncompressed_data<R: Read>(
    r: R,
    encoding: &str,
    max_data_size: usize,
    callback: impl FnOnce(&[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(), UtilError> {
    let data = match encoding {
        "zstd" => {
            let raw = read_capped(r, max_data_size)?;
            zstd_decode(&raw, max_data_size)?
        }
        "snappy" => {
            let raw = read_capped(r, max_data_size)?;
            snappy_block_decode(&raw, max_data_size)?
        }
        _ => {
            let reader = uncompressed_reader(r, encoding)?;
            read_capped(reader, max_data_size)?
        }
    };
    callback(&data).map_err(UtilError::Callback)
}

/// Go: protoparserutil.GetUncompressedReader, for line-streaming parsers.
/// `snappy` is not supported here (upstream buffers it; callers use
/// [`read_uncompressed_data`] instead).
pub fn uncompressed_reader<'a, R: Read + 'a>(
    r: R,
    encoding: &str,
) -> Result<Box<dyn Read + 'a>, UtilError> {
    match encoding {
        "zstd" => {
            let decoder = zstd::stream::read::Decoder::new(r).map_err(|err| {
                UtilError::Decompress(format!("cannot create zstd decoder: {err}"))
            })?;
            Ok(Box::new(decoder))
        }
        "gzip" => Ok(Box::new(flate2::read::GzDecoder::new(r))),
        "deflate" => Ok(Box::new(flate2::read::ZlibDecoder::new(r))),
        "" | "none" | "identity" => Ok(Box::new(r)),
        other => Err(UtilError::UnsupportedEncoding(other.to_string())),
    }
}

/// Go: lib/encoding/snappy.Decode — block-format snappy with a
/// decompressed-size cap checked *before* allocating.
pub fn snappy_block_decode(src: &[u8], max_size: usize) -> Result<Vec<u8>, UtilError> {
    let decoded_len = snap::raw::decompress_len(src)
        .map_err(|err| UtilError::Decompress(format!("cannot read snappy header: {err}")))?;
    if decoded_len > max_size {
        return Err(UtilError::TooBig { limit: max_size });
    }
    let mut decoder = snap::raw::Decoder::new();
    decoder
        .decompress_vec(src)
        .map_err(|err| UtilError::Decompress(format!("cannot decode snappy block: {err}")))
}

/// Go: encoding.DecompressZSTDLimited.
///
/// Delegates to [`esm_encoding::decompress_zstd_limited`], the existing port
/// of the same upstream function (including its `window_log_max`
/// decompression-bomb guard). Its error is a flat string, so a size-cap
/// violation surfaces as [`UtilError::Decompress`] rather than
/// [`UtilError::TooBig`].
pub fn zstd_decode(src: &[u8], max_size: usize) -> Result<Vec<u8>, UtilError> {
    let mut dst = Vec::new();
    esm_encoding::decompress_zstd_limited(&mut dst, src, max_size)
        .map_err(UtilError::Decompress)?;
    Ok(dst)
}

/// Reads at most `max_size + 1` bytes from `r`, returning `TooBig` if more
/// than `max_size` bytes were available. This detects an oversized payload
/// without buffering it beyond the limit. Port of the `maxDataSize`-capped
/// `readFull` helper in the Go original.
///
/// Only the [`UtilError::Io`] and [`UtilError::TooBig`] variants are
/// returned. `pub(crate)` so `promremotewrite::parse` can reuse the same
/// raw-body cap semantics.
pub(crate) fn read_capped<R: Read>(mut r: R, max_size: usize) -> Result<Vec<u8>, UtilError> {
    let mut buf = Vec::new();
    let limit = (max_size as u64).saturating_add(1);
    r.by_ref().take(limit).read_to_end(&mut buf)?;
    if buf.len() > max_size {
        return Err(UtilError::TooBig { limit: max_size });
    }
    Ok(buf)
}

/// Default size in bytes of a single block returned by [`read_lines_block`].
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

/// Error from [`read_lines_block`]: either an I/O error, or a single line
/// (no newline found) exceeding `max_line_len`. Kept separate from
/// [`UtilError`] since it isn't about decompression; callers (`crate::stream`,
/// `crate::prometheus_stream`) convert it into their own parser-specific
/// `Error` type via `From`.
#[derive(Debug)]
pub(crate) enum LinesReadError {
    Io(io::Error),
    TooLongLine { max_line_len: usize },
}

impl From<io::Error> for LinesReadError {
    fn from(err: io::Error) -> Self {
        LinesReadError::Io(err)
    }
}

/// Reads a block of lines delimited by `\n` from `tail_buf` and `r` into
/// `dst_buf`. Trailing chars after the last newline are put into `tail_buf`.
///
/// Returns `Ok(true)` if a block is available in `dst_buf`, `Ok(false)` on
/// clean EOF. Port of Go `protoparserutil.ReadLinesBlockExt`. `pub(crate)`
/// so both `crate::stream` (influx) and `crate::prometheus_stream` share the
/// same chunked line-reading logic, matching how upstream's `stream.Parse`
/// implementations both call the same `protoparserutil.ReadLinesBlock`.
pub(crate) fn read_lines_block<R: Read>(
    r: &mut R,
    dst_buf: &mut Vec<u8>,
    tail_buf: &mut Vec<u8>,
    max_line_len: usize,
) -> Result<bool, LinesReadError> {
    dst_buf.clear();
    if dst_buf.capacity() < DEFAULT_BLOCK_SIZE {
        dst_buf.reserve(DEFAULT_BLOCK_SIZE);
    }
    dst_buf.extend_from_slice(tail_buf);
    tail_buf.clear();
    loop {
        let old_len = dst_buf.len();
        let cap = dst_buf.capacity();
        dst_buf.resize(cap, 0);
        let n = match r.read(&mut dst_buf[old_len..]) {
            Ok(n) => n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                dst_buf.truncate(old_len);
                continue;
            }
            Err(err) => {
                dst_buf.truncate(old_len);
                return Err(LinesReadError::Io(err));
            }
        };
        dst_buf.truncate(old_len + n);
        if n == 0 {
            // EOF.
            if !dst_buf.is_empty() {
                // Missing newline in the end of stream. This is OK: return
                // the pending data as the final block; the next call returns
                // clean EOF. See VictoriaMetrics issue #60.
                return Ok(true);
            }
            return Ok(false);
        }

        // Search for the last newline in the newly read data and put the
        // rest into tail_buf.
        match dst_buf[old_len..].iter().rposition(|&b| b == b'\n') {
            None => {
                // Didn't find at least a single line.
                if dst_buf.len() > max_line_len {
                    return Err(LinesReadError::TooLongLine { max_line_len });
                }
                if dst_buf.capacity() < 2 * dst_buf.len() {
                    // Increase dst_buf capacity, so more data could be read into it.
                    dst_buf.reserve(dst_buf.capacity());
                }
            }
            Some(pos) => {
                // Found at least a single line. Return it.
                let nn = old_len + pos;
                tail_buf.extend_from_slice(&dst_buf[nn + 1..]);
                dst_buf.truncate(nn);
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn payload(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 256) as u8).collect()
    }

    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn deflate_compress(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn zstd_compress(data: &[u8]) -> Vec<u8> {
        // Block (bulk) compression records the frame content size, like the
        // Go test's zstd.CompressLevel; streamed frames are covered by
        // read_uncompressed_data_zstd_streamed_frame_roundtrip.
        zstd::bulk::compress(data, 1).unwrap()
    }

    fn snappy_compress(data: &[u8]) -> Vec<u8> {
        let mut encoder = snap::raw::Encoder::new();
        encoder.compress_vec(data).unwrap()
    }

    fn assert_roundtrip(encoding: &str, encoded: &[u8], original: &[u8]) {
        let mut got: Option<Vec<u8>> = None;
        read_uncompressed_data(encoded, encoding, original.len(), |data| {
            got = Some(data.to_vec());
            Ok(())
        })
        .unwrap_or_else(|err| panic!("unexpected error for encoding={encoding:?}: {err}"));
        assert_eq!(got.as_deref(), Some(original), "encoding={encoding:?}");
    }

    #[test]
    fn read_uncompressed_data_gzip_roundtrip() {
        let data = payload(1024);
        let encoded = gzip_compress(&data);
        assert_roundtrip("gzip", &encoded, &data);
    }

    #[test]
    fn read_uncompressed_data_deflate_roundtrip() {
        let data = payload(1024);
        let encoded = deflate_compress(&data);
        assert_roundtrip("deflate", &encoded, &data);
    }

    #[test]
    fn read_uncompressed_data_zstd_roundtrip() {
        let data = payload(1024);
        let encoded = zstd_compress(&data);
        assert_roundtrip("zstd", &encoded, &data);
    }

    #[test]
    fn read_uncompressed_data_zstd_streamed_frame_roundtrip() {
        // Streamed zstd frames (no content size in the header, as produced
        // by typical HTTP clients compressing on the fly) must decode fine
        // under a realistic size limit despite the window_log_max guard in
        // esm_encoding::decompress_zstd_limited.
        let data = payload(1024);
        let encoded = zstd::encode_all(data.as_slice(), 1).unwrap();
        let mut got: Option<Vec<u8>> = None;
        read_uncompressed_data(encoded.as_slice(), "zstd", MAX_INSERT_REQUEST_SIZE, |d| {
            got = Some(d.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(got.as_deref(), Some(data.as_slice()));
    }

    #[test]
    fn read_uncompressed_data_snappy_roundtrip() {
        let data = payload(1024);
        let encoded = snappy_compress(&data);
        assert_roundtrip("snappy", &encoded, &data);
    }

    #[test]
    fn read_uncompressed_data_identity_roundtrip() {
        let data = payload(1024);
        assert_roundtrip("", &data, &data);
        assert_roundtrip("none", &data, &data);
        assert_roundtrip("identity", &data, &data);
    }

    #[test]
    fn too_big_decompressed_data_is_rejected() {
        let data = payload(2048);
        let encoded = zstd_compress(&data);
        let err = read_uncompressed_data(encoded.as_slice(), "zstd", 1024, |_| {
            panic!("callback must not run when data is too big")
        })
        .unwrap_err();
        // The decompressed-size cap for zstd is enforced by
        // esm_encoding::decompress_zstd_limited, whose flat string error maps
        // to Decompress; TooBig is still possible when the raw read cap trips
        // first.
        assert!(
            matches!(
                err,
                UtilError::TooBig { limit: 1024 } | UtilError::Decompress(_)
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn too_big_streamed_decompressed_data_is_rejected() {
        // The gzip/deflate/identity slow path caps the *decompressed* stream
        // via read_capped, which reports TooBig deterministically (unlike the
        // zstd fast path, where the cap lives in esm-encoding).
        let data = payload(2048);
        let encoded = gzip_compress(&data);
        let err = read_uncompressed_data(encoded.as_slice(), "gzip", 1024, |_| {
            panic!("callback must not run when data is too big")
        })
        .unwrap_err();
        assert!(
            matches!(err, UtilError::TooBig { limit: 1024 }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn unsupported_encoding_is_rejected() {
        let err = read_uncompressed_data(b"foo bar baz".as_slice(), "br", 10_000, |_| {
            panic!("callback must not run for unsupported encoding")
        })
        .unwrap_err();
        assert!(
            matches!(err, UtilError::UnsupportedEncoding(ref enc) if enc == "br"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn snappy_size_cap_checked_before_alloc() {
        // Hand-craft a snappy block header claiming a huge decoded length
        // (a few bytes, varint-encoded) without providing the actual
        // compressed payload for that length. If the size cap were only
        // checked after decompressing, this would attempt an enormous
        // allocation (or read past the buffer) instead of failing fast.
        let huge_len: u64 = 4_000_000_000; // ~4GB, far above any sane cap.
        let mut src = Vec::new();
        let mut n = huge_len;
        loop {
            let mut byte = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                byte |= 0x80;
            }
            src.push(byte);
            if n == 0 {
                break;
            }
        }
        // No compressed body follows; decompress_len must reject this before
        // any allocation is attempted.
        let err = snappy_block_decode(&src, 1024).unwrap_err();
        assert!(
            matches!(err, UtilError::TooBig { limit: 1024 }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn uncompressed_reader_rejects_snappy() {
        let err = match uncompressed_reader(b"".as_slice(), "snappy") {
            Err(err) => err,
            Ok(_) => panic!("expected UnsupportedEncoding for snappy"),
        };
        assert!(
            matches!(err, UtilError::UnsupportedEncoding(ref enc) if enc == "snappy"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn uncompressed_reader_rejects_unsupported() {
        let err = match uncompressed_reader(b"".as_slice(), "unsupported") {
            Err(err) => err,
            Ok(_) => panic!("expected UnsupportedEncoding for unsupported"),
        };
        assert!(
            matches!(err, UtilError::UnsupportedEncoding(ref enc) if enc == "unsupported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn uncompressed_reader_gzip_streams() {
        use std::io::Read;

        let data = payload(4096);
        let encoded = gzip_compress(&data);
        let mut reader = uncompressed_reader(encoded.as_slice(), "gzip").unwrap();
        let mut got = Vec::new();
        reader.read_to_end(&mut got).unwrap();
        assert_eq!(got, data);
    }

    /// Reader yielding fixed-size chunks, to make `read_lines_block` see
    /// partial lines across reads.
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
    fn read_lines_block_carries_tail_between_calls() {
        // A 5-byte chunked reader splits "abc\ndefgh\nij" mid-line; the
        // partial line after the last newline of each block must be carried
        // into the next block via tail_buf.
        let mut r = ChunkReader {
            data: b"abc\ndefgh\nij",
            chunk: 5,
        };
        let mut dst = Vec::new();
        let mut tail = Vec::new();

        // First read gets "abc\nd": block is "abc", tail carries "d".
        assert!(read_lines_block(&mut r, &mut dst, &mut tail, 1024).unwrap());
        assert_eq!(dst, b"abc");
        assert_eq!(tail, b"d");

        // Next block starts from the carried "d" and reads the next chunk
        // "efgh\n" through the second newline: block is "defgh".
        assert!(read_lines_block(&mut r, &mut dst, &mut tail, 1024).unwrap());
        assert_eq!(dst, b"defgh");
        assert_eq!(tail, b"");

        // The trailing "ij" (no newline) becomes the final block at EOF.
        assert!(read_lines_block(&mut r, &mut dst, &mut tail, 1024).unwrap());
        assert_eq!(dst, b"ij");
        assert!(!read_lines_block(&mut r, &mut dst, &mut tail, 1024).unwrap());
    }

    #[test]
    fn read_lines_block_returns_final_block_without_trailing_newline() {
        // Missing newline at end of stream: the pending bytes are returned
        // as the final block (VictoriaMetrics issue #60), then clean EOF.
        let mut r: &[u8] = b"abc\ndef";
        let mut dst = Vec::new();
        let mut tail = Vec::new();

        assert!(read_lines_block(&mut r, &mut dst, &mut tail, 1024).unwrap());
        assert_eq!(dst, b"abc");
        assert!(read_lines_block(&mut r, &mut dst, &mut tail, 1024).unwrap());
        assert_eq!(dst, b"def");
        assert!(!read_lines_block(&mut r, &mut dst, &mut tail, 1024).unwrap());
    }

    #[test]
    fn read_lines_block_rejects_too_long_line() {
        // No newline within max_line_len bytes: TooLongLine.
        let data = vec![b'a'; 64];
        let mut r: &[u8] = &data;
        let mut dst = Vec::new();
        let mut tail = Vec::new();

        let err = read_lines_block(&mut r, &mut dst, &mut tail, 16).unwrap_err();
        assert!(
            matches!(err, LinesReadError::TooLongLine { max_line_len: 16 }),
            "unexpected error: {err:?}"
        );
    }
}
