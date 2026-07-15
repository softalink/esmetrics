//! ZSTD block compression. Port of Go lib/encoding/compress.go +
//! lib/encoding/zstd/zstd_pure.go, plus util.go `IsZstd`.
//!
//! Uses reusable per-thread bulk (de)compression contexts to avoid per-call
//! context allocations on the hot path — the Rust analogue of Go's cached
//! encoder/decoder maps.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;

use zstd::bulk::{Compressor, Decompressor};

thread_local! {
    static COMPRESSORS: RefCell<HashMap<i32, Compressor<'static>>> =
        RefCell::new(HashMap::new());
    static DECOMPRESSOR: RefCell<Option<Decompressor<'static>>> = const { RefCell::new(None) };
}

/// Appends compressed `src` to `dst`.
///
/// The given `compress_level` is used for the compression.
///
/// Go: CompressZSTDLevel (metrics counters are not ported).
pub fn compress_zstd_level(dst: &mut Vec<u8>, src: &[u8], compress_level: i32) {
    let bound = zstd::zstd_safe::compress_bound(src.len());
    let dst_len = dst.len();
    dst.resize(dst_len + bound, 0);
    let n = COMPRESSORS.with(|cell| {
        let mut m = cell.borrow_mut();
        let c = m.entry(compress_level).or_insert_with(|| {
            Compressor::new(compress_level).expect("BUG: failed to create ZSTD compressor")
        });
        c.compress_to_buffer(src, &mut dst[dst_len..])
    });
    let n = n.expect("BUG: ZSTD compression into a compress_bound-sized buffer must not fail");
    dst.truncate(dst_len + n);
}

/// Decompresses `src`, appending the result to `dst`.
///
/// This function must be called only for trusted `src`.
/// Use [`decompress_zstd_limited`] for untrusted `src`.
///
/// Go: DecompressZSTD.
pub fn decompress_zstd(dst: &mut Vec<u8>, src: &[u8]) -> Result<(), String> {
    decompress_zstd_internal(dst, src)
        .map_err(|err| format!("cannot decompress zstd block with len={}: {err}", src.len()))
}

fn decompress_zstd_internal(dst: &mut Vec<u8>, src: &[u8]) -> Result<(), String> {
    if src.is_empty() {
        // Go's klauspost decoder treats empty input as zero frames and
        // returns empty output without an error (verified against the
        // upstream lib/encoding/zstd wrappers); the Rust streaming decoder
        // would fail reading the frame header instead.
        return Ok(());
    }
    if let Ok(Some(content_size)) = zstd::zstd_safe::get_frame_content_size(src) {
        // Fast path: the frame content size is known (always the case for
        // blocks produced by compress_zstd_level), so decompress with the
        // reusable bulk context directly into dst.
        let content_size =
            usize::try_from(content_size).map_err(|_| "too big frame content size".to_string())?;
        let dst_len = dst.len();
        dst.resize(dst_len + content_size, 0);
        let n = DECOMPRESSOR.with(|cell| {
            let mut d = cell.borrow_mut();
            let d = match d.as_mut() {
                Some(d) => d,
                None => {
                    d.insert(Decompressor::new().expect("BUG: failed to create ZSTD decompressor"))
                }
            };
            d.decompress_to_buffer(src, &mut dst[dst_len..])
        });
        match n {
            Ok(n) => {
                dst.truncate(dst_len + n);
                Ok(())
            }
            Err(err) => {
                dst.truncate(dst_len);
                Err(err.to_string())
            }
        }
    } else {
        // Slow path: unknown content size (streamed frame) — fall back to
        // streaming decompression.
        let dst_len = dst.len();
        let result = zstd::stream::read::Decoder::with_buffer(src)
            .and_then(|mut decoder| decoder.read_to_end(dst));
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                dst.truncate(dst_len);
                Err(err.to_string())
            }
        }
    }
}

/// Decompresses `src`, appending the result to `dst`.
///
/// If the decompressed result exceeds `max_data_size_bytes`, then an error is
/// returned. `max_data_size_bytes == 0` means no limit.
///
/// Go: DecompressZSTDLimited (zstd.DecompressLimited with WithDecoderMaxMemory).
pub fn decompress_zstd_limited(
    dst: &mut Vec<u8>,
    src: &[u8],
    max_data_size_bytes: usize,
) -> Result<(), String> {
    decompress_zstd_limited_internal(dst, src, max_data_size_bytes).map_err(|err| {
        format!(
            "cannot decompress zstd block with len={} and maxDataSizeBytes={max_data_size_bytes}: {err}",
            src.len()
        )
    })
}

fn decompress_zstd_limited_internal(
    dst: &mut Vec<u8>,
    src: &[u8],
    max_data_size_bytes: usize,
) -> Result<(), String> {
    if src.is_empty() {
        // Zero frames decode to empty output, as in Go (see
        // decompress_zstd_internal).
        return Ok(());
    }
    if max_data_size_bytes == 0 {
        // Unlimited, as in Go where maxMemory=0 creates a default decoder.
        return decompress_zstd_internal(dst, src);
    }
    if let Ok(Some(content_size)) = zstd::zstd_safe::get_frame_content_size(src) {
        if content_size > max_data_size_bytes as u64 {
            return Err(format!(
                "decompressed data size {content_size} exceeds maxDataSizeBytes {max_data_size_bytes}"
            ));
        }
        return decompress_zstd_internal(dst, src);
    }

    // Unknown content size: stream-decode with a bounded window and byte cap,
    // mirroring klauspost's WithDecoderMaxMemory behavior.
    let window_log_max = ceil_log2(max_data_size_bytes);
    let dst_len = dst.len();
    let result = zstd::stream::read::Decoder::with_buffer(src).and_then(|mut decoder| {
        decoder.window_log_max(window_log_max)?;
        decoder
            .take(max_data_size_bytes as u64 + 1)
            .read_to_end(dst)
    });
    match result {
        Ok(_) => {
            if dst.len() - dst_len > max_data_size_bytes {
                dst.truncate(dst_len);
                return Err(format!(
                    "decompressed data size exceeds maxDataSizeBytes {max_data_size_bytes}"
                ));
            }
            Ok(())
        }
        Err(err) => {
            dst.truncate(dst_len);
            Err(err.to_string())
        }
    }
}

fn ceil_log2(n: usize) -> u32 {
    (usize::BITS - n.saturating_sub(1).leading_zeros()).max(10)
}

/// Checks if the given data is compressed using the zstd format by verifying
/// the presence of the zstd magic number (0xFD2FB528) at the beginning.
///
/// Go: IsZstd (util.go).
pub fn is_zstd(data: &[u8]) -> bool {
    data.len() >= 4 && u32::from_le_bytes([data[0], data[1], data[2], data[3]]) == 0xFD2F_B528
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::Rng;

    // Port of TestCompressDecompressZSTD.
    #[test]
    fn test_compress_decompress_zstd() {
        check_roundtrip(b"a");
        check_roundtrip(b"foobarbaz");

        let mut r = Rng::new(1);
        let mut b = Vec::new();
        for _ in 0..64 * 1024 {
            b.push(r.byte());
        }
        check_roundtrip(&b);
    }

    fn check_roundtrip(b: &[u8]) {
        let mut bc = Vec::new();
        compress_zstd_level(&mut bc, b, 5);
        let mut b_new = Vec::new();
        decompress_zstd(&mut b_new, &bc)
            .unwrap_or_else(|err| panic!("unexpected error when decompressing: {err}"));
        assert_eq!(b_new, b, "invalid bNew");

        let prefix = [1u8, 2, 33];
        let mut bc_new = prefix.to_vec();
        compress_zstd_level(&mut bc_new, b, 5);
        assert_eq!(&bc_new[..prefix.len()], &prefix, "invalid prefix");
        assert_eq!(&bc_new[prefix.len()..], &bc[..], "invalid prefixed bcNew");

        let mut b_new = prefix.to_vec();
        decompress_zstd(&mut b_new, &bc)
            .unwrap_or_else(|err| panic!("unexpected error when decompressing with prefix: {err}"));
        assert_eq!(&b_new[..prefix.len()], &prefix, "invalid bNew prefix");
        assert_eq!(&b_new[prefix.len()..], b, "invalid prefixed bNew");
    }

    fn big_test_data() -> Vec<u8> {
        let mut bb = Vec::new();
        while bb.len() < 12 * 128 * 1024 {
            bb.extend_from_slice(format!("compress/decompress big data {}, ", bb.len()).as_bytes());
        }
        bb
    }

    // Port of TestDecomrpessLimitedOk (zstd_pure_test.go).
    #[test]
    fn test_decompress_limited_ok() {
        let f = |compressed_data: &[u8], limit: usize| {
            let mut dst = Vec::new();
            decompress_zstd_limited(&mut dst, compressed_data, limit)
                .unwrap_or_else(|err| panic!("cannot decompress data with limit={limit}: {err}"));
        };

        let origin_data = big_test_data();
        let mut cd = Vec::new();
        compress_zstd_level(&mut cd, &origin_data, 0);

        // decompressed size matches block limit
        f(&cd, origin_data.len());

        // unlimited
        f(&cd, 0);
    }

    // Port of TestDecompressLimitedFail (zstd_pure_test.go).
    #[test]
    fn test_decompress_limited_fail() {
        let f = |input: &[u8], limit: usize| {
            let mut dst = Vec::new();
            let result = decompress_zstd_limited(&mut dst, input, limit);
            assert!(
                result.is_err(),
                "unexpected nil-error for decompress with limit: {limit}"
            );
        };

        let bb = big_test_data();

        // valid input bigger than limit
        f(&bb, 1024);

        // input with framecontent bigger than actual payload
        let input = hex_decode("28b52ffd8400005ed0b209000030ecaf4412");
        f(&input, 512);

        // input with stream windowSize bigger than limit
        let input = hex_decode("28b52ffd04981900003030304e8da22b");
        f(&input, 8 * 1_000_000 * 10);
    }

    fn hex_decode(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
            .collect()
    }

    // Port of TestIsZstd (util_test.go).
    #[test]
    fn test_is_zstd() {
        // nil / empty
        assert!(!is_zstd(&[]));

        // less than 4 bytes
        assert!(!is_zstd(b"foo"));

        // plain text
        assert!(!is_zstd(b"foobar"));

        // non-zstd compressed data (snappy in the Go test; a snappy-framed
        // header here, since this crate doesn't ship a snappy codec)
        assert!(!is_zstd(&[
            0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59
        ]));

        // zstd minimum compression level
        let mut b = Vec::new();
        compress_zstd_level(&mut b, b"foobar", -22);
        assert!(
            is_zstd(&b),
            "unexpected IsZstd result; got false; expecting true"
        );

        // zstd maximum compression level
        let mut b = Vec::new();
        compress_zstd_level(&mut b, b"foobar", 22);
        assert!(
            is_zstd(&b),
            "unexpected IsZstd result; got false; expecting true"
        );
    }
}

#[cfg(test)]
mod empty_input_tests {
    use super::*;

    // Go's klauspost decoder returns empty output, no error, for empty
    // input (zero frames) — verified against upstream lib/encoding/zstd's
    // Decompress/DecompressLimited wrappers.
    #[test]
    fn decompress_zstd_accepts_empty_input() {
        let mut dst = Vec::new();
        decompress_zstd(&mut dst, &[]).unwrap();
        assert!(dst.is_empty());
    }

    #[test]
    fn decompress_zstd_limited_accepts_empty_input() {
        let mut dst = Vec::new();
        decompress_zstd_limited(&mut dst, &[], 1024).unwrap();
        assert!(dst.is_empty());
        // Unlimited path too.
        decompress_zstd_limited(&mut dst, &[], 0).unwrap();
        assert!(dst.is_empty());
    }
}
