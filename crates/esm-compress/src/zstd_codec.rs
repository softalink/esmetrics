//! Block-level zstd compression wrappers.
//!
//! Matches VictoriaMetrics `lib/encoding/compress.go`:
//! - `CompressZSTDLevel(dst, src, level) []byte` — compress `src` to `dst`
//!   at the given level (5 by default in VM).
//! - `DecompressZSTD(dst, src) ([]byte, error)` — decompress `src` into `dst`.
//!
//! ## Output stability
//!
//! Zstd output is **deterministic for a fixed library version + level + input**.
//! ADR-001 #14 pins the `zstd` crate to the version vendored by VM so
//! `CompressZSTDLevel` produces byte-identical output. The output bytes here
//! must match VM's bytes character-for-character; the conformance harness
//! enforces this.

use std::io;

use thiserror::Error;

/// Default compression level used by VictoriaMetrics for mergeset parts. VM
/// passes 5 from `lib/mergeset/table.go` writer paths.
pub const DEFAULT_LEVEL: i32 = 5;

/// Compress `src` with zstd at the given `level`, appending to `dst`.
///
/// # Errors
/// Returns [`ZstdError::Compress`] if the zstd library reports a failure.
pub fn compress_zstd_level(dst: &mut Vec<u8>, src: &[u8], level: i32) -> Result<(), ZstdError> {
    let mut encoder = zstd::stream::Encoder::new(dst, level).map_err(ZstdError::Compress)?;
    io::Write::write_all(&mut encoder, src).map_err(ZstdError::Compress)?;
    encoder.finish().map_err(ZstdError::Compress)?;
    Ok(())
}

/// Decompress `src` into `dst` (which is cleared first).
///
/// # Errors
/// Returns [`ZstdError::Decompress`] if `src` is not valid zstd or the
/// underlying decompressor errors.
pub fn decompress_zstd(dst: &mut Vec<u8>, src: &[u8]) -> Result<(), ZstdError> {
    dst.clear();
    let mut decoder = zstd::stream::Decoder::new(src).map_err(ZstdError::Decompress)?;
    io::Read::read_to_end(&mut decoder, dst).map_err(ZstdError::Decompress)?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum ZstdError {
    #[error("zstd compress: {0}")]
    Compress(io::Error),
    #[error("zstd decompress: {0}")]
    Decompress(io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_default_level() {
        let payload = b"the quick brown fox jumps over the lazy dog".repeat(20);
        let mut compressed = Vec::new();
        compress_zstd_level(&mut compressed, &payload, DEFAULT_LEVEL).unwrap();
        assert!(compressed.len() < payload.len(), "expected zstd to shrink repeated payload");

        let mut decompressed = Vec::new();
        decompress_zstd(&mut decompressed, &compressed).unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn roundtrip_empty() {
        let mut compressed = Vec::new();
        compress_zstd_level(&mut compressed, &[], DEFAULT_LEVEL).unwrap();
        let mut decompressed = Vec::new();
        decompress_zstd(&mut decompressed, &compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn decompress_rejects_garbage() {
        let mut decompressed = Vec::new();
        assert!(decompress_zstd(&mut decompressed, b"not zstd").is_err());
    }
}
