//! Time-series compression codecs and binary-encoding helpers for EsMetrics.
//!
//! Provides:
//! - [`int`] — primitive binary-encoding helpers shared with `esm-storage`
//!   (big-endian fixed-width ints, varuint, and length-prefixed `Bytes`).
//! - Gorilla XOR encoding/decoding for float sample values (Phase 1B).
//! - Delta-of-delta + variable-bit packing for timestamps (Phase 1B).
//! - Wrappers around block-level compression libraries (`zstd`, `snap`, `lz4`).
//!
//! Byte-identical output vs VictoriaMetrics v1.144.0 is the design goal.
//! Scalar reference implementations live alongside SIMD-accelerated variants
//! gated on `cfg(target_feature)`.

pub mod int;
pub mod timeseries;
pub mod zstd_codec;
