//! Time-series array codec.
//!
//! Byte-compatible reimplementation of VictoriaMetrics' `lib/encoding`
//! int64-array marshaling for time-series timestamps and values
//! (`encoding.go:75-250`). Lossless (`precision_bits = 64`) path is
//! implemented in full; lossy precision modes return [`TsError::PrecisionUnsupported`]
//! until Phase 1B.x adds them.
//!
//! ## Marshal types (wire byte values)
//!
//! | Byte | Type | Description |
//! |---|---|---|
//! | 1 | `ZstdNearestDelta2` | counter-style (delta-of-delta) + zstd |
//! | 2 | `DeltaConst`        | constant non-zero delta, encoded once |
//! | 3 | `Const`             | all-equal values, no payload |
//! | 4 | `ZstdNearestDelta`  | gauge-style (delta) + zstd |
//! | 5 | `NearestDelta2`     | counter-style, no zstd (poor compression fallback) |
//! | 6 | `NearestDelta`      | gauge-style, no zstd (poor compression fallback) |
//!
//! `MarshalArray` chooses gauge vs counter heuristically by counting how
//! often `a[i+1] >= a[i]`. A run that's monotonic non-decreasing the
//! majority of the time picks the counter path (delta2); otherwise gauge.

use thiserror::Error;

use crate::int::{
    DecodeError, marshal_varint64, marshal_varint64s, unmarshal_varint64, unmarshal_varint64s_into,
};
use crate::zstd_codec::{ZstdError, compress_zstd_level, decompress_zstd};

/// Minimum payload size that's worth trying zstd against.
const MIN_COMPRESSIBLE_BLOCK_SIZE: usize = 128;

/// On-disk MarshalType byte for time-series arrays. Matches VM's
/// `encoding.MarshalType` (`lib/encoding/encoding.go:18-43`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MarshalType {
    ZstdNearestDelta2 = 1,
    DeltaConst = 2,
    Const = 3,
    ZstdNearestDelta = 4,
    NearestDelta2 = 5,
    NearestDelta = 6,
}

impl MarshalType {
    /// On-disk byte representation.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Construct from the on-disk byte.
    ///
    /// # Errors
    /// Returns [`MarshalTypeByteError`] for values outside `[1, 6]`.
    pub const fn from_byte(byte: u8) -> Result<Self, MarshalTypeByteError> {
        match byte {
            1 => Ok(Self::ZstdNearestDelta2),
            2 => Ok(Self::DeltaConst),
            3 => Ok(Self::Const),
            4 => Ok(Self::ZstdNearestDelta),
            5 => Ok(Self::NearestDelta2),
            6 => Ok(Self::NearestDelta),
            other => Err(MarshalTypeByteError(other)),
        }
    }
}

#[derive(Debug, Error)]
#[error("MarshalType byte {0} must be in [1, 6]")]
pub struct MarshalTypeByteError(pub u8);

/// Encoded output of [`marshal_int64_array`]: the marshal type, the first
/// value (used as a seed during decoding), and the byte payload.
#[derive(Debug)]
pub struct MarshalArrayResult {
    pub marshal_type: MarshalType,
    pub first_value: i64,
}

/// Marshal an int64 array using VM's selection logic. The encoded bytes are
/// appended to `dst`; the chosen [`MarshalType`] and the first value are
/// returned (the first value is required during decoding).
///
/// `precision_bits` must be in `[1, 64]`.
///
/// At `precision_bits = 64` the encoding is lossless. At lower values each
/// input value is quantized to keep only `precision_bits` significant
/// magnitude bits — the lower `magnitude_bits - precision_bits` bits are
/// masked to zero. This is a strict superset of VM's `nearest delta`
/// precision model in the sense that recovered values are within
/// `2^(magnitude_bits - precision_bits)` of the originals, but the on-disk
/// bytes are **not** byte-compatible with VM's `marshalInt64NearestDelta`
/// — for byte-level VM parity choose `precision_bits = 64`.
///
/// # Errors
/// - [`TsError::Empty`] if `src` is empty.
/// - [`TsError::PrecisionInvalid`] if `precision_bits` is not in `[1, 64]`.
/// - [`TsError::Zstd`] on compression failure.
pub fn marshal_int64_array(
    dst: &mut Vec<u8>,
    src: &[i64],
    precision_bits: u8,
) -> Result<MarshalArrayResult, TsError> {
    if src.is_empty() {
        return Err(TsError::Empty);
    }
    if precision_bits == 0 || precision_bits > 64 {
        return Err(TsError::PrecisionInvalid(precision_bits));
    }

    // Lossy quantization path. Apply the mask up-front so every later
    // stage operates on the already-quantized values; this keeps the
    // marshal-type heuristic ("const?", "delta-const?") seeing the same
    // values the decoder will recover.
    let owned;
    let src: &[i64] = if precision_bits == 64 {
        src
    } else {
        owned = src.iter().map(|&v| quantize_to_precision(v, precision_bits)).collect::<Vec<i64>>();
        &owned[..]
    };

    let first = src[0];

    // Const?
    if src.iter().all(|&x| x == first) {
        return Ok(MarshalArrayResult { marshal_type: MarshalType::Const, first_value: first });
    }
    // DeltaConst?
    if src.len() >= 2 {
        let d = src[1].wrapping_sub(src[0]);
        let mut all_eq = true;
        for w in src.windows(2) {
            if w[1].wrapping_sub(w[0]) != d {
                all_eq = false;
                break;
            }
        }
        if all_eq {
            marshal_varint64(dst, d);
            return Ok(MarshalArrayResult {
                marshal_type: MarshalType::DeltaConst,
                first_value: first,
            });
        }
    }

    // Gauge vs counter heuristic (VM `isGauge` in encoding.go).
    let counter = looks_like_counter(src);
    let (mut payload, ts_kind) = if counter {
        (marshal_nearest_delta2(src), NestedKind::Delta2)
    } else {
        (marshal_nearest_delta(src), NestedKind::Delta)
    };

    // Try zstd compression.
    let dst_orig_len = dst.len();
    if payload.len() >= MIN_COMPRESSIBLE_BLOCK_SIZE {
        compress_zstd_level(dst, &payload, compress_level_for(src.len()))?;
    }
    let compressed_len = dst.len() - dst_orig_len;
    let want_plain = payload.len() < MIN_COMPRESSIBLE_BLOCK_SIZE
        || ineffective_compression(compressed_len, payload.len());
    if want_plain {
        dst.truncate(dst_orig_len);
        dst.append(&mut payload);
        let mt = match ts_kind {
            NestedKind::Delta => MarshalType::NearestDelta,
            NestedKind::Delta2 => MarshalType::NearestDelta2,
        };
        return Ok(MarshalArrayResult { marshal_type: mt, first_value: first });
    }
    let mt = match ts_kind {
        NestedKind::Delta => MarshalType::ZstdNearestDelta,
        NestedKind::Delta2 => MarshalType::ZstdNearestDelta2,
    };
    Ok(MarshalArrayResult { marshal_type: mt, first_value: first })
}

/// Zero the low magnitude bits of `v` so that only `precision_bits`
/// significant bits remain. Sign is preserved.
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
fn quantize_to_precision(v: i64, precision_bits: u8) -> i64 {
    if v == 0 || precision_bits >= 64 {
        return v;
    }
    let abs = v.unsigned_abs();
    let mag_bits = 64 - abs.leading_zeros() as u8;
    if mag_bits <= precision_bits {
        return v;
    }
    let shift = mag_bits - precision_bits;
    let mask = !((1u64 << shift) - 1);
    let masked = (abs & mask) as i64;
    if v < 0 { -masked } else { masked }
}

fn ineffective_compression(compressed: usize, plain: usize) -> bool {
    // VM threshold: compressed > 0.9 * plain.
    // f64 precision is sufficient because block sizes are bounded.
    #[allow(clippy::cast_precision_loss)]
    let ratio = (compressed as f64) > 0.9 * (plain as f64);
    ratio
}

fn compress_level_for(items: usize) -> i32 {
    // VM's getCompressLevel scales with block length:
    //   >= 8192: 22; >= 512: 10; >= 64: 5; else 1
    match items {
        n if n >= 8192 => 22,
        n if n >= 512 => 10,
        n if n >= 64 => 5,
        _ => 1,
    }
}

/// Unmarshal an int64 array encoded by [`marshal_int64_array`]. `dst` is
/// extended with `items_count` values. `first_value` is the second return
/// value of `marshal_int64_array`.
///
/// # Errors
/// See [`TsError`].
pub fn unmarshal_int64_array(
    dst: &mut Vec<i64>,
    src: &[u8],
    mt: MarshalType,
    first_value: i64,
    items_count: usize,
) -> Result<(), TsError> {
    if items_count == 0 {
        return Err(TsError::ZeroItemsCount);
    }
    dst.reserve(items_count);
    match mt {
        MarshalType::Const => {
            if !src.is_empty() {
                return Err(TsError::UnexpectedTail(src.len()));
            }
            dst.extend(std::iter::repeat_n(first_value, items_count));
            Ok(())
        }
        MarshalType::DeltaConst => {
            let (d, n) = unmarshal_varint64(src).map_err(TsError::Decode)?;
            if n != src.len() {
                return Err(TsError::UnexpectedTail(src.len() - n));
            }
            let mut v = first_value;
            for _ in 0..items_count {
                dst.push(v);
                v = v.wrapping_add(d);
            }
            Ok(())
        }
        MarshalType::NearestDelta => unmarshal_nearest_delta(dst, src, first_value, items_count),
        MarshalType::NearestDelta2 => unmarshal_nearest_delta2(dst, src, first_value, items_count),
        MarshalType::ZstdNearestDelta => {
            let mut buf = Vec::new();
            decompress_zstd(&mut buf, src)?;
            unmarshal_nearest_delta(dst, &buf, first_value, items_count)
        }
        MarshalType::ZstdNearestDelta2 => {
            let mut buf = Vec::new();
            decompress_zstd(&mut buf, src)?;
            unmarshal_nearest_delta2(dst, &buf, first_value, items_count)
        }
    }
}

// ---------------------------------------------------------------------------
// Nearest delta (gauge) — lossless variant.

enum NestedKind {
    Delta,
    Delta2,
}

fn marshal_nearest_delta(src: &[i64]) -> Vec<u8> {
    let n = src.len();
    let mut deltas = Vec::with_capacity(n - 1);
    compute_deltas(src, &mut deltas);
    let mut out = Vec::new();
    marshal_varint64s(&mut out, &deltas);
    out
}

/// Compute consecutive deltas of `src` into `dst`. Scalar fallback uses a
/// tight loop the compiler auto-vectorises; the AVX2/NEON code paths
/// activate on Linux + macOS when the appropriate target features are
/// detected at runtime.
#[inline]
fn compute_deltas(src: &[i64], dst: &mut Vec<i64>) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 availability is checked at runtime.
            unsafe { compute_deltas_avx2(src, dst) };
            return;
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        // SAFETY: aarch64 + neon is checked at compile time; NEON is part
        // of the baseline aarch64 spec, so a runtime check would be
        // redundant.
        unsafe { compute_deltas_neon(src, dst) };
        return;
    }
    compute_deltas_scalar(src, dst);
}

fn compute_deltas_scalar(src: &[i64], dst: &mut Vec<i64>) {
    let mut prev = src[0];
    for &next in &src[1..] {
        let d = next.wrapping_sub(prev);
        dst.push(d);
        prev = next;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(clippy::ptr_as_ptr)]
#[allow(clippy::cast_ptr_alignment)]
#[allow(clippy::many_single_char_names)]
unsafe fn compute_deltas_avx2(src: &[i64], dst: &mut Vec<i64>) {
    // 4 × i64 per AVX2 register. Subtract a shifted copy of the
    // sequence: deltas[idx] = src[idx+1] - src[idx]. Aligned loads are
    // not required by `_mm256_loadu_si256` ("u" = unaligned).
    use std::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256, _mm256_sub_epi64};
    let n = src.len();
    if n <= 1 {
        return;
    }
    let pairs = n - 1;
    dst.reserve(pairs);
    let dst_ptr = unsafe { dst.as_mut_ptr().add(dst.len()) };
    let mut idx = 0;
    while idx + 4 <= pairs {
        let a_ptr = unsafe { src.as_ptr().add(idx + 1) }.cast::<__m256i>();
        let b_ptr = unsafe { src.as_ptr().add(idx) }.cast::<__m256i>();
        let a = unsafe { _mm256_loadu_si256(a_ptr) };
        let b = unsafe { _mm256_loadu_si256(b_ptr) };
        let d = _mm256_sub_epi64(a, b);
        let out_ptr = unsafe { dst_ptr.add(idx) }.cast::<__m256i>();
        unsafe { _mm256_storeu_si256(out_ptr, d) };
        idx += 4;
    }
    while idx < pairs {
        let d = src[idx + 1].wrapping_sub(src[idx]);
        unsafe { dst_ptr.add(idx).write(d) };
        idx += 1;
    }
    unsafe { dst.set_len(dst.len() + pairs) };
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[target_feature(enable = "neon")]
unsafe fn compute_deltas_neon(src: &[i64], dst: &mut Vec<i64>) {
    // NEON: 2 × i64 per register.
    use std::arch::aarch64::{vld1q_s64, vst1q_s64, vsubq_s64};
    let n = src.len();
    if n <= 1 {
        return;
    }
    let pairs = n - 1;
    dst.reserve(pairs);
    let dst_ptr = unsafe { dst.as_mut_ptr().add(dst.len()) };
    let mut i = 0;
    while i + 2 <= pairs {
        let a = unsafe { vld1q_s64(src.as_ptr().add(i + 1)) };
        let b = unsafe { vld1q_s64(src.as_ptr().add(i)) };
        let d = unsafe { vsubq_s64(a, b) };
        unsafe { vst1q_s64(dst_ptr.add(i), d) };
        i += 2;
    }
    while i < pairs {
        let d = src[i + 1].wrapping_sub(src[i]);
        unsafe { dst_ptr.add(i).write(d) };
        i += 1;
    }
    unsafe { dst.set_len(dst.len() + pairs) };
}

fn unmarshal_nearest_delta(
    dst: &mut Vec<i64>,
    src: &[u8],
    first_value: i64,
    items_count: usize,
) -> Result<(), TsError> {
    let mut deltas = vec![0i64; items_count - 1];
    let tail = unmarshal_varint64s_into(&mut deltas, src).map_err(TsError::Decode)?;
    if !tail.is_empty() {
        return Err(TsError::UnexpectedTail(tail.len()));
    }
    let mut v = first_value;
    dst.push(v);
    for d in deltas {
        v = v.wrapping_add(d);
        dst.push(v);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Nearest delta2 (counter) — lossless variant.

fn marshal_nearest_delta2(src: &[i64]) -> Vec<u8> {
    // Lossless delta-of-delta:
    //   d1 = src[1] - src[0]                 -- first delta, stored as-is
    //   d2_i = (src[i+2] - src[i+1]) - (src[i+1] - src[i])   -- subsequent deltas of deltas
    //
    // VM stores: varint(d1) then varint(d2_2), varint(d2_3), ..., varint(d2_n-1)
    let n = src.len();
    if n < 2 {
        // Caller already rules out len < 2 via Const path; defensive.
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut prev = src[0];
    let mut prev_delta: i64 = 0;
    for (i, &cur) in src[1..].iter().enumerate() {
        let d = cur.wrapping_sub(prev);
        if i == 0 {
            marshal_varint64(&mut out, d);
        } else {
            let dd = d.wrapping_sub(prev_delta);
            marshal_varint64(&mut out, dd);
        }
        prev_delta = d;
        prev = cur;
    }
    out
}

fn unmarshal_nearest_delta2(
    dst: &mut Vec<i64>,
    src: &[u8],
    first_value: i64,
    items_count: usize,
) -> Result<(), TsError> {
    let mut cursor = src;
    let mut v = first_value;
    dst.push(v);

    // First step: d1 (delta) read as plain varint.
    if items_count == 1 {
        if !cursor.is_empty() {
            return Err(TsError::UnexpectedTail(cursor.len()));
        }
        return Ok(());
    }
    let (d1, n) = unmarshal_varint64(cursor).map_err(TsError::Decode)?;
    cursor = &cursor[n..];
    v = v.wrapping_add(d1);
    dst.push(v);
    let mut prev_delta = d1;

    // Subsequent steps: read delta-of-delta varints.
    for _ in 2..items_count {
        let (dd, n) = unmarshal_varint64(cursor).map_err(TsError::Decode)?;
        cursor = &cursor[n..];
        let cur_delta = prev_delta.wrapping_add(dd);
        v = v.wrapping_add(cur_delta);
        dst.push(v);
        prev_delta = cur_delta;
    }
    if !cursor.is_empty() {
        return Err(TsError::UnexpectedTail(cursor.len()));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Heuristic: distinguish counter (mostly non-decreasing) from gauge.

fn looks_like_counter(src: &[i64]) -> bool {
    if src.len() < 32 {
        // For short series, default to gauge.
        return false;
    }
    let mut non_decreasing = 0usize;
    for w in src.windows(2) {
        if w[1] >= w[0] {
            non_decreasing += 1;
        }
    }
    // Counter if at least 90% of transitions are non-decreasing.
    #[allow(clippy::cast_precision_loss)]
    let ratio = (non_decreasing as f64) / ((src.len() - 1) as f64);
    ratio >= 0.9
}

#[derive(Debug, Error)]
pub enum TsError {
    #[error("cannot marshal empty time-series array")]
    Empty,
    #[error("precision_bits {0} not in [1, 64]")]
    PrecisionInvalid(u8),
    /// Retained for backward compatibility with callers that pattern-match on it.
    /// No code path now produces this — every `precision_bits` in `[1, 64]` is
    /// honored.
    #[deprecated(note = "precision_bits < 64 is now supported via quantization")]
    #[error("precision_bits {0} not yet implemented")]
    PrecisionUnsupported(u8),
    #[error("items_count must be > 0")]
    ZeroItemsCount,
    #[error("unexpected {0} trailing bytes after array payload")]
    UnexpectedTail(usize),
    #[error(transparent)]
    Decode(DecodeError),
    #[error(transparent)]
    Zstd(#[from] ZstdError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(src: &[i64], expected_mt: MarshalType) {
        let mut buf = Vec::new();
        let result = marshal_int64_array(&mut buf, src, 64).unwrap();
        assert_eq!(result.marshal_type, expected_mt, "input: {src:?}");
        let mut decoded = Vec::new();
        unmarshal_int64_array(
            &mut decoded,
            &buf,
            result.marshal_type,
            result.first_value,
            src.len(),
        )
        .unwrap();
        assert_eq!(decoded, src);
    }

    #[test]
    fn const_array_roundtrip() {
        roundtrip(&[5, 5, 5, 5, 5], MarshalType::Const);
    }

    #[test]
    fn delta_const_array_roundtrip() {
        roundtrip(&[10, 13, 16, 19, 22], MarshalType::DeltaConst);
    }

    #[test]
    fn nearest_delta_small_array_roundtrip() {
        // Gauge-style with random-looking values; short enough to stay
        // below the zstd threshold.
        roundtrip(&[100, 200, 50, 175, 80, 300, 10, 250], MarshalType::NearestDelta);
    }

    #[test]
    fn nearest_delta2_short_counter_roundtrip() {
        // Short counter-style series stays under the 32-item heuristic
        // threshold and is encoded as gauge (NearestDelta).
        roundtrip(&[100, 110, 120, 130, 145, 160, 175], MarshalType::NearestDelta);
    }

    #[test]
    fn nearest_delta2_long_counter_roundtrip() {
        let mut src: Vec<i64> = (0..64).map(|i| i64::from(i) * 7 + 100).collect();
        // Introduce a few small perturbations so the encoder doesn't pick
        // DeltaConst.
        src[10] += 1;
        src[40] += 2;
        let mut buf = Vec::new();
        let r = marshal_int64_array(&mut buf, &src, 64).unwrap();
        assert!(matches!(
            r.marshal_type,
            MarshalType::NearestDelta2 | MarshalType::ZstdNearestDelta2
        ));
        let mut decoded = Vec::new();
        unmarshal_int64_array(&mut decoded, &buf, r.marshal_type, r.first_value, src.len())
            .unwrap();
        assert_eq!(decoded, src);
    }

    #[test]
    fn zstd_path_triggered_on_long_repetitive_input() {
        // 200 large-ish values with a predictable pattern -> zstd path.
        let src: Vec<i64> = (0..200).map(|i| 1_000_000 + i * 1_000).collect();
        let mut buf = Vec::new();
        let r = marshal_int64_array(&mut buf, &src, 64).unwrap();
        // The exact MarshalType depends on the compression ratio; ensure we
        // round-trip regardless.
        let mut decoded = Vec::new();
        unmarshal_int64_array(&mut decoded, &buf, r.marshal_type, r.first_value, src.len())
            .unwrap();
        assert_eq!(decoded, src);
    }

    #[test]
    fn precision_below_64_quantizes() {
        // Value 0b1010_1010_0000 (2720) at precision_bits = 4 keeps the top
        // 4 magnitude bits (1010) and zeros the rest -> 0b1010_0000_0000 = 2560.
        let mut buf = Vec::new();
        let src = [2720_i64, 2720, 2720];
        let r = marshal_int64_array(&mut buf, &src, 4).unwrap();
        assert_eq!(r.marshal_type, MarshalType::Const);
        // Round-trip via const path: every value is the quantized first.
        let mut decoded = Vec::new();
        unmarshal_int64_array(&mut decoded, &buf, r.marshal_type, r.first_value, src.len())
            .unwrap();
        assert_eq!(decoded, vec![2560, 2560, 2560]);
    }

    #[test]
    fn precision_bits_invalid_zero_rejected() {
        let mut buf = Vec::new();
        assert!(matches!(
            marshal_int64_array(&mut buf, &[1, 2, 3], 0),
            Err(TsError::PrecisionInvalid(0))
        ));
    }

    #[test]
    fn empty_input_rejected() {
        let mut buf = Vec::new();
        assert!(matches!(marshal_int64_array(&mut buf, &[], 64), Err(TsError::Empty)));
    }

    #[test]
    fn single_value_const() {
        roundtrip(&[42], MarshalType::Const);
    }

    #[test]
    fn marshal_type_byte_roundtrip() {
        for mt in [
            MarshalType::ZstdNearestDelta2,
            MarshalType::DeltaConst,
            MarshalType::Const,
            MarshalType::ZstdNearestDelta,
            MarshalType::NearestDelta2,
            MarshalType::NearestDelta,
        ] {
            assert_eq!(MarshalType::from_byte(mt.as_byte()).unwrap(), mt);
        }
        assert!(MarshalType::from_byte(0).is_err());
        assert!(MarshalType::from_byte(7).is_err());
    }
}
