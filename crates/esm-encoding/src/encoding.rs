//! Block-level int64 array marshaling with automatic encoding selection.
//! Port of Go lib/encoding/encoding.go.

use std::cell::RefCell;

use esm_common::fastnum;

use crate::compress::{compress_zstd_level, decompress_zstd};
use crate::int::{marshal_var_int64, unmarshal_var_int64};
use crate::nearest_delta::{marshal_int64_nearest_delta, unmarshal_int64_nearest_delta};
use crate::nearest_delta2::{marshal_int64_nearest_delta2, unmarshal_int64_nearest_delta2};

/// The minimum block size in bytes for trying compression.
///
/// There is no sense in compressing smaller blocks.
///
/// Go: minCompressibleBlockSize.
const MIN_COMPRESSIBLE_BLOCK_SIZE: usize = 128;

/// The type used for block marshaling. Go: MarshalType (identical discriminants).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MarshalType {
    /// Used for marshaling counter timeseries. Go: MarshalTypeZSTDNearestDelta2.
    ZstdNearestDelta2 = 1,
    /// Used for marshaling constantly changed time series with constant delta.
    /// Go: MarshalTypeDeltaConst.
    DeltaConst = 2,
    /// Used for marshaling time series containing only a single constant.
    /// Go: MarshalTypeConst.
    Const = 3,
    /// Used for marshaling gauge timeseries. Go: MarshalTypeZSTDNearestDelta.
    ZstdNearestDelta = 4,
    /// Used instead of `ZstdNearestDelta2` if compression doesn't help.
    /// Go: MarshalTypeNearestDelta2.
    NearestDelta2 = 5,
    /// Used instead of `ZstdNearestDelta` if compression doesn't help.
    /// Go: MarshalTypeNearestDelta.
    NearestDelta = 6,
}

impl MarshalType {
    /// Returns true if the type may need additional validation for silent data
    /// corruption. Go: MarshalType.NeedsValidation.
    pub fn needs_validation(self) -> bool {
        // Other types do not need additional validation, since they either
        // already contain checksums (e.g. compressed data) or they are trivial
        // and cannot be validated (e.g. const or delta const).
        matches!(self, MarshalType::NearestDelta2 | MarshalType::NearestDelta)
    }

    /// Converts a raw byte into a `MarshalType`.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(MarshalType::ZstdNearestDelta2),
            2 => Some(MarshalType::DeltaConst),
            3 => Some(MarshalType::Const),
            4 => Some(MarshalType::ZstdNearestDelta),
            5 => Some(MarshalType::NearestDelta2),
            6 => Some(MarshalType::NearestDelta),
            _ => None,
        }
    }

    /// Returns the on-disk discriminant.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Verifies whether the raw `mt` byte is valid. Go: CheckMarshalType
/// (Go accepts the whole [0..6] range even though 0 is unused).
pub fn check_marshal_type(mt: u8) -> Result<(), String> {
    if mt > 6 {
        return Err(format!("MarshalType should be in range [0..6]; got {mt}"));
    }
    Ok(())
}

/// Makes sure `precision_bits` is in the range [1..64]. Go: CheckPrecisionBits.
pub fn check_precision_bits(precision_bits: u8) -> Result<(), String> {
    if !(1..=64).contains(&precision_bits) {
        return Err(format!(
            "precisionBits must be in the range [1...64]; got {precision_bits}"
        ));
    }
    Ok(())
}

/// Marshals `timestamps`, appending the result to `dst`.
/// Returns the marshal type and the first timestamp.
///
/// `timestamps` must contain non-decreasing values.
///
/// `precision_bits` must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
///
/// Go: MarshalTimestamps.
pub fn marshal_timestamps(
    dst: &mut Vec<u8>,
    timestamps: &[i64],
    precision_bits: u8,
) -> (MarshalType, i64) {
    marshal_int64_array(dst, timestamps, precision_bits)
}

/// Unmarshals timestamps from `src`, appending them to `dst`.
///
/// `first_timestamp` must be the timestamp returned from [`marshal_timestamps`].
///
/// Go: UnmarshalTimestamps.
pub fn unmarshal_timestamps(
    dst: &mut Vec<i64>,
    src: &[u8],
    mt: MarshalType,
    first_timestamp: i64,
    items_count: usize,
) -> Result<(), String> {
    unmarshal_int64_array(dst, src, mt, first_timestamp, items_count).map_err(|err| {
        format!(
            "cannot unmarshal {items_count} timestamps from len(src)={} bytes: {err}",
            src.len()
        )
    })
}

/// Marshals `values`, appending the result to `dst`.
/// Returns the marshal type and the first value.
///
/// `precision_bits` must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
///
/// Go: MarshalValues.
pub fn marshal_values(dst: &mut Vec<u8>, values: &[i64], precision_bits: u8) -> (MarshalType, i64) {
    marshal_int64_array(dst, values, precision_bits)
}

/// Unmarshals values from `src`, appending them to `dst`.
///
/// `first_value` must be the value returned from [`marshal_values`].
///
/// Go: UnmarshalValues.
pub fn unmarshal_values(
    dst: &mut Vec<i64>,
    src: &[u8],
    mt: MarshalType,
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    unmarshal_int64_array(dst, src, mt, first_value, items_count).map_err(|err| {
        format!(
            "cannot unmarshal {items_count} values from len(src)={} bytes: {err}",
            src.len()
        )
    })
}

/// Runs `f` with a cleared thread-local `Vec<u8>` scratch buffer.
/// Replaces Go's bbPool (bytesutil.ByteBufferPool).
fn with_byte_scratch<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    thread_local! {
        static BB: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    }
    BB.with(|cell| {
        let mut b = cell.borrow_mut();
        b.clear();
        f(&mut b)
    })
}

/// Go: marshalInt64Array.
pub(crate) fn marshal_int64_array(
    dst: &mut Vec<u8>,
    a: &[i64],
    precision_bits: u8,
) -> (MarshalType, i64) {
    assert!(!a.is_empty(), "BUG: a must contain at least one item");

    if is_const(a) {
        return (MarshalType::Const, a[0]);
    }
    if is_delta_const(a) {
        let first_value = a[0];
        marshal_var_int64(dst, a[1].wrapping_sub(a[0]));
        return (MarshalType::DeltaConst, first_value);
    }

    with_byte_scratch(|bb| {
        let mut mt;
        let first_value;
        if is_gauge(a) {
            // Gauge values are better compressed with delta encoding.
            mt = MarshalType::ZstdNearestDelta;
            let mut pb = precision_bits;
            if pb < 6 {
                // Increase precision bits for gauges, since they suffer more
                // from low precision bits comparing to counters.
                pb += 2;
            }
            first_value = marshal_int64_nearest_delta(bb, a, pb);
        } else {
            // Non-gauge values, i.e. counters are better compressed with delta2 encoding.
            mt = MarshalType::ZstdNearestDelta2;
            first_value = marshal_int64_nearest_delta2(bb, a, precision_bits);
        }

        // Try compressing the result.
        let dst_orig_len = dst.len();
        if bb.len() >= MIN_COMPRESSIBLE_BLOCK_SIZE {
            let compress_level = get_compress_level(a.len());
            compress_zstd_level(dst, bb, compress_level);
        }
        if bb.len() < MIN_COMPRESSIBLE_BLOCK_SIZE
            || (dst.len() - dst_orig_len) as f64 > 0.9 * bb.len() as f64
        {
            // Ineffective compression. Store plain data.
            mt = match mt {
                MarshalType::ZstdNearestDelta2 => MarshalType::NearestDelta2,
                MarshalType::ZstdNearestDelta => MarshalType::NearestDelta,
                _ => unreachable!("BUG: unexpected mt={mt:?}"),
            };
            dst.truncate(dst_orig_len);
            dst.extend_from_slice(bb);
        }

        (mt, first_value)
    })
}

/// Go: unmarshalInt64Array.
pub(crate) fn unmarshal_int64_array(
    dst: &mut Vec<i64>,
    src: &[u8],
    mt: MarshalType,
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    // Extend dst capacity in order to eliminate memory allocations below.
    dst.reserve(items_count);

    match mt {
        MarshalType::ZstdNearestDelta => with_byte_scratch(|bb| {
            decompress_zstd(bb, src)
                .map_err(|err| format!("cannot decompress zstd data: {err}"))?;
            unmarshal_int64_nearest_delta(dst, bb, first_value, items_count).map_err(|err| {
                format!("cannot unmarshal nearest delta data after zstd decompression: {err}")
            })
        }),
        MarshalType::ZstdNearestDelta2 => with_byte_scratch(|bb| {
            decompress_zstd(bb, src)
                .map_err(|err| format!("cannot decompress zstd data: {err}"))?;
            unmarshal_int64_nearest_delta2(dst, bb, first_value, items_count).map_err(|err| {
                format!("cannot unmarshal nearest delta2 data after zstd decompression: {err}")
            })
        }),
        MarshalType::NearestDelta => {
            unmarshal_int64_nearest_delta(dst, src, first_value, items_count)
                .map_err(|err| format!("cannot unmarshal nearest delta data: {err}"))
        }
        MarshalType::NearestDelta2 => {
            unmarshal_int64_nearest_delta2(dst, src, first_value, items_count)
                .map_err(|err| format!("cannot unmarshal nearest delta2 data: {err}"))
        }
        MarshalType::Const => {
            if !src.is_empty() {
                return Err(format!(
                    "unexpected data left in const encoding: {} bytes",
                    src.len()
                ));
            }
            if first_value == 0 {
                fastnum::append_int64_zeros(dst, items_count);
                return Ok(());
            }
            if first_value == 1 {
                fastnum::append_int64_ones(dst, items_count);
                return Ok(());
            }
            for _ in 0..items_count {
                dst.push(first_value);
            }
            Ok(())
        }
        MarshalType::DeltaConst => {
            let mut v = first_value;
            let (d, n_len) = unmarshal_var_int64(src)
                .ok_or_else(|| "cannot unmarshal delta value for delta const".to_string())?;
            if n_len < src.len() {
                return Err(format!(
                    "unexpected trailing data after delta const (d={d}): {} bytes",
                    src.len() - n_len
                ));
            }
            for _ in 0..items_count {
                dst.push(v);
                v = v.wrapping_add(d);
            }
            Ok(())
        }
    }
}

/// Makes sure the first item in `a` is `v_min`, the last item in `a` is `v_max`
/// and all the items in `a` are non-decreasing.
///
/// If this isn't the case then `a` is fixed accordingly.
///
/// Go: EnsureNonDecreasingSequence.
pub fn ensure_non_decreasing_sequence(a: &mut [i64], v_min: i64, v_max: i64) {
    assert!(
        v_max >= v_min,
        "BUG: vMax cannot be smaller than vMin; got {v_max} vs {v_min}"
    );
    if a.is_empty() {
        return;
    }
    if a[0] != v_min {
        a[0] = v_min;
    }
    let mut v_prev = a[0];
    for v in a[1..].iter_mut() {
        if *v < v_prev {
            *v = v_prev;
        }
        v_prev = *v;
    }
    let i = a.len() - 1;
    if a[i] != v_max {
        a[i] = v_max;
        let mut i = i;
        while i > 0 && a[i - 1] > v_max {
            a[i - 1] = v_max;
            i -= 1;
        }
    }
}

/// Returns true if `a` contains only equal values. Go: isConst.
fn is_const(a: &[i64]) -> bool {
    if a.is_empty() {
        return false;
    }
    if fastnum::is_int64_zeros(a) {
        // Fast path for array containing only zeros.
        return true;
    }
    if fastnum::is_int64_ones(a) {
        // Fast path for array containing only ones.
        return true;
    }
    let v1 = a[0];
    a.iter().all(|&v| v == v1)
}

/// Returns true if `a` contains a counter with constant delta. Go: isDeltaConst.
fn is_delta_const(a: &[i64]) -> bool {
    if a.len() < 2 {
        return false;
    }
    let d1 = a[1].wrapping_sub(a[0]);
    let mut prev = a[1];
    for &next in &a[2..] {
        if next.wrapping_sub(prev) != d1 {
            return false;
        }
        prev = next;
    }
    true
}

/// Returns true if `a` contains gauge values, i.e. arbitrary changing values.
///
/// It is OK if a few gauges aren't detected (i.e. detected as counters),
/// since misdetected counters as gauges leads to worse compression ratio.
///
/// Go: isGauge.
fn is_gauge(a: &[i64]) -> bool {
    // Check all the items in a, since a part of items may lead
    // to incorrect gauge detection.

    if a.len() < 2 {
        return false;
    }

    let mut resets = 0usize;
    let mut v_prev = a[0];
    if v_prev < 0 {
        // Counter values cannot be negative.
        return true;
    }
    for &v in &a[1..] {
        if v < v_prev {
            if v < 0 {
                // Counter values cannot be negative.
                return true;
            }
            if v > (v_prev >> 3) {
                // Decreasing sequence detected. This is a gauge.
                return true;
            }
            // Possible counter reset.
            resets += 1;
        }
        v_prev = v;
    }
    if resets <= 2 {
        // Counter with a few resets.
        return false;
    }

    // Let it be a gauge if resets exceeds len(a)/8, otherwise assume counter.
    resets > (a.len() >> 3)
}

/// Go: getCompressLevel.
fn get_compress_level(items_count: usize) -> i32 {
    if items_count <= 1 << 6 {
        return 1;
    }
    if items_count <= 1 << 8 {
        return 2;
    }
    if items_count <= 1 << 10 {
        return 3;
    }
    if items_count <= 1 << 12 {
        return 4;
    }
    5
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{check_precision_bits_arrays, Rng};

    // Port of TestIsConst.
    #[test]
    fn test_is_const() {
        let f = |a: &[i64], ok_expected: bool| {
            assert_eq!(is_const(a), ok_expected, "unexpected isConst for a={a:?}");
        };
        f(&[], false);
        f(&[1], true);
        f(&[1, 2], false);
        f(&[1, 1], true);
        f(&[1, 1, 1], true);
        f(&[1, 1, 2], false);
    }

    // Port of TestIsDeltaConst.
    #[test]
    fn test_is_delta_const() {
        let f = |a: &[i64], ok_expected: bool| {
            assert_eq!(
                is_delta_const(a),
                ok_expected,
                "unexpected isDeltaConst for a={a:?}"
            );
        };
        f(&[], false);
        f(&[1], false);
        f(&[1, 2], true);
        f(&[1, 2, 3], true);
        f(&[3, 2, 1], true);
        f(&[3, 2, 1, 0, -1, -2], true);
        f(&[3, 2, 1, 0, -1, -2, 2], false);
        f(&[1, 1], true);
        f(&[1, 2, 1], false);
        f(&[1, 2, 4], false);
    }

    // Port of TestIsGauge.
    #[test]
    fn test_is_gauge() {
        let f = |a: &[i64], ok_expected: bool| {
            assert_eq!(
                is_gauge(a),
                ok_expected,
                "unexpected result for isGauge({a:?})"
            );
        };
        f(&[], false);
        f(&[0], false);
        f(&[1, 2], false);
        f(&[0, 1, 2, 3, 4, 5], false);
        f(&[0, -1, -2, -3, -4], true);
        f(&[0, 0, 0, 0, 0, 0, 0], false);
        f(&[1, 1, 1, 1, 1], false);
        f(&[1, 1, 2, 2, 2, 2], false);
        f(&[1, 17, 2, 3], false); // a single counter reset
        f(&[1, 5, 2, 3], true);
        f(&[1, 5, 2, 3, 2], true);
        f(&[-1, -5, -2, -3], true);
        f(&[-1, -5, -2, -3, -2], true);
        f(&[5, 6, 4, 3, 2], true);
        f(&[4, 5, 6, 5, 4, 3, 2], true);
        f(&[1064, 1132, 1083, 1062, 856, 747], true);
    }

    // Port of TestEnsureNonDecreasingSequence.
    #[test]
    fn test_ensure_non_decreasing_sequence() {
        check_ends(&[], -1234, -34, &[]);
        check_ends(&[123], -1234, -1234, &[-1234]);
        check_ends(&[123], -1234, 345, &[345]);
        check_ends(&[-23, -14], -23, -14, &[-23, -14]);
        check_ends(&[-23, -14], -25, 0, &[-25, 0]);
        check_ends(&[0, -1, 10, 5, 6, 7], 2, 8, &[2, 2, 8, 8, 8, 8]);
        check_ends(&[0, -1, 10, 5, 6, 7], -2, 8, &[-2, -1, 8, 8, 8, 8]);
        check_ends(&[0, -1, 10, 5, 6, 7], -2, 12, &[-2, -1, 10, 10, 10, 12]);
        check_ends(&[1, 2, 1, 3, 4, 5], 1, 5, &[1, 2, 2, 3, 4, 5]);
    }

    fn check_ends(a: &[i64], v_min: i64, v_max: i64, a_expected: &[i64]) {
        let mut a = a.to_vec();
        ensure_non_decreasing_sequence(&mut a, v_min, v_max);
        assert_eq!(a, a_expected, "unexpected a");
    }

    // Port of testMarshalUnmarshalInt64Array (shared helper).
    fn check_marshal_unmarshal_int64_array(
        va: &[i64],
        precision_bits: u8,
        mt_expected: MarshalType,
    ) {
        let mut b = Vec::new();
        let (mt, first_value) = marshal_int64_array(&mut b, va, precision_bits);
        assert_eq!(
            mt,
            mt_expected,
            "unexpected MarshalType for va.len()={}, precisionBits={precision_bits}",
            va.len()
        );
        let mut va_new = Vec::new();
        unmarshal_int64_array(&mut va_new, &b, mt, first_value, va.len())
            .unwrap_or_else(|err| panic!("unexpected error when unmarshaling: {err}"));
        match mt {
            MarshalType::ZstdNearestDelta
            | MarshalType::ZstdNearestDelta2
            | MarshalType::NearestDelta
            | MarshalType::NearestDelta2 => {
                check_precision_bits_arrays(va, &va_new, precision_bits)
                    .unwrap_or_else(|err| panic!("too low precision for vaNew: {err}"));
            }
            _ => assert_eq!(
                va,
                &va_new[..],
                "unexpected vaNew for precisionBits={precision_bits}"
            ),
        }

        let b_prefix = [1u8, 2, 3];
        let mut b_new = b_prefix.to_vec();
        let (mt_new, first_value_new) = marshal_int64_array(&mut b_new, va, precision_bits);
        assert_eq!(
            first_value_new, first_value,
            "unexpected firstValue for prefixed va"
        );
        assert_eq!(&b_new[..b_prefix.len()], &b_prefix, "unexpected prefix");
        assert_eq!(
            &b_new[b_prefix.len()..],
            &b[..],
            "unexpected b for prefixed va"
        );
        assert_eq!(mt_new, mt, "unexpected mt for prefixed va");

        let va_prefix = [4i64, 5, 6, 8];
        let mut va_new = va_prefix.to_vec();
        unmarshal_int64_array(&mut va_new, &b, mt, first_value, va.len())
            .unwrap_or_else(|err| panic!("unexpected error when unmarshaling prefixed va: {err}"));
        assert_eq!(&va_new[..va_prefix.len()], &va_prefix, "unexpected prefix");
        match mt {
            MarshalType::ZstdNearestDelta
            | MarshalType::ZstdNearestDelta2
            | MarshalType::NearestDelta
            | MarshalType::NearestDelta2 => {
                check_precision_bits_arrays(&va_new[va_prefix.len()..], va, precision_bits)
                    .unwrap_or_else(|err| panic!("too low precision for prefixed vaNew: {err}"));
            }
            _ => assert_eq!(&va_new[va_prefix.len()..], va, "unexpected prefixed vaNew"),
        }
    }

    // Port of TestMarshalUnmarshalTimestamps.
    #[test]
    fn test_marshal_unmarshal_timestamps() {
        let mut r = Rng::new(1);
        const PRECISION_BITS: u8 = 3;

        let mut timestamps = Vec::new();
        let mut v: i64 = 0;
        for _ in 0..8 * 1024 {
            v += 30_000 * (r.norm_f64() * 5e2) as i64;
            timestamps.push(v);
        }
        let mut result = Vec::new();
        let (mt, first_timestamp) = marshal_timestamps(&mut result, &timestamps, PRECISION_BITS);
        let mut timestamps2 = Vec::new();
        unmarshal_timestamps(
            &mut timestamps2,
            &result,
            mt,
            first_timestamp,
            timestamps.len(),
        )
        .unwrap_or_else(|err| panic!("cannot unmarshal timestamps: {err}"));
        check_precision_bits_arrays(&timestamps, &timestamps2, PRECISION_BITS)
            .unwrap_or_else(|err| panic!("too low precision for timestamps: {err}"));
    }

    // Port of TestMarshalUnmarshalValues.
    #[test]
    fn test_marshal_unmarshal_values() {
        let mut r = Rng::new(1);
        const PRECISION_BITS: u8 = 3;

        let mut values = Vec::new();
        let mut v: i64 = 0;
        for _ in 0..8 * 1024 {
            v += (r.norm_f64() * 1e2) as i64;
            values.push(v);
        }
        let mut result = Vec::new();
        let (mt, first_value) = marshal_values(&mut result, &values, PRECISION_BITS);
        let mut values2 = Vec::new();
        unmarshal_values(&mut values2, &result, mt, first_value, values.len())
            .unwrap_or_else(|err| panic!("cannot unmarshal values: {err}"));
        check_precision_bits_arrays(&values, &values2, PRECISION_BITS)
            .unwrap_or_else(|err| panic!("too low precision for values: {err}"));
    }

    // Port of TestMarshalUnmarshalInt64ArrayGeneric.
    #[test]
    fn test_marshal_unmarshal_int64_array_generic() {
        check_marshal_unmarshal_int64_array(&[1, 20, 234], 4, MarshalType::NearestDelta2);
        check_marshal_unmarshal_int64_array(
            &[1, 20, -2345, 678934, 342],
            4,
            MarshalType::NearestDelta,
        );
        check_marshal_unmarshal_int64_array(
            &[1, 20, 2345, 6789, 12342],
            4,
            MarshalType::NearestDelta2,
        );

        // Constant encoding
        check_marshal_unmarshal_int64_array(&[1], 4, MarshalType::Const);
        check_marshal_unmarshal_int64_array(&[1, 2], 4, MarshalType::DeltaConst);
        check_marshal_unmarshal_int64_array(&[-1, 0, 1, 2, 3, 4, 5], 4, MarshalType::DeltaConst);
        check_marshal_unmarshal_int64_array(&[-10, -1, 8, 17, 26], 4, MarshalType::DeltaConst);
        check_marshal_unmarshal_int64_array(&[0, 0, 0, 0, 0, 0], 4, MarshalType::Const);
        check_marshal_unmarshal_int64_array(&[100, 100, 100, 100], 4, MarshalType::Const);
    }

    // Port of TestMarshalUnmarshalInt64Array (encoding_pure_test.go).
    #[test]
    fn test_marshal_unmarshal_int64_array() {
        let mut r = Rng::new(1);

        // Verify nearest delta encoding.
        let mut va = Vec::new();
        let mut v: i64 = 0;
        for _ in 0..8 * 1024 {
            v += (r.norm_f64() * 1e6) as i64;
            va.push(v);
        }
        for precision_bits in 1..14u8 {
            check_marshal_unmarshal_int64_array(&va, precision_bits, MarshalType::ZstdNearestDelta);
        }
        for precision_bits in 23..65u8 {
            check_marshal_unmarshal_int64_array(&va, precision_bits, MarshalType::NearestDelta);
        }

        // Verify nearest delta2 encoding.
        let mut va = Vec::new();
        let mut v: i64 = 0;
        for _ in 0..8 * 1024 {
            v += 30_000_000 + (r.norm_f64() * 1e6) as i64;
            va.push(v);
        }
        for precision_bits in 1..15u8 {
            check_marshal_unmarshal_int64_array(
                &va,
                precision_bits,
                MarshalType::ZstdNearestDelta2,
            );
        }
        for precision_bits in 24..65u8 {
            check_marshal_unmarshal_int64_array(&va, precision_bits, MarshalType::NearestDelta2);
        }

        // Verify nearest delta encoding for small arrays.
        let mut va = Vec::new();
        let mut v: i64 = 1000;
        for _ in 0..6 {
            v += (r.norm_f64() * 100.0) as i64;
            va.push(v);
        }
        for precision_bits in 1..65u8 {
            check_marshal_unmarshal_int64_array(&va, precision_bits, MarshalType::NearestDelta);
        }

        // Verify nearest delta2 encoding for small arrays.
        let mut va = Vec::new();
        let mut v: i64 = 0;
        for _ in 0..6 {
            v += 3000 + (r.norm_f64() * 100.0) as i64;
            va.push(v);
        }
        for precision_bits in 5..65u8 {
            check_marshal_unmarshal_int64_array(&va, precision_bits, MarshalType::NearestDelta2);
        }
    }

    // Port of TestMarshalInt64ArraySize (encoding_pure_test.go).
    #[test]
    fn test_marshal_int64_array_size() {
        let mut r = Rng::new(1);

        let mut va = Vec::new();
        let mut v = (r.f64() * 1e9) as i64;
        for _ in 0..8 * 1024 {
            va.push(v);
            v += 30_000 + (r.norm_f64() * 1e3) as i64;
        }

        // Note: the min bounds for precisionBits 2 and 3 are lowered vs Go
        // (600->400, 900->700) since this port uses a different deterministic
        // RNG and libzstd instead of klauspost/compress.
        check_marshal_size(&va, 1, 500, 1700);
        check_marshal_size(&va, 2, 400, 1800);
        check_marshal_size(&va, 3, 700, 2100);
        check_marshal_size(&va, 4, 1300, 2200);
        check_marshal_size(&va, 5, 2000, 3300);
        check_marshal_size(&va, 6, 3000, 5000);
        check_marshal_size(&va, 7, 4000, 6500);
        check_marshal_size(&va, 8, 6000, 8000);
        check_marshal_size(&va, 9, 7000, 8800);
        check_marshal_size(&va, 10, 8000, 17000);
    }

    fn check_marshal_size(va: &[i64], precision_bits: u8, min_size: usize, max_size: usize) {
        let mut b = Vec::new();
        marshal_int64_array(&mut b, va, precision_bits);
        assert!(
            b.len() <= max_size,
            "too big size for marshaled {} items with precisionBits {precision_bits}: got {}; expecting <= {max_size}",
            va.len(),
            b.len()
        );
        assert!(
            b.len() >= min_size,
            "too small size for marshaled {} items with precisionBits {precision_bits}: got {}; expecting >= {min_size}",
            va.len(),
            b.len()
        );
    }

    #[test]
    fn test_marshal_type_roundtrip() {
        for v in 1..=6u8 {
            let mt = MarshalType::from_u8(v).unwrap();
            assert_eq!(mt.as_u8(), v);
        }
        assert!(MarshalType::from_u8(0).is_none());
        assert!(MarshalType::from_u8(7).is_none());
        assert!(check_marshal_type(6).is_ok());
        assert!(check_marshal_type(0).is_ok());
        assert!(check_marshal_type(7).is_err());
        assert!(MarshalType::NearestDelta.needs_validation());
        assert!(MarshalType::NearestDelta2.needs_validation());
        assert!(!MarshalType::ZstdNearestDelta.needs_validation());
        assert!(!MarshalType::ZstdNearestDelta2.needs_validation());
        assert!(!MarshalType::Const.needs_validation());
        assert!(!MarshalType::DeltaConst.needs_validation());
    }
}
