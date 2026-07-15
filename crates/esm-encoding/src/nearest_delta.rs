//! `nearest delta` encoding. Port of Go lib/encoding/nearest_delta.go.

use crate::encoding::check_precision_bits;
use crate::int::{marshal_var_int64s, unmarshal_var_int64s};
use crate::with_int64_scratch;

/// Encodes `src` using `nearest delta` encoding with the given `precision_bits`
/// and appends the encoded data to `dst`. Returns the first value.
///
/// `precision_bits` must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
///
/// Go: marshalInt64NearestDelta.
pub(crate) fn marshal_int64_nearest_delta(
    dst: &mut Vec<u8>,
    src: &[i64],
    precision_bits: u8,
) -> i64 {
    assert!(
        !src.is_empty(),
        "BUG: src must contain at least 1 item; got {} items",
        src.len()
    );
    if let Err(err) = check_precision_bits(precision_bits) {
        panic!("BUG: {err}");
    }

    let first_value = src[0];
    let mut v = src[0];
    let src = &src[1..];
    with_int64_scratch(src.len(), |is| {
        if precision_bits == 64 {
            // Fast path.
            for (i, &next) in src.iter().enumerate() {
                let d = next.wrapping_sub(v);
                v = v.wrapping_add(d);
                is[i] = d;
            }
        } else {
            // Slower path.
            let mut trailing_zeros = get_trailing_zeros(v, precision_bits);
            for (i, &next) in src.iter().enumerate() {
                let (d, tzs) = nearest_delta(next, v, precision_bits, trailing_zeros);
                trailing_zeros = tzs;
                v = v.wrapping_add(d);
                is[i] = d;
            }
        }
        marshal_var_int64s(dst, is);
    });
    first_value
}

/// Decodes `nearest delta`-encoded `src`, appending `items_count` values to `dst`.
///
/// `first_value` must be the value returned from [`marshal_int64_nearest_delta`].
///
/// Go: unmarshalInt64NearestDelta.
pub(crate) fn unmarshal_int64_nearest_delta(
    dst: &mut Vec<i64>,
    src: &[u8],
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    assert!(
        items_count >= 1,
        "BUG: items_count must be greater than 0; got {items_count}"
    );

    with_int64_scratch(items_count - 1, |is| {
        let tail = unmarshal_var_int64s(is, src).map_err(|err| {
            format!(
                "cannot unmarshal nearest delta from {} bytes: {err}",
                src.len()
            )
        })?;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected tail left after unmarshaling {items_count} items from {} bytes; tail size={}",
                src.len(),
                tail.len()
            ));
        }

        let mut v = first_value;
        dst.push(v);
        for &d in is.iter() {
            v = v.wrapping_add(d);
            dst.push(v);
        }
        Ok(())
    })
}

/// Returns the nearest value for `next - prev` with the given `precision_bits`,
/// plus the number of zeroed trailing bits in the returned delta.
///
/// Go: nearestDelta.
pub(crate) fn nearest_delta(
    next: i64,
    prev: i64,
    precision_bits: u8,
    prev_trailing_zeros: u8,
) -> (i64, u8) {
    let d = next.wrapping_sub(prev);
    if d == 0 {
        // Fast path.
        return (0, dec_if_non_zero(prev_trailing_zeros));
    }

    let mut origin = next;
    if origin < 0 {
        // There is no need in handling the special case origin = -1<<63 (matches Go).
        origin = origin.wrapping_neg();
    }

    let origin_bits = (64 - (origin as u64).leading_zeros()) as u8;
    if origin_bits <= precision_bits {
        // Cannot zero trailing bits for the given precision_bits.
        return (d, dec_if_non_zero(prev_trailing_zeros));
    }

    // origin_bits > precision_bits. May zero trailing bits in d.
    let trailing_zeros = origin_bits - precision_bits;
    if trailing_zeros > prev_trailing_zeros + 4 {
        // Probably counter reset. Return d with full precision.
        return (d, prev_trailing_zeros + 2);
    }
    if trailing_zeros + 4 < prev_trailing_zeros {
        // Probably counter reset. Return d with full precision.
        return (d, prev_trailing_zeros - 2);
    }

    // Zero trailing bits in d.
    let mut d = d;
    let minus = d < 0;
    if minus {
        // There is no need in handling the special case d = -1<<63 (matches Go).
        d = d.wrapping_neg();
    }
    let mut nd = ((d as u64) & (u64::MAX << trailing_zeros)) as i64;
    if minus {
        nd = nd.wrapping_neg();
    }
    (nd, trailing_zeros)
}

/// Go: decIfNonZero.
#[inline]
fn dec_if_non_zero(n: u8) -> u8 {
    n.saturating_sub(1)
}

/// Go: getTrailingZeros.
pub(crate) fn get_trailing_zeros(v: i64, precision_bits: u8) -> u8 {
    // There is no need in special case handling for v = -1<<63 (matches Go).
    let v = if v < 0 { v.wrapping_neg() } else { v };
    let v_bits = (64 - (v as u64).leading_zeros()) as u8;
    if v_bits <= precision_bits {
        return 0;
    }
    v_bits - precision_bits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{check_precision_bits_arrays, to_hex, Rng};

    // Port of TestMarshalInt64NearestDelta.
    #[test]
    fn test_marshal_int64_nearest_delta() {
        check_marshal(&[0], 4, 0, "");
        check_marshal(&[0, 0], 4, 0, "00");
        check_marshal(&[1, -3], 4, 1, "07");
        check_marshal(&[255, 255], 4, 255, "00");
        check_marshal(&[0, 1, 2, 3, 4, 5], 4, 0, "0202020202");
        check_marshal(&[5, 4, 3, 2, 1, 0], 1, 5, "0003000301");
        check_marshal(&[5, 4, 3, 2, 1, 0], 2, 5, "0003010101");
        check_marshal(&[5, 4, 3, 2, 1, 0], 3, 5, "0101010101");
        check_marshal(&[5, 4, 3, 2, 1, 0], 4, 5, "0101010101");

        check_marshal(&[-500, -600, -700, -800, -890], 1, -500, "00000000");
        check_marshal(&[-500, -600, -700, -800, -890], 2, -500, "0000ff0300");
        check_marshal(&[-500, -600, -700, -800, -890], 3, -500, "00ff01ff01ff01");
        check_marshal(&[-500, -600, -700, -800, -890], 4, -500, "7fff017fff01");
        check_marshal(&[-500, -600, -700, -800, -890], 5, -500, "bf01bf01bf01bf01");
        check_marshal(&[-500, -600, -700, -800, -890], 6, -500, "bf01bf01bf01bf01");
        check_marshal(&[-500, -600, -700, -800, -890], 7, -500, "bf01cf01bf01af01");
        check_marshal(&[-500, -600, -700, -800, -890], 8, -500, "c701c701c701af01");
    }

    fn check_marshal(va: &[i64], precision_bits: u8, first_value_expected: i64, b_expected: &str) {
        let mut b = Vec::new();
        let first_value = marshal_int64_nearest_delta(&mut b, va, precision_bits);
        assert_eq!(
            first_value, first_value_expected,
            "unexpected firstValue for va={va:?}, precisionBits={precision_bits}"
        );
        assert_eq!(
            to_hex(&b),
            b_expected,
            "invalid marshaled data for va={va:?}, precisionBits={precision_bits}"
        );

        let prefix = b"foobar";
        let mut b = prefix.to_vec();
        let first_value = marshal_int64_nearest_delta(&mut b, va, precision_bits);
        assert_eq!(first_value, first_value_expected);
        assert_eq!(&b[..prefix.len()], prefix, "invalid prefix for va={va:?}");
        assert_eq!(
            to_hex(&b[prefix.len()..]),
            b_expected,
            "invalid marshaled prefixed data for va={va:?}, precisionBits={precision_bits}"
        );
    }

    // Port of TestMarshalUnmarshalInt64NearestDelta.
    #[test]
    fn test_marshal_unmarshal_int64_nearest_delta() {
        let mut r = Rng::new(1);

        check_roundtrip(&[0], 4);
        check_roundtrip(&[0, 0], 4);
        check_roundtrip(&[1, -3], 4);
        check_roundtrip(&[255, 255], 4);
        check_roundtrip(&[0, 1, 2, 3, 4, 5], 4);
        check_roundtrip(&[5, 4, 3, 2, 1, 0], 4);
        check_roundtrip(
            &[
                -5_000_000_000_000,
                -6e12 as i64,
                -7e12 as i64,
                -8e12 as i64,
                -8.9e12 as i64,
            ],
            1,
        );
        check_roundtrip(
            &[
                -5e12 as i64,
                -6e12 as i64,
                -7e12 as i64,
                -8e12 as i64,
                -8.9e12 as i64,
            ],
            2,
        );
        check_roundtrip(
            &[
                -5e12 as i64,
                -6e12 as i64,
                -7e12 as i64,
                -8e12 as i64,
                -8.9e12 as i64,
            ],
            3,
        );
        check_roundtrip(
            &[
                -5e12 as i64,
                -5.6e12 as i64,
                -7e12 as i64,
                -8e12 as i64,
                -8.9e12 as i64,
            ],
            4,
        );

        // Verify constant encoding.
        let va: Vec<i64> = vec![9876543210123; 1024];
        check_roundtrip(&va, 4);
        check_roundtrip(&va, 63);

        // Verify encoding for monotonically incremented va.
        let mut v: i64 = -35;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v += 8;
            va.push(v);
        }
        check_roundtrip(&va, 4);
        check_roundtrip(&va, 63);

        // Verify encoding for monotonically decremented va.
        let mut v: i64 = 793;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v -= 16;
            va.push(v);
        }
        check_roundtrip(&va, 4);
        check_roundtrip(&va, 63);

        // Verify encoding for quadratically incremented va.
        let mut v: i64 = -1234567;
        let mut va = Vec::new();
        for i in 0..1024 {
            v += 32 + i as i64;
            va.push(v);
        }
        check_roundtrip(&va, 4);

        // Verify encoding for decremented va with norm-float noise.
        let mut v: i64 = 787933;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v -= 25 + (r.norm_f64() * 2.0) as i64;
            va.push(v);
        }
        check_roundtrip(&va, 4);

        // Verify encoding for incremented va with random noise.
        let mut v: i64 = 943854;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v += 30 + r.i64n(5);
            va.push(v);
        }
        check_roundtrip(&va, 4);

        // Verify encoding for constant va with norm-float noise.
        let mut v: i64 = -12345;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v += (r.norm_f64() * 10.0) as i64;
            va.push(v);
        }
        check_roundtrip(&va, 4);

        // Verify encoding for constant va with random noise.
        let mut v: i64 = -12345;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v += r.i64n(15) - 1;
            va.push(v);
        }
        check_roundtrip(&va, 4);
    }

    // Full precision-bits sweep for a variety of sequences.
    #[test]
    fn test_marshal_unmarshal_int64_nearest_delta_precision_bits_sweep() {
        let mut r = Rng::new(7);
        let mut v: i64 = 0;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v += (r.norm_f64() * 1e6) as i64;
            va.push(v);
        }
        for precision_bits in 1..=64u8 {
            check_roundtrip(&va, precision_bits);
        }
    }

    fn check_roundtrip(va: &[i64], precision_bits: u8) {
        let mut b = Vec::new();
        let first_value = marshal_int64_nearest_delta(&mut b, va, precision_bits);
        let mut va_new = Vec::new();
        unmarshal_int64_nearest_delta(&mut va_new, &b, first_value, va.len()).unwrap_or_else(
            |err| {
                panic!("cannot unmarshal data for va={va:?}, precisionBits={precision_bits}: {err}")
            },
        );
        check_precision_bits_arrays(&va_new, va, precision_bits).unwrap_or_else(|err| {
            panic!("too small precisionBits for va={va:?}, precisionBits={precision_bits}: {err}")
        });

        let va_prefix = [1i64, 2, 3, 4];
        let mut va_new = va_prefix.to_vec();
        unmarshal_int64_nearest_delta(&mut va_new, &b, first_value, va.len()).unwrap_or_else(
            |err| {
                panic!("cannot unmarshal prefixed data for precisionBits={precision_bits}: {err}")
            },
        );
        assert_eq!(&va_new[..va_prefix.len()], &va_prefix, "unexpected prefix");
        check_precision_bits_arrays(&va_new[va_prefix.len()..], va, precision_bits)
            .unwrap_or_else(|err| panic!("too small precisionBits for prefixed va: {err}"));
    }
}
