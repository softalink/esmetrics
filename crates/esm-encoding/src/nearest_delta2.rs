//! `nearest delta2` encoding. Port of Go lib/encoding/nearest_delta2.go.

use crate::encoding::check_precision_bits;
use crate::int::{marshal_var_int64, marshal_var_int64s, unmarshal_var_int64s};
use crate::nearest_delta::{get_trailing_zeros, nearest_delta};
use crate::with_int64_scratch;

/// Encodes `src` using `nearest delta2` encoding with the given `precision_bits`
/// and appends the encoded data to `dst`. Returns the first value.
///
/// `precision_bits` must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
///
/// Go: marshalInt64NearestDelta2.
pub(crate) fn marshal_int64_nearest_delta2(
    dst: &mut Vec<u8>,
    src: &[i64],
    precision_bits: u8,
) -> i64 {
    assert!(
        src.len() >= 2,
        "BUG: src must contain at least 2 items; got {} items",
        src.len()
    );
    if let Err(err) = check_precision_bits(precision_bits) {
        panic!("BUG: {err}");
    }

    let first_value = src[0];
    let mut d1 = src[1].wrapping_sub(src[0]);
    marshal_var_int64(dst, d1);
    let mut v = src[1];
    let src = &src[2..];
    with_int64_scratch(src.len(), |is| {
        if precision_bits == 64 {
            // Fast path.
            for (i, &next) in src.iter().enumerate() {
                let d2 = next.wrapping_sub(v).wrapping_sub(d1);
                d1 = d1.wrapping_add(d2);
                v = v.wrapping_add(d1);
                is[i] = d2;
            }
        } else {
            // Slower path.
            let mut trailing_zeros = get_trailing_zeros(v, precision_bits);
            for (i, &next) in src.iter().enumerate() {
                let (d2, tzs) =
                    nearest_delta(next.wrapping_sub(v), d1, precision_bits, trailing_zeros);
                trailing_zeros = tzs;
                d1 = d1.wrapping_add(d2);
                v = v.wrapping_add(d1);
                is[i] = d2;
            }
        }
        marshal_var_int64s(dst, is);
    });
    first_value
}

/// Decodes `nearest delta2`-encoded `src`, appending `items_count` values to `dst`.
///
/// `first_value` must be the value returned from [`marshal_int64_nearest_delta2`].
///
/// Go: unmarshalInt64NearestDelta2.
pub(crate) fn unmarshal_int64_nearest_delta2(
    dst: &mut Vec<i64>,
    src: &[u8],
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    assert!(
        items_count >= 2,
        "BUG: items_count must be greater than 1; got {items_count}"
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

        let dst_len = dst.len();
        dst.resize(dst_len + items_count, 0);
        let a = &mut dst[dst_len..];

        let mut v = first_value;
        let mut d1 = is[0];
        a[0] = v;
        v = v.wrapping_add(d1);
        a[1] = v;
        for (i, &d2) in is[1..].iter().enumerate() {
            d1 = d1.wrapping_add(d2);
            v = v.wrapping_add(d1);
            a[i + 2] = v;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nearest_delta::{get_trailing_zeros, nearest_delta};
    use crate::testutil::{check_precision_bits_arrays, to_hex, Rng};

    // Port of TestNearestDelta (lives in nearest_delta2_test.go in Go).
    #[test]
    fn test_nearest_delta() {
        check_nd(0, 0, 1, 0, 0);
        check_nd(0, 0, 2, 0, 0);
        check_nd(0, 0, 3, 0, 0);
        check_nd(0, 0, 4, 0, 0);

        check_nd(100, 100, 4, 0, 2);
        check_nd(123456, 123456, 4, 0, 12);
        check_nd(-123456, -123456, 4, 0, 12);
        check_nd(9876543210, 9876543210, 4, 0, 29);

        check_nd(1, 2, 3, -1, 0);
        check_nd(2, 1, 3, 1, 0);
        check_nd(-1, -2, 3, 1, 0);
        check_nd(-2, -1, 3, -1, 0);

        check_nd(0, 1, 1, -1, 0);
        check_nd(1, 2, 1, -1, 0);
        check_nd(2, 3, 1, 0, 1);
        check_nd(1, 0, 1, 1, 0);
        check_nd(2, 1, 1, 0, 1);
        check_nd(2, 1, 2, 1, 0);
        check_nd(2, 1, 3, 1, 0);

        check_nd(0, -1, 1, 1, 0);
        check_nd(-1, -2, 1, 1, 0);
        check_nd(-2, -3, 1, 0, 1);
        check_nd(-1, 0, 1, -1, 0);
        check_nd(-2, -1, 1, 0, 1);
        check_nd(-2, -1, 2, -1, 0);
        check_nd(-2, -1, 3, -1, 0);

        check_nd(0, 2, 3, -2, 0);
        check_nd(3, 0, 3, 3, 0);
        check_nd(4, 0, 3, 4, 0);
        check_nd(5, 0, 3, 5, 0);
        check_nd(6, 0, 3, 6, 0);
        check_nd(0, 7, 3, -7, 0);
        check_nd(8, 0, 3, 8, 1);
        check_nd(9, 0, 3, 8, 1);
        check_nd(15, 0, 3, 14, 1);
        check_nd(16, 0, 3, 16, 2);
        check_nd(17, 0, 3, 16, 2);
        check_nd(18, 0, 3, 16, 2);
        check_nd(0, 59, 6, -59, 0);

        check_nd(128, 121, 1, 0, 7);
        check_nd(128, 121, 2, 0, 6);
        check_nd(128, 121, 3, 0, 5);
        check_nd(128, 121, 4, 0, 4);
        check_nd(128, 121, 5, 0, 3);
        check_nd(128, 121, 6, 4, 2);
        check_nd(128, 121, 7, 6, 1);
        check_nd(128, 121, 8, 7, 0);

        check_nd(32, 37, 1, 0, 5);
        check_nd(32, 37, 2, 0, 4);
        check_nd(32, 37, 3, 0, 3);
        check_nd(32, 37, 4, -4, 2);
        check_nd(32, 37, 5, -4, 1);
        check_nd(32, 37, 6, -5, 0);

        check_nd(-10, 20, 1, -24, 3);
        check_nd(-10, 20, 2, -28, 2);
        check_nd(-10, 20, 3, -30, 1);
        check_nd(-10, 20, 4, -30, 0);
        check_nd(-10, 21, 4, -31, 0);
        check_nd(-10, 21, 5, -31, 0);

        check_nd(10, -20, 1, 24, 3);
        check_nd(10, -20, 2, 28, 2);
        check_nd(10, -20, 3, 30, 1);
        check_nd(10, -20, 4, 30, 0);
        check_nd(10, -21, 4, 31, 0);
        check_nd(10, -21, 5, 31, 0);

        check_nd(1234e12 as i64, 1235e12 as i64, 1, 0, 50);
        check_nd(1234e12 as i64, 1235e12 as i64, 10, 0, 41);
        check_nd(1234e12 as i64, 1235e12 as i64, 35, -999999995904, 16);

        check_nd(i64::MAX, 0, 1, i64::MAX, 2);
    }

    fn check_nd(next: i64, prev: i64, precision_bits: u8, d_expected: i64, tz_expected: u8) {
        let tz = get_trailing_zeros(prev, precision_bits);
        let (d, trailing_zero_bits) = nearest_delta(next, prev, precision_bits, tz);
        assert_eq!(
            d, d_expected,
            "unexpected d for next={next}, prev={prev}, precisionBits={precision_bits}"
        );
        assert_eq!(
            trailing_zero_bits, tz_expected,
            "unexpected trailingZeroBits for next={next}, prev={prev}, precisionBits={precision_bits}"
        );
    }

    // Port of TestMarshalInt64NearestDelta2.
    #[test]
    fn test_marshal_int64_nearest_delta2() {
        check_marshal(&[0, 0], 4, 0, "00");
        check_marshal(&[1, -3], 4, 1, "07");
        check_marshal(&[255, 255], 4, 255, "00");
        check_marshal(&[0, 1, 2, 3, 4, 5], 4, 0, "0200000000");
        check_marshal(&[5, 4, 3, 2, 1, 0], 4, 5, "0100000000");

        check_marshal(&[-5000, -6000, -7000, -8000, -8900], 1, -5000, "cf0f000000");
        check_marshal(&[-5000, -6000, -7000, -8000, -8900], 2, -5000, "cf0f000000");
        check_marshal(&[-5000, -6000, -7000, -8000, -8900], 3, -5000, "cf0f000000");
        check_marshal(
            &[-5000, -6000, -7000, -8000, -8900],
            4,
            -5000,
            "cf0f00008001",
        );
        check_marshal(
            &[-5000, -6000, -7000, -8000, -8900],
            5,
            -5000,
            "cf0f0000c001",
        );
        check_marshal(
            &[-5000, -6000, -7000, -8000, -8900],
            6,
            -5000,
            "cf0f0000c001",
        );
        check_marshal(
            &[-5000, -6000, -7000, -8000, -8900],
            7,
            -5000,
            "cf0f0000c001",
        );
        check_marshal(
            &[-5000, -6000, -7000, -8000, -8900],
            8,
            -5000,
            "cf0f0000c801",
        );
    }

    fn check_marshal(va: &[i64], precision_bits: u8, first_value_expected: i64, b_expected: &str) {
        let mut b = Vec::new();
        let first_value = marshal_int64_nearest_delta2(&mut b, va, precision_bits);
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
        let first_value = marshal_int64_nearest_delta2(&mut b, va, precision_bits);
        assert_eq!(first_value, first_value_expected);
        assert_eq!(&b[..prefix.len()], prefix, "invalid prefix for va={va:?}");
        assert_eq!(
            to_hex(&b[prefix.len()..]),
            b_expected,
            "invalid marshaled prefixed data for va={va:?}, precisionBits={precision_bits}"
        );
    }

    // Port of TestMarshalUnmarshalInt64NearestDelta2.
    #[test]
    fn test_marshal_unmarshal_int64_nearest_delta2() {
        let mut r = Rng::new(1);

        check_roundtrip(&[0, 0], 4);
        check_roundtrip(&[1, -3], 4);
        check_roundtrip(&[255, 255], 4);
        check_roundtrip(&[0, 1, 2, 3, 4, 5], 4);
        check_roundtrip(&[5, 4, 3, 2, 1, 0], 4);
        check_roundtrip(
            &[
                -5e12 as i64,
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
                -6e12 as i64,
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
        check_roundtrip(&va, 63);

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
        check_roundtrip(&va, 2);

        // Verify encoding for constant va with random noise.
        let mut v: i64 = -12345;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v += r.i64n(15) - 1;
            va.push(v);
        }
        check_roundtrip(&va, 3);
    }

    // Full precision-bits sweep for a counter-like sequence.
    #[test]
    fn test_marshal_unmarshal_int64_nearest_delta2_precision_bits_sweep() {
        let mut r = Rng::new(9);
        let mut v: i64 = 0;
        let mut va = Vec::new();
        for _ in 0..1024 {
            v += 30_000_000 + (r.norm_f64() * 1e6) as i64;
            va.push(v);
        }
        for precision_bits in 1..=64u8 {
            check_roundtrip(&va, precision_bits);
        }
    }

    fn check_roundtrip(va: &[i64], precision_bits: u8) {
        let mut b = Vec::new();
        let first_value = marshal_int64_nearest_delta2(&mut b, va, precision_bits);
        let mut va_new = Vec::new();
        unmarshal_int64_nearest_delta2(&mut va_new, &b, first_value, va.len()).unwrap_or_else(
            |err| {
                panic!("cannot unmarshal data for va={va:?}, precisionBits={precision_bits}: {err}")
            },
        );
        check_precision_bits_arrays(&va_new, va, precision_bits).unwrap_or_else(|err| {
            panic!("too small precisionBits for va={va:?}, precisionBits={precision_bits}: {err}")
        });

        let va_prefix = [1i64, 2, 3, 4];
        let mut va_new = va_prefix.to_vec();
        unmarshal_int64_nearest_delta2(&mut va_new, &b, first_value, va.len()).unwrap_or_else(
            |err| {
                panic!("cannot unmarshal prefixed data for precisionBits={precision_bits}: {err}")
            },
        );
        assert_eq!(&va_new[..va_prefix.len()], &va_prefix, "unexpected prefix");
        check_precision_bits_arrays(&va_new[va_prefix.len()..], va, precision_bits)
            .unwrap_or_else(|err| panic!("too small precisionBits for prefixed va: {err}"));
    }
}
