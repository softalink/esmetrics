//! Port of the upstream VictoriaMetrics `lib/fastnum` (v1.146.0).
//!
//! Fast checks and fills for all-zeros / all-ones `i64` and `f64` slices.
//!
//! Deviation from Go: the Go implementation compares/copies data in
//! 8*1024-item chunks via `unsafe` byte reinterpretation as a `memcmp`
//! speedup. Rust iterator comparisons compile to equivalent vectorized
//! code, so no `unsafe` is used. Float comparisons are done bitwise
//! (`f64::to_bits`), exactly matching Go's byte-level comparison semantics.

/// Port of Go `fastnum.AppendInt64Zeros`: appends `items` zeros to `dst`.
pub fn append_int64_zeros(dst: &mut Vec<i64>, items: usize) {
    dst.extend(std::iter::repeat_n(0i64, items));
}

/// Port of Go `fastnum.AppendInt64Ones`: appends `items` ones to `dst`.
pub fn append_int64_ones(dst: &mut Vec<i64>, items: usize) {
    dst.extend(std::iter::repeat_n(1i64, items));
}

/// Port of Go `fastnum.AppendFloat64Zeros`: appends `items` zeros to `dst`.
pub fn append_float64_zeros(dst: &mut Vec<f64>, items: usize) {
    dst.extend(std::iter::repeat_n(0f64, items));
}

/// Port of Go `fastnum.AppendFloat64Ones`: appends `items` ones to `dst`.
pub fn append_float64_ones(dst: &mut Vec<f64>, items: usize) {
    dst.extend(std::iter::repeat_n(1f64, items));
}

/// Port of Go `fastnum.IsInt64Zeros`: checks whether `a` contains only zeros.
pub fn is_int64_zeros(a: &[i64]) -> bool {
    a.iter().all(|&x| x == 0)
}

/// Port of Go `fastnum.IsInt64Ones`: checks whether `a` contains only ones.
pub fn is_int64_ones(a: &[i64]) -> bool {
    a.iter().all(|&x| x == 1)
}

/// Port of Go `fastnum.IsFloat64Zeros`: checks whether `a` contains only zeros.
///
/// The comparison is bitwise (like Go's byte-level comparison), so `-0.0`
/// does not count as zero and `NaN` never matches.
pub fn is_float64_zeros(a: &[f64]) -> bool {
    let zero_bits = 0f64.to_bits();
    a.iter().all(|&x| x.to_bits() == zero_bits)
}

/// Port of Go `fastnum.IsFloat64Ones`: checks whether `a` contains only ones.
///
/// The comparison is bitwise, matching Go's byte-level comparison semantics.
pub fn is_float64_ones(a: &[f64]) -> bool {
    let one_bits = 1f64.to_bits();
    a.iter().all(|&x| x.to_bits() == one_bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIZES: &[usize] = &[0, 1, 10, 100, 1000, 10_000, 100_000, 8 * 1024 + 1];

    #[test]
    fn test_is_int64_zeros() {
        for &n in SIZES {
            let mut a = vec![0i64; n];
            assert!(is_int64_zeros(&a), "is_int64_zeros must return true");
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 1;
                assert!(!is_int64_zeros(&a), "is_int64_zeros must return false");
            }
        }
    }

    #[test]
    fn test_is_int64_ones() {
        for &n in SIZES {
            let mut a = vec![1i64; n];
            assert!(is_int64_ones(&a), "is_int64_ones must return true");
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 0;
                assert!(!is_int64_ones(&a), "is_int64_ones must return false");
            }
        }
    }

    #[test]
    fn test_is_float64_zeros() {
        for &n in SIZES {
            let mut a = vec![0f64; n];
            assert!(is_float64_zeros(&a), "is_float64_zeros must return true");
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 1.0;
                assert!(!is_float64_zeros(&a), "is_float64_zeros must return false");
            }
        }
    }

    #[test]
    fn test_is_float64_ones() {
        for &n in SIZES {
            let mut a = vec![1f64; n];
            assert!(is_float64_ones(&a), "is_float64_ones must return true");
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 0.0;
                assert!(!is_float64_ones(&a), "is_float64_ones must return false");
            }
        }
    }

    #[test]
    fn test_append_int64_zeros() {
        for &n in SIZES {
            let mut a = Vec::new();
            append_int64_zeros(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_int64_zeros(&a), "is_int64_zeros must return true");

            let prefix = vec![1i64, 2, 3];
            let mut a = prefix.clone();
            append_int64_zeros(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            for (i, &p) in prefix.iter().enumerate() {
                assert_eq!(a[i], p, "unexpected prefix[{i}]; got {}; want {p}", a[i]);
            }
            assert!(
                is_int64_zeros(&a[prefix.len()..]),
                "is_int64_zeros for prefixed a must return true"
            );
        }
    }

    #[test]
    fn test_append_int64_ones() {
        for &n in SIZES {
            let mut a = Vec::new();
            append_int64_ones(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_int64_ones(&a), "is_int64_ones must return true");

            let prefix = vec![1i64, 2, 3];
            let mut a = prefix.clone();
            append_int64_ones(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            for (i, &p) in prefix.iter().enumerate() {
                assert_eq!(a[i], p, "unexpected prefix[{i}]; got {}; want {p}", a[i]);
            }
            assert!(
                is_int64_ones(&a[prefix.len()..]),
                "is_int64_ones for prefixed a must return true"
            );
        }
    }

    #[test]
    fn test_append_float64_zeros() {
        for &n in SIZES {
            let mut a = Vec::new();
            append_float64_zeros(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_float64_zeros(&a), "is_float64_zeros must return true");

            let prefix = vec![1f64, 2.0, 3.0];
            let mut a = prefix.clone();
            append_float64_zeros(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            for (i, &p) in prefix.iter().enumerate() {
                assert_eq!(a[i], p, "unexpected prefix[{i}]; got {}; want {p}", a[i]);
            }
            assert!(
                is_float64_zeros(&a[prefix.len()..]),
                "is_float64_zeros for prefixed a must return true"
            );
        }
    }

    #[test]
    fn test_append_float64_ones() {
        for &n in SIZES {
            let mut a = Vec::new();
            append_float64_ones(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_float64_ones(&a), "is_float64_ones must return true");

            let prefix = vec![1f64, 2.0, 3.0];
            let mut a = prefix.clone();
            append_float64_ones(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            for (i, &p) in prefix.iter().enumerate() {
                assert_eq!(a[i], p, "unexpected prefix[{i}]; got {}; want {p}", a[i]);
            }
            assert!(
                is_float64_ones(&a[prefix.len()..]),
                "is_float64_ones for prefixed a must return true"
            );
        }
    }
}
