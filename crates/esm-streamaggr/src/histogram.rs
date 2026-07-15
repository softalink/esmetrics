//! Numeric helpers for the `quantiles` and `histogram_bucket` outputs.
//!
//! * [`Fast`] ports `github.com/valyala/histogram.Fast` — a fixed-capacity
//!   (1000-sample) streaming quantile estimator with deterministic reservoir
//!   sampling above capacity.
//! * [`VmHistogram`] ports the bucketing subset of
//!   `github.com/VictoriaMetrics/metrics.Histogram` — the automatically
//!   ranged `vmrange` buckets used by the `histogram_bucket` output.

use std::sync::OnceLock;

const MAX_SAMPLES: usize = 1000;

/// Deterministic xorshift32 PRNG. Ports `valyala/fastrand.RNG`.
struct Rng {
    x: u32,
}

impl Rng {
    fn next(&mut self) -> u32 {
        // The seed is always non-zero (see Fast::reset), so the upstream
        // "reseed while zero" loop can never spin here.
        let mut x = self.x;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.x = x;
        x
    }

    fn uint32n(&mut self, max_n: u32) -> u32 {
        let x = self.next();
        ((x as u64 * max_n as u64) >> 32) as u32
    }
}

/// A fast, fixed-capacity streaming quantile estimator. Ports
/// `valyala/histogram.Fast`.
pub(crate) struct Fast {
    max: f64,
    min: f64,
    count: u64,
    a: Vec<f64>,
    rng: Rng,
}

impl Fast {
    pub(crate) fn new() -> Fast {
        let mut f = Fast {
            max: f64::NEG_INFINITY,
            min: f64::INFINITY,
            count: 0,
            a: Vec::new(),
            rng: Rng { x: 1 },
        };
        f.reset();
        f
    }

    fn reset(&mut self) {
        self.max = f64::NEG_INFINITY;
        self.min = f64::INFINITY;
        self.count = 0;
        self.a.clear();
        // Reset rng state to get repeatable results for the same input
        // sequence (upstream seeds with 1).
        self.rng.x = 1;
    }

    /// Updates the estimator with `v`. Ports `Fast.Update`.
    pub(crate) fn update(&mut self, v: f64) {
        if v > self.max {
            self.max = v;
        }
        if v < self.min {
            self.min = v;
        }
        self.count += 1;
        if self.a.len() < MAX_SAMPLES {
            self.a.push(v);
            return;
        }
        let n = self.rng.uint32n(self.count as u32) as usize;
        if n < self.a.len() {
            self.a[n] = v;
        }
    }

    /// Appends the quantile values for `phis` to `dst`. Ports
    /// `Fast.Quantiles`.
    pub(crate) fn quantiles(&self, dst: &mut Vec<f64>, phis: &[f64]) {
        let mut tmp = self.a.clone();
        tmp.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        for &phi in phis {
            dst.push(self.quantile(&tmp, phi));
        }
    }

    fn quantile(&self, tmp: &[f64], phi: f64) -> f64 {
        if tmp.is_empty() || phi.is_nan() {
            return f64::NAN;
        }
        if phi <= 0.0 {
            return self.min;
        }
        if phi >= 1.0 {
            return self.max;
        }
        let mut idx = (phi * (tmp.len() - 1) as f64 + 0.5) as usize;
        if idx >= tmp.len() {
            idx = tmp.len() - 1;
        }
        tmp[idx]
    }
}

// ---- VictoriaMetrics histogram (vmrange buckets) ----

const E10_MIN: i32 = -9;
const E10_MAX: i32 = 18;
const BUCKETS_PER_DECIMAL: usize = 18;
const DECIMAL_BUCKETS_COUNT: usize = (E10_MAX - E10_MIN) as usize;
const BUCKETS_COUNT: usize = DECIMAL_BUCKETS_COUNT * BUCKETS_PER_DECIMAL;

fn bucket_multiplier() -> f64 {
    10f64.powf(1.0 / BUCKETS_PER_DECIMAL as f64)
}

struct BucketRanges {
    ranges: Vec<String>,
    lower: String,
    upper: String,
}

fn bucket_ranges() -> &'static BucketRanges {
    static RANGES: OnceLock<BucketRanges> = OnceLock::new();
    RANGES.get_or_init(|| {
        let mut ranges = Vec::with_capacity(BUCKETS_COUNT);
        let mut v = 10f64.powi(E10_MIN);
        let mut start = format_e3(v);
        let mult = bucket_multiplier();
        for _ in 0..BUCKETS_COUNT {
            v *= mult;
            let end = format_e3(v);
            ranges.push(format!("{start}...{end}"));
            start = end;
        }
        BucketRanges {
            ranges,
            lower: format!("0...{}", format_e3(10f64.powi(E10_MIN))),
            upper: format!("{}...+Inf", format_e3(10f64.powi(E10_MAX))),
        }
    })
}

/// An auto-bucketing histogram over non-negative values. Ports the bucketing
/// subset of `VictoriaMetrics/metrics.Histogram`.
pub(crate) struct VmHistogram {
    decimal_buckets: Vec<Option<Box<[u64; BUCKETS_PER_DECIMAL]>>>,
    lower: u64,
    upper: u64,
    sum: f64,
}

impl VmHistogram {
    pub(crate) fn new() -> VmHistogram {
        VmHistogram {
            decimal_buckets: (0..DECIMAL_BUCKETS_COUNT).map(|_| None).collect(),
            lower: 0,
            upper: 0,
            sum: 0.0,
        }
    }

    /// Updates the histogram with `v`. Negative values and NaNs are ignored.
    /// Ports `Histogram.Update`.
    pub(crate) fn update(&mut self, v: f64) {
        if v.is_nan() || v < 0.0 {
            return;
        }
        let bucket_idx = (v.log10() - E10_MIN as f64) * BUCKETS_PER_DECIMAL as f64;
        self.sum += v;
        if bucket_idx < 0.0 {
            self.lower += 1;
        } else if bucket_idx >= BUCKETS_COUNT as f64 {
            self.upper += 1;
        } else {
            let mut idx = bucket_idx as usize;
            if bucket_idx == idx as f64 && idx > 0 {
                // Edge case for 10^n values, which go to the lower bucket to
                // match Prometheus `le`-based histogram semantics.
                idx -= 1;
            }
            let decimal_bucket_idx = idx / BUCKETS_PER_DECIMAL;
            let offset = idx % BUCKETS_PER_DECIMAL;
            let db = self.decimal_buckets[decimal_bucket_idx]
                .get_or_insert_with(|| Box::new([0u64; BUCKETS_PER_DECIMAL]));
            db[offset] += 1;
        }
    }

    /// Merges `src` into `self`. Ports `Histogram.Merge`.
    pub(crate) fn merge(&mut self, src: &VmHistogram) {
        self.lower += src.lower;
        self.upper += src.upper;
        self.sum += src.sum;
        for (i, db_src) in src.decimal_buckets.iter().enumerate() {
            let Some(db_src) = db_src else { continue };
            let db_dst = self.decimal_buckets[i]
                .get_or_insert_with(|| Box::new([0u64; BUCKETS_PER_DECIMAL]));
            for j in 0..BUCKETS_PER_DECIMAL {
                db_dst[j] += db_src[j];
            }
        }
    }

    pub(crate) fn reset(&mut self) {
        for db in self.decimal_buckets.iter_mut() {
            *db = None;
        }
        self.lower = 0;
        self.upper = 0;
        self.sum = 0.0;
    }

    /// Calls `f(vmrange, count)` for every non-zero bucket, in ascending
    /// order. Ports `Histogram.VisitNonZeroBuckets`.
    pub(crate) fn visit_non_zero_buckets(&self, mut f: impl FnMut(&str, u64)) {
        let br = bucket_ranges();
        if self.lower > 0 {
            f(&br.lower, self.lower);
        }
        for (decimal_bucket_idx, db) in self.decimal_buckets.iter().enumerate() {
            let Some(db) = db else { continue };
            for (offset, &count) in db.iter().enumerate() {
                if count > 0 {
                    let bucket_idx = decimal_bucket_idx * BUCKETS_PER_DECIMAL + offset;
                    f(&br.ranges[bucket_idx], count);
                }
            }
        }
        if self.upper > 0 {
            f(&br.upper, self.upper);
        }
    }
}

/// Go `fmt.Sprintf("%.3e", v)`: scientific notation, exactly 3 fractional
/// digits, exponent sign-and-zero-padded to at least 2 digits.
fn format_e3(v: f64) -> String {
    let s = format!("{v:.3e}");
    let (mantissa, exp_str) = s.split_once('e').expect("`{:.3e}` always contains 'e'");
    let exp: i32 = exp_str.parse().expect("valid exponent");
    let sign_char = if exp < 0 { '-' } else { '+' };
    let abs_exp = exp.unsigned_abs();
    if abs_exp < 10 {
        format!("{mantissa}e{sign_char}0{abs_exp}")
    } else {
        format!("{mantissa}e{sign_char}{abs_exp}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_exact_for_small_input() {
        let mut f = Fast::new();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            f.update(v);
        }
        let mut dst = Vec::new();
        f.quantiles(&mut dst, &[0.0, 0.5, 1.0]);
        assert_eq!(dst, vec![1.0, 3.0, 5.0]);
    }

    #[test]
    fn quantile_nan_on_empty() {
        let f = Fast::new();
        let mut dst = Vec::new();
        f.quantiles(&mut dst, &[0.5]);
        assert!(dst[0].is_nan());
    }

    #[test]
    fn format_e3_matches_go() {
        assert_eq!(format_e3(1e-9), "1.000e-09");
        assert_eq!(format_e3(1e18), "1.000e+18");
    }

    #[test]
    fn histogram_bucket_ranges() {
        let mut h = VmHistogram::new();
        h.update(1.0);
        h.update(1.0);
        let mut got = Vec::new();
        h.visit_non_zero_buckets(|r, c| got.push((r.to_string(), c)));
        // 1.0 is a 10^0 edge value → lands in the bucket ending at 1.000e+00.
        assert_eq!(got.len(), 1);
        assert!(got[0].0.ends_with("...1.000e+00"), "got {}", got[0].0);
        assert_eq!(got[0].1, 2);
    }

    #[test]
    fn reservoir_is_deterministic_above_capacity() {
        let mut a = Fast::new();
        let mut b = Fast::new();
        for i in 0..5000 {
            a.update(i as f64);
            b.update(i as f64);
        }
        let mut qa = Vec::new();
        let mut qb = Vec::new();
        a.quantiles(&mut qa, &[0.5, 0.9]);
        b.quantiles(&mut qb, &[0.5, 0.9]);
        assert_eq!(qa, qb);
    }
}
