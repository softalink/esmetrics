//! Strict floating-point parsing shared by [`crate::graphite`] and
//! [`crate::opentsdb`].
//!
//! Port of `github.com/valyala/fastjson/fastfloat`'s `Parse` (the strict
//! variant that errors on invalid/trailing input, as opposed to
//! `ParseBestEffort`, which silently returns 0). Both `graphite/parser.go`
//! and `opentsdb/parser.go` call this same shared Go helper for value and
//! timestamp parsing.
//!
//! This is a deliberate exception to this crate's usual per-module
//! duplication of upstream number-parsing helpers (see the separate
//! `parse_prom_float` in [`crate::prometheus`] and `parse_best_effort` in
//! [`crate::influx`]): graphite and opentsdb both need byte-for-byte the
//! same strict-parse semantics, so factoring it out here avoids a third
//! ~150-line copy of the same logic.

const F64_POW10: [f64; 17] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
];

/// Parses a floating-point number from `s`. Port of `fastfloat.Parse`.
pub(crate) fn parse(s: &str) -> Result<f64, String> {
    let b = s.as_bytes();
    if b.is_empty() {
        return Err("cannot parse float64 from empty string".to_string());
    }
    let mut i = 0usize;
    let minus = b[0] == b'-';
    if minus {
        i += 1;
        if i >= b.len() {
            return Err(format!("cannot parse float64 from {s:?}"));
        }
    }

    // The integer part might be elided, e.g. `.5`.
    if b[i] == b'.' && (i + 1 >= b.len() || !b[i + 1].is_ascii_digit()) {
        return Err(format!("missing integer and fractional part in {s:?}"));
    }

    let mut d: u64 = 0;
    let j = i;
    while i < b.len() && b[i].is_ascii_digit() {
        d = d * 10 + u64::from(b[i] - b'0');
        i += 1;
        if i > 18 {
            // The integer part may be out of range for u64. Fall back to
            // standard parsing.
            return s
                .parse::<f64>()
                .map_err(|err| format!("cannot parse float64 from {s:?}: {err}"));
        }
    }
    if i <= j && b[i] != b'.' {
        let mut tail = &s[i..];
        if let Some(stripped) = tail.strip_prefix('+') {
            tail = stripped;
        }
        // "infinity" is needed for OpenMetrics support.
        if tail.eq_ignore_ascii_case("inf") || tail.eq_ignore_ascii_case("infinity") {
            return Ok(if minus {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            });
        }
        if tail.eq_ignore_ascii_case("nan") {
            return Ok(f64::NAN);
        }
        return Err(format!(
            "unparsed tail left after parsing float64 from {s:?}: {tail:?}"
        ));
    }
    let mut f = d as f64;
    if i >= b.len() {
        // Fast path - just an integer.
        return Ok(if minus { -f } else { f });
    }

    if b[i] == b'.' {
        i += 1;
        if i >= b.len() {
            // The fractional part may be elided. NOTE: no minus sign applied
            // here - matches the pre-existing upstream quirk (see the
            // divergence note on `crate::prometheus::parse_prom_float`).
            return Ok(f);
        }
        let k = i;
        while i < b.len() && b[i].is_ascii_digit() {
            d = d * 10 + u64::from(b[i] - b'0');
            i += 1;
            if i - j >= F64_POW10.len() {
                return s
                    .parse::<f64>()
                    .map_err(|err| format!("cannot parse mantissa in {s:?}: {err}"));
            }
        }
        if i < k {
            return Err(format!("cannot find mantissa in {s:?}"));
        }
        // Convert the entire mantissa to a float at once to avoid rounding
        // errors.
        f = d as f64 / F64_POW10[i - k];
        if i >= b.len() {
            return Ok(if minus { -f } else { f });
        }
    }
    if b[i] == b'e' || b[i] == b'E' {
        i += 1;
        if i >= b.len() {
            return Err(format!("cannot parse exponent in {s:?}"));
        }
        let mut exp_minus = false;
        if b[i] == b'+' || b[i] == b'-' {
            exp_minus = b[i] == b'-';
            i += 1;
            if i >= b.len() {
                return Err(format!("cannot parse exponent in {s:?}"));
            }
        }
        let mut exp: i32 = 0;
        let jj = i;
        while i < b.len() && b[i].is_ascii_digit() {
            exp = exp * 10 + i32::from(b[i] - b'0');
            i += 1;
            if exp > 300 {
                return s
                    .parse::<f64>()
                    .map_err(|err| format!("cannot parse exponent in {s:?}: {err}"));
            }
        }
        if i <= jj {
            return Err(format!("cannot parse exponent in {s:?}"));
        }
        if exp_minus {
            exp = -exp;
        }
        f *= 10f64.powi(exp);
        if i >= b.len() {
            return Ok(if minus { -f } else { f });
        }
    }
    Err(format!("cannot parse float64 from {s:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_integers_decimals_and_specials() {
        assert_eq!(parse("0").unwrap(), 0.0);
        assert_eq!(parse("123").unwrap(), 123.0);
        assert_eq!(parse("-123.456").unwrap(), -123.456);
        assert_eq!(parse("1e5").unwrap(), 100000.0);
        assert_eq!(parse("-1.5E+3").unwrap(), -1500.0);
        assert!(parse("NaN").unwrap().is_nan());
        assert_eq!(parse("+Inf").unwrap(), f64::INFINITY);
        assert_eq!(parse("-infinity").unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn rejects_empty_and_malformed_input() {
        assert!(parse("").is_err());
        assert!(parse("+5").is_err());
        assert!(parse("5a").is_err());
        assert!(parse(".").is_err());
        assert!(parse("bar").is_err());
    }
}
