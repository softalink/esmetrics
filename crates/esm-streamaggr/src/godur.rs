//! A faithful port of Go's `time.ParseDuration`.
//!
//! The stream-aggregation config uses Go's duration grammar (unit-suffixed
//! decimal sequences like `1h30m`, `500ms`, `-5m`), which differs from
//! MetricsQL's duration grammar (which additionally accepts `d`/`w`/`y` and
//! bare seconds). Using the real Go semantics keeps config validation
//! byte-identical to upstream — e.g. `1d` and a bare `5` are rejected here,
//! as they are upstream.

use crate::Error;

/// Parses a Go duration string, returning nanoseconds (may be negative).
/// Ports `time.ParseDuration`.
pub(crate) fn parse_duration_nanos(s: &str) -> Result<i64, Error> {
    let orig = s;
    let mut s = s;
    let mut d: i64 = 0;
    let mut neg = false;

    // Consume optional sign.
    if let Some(rest) = s.strip_prefix('-') {
        neg = true;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }

    // Special case: "0".
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(Error::new(format!("time: invalid duration {orig:?}")));
    }

    while !s.is_empty() {
        // The next character must be [0-9.].
        if !(s.starts_with('.') || s.starts_with(|c: char| c.is_ascii_digit())) {
            return Err(Error::new(format!("time: invalid duration {orig:?}")));
        }
        // Consume [0-9]* integer part.
        let (int_val, int_digits, rest) = leading_int(s)?;
        s = rest;
        let mut f: f64 = int_val as f64;
        let pre = int_digits > 0;

        // Consume (.[0-9]*)? fractional part.
        let mut post = false;
        if s.starts_with('.') {
            s = &s[1..];
            let (frac, scale, rest) = leading_fraction(s);
            f += frac / scale;
            s = rest;
            // A fractional part was present iff at least one digit was
            // consumed (scale advances past 1.0 per digit).
            post = scale != 1.0;
        }
        if !pre && !post {
            // No digits (e.g. ".s" or "-.s").
            return Err(Error::new(format!("time: invalid duration {orig:?}")));
        }

        // Consume unit.
        let unit_end = s
            .find(|c: char| c == '.' || c.is_ascii_digit())
            .unwrap_or(s.len());
        let unit = &s[..unit_end];
        if unit.is_empty() {
            return Err(Error::new(format!(
                "time: missing unit in duration {orig:?}"
            )));
        }
        s = &s[unit_end..];
        let unit_nanos = unit_to_nanos(unit).ok_or_else(|| {
            Error::new(format!("time: unknown unit {unit:?} in duration {orig:?}"))
        })?;

        let scaled = f * unit_nanos as f64;
        if scaled > i64::MAX as f64 {
            return Err(Error::new(format!("time: invalid duration {orig:?}")));
        }
        d = d
            .checked_add(scaled as i64)
            .ok_or_else(|| Error::new(format!("time: invalid duration {orig:?}")))?;
        if d < 0 {
            return Err(Error::new(format!("time: invalid duration {orig:?}")));
        }
    }

    Ok(if neg { -d } else { d })
}

/// Parses `interval`/`dedup_interval` style config values into whole
/// milliseconds (may be negative). Convenience over [`parse_duration_nanos`].
pub(crate) fn parse_duration_millis(s: &str) -> Result<i64, Error> {
    Ok(parse_duration_nanos(s)? / 1_000_000)
}

fn leading_int(s: &str) -> Result<(i64, usize, &str), Error> {
    let mut x: i64 = 0;
    let mut i = 0;
    for c in s.chars() {
        if !c.is_ascii_digit() {
            break;
        }
        let digit = (c as u8 - b'0') as i64;
        x = x
            .checked_mul(10)
            .and_then(|v| v.checked_add(digit))
            .ok_or_else(|| Error::new("time: bad [0-9]*"))?;
        i += 1;
    }
    Ok((x, i, &s[i..]))
}

/// Returns `(value, scale, rest)` for a leading fractional digit run, where
/// the fraction is `value / scale`. Ports `leadingFraction`.
fn leading_fraction(s: &str) -> (f64, f64, &str) {
    let mut i = 0;
    let mut x: i64 = 0;
    let mut scale: f64 = 1.0;
    let mut overflow = false;
    for c in s.chars() {
        if !c.is_ascii_digit() {
            break;
        }
        if overflow {
            i += 1;
            continue;
        }
        // Guard against int64 overflow, matching upstream.
        if x > (i64::MAX - 9) / 10 {
            overflow = true;
            i += 1;
            continue;
        }
        x = x * 10 + (c as u8 - b'0') as i64;
        scale *= 10.0;
        i += 1;
    }
    (x as f64, scale, &s[i..])
}

fn unit_to_nanos(unit: &str) -> Option<i64> {
    Some(match unit {
        "ns" => 1,
        "us" | "µs" | "μs" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3600 * 1_000_000_000,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_units() {
        assert_eq!(parse_duration_millis("1m").unwrap(), 60_000);
        assert_eq!(parse_duration_millis("30s").unwrap(), 30_000);
        assert_eq!(parse_duration_millis("1h").unwrap(), 3_600_000);
        assert_eq!(parse_duration_millis("10ms").unwrap(), 10);
        assert_eq!(parse_duration_millis("100s").unwrap(), 100_000);
        assert_eq!(parse_duration_millis("35s").unwrap(), 35_000);
    }

    #[test]
    fn parses_combined_and_fraction() {
        assert_eq!(parse_duration_nanos("1h30m").unwrap(), 5_400_000_000_000);
        assert_eq!(parse_duration_nanos("1.5s").unwrap(), 1_500_000_000);
    }

    #[test]
    fn parses_sign() {
        assert_eq!(parse_duration_millis("-5m").unwrap(), -300_000);
        assert_eq!(parse_duration_millis("0").unwrap(), 0);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(parse_duration_nanos("1foo").is_err());
        assert!(parse_duration_nanos("5").is_err()); // bare number, no unit
        assert!(parse_duration_nanos("1d").is_err()); // Go has no day unit
        assert!(parse_duration_nanos("").is_err());
        assert!(parse_duration_nanos("m").is_err());
    }
}
