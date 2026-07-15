//! Time parsing for the `vm-native` filters. Ports `vmctlutil.ParseTime` /
//! `lib/timeutil.ParseTimeMsec` closely enough for RFC3339, fixed-length
//! calendar prefixes, unix timestamps, and `now`-relative durations.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::civil::{days_from_civil, from_components, NANOS_PER_SEC};

fn now_unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Current unix time in milliseconds.
pub(crate) fn now_ms() -> i64 {
    now_unix_ns() / 1_000_000
}

/// Parses `s` into milliseconds since the Unix epoch (UTC). Ports
/// `timeutil.ParseTimeMsec`.
pub(crate) fn parse_time_msec(s: &str) -> Result<i64, String> {
    let ns = parse_time_at(s, now_unix_ns())?;
    Ok(ns / 1_000_000)
}

fn parse_time_at(s_orig: &str, current_ns: i64) -> Result<i64, String> {
    if s_orig.is_empty() {
        return Err("cannot parse an empty time string".to_string());
    }
    if s_orig == "now" {
        return Ok(current_ns);
    }

    let mut s = s_orig;
    let mut tz_offset_ns: i64 = 0;
    if s_orig.len() > 6 {
        let tz = &s_orig.as_bytes()[s_orig.len() - 6..];
        if (tz[0] == b'-' || tz[0] == b'+') && tz[3] == b':' {
            let hour = parse_2digits(&tz[1..3])
                .ok_or_else(|| format!("cannot parse tz hour in {s_orig:?}"))?;
            let minute = parse_2digits(&tz[4..6])
                .ok_or_else(|| format!("cannot parse tz minute in {s_orig:?}"))?;
            tz_offset_ns = i64::from(hour * 3600 + minute * 60) * NANOS_PER_SEC;
            if tz[0] == b'+' {
                tz_offset_ns = -tz_offset_ns;
            }
            s = &s_orig[..s_orig.len() - 6];
        } else if let Some(stripped) = s.strip_suffix('Z') {
            s = stripped;
        } else {
            // No explicit offset and no `Z` suffix: the string carries no
            // timezone information, so interpret it in the host's local
            // timezone. Go: `tzOffset = -GetLocalTimezoneOffsetNsecs()`.
            tz_offset_ns = -local_tz_offset_ns();
        }
    }
    s = s.strip_suffix('Z').unwrap_or(s);

    let b = s.as_bytes();
    if (!b.is_empty() && (b[b.len() - 1] > b'9' || b[0] == b'-')) || s.starts_with("now") {
        // Relative to the current time (`now-1h`, `-30m`, …).
        let rel = s.strip_prefix("now").unwrap_or(s);
        if rel.is_empty() {
            return Ok(current_ns);
        }
        // A relative duration must end in a unit; a bare (possibly negative)
        // number is not a valid time here — matches Go's `ParseDuration`,
        // which rejects unit-less values (unlike MetricsQL's lenient grammar).
        let last = rel.as_bytes()[rel.len() - 1];
        if last.is_ascii_digit() || last == b'.' {
            return Err(format!(
                "cannot parse relative time {s_orig:?}: missing unit"
            ));
        }
        let d_ms = esm_metricsql::duration_value(rel, 0).map_err(|e| e.to_string())?;
        // Go takes the absolute value of the parsed duration (`if d < 0 { d = -d }`)
        // before subtracting, so `now-1h` / `-30d` resolve into the PAST.
        let d_ns = ((d_ms as i128) * 1_000_000).abs();
        return Ok(saturating_from_i128(current_ns as i128 - d_ns));
    }

    if s.len() == 4 {
        return calendar(s, 1, tz_offset_ns);
    }
    if !s_orig.contains('-') {
        return unix_timestamp(s_orig);
    }
    match s.len() {
        7 => calendar(s, 2, tz_offset_ns),
        10 => calendar(s, 3, tz_offset_ns),
        13 => calendar(s, 4, tz_offset_ns),
        16 => calendar(s, 5, tz_offset_ns),
        19 => calendar(s, 6, tz_offset_ns),
        _ => rfc3339_fractional(s, tz_offset_ns),
    }
}

fn parse_2digits(b: &[u8]) -> Option<u32> {
    if b.len() != 2 || !b[0].is_ascii_digit() || !b[1].is_ascii_digit() {
        return None;
    }
    Some(u32::from(b[0] - b'0') * 10 + u32::from(b[1] - b'0'))
}

/// Parses fixed-length `YYYY[-MM[-DD[THH[:MM[:SS]]]]]` (Go layouts `"2006"`
/// .. `"2006-01-02T15:04:05"`), `n_fields` giving how many components are
/// present.
fn calendar(s: &str, n_fields: usize, tz_offset_ns: i64) -> Result<i64, String> {
    const SEPS: [u8; 5] = *b"--T::";
    let b = s.as_bytes();
    let widths = [4usize, 2, 2, 2, 2, 2];
    let mut fields = [0i64; 6];
    let mut pos = 0usize;
    for i in 0..n_fields {
        if i > 0 {
            if pos >= b.len() || b[pos] != SEPS[i - 1] {
                return Err(format!("cannot parse {s:?} as a calendar timestamp"));
            }
            pos += 1;
        }
        let w = widths[i];
        if pos + w > b.len() {
            return Err(format!("cannot parse {s:?} as a calendar timestamp"));
        }
        let mut v: i64 = 0;
        for &c in &b[pos..pos + w] {
            if !c.is_ascii_digit() {
                return Err(format!("cannot parse {s:?} as a calendar timestamp"));
            }
            v = v * 10 + i64::from(c - b'0');
        }
        fields[i] = v;
        pos += w;
    }
    if pos != b.len() {
        return Err(format!("cannot parse {s:?} as a calendar timestamp"));
    }
    let year = fields[0];
    let month = if n_fields > 1 { fields[1] } else { 1 };
    let day = if n_fields > 2 { fields[2] } else { 1 };
    let hour = if n_fields > 3 { fields[3] } else { 0 };
    let minute = if n_fields > 4 { fields[4] } else { 0 };
    let sec = if n_fields > 5 { fields[5] } else { 0 };
    let ns = from_components(year, month, day, hour, minute, sec);
    Ok(ns + tz_offset_ns)
}

/// Parses an RFC3339 value that carries fractional seconds (after the tz has
/// been stripped by the caller): `YYYY-MM-DDTHH:MM:SS.fff…`.
fn rfc3339_fractional(s: &str, tz_offset_ns: i64) -> Result<i64, String> {
    let (base, frac) = match s.split_once('.') {
        Some((b, f)) => (b, f),
        None => (s, ""),
    };
    if base.len() != 19 {
        return Err(format!("cannot parse {s:?} as RFC3339"));
    }
    let mut ns = calendar(base, 6, tz_offset_ns)?;
    // Fractional seconds → nanoseconds (truncate/pad to 9 digits).
    if !frac.is_empty() {
        if !frac.bytes().all(|c| c.is_ascii_digit()) {
            return Err(format!("cannot parse fractional seconds in {s:?}"));
        }
        let mut digits = frac.to_string();
        digits.truncate(9);
        while digits.len() < 9 {
            digits.push('0');
        }
        let frac_ns: i64 = digits.parse().map_err(|_| "bad fraction".to_string())?;
        ns += frac_ns;
    }
    Ok(ns)
}

fn unix_timestamp(s: &str) -> Result<i64, String> {
    // Integer epoch: auto-detect the unit (seconds/ms/µs/ns) by magnitude, as
    // Go's `getUnixTimestampNanoseconds` does. This lets 13/16/19-digit epoch
    // values through instead of overflowing the seconds×1e9 path below.
    if !s.contains(['.', 'e', 'E']) {
        let n: i64 = s
            .parse()
            .map_err(|_| format!("cannot parse numeric timestamp {s:?}"))?;
        return Ok(unix_ts_nanoseconds(n));
    }
    // Fractional or scientific values are treated as seconds.
    let secs: f64 = s
        .parse()
        .map_err(|_| format!("cannot parse numeric timestamp {s:?}"))?;
    let ns = secs * NANOS_PER_SEC as f64;
    if !ns.is_finite() || ns.abs() >= i64::MAX as f64 {
        return Err(format!("timestamp {s:?} is out of range"));
    }
    Ok(ns as i64)
}

/// Interprets an integer epoch value as seconds, milliseconds, microseconds or
/// nanoseconds based on its magnitude and returns nanoseconds. Ports Go's
/// `getUnixTimestampNanoseconds` (`lib/timeutil.getUnixTimestampNanoseconds`).
fn unix_ts_nanoseconds(n: i64) -> i64 {
    const MAX_VALID_SECOND: i64 = i64::MAX / 1_000_000_000;
    const MAX_VALID_MILLI: i64 = i64::MAX / 1_000_000;
    const MAX_VALID_MICRO: i64 = i64::MAX / 1_000;
    const MIN_VALID_SECOND: i64 = i64::MIN / 1_000_000_000;
    const MIN_VALID_MILLI: i64 = i64::MIN / 1_000_000;
    const MIN_VALID_MICRO: i64 = i64::MIN / 1_000;

    if (MIN_VALID_SECOND..=MAX_VALID_SECOND).contains(&n) {
        n * 1_000_000_000
    } else if (MIN_VALID_MILLI..=MAX_VALID_MILLI).contains(&n) {
        n * 1_000_000
    } else if (MIN_VALID_MICRO..=MAX_VALID_MICRO).contains(&n) {
        n * 1_000
    } else {
        n
    }
}

/// Returns the host's local timezone offset east of UTC in nanoseconds — the
/// value Go's `timeutil.GetLocalTimezoneOffsetNsecs` reports from
/// `time.Now().Zone()`.
///
/// It is computed by breaking the current instant into calendar components in
/// both local and UTC time (via the C library's `localtime`/`gmtime`) and
/// diffing them. This deliberately avoids the `tm_gmtoff` field, which the
/// portable `libc::tm` exposes on Unix but not on Windows.
fn local_tz_offset_ns() -> i64 {
    // SAFETY: `time`, `gmtime` and `localtime` are standard C library calls
    // available on every supported platform. `gmtime`/`localtime` return
    // pointers into shared static storage, so each result is copied out (`*ptr`
    // into an owned `libc::tm`) before the next call can overwrite it.
    unsafe {
        let t: libc::time_t = libc::time(std::ptr::null_mut());
        let gm = libc::gmtime(&t);
        if gm.is_null() {
            return 0;
        }
        let utc = *gm;
        let lm = libc::localtime(&t);
        if lm.is_null() {
            return 0;
        }
        let local = *lm;
        (tm_unix_secs(&local) - tm_unix_secs(&utc)) * NANOS_PER_SEC
    }
}

/// Treats a broken-down `tm` as a UTC calendar time and returns the
/// corresponding unix-second count. Used only to diff two `tm` values, so the
/// (identical) epoch offset cancels out.
fn tm_unix_secs(tm: &libc::tm) -> i64 {
    let days = days_from_civil(
        i64::from(tm.tm_year) + 1900,
        (tm.tm_mon as u32) + 1,
        tm.tm_mday as u32,
    );
    days * 86400 + i64::from(tm.tm_hour) * 3600 + i64::from(tm.tm_min) * 60 + i64::from(tm.tm_sec)
}

fn saturating_from_i128(v: i128) -> i64 {
    if v > i64::MAX as i128 {
        i64::MAX
    } else if v < i64::MIN as i128 {
        i64::MIN
    } else {
        v as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_error() {
        assert!(parse_time_msec("").is_err());
    }

    #[test]
    fn relative_without_unit_is_error() {
        // Ports the TestGetTime_Failure cases.
        assert!(parse_time_msec("-9223372036.855").is_err());
        assert!(parse_time_msec("-292273086-05-16T16:47:06Z").is_err());
    }

    #[test]
    fn parses_rfc3339() {
        let ms = parse_time_msec("2024-01-02T03:04:05Z").unwrap();
        let expect = from_components(2024, 1, 2, 3, 4, 5) / 1_000_000;
        assert_eq!(ms, expect);
    }

    #[test]
    fn parses_date_only() {
        // A tz-less date is interpreted in the host's local timezone, so its
        // UTC instant is shifted by the local offset (Go: tz-less strings use
        // the local timezone). On a UTC host the offset is zero.
        let ms = parse_time_msec("2024-01-02").unwrap();
        let expected = (from_components(2024, 1, 2, 0, 0, 0) - local_tz_offset_ns()) / 1_000_000;
        assert_eq!(ms, expected);
    }

    #[test]
    fn tzless_datetime_uses_local_offset() {
        // A datetime longer than 6 chars with neither an explicit `+-hh:mm`
        // offset nor a `Z` suffix must be interpreted in the host's local
        // timezone (Go: `tzOffset = -GetLocalTimezoneOffsetNsecs()`), not as
        // UTC. Asserting against the computed local offset keeps this
        // deterministic across host timezones (including UTC, where it is 0).
        let ms = parse_time_msec("2024-01-02T03:04:05").unwrap();
        let expected = (from_components(2024, 1, 2, 3, 4, 5) - local_tz_offset_ns()) / 1_000_000;
        assert_eq!(ms, expected);

        // The equivalent `Z`-suffixed string is anchored to UTC and must NOT
        // pick up the local offset — it differs from the tz-less form by
        // exactly the local offset.
        let ms_utc = parse_time_msec("2024-01-02T03:04:05Z").unwrap();
        assert_eq!(ms_utc, from_components(2024, 1, 2, 3, 4, 5) / 1_000_000);
        assert_eq!(ms_utc - ms, local_tz_offset_ns() / 1_000_000);
    }

    #[test]
    fn parses_unix_seconds() {
        // A non-4-digit number without `-` is a unix timestamp in seconds
        // (a bare 4-digit value like "1000" is a calendar year, matching VM).
        assert_eq!(parse_time_msec("100000").unwrap(), 100_000_000);
    }

    #[test]
    fn parses_rfc3339_with_offset() {
        // 2024-01-02T03:04:05+01:00 == 02:04:05Z
        let ms = parse_time_msec("2024-01-02T03:04:05+01:00").unwrap();
        assert_eq!(ms, from_components(2024, 1, 2, 2, 4, 5) / 1_000_000);
    }

    #[test]
    fn relative_now_minus_resolves_into_past() {
        // Ports Go's `if d < 0 { d = -d }` step: `now-1h` and a bare `-30d`
        // must resolve into the PAST, not the future.
        let current = 1_000_000_000_000_000_000i64; // fixed ns for determinism
        let one_hour_ns = 3_600_000_000_000i64;
        assert_eq!(
            parse_time_at("now-1h", current).unwrap(),
            current - one_hour_ns
        );
        let thirty_days_ns = 30 * 24 * one_hour_ns;
        assert_eq!(
            parse_time_at("-30d", current).unwrap(),
            current - thirty_days_ns
        );
    }

    #[test]
    fn parses_unix_millis_epoch() {
        // Upstream TestTryParseUnixTimestamp: 1223372036855 is auto-detected as
        // milliseconds (2008-10-07), not rejected as out of range.
        assert_eq!(parse_time_msec("1223372036855").unwrap(), 1_223_372_036_855);
    }

    #[test]
    fn parses_unix_nanos_epoch() {
        // A 19-digit epoch value is auto-detected as nanoseconds.
        assert_eq!(
            parse_time_msec("1700000000000000000").unwrap(),
            1_700_000_000_000
        );
    }
}
