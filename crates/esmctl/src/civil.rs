//! Minimal proleptic-Gregorian civil-date math in UTC, sufficient for the
//! `vm-native` time filters and the month/week/day/hour/minute range stepper.
//!
//! All times are represented as `i64` unix **nanoseconds** (UTC), matching
//! the precision Go's `time.Time` uses internally; RFC3339 formatting
//! truncates to whole seconds exactly as Go's `time.Format(time.RFC3339)`
//! does.
//!
//! The `days_from_civil` / `civil_from_days` pair is Howard Hinnant's
//! well-known algorithm (<http://howardhinnant.github.io/date_algorithms.html>).

pub(crate) const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Days since the Unix epoch (1970-01-01) for the given civil date.
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Civil `(year, month, day)` for the given days-since-epoch.
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Decomposes unix nanoseconds into UTC `(year, month, day, hour, min, sec)`.
pub(crate) fn to_components(ns: i64) -> (i64, u32, u32, u32, u32, u32) {
    let secs = ns.div_euclid(NANOS_PER_SEC);
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let (y, mo, d) = civil_from_days(days);
    let h = (sod / 3600) as u32;
    let mi = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    (y, mo, d, h, mi, s)
}

/// Composes UTC `(year, month, day, hour, min, sec)` into unix nanoseconds.
/// `month`/`day` overflow is normalized (`time.Date` semantics), so a
/// `month` of 13 rolls into the next year.
pub(crate) fn from_components(
    mut year: i64,
    mut month: i64,
    day: i64,
    h: i64,
    mi: i64,
    s: i64,
) -> i64 {
    // Normalize month into [1, 12].
    while month > 12 {
        month -= 12;
        year += 1;
    }
    while month < 1 {
        month += 12;
        year -= 1;
    }
    let days = days_from_civil(year, month as u32, 1) + (day - 1);
    // Compute in i128 and saturate to i64: dates outside the ~1678–2262
    // nanosecond-representable window clamp rather than panic (they never
    // arise from real migration inputs).
    let secs = days as i128 * 86400 + h as i128 * 3600 + mi as i128 * 60 + s as i128;
    let ns = secs * NANOS_PER_SEC as i128;
    ns.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Unix nanoseconds for the first instant of `(year, month)` (day 1, 00:00).
pub(crate) fn first_of_month(year: i64, month: i64) -> i64 {
    from_components(year, month, 1, 0, 0, 0)
}

/// Formats unix nanoseconds as an RFC3339 UTC string, truncated to whole
/// seconds (`YYYY-MM-DDTHH:MM:SSZ`) — matches Go `time.Format(time.RFC3339)`.
pub(crate) fn format_rfc3339(ns: i64) -> String {
    let (y, mo, d, h, mi, s) = to_components(ns);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_round_trips() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn known_date() {
        // 2024-02-29 (leap day) is 19782 days after epoch.
        let days = days_from_civil(2024, 2, 29);
        assert_eq!(civil_from_days(days), (2024, 2, 29));
    }

    #[test]
    fn format_matches_go_rfc3339() {
        // 2024-01-31T23:59:59Z
        let ns = from_components(2024, 2, 1, 0, 0, 0) - 1;
        assert_eq!(format_rfc3339(ns), "2024-01-31T23:59:59Z");
    }

    #[test]
    fn month_overflow_normalizes() {
        // Month 13 of 2023 == month 1 of 2024.
        assert_eq!(first_of_month(2023, 13), first_of_month(2024, 1));
    }
}
