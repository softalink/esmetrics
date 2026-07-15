//! Minimal UTC time formatting (no chrono dependency).

/// Formats a unix timestamp as RFC3339, e.g. "2026-07-03T12:34:56Z".
pub fn rfc3339_from_unix(unix_secs: u64) -> String {
    let (y, mo, d, h, mi, s) = civil_from_unix(unix_secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

pub fn now_rfc3339() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    rfc3339_from_unix(secs)
}

/// Converts a snapshot name (`YYYYMMDDhhmmss-XXXX`, UTC hex suffix) to
/// RFC3339, validating it exactly like Go
/// `lib/snapshot/snapshotutil.Time`: the name must match the
/// `^[0-9]{14}-[0-9A-Fa-f]+$` regexp and the leading 14 digits must parse as
/// a real calendar timestamp (`YYYYMMDDhhmmss`, UTC). Returns an error for
/// any name upstream would reject, so callers hard-error instead of
/// recording a bogus fallback timestamp. Also implements
/// `snapshotutil.Validate` (which is just `Time` with the result discarded).
pub fn rfc3339_from_snapshot_name(name: &str) -> anyhow::Result<String> {
    // Go: snapshotNameRegexp = `^[0-9]{14}-[0-9A-Fa-f]+$`.
    let bytes = name.as_bytes();
    let matches_regexp = bytes.len() >= 16
        && bytes[14] == b'-'
        && bytes[..14].iter().all(u8::is_ascii_digit)
        && bytes[15..].iter().all(u8::is_ascii_hexdigit);
    anyhow::ensure!(
        matches_regexp,
        "unexpected snapshot name {name:?}; it must match `^[0-9]{{14}}-[0-9A-Fa-f]+$` regexp"
    );

    // Go: time.Parse("20060102150405", name[:14]) — range-checks every field
    // and validates the day against the (leap-year-aware) month length.
    let ts = &name[..14];
    let field = |a: usize, b: usize| ts[a..b].parse::<u32>().expect("14 ascii digits");
    let (year, month, day) = (field(0, 4), field(4, 6), field(6, 8));
    let (hour, min, sec) = (field(8, 10), field(10, 12), field(12, 14));
    anyhow::ensure!(
        (1..=12).contains(&month),
        "unexpected timestamp {ts:?} in snapshot name: month out of range"
    );
    anyhow::ensure!(
        day >= 1 && day <= days_in_month(year, month),
        "unexpected timestamp {ts:?} in snapshot name: day out of range"
    );
    anyhow::ensure!(
        hour <= 23,
        "unexpected timestamp {ts:?} in snapshot name: hour out of range"
    );
    anyhow::ensure!(
        min <= 59,
        "unexpected timestamp {ts:?} in snapshot name: minute out of range"
    );
    anyhow::ensure!(
        sec <= 59,
        "unexpected timestamp {ts:?} in snapshot name: second out of range"
    );
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z"
    ))
}

/// Days in `month` (1-12) for the (proleptic Gregorian) `year`, matching the
/// leap-year rule Go's `time` package uses.
fn days_in_month(year: u32, month: u32) -> u32 {
    const DAYS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    if month == 2 && is_leap {
        29
    } else {
        DAYS[(month - 1) as usize]
    }
}

fn civil_from_unix(unix_secs: u64) -> (i64, u64, u64, u64, u64, u64) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, d, h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_known_values() {
        assert_eq!(rfc3339_from_unix(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_from_unix(1_783_082_096), "2026-07-03T12:34:56Z");
    }

    #[test]
    fn snapshot_name_to_rfc3339() {
        assert_eq!(
            rfc3339_from_snapshot_name("20260705123456-0000000A").unwrap(),
            "2026-07-05T12:34:56Z"
        );
        // A create-URL-style name (`%s-%08X`) also validates.
        assert_eq!(
            rfc3339_from_snapshot_name("20240229000000-16E3C4B7").unwrap(),
            "2024-02-29T00:00:00Z"
        );
    }

    #[test]
    fn snapshot_name_rejects_invalid_names() {
        // Regexp failures (upstream snapshotutil.Validate rejects these).
        assert!(rfc3339_from_snapshot_name("garbage").is_err());
        assert!(rfc3339_from_snapshot_name("20260705123456").is_err()); // no suffix
        assert!(rfc3339_from_snapshot_name("20260705123456-").is_err()); // empty suffix
        assert!(rfc3339_from_snapshot_name("20260705123456-XYZ").is_err()); // non-hex suffix
        assert!(rfc3339_from_snapshot_name("2026070512345-0A").is_err()); // 13 digits
                                                                          // Real-calendar failures — parsed differently by Go time.Parse.
        assert!(rfc3339_from_snapshot_name("20261305123456-0A").is_err()); // month 13
        assert!(rfc3339_from_snapshot_name("20260732123456-0A").is_err()); // day 32
        assert!(rfc3339_from_snapshot_name("20260230123456-0A").is_err()); // Feb 30
        assert!(rfc3339_from_snapshot_name("20250229123456-0A").is_err()); // 2025 not leap
        assert!(rfc3339_from_snapshot_name("20260705253456-0A").is_err()); // hour 25
        assert!(rfc3339_from_snapshot_name("20260705126056-0A").is_err()); // minute 60
        assert!(rfc3339_from_snapshot_name("20260705123460-0A").is_err()); // second 60
    }
}
