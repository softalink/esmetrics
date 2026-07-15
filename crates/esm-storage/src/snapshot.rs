//! Snapshot name generation and validation.
//! Go: lib/snapshot/snapshotutil/snapshotutil.go

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

/// Generates a new snapshot name: UTC `YYYYMMDDhhmmss` + `-` + 8-hex counter.
/// Go: snapshotutil.NewName.
pub(crate) fn new_name() -> String {
    static NEXT_IDX: AtomicU64 = AtomicU64::new(0);
    // Seed once from wall-clock nanos so names stay unique across restarts.
    static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let seed = *SEED.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    });
    let idx = seed.wrapping_add(NEXT_IDX.fetch_add(1, Ordering::Relaxed));
    format!("{}-{:08X}", utc_compact_timestamp(), idx as u32)
}

/// Returns the current UTC time formatted as `YYYYMMDDhhmmss` without
/// pulling in a date-time dependency (days-from-civil algorithm).
fn utc_compact_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_compact_timestamp(secs)
}

fn format_compact_timestamp(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil_from_days.
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
    format!("{year:04}{month:02}{d:02}{h:02}{m:02}{s:02}")
}

/// Validates a snapshot name. Go: snapshotutil.Validate
/// (regex `^[0-9]{14}-[0-9A-Fa-f]+$`, then `time.Parse("20060102150405", ...)`
/// on the 14-digit prefix so calendar-invalid timestamps are rejected).
pub(crate) fn validate_name(name: &str) -> Result<(), String> {
    let err = || format!("invalid snapshot name {name:?}");
    let (ts, idx) = name.split_once('-').ok_or_else(err)?;
    if ts.len() != 14 || !ts.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err());
    }
    if idx.is_empty() || !idx.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(err());
    }
    if !is_valid_utc_timestamp(ts) {
        return Err(err());
    }
    Ok(())
}

/// Parses a `YYYYMMDDhhmmss` digit string and checks it names a real UTC
/// calendar timestamp (mirrors Go's `time.Parse("20060102150405", ...)`).
fn is_valid_utc_timestamp(ts: &str) -> bool {
    let digit = |s: &str| s.parse::<u32>().unwrap_or(u32::MAX);
    let year = digit(&ts[0..4]);
    let month = digit(&ts[4..6]);
    let day = digit(&ts[6..8]);
    let hour = digit(&ts[8..10]);
    let minute = digit(&ts[10..12]);
    let second = digit(&ts[12..14]);

    if !(1..=12).contains(&month) {
        return false;
    }
    if day < 1 || day > days_in_month(year, month) {
        return false;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return false;
    }
    true
}

/// Number of days in `month` (1-12) for the given `year`, honoring the
/// Gregorian leap-year rule (divisible by 4, except centuries unless
/// divisible by 400).
fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if is_leap {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_names_are_valid_and_unique() {
        let a = new_name();
        let b = new_name();
        assert_ne!(a, b);
        validate_name(&a).unwrap();
        validate_name(&b).unwrap();
    }

    #[test]
    fn format_compact_timestamp_known_values() {
        assert_eq!(format_compact_timestamp(0), "19700101000000");
        // 2026-07-03 12:34:56 UTC
        assert_eq!(format_compact_timestamp(1_783_082_096), "20260703123456");
    }

    #[test]
    fn validate_rejects_bad_names() {
        assert!(validate_name("").is_err());
        assert!(validate_name("20260705123456").is_err()); // no dash/idx
        assert!(validate_name("2026070512345-0A").is_err()); // 13 digits
        assert!(validate_name("20260705123456-").is_err()); // empty idx
        assert!(validate_name("20260705123456-XYZ").is_err()); // non-hex
        assert!(validate_name("20260705123456-0000000A").is_ok());
        assert!(validate_name("20260705123456-deadBEEF").is_ok());
    }

    #[test]
    fn validate_accepts_idx_longer_than_8_hex_chars() {
        // Upstream allows arbitrary-length hex idx.
        assert!(validate_name("20260705123456-16EB56ADB4110CF2").is_ok());
    }

    #[test]
    fn validate_rejects_calendar_invalid_dates() {
        // All-zero timestamp is not a real UTC calendar date.
        assert!(validate_name("00000000000000-16EB56ADB4110CF2").is_err());
        assert!(validate_name("20261301000000-0A").is_err()); // month 13
        assert!(validate_name("20260230000000-0A").is_err()); // Feb 30 (non-leap)
        assert!(validate_name("20240229000000-0A").is_ok()); // Feb 29 (leap year)
        assert!(validate_name("20260705235959-0A").is_ok());
    }
}
