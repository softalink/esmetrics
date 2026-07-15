//! Small shared helpers extracted from Go lib/storage common places.
//!
//! Stage 1 only needs proleptic-Gregorian civil-date math (Go uses the
//! `time` package for this; Rust gets by without a date crate).
//! Later stages may add more helpers here (keep this module small).

/// Returns the number of days since the Unix epoch (1970-01-01) for the given
/// civil date. Negative results are valid (dates before the epoch).
///
/// Based on Howard Hinnant's `days_from_civil` algorithm, which matches Go's
/// `time.Date(y, m, d, ...)` for all dates representable in the storage
/// (proleptic Gregorian calendar, UTC).
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = (if m > 2 { m - 3 } else { m + 9 }) as u64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + (d as u64 - 1); // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe as i64 - 719_468
}

/// Inverse of [`days_from_civil`]: converts days since the Unix epoch to
/// a civil (year, month, day) date.
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Deterministic pseudo-random generator for tests (splitmix64).
/// Replaces Go's `rand.New(rand.NewSource(seed))` in ported tests.
#[cfg(test)]
pub(crate) fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_days_roundtrip() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        // Roundtrip over a wide range of days (deterministic).
        let mut day = -400 * 365;
        while day < 400 * 365 {
            let (y, m, d) = civil_from_days(day);
            assert_eq!(days_from_civil(y, m, d), day, "date {y:04}-{m:02}-{d:02}");
            day += 13;
        }
    }
}
