//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/time.go.
//!
//! All timestamps are int64 milliseconds since the Unix epoch. `date` values
//! are `timestamp_ms / MSEC_PER_DAY`.

use crate::util::{civil_from_days, days_from_civil};
use std::fmt;

/// Go: msecPerDay.
pub const MSEC_PER_DAY: i64 = 24 * 3600 * 1000;

/// Go: msecPerHour.
pub const MSEC_PER_HOUR: i64 = 3600 * 1000;

/// The max millisecond that is allowed to be used as the sample timestamp:
/// the last millisecond of the last complete monthly partition representable
/// in Go's int64-nanosecond time math — 2262-03-31 23:59:59.999 UTC.
/// Go: maxUnixMilli.
pub const MAX_UNIX_MILLI: i64 = 9_222_422_399_999;

/// TimeRange is a time range (both bounds are inclusive milliseconds).
/// Go: TimeRange.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeRange {
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

/// Zero time range used to force global index search.
/// Go: globalIndexTimeRange.
pub const GLOBAL_INDEX_TIME_RANGE: TimeRange = TimeRange {
    min_timestamp: 0,
    max_timestamp: 0,
};

/// Zero date used to force global index search. Go: globalIndexDate.
pub const GLOBAL_INDEX_DATE: u64 = 0;

impl TimeRange {
    /// Returns the date range for the time range. Go: TimeRange.DateRange.
    pub fn date_range(&self) -> (u64, u64) {
        let min_date = self.min_timestamp as u64 / MSEC_PER_DAY as u64;

        // Sample at max timestamp should be included because the end is
        // inclusive. However, if both timestamps are the same and point to
        // the beginning of the day, maxDate would be smaller than minDate;
        // in this case maxDate is set to minDate.
        let max_date = (self.max_timestamp as u64 / MSEC_PER_DAY as u64).max(min_date);

        (min_date, max_date)
    }

    /// Returns the time range of the whole month containing `timestamp`:
    /// `[first ms of the month .. last ms of the month]`.
    /// Go: TimeRange.fromPartitionTimestamp.
    pub fn from_partition_timestamp(timestamp: i64) -> TimeRange {
        let days = timestamp.div_euclid(MSEC_PER_DAY);
        let (y, m, _) = civil_from_days(days);
        Self::from_month(y, m)
    }

    /// Initializes the time range from the given partition name (`"2006_01"`
    /// format). Go: TimeRange.fromPartitionName.
    pub fn from_partition_name(name: &str) -> Result<TimeRange, String> {
        let err = || format!("cannot parse partition name {name:?}");
        let b = name.as_bytes();
        if b.len() != 7
            || b[4] != b'_'
            || !b
                .iter()
                .enumerate()
                .all(|(i, c)| i == 4 || c.is_ascii_digit())
        {
            return Err(err());
        }
        let y: i64 = name[..4].parse().map_err(|_| err())?;
        let m: u32 = name[5..].parse().map_err(|_| err())?;
        if !(1..=12).contains(&m) {
            return Err(err());
        }
        Ok(Self::from_month(y, m))
    }

    /// Go: TimeRange.fromPartitionTime.
    fn from_month(y: i64, m: u32) -> TimeRange {
        let min_days = days_from_civil(y, m, 1);
        let (next_y, next_m) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
        let max_days = days_from_civil(next_y, next_m, 1);
        TimeRange {
            min_timestamp: min_days * MSEC_PER_DAY,
            max_timestamp: max_days * MSEC_PER_DAY - 1,
        }
    }

    /// Returns true if the time range overlaps with `v`.
    /// Go: TimeRange.overlapsWith.
    pub fn overlaps_with(&self, v: TimeRange) -> bool {
        self.min_timestamp <= v.max_timestamp && self.max_timestamp >= v.min_timestamp
    }

    /// Returns true if the time range contains the given timestamp.
    /// Go: TimeRange.contains.
    pub fn contains(&self, timestamp: i64) -> bool {
        self.min_timestamp <= timestamp && timestamp <= self.max_timestamp
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == GLOBAL_INDEX_TIME_RANGE {
            return write!(f, "[entire retention period]");
        }
        write!(
            f,
            "[{}..{}]",
            timestamp_to_human_readable_format(self.min_timestamp),
            timestamp_to_human_readable_format(self.max_timestamp)
        )
    }
}

/// Returns human readable representation of the date (days since epoch).
/// Go: dateToString.
pub fn date_to_string(date: u64) -> String {
    if date == GLOBAL_INDEX_DATE {
        return "[entire retention period]".to_string();
    }
    let (y, m, d) = civil_from_days(date as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Converts the given millisecond timestamp to human-readable RFC3339-like
/// UTC format with trailing-zero-trimmed milliseconds (Go layout
/// `"2006-01-02T15:04:05.999Z"`). Go: TimestampToHumanReadableFormat.
pub fn timestamp_to_human_readable_format(timestamp: i64) -> String {
    let days = timestamp.div_euclid(MSEC_PER_DAY);
    let mut rem = timestamp - days * MSEC_PER_DAY; // [0, MSEC_PER_DAY)
    let (y, m, d) = civil_from_days(days);
    let msec = rem % 1000;
    rem /= 1000;
    let sec = rem % 60;
    rem /= 60;
    let min = rem % 60;
    let hour = rem / 60;
    let mut s = format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}");
    if msec != 0 {
        let frac = format!("{msec:03}");
        s.push('.');
        s.push_str(frac.trim_end_matches('0'));
    }
    s.push('Z');
    s
}

/// Returns partition name (`"2006_01"` format) for the given timestamp.
/// Go: timestampToPartitionName.
pub fn timestamp_to_partition_name(timestamp: i64) -> String {
    let days = timestamp.div_euclid(MSEC_PER_DAY);
    let (y, m, _) = civil_from_days(days);
    format!("{y:04}_{m:02}")
}

/// Returns true if the timestamp (must be in seconds) is within the first
/// hour of the day. Go: isFirstHourOfDay.
pub fn is_first_hour_of_day(timestamp_secs: u64) -> bool {
    (timestamp_secs / 3600) % 24 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ymd_to_msec(y: i64, m: u32, d: u32) -> i64 {
        days_from_civil(y, m, d) * MSEC_PER_DAY
    }

    // Port of TestTimeRangeFromPartition. Go iterates hours from time.Now();
    // a fixed base timestamp is used here to keep the test deterministic.
    #[test]
    fn time_range_from_partition() {
        let base = ymd_to_msec(2026, 1, 1);
        for i in 0..24 * 30 * 365 {
            check_time_range_from_partition(base + i * MSEC_PER_HOUR);
        }
    }

    fn check_time_range_from_partition(initial_ts: i64) {
        let (y, m, _) = civil_from_days(initial_ts.div_euclid(MSEC_PER_DAY));
        let tr = TimeRange::from_partition_timestamp(initial_ts);

        let (min_y, min_m, _) = civil_from_days(tr.min_timestamp.div_euclid(MSEC_PER_DAY));
        assert_eq!((min_y, min_m), (y, m), "unexpected month for MinTimestamp");

        // Verify that the previous millisecond from tr.min_timestamp belongs
        // to the previous month.
        let (prev_y, prev_m, _) = civil_from_days((tr.min_timestamp - 1).div_euclid(MSEC_PER_DAY));
        assert_eq!(
            prev_y * 12 + (prev_m as i64 - 1) + 1,
            min_y * 12 + (min_m as i64 - 1),
            "unexpected prev month for ts {initial_ts}"
        );

        let (max_y, max_m, _) = civil_from_days(tr.max_timestamp.div_euclid(MSEC_PER_DAY));
        assert_eq!((max_y, max_m), (y, m), "unexpected month for MaxTimestamp");

        // Verify that the next millisecond from tr.max_timestamp belongs to
        // the next month.
        let (next_y, next_m, _) = civil_from_days((tr.max_timestamp + 1).div_euclid(MSEC_PER_DAY));
        assert_eq!(
            next_y * 12 + (next_m as i64 - 1) - 1,
            max_y * 12 + (max_m as i64 - 1),
            "unexpected next month for ts {initial_ts}"
        );
    }

    // Port of TestTimeRangeOverlapsWith.
    #[test]
    fn time_range_overlaps_with() {
        fn f(min1: i64, max1: i64, min2: i64, max2: i64, want: bool) {
            let tr1 = TimeRange {
                min_timestamp: min1,
                max_timestamp: max1,
            };
            let tr2 = TimeRange {
                min_timestamp: min2,
                max_timestamp: max2,
            };
            assert_eq!(tr1.overlaps_with(tr2), want);
        }
        f(0, 0, 0, 0, true);
        f(0, 0, 0, 1, true);
        f(0, 1, 0, 0, true);
        f(1, 2, 0, 0, false);
        f(0, 0, 1, 2, false);
        f(1, 2, 0, 3, true);
        f(1, 10, 5, 15, true);
        f(5, 15, 1, 10, true);
    }

    // Port of TestTimeRangeContains.
    #[test]
    fn time_range_contains() {
        fn f(min: i64, max: i64, ts: i64, want: bool) {
            let tr = TimeRange {
                min_timestamp: min,
                max_timestamp: max,
            };
            assert_eq!(tr.contains(ts), want);
        }
        f(0, 0, 0, true);
        f(0, 0, 1, false);
        f(0, 0, -1, false);

        f(1, 3, 0, false);
        f(1, 3, 1, true);
        f(1, 3, 2, true);
        f(1, 3, 3, true);
        f(1, 3, 4, false);

        f(0, i64::MAX, -1, false);
        f(0, i64::MAX, 0, true);
        f(0, i64::MAX, 1, true);
        f(0, i64::MAX, i64::MAX / 2, true);
        f(0, i64::MAX, i64::MAX - 1, true);
        f(0, i64::MAX, i64::MAX, true);
    }

    // Port of TestTimeRangeDateRange.
    #[test]
    fn time_range_date_range() {
        fn f(tr: TimeRange, want_min_date: u64, want_max_date: u64) {
            assert_eq!(tr.date_range(), (want_min_date, want_max_date), "{tr:?}");
        }
        let tr = |min, max| TimeRange {
            min_timestamp: min,
            max_timestamp: max,
        };

        // Timestamps belong to different days.
        f(tr(MSEC_PER_DAY + 123, 2 * MSEC_PER_DAY + 456), 1, 2);
        // Both timestamps belong to the same day.
        f(tr(MSEC_PER_DAY + 123, MSEC_PER_DAY + 456), 1, 1);
        // MinTimestamp equals MaxTimestamp.
        f(tr(MSEC_PER_DAY + 123, MSEC_PER_DAY + 123), 1, 1);
        // MinTimestamp is the first ms of the day and equals MaxTimestamp.
        f(tr(MSEC_PER_DAY, MSEC_PER_DAY), 1, 1);
        // MinTimestamp is greater than MaxTimestamp.
        f(tr(2 * MSEC_PER_DAY + 654, MSEC_PER_DAY + 321), 2, 2);
        // MaxTimestamp is the first millisecond of the day.
        f(tr(MSEC_PER_DAY + 123, 2 * MSEC_PER_DAY), 1, 2);
        f(tr(MSEC_PER_DAY + 123, 2 * MSEC_PER_DAY + 1), 1, 2);
    }

    // Port of TestDateToString.
    #[test]
    fn date_to_string_table() {
        assert_eq!(
            date_to_string(GLOBAL_INDEX_DATE),
            "[entire retention period]"
        );
        assert_eq!(date_to_string(1), "1970-01-02");
        assert_eq!(date_to_string(10), "1970-01-11");
    }

    // Port of TestTimeRangeString.
    #[test]
    fn time_range_string() {
        assert_eq!(
            GLOBAL_INDEX_TIME_RANGE.to_string(),
            "[entire retention period]"
        );
        assert_eq!(
            TimeRange {
                min_timestamp: 0,
                max_timestamp: 1,
            }
            .to_string(),
            "[1970-01-01T00:00:00Z..1970-01-01T00:00:00.001Z]"
        );
        assert_eq!(
            TimeRange {
                min_timestamp: 1,
                max_timestamp: 2,
            }
            .to_string(),
            "[1970-01-01T00:00:00.001Z..1970-01-01T00:00:00.002Z]"
        );
        assert_eq!(
            TimeRange {
                min_timestamp: ymd_to_msec(2024, 9, 6),
                max_timestamp: ymd_to_msec(2024, 9, 7) - 1,
            }
            .to_string(),
            "[2024-09-06T00:00:00Z..2024-09-06T23:59:59.999Z]"
        );
    }

    // Port of TestTimeRange_fromPartitionTimestamp.
    #[test]
    fn time_range_from_partition_timestamp() {
        // 2025-03-23T14:07:56.999Z
        let ts = ymd_to_msec(2025, 3, 23) + 14 * MSEC_PER_HOUR + 7 * 60_000 + 56 * 1000 + 999;
        let got = TimeRange::from_partition_timestamp(ts);
        let want = TimeRange {
            min_timestamp: ymd_to_msec(2025, 3, 1),
            max_timestamp: ymd_to_msec(2025, 4, 1) - 1,
        };
        assert_eq!(got, want);
    }

    #[test]
    fn time_range_from_partition_name() {
        let got = TimeRange::from_partition_name("2025_03").unwrap();
        assert_eq!(
            got,
            TimeRange::from_partition_timestamp(ymd_to_msec(2025, 3, 15))
        );
        assert_eq!(timestamp_to_partition_name(got.min_timestamp), "2025_03");
        assert_eq!(timestamp_to_partition_name(got.max_timestamp), "2025_03");

        for bad in [
            "", "2025-03", "2025_13", "2025_00", "202_035", "x025_03", "2025_3",
        ] {
            assert!(
                TimeRange::from_partition_name(bad).is_err(),
                "expected error for {bad:?}"
            );
        }
    }

    // Port of TestIsFirstHourOfDay.
    #[test]
    fn first_hour_of_day() {
        let day = ymd_to_msec(2000, 1, 1) / 1000;
        fn f(secs: i64, want: bool) {
            assert_eq!(is_first_hour_of_day(secs as u64), want, "secs={secs}");
        }
        f(day, true);
        f(day + 12 * 60 + 34, true); // 00:12:34
        f(day + 59 * 60 + 59, true); // 00:59:59
        f(day + 3600, false); // 01:00:00
        f(day + 5 * 3600, false); // 05:00:00
        f(day + 24 * 3600 - 1, false); // 23:59:59
    }

    // Port of TestMaxUnixMilli.
    #[test]
    fn max_unix_milli() {
        // 2262-03-31 23:59:59.999 UTC.
        let last_future_pt_max_time =
            ymd_to_msec(2262, 3, 31) + 23 * MSEC_PER_HOUR + 59 * 60_000 + 59 * 1000 + 999;
        assert_eq!(last_future_pt_max_time, MAX_UNIX_MILLI);
    }
}
