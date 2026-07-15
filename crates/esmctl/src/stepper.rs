//! Splits a `[start, end]` UTC range into sub-ranges by a step granularity.
//! Ports `app/vmctl/stepper/split.go`. Times are unix nanoseconds (UTC).

use crate::civil::{first_of_month, to_components, NANOS_PER_SEC};

const DAY_NS: i64 = 86_400 * NANOS_PER_SEC;
const HOUR_NS: i64 = 3_600 * NANOS_PER_SEC;
const MINUTE_NS: i64 = 60 * NANOS_PER_SEC;

/// Splits `[start_ns, end_ns]` into sub-ranges of the given `step`
/// (`month`/`week`/`day`/`hour`/`minute`). Month ranges are aligned to the
/// 1st of each month. Ports `SplitDateRange`.
pub(crate) fn split_date_range(
    start_ns: i64,
    end_ns: i64,
    step: &str,
    time_reverse: bool,
) -> Result<Vec<(i64, i64)>, String> {
    if start_ns > end_ns {
        return Err(format!(
            "start time {:?} should come before end time {:?}",
            crate::civil::format_rfc3339(start_ns),
            crate::civil::format_rfc3339(end_ns)
        ));
    }

    let next_step: fn(i64) -> (i64, i64) = match step {
        "month" => next_month,
        "day" => |t| (t, t + DAY_NS),
        "week" => |t| (t, t + 7 * DAY_NS),
        "hour" => |t| (t, t + HOUR_NS),
        "minute" => |t| (t, t + MINUTE_NS),
        other => {
            return Err(format!(
                "failed to parse step value, valid values are: 'month', 'day', 'hour', 'minute'. provided: '{other}'"
            ))
        }
    };

    let mut current = start_ns;
    let mut ranges: Vec<(i64, i64)> = Vec::new();
    while end_ns > current {
        let (s, mut e) = next_step(current);
        if e > end_ns {
            e = end_ns;
        }
        ranges.push((s, e));
        current = e;
    }
    if time_reverse {
        // Stable sort by start descending (ports sort.SliceStable).
        ranges.sort_by_key(|r| std::cmp::Reverse(r.0));
    }
    Ok(ranges)
}

fn next_month(t: i64) -> (i64, i64) {
    let (y, mo, ..) = to_components(t);
    let mo = mo as i64;
    let mut end_of_month = first_of_month(y, mo + 1) - 1;
    let mut start = t;
    if t == end_of_month {
        end_of_month = first_of_month(y, mo + 2) - 1;
        start = first_of_month(y, mo + 1);
    }
    (start, end_of_month)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::civil::from_components;

    #[test]
    fn rejects_bad_step() {
        assert!(split_date_range(0, DAY_NS, "year", false).is_err());
    }

    #[test]
    fn rejects_reversed_range() {
        assert!(split_date_range(DAY_NS, 0, "day", false).is_err());
    }

    #[test]
    fn splits_by_day() {
        let start = from_components(2024, 1, 1, 0, 0, 0);
        let end = from_components(2024, 1, 3, 0, 0, 0);
        let ranges = split_date_range(start, end, "day", false).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], (start, start + DAY_NS));
        assert_eq!(ranges[1], (start + DAY_NS, end));
    }

    #[test]
    fn splits_by_month_aligned() {
        let start = from_components(2024, 1, 15, 0, 0, 0);
        let end = from_components(2024, 3, 10, 0, 0, 0);
        let ranges = split_date_range(start, end, "month", false).unwrap();
        // Jan 15 → end of Jan, Feb, then Mar up to end.
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].0, start);
        // second range starts on Feb 1 00:00.
        assert_eq!(ranges[1].0, from_components(2024, 2, 1, 0, 0, 0));
        assert_eq!(ranges[2].1, end);
    }

    #[test]
    fn reverse_orders_descending() {
        let start = from_components(2024, 1, 1, 0, 0, 0);
        let end = from_components(2024, 1, 3, 0, 0, 0);
        let ranges = split_date_range(start, end, "day", true).unwrap();
        assert!(ranges[0].0 > ranges[1].0);
    }
}
