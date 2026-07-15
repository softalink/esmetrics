//! Query-arg parsing helpers. Ports of `lib/httputil/{time,duration,bool,
//! int}.go`, `lib/timeutil/time.go` and `app/vmselect/searchutil/
//! searchutil.go` (the subsets used by the query handlers).
//!
//! PORT-DIVERGENCE: timestamps without timezone information are interpreted
//! as UTC instead of the server's local timezone (Go uses
//! `GetLocalTimezoneOffsetNsecs`). TSBS and Grafana always send unix
//! timestamps or RFC3339 with an explicit zone, so this only affects
//! hand-written naive timestamps.

use crate::params::Params;
use esm_metricsql::LabelFilter;
use std::time::{SystemTime, UNIX_EPOCH};

/// `defaultStep` from prometheus.go: 5 minutes in milliseconds.
pub(crate) const DEFAULT_STEP: i64 = 5 * 60 * 1000;

/// Maximum millisecond timestamp storable in an int64 of nanoseconds.
pub(crate) const MAX_TIME_MSECS: i64 = i64::MAX / 1_000_000;

const MAX_DURATION_MSECS: i64 = 100 * 365 * 24 * 3600 * 1000;

/// `time.Unix(math.MinInt64/1000+62135596801, 0).UTC().Format(RFC3339Nano)`;
/// Prometheus clients send this literal string for "min time".
const PROMETHEUS_MIN_TIME: &str = "-292273086-05-16T16:47:06Z";
/// `time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()` formatted.
const PROMETHEUS_MAX_TIME: &str = "292277025-08-18T07:12:54.999999999Z";

pub(crate) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn now_unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| clamp_i128(d.as_nanos() as i128))
        .unwrap_or(0)
}

fn clamp_i128(v: i128) -> i64 {
    if v > i64::MAX as i128 {
        i64::MAX
    } else if v < i64::MIN as i128 {
        i64::MIN
    } else {
        v as i64
    }
}

/// Port of `httputil.GetTime`: returns milliseconds. Missing arg → the
/// default rounded down to whole seconds (Grafana alignment).
pub(crate) fn get_time(params: &Params, key: &str, default_ms: i64) -> Result<i64, String> {
    let arg = params.get(key).unwrap_or("");
    if arg.is_empty() {
        return Ok(default_ms - default_ms % 1000);
    }
    match arg {
        PROMETHEUS_MIN_TIME => return Ok(0),
        PROMETHEUS_MAX_TIME => return Ok(MAX_TIME_MSECS),
        _ => {}
    }
    let msecs = parse_time_msec(arg).map_err(|err| format!("cannot parse {key}={arg}: {err}"))?;
    // The storage engine doesn't support negative time.
    Ok(msecs.clamp(0, MAX_TIME_MSECS))
}

/// Port of `timeutil.ParseTimeMsec`.
pub(crate) fn parse_time_msec(s: &str) -> Result<i64, String> {
    let nsecs = parse_time_at(s, now_unix_ns())?;
    Ok((nsecs as f64 / 1e6).round() as i64)
}

/// Port of `timeutil.ParseTimeAt`: returns unix nanoseconds.
pub(crate) fn parse_time_at(s_orig: &str, current_ns: i64) -> Result<i64, String> {
    if s_orig == "now" {
        return Ok(current_ns);
    }
    let mut s = s_orig;
    let mut tz_offset_ns: i64 = 0;
    if s_orig.len() > 6 {
        // Try parsing a trailing `±HH:MM` timezone offset.
        let tz = &s_orig.as_bytes()[s_orig.len() - 6..];
        if (tz[0] == b'-' || tz[0] == b'+') && tz[3] == b':' {
            let hour = parse_2digits(&tz[1..3]).ok_or_else(|| {
                format!(
                    "cannot parse hour from timezone offset {:?}",
                    &s_orig[s_orig.len() - 6..]
                )
            })?;
            let minute = parse_2digits(&tz[4..6]).ok_or_else(|| {
                format!(
                    "cannot parse minute from timezone offset {:?}",
                    &s_orig[s_orig.len() - 6..]
                )
            })?;
            tz_offset_ns = i64::from(hour * 3600 + minute * 60) * 1_000_000_000;
            if tz[0] == b'+' {
                tz_offset_ns = -tz_offset_ns;
            }
            s = &s_orig[..s_orig.len() - 6];
        } else if let Some(stripped) = s.strip_suffix('Z') {
            s = stripped;
        }
        // else: no timezone info — Go uses the local timezone here; this
        // port interprets the value as UTC (see the module docs).
    }
    s = s.strip_suffix('Z').unwrap_or(s);
    let b = s.as_bytes();
    if (!b.is_empty() && (b[b.len() - 1] > b'9' || b[0] == b'-')) || s.starts_with("now") {
        // Duration relative to the current time.
        let s = s.strip_prefix("now").unwrap_or(s);
        let d_ms = esm_metricsql::duration_value(s, 0).map_err(|e| e.to_string())?;
        let d_ns = (d_ms.unsigned_abs() as i128) * 1_000_000;
        return Ok(clamp_i128(current_ns as i128 - d_ns));
    }
    if s.len() == 4 {
        return parse_calendar(s, &[4], tz_offset_ns);
    }
    if !s_orig.contains('-') {
        return try_parse_unix_timestamp(s_orig)
            .ok_or_else(|| format!("cannot parse numeric timestamp {s_orig:?}"));
    }
    match s.len() {
        7 => parse_calendar(s, &[4, 2], tz_offset_ns),
        10 => parse_calendar(s, &[4, 2, 2], tz_offset_ns),
        13 => parse_calendar(s, &[4, 2, 2, 2], tz_offset_ns),
        16 => parse_calendar(s, &[4, 2, 2, 2, 2], tz_offset_ns),
        19 => parse_calendar(s, &[4, 2, 2, 2, 2, 2], tz_offset_ns),
        _ => parse_rfc3339(s_orig),
    }
}

fn parse_2digits(b: &[u8]) -> Option<u32> {
    if b.len() != 2 || !b[0].is_ascii_digit() || !b[1].is_ascii_digit() {
        return None;
    }
    Some(u32::from(b[0] - b'0') * 10 + u32::from(b[1] - b'0'))
}

/// Parses the fixed-length calendar prefixes `YYYY[-MM[-DD[THH[:MM[:SS]]]]]`
/// (Go layouts `"2006"` .. `"2006-01-02T15:04:05"`). `field_widths` gives
/// the number of expected fields; separators are `-`, `-`, `T`, `:`, `:`.
fn parse_calendar(s: &str, field_widths: &[usize], tz_offset_ns: i64) -> Result<i64, String> {
    const SEPS: [u8; 5] = *b"--T::";
    let b = s.as_bytes();
    let mut fields = [0u32; 6];
    let mut pos = 0usize;
    for (i, &width) in field_widths.iter().enumerate() {
        if i > 0 {
            if pos >= b.len() || b[pos] != SEPS[i - 1] {
                return Err(format!("cannot parse {s:?} as a calendar timestamp"));
            }
            pos += 1;
        }
        if pos + width > b.len() {
            return Err(format!("cannot parse {s:?} as a calendar timestamp"));
        }
        let mut v: u32 = 0;
        for &c in &b[pos..pos + width] {
            if !c.is_ascii_digit() {
                return Err(format!("cannot parse {s:?} as a calendar timestamp"));
            }
            v = v * 10 + u32::from(c - b'0');
        }
        fields[i] = v;
        pos += width;
    }
    if pos != b.len() {
        return Err(format!("cannot parse {s:?} as a calendar timestamp"));
    }
    let year = i64::from(fields[0]);
    let month = if field_widths.len() > 1 { fields[1] } else { 1 };
    let day = if field_widths.len() > 2 { fields[2] } else { 1 };
    let (hour, minute, sec) = (
        if field_widths.len() > 3 { fields[3] } else { 0 },
        if field_widths.len() > 4 { fields[4] } else { 0 },
        if field_widths.len() > 5 { fields[5] } else { 0 },
    );
    let ns = civil_to_unix_ns(year, month, day, hour, minute, sec, 0)
        .ok_or_else(|| format!("invalid calendar timestamp {s:?}"))?;
    Ok(clamp_i128(ns as i128 + tz_offset_ns as i128))
}

/// Strict RFC3339: `YYYY-MM-DDTHH:MM:SS[.fffffffff](Z|±HH:MM)`.
fn parse_rfc3339(s: &str) -> Result<i64, String> {
    let err = || format!("cannot parse {s:?} as RFC3339 timestamp");
    let b = s.as_bytes();
    if b.len() < 20 {
        return Err(err());
    }
    let field = |from: usize, to: usize| -> Option<u32> {
        let mut v = 0u32;
        for &c in b.get(from..to)? {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + u32::from(c - b'0');
        }
        Some(v)
    };
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return Err(err());
    }
    let year = field(0, 4).ok_or_else(err)?;
    let month = field(5, 7).ok_or_else(err)?;
    let day = field(8, 10).ok_or_else(err)?;
    let hour = field(11, 13).ok_or_else(err)?;
    let minute = field(14, 16).ok_or_else(err)?;
    let sec = field(17, 19).ok_or_else(err)?;
    let mut pos = 19;
    let mut frac_ns: u32 = 0;
    if b[pos] == b'.' {
        pos += 1;
        let start = pos;
        while pos < b.len() && b[pos].is_ascii_digit() {
            pos += 1;
        }
        let digits = pos - start;
        if digits == 0 || digits > 9 {
            return Err(err());
        }
        for &c in &b[start..pos] {
            frac_ns = frac_ns * 10 + u32::from(c - b'0');
        }
        frac_ns *= 10u32.pow(9 - digits as u32);
    }
    let tz_ns: i64 = if pos < b.len() && b[pos] == b'Z' {
        pos += 1;
        0
    } else if pos + 6 == b.len() && (b[pos] == b'+' || b[pos] == b'-') && b[pos + 3] == b':' {
        let h = parse_2digits(&b[pos + 1..pos + 3]).ok_or_else(err)?;
        let m = parse_2digits(&b[pos + 4..pos + 6]).ok_or_else(err)?;
        let off = i64::from(h * 3600 + m * 60) * 1_000_000_000;
        pos += 6;
        if b[pos - 6] == b'+' {
            -off
        } else {
            off
        }
    } else {
        return Err(err());
    };
    if pos != b.len() {
        return Err(err());
    }
    let ns = civil_to_unix_ns(i64::from(year), month, day, hour, minute, sec, frac_ns)
        .ok_or_else(err)?;
    Ok(clamp_i128(ns as i128 + tz_ns as i128))
}

/// Days-from-civil (Howard Hinnant's algorithm) → unix nanoseconds.
fn civil_to_unix_ns(
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    sec: u32,
    frac_ns: u32,
) -> Option<i64> {
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || sec > 59 {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp as i64 + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719_468;
    let secs =
        days as i128 * 86_400 + i128::from(hour) * 3600 + i128::from(minute) * 60 + i128::from(sec);
    Some(clamp_i128(secs * 1_000_000_000 + i128::from(frac_ns)))
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Port of `timeutil.TryParseUnixTimestamp`: parses seconds, milliseconds,
/// microseconds or nanoseconds (auto-detected by magnitude), in integer,
/// fractional or scientific notation. Returns nanoseconds.
pub(crate) fn try_parse_unix_timestamp(s: &str) -> Option<i64> {
    if let Some(exp_idx) = s.find(['e', 'E']) {
        let decimal_exp: i64 = s[exp_idx + 1..].parse().ok()?;
        let n = parse_scientific(&s[..exp_idx], decimal_exp)?;
        return Some(unix_timestamp_ns(n));
    }
    let Some(dot_idx) = s.find('.') else {
        let n: i64 = s.parse().ok()?;
        return Some(unix_timestamp_ns(n));
    };
    let mut n = parse_fractional(&s[..dot_idx], &s[dot_idx + 1..])?;
    let mut decimal_exp = s.len() - dot_idx - 1;
    while decimal_exp % 3 != 0 {
        n = n.checked_mul(10)?;
        decimal_exp += 1;
    }
    Some(unix_timestamp_ns(n))
}

fn parse_scientific(s: &str, decimal_exp: i64) -> Option<i64> {
    let Some(dot_idx) = s.find('.') else {
        let n: i64 = s.parse().ok()?;
        return multiply_by_decimal_exp(n, decimal_exp);
    };
    let frac = &s[dot_idx + 1..];
    if decimal_exp < frac.len() as i64 {
        return None;
    }
    let n = parse_fractional(&s[..dot_idx], frac)?;
    multiply_by_decimal_exp(n, decimal_exp - frac.len() as i64)
}

fn parse_fractional(int_str: &str, frac_str: &str) -> Option<i64> {
    let n: i64 = int_str.parse().ok()?;
    let num = multiply_by_decimal_exp(n, frac_str.len() as i64)?;
    let frac: i64 = frac_str.parse().ok()?;
    if num >= 0 {
        num.checked_add(frac)
    } else {
        num.checked_sub(frac)
    }
}

fn multiply_by_decimal_exp(n: i64, decimal_exp: i64) -> Option<i64> {
    if !(0..=18).contains(&decimal_exp) {
        return None;
    }
    if decimal_exp == 0 {
        return Some(n);
    }
    n.checked_mul(10i64.pow(decimal_exp as u32))
}

fn unix_timestamp_ns(n: i64) -> i64 {
    const MAX_SECOND: i64 = i64::MAX / 1_000_000_000;
    const MIN_SECOND: i64 = i64::MIN / 1_000_000_000;
    const MAX_MILLI: i64 = i64::MAX / 1_000_000;
    const MIN_MILLI: i64 = i64::MIN / 1_000_000;
    const MAX_MICRO: i64 = i64::MAX / 1_000;
    const MIN_MICRO: i64 = i64::MIN / 1_000;
    if (MIN_SECOND..=MAX_SECOND).contains(&n) {
        n * 1_000_000_000
    } else if (MIN_MILLI..=MAX_MILLI).contains(&n) {
        n * 1_000_000
    } else if (MIN_MICRO..=MAX_MICRO).contains(&n) {
        n * 1_000
    } else {
        n
    }
}

/// Port of `httputil.GetDuration`: returns milliseconds. Numeric values are
/// float **seconds**; otherwise a Prometheus duration string. The value must
/// be in `[1ms, 100y]`.
pub(crate) fn get_duration(params: &Params, key: &str, default_ms: i64) -> Result<i64, String> {
    let arg = params.get(key).unwrap_or("");
    if arg.is_empty() || arg == "undefined" {
        // "undefined" is a hack for Grafana, which may send that literal.
        return Ok(default_ms);
    }
    let msecs = match arg.parse::<f64>() {
        Ok(secs) => {
            let ms = secs * 1e3;
            if ms >= i64::MAX as f64 {
                i64::MAX
            } else if ms <= i64::MIN as f64 {
                i64::MIN
            } else {
                ms as i64
            }
        }
        Err(_) => esm_metricsql::duration_value(arg, 0)
            .map_err(|e| format!("cannot parse {key:?}={arg:?}: {e}"))?,
    };
    if msecs <= 0 || msecs > MAX_DURATION_MSECS {
        return Err(format!(
            "{key}={msecs}ms is out of allowed range [1ms ... {MAX_DURATION_MSECS}ms]"
        ));
    }
    Ok(msecs)
}

/// Port of `httputil.GetBool`.
pub(crate) fn get_bool(params: &Params, key: &str) -> bool {
    let v = params.get(key).unwrap_or("").to_ascii_lowercase();
    !matches!(v.as_str(), "" | "0" | "f" | "false" | "no")
}

/// Port of `httputil.GetInt`.
pub(crate) fn get_int(params: &Params, key: &str) -> Result<i64, String> {
    let arg = params.get(key).unwrap_or("");
    if arg.is_empty() {
        return Ok(0);
    }
    arg.parse::<i64>()
        .map_err(|e| format!("cannot parse integer {key:?}={arg:?}: {e}"))
}

/// Port of `searchutil.getDeadlineWithMaxDuration`: the effective timeout in
/// milliseconds — the `timeout` query arg clamps `d_max_ms` down only.
pub(crate) fn get_timeout_ms(params: &Params, d_max_ms: i64) -> i64 {
    let d = get_duration(params, "timeout", 0).unwrap_or(0);
    if d <= 0 || d > d_max_ms {
        d_max_ms
    } else {
        d
    }
}

/// Port of `searchutil.ParseMetricSelector`, kept at the
/// `metricsql::LabelFilter` level (the [`esm_promql::MetricsProvider`]
/// boundary takes label filters, not storage tag filters).
pub(crate) fn parse_metric_selector(s: &str) -> Result<Vec<Vec<LabelFilter>>, String> {
    let expr = esm_metricsql::parse(s).map_err(|e| e.to_string())?;
    let esm_metricsql::Expr::Metric(me) = expr else {
        let mut got = String::new();
        expr.append_string(&mut got);
        return Err(format!("expecting metricSelector; got {got:?}"));
    };
    if me.label_filterss.is_empty() {
        return Err("labelFilterss cannot be empty".to_string());
    }
    Ok(me.label_filterss)
}

/// Port of `getTagFilterssFromMatches`.
pub(crate) fn tag_filterss_from_matches(
    matches: &[String],
) -> Result<Vec<Vec<LabelFilter>>, String> {
    let mut tfss = Vec::with_capacity(matches.len());
    for m in matches {
        let local =
            parse_metric_selector(m).map_err(|e| format!("cannot parse matches[]={m}: {e}"))?;
        tfss.extend(local);
    }
    Ok(tfss)
}

/// Port of `searchutil.GetExtraTagFilters` (`extra_label` + `extra_filters`
/// args). URL query args take precedence over POST form args.
pub(crate) fn get_extra_tag_filters(params: &Params) -> Result<Vec<Vec<LabelFilter>>, String> {
    let mut tag_filters: Vec<LabelFilter> = Vec::new();
    for label in params.get_all_url_preferred("extra_label") {
        let Some((name, value)) = label.split_once('=') else {
            return Err(format!(
                "`extra_label` query arg must have the format `name=value`; got {label:?}"
            ));
        };
        tag_filters.push(LabelFilter {
            label: name.to_string(),
            value: value.to_string(),
            is_negative: false,
            is_regexp: false,
        });
    }
    let mut extra_filters = params.get_all_url_preferred("extra_filters");
    extra_filters.extend(params.get_all_url_preferred("extra_filters[]"));
    if extra_filters.is_empty() {
        if tag_filters.is_empty() {
            return Ok(Vec::new());
        }
        return Ok(vec![tag_filters]);
    }
    let mut etfs = Vec::new();
    for extra_filter in &extra_filters {
        let mut tfss = parse_metric_selector(extra_filter)
            .map_err(|e| format!("cannot parse extra_filters={extra_filter}: {e}"))?;
        for tfs in tfss.iter_mut() {
            tfs.extend(tag_filters.iter().cloned());
        }
        etfs.extend(tfss);
    }
    Ok(etfs)
}

/// Port of `searchutil.JoinTagFilterss`: ANDs every etfs group into every
/// src group (cross product).
pub(crate) fn join_tag_filterss(
    src: Vec<Vec<LabelFilter>>,
    etfs: &[Vec<LabelFilter>],
) -> Vec<Vec<LabelFilter>> {
    if src.is_empty() {
        return etfs.to_vec();
    }
    if etfs.is_empty() {
        return src;
    }
    let mut dst = Vec::with_capacity(src.len() * etfs.len());
    for tf in &src {
        for etf in etfs {
            let mut tfs = tf.clone();
            tfs.extend(etf.iter().cloned());
            dst.push(tfs);
        }
    }
    dst
}

/// Port of `unescapePrometheusLabelName` (prometheus.go): decodes the
/// Prometheus `U__` UTF-8 label-name escaping.
pub(crate) fn unescape_prometheus_label_name(name: &str) -> String {
    let Some(escaped) = name.strip_prefix("U__") else {
        return name.to_string();
    };
    let b = escaped.as_bytes();
    let mut out = String::with_capacity(escaped.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'_' {
            out.push(b[i] as char);
            i += 1;
            continue;
        }
        i += 1;
        if i >= b.len() {
            return name.to_string();
        }
        if b[i] == b'_' {
            out.push('_');
            i += 1;
            continue;
        }
        let mut val: u32 = 0;
        let mut j = 0;
        loop {
            if j >= 6 || i >= b.len() {
                return name.to_string();
            }
            if b[i] == b'_' {
                match char::from_u32(val) {
                    Some(c) => out.push(c),
                    None => return name.to_string(),
                }
                i += 1;
                break;
            }
            let c = b[i].to_ascii_lowercase();
            val *= 16;
            match c {
                b'0'..=b'9' => val += u32::from(c - b'0'),
                b'a'..=b'f' => val += u32::from(c - b'a') + 10,
                _ => return name.to_string(),
            }
            i += 1;
            j += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::Params;

    fn params(query: &str) -> Params {
        Params::from_query_string(query)
    }

    fn get_time_arg(s: &str) -> Result<i64, String> {
        let encoded: String = s.bytes().map(|b| format!("%{b:02X}")).collect();
        get_time(&params(&format!("s={encoded}")), "s", 123)
    }

    /// Port of `lib/httputil/time_test.go` `TestGetTimeSuccess`.
    #[test]
    fn get_time_success() {
        let f = |s: &str, expected: i64| {
            // Default value is rounded down to seconds.
            assert_eq!(get_time(&params(""), "s", 123_456).unwrap(), 123_000);
            let ts = get_time_arg(s).unwrap_or_else(|e| panic!("GetTime({s:?}): {e}"));
            assert_eq!(ts, expected, "GetTime({s:?})");
        };
        f("2019Z", 1546300800000);
        f("2019-01Z", 1546300800000);
        f("2019-02Z", 1548979200000);
        f("2019-02-01Z", 1548979200000);
        f("2019-02-02Z", 1549065600000);
        f("2019-02-02T00Z", 1549065600000);
        f("2019-02-02T01Z", 1549069200000);
        f("2019-02-02T01:00Z", 1549069200000);
        f("2019-02-02T01:01Z", 1549069260000);
        f("2019-02-02T01:01:00Z", 1549069260000);
        f("2019-02-02T01:01:01Z", 1549069261000);
        f("2020-02-21T16:07:49.433Z", 1582301269433);
        f("2019-07-07T20:47:40+03:00", 1562521660000);
        f("-292273086-05-16T16:47:06Z", 0); // prometheus minTime literal
        f("292277025-08-18T07:12:54.999999999Z", MAX_TIME_MSECS);
        f("1562529662.324", 1562529662324);
        f("1223372036.855", 1223372036855);
        // Relative duration resolving to a timestamp before 1970.
        f("-9223372036.854", 0);
    }

    /// Port of `lib/httputil/time_test.go` `TestGetTimeError`.
    #[test]
    fn get_time_error() {
        let f = |s: &str| {
            assert!(get_time_arg(s).is_err(), "expected error for {s:?}");
        };
        f("foo");
        f("foo1");
        f("1245-5");
        f("2022-x7");
        f("2022-02-x7");
        f("2022-02-02Tx7");
        f("2022-02-02T00:x7");
        f("2022-02-02T00:00:x7");
        f("2022-02-02T00:00:00a");
        f("2019-07-07T20:01:02Zisdf");
        f("2019-07-07T20:47:40+03:00123");
        f("-292273086-05-16T16:47:07Z");
        f("292277025-08-18T07:12:54.999999998Z");
        f("123md");
        f("-12.3md");
    }

    #[test]
    fn get_time_relative_now() {
        let now = now_unix_ms();
        let ts = get_time(&params("s=now-1h"), "s", 0).unwrap();
        let want = now - 3_600_000;
        assert!((ts - want).abs() < 2_000, "now-1h: got {ts}, want ~{want}");
        let ts = get_time(&params("s=now"), "s", 0).unwrap();
        assert!((ts - now).abs() < 2_000);
    }

    #[test]
    fn unix_timestamp_formats() {
        // seconds / milliseconds / microseconds / nanoseconds by magnitude.
        let f = |s: &str, want_ms: i64| {
            assert_eq!(get_time_arg(s).unwrap(), want_ms, "s={s}");
        };
        f("1562529662", 1562529662000);
        f("1562529662678", 1562529662678);
        f("1562529662678901", 1562529662679); // µs, rounded
        f("1562529662678901234", 1562529662679); // ns, rounded
        f("1.23456789e9", 1234567890000);
        f("0", 0);
    }

    #[test]
    fn get_duration_semantics() {
        let d = |q: &str, def: i64| get_duration(&params(q), "step", def);
        assert_eq!(d("", 300_000).unwrap(), 300_000);
        assert_eq!(d("step=undefined", 42).unwrap(), 42);
        assert_eq!(d("step=1.5", 0).unwrap(), 1_500); // float seconds
        assert_eq!(d("step=10", 0).unwrap(), 10_000);
        assert_eq!(d("step=10s", 0).unwrap(), 10_000);
        assert_eq!(d("step=5m", 0).unwrap(), 300_000);
        assert_eq!(d("step=1h", 0).unwrap(), 3_600_000);
        assert!(d("step=0", 0).is_err()); // out of [1ms, 100y]
        assert!(d("step=-1", 0).is_err());
        assert!(d("step=foo", 0).is_err());
    }

    #[test]
    fn timeout_clamps_down_only() {
        assert_eq!(get_timeout_ms(&params(""), 30_000), 30_000);
        assert_eq!(get_timeout_ms(&params("timeout=5"), 30_000), 5_000);
        assert_eq!(get_timeout_ms(&params("timeout=60"), 30_000), 30_000);
        assert_eq!(get_timeout_ms(&params("timeout=0"), 30_000), 30_000);
        assert_eq!(get_timeout_ms(&params("timeout=bad"), 30_000), 30_000);
    }

    #[test]
    fn metric_selector_parsing() {
        let tfss = parse_metric_selector("up{job=\"esm\"}").unwrap();
        assert_eq!(tfss.len(), 1);
        assert_eq!(tfss[0][0].label, "__name__");
        assert_eq!(tfss[0][0].value, "up");
        assert_eq!(tfss[0][1].label, "job");
        assert!(parse_metric_selector("rate(up[1m])").is_err());
    }

    #[test]
    fn extra_tag_filters() {
        let p = params("extra_label=env%3Dprod&extra_label=team%3Dcore");
        let etfs = get_extra_tag_filters(&p).unwrap();
        assert_eq!(etfs.len(), 1);
        assert_eq!(etfs[0].len(), 2);
        assert_eq!(etfs[0][0].label, "env");
        assert_eq!(etfs[0][0].value, "prod");

        let p = params("extra_filters=%7Benv%3D%22prod%22%7D&extra_label=t%3Dv");
        let etfs = get_extra_tag_filters(&p).unwrap();
        assert_eq!(etfs.len(), 1);
        assert_eq!(etfs[0].len(), 2); // env filter + t=v appended
        assert_eq!(etfs[0][1].label, "t");

        assert!(get_extra_tag_filters(&params("extra_label=broken")).is_err());
    }

    #[test]
    fn join_filterss_cross_product() {
        let mk = |label: &str| LabelFilter {
            label: label.to_string(),
            value: "v".to_string(),
            is_negative: false,
            is_regexp: false,
        };
        let src = vec![vec![mk("a")], vec![mk("b")]];
        let etfs = vec![vec![mk("x")]];
        let joined = join_tag_filterss(src, &etfs);
        assert_eq!(joined.len(), 2);
        assert_eq!(joined[0].len(), 2);
        assert_eq!(joined[1][1].label, "x");
    }

    #[test]
    fn unescape_label_names() {
        assert_eq!(unescape_prometheus_label_name("plain"), "plain");
        assert_eq!(unescape_prometheus_label_name("U__foo__bar"), "foo_bar");
        assert_eq!(unescape_prometheus_label_name("U__f_6f_o"), "foo");
        // Invalid escapes fall back to the original name.
        assert_eq!(unescape_prometheus_label_name("U__f_"), "U__f_");
        assert_eq!(unescape_prometheus_label_name("U__f_zz_"), "U__f_zz_");
    }
}
