//! Time-related humanize/time builtins: `humanizeTimestamp`, `toTime`,
//! `formatTime`, `parseDuration`, `parseDurationTime`, `now`.
//!
//! Reference: `app/vmalert/templates/template.go` (upstream VictoriaMetrics
//! vmalert), `lib/timeutil/time.go`'s `Time`/`timeFromUnixTimestamp`
//! (millisecond-precision `time.Time` conversion used by
//! `humanizeTimestamp`/`toTime`/`formatTime`), and Go stdlib
//! `time.Time.String()`/`time.Time.Format()` for the calendar/layout
//! rendering this module ports.
//!
//! `parseDuration`/`parseDurationTime` delegate to
//! `esm_metricsql::duration_value` — this repo's port of
//! `metricsql.DurationValue`, which is exactly what vmalert's upstream
//! `timeutil.ParseDuration` calls. That is the Prometheus/VictoriaMetrics
//! duration grammar: `y`/`w`/`d`/`h`/`m`/`s`/`ms` suffixes, a bare number
//! interpreted as seconds, and compound forms (`1h30m`). The one remaining
//! documented limitation is representational, not grammatical: this crate's
//! `Value` has no distinct duration type and its executor has no
//! method-call dispatch, so `parseDurationTime` returns a seconds
//! `Value::Float` like `parseDuration` rather than a Go `time.Duration`.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::exec::FuncFn;
use crate::value::{format_float_go_g_prec, Value};
use crate::TemplateError;

use super::to_float64_value;

pub(super) fn register_time_funcs(m: &mut HashMap<String, FuncFn>) {
    m.insert(
        "humanizeTimestamp".to_string(),
        Box::new(|args| {
            let v = one_float64(args, "humanizeTimestamp")?;
            if v.is_nan() || v.is_infinite() {
                return Ok(Value::Str(format_float_go_g_prec(v, Some(4))));
            }
            let (secs, ms) = time_from_unix_timestamp(v);
            Ok(Value::Str(format_go_time_default(secs, ms)))
        }),
    );
    m.insert(
        "toTime".to_string(),
        Box::new(|args| {
            let v = one_float64(args, "toTime")?;
            if v.is_nan() || v.is_infinite() {
                return Err(TemplateError::new(format!(
                    "toTime: cannot convert {} to time.Time",
                    format_float_go_g_prec(v, None)
                )));
            }
            let (secs, ms) = time_from_unix_timestamp(v);
            // Go returns a `time.Time`, usable for further method calls
            // (`.Sub`, `.Format`, ...) that this crate's executor doesn't
            // support at all (`exec.rs` has no method-call dispatch, only
            // struct-like field access). A `Value::Str` of the same
            // default rendering `humanizeTimestamp` produces is the
            // closest useful analog for bare interpolation
            // (`{{ now | toTime }}`); this is a documented fidelity gap,
            // not a hidden one.
            Ok(Value::Str(format_go_time_default(secs, ms)))
        }),
    );
    m.insert(
        "formatTime".to_string(),
        Box::new(|args| {
            let (layout, v) = layout_and_float64(args, "formatTime")?;
            if v.is_nan() || v.is_infinite() {
                return Err(TemplateError::new(format!(
                    "formatTime: cannot convert {} to time",
                    format_float_go_g_prec(v, None)
                )));
            }
            let (secs, ms) = time_from_unix_timestamp(v);
            Ok(Value::Str(go_time_format(secs, ms, &layout)))
        }),
    );
    m.insert(
        "parseDuration".to_string(),
        Box::new(|args| {
            let s = one_str(args, "parseDuration")?;
            Ok(Value::Float(parse_duration_seconds(&s, "parseDuration")?))
        }),
    );
    m.insert(
        "parseDurationTime".to_string(),
        Box::new(|args| {
            let s = one_str(args, "parseDurationTime")?;
            // Go returns a `time.Duration`; this crate's `Value` has no
            // distinct duration type (see module docs above), so the
            // seconds count is carried as a `Value::Float`, same as
            // `parseDuration`. Bare interpolation therefore renders e.g.
            // "5400" rather than Go's `Duration.String()` output
            // "1h30m0s" — a documented fidelity gap.
            Ok(Value::Float(parse_duration_seconds(
                &s,
                "parseDurationTime",
            )?))
        }),
    );
    m.insert(
        "now".to_string(),
        Box::new(|args| {
            if !args.is_empty() {
                return Err(TemplateError::new(format!(
                    "now: expected 0 arguments, got {}",
                    args.len()
                )));
            }
            // Go: `float64(time.Now().Unix())` — whole seconds only, no
            // sub-second fraction.
            let secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            Ok(Value::Float(secs as f64))
        }),
    );
}

fn one_float64(args: &[Value], name: &str) -> Result<f64, TemplateError> {
    match args {
        [v] => to_float64_value(v, name),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 1 argument, got {}",
            args.len()
        ))),
    }
}

fn one_str(args: &[Value], name: &str) -> Result<String, TemplateError> {
    match args {
        [v] => Ok(v.render_string()),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 1 argument, got {}",
            args.len()
        ))),
    }
}

fn layout_and_float64(args: &[Value], name: &str) -> Result<(String, f64), TemplateError> {
    match args {
        [layout, v] => Ok((layout.render_string(), to_float64_value(v, name)?)),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 2 arguments, got {}",
            args.len()
        ))),
    }
}

/// Go: `timeFromUnixTimestamp(v).Time()`. `Time` is milliseconds since the
/// epoch (`Time(t * 1e3)`, truncating like Go's `float64`-to-`int64`
/// conversion); `.Time()` reconstructs `(seconds, nanoseconds)` via
/// `time.Unix`, which normalizes the nanosecond remainder into `[0, 1e9)`
/// regardless of sign. Returns `(whole_seconds, millis_0_999)` — the same
/// normalized instant, expressed in whole seconds plus a millisecond
/// remainder in `[0, 999]` (this crate's callers only need up-to-3-digit
/// sub-second precision, matching this port's millisecond granularity).
fn time_from_unix_timestamp(v: f64) -> (i64, i64) {
    let millis = (v * 1e3) as i64; // Go: `int64` conversion truncates toward zero.
    (millis.div_euclid(1000), millis.rem_euclid(1000))
}

/// Converts days since 1970-01-01 to `(year, month, day)`.
///
/// Duplicated (not shared as a dependency, per this repo's convention —
/// see `esm-common::logger::civil_from_days` and
/// `esm-backup::timeutil::civil_from_unix`, neither `pub`) from Howard
/// Hinnant's `civil_from_days` algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    let year = yoe as i64 + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// `(year, month, day, hour, minute, second)` for `secs` seconds since the
/// Unix epoch (UTC, proleptic Gregorian, no leap seconds — same model as
/// Go's `time` package).
fn civil_from_unix(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400); // [0, 86399]
    let (year, month, day) = civil_from_days(days);
    let hour = (sod / 3600) as u32;
    let minute = ((sod % 3600) / 60) as u32;
    let second = (sod % 60) as u32;
    (year, month, day, hour, minute, second)
}

/// Sunday = 0, ..., Saturday = 6 (Go's `time.Weekday`), for `secs` seconds
/// since the Unix epoch. 1970-01-01 (`days = 0`) was a Thursday (index 4).
fn weekday_from_unix(secs: i64) -> usize {
    let days = secs.div_euclid(86_400);
    ((days.rem_euclid(7) + 4) % 7) as usize
}

const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const WEEKDAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

/// Go's default `time.Time.String()` for a UTC instant: the layout
/// `"2006-01-02 15:04:05.999999999 -0700 MST"`, always with a `+0000 UTC`
/// suffix since `humanizeTimestamp`/`toTime` always operate on `.UTC()`.
fn format_go_time_default(secs: i64, ms: i64) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(secs);
    let mut out = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
    if let Some(frac) = trimmed_fraction(ms, 9) {
        out.push('.');
        out.push_str(&frac);
    }
    out.push_str(" +0000 UTC");
    out
}

/// The `.999999999`-style trimmed fractional-seconds digits (up to
/// `max_digits`, trailing zeros removed), or `None` if the whole fraction
/// is zero (Go omits the fraction entirely in that case).
fn trimmed_fraction(ms: i64, max_digits: usize) -> Option<String> {
    let ns = ms * 1_000_000;
    let digits = format!("{ns:09}");
    let mut digits: String = digits.chars().take(max_digits).collect();
    while digits.ends_with('0') {
        digits.pop();
    }
    if digits.is_empty() {
        None
    } else {
        Some(digits)
    }
}

/// The `.000000000`-style fixed-width fractional-seconds digits (exactly
/// `digit_count` digits, zero-padded, never omitted).
fn fixed_fraction(ms: i64, digit_count: usize) -> String {
    let ns = ms * 1_000_000;
    format!("{ns:09}").chars().take(digit_count).collect()
}

/// Go's `time.Time.Format(layout)`, restricted to `secs`/`ms` always being
/// a UTC instant (matching this crate's only caller, `formatTime`, which
/// never carries a non-UTC offset). Scans `layout` for Go's reference-time
/// tokens (`"2006-01-02T15:04:05Z07:00"` etc.) longest-match-first,
/// copying any other byte through literally.
fn go_time_format(secs: i64, ms: i64, layout: &str) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(secs);
    let hour12 = match hour % 12 {
        0 => 12,
        h => h,
    };
    let weekday = WEEKDAY_NAMES[weekday_from_unix(secs)];
    let mut out = String::with_capacity(layout.len());
    let mut rest = layout;
    while !rest.is_empty() {
        if let Some((piece, consumed)) = match_std_chunk(rest, |token| match token {
            "2006" => format!("{year:04}"),
            "06" => format!("{:02}", year.rem_euclid(100)),
            "January" => MONTH_NAMES[(month - 1) as usize].to_string(),
            "Jan" => MONTH_NAMES[(month - 1) as usize][..3].to_string(),
            "01" => format!("{month:02}"),
            "1" => month.to_string(),
            "Monday" => weekday.to_string(),
            "Mon" => weekday[..3].to_string(),
            "02" => format!("{day:02}"),
            "_2" => format!("{day:2}"),
            "2" => day.to_string(),
            "15" => format!("{hour:02}"),
            "03" => format!("{hour12:02}"),
            "3" => hour12.to_string(),
            "04" => format!("{minute:02}"),
            "4" => minute.to_string(),
            "05" => format!("{second:02}"),
            "5" => second.to_string(),
            "PM" => (if hour < 12 { "AM" } else { "PM" }).to_string(),
            "pm" => (if hour < 12 { "am" } else { "pm" }).to_string(),
            "Z07:00" | "Z0700" | "Z07" => "Z".to_string(),
            "-07:00" => "+00:00".to_string(),
            "-0700" => "+0000".to_string(),
            "-07" => "+00".to_string(),
            "MST" => "UTC".to_string(),
            fixed if fixed.starts_with('.') && fixed.bytes().all(|b| b == b'.' || b == b'0') => {
                format!(".{}", fixed_fraction(ms, fixed.len() - 1))
            }
            trimmed
                if trimmed.starts_with('.') && trimmed.bytes().all(|b| b == b'.' || b == b'9') =>
            {
                match trimmed_fraction(ms, trimmed.len() - 1) {
                    Some(digits) => format!(".{digits}"),
                    None => String::new(),
                }
            }
            other => other.to_string(),
        }) {
            out.push_str(&piece);
            rest = &rest[consumed..];
        } else {
            let ch_len = rest.chars().next().map(char::len_utf8).unwrap_or(1);
            out.push_str(&rest[..ch_len]);
            rest = &rest[ch_len..];
        }
    }
    out
}

/// Reference-layout tokens, sorted strictly by descending byte length so
/// that whenever one token is a prefix of another (`"1"` of `"15"`,
/// `"2"` of `"2006"`, `"Mon"` of `"Monday"`, `"Z07"` of `"Z07:00"`, ...)
/// the longer one is always tried first. No two tokens of the same length
/// are prefixes of each other, so ties within a length group are order
/// -independent.
const STD_CHUNKS: &[&str] = &[
    // len 10
    ".000000000",
    ".999999999",
    // len 9
    ".00000000",
    ".99999999",
    // len 8
    ".0000000",
    ".9999999",
    // len 7
    "January",
    ".000000",
    ".999999",
    // len 6
    "Monday",
    "Z07:00",
    "-07:00",
    ".00000",
    ".99999",
    // len 5
    "Z0700",
    "-0700",
    ".0000",
    ".9999",
    // len 4
    "2006",
    ".000",
    ".999",
    // len 3
    "Jan",
    "Mon",
    "MST",
    "Z07",
    "-07",
    ".00",
    ".99",
    // len 2
    "01",
    "06",
    "02",
    "_2",
    "15",
    "04",
    "05",
    "PM",
    "pm",
    ".0",
    ".9",
    // len 1
    "2",
    "1",
    "3",
    "4",
    "5",
];

/// Finds the first `STD_CHUNKS` entry that prefixes `rest`, returning the
/// rendered replacement and how many bytes of `rest` it consumed. `None`
/// if `rest` doesn't start with any known token (the caller then copies
/// one literal character through unchanged, matching Go's `AppendFormat`
/// behavior for non-token bytes).
fn match_std_chunk(rest: &str, render: impl Fn(&str) -> String) -> Option<(String, usize)> {
    STD_CHUNKS
        .iter()
        .find(|token| rest.starts_with(*token))
        .map(|token| (render(token), token.len()))
}

/// Parses a Prometheus/VictoriaMetrics duration string into seconds via
/// `esm_metricsql::duration_value` (this repo's port of
/// `metricsql.DurationValue`, which returns milliseconds); `name` prefixes
/// the `TemplateError` on failure. Upstream vmalert's
/// `parseDuration`/`parseDurationTime` compute `d.Seconds()` on the parsed
/// `time.Duration`, i.e. milliseconds / 1000 — matched here exactly.
fn parse_duration_seconds(s: &str, name: &str) -> Result<f64, TemplateError> {
    let ms = esm_metricsql::duration_value(s, 0)
        .map_err(|e| TemplateError::new(format!("{name}: {e}")))?;
    Ok(ms as f64 / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(m: &HashMap<String, FuncFn>, name: &str, args: &[Value]) -> Value {
        m.get(name)
            .unwrap_or_else(|| panic!("{name} not registered"))(args)
        .unwrap_or_else(|e| panic!("{name} call failed: {e}"))
    }

    fn call_err(m: &HashMap<String, FuncFn>, name: &str, args: &[Value]) -> TemplateError {
        m.get(name)
            .unwrap_or_else(|| panic!("{name} not registered"))(args)
        .expect_err("expected an error")
    }

    fn setup() -> HashMap<String, FuncFn> {
        let mut m = HashMap::new();
        register_time_funcs(&mut m);
        m
    }

    #[test]
    fn std_chunks_are_sorted_by_descending_length() {
        // Guards the longest-prefix-first invariant `match_std_chunk`
        // relies on: a shorter token (e.g. "1") must never be checked
        // before a longer token it prefixes (e.g. "15").
        let lens: Vec<usize> = STD_CHUNKS.iter().map(|s| s.len()).collect();
        assert!(
            lens.windows(2).all(|w| w[0] >= w[1]),
            "STD_CHUNKS is not sorted by descending length: {lens:?}"
        );
    }

    #[test]
    fn humanize_timestamp_matches_go_default_time_string() {
        let m = setup();
        assert_eq!(
            call(&m, "humanizeTimestamp", &[Value::Float(0.0)]).render_string(),
            "1970-01-01 00:00:00 +0000 UTC"
        );
        // 2009-11-10 23:00:00 UTC, the well-known Go playground default time.
        assert_eq!(
            call(&m, "humanizeTimestamp", &[Value::Float(1_257_894_000.0)]).render_string(),
            "2009-11-10 23:00:00 +0000 UTC"
        );
    }

    #[test]
    fn humanize_timestamp_shows_trimmed_fractional_seconds() {
        let m = setup();
        assert_eq!(
            call(&m, "humanizeTimestamp", &[Value::Float(1_257_894_000.5)]).render_string(),
            "2009-11-10 23:00:00.5 +0000 UTC"
        );
    }

    #[test]
    fn humanize_timestamp_leaves_nan_and_inf_unformatted() {
        let m = setup();
        assert_eq!(
            call(&m, "humanizeTimestamp", &[Value::Float(f64::NAN)]).render_string(),
            "NaN"
        );
    }

    #[test]
    fn to_time_renders_like_humanize_timestamp() {
        let m = setup();
        assert_eq!(
            call(&m, "toTime", &[Value::Float(0.0)]).render_string(),
            "1970-01-01 00:00:00 +0000 UTC"
        );
    }

    #[test]
    fn to_time_rejects_nan_as_a_template_error() {
        let m = setup();
        let err = call_err(&m, "toTime", &[Value::Float(f64::NAN)]);
        assert!(err.msg.contains("toTime"), "got: {}", err.msg);
    }

    #[test]
    fn format_time_renders_epoch_zero_as_rfc3339() {
        let m = setup();
        assert_eq!(
            call(
                &m,
                "formatTime",
                &[
                    Value::Str("2006-01-02T15:04:05Z07:00".into()),
                    Value::Float(0.0)
                ]
            )
            .render_string(),
            "1970-01-01T00:00:00Z"
        );
    }

    #[test]
    fn format_time_renders_known_timestamp_as_rfc3339() {
        let m = setup();
        assert_eq!(
            call(
                &m,
                "formatTime",
                &[
                    Value::Str("2006-01-02T15:04:05Z07:00".into()),
                    Value::Float(1_257_894_000.0)
                ]
            )
            .render_string(),
            "2009-11-10T23:00:00Z"
        );
    }

    #[test]
    fn format_time_renders_names_and_12_hour_clock() {
        let m = setup();
        // 2009-11-10 23:00:00 UTC was a Tuesday.
        assert_eq!(
            call(
                &m,
                "formatTime",
                &[
                    Value::Str("Monday, Jan 2, 2006 3:04PM".into()),
                    Value::Float(1_257_894_000.0)
                ]
            )
            .render_string(),
            "Tuesday, Nov 10, 2009 11:00PM"
        );
    }

    #[test]
    fn format_time_rejects_wrong_arity() {
        let m = setup();
        let err = call_err(&m, "formatTime", &[Value::Str("2006".into())]);
        assert!(err.msg.contains("formatTime"), "got: {}", err.msg);
    }

    #[test]
    fn probe_duration_value_bare_number() {
        // Documents the exact `duration_value` behavior the assertions
        // below rely on: a bare number is parsed as seconds and returned
        // in milliseconds (Prometheus/VM grammar), so 30 -> 30000 ms.
        assert_eq!(esm_metricsql::duration_value("30", 0).unwrap(), 30_000);
    }

    #[test]
    fn parse_duration_parses_prometheus_vm_grammar() {
        let m = setup();
        // Stdlib-style units still parse.
        assert_eq!(
            call(&m, "parseDuration", &[Value::Str("5m".into())]).render_string(),
            "300"
        );
        assert_eq!(
            call(&m, "parseDuration", &[Value::Str("1h30m".into())]).render_string(),
            "5400"
        );
        assert_eq!(
            call(&m, "parseDuration", &[Value::Str("1.5h".into())]).render_string(),
            "5400"
        );
        assert_eq!(
            call(&m, "parseDuration", &[Value::Str("0".into())]).render_string(),
            "0"
        );
        // Prometheus/VM-only suffixes now work (regression: "1d"/"1w"
        // previously failed under the stdlib-only parser).
        assert_eq!(
            call(&m, "parseDuration", &[Value::Str("1d".into())]).render_string(),
            "86400"
        );
        assert_eq!(
            call(&m, "parseDuration", &[Value::Str("1w".into())]).render_string(),
            "604800"
        );
        // Bare number is interpreted as seconds.
        assert_eq!(
            call(&m, "parseDuration", &[Value::Str("30".into())]).render_string(),
            "30"
        );
    }

    #[test]
    fn parse_duration_reports_bad_input_as_template_error() {
        let m = setup();
        let err = call_err(&m, "parseDuration", &[Value::Str("banana".into())]);
        assert!(err.msg.contains("parseDuration"), "got: {}", err.msg);
    }

    #[test]
    fn parse_duration_time_parses_the_same_as_parse_duration() {
        let m = setup();
        assert_eq!(
            call(&m, "parseDurationTime", &[Value::Str("1h30m".into())]).render_string(),
            "5400"
        );
    }

    #[test]
    fn now_returns_a_plausible_current_unix_timestamp() {
        let m = setup();
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as f64;
        let got = match call(&m, "now", &[]) {
            Value::Float(f) => f,
            other => panic!("expected Value::Float, got {other:?}"),
        };
        assert!((before..=before + 5.0).contains(&got), "now() = {got}");
    }

    #[test]
    fn now_rejects_arguments() {
        let m = setup();
        let err = call_err(&m, "now", &[Value::Int(1)]);
        assert!(err.msg.contains("now"), "got: {}", err.msg);
    }
}
