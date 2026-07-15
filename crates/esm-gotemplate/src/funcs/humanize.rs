//! Numeric humanize builtin functions.
//!
//! Reference: `app/vmalert/templates/template.go`'s `/* Numbers */` section
//! (upstream VictoriaMetrics vmalert): `humanize`, `humanizeDuration`,
//! `humanizePercentage`. The time-related builtins from the same file
//! (`humanizeTimestamp`, `toTime`, `formatTime`, `parseDuration`,
//! `parseDurationTime`, `now`) live in the `gotime` submodule.

use std::collections::HashMap;

use crate::exec::FuncFn;
use crate::value::{format_float_go_g_prec, Value};
use crate::TemplateError;

mod gotime;

/// Registers the 10 humanize/time builtins into `m`.
pub fn register_humanize_funcs(m: &mut HashMap<String, FuncFn>) {
    m.insert(
        "humanize".to_string(),
        Box::new(|args| Ok(Value::Str(humanize(to_float64(args, "humanize")?)))),
    );
    m.insert(
        "humanize1024".to_string(),
        Box::new(|args| Ok(Value::Str(humanize1024(to_float64(args, "humanize1024")?)))),
    );
    m.insert(
        "humanizeDuration".to_string(),
        Box::new(|args| {
            Ok(Value::Str(humanize_duration(to_float64(
                args,
                "humanizeDuration",
            )?)))
        }),
    );
    m.insert(
        "humanizePercentage".to_string(),
        Box::new(|args| {
            Ok(Value::Str(humanize_percentage(to_float64(
                args,
                "humanizePercentage",
            )?)))
        }),
    );
    gotime::register_time_funcs(m);
}

/// Go: `toFloat64(i any) (float64, error)`, arity-checked for the common
/// single-argument builtins. Delegates the actual coercion to
/// [`to_float64_value`], which `gotime`'s two-argument `formatTime` also
/// uses directly.
fn to_float64(args: &[Value], name: &str) -> Result<f64, TemplateError> {
    match args {
        [v] => to_float64_value(v, name),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 1 argument, got {}",
            args.len()
        ))),
    }
}

/// Go: `toFloat64(i any) (float64, error)`. Numeric `Value`s convert
/// directly; `Value::Str` parses as a Go float literal
/// (`strconv.ParseFloat`); anything else is an error, never a panic.
fn to_float64_value(v: &Value, name: &str) -> Result<f64, TemplateError> {
    match v {
        Value::Float(f) => Ok(*f),
        Value::Int(i) => Ok(*i as f64),
        Value::Str(s) => s
            .parse::<f64>()
            .map_err(|e| TemplateError::new(format!("{name}: {e}"))),
        other => Err(TemplateError::new(format!(
            "{name}: unexpected value type {other:?}"
        ))),
    }
}

/// Go's `humanize`: adds a metric (SI) prefix to `v`, formatted with
/// `%.4g`. `0`/`NaN`/`Inf` skip prefix scaling entirely, matching
/// upstream's explicit special case.
fn humanize(v: f64) -> String {
    if v == 0.0 || v.is_nan() || v.is_infinite() {
        return format_float_go_g_prec(v, Some(4));
    }
    if v.abs() >= 1.0 {
        let mut v = v;
        let mut prefix = "";
        for p in ["k", "M", "G", "T", "P", "E", "Z", "Y"] {
            if v.abs() < 1000.0 {
                break;
            }
            prefix = p;
            v /= 1000.0;
        }
        return format!("{}{prefix}", format_float_go_g_prec(v, Some(4)));
    }
    let mut v = v;
    let mut prefix = "";
    for p in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if v.abs() >= 1.0 {
            break;
        }
        prefix = p;
        v *= 1000.0;
    }
    format!("{}{prefix}", format_float_go_g_prec(v, Some(4)))
}

/// Go's `humanize1024`: human-readable byte size with 1024 as base, backed by
/// `formatutil.HumanizeBytes`. Values with `|v| <= 1` (and `NaN`/`Inf`) skip
/// prefix scaling and format as `%.4g`; otherwise the value is divided down
/// through the binary prefixes `ki, Mi, Gi, Ti, Pi, Ei, Zi, Yi` (upstream's
/// exact, lowercase-`k` spelling) and formatted `%.4g` with the prefix
/// appended. Reference: vmalert `app/vmalert/templates/template.go` and
/// `lib/formatutil/human.go`.
fn humanize1024(v: f64) -> String {
    if v.abs() <= 1.0 || v.is_nan() || v.is_infinite() {
        return format_float_go_g_prec(v, Some(4));
    }
    let mut size = v;
    let mut prefix = "";
    for p in ["ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"] {
        if size.abs() < 1024.0 {
            break;
        }
        prefix = p;
        size /= 1024.0;
    }
    format!("{}{prefix}", format_float_go_g_prec(size, Some(4)))
}

/// Go's `humanizeDuration`: renders `v` seconds as a human-readable
/// duration. Ported branch-for-branch from upstream (days/hours/minutes
/// use integer components; sub-minute and sub-second values fall back to
/// `%.4g`-based formatting).
fn humanize_duration(v: f64) -> String {
    if v.is_nan() || v.is_infinite() {
        return format_float_go_g_prec(v, Some(4));
    }
    if v == 0.0 {
        return format!("{}s", format_float_go_g_prec(v, Some(4)));
    }
    if v.abs() >= 1.0 {
        let (sign, v) = if v < 0.0 { ("-", -v) } else { ("", v) };
        let iv = v as i64;
        let seconds = iv % 60;
        let minutes = (iv / 60) % 60;
        let hours = (iv / 60 / 60) % 24;
        let days = iv / 60 / 60 / 24;
        if days != 0 {
            return format!("{sign}{days}d {hours}h {minutes}m {seconds}s");
        }
        if hours != 0 {
            return format!("{sign}{hours}h {minutes}m {seconds}s");
        }
        if minutes != 0 {
            return format!("{sign}{minutes}m {seconds}s");
        }
        return format!("{sign}{}s", format_float_go_g_prec(v, Some(4)));
    }
    let mut v = v;
    let mut prefix = "";
    for p in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if v.abs() >= 1.0 {
            break;
        }
        prefix = p;
        v *= 1000.0;
    }
    format!("{}{prefix}s", format_float_go_g_prec(v, Some(4)))
}

/// Go's `humanizePercentage`: `v` is a ratio (e.g. `0.1234`), rendered as a
/// percentage (`"12.34%"`).
fn humanize_percentage(v: f64) -> String {
    format!("{}%", format_float_go_g_prec(v * 100.0, Some(4)))
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
        register_humanize_funcs(&mut m);
        m
    }

    #[test]
    fn humanize_matches_go_4g() {
        let m = setup();
        assert_eq!(
            call(&m, "humanize", &[Value::Float(1234.0)]).render_string(),
            "1.234k"
        );
        assert_eq!(
            call(&m, "humanize", &[Value::Float(0.0)]).render_string(),
            "0"
        );
        assert_eq!(
            call(&m, "humanizePercentage", &[Value::Float(0.1234)]).render_string(),
            "12.34%"
        );
    }

    #[test]
    fn humanize1024_uses_binary_prefixes() {
        let m = setup();
        // 2048 = 2 * 1024 -> "2ki" (upstream's lowercase-k spelling).
        assert_eq!(
            call(&m, "humanize1024", &[Value::Float(2048.0)]).render_string(),
            "2ki"
        );
        // 1536 = 1.5 * 1024 -> "1.5ki".
        assert_eq!(
            call(&m, "humanize1024", &[Value::Float(1536.0)]).render_string(),
            "1.5ki"
        );
        // 1048576 = 1024^2 -> "1Mi".
        assert_eq!(
            call(&m, "humanize1024", &[Value::Float(1_048_576.0)]).render_string(),
            "1Mi"
        );
        // |v| <= 1 skips scaling and formats %.4g.
        assert_eq!(
            call(&m, "humanize1024", &[Value::Float(0.5)]).render_string(),
            "0.5"
        );
        // sub-1024 magnitude stays unscaled.
        assert_eq!(
            call(&m, "humanize1024", &[Value::Float(512.0)]).render_string(),
            "512"
        );
    }

    #[test]
    fn humanize_scales_down_for_small_values() {
        let m = setup();
        assert_eq!(
            call(&m, "humanize", &[Value::Float(0.001234)]).render_string(),
            "1.234m"
        );
    }

    #[test]
    fn humanize_leaves_nan_and_inf_unscaled() {
        let m = setup();
        assert_eq!(
            call(&m, "humanize", &[Value::Float(f64::NAN)]).render_string(),
            "NaN"
        );
        assert_eq!(
            call(&m, "humanize", &[Value::Float(f64::INFINITY)]).render_string(),
            "+Inf"
        );
    }

    #[test]
    fn humanize_accepts_string_and_int_args() {
        let m = setup();
        assert_eq!(
            call(&m, "humanize", &[Value::Str("1234".into())]).render_string(),
            "1.234k"
        );
        assert_eq!(
            call(&m, "humanize", &[Value::Int(1234)]).render_string(),
            "1.234k"
        );
    }

    #[test]
    fn humanize_duration_renders_days_hours_minutes_seconds() {
        let m = setup();
        assert_eq!(
            call(&m, "humanizeDuration", &[Value::Float(0.0)]).render_string(),
            "0s"
        );
        assert_eq!(
            call(&m, "humanizeDuration", &[Value::Float(90.0)]).render_string(),
            "1m 30s"
        );
        assert_eq!(
            call(&m, "humanizeDuration", &[Value::Float(3661.0)]).render_string(),
            "1h 1m 1s"
        );
        assert_eq!(
            call(&m, "humanizeDuration", &[Value::Float(100_000.0)]).render_string(),
            "1d 3h 46m 40s"
        );
        assert_eq!(
            call(&m, "humanizeDuration", &[Value::Float(-90.0)]).render_string(),
            "-1m 30s"
        );
    }

    #[test]
    fn humanize_duration_renders_sub_second_and_sub_minute_values() {
        let m = setup();
        assert_eq!(
            call(&m, "humanizeDuration", &[Value::Float(0.5)]).render_string(),
            "500ms"
        );
        assert_eq!(
            call(&m, "humanizeDuration", &[Value::Float(45.0)]).render_string(),
            "45s"
        );
    }

    #[test]
    fn humanize_percentage_handles_negative_and_over_100() {
        let m = setup();
        assert_eq!(
            call(&m, "humanizePercentage", &[Value::Float(-0.5)]).render_string(),
            "-50%"
        );
        assert_eq!(
            call(&m, "humanizePercentage", &[Value::Float(1.5)]).render_string(),
            "150%"
        );
    }

    #[test]
    fn wrong_arity_or_type_is_a_template_error_not_a_panic() {
        let m = setup();
        let err = call_err(&m, "humanize", &[]);
        assert!(err.msg.contains("humanize"), "got: {}", err.msg);
        let err = call_err(&m, "humanize", &[Value::Str("not-a-number".into())]);
        assert!(err.msg.contains("humanize"), "got: {}", err.msg);
        let err = call_err(&m, "humanizeDuration", &[Value::Bool(true)]);
        assert!(err.msg.contains("humanizeDuration"), "got: {}", err.msg);
    }
}
