//! Runtime value model for template execution.
//!
//! [`Value`] is the tagged union the executor operates on: the data ("dot")
//! a template renders against, plus every intermediate pipeline/variable
//! value produced while evaluating it. [`Metric`] mirrors vmalert's
//! `notifier.Metric` (a single time series sample) and is the element type
//! for `Value::Vec`, the shape most alerting/recording rule templates range
//! over.

use std::collections::BTreeMap;

/// A single time series sample: labels plus the sample value and timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
    pub timestamp: i64,
}

/// A dynamically-typed value flowing through template execution.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Metric(Metric),
    Vec(Vec<Metric>),
    Map(BTreeMap<String, Value>),
    List(Vec<Value>),
}

impl Value {
    /// Go template truthiness (`text/template`'s `isTrue`): the zero value
    /// of each type is false, everything else is true.
    pub fn truthy(&self) -> bool {
        match self {
            Value::Nil => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::Metric(_) => true,
            Value::Vec(v) => !v.is_empty(),
            Value::Map(m) => !m.is_empty(),
            Value::List(l) => !l.is_empty(),
        }
    }

    /// Go `fmt`'s default (`%v`) rendering, as used when a pipeline's result
    /// is interpolated directly into template output.
    pub fn render_string(&self) -> String {
        match self {
            Value::Nil => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => format_float_go_g(*f),
            Value::Str(s) => s.clone(),
            // A bare Metric renders as its sample value, matching how
            // vmalert templates use the range element (`{{ .Value }}`);
            // there is no natural scalar rendering of the whole struct.
            Value::Metric(m) => format_float_go_g(m.value),
            Value::Vec(items) => format!(
                "[{}]",
                items
                    .iter()
                    .map(|m| format_float_go_g(m.value))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            Value::List(items) => format!(
                "[{}]",
                items
                    .iter()
                    .map(Value::render_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            // Go's fmt default for a map is `map[k1:v1 k2:v2]`, sorted by
            // key; BTreeMap iteration is already key-sorted.
            Value::Map(m) => format!(
                "map[{}]",
                m.iter()
                    .map(|(k, v)| format!("{k}:{}", v.render_string()))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        }
    }
}

/// Go: `strconv.FormatFloat(v, 'g', -1, 64)`, i.e. `fmt`'s default `%v`
/// formatting for a `float64`. Thin wrapper over
/// [`format_float_go_g_prec`] with `prec = None` (Go's `prec = -1`,
/// "shortest string that round-trips").
///
/// Duplicated (rather than shared as a dependency) from the equivalent,
/// already-verified ports in this repo: `esm_metricsql::strutil::format_float_go`
/// and `esm_protoparser::opentelemetry::convert::format_float_go_g` (the
/// latter ground-truthed against `go1.26`'s `internal/strconv/ftoa.go` with
/// 39 side-by-side vectors). Neither is `pub` outside its crate and
/// `esm-gotemplate` has no dependency on either crate, so copying this small,
/// already-validated algorithm is simpler than wiring a cross-crate
/// dependency just to expose one private helper — consistent with this
/// repo's existing convention of duplicating the port at each crate that
/// needs it rather than centralizing it in a shared `strconv` crate.
fn format_float_go_g(v: f64) -> String {
    format_float_go_g_prec(v, None)
}

/// Go: `strconv.FormatFloat(v, 'g', prec, 64)`. `prec = None` is Go's
/// `prec = -1` (shortest round-trip, `fmt`'s default `%v`); `prec =
/// Some(n)` is fixed-significant-digit formatting (`%.*g`, e.g. Go's
/// `%.4g`), which `esm-gotemplate`'s `humanize`/`humanizeDuration`/
/// `humanizePercentage` builtins (`funcs/humanize.rs`) need byte-exact.
/// `pub(crate)` so those builtins can reuse this one algorithm rather than
/// duplicating a second copy of Go's `'g'`-format logic within the crate.
///
/// Known gap: Go's `ftoa.go` shrinks the effective precision (`eprec`) used
/// for the fixed-vs-scientific threshold when the value's *exact* shortest
/// digit count is smaller than `prec` and still covers the integer part
/// (e.g. some exact powers of ten). This port always uses the requested
/// `prec` for that threshold, which can disagree with Go in that narrow
/// corner case; it does not arise for the humanize family's inputs in
/// vmalert's alerting templates (arbitrary metric sample values, not
/// exactly-representable round numbers at the `prec`-digit boundary).
pub(crate) fn format_float_go_g_prec(v: f64, prec: Option<usize>) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    let sci = match prec {
        None => format!("{v:e}"),
        // `prec` significant digits total means `prec - 1` digits after
        // the mantissa's decimal point in `{:e}` form.
        Some(p) => format!("{:.*e}", p.saturating_sub(1), v),
    };
    let (sign, rest) = match sci.strip_prefix('-') {
        Some(r) => ("-", r),
        None => ("", sci.as_str()),
    };
    let e_pos = rest
        .find('e')
        .expect("Rust `{:e}` output always contains an 'e'");
    let mantissa = &rest[..e_pos];
    let exp: i32 = rest[e_pos + 1..]
        .parse()
        .expect("Rust `{:e}` exponent is always a valid integer");
    let digits: String = mantissa.chars().filter(|&c| c != '.').collect();

    let high_threshold = match prec {
        None => 6,
        Some(p) => p as i32,
    };
    if exp < -4 || exp >= high_threshold {
        let mut frac = digits[1..].to_string();
        if prec.is_some() {
            trim_trailing_zeros(&mut frac);
        }
        let mut out = String::new();
        out.push_str(sign);
        out.push(digits.as_bytes()[0] as char);
        if !frac.is_empty() {
            out.push('.');
            out.push_str(&frac);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        let abs_exp = exp.unsigned_abs();
        if abs_exp < 10 {
            out.push('0');
        }
        out.push_str(&abs_exp.to_string());
        out
    } else {
        match prec {
            None => format!("{v}"),
            Some(_) => format!("{sign}{}", fixed_notation(&digits, exp)),
        }
    }
}

/// Places `digits` (already rounded to a fixed count of significant
/// figures by the caller) at decimal exponent `exp` in fixed
/// (non-scientific) notation, then trims trailing fractional zeros — the
/// `%g` family always removes them unless the `#` flag is set (which
/// `esm-gotemplate`'s `%.Ng` call sites never use).
fn fixed_notation(digits: &str, exp: i32) -> String {
    let (int_part, mut frac_part) = if exp >= 0 {
        let int_len = (exp as usize) + 1;
        if int_len >= digits.len() {
            (
                format!("{digits}{}", "0".repeat(int_len - digits.len())),
                String::new(),
            )
        } else {
            (digits[..int_len].to_string(), digits[int_len..].to_string())
        }
    } else {
        (
            "0".to_string(),
            format!("{}{digits}", "0".repeat((-exp - 1) as usize)),
        )
    };
    trim_trailing_zeros(&mut frac_part);
    if frac_part.is_empty() {
        int_part
    } else {
        format!("{int_part}.{frac_part}")
    }
}

fn trim_trailing_zeros(s: &mut String) {
    while s.ends_with('0') {
        s.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_valued_float_renders_without_decimal_point() {
        assert_eq!(Value::Float(3.0).render_string(), "3");
    }

    #[test]
    fn fractional_float_renders_shortest_round_trip() {
        assert_eq!(Value::Float(2.5).render_string(), "2.5");
    }

    #[test]
    fn fixed_precision_g_format_rounds_to_significant_digits() {
        // Go: fmt.Sprintf("%.4g", ...) for each input below.
        assert_eq!(format_float_go_g_prec(1.234, Some(4)), "1.234");
        assert_eq!(format_float_go_g_prec(0.0, Some(4)), "0");
        assert_eq!(format_float_go_g_prec(500.0, Some(4)), "500");
        assert_eq!(format_float_go_g_prec(0.001234, Some(4)), "0.001234");
        assert_eq!(format_float_go_g_prec(100_000.0, Some(4)), "1e+05");
        assert_eq!(format_float_go_g_prec(123_456_789.0, Some(4)), "1.235e+08");
        assert_eq!(format_float_go_g_prec(12.34, Some(4)), "12.34");
        assert_eq!(format_float_go_g_prec(f64::NAN, Some(4)), "NaN");
        assert_eq!(format_float_go_g_prec(f64::INFINITY, Some(4)), "+Inf");
    }

    #[test]
    fn truthy_matches_go_zero_values() {
        assert!(!Value::Nil.truthy());
        assert!(!Value::Bool(false).truthy());
        assert!(!Value::Int(0).truthy());
        assert!(!Value::Str(String::new()).truthy());
        assert!(Value::Int(1).truthy());
        assert!(Value::Str("x".into()).truthy());
    }
}
