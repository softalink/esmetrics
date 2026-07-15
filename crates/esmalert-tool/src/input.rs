//! `input_series` value-sequence parser and series-selector expansion.
//!
//! Port of VictoriaMetrics `app/vmalert-tool/unittest/input.go:94-196`
//! (`parseInputValue`) plus the selector-parsing / sample-expansion glue
//! from `parseInputSeries` (same file, lines 59-92).

// Scaffold stage: this parser isn't wired into `main()` yet — the harness
// and runner that consume it land in later tasks.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;

use esm_common::decimal::STALE_NAN;
use esm_metricsql::Expr;

use crate::ToolError;

/// One point of a parsed value sequence.
///
/// Port of the upstream `sequenceValue` struct (`Value`/`Omitted` fields),
/// split here into an explicit `Stale` variant instead of upstream's
/// `Value == decimal.StaleNaN` encoding, and `Gap` instead of upstream's
/// `Omitted` bool.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SeqValue {
    Value(f64),
    Gap,
    Stale,
}

/// One expanded `(labels, timestamp, value)` sample produced by
/// [`expand_series`].
#[derive(Debug, Clone, PartialEq)]
pub struct InputSample {
    pub labels: Vec<(String, String)>,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// Port of Go's `numReg`:
/// `(?i)[+x-]?(?:\d+(?:\.\d*)?|\.\d+|inf|nan|_)(?:e[+-]?\d+)?[+x-]?`
/// (`unittest/input.go:21`).
fn num_reg() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)[+x-]?(?:\d+(?:\.\d*)?|\.\d+|inf|nan|_)(?:e[+-]?\d+)?[+x-]?")
            .expect("static numReg pattern is valid")
    })
}

/// Parses `input_series` value notation, e.g. `1+1x3`, `1 _ 3`, `stale`.
///
/// Port of Go `parseInputValue(input, true)` (`unittest/input.go:95`).
pub fn parse_input_value(input: &str) -> Result<Vec<SeqValue>, ToolError> {
    parse_input_value_impl(input, true)
}

/// Extracts the numeric value out of a [`SeqValue`], mirroring how upstream
/// `sequenceValue.Value` reads as `0.0` for an omitted point and as
/// `decimal.StaleNaN` for a stale one (both variants keep `Omitted == false`
/// and a concrete `Value`, so upstream's arithmetic in the "3 token" branch
/// of `parseInputValue` operates on it unconditionally instead of checking
/// `Omitted` first). Used only by the case-3 (`a<op>b<op>c`) branch below,
/// which is likewise unconditional in the Go source.
fn raw_value(v: &SeqValue) -> f64 {
    match v {
        SeqValue::Value(f) => *f,
        SeqValue::Gap => 0.0,
        SeqValue::Stale => STALE_NAN,
    }
}

fn parse_float(s: &str) -> Result<f64, ToolError> {
    s.parse::<f64>()
        .map_err(|e| ToolError::new(format!("invalid number {s:?}: {e}")))
}

/// Port of Go `parseInputValue(input, origin)` (`unittest/input.go:95-196`).
///
/// `origin` mirrors the Go parameter of the same name: it is `true` only for
/// the outermost call, and is threaded through to `false` for the recursive
/// calls upstream uses to re-expand `a op b x N` into per-point values.
fn parse_input_value_impl(input: &str, origin: bool) -> Result<Vec<SeqValue>, ToolError> {
    let items: Vec<&str> = input.split_whitespace().collect();
    if items.is_empty() {
        return Err(ToolError::new("values cannot be an empty string"));
    }
    let mut res = Vec::new();
    for item in items {
        if item == "stale" {
            res.push(SeqValue::Stale);
            continue;
        }
        if item.contains("stale") {
            return Err(ToolError::new("stale metric doesn't support operations"));
        }
        let vals: Vec<&str> = num_reg().find_iter(item).map(|m| m.as_str()).collect();
        match vals.len() {
            1 => parse_single_token(vals[0], &mut res)?,
            2 => parse_two_tokens(item, &vals, origin, &mut res)?,
            3 => parse_three_tokens(&vals, &mut res)?,
            _ => return Err(ToolError::new(format!("unsupported input {input}"))),
        }
    }
    Ok(res)
}

/// Case `len(vals) == 1`: a bare literal, `_` gap marker, or `inf`/`nan`.
/// Port of `unittest/input.go:110-121`.
fn parse_single_token(tok: &str, res: &mut Vec<SeqValue>) -> Result<(), ToolError> {
    if tok == "_" {
        res.push(SeqValue::Gap);
        return Ok(());
    }
    res.push(SeqValue::Value(parse_float(tok)?));
    Ok(())
}

/// Case `len(vals) == 2`: either `a+b` (plain addition) or `a x N` /
/// `a x N`-with-gap-base (repeat/expand). Port of `unittest/input.go:122-164`.
fn parse_two_tokens(
    item: &str,
    vals: &[&str],
    origin: bool,
    res: &mut Vec<SeqValue>,
) -> Result<(), ToolError> {
    let p1 = &vals[0][..vals[0].len() - 1];
    let v2: i64 = vals[1]
        .parse()
        .map_err(|e| ToolError::new(format!("invalid count in {item:?}: {e}")))?;
    let option = vals[0].as_bytes()[vals[0].len() - 1];
    match option {
        b'+' => {
            let v1 = parse_float(p1)?;
            res.push(SeqValue::Value(v1 + v2 as f64));
            Ok(())
        }
        b'x' => expand_x(p1, v2, vals[1], origin, res),
        _ => Err(ToolError::new(format!(
            "got invalid operation {}",
            option as char
        ))),
    }
}

/// The `for i := 0; i <= v2; i++ { ... }` loop of `unittest/input.go:137-160`,
/// transliterated with the same `continue`/`break` control flow (including
/// the `i = 1` jump on the gap branch, which is why `_x1` yields exactly one
/// gap instead of the two a naive "N+1 points" reading would suggest).
fn expand_x(
    p1: &str,
    v2: i64,
    count_str: &str,
    origin: bool,
    res: &mut Vec<SeqValue>,
) -> Result<(), ToolError> {
    let mut i: i64 = 0;
    while i <= v2 {
        if p1 == "_" {
            if i == 0 {
                i = 1;
            }
            res.push(SeqValue::Gap);
            i += 1;
            continue;
        }
        let v1 = parse_float(p1)?;
        if !origin || v1 == 0.0 {
            res.push(SeqValue::Value(v1 * i as f64));
            i += 1;
            continue;
        }
        let new_val = format!("{p1}+0x{count_str}");
        let new_res = parse_input_value_impl(&new_val, false)?;
        res.extend(new_res);
        break;
    }
    Ok(())
}

/// Case `len(vals) == 3`: `a<+|->b x N`, e.g. `1+1x3` or `5-1x2`.
/// Port of `unittest/input.go:165-190`.
fn parse_three_tokens(vals: &[&str], res: &mut Vec<SeqValue>) -> Result<(), ToolError> {
    let r1 = parse_input_value_impl(&format!("{}{}", vals[1], vals[2]), false)?;
    let p1 = &vals[0][..vals[0].len() - 1];
    let v1 = parse_float(p1)?;
    let option = vals[0].as_bytes()[vals[0].len() - 1];
    let is_add = option == b'+';
    for r in &r1 {
        let rv = raw_value(r);
        res.push(SeqValue::Value(if is_add { rv + v1 } else { v1 - rv }));
    }
    Ok(())
}

/// Parses a `metric{labels}` series selector and a value sequence, then
/// zips them into one [`InputSample`] per non-gap point at
/// `start_ms + i * interval_ms` (the gap still advances the timestamp, it
/// just emits no sample). A `Stale` point emits a sample carrying VM's
/// stale-NaN marker.
///
/// Port of the sample-building half of Go `parseInputSeries`
/// (`unittest/input.go:59-92`), restricted to a single `(selector, values)`
/// pair instead of a whole `[]series`.
pub fn expand_series(
    series_selector: &str,
    values: &str,
    interval: Duration,
    start_ms: i64,
) -> Result<Vec<InputSample>, ToolError> {
    let expr = esm_metricsql::parse(series_selector)
        .map_err(|e| ToolError::new(format!("failed to parse series {series_selector}: {e}")))?;
    let Expr::Metric(metric_expr) = expr else {
        return Err(ToolError::new(format!(
            "got invalid input series {series_selector}"
        )));
    };
    if metric_expr.label_filterss.len() != 1 {
        return Err(ToolError::new(format!(
            "got invalid input series {series_selector}"
        )));
    }
    let labels: Vec<(String, String)> = metric_expr.label_filterss[0]
        .iter()
        .map(|f| (f.label.clone(), f.value.clone()))
        .collect();

    let seq = parse_input_value(values)
        .map_err(|e| ToolError::new(format!("failed to parse input series value {values}: {e}")))?;

    let interval_ms = interval.as_millis() as i64;
    let mut samples = Vec::with_capacity(seq.len());
    for (i, v) in seq.iter().enumerate() {
        let timestamp_ms = start_ms + i as i64 * interval_ms;
        let value = match v {
            SeqValue::Value(val) => *val,
            SeqValue::Stale => STALE_NAN,
            SeqValue::Gap => continue,
        };
        samples.push(InputSample {
            labels: labels.clone(),
            timestamp_ms,
            value,
        });
    }
    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_expanding_and_gaps_and_stale() {
        use SeqValue::*;
        let got = parse_input_value("1+1x3").unwrap();
        assert_eq!(got, vec![Value(1.0), Value(2.0), Value(3.0), Value(4.0)]);
        let got = parse_input_value("1 _ 3").unwrap();
        assert_eq!(got, vec![Value(1.0), Gap, Value(3.0)]);
        let got = parse_input_value("5-1x2").unwrap();
        assert_eq!(got, vec![Value(5.0), Value(4.0), Value(3.0)]);
        let got = parse_input_value("stale").unwrap();
        assert_eq!(got, vec![Stale]);
        assert!(parse_input_value("1+stalex2").is_err()); // stale + op -> error
    }

    #[test]
    fn expands_series_to_samples() {
        let s = expand_series(r#"up{job="x"}"#, "0 1", Duration::from_secs(60), 1_000_000).unwrap();
        assert_eq!(s.len(), 2);
        assert!(s[0]
            .labels
            .iter()
            .any(|(k, v)| k == "__name__" && v == "up"));
        assert!(s[0].labels.iter().any(|(k, v)| k == "job" && v == "x"));
        assert_eq!(s[0].timestamp_ms, 1_000_000);
        assert_eq!(s[1].timestamp_ms, 1_000_000 + 60_000);
        assert_eq!(s[1].value, 1.0);
    }

    /// Port of `unittest/input_test.go:74`: a combined item sequence mixing
    /// plain addition, a gap, a bare negative literal, `stale`, and an
    /// `a+bxN` expansion in one call.
    #[test]
    fn parses_combined_sequence_from_upstream_test() {
        use SeqValue::*;
        let got = parse_input_value("1+1x1 _ -4 stale 3+20x1").unwrap();
        assert_eq!(
            got,
            vec![
                Value(1.0),
                Value(2.0),
                Gap,
                Value(-4.0),
                Stale,
                Value(3.0),
                Value(23.0)
            ]
        );
    }

    /// Port of `unittest/input_test.go:76,78,80`: `inf`/`nan` literals and
    /// their `x`-expansion, case-insensitively.
    #[test]
    fn parses_inf_and_nan_literals() {
        use SeqValue::*;
        let got = parse_input_value("Inf +Inf -Inf").unwrap();
        assert_eq!(
            got,
            vec![
                Value(f64::INFINITY),
                Value(f64::INFINITY),
                Value(f64::NEG_INFINITY)
            ]
        );

        let got = parse_input_value("Nan Infx2").unwrap();
        assert_eq!(got.len(), 4);
        assert!(matches!(got[0], Value(v) if v.is_nan()));
        assert_eq!(got[1], Value(f64::INFINITY));
        assert_eq!(got[2], Value(f64::INFINITY));
        assert_eq!(got[3], Value(f64::INFINITY));

        let got = parse_input_value("NaNx2").unwrap();
        assert_eq!(got.len(), 3);
        for v in got {
            assert!(matches!(v, Value(x) if x.is_nan()));
        }
    }

    /// Port of `unittest/input_test.go:66,68`: a bare `a x N` repeats `a`
    /// (rather than scaling it by the loop index), and a gapped `_ x N`
    /// collapses to a single gap.
    #[test]
    fn expands_repeat_and_gap_x_forms() {
        use SeqValue::*;
        assert_eq!(
            parse_input_value("-4x1").unwrap(),
            vec![Value(-4.0), Value(-4.0)]
        );
        assert_eq!(parse_input_value("_x1").unwrap(), vec![Gap]);
    }

    /// Port of `unittest/input_test.go:22-29`: malformed notation errors
    /// instead of panicking.
    #[test]
    fn rejects_invalid_input() {
        assert!(parse_input_value("").is_err());
        assert!(parse_input_value("x4").is_err());
        assert!(parse_input_value("testfailed").is_err());
        assert!(parse_input_value("stalex3").is_err());
    }
}
