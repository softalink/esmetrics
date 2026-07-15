//! Go `text/template` builtin functions.
//!
//! Go's `text/template` *always* injects a fixed set of builtins (see the
//! `builtins()` map in the stdlib's `text/template/funcs.go`) in addition to
//! any user-supplied FuncMap. vmalert relies on them implicitly — templates
//! such as `{{ if eq $labels.severity "critical" }}`, `{{ if gt $value 100.0 }}`,
//! `{{ printf "%.2f%%" $value }}`, `{{ index .Labels "instance" }}`,
//! `{{ len .Alerts }}` and `{{ and .A .B }}` all resolve to these builtins.
//!
//! Registration order matters: [`crate::default_funcs`] registers these
//! builtins *first* and the vmalert FuncMap afterwards, so a same-named
//! vmalert (or caller) entry overrides the builtin — matching Go, which
//! merges the builtins first and lets custom funcs win.
//!
//! # Faithfulness notes / deliberate divergences
//!
//! * **Numeric comparisons merge int and float.** Go's ordering builtins
//!   (`lt`/`le`/`gt`/`ge`) reject an `int`-vs-`float64` comparison (only
//!   signed/unsigned integer mixing is special-cased). This crate parses
//!   *every* numeric literal as [`Value::Float`] while range indices and
//!   timestamps arrive as [`Value::Int`], so a strict port would reject the
//!   ubiquitous `{{ if eq $i 0 }}` / `{{ gt $value 100 }}` patterns. We
//!   therefore treat `Int` and `Float` as a single numeric kind and compare
//!   by `f64`. `eq`/`ne` likewise compare across int/float.
//! * **`call`** always errors: this `Value` model has no first-class function
//!   variant, so a function value can never be produced to pass to `call`.
//!   The error mirrors Go's "call of non-function" path.
//! * **`js`** ports Go's exact `jsReplacementTable`; runes `< 0x20` or
//!   `>= 0x80` not in the table are emitted as `\uXXXX` (Go's fallback),
//!   which round-trips for the BMP.
//! * **`printf`** ports the verbs vmalert templates actually use — `v s d f
//!   F e E g G x X q t c b o` plus `%%` — with flags (`- + 0 space #`),
//!   width, and precision. Uncommon verbs (`p`, `U`, indexed args) are not
//!   ported.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::exec::FuncFn;
use crate::value::{format_float_go_g_prec, Value};
use crate::TemplateError;

/// Registers the 19 Go `text/template` builtins into `m`.
pub fn register_builtin_funcs(m: &mut HashMap<String, FuncFn>) {
    // Comparison.
    m.insert("eq".to_string(), Box::new(builtin_eq));
    m.insert(
        "ne".to_string(),
        Box::new(|args| {
            let (a, b) = two(args, "ne")?;
            Ok(Value::Bool(!eq_pair(a, b)?))
        }),
    );
    m.insert(
        "lt".to_string(),
        Box::new(|args| {
            let (a, b) = two(args, "lt")?;
            Ok(Value::Bool(less(a, b)?))
        }),
    );
    m.insert(
        "le".to_string(),
        Box::new(|args| {
            let (a, b) = two(args, "le")?;
            Ok(Value::Bool(less(a, b)? || eq_pair(a, b)?))
        }),
    );
    m.insert(
        "gt".to_string(),
        Box::new(|args| {
            let (a, b) = two(args, "gt")?;
            // gt = !le
            Ok(Value::Bool(!(less(a, b)? || eq_pair(a, b)?)))
        }),
    );
    m.insert(
        "ge".to_string(),
        Box::new(|args| {
            let (a, b) = two(args, "ge")?;
            // ge = !lt
            Ok(Value::Bool(!less(a, b)?))
        }),
    );

    // Logic.
    m.insert("and".to_string(), Box::new(builtin_and));
    m.insert("or".to_string(), Box::new(builtin_or));
    m.insert(
        "not".to_string(),
        Box::new(|args| {
            let [v] = arity(args, 1, "not")?;
            Ok(Value::Bool(!v.truthy()))
        }),
    );

    // Collections.
    m.insert("len".to_string(), Box::new(builtin_len));
    m.insert("index".to_string(), Box::new(builtin_index));
    m.insert("slice".to_string(), Box::new(builtin_slice));
    m.insert(
        "call".to_string(),
        // No function values exist in this Value model, so the first argument
        // can never be callable. Mirrors Go's "call of non-function" error.
        Box::new(|_args| Err(TemplateError::new("call of non-function type"))),
    );

    // Formatting.
    m.insert(
        "print".to_string(),
        Box::new(|args| Ok(Value::Str(go_sprint(args)))),
    );
    m.insert(
        "println".to_string(),
        Box::new(|args| Ok(Value::Str(go_sprintln(args)))),
    );
    m.insert(
        "printf".to_string(),
        Box::new(|args| {
            let format = args
                .first()
                .ok_or_else(|| TemplateError::new("printf: missing format string"))?
                .render_string();
            Ok(Value::Str(go_sprintf(&format, &args[1..])?))
        }),
    );

    // Escapers.
    m.insert(
        "html".to_string(),
        Box::new(|args| Ok(Value::Str(go_html_escape(&go_sprint(args))))),
    );
    m.insert(
        "urlquery".to_string(),
        Box::new(|args| Ok(Value::Str(go_url_query_escape(&go_sprint(args))))),
    );
    m.insert(
        "js".to_string(),
        Box::new(|args| Ok(Value::Str(go_js_escape(&go_sprint(args))))),
    );
}

// ---------------------------------------------------------------------------
// Arity helpers
// ---------------------------------------------------------------------------

fn arity<'a, const N: usize>(
    args: &'a [Value],
    n: usize,
    name: &str,
) -> Result<&'a [Value; N], TemplateError> {
    args.try_into().map_err(|_| {
        TemplateError::new(format!(
            "{name}: expected {n} argument(s), got {}",
            args.len()
        ))
    })
}

fn two<'a>(args: &'a [Value], name: &str) -> Result<(&'a Value, &'a Value), TemplateError> {
    let [a, b] = arity::<2>(args, 2, name)?;
    Ok((a, b))
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/// A Go "basic kind" projection used for comparison. `Int`/`Float` collapse to
/// a single numeric kind (see module docs).
enum Basic {
    Bool(bool),
    Num(f64),
    Str(String),
}

fn basic_kind(v: &Value) -> Option<Basic> {
    match v {
        Value::Bool(b) => Some(Basic::Bool(*b)),
        Value::Int(i) => Some(Basic::Num(*i as f64)),
        Value::Float(f) => Some(Basic::Num(*f)),
        Value::Str(s) => Some(Basic::Str(s.clone())),
        _ => None,
    }
}

fn err_incompatible() -> TemplateError {
    TemplateError::new("incompatible types for comparison")
}

fn err_uncomparable() -> TemplateError {
    TemplateError::new("invalid type for comparison")
}

/// Go `eq(a, b)`: equality over basic kinds.
fn eq_pair(a: &Value, b: &Value) -> Result<bool, TemplateError> {
    match (basic_kind(a), basic_kind(b)) {
        (Some(Basic::Bool(x)), Some(Basic::Bool(y))) => Ok(x == y),
        (Some(Basic::Num(x)), Some(Basic::Num(y))) => Ok(x == y),
        (Some(Basic::Str(x)), Some(Basic::Str(y))) => Ok(x == y),
        (Some(_), Some(_)) => Err(err_incompatible()),
        _ => Err(err_uncomparable()),
    }
}

/// Go `lt(a, b)`: strict ordering over numeric/string basic kinds. Bool (and
/// any non-basic kind) is not orderable.
fn less(a: &Value, b: &Value) -> Result<bool, TemplateError> {
    match (basic_kind(a), basic_kind(b)) {
        (Some(Basic::Num(x)), Some(Basic::Num(y))) => Ok(x < y),
        (Some(Basic::Str(x)), Some(Basic::Str(y))) => Ok(x.cmp(&y) == Ordering::Less),
        (Some(Basic::Bool(_)), Some(Basic::Bool(_))) => Err(err_uncomparable()),
        (Some(_), Some(_)) => Err(err_incompatible()),
        _ => Err(err_uncomparable()),
    }
}

/// Go `eq` is variadic: `eq a b c` is `a==b || a==c`.
fn builtin_eq(args: &[Value]) -> Result<Value, TemplateError> {
    if args.len() < 2 {
        return Err(TemplateError::new("eq: missing argument for comparison"));
    }
    let first = &args[0];
    for other in &args[1..] {
        if eq_pair(first, other)? {
            return Ok(Value::Bool(true));
        }
    }
    Ok(Value::Bool(false))
}

// ---------------------------------------------------------------------------
// Logic
// ---------------------------------------------------------------------------

/// Go `and`: returns the first non-truthy argument, or the last argument.
fn builtin_and(args: &[Value]) -> Result<Value, TemplateError> {
    if args.is_empty() {
        return Err(TemplateError::new("and: missing arguments"));
    }
    let mut result = &args[0];
    if result.truthy() {
        for a in &args[1..] {
            result = a;
            if !a.truthy() {
                break;
            }
        }
    }
    Ok(result.clone())
}

/// Go `or`: returns the first truthy argument, or the last argument.
fn builtin_or(args: &[Value]) -> Result<Value, TemplateError> {
    if args.is_empty() {
        return Err(TemplateError::new("or: missing arguments"));
    }
    let mut result = &args[0];
    if !result.truthy() {
        for a in &args[1..] {
            result = a;
            if a.truthy() {
                break;
            }
        }
    }
    Ok(result.clone())
}

// ---------------------------------------------------------------------------
// Collections
// ---------------------------------------------------------------------------

/// Go `len`: length of a string (bytes), slice, or map. Anything else errors.
fn builtin_len(args: &[Value]) -> Result<Value, TemplateError> {
    let [v] = arity::<1>(args, 1, "len")?;
    let n = match v {
        Value::Str(s) => s.len(),
        Value::Vec(x) => x.len(),
        Value::List(x) => x.len(),
        Value::Map(m) => m.len(),
        other => {
            return Err(TemplateError::new(format!(
                "len of type {}",
                type_name(other)
            )))
        }
    };
    Ok(Value::Int(n as i64))
}

/// Go `index x i j ...`: nested indexing `x[i][j]...`.
fn builtin_index(args: &[Value]) -> Result<Value, TemplateError> {
    let item = args
        .first()
        .ok_or_else(|| TemplateError::new("index: missing argument"))?;
    let mut cur = item.clone();
    for idx in &args[1..] {
        cur = index_one(&cur, idx)?;
    }
    Ok(cur)
}

fn index_one(cur: &Value, idx: &Value) -> Result<Value, TemplateError> {
    match cur {
        // missingkey=zero: an absent map key yields the zero value (Nil).
        Value::Map(m) => Ok(m.get(&idx.render_string()).cloned().unwrap_or(Value::Nil)),
        Value::List(l) => {
            let i = as_index(idx, l.len())?;
            Ok(l[i].clone())
        }
        Value::Vec(v) => {
            let i = as_index(idx, v.len())?;
            Ok(Value::Metric(v[i].clone()))
        }
        Value::Str(s) => {
            let bytes = s.as_bytes();
            let i = as_index(idx, bytes.len())?;
            // Go: indexing a string yields the byte (uint8) at that position.
            Ok(Value::Int(bytes[i] as i64))
        }
        other => Err(TemplateError::new(format!(
            "can't index item of type {}",
            type_name(other)
        ))),
    }
}

/// Coerces an index value to a bounds-checked `usize`.
fn as_index(idx: &Value, len: usize) -> Result<usize, TemplateError> {
    let i = as_i64(idx)
        .ok_or_else(|| TemplateError::new("cannot index slice/array with non-integer"))?;
    if i < 0 || i as usize >= len {
        return Err(TemplateError::new(format!("index out of range: {i}")));
    }
    Ok(i as usize)
}

/// Go 1.13+ `slice x [i [j [k]]]`: `x[i:j]` (capacity `k` ignored — this model
/// tracks no capacity). Supports slices/vectors and strings.
fn builtin_slice(args: &[Value]) -> Result<Value, TemplateError> {
    let item = args
        .first()
        .ok_or_else(|| TemplateError::new("slice: missing argument"))?;
    let idxs = &args[1..];
    let len = match item {
        Value::List(l) => l.len(),
        Value::Vec(v) => v.len(),
        Value::Str(s) => s.len(),
        other => {
            return Err(TemplateError::new(format!(
                "can't slice item of type {}",
                type_name(other)
            )))
        }
    };
    if matches!(item, Value::Str(_)) && idxs.len() > 2 {
        return Err(TemplateError::new("cannot 3-index slice a string"));
    }
    if idxs.len() > 3 {
        return Err(TemplateError::new("too many slice indexes"));
    }
    let i = match idxs.first() {
        Some(v) => bound(v, len)?,
        None => 0,
    };
    let j = match idxs.get(1) {
        Some(v) => bound(v, len)?,
        None => len,
    };
    if i > j {
        return Err(TemplateError::new(format!(
            "slice bounds out of range: {i} > {j}"
        )));
    }
    Ok(match item {
        Value::List(l) => Value::List(l[i..j].to_vec()),
        Value::Vec(v) => Value::Vec(v[i..j].to_vec()),
        Value::Str(s) => Value::Str(String::from_utf8_lossy(&s.as_bytes()[i..j]).into_owned()),
        _ => unreachable!("guarded above"),
    })
}

/// Slice-bound coercion: like [`as_index`] but the upper bound is inclusive of
/// `len` (a slice may end at `len`).
fn bound(idx: &Value, len: usize) -> Result<usize, TemplateError> {
    let i = as_i64(idx).ok_or_else(|| TemplateError::new("cannot slice with non-integer index"))?;
    if i < 0 || i as usize > len {
        return Err(TemplateError::new(format!("slice index out of range: {i}")));
    }
    Ok(i as usize)
}

// ---------------------------------------------------------------------------
// Numeric coercion
// ---------------------------------------------------------------------------

/// Integer coercion for `index`/`printf %d` etc.: an `Int`, or a `Float` with
/// no fractional part, or a parseable string.
fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
        Value::Float(f) => Some(*f as i64),
        Value::Str(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn to_i64(v: &Value, verb: char) -> Result<i64, TemplateError> {
    as_i64(v).ok_or_else(|| TemplateError::new(format!("printf %{verb}: expected an integer")))
}

fn to_f64(v: &Value, verb: char) -> Result<f64, TemplateError> {
    match v {
        Value::Float(f) => Ok(*f),
        Value::Int(i) => Ok(*i as f64),
        Value::Str(s) => s
            .parse::<f64>()
            .map_err(|_| TemplateError::new(format!("printf %{verb}: expected a number"))),
        _ => Err(TemplateError::new(format!(
            "printf %{verb}: expected a number"
        ))),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Nil => "nil",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float64",
        Value::Str(_) => "string",
        Value::Metric(_) => "metric",
        Value::Vec(_) => "[]metric",
        Value::Map(_) => "map",
        Value::List(_) => "slice",
    }
}

// ---------------------------------------------------------------------------
// fmt.Sprint / Sprintln
// ---------------------------------------------------------------------------

/// Go `fmt.Sprint`: default-format each operand, inserting a space between two
/// operands only when *neither* is a string.
fn go_sprint(args: &[Value]) -> String {
    let mut out = String::new();
    let mut prev_string = false;
    for (i, a) in args.iter().enumerate() {
        let is_string = matches!(a, Value::Str(_));
        if i > 0 && !prev_string && !is_string {
            out.push(' ');
        }
        out.push_str(&a.render_string());
        prev_string = is_string;
    }
    out
}

/// Go `fmt.Sprintln`: space between every operand, trailing newline.
fn go_sprintln(args: &[Value]) -> String {
    let mut out = args
        .iter()
        .map(Value::render_string)
        .collect::<Vec<_>>()
        .join(" ");
    out.push('\n');
    out
}

// ---------------------------------------------------------------------------
// fmt.Sprintf
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Flags {
    minus: bool,
    plus: bool,
    zero: bool,
    space: bool,
    hash: bool,
}

/// Go `fmt.Sprintf` over the verb subset vmalert templates use.
fn go_sprintf(format: &str, args: &[Value]) -> Result<String, TemplateError> {
    let chars: Vec<char> = format.chars().collect();
    let mut out = String::new();
    let mut arg_idx = 0usize;
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c != '%' {
            out.push(c);
            i += 1;
            continue;
        }
        i += 1; // consume '%'
        if i < chars.len() && chars[i] == '%' {
            out.push('%');
            i += 1;
            continue;
        }
        // Flags.
        let mut flags = Flags::default();
        while i < chars.len() {
            match chars[i] {
                '-' => flags.minus = true,
                '+' => flags.plus = true,
                '0' => flags.zero = true,
                ' ' => flags.space = true,
                '#' => flags.hash = true,
                _ => break,
            }
            i += 1;
        }
        // Width.
        let mut width_str = String::new();
        while i < chars.len() && chars[i].is_ascii_digit() {
            width_str.push(chars[i]);
            i += 1;
        }
        let width = width_str.parse::<usize>().ok();
        // Precision.
        let mut prec = None;
        if i < chars.len() && chars[i] == '.' {
            i += 1;
            let mut p = String::new();
            while i < chars.len() && chars[i].is_ascii_digit() {
                p.push(chars[i]);
                i += 1;
            }
            prec = Some(p.parse::<usize>().unwrap_or(0));
        }
        // Verb.
        let verb = *chars
            .get(i)
            .ok_or_else(|| TemplateError::new("printf: missing verb after %"))?;
        i += 1;
        let arg = args.get(arg_idx);
        arg_idx += 1;
        out.push_str(&format_verb(verb, arg, &flags, width, prec)?);
    }
    Ok(out)
}

fn format_verb(
    verb: char,
    arg: Option<&Value>,
    flags: &Flags,
    width: Option<usize>,
    prec: Option<usize>,
) -> Result<String, TemplateError> {
    let arg =
        arg.ok_or_else(|| TemplateError::new(format!("printf %{verb}: not enough arguments")))?;
    let (body, numeric) = match verb {
        'v' | 's' => {
            let mut s = arg.render_string();
            if let Some(p) = prec {
                s = s.chars().take(p).collect();
            }
            (s, false)
        }
        'd' => {
            let n = to_i64(arg, verb)?;
            (with_sign(n.to_string(), n >= 0, flags), true)
        }
        'f' | 'F' => {
            let f = to_f64(arg, verb)?;
            let p = prec.unwrap_or(6);
            let core = format!("{:.*}", p, f);
            (with_sign_str(core, flags), true)
        }
        'e' | 'E' => {
            let f = to_f64(arg, verb)?;
            let p = prec.unwrap_or(6);
            let core = fix_exponent(format!("{:.*e}", p, f), verb == 'E');
            (with_sign_str(core, flags), true)
        }
        'g' | 'G' => {
            let f = to_f64(arg, verb)?;
            let mut core = format_float_go_g_prec(f, prec);
            if verb == 'G' {
                core = core.replace('e', "E");
            }
            (with_sign_str(core, flags), true)
        }
        'x' | 'X' => (
            format_hex(arg, verb == 'X', flags)?,
            !matches!(arg, Value::Str(_)),
        ),
        'b' => {
            let n = to_i64(arg, verb)?;
            let core = if n < 0 {
                format!("-{:b}", n.unsigned_abs())
            } else {
                format!("{:b}", n)
            };
            (with_sign_str(core, flags), true)
        }
        'o' => {
            let n = to_i64(arg, verb)?;
            let core = if n < 0 {
                format!("-{:o}", n.unsigned_abs())
            } else {
                format!("{:o}", n)
            };
            (with_sign_str(core, flags), true)
        }
        'c' => {
            let n = to_i64(arg, verb)?;
            let ch = u32::try_from(n)
                .ok()
                .and_then(char::from_u32)
                .unwrap_or('\u{FFFD}');
            (ch.to_string(), false)
        }
        't' => {
            let b = match arg {
                Value::Bool(b) => *b,
                other => other.truthy(),
            };
            (b.to_string(), false)
        }
        'q' => (go_quote(&arg.render_string()), false),
        other => {
            return Err(TemplateError::new(format!(
                "printf: unsupported verb %{other}"
            )))
        }
    };
    Ok(pad(body, flags, width, numeric))
}

/// Applies `+`/space flags to a signed integer already rendered by `to_string`.
fn with_sign(core: String, nonneg: bool, flags: &Flags) -> String {
    if nonneg {
        if flags.plus {
            return format!("+{core}");
        } else if flags.space {
            return format!(" {core}");
        }
    }
    core
}

/// As [`with_sign`] but infers non-negativity from a `-` prefix (for
/// float/hex/bin/oct cores).
fn with_sign_str(core: String, flags: &Flags) -> String {
    let nonneg = !core.starts_with('-');
    with_sign(core, nonneg, flags)
}

fn format_hex(arg: &Value, upper: bool, flags: &Flags) -> Result<String, TemplateError> {
    let core = match arg {
        Value::Str(s) => s.bytes().map(|b| format!("{b:02x}")).collect::<String>(),
        _ => {
            let n = to_i64(arg, if upper { 'X' } else { 'x' })?;
            if n < 0 {
                format!("-{:x}", n.unsigned_abs())
            } else {
                format!("{:x}", n)
            }
        }
    };
    let core = if upper { core.to_uppercase() } else { core };
    let core = if flags.hash {
        let prefix = if upper { "0X" } else { "0x" };
        if let Some(rest) = core.strip_prefix('-') {
            format!("-{prefix}{rest}")
        } else {
            format!("{prefix}{core}")
        }
    } else {
        core
    };
    Ok(with_sign_str(core, flags))
}

/// Rust's `{:e}` emits `3.14e0` / `1.5e-5`; Go's `%e` wants `3.14e+00` /
/// `1.5e-05` — a signed, min-two-digit exponent.
fn fix_exponent(s: String, upper: bool) -> String {
    let Some(pos) = s.find('e') else {
        return s;
    };
    let (mant, exp) = s.split_at(pos);
    let exp = &exp[1..];
    let (sign, digits) = if let Some(r) = exp.strip_prefix('-') {
        ("-", r)
    } else if let Some(r) = exp.strip_prefix('+') {
        ("+", r)
    } else {
        ("+", exp)
    };
    let digits = if digits.len() < 2 {
        format!("{digits:0>2}")
    } else {
        digits.to_string()
    };
    let e = if upper { 'E' } else { 'e' };
    format!("{mant}{e}{sign}{digits}")
}

fn pad(s: String, flags: &Flags, width: Option<usize>, numeric: bool) -> String {
    let Some(w) = width else {
        return s;
    };
    let len = s.chars().count();
    if len >= w {
        return s;
    }
    let fill = w - len;
    if flags.minus {
        format!("{s}{}", " ".repeat(fill))
    } else if flags.zero && numeric {
        // Zero-padding goes after any leading sign.
        let (sign, rest) = match s.chars().next() {
            Some(c @ ('-' | '+' | ' ')) => (&s[..c.len_utf8()], &s[c.len_utf8()..]),
            _ => ("", s.as_str()),
        };
        format!("{sign}{}{rest}", "0".repeat(fill))
    } else {
        format!("{}{s}", " ".repeat(fill))
    }
}

/// Minimal Go `strconv.Quote`: double-quoted, backslash escapes for the common
/// control characters, other control bytes as `\x{:02x}`. Printable (including
/// non-ASCII) characters pass through.
fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// Escapers
// ---------------------------------------------------------------------------

/// Go `template.HTMLEscapeString` (the `html` builtin). Note this differs from
/// the crate's `htmlEscape` vmalert func (quicktemplate) in using `&#34;` /
/// `&#39;` for the quote characters and mapping NUL to U+FFFD.
fn go_html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\0' => out.push('\u{FFFD}'),
            c => out.push(c),
        }
    }
    out
}

/// Go `net/url.QueryEscape` (the `urlquery` builtin): unreserved bytes are
/// ASCII alphanumerics plus `-_.~`; space becomes `+`; everything else is
/// percent-encoded with uppercase hex.
fn go_url_query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b == b' ' {
            out.push('+');
        } else if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Go `template.JSEscapeString` (the `js` builtin), porting the stdlib's
/// `jsReplacementTable`. Runes below `0x20` or at/above `0x80` that are not in
/// the table are emitted as `\uXXXX`.
fn go_js_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        let repl = match c {
            '\0' => Some("\\u0000"),
            '\t' => Some("\\t"),
            '\n' => Some("\\n"),
            '\u{000b}' => Some("\\u000b"),
            '\u{000c}' => Some("\\f"),
            '\r' => Some("\\r"),
            '"' => Some("\\u0022"),
            '&' => Some("\\u0026"),
            '\'' => Some("\\u0027"),
            '+' => Some("\\u002b"),
            '/' => Some("\\/"),
            '<' => Some("\\u003c"),
            '>' => Some("\\u003e"),
            '\\' => Some("\\\\"),
            _ => None,
        };
        match repl {
            Some(r) => out.push_str(r),
            None if (c as u32) < 0x20 || (c as u32) >= 0x80 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            None => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Metric;
    use std::collections::BTreeMap;

    fn setup() -> HashMap<String, FuncFn> {
        let mut m = HashMap::new();
        register_builtin_funcs(&mut m);
        m
    }

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

    fn f(x: f64) -> Value {
        Value::Float(x)
    }
    fn i(x: i64) -> Value {
        Value::Int(x)
    }
    fn s(x: &str) -> Value {
        Value::Str(x.to_string())
    }

    #[test]
    fn eq_is_variadic_and_crosses_int_float() {
        let m = setup();
        assert!(call(&m, "eq", &[i(1), i(1)]).truthy());
        assert!(call(&m, "eq", &[f(1.0), i(1)]).truthy()); // cross int/float
        assert!(!call(&m, "eq", &[i(1), i(2)]).truthy());
        // variadic: eq a b c == (a==b || a==c)
        assert!(call(&m, "eq", &[i(3), i(1), i(3)]).truthy());
        assert!(call(&m, "eq", &[s("a"), s("b"), s("a")]).truthy());
    }

    #[test]
    fn ne_lt_le_gt_ge_behave() {
        let m = setup();
        assert!(call(&m, "ne", &[i(1), i(2)]).truthy());
        assert!(call(&m, "lt", &[f(1.0), f(2.0)]).truthy());
        assert!(call(&m, "le", &[i(2), i(2)]).truthy());
        assert!(call(&m, "gt", &[f(100.5), f(100.0)]).truthy());
        assert!(call(&m, "ge", &[i(2), i(2)]).truthy());
        assert!(!call(&m, "ge", &[i(1), i(2)]).truthy());
        // string ordering
        assert!(call(&m, "lt", &[s("a"), s("b")]).truthy());
    }

    #[test]
    fn comparison_of_incompatible_or_uncomparable_kinds_errors() {
        let m = setup();
        // string vs number
        assert!(call_err(&m, "eq", &[s("a"), i(1)])
            .msg
            .contains("incompatible"));
        // bool is not orderable
        assert!(call_err(&m, "lt", &[Value::Bool(true), Value::Bool(false)])
            .msg
            .contains("invalid type"));
        // non-basic kind
        assert!(call_err(&m, "eq", &[Value::Nil, Value::Nil])
            .msg
            .contains("invalid type"));
    }

    #[test]
    fn and_returns_first_empty_or_last() {
        let m = setup();
        assert_eq!(call(&m, "and", &[i(1), i(2)]), i(2));
        assert_eq!(call(&m, "and", &[i(1), i(0), i(3)]), i(0));
        assert_eq!(call(&m, "or", &[i(0), i(2)]), i(2));
        assert_eq!(call(&m, "or", &[i(0), Value::Str(String::new())]), s(""));
        assert!(call(&m, "not", &[i(0)]).truthy()); // not 0 -> true
        assert!(!call(&m, "not", &[i(1)]).truthy());
    }

    #[test]
    fn len_of_string_slice_and_map() {
        let m = setup();
        assert_eq!(call(&m, "len", &[s("hello")]), i(5));
        assert_eq!(
            call(&m, "len", &[Value::List(vec![i(1), i(2), i(3)])]),
            i(3)
        );
        let mut map = BTreeMap::new();
        map.insert("a".to_string(), i(1));
        assert_eq!(call(&m, "len", &[Value::Map(map)]), i(1));
        assert!(call_err(&m, "len", &[i(1)]).msg.contains("len of type"));
    }

    #[test]
    fn index_into_map_list_and_nested() {
        let m = setup();
        let mut map = BTreeMap::new();
        map.insert("k".to_string(), s("v"));
        assert_eq!(
            call(&m, "index", &[Value::Map(map.clone()), s("k")]),
            s("v")
        );
        // absent key -> Nil (missingkey=zero)
        assert_eq!(
            call(&m, "index", &[Value::Map(map), s("missing")]),
            Value::Nil
        );
        let list = Value::List(vec![
            Value::List(vec![s("a"), s("b")]),
            Value::List(vec![s("c"), s("d")]),
        ]);
        // index x 1 0 -> x[1][0]
        assert_eq!(call(&m, "index", &[list.clone(), i(1), i(0)]), s("c"));
        // out of range
        assert!(call_err(&m, "index", &[list, i(9)])
            .msg
            .contains("out of range"));
    }

    #[test]
    fn slice_of_list_and_string() {
        let m = setup();
        let list = Value::List(vec![i(0), i(1), i(2), i(3)]);
        assert_eq!(
            call(&m, "slice", &[list.clone(), i(1), i(3)]),
            Value::List(vec![i(1), i(2)])
        );
        assert_eq!(
            call(&m, "slice", &[list, i(2)]),
            Value::List(vec![i(2), i(3)])
        );
        assert_eq!(call(&m, "slice", &[s("hello"), i(1), i(3)]), s("el"));
    }

    #[test]
    fn call_always_errors() {
        let m = setup();
        assert!(call_err(&m, "call", &[i(1)]).msg.contains("non-function"));
    }

    #[test]
    fn print_and_println_spacing() {
        let m = setup();
        // Sprint: space only when neither operand is a string.
        assert_eq!(call(&m, "print", &[i(1), i(2)]), s("1 2"));
        assert_eq!(call(&m, "print", &[s("a"), s("b")]), s("ab"));
        assert_eq!(call(&m, "print", &[s("a"), i(2)]), s("a2"));
        assert_eq!(call(&m, "println", &[i(1), i(2)]), s("1 2\n"));
    }

    #[test]
    fn printf_common_verbs() {
        let m = setup();
        assert_eq!(call(&m, "printf", &[s("%.2f"), f(1.23456)]), s("1.23"));
        assert_eq!(call(&m, "printf", &[s("%.2f%%"), f(42.0)]), s("42.00%"));
        assert_eq!(call(&m, "printf", &[s("%d"), i(42)]), s("42"));
        assert_eq!(call(&m, "printf", &[s("%5d"), i(42)]), s("   42"));
        assert_eq!(call(&m, "printf", &[s("%-5d|"), i(42)]), s("42   |"));
        assert_eq!(call(&m, "printf", &[s("%05d"), i(42)]), s("00042"));
        assert_eq!(call(&m, "printf", &[s("%+d"), i(42)]), s("+42"));
        assert_eq!(call(&m, "printf", &[s("%x"), i(255)]), s("ff"));
        assert_eq!(call(&m, "printf", &[s("%X"), i(255)]), s("FF"));
        assert_eq!(call(&m, "printf", &[s("%q"), s("hi")]), s("\"hi\""));
        assert_eq!(call(&m, "printf", &[s("%t"), Value::Bool(true)]), s("true"));
        assert_eq!(call(&m, "printf", &[s("%s!"), s("hi")]), s("hi!"));
        assert_eq!(call(&m, "printf", &[s("%v"), i(7)]), s("7"));
        assert_eq!(call(&m, "printf", &[s("%c"), i(65)]), s("A"));
        assert_eq!(
            call(&m, "printf", &[s("%e"), f(1.23456)]),
            s("1.234560e+00")
        );
        assert_eq!(call(&m, "printf", &[s("%g"), f(1.23456)]), s("1.23456"));
    }

    #[test]
    fn escapers_match_go() {
        let m = setup();
        assert_eq!(
            call(&m, "html", &[s("<a href=\"x\">&'y'</a>")]),
            s("&lt;a href=&#34;x&#34;&gt;&amp;&#39;y&#39;&lt;/a&gt;")
        );
        assert_eq!(call(&m, "urlquery", &[s("a b&c=d")]), s("a+b%26c%3Dd"));
        assert_eq!(
            call(&m, "js", &[s("a'b\"c<d>")]),
            s("a\\u0027b\\u0022c\\u003cd\\u003e")
        );
    }

    #[test]
    fn index_metric_vector_yields_metric() {
        let m = setup();
        let metric = Metric {
            labels: BTreeMap::new(),
            value: 5.0,
            timestamp: 0,
        };
        let v = Value::Vec(vec![metric.clone()]);
        assert_eq!(call(&m, "index", &[v, i(0)]), Value::Metric(metric));
    }
}
