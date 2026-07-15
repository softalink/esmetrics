//! String/format builtin functions.
//!
//! Reference: `app/vmalert/templates/template.go`'s `/* Strings */` section
//! (upstream VictoriaMetrics vmalert), plus the quicktemplate-generated
//! escaping helpers in `app/vmalert/templates/funcs.qtpl.go` /
//! `vendor/github.com/valyala/quicktemplate/jsonstring.go` and
//! `htmlescapewriter.go` that back `quotesEscape`/`jsonEscape`/`htmlEscape`,
//! and Go stdlib `net.SplitHostPort`/`net.JoinHostPort` and
//! `net/url.PathEscape`/`QueryEscape` for the host/URL helpers.

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;

use regex::Regex;

use crate::exec::FuncFn;
use crate::value::Value;
use crate::TemplateError;

/// Registers the 14 string/format builtins into `m`.
pub fn register_string_funcs(m: &mut HashMap<String, FuncFn>) {
    m.insert(
        "toUpper".to_string(),
        Box::new(|args| Ok(Value::Str(one_str(args, "toUpper")?.to_uppercase()))),
    );
    m.insert(
        "toLower".to_string(),
        Box::new(|args| Ok(Value::Str(one_str(args, "toLower")?.to_lowercase()))),
    );
    m.insert(
        "title".to_string(),
        Box::new(|args| Ok(Value::Str(go_title(&one_str(args, "title")?)))),
    );
    m.insert(
        "crlfEscape".to_string(),
        Box::new(|args| {
            let s = one_str(args, "crlfEscape")?;
            Ok(Value::Str(s.replace('\n', "\\n").replace('\r', "\\r")))
        }),
    );
    m.insert(
        "quotesEscape".to_string(),
        Box::new(|args| {
            Ok(Value::Str(json_string_escape(
                &one_str(args, "quotesEscape")?,
                false,
            )))
        }),
    );
    m.insert(
        "jsonEscape".to_string(),
        Box::new(|args| {
            Ok(Value::Str(json_string_escape(
                &one_str(args, "jsonEscape")?,
                true,
            )))
        }),
    );
    m.insert(
        "htmlEscape".to_string(),
        Box::new(|args| Ok(Value::Str(html_escape(&one_str(args, "htmlEscape")?)))),
    );
    m.insert(
        "stripPort".to_string(),
        Box::new(|args| Ok(Value::Str(strip_port(&one_str(args, "stripPort")?)))),
    );
    m.insert(
        "stripDomain".to_string(),
        Box::new(|args| Ok(Value::Str(strip_domain(&one_str(args, "stripDomain")?)))),
    );
    m.insert(
        "match".to_string(),
        Box::new(|args| {
            let (pattern, text) = two_str(args, "match")?;
            let re = Regex::new(&pattern)
                .map_err(|e| TemplateError::new(format!("match: bad pattern {pattern:?}: {e}")))?;
            Ok(Value::Bool(re.is_match(&text)))
        }),
    );
    m.insert(
        "reReplaceAll".to_string(),
        Box::new(|args| {
            let (pattern, repl, text) = three_str(args, "reReplaceAll")?;
            let re = Regex::new(&pattern).map_err(|e| {
                TemplateError::new(format!("reReplaceAll: bad pattern {pattern:?}: {e}"))
            })?;
            Ok(Value::Str(
                re.replace_all(&text, repl.as_str()).into_owned(),
            ))
        }),
    );
    m.insert(
        "pathEscape".to_string(),
        Box::new(|args| Ok(Value::Str(go_path_escape(&one_str(args, "pathEscape")?)))),
    );
    m.insert(
        "queryEscape".to_string(),
        Box::new(|args| Ok(Value::Str(go_query_escape(&one_str(args, "queryEscape")?)))),
    );
    // safeHtml marks a string as not requiring auto-escaping in Go's
    // html/template. This crate only implements text/template semantics, so
    // there is no escaping to suppress and the function is the identity.
    m.insert(
        "safeHtml".to_string(),
        Box::new(|args| Ok(Value::Str(one_str(args, "safeHtml")?))),
    );
}

fn one_str(args: &[Value], name: &str) -> Result<String, TemplateError> {
    match args {
        [a] => Ok(a.render_string()),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 1 argument, got {}",
            args.len()
        ))),
    }
}

fn two_str(args: &[Value], name: &str) -> Result<(String, String), TemplateError> {
    match args {
        [a, b] => Ok((a.render_string(), b.render_string())),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 2 arguments, got {}",
            args.len()
        ))),
    }
}

fn three_str(args: &[Value], name: &str) -> Result<(String, String, String), TemplateError> {
    match args {
        [a, b, c] => Ok((a.render_string(), b.render_string(), c.render_string())),
        _ => Err(TemplateError::new(format!(
            "{name}: expected 3 arguments, got {}",
            args.len()
        ))),
    }
}

/// Go `strings.Title`: uppercases the first letter of each "word", where a
/// word boundary is any non-alphanumeric, non-underscore rune (ASCII fast
/// path) or any non-letter/non-digit rune that is whitespace (Unicode path).
/// Note: this uses Rust's `char::to_uppercase` rather than a dedicated
/// Unicode *titlecase* mapping (Rust's std has no `to_titlecase`); the two
/// differ only for a handful of digraph characters (e.g. `ǆ` -> `ǅ` vs `Ǆ`)
/// that vmalert's alerting templates do not exercise in practice.
fn go_title(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_is_separator = true; // Go's `prev := ' '`, and ' ' is a separator.
    for c in s.chars() {
        if prev_is_separator {
            out.extend(c.to_uppercase());
        } else {
            out.push(c);
        }
        prev_is_separator = is_word_separator(c);
    }
    out
}

fn is_word_separator(c: char) -> bool {
    if c.is_ascii() {
        !(c.is_ascii_alphanumeric() || c == '_')
    } else if c.is_alphanumeric() {
        false
    } else {
        c.is_whitespace()
    }
}

/// Byte-for-byte port of quicktemplate's `AppendJSONString` (used by both
/// `jsonEscape` (`addQuotes = true`) and `quotesEscape` (`addQuotes =
/// false`)). Escapes control characters, `"`, `\`, `<`, and `'`; everything
/// else (including non-ASCII UTF-8 bytes) passes through unchanged.
fn json_string_escape(s: &str, add_quotes: bool) -> String {
    let mut out = String::with_capacity(s.len() + if add_quotes { 2 } else { 0 });
    if add_quotes {
        out.push('"');
    }
    for c in s.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '<' => out.push_str("\\u003c"),
            '\'' => out.push_str("\\u0027"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    if add_quotes {
        out.push('"');
    }
    out
}

/// quicktemplate's `htmlEscape` (`E().S(s)`): escapes `<`, `>`, `"`, `'`,
/// and `&` to their named HTML entities. A single pass over the original
/// characters avoids double-escaping the `&` introduced by other
/// substitutions.
fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            c => out.push(c),
        }
    }
    out
}

/// Go `net.SplitHostPort`, restricted to what `stripPort`/`stripDomain`
/// need. Returns `Err(())` for any malformed input (missing port, too many
/// colons, unbalanced brackets), mirroring the upstream error cases; callers
/// fall back to the original string on error exactly as vmalert's closures
/// do.
fn split_host_port(hostport: &str) -> Result<(String, String), ()> {
    let last_colon = hostport.rfind(':').ok_or(())?;

    let (host, port, unbracketed_check_from);
    if hostport.as_bytes().first() == Some(&b'[') {
        let end = hostport.find(']').ok_or(())?;
        if end + 1 == hostport.len() {
            return Err(()); // missing port
        } else if end + 1 != last_colon {
            return Err(()); // ']' not immediately followed by the last ':'
        }
        host = hostport[1..end].to_string();
        port = hostport[last_colon + 1..].to_string();
        // j, k = 1, end+1 in upstream: only the parts outside the bracket
        // span are checked for stray '[' / ']'.
        if hostport[1..].contains('[') {
            return Err(());
        }
        if hostport[end + 1..].contains(']') {
            return Err(());
        }
        return Ok((host, port));
    } else {
        host = hostport[..last_colon].to_string();
        if host.contains(':') {
            return Err(()); // too many colons
        }
        port = hostport[last_colon + 1..].to_string();
        unbracketed_check_from = 0;
    }
    if hostport[unbracketed_check_from..].contains('[') {
        return Err(());
    }
    if hostport[unbracketed_check_from..].contains(']') {
        return Err(());
    }
    Ok((host, port))
}

fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn strip_port(host_port: &str) -> String {
    match split_host_port(host_port) {
        Ok((host, _port)) => host,
        Err(()) => host_port.to_string(),
    }
}

fn strip_domain(host_port: &str) -> String {
    let (mut host, port) = match split_host_port(host_port) {
        Ok((host, port)) => (host, port),
        Err(()) => (host_port.to_string(), String::new()),
    };
    if IpAddr::from_str(&host).is_ok() {
        return host_port.to_string();
    }
    host = host.split('.').next().unwrap_or("").to_string();
    if !port.is_empty() {
        join_host_port(&host, &port)
    } else {
        host
    }
}

/// Go `net/url.PathEscape` (`encodePathSegment` mode): unreserved bytes are
/// ASCII alphanumerics plus `-_.~$&+:=@`; everything else (including
/// non-ASCII UTF-8 bytes) is percent-encoded with uppercase hex.
fn go_path_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if is_path_segment_unreserved(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn is_path_segment_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b':' | b'=' | b'@'
        )
}

/// Go `net/url.QueryEscape` (`encodeQueryComponent` mode): unreserved bytes
/// are ASCII alphanumerics plus `-_.~`; spaces become `+`; everything else
/// is percent-encoded with uppercase hex.
fn go_query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b == b' ' {
            out.push('+');
        } else if is_query_unreserved(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn is_query_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
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
        register_string_funcs(&mut m);
        m
    }

    fn s(v: &str) -> Value {
        Value::Str(v.to_string())
    }

    #[test]
    fn string_funcs_behave() {
        let m = setup();
        assert_eq!(call(&m, "toUpper", &[s("ab")]).render_string(), "AB");
        assert_eq!(
            call(&m, "reReplaceAll", &[s("a(.)c"), s("X$1"), s("abc")]).render_string(),
            "Xb"
        );
        assert_eq!(
            call(&m, "stripPort", &[s("host:9090")]).render_string(),
            "host"
        );
        assert!(call(&m, "match", &[s("^a"), s("abc")]).truthy());
    }

    #[test]
    fn to_lower_lowercases() {
        let m = setup();
        assert_eq!(call(&m, "toLower", &[s("ABc")]).render_string(), "abc");
    }

    #[test]
    fn title_capitalizes_each_word() {
        let m = setup();
        assert_eq!(
            call(&m, "title", &[s("hello world-foo")]).render_string(),
            "Hello World-Foo"
        );
    }

    #[test]
    fn crlf_escape_replaces_newlines_and_carriage_returns() {
        let m = setup();
        assert_eq!(
            call(&m, "crlfEscape", &[s("a\nb\rc")]).render_string(),
            "a\\nb\\rc"
        );
    }

    #[test]
    fn quotes_escape_escapes_without_wrapping_quotes() {
        let m = setup();
        assert_eq!(
            call(&m, "quotesEscape", &[s("say \"hi\"")]).render_string(),
            "say \\\"hi\\\""
        );
    }

    #[test]
    fn json_escape_wraps_in_quotes_and_escapes_special_chars() {
        let m = setup();
        assert_eq!(
            call(&m, "jsonEscape", &[s("say \"hi\"\n")]).render_string(),
            "\"say \\\"hi\\\"\\n\""
        );
    }

    #[test]
    fn json_escape_escapes_lt_and_apostrophe() {
        let m = setup();
        assert_eq!(
            call(&m, "jsonEscape", &[s("<a>'b'")]).render_string(),
            "\"\\u003ca>\\u0027b\\u0027\""
        );
    }

    #[test]
    fn html_escape_escapes_reserved_chars_once() {
        let m = setup();
        assert_eq!(
            call(&m, "htmlEscape", &[s("<a href=\"x\">&'y'</a>")]).render_string(),
            "&lt;a href=&quot;x&quot;&gt;&amp;&#39;y&#39;&lt;/a&gt;"
        );
    }

    #[test]
    fn strip_port_returns_input_unchanged_when_no_port() {
        let m = setup();
        assert_eq!(
            call(&m, "stripPort", &[s("host.example.com")]).render_string(),
            "host.example.com"
        );
    }

    #[test]
    fn strip_port_handles_bracketed_ipv6() {
        let m = setup();
        assert_eq!(
            call(&m, "stripPort", &[s("[::1]:9090")]).render_string(),
            "::1"
        );
    }

    #[test]
    fn strip_domain_strips_first_label_only() {
        let m = setup();
        assert_eq!(call(&m, "stripDomain", &[s("a.b.c")]).render_string(), "a");
    }

    #[test]
    fn strip_domain_preserves_port() {
        let m = setup();
        assert_eq!(
            call(&m, "stripDomain", &[s("a.b.c:9090")]).render_string(),
            "a:9090"
        );
    }

    #[test]
    fn strip_domain_leaves_ip_literal_untouched() {
        let m = setup();
        assert_eq!(
            call(&m, "stripDomain", &[s("192.168.1.1:9090")]).render_string(),
            "192.168.1.1:9090"
        );
    }

    #[test]
    fn match_returns_false_when_no_match() {
        let m = setup();
        assert!(!call(&m, "match", &[s("^z"), s("abc")]).truthy());
    }

    #[test]
    fn match_reports_bad_pattern_as_template_error() {
        let m = setup();
        let err = call_err(&m, "match", &[s("(unclosed"), s("abc")]);
        assert!(err.msg.contains("match"), "got: {}", err.msg);
    }

    #[test]
    fn re_replace_all_reports_bad_pattern_as_template_error() {
        let m = setup();
        let err = call_err(&m, "reReplaceAll", &[s("(unclosed"), s("x"), s("abc")]);
        assert!(err.msg.contains("reReplaceAll"), "got: {}", err.msg);
    }

    #[test]
    fn path_escape_matches_go_url_path_escape() {
        let m = setup();
        assert_eq!(
            call(&m, "pathEscape", &[s("a b/c?d")]).render_string(),
            "a%20b%2Fc%3Fd"
        );
    }

    #[test]
    fn query_escape_uses_plus_for_space() {
        let m = setup();
        assert_eq!(
            call(&m, "queryEscape", &[s("a b&c=d")]).render_string(),
            "a+b%26c%3Dd"
        );
    }

    #[test]
    fn safe_html_is_identity() {
        let m = setup();
        assert_eq!(
            call(&m, "safeHtml", &[s("<b>x</b>")]).render_string(),
            "<b>x</b>"
        );
    }

    #[test]
    fn wrong_arity_is_a_template_error_not_a_panic() {
        let m = setup();
        let err = call_err(&m, "toUpper", &[]);
        assert!(err.msg.contains("toUpper"), "got: {}", err.msg);
        let err = call_err(&m, "match", &[s("only-one")]);
        assert!(err.msg.contains("match"), "got: {}", err.msg);
    }
}
