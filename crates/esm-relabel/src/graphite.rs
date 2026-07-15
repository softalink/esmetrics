//! The `graphite` relabel action, ported from `lib/promrelabel/graphite.go`.
//!
//! Unlike every other action, `graphite` matches a `*`-glob template (not a
//! RE2 regex) against the `__name__` label, captures the `*` groups, and
//! fills a `labels` map of `target_label -> template` where the template may
//! reference captures as `$1`/`${1}` (with `$0`/`${0}` being the whole
//! matched name).

use crate::config::RelabelConfig;
use crate::label::{get_label_value, set_label, Label};

/// Applies the `graphite` action to `labels` in place.
///
/// A `__name__` that doesn't match `cfg.graphite_match` leaves `labels`
/// untouched (relabel.go:174-193, the `!ok` fast path). Ports
/// `parsedRelabelConfig.apply`'s `"graphite"` case.
pub fn apply_graphite(cfg: &RelabelConfig, labels: &mut Vec<Label>) {
    let Some(match_template) = &cfg.graphite_match else {
        return;
    };
    let metric_name = get_label_value(labels, "__name__");
    let Some(captures) = match_graphite_template(match_template, metric_name) else {
        return;
    };
    for (target_label, replace_template) in &cfg.graphite_labels {
        let value = expand_graphite_template(replace_template, &captures);
        set_label(labels, target_label, value);
    }
}

/// Matches `s` against the `*`-glob `pattern`, returning the captures
/// (`captures[0]` is the whole input, `captures[1..]` are the `*` groups in
/// order) on success. Ports `newGraphiteMatchTemplate` +
/// `graphiteMatchTemplate.Match` (graphite.go:49-129).
fn match_graphite_template(pattern: &str, s: &str) -> Option<Vec<String>> {
    let parts = split_match_template(pattern);
    let mut captures = vec![s.to_string()];

    // Fast path: a literal (non-`*`) suffix must match.
    if let Some(last) = parts.last() {
        if last != "*" && !s.ends_with(last.as_str()) {
            return None;
        }
    }

    let mut rest = s;
    let mut i = 0;
    while i < parts.len() {
        let part = &parts[i];
        if part != "*" {
            if !rest.starts_with(part.as_str()) {
                return None;
            }
            rest = &rest[part.len()..];
            i += 1;
            continue;
        }
        if i + 1 >= parts.len() {
            // Matching the last part: `*` cannot span a `.`.
            if rest.contains('.') {
                return None;
            }
            captures.push(rest.to_string());
            return Some(captures);
        }
        let next = &parts[i + 1];
        i += 2;
        let n = rest.find(next.as_str())?;
        let captured = &rest[..n];
        if captured.contains('.') {
            return None;
        }
        captures.push(captured.to_string());
        rest = &rest[n + next.len()..];
    }
    if rest.is_empty() {
        Some(captures)
    } else {
        None
    }
}

/// Splits a match template into literal parts and `"*"` wildcard markers.
/// Ports `newGraphiteMatchTemplate` (graphite.go:49-74).
fn split_match_template(pattern: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = pattern;
    loop {
        match rest.find('*') {
            None => {
                push_nonempty(&mut parts, rest);
                break;
            }
            Some(n) => {
                push_nonempty(&mut parts, &rest[..n]);
                parts.push("*".to_string());
                rest = &rest[n + 1..];
            }
        }
    }
    parts
}

fn push_nonempty(parts: &mut Vec<String>, s: &str) {
    if !s.is_empty() {
        parts.push(s.to_string());
    }
}

/// One piece of a compiled `graphiteReplaceTemplate`: either literal text, or
/// a reference to `captures[idx]` (falling back to the original literal text
/// when `idx` is out of range, matching Go's `if n >= 0 && n < len(matches)`
/// guard).
struct ReplacePart {
    text: String,
    idx: Option<usize>,
}

/// Expands `template` (e.g. `"${2}_total"` or `"$1-$2"`) against `captures`.
/// Ports `newGraphiteReplaceTemplate` + `graphiteReplaceTemplate.Expand`
/// (graphite.go:145-215).
fn expand_graphite_template(template: &str, captures: &[String]) -> String {
    let parts = split_replace_template(template);
    let mut out = String::new();
    for part in &parts {
        match part.idx {
            Some(idx) if idx < captures.len() => out.push_str(&captures[idx]),
            _ => out.push_str(&part.text),
        }
    }
    out
}

fn split_replace_template(template: &str) -> Vec<ReplacePart> {
    let mut parts = Vec::new();
    let mut rest = template;
    loop {
        let Some(n) = rest.find('$') else {
            push_replace_part(&mut parts, rest, None);
            break;
        };
        if n > 0 {
            push_replace_part(&mut parts, &rest[..n], None);
        }
        rest = &rest[n + 1..];
        if let Some(after_brace) = rest.strip_prefix('{') {
            match after_brace.find('}') {
                None => {
                    push_replace_part(&mut parts, &format!("${rest}"), None);
                    break;
                }
                Some(close) => {
                    let idx_str = &after_brace[..close];
                    rest = &after_brace[close + 1..];
                    let idx = idx_str.parse::<usize>().ok();
                    push_replace_part(&mut parts, &format!("${{{idx_str}}}"), idx);
                }
            }
        } else {
            let digit_end = rest
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len());
            let idx_str = &rest[..digit_end];
            rest = &rest[digit_end..];
            let idx = idx_str.parse::<usize>().ok();
            push_replace_part(&mut parts, &format!("${idx_str}"), idx);
        }
    }
    parts
}

fn push_replace_part(parts: &mut Vec<ReplacePart>, text: &str, idx: Option<usize>) {
    if !text.is_empty() {
        parts.push(ReplacePart {
            text: text.to_string(),
            idx,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_and_expand_examples_from_upstream_tests() {
        // Ported verbatim from graphite_test.go's TestGraphiteTemplateMatchExpand.
        let cases = [
            (
                "test.*.*.counter",
                "test.foo.bar.counter",
                "${2}_total",
                "bar_total",
            ),
            (
                "test.*.*.counter",
                "test.foo.bar.counter",
                "$1_total",
                "foo_total",
            ),
            (
                "test.*.*.counter",
                "test.foo.bar.counter",
                "total_$0",
                "total_test.foo.bar.counter",
            ),
            (
                "test.dispatcher.*.*.*",
                "test.dispatcher.foo.bar.baz",
                "$3-$2-$1",
                "baz-bar-foo",
            ),
            (
                "*.signup.*.*",
                "foo.signup.bar.baz",
                "$1-${3}_$2_total",
                "foo-baz_bar_total",
            ),
        ];
        for (match_tpl, s, replace_tpl, expected) in cases {
            let captures = match_graphite_template(match_tpl, s).unwrap();
            assert_eq!(expand_graphite_template(replace_tpl, &captures), expected);
        }
    }

    #[test]
    fn match_template_examples_from_upstream_tests() {
        // Ported from graphite_test.go's TestGraphiteMatchTemplateMatch.
        assert_eq!(match_graphite_template("", ""), Some(vec!["".to_string()]));
        assert_eq!(match_graphite_template("", "foobar"), None);
        assert_eq!(
            match_graphite_template("foo", "foo"),
            Some(vec!["foo".to_string()])
        );
        assert_eq!(match_graphite_template("foo", ""), None);
        assert_eq!(
            match_graphite_template("*", "foobar"),
            Some(vec!["foobar".to_string(), "foobar".to_string()])
        );
        assert_eq!(match_graphite_template("**", "foobar"), None); // consecutive stars never match, matching upstream
        assert_eq!(match_graphite_template("*", "foo.bar"), None); // `*` cannot span a dot
        assert_eq!(
            match_graphite_template("*foo", "barfoo"),
            Some(vec!["barfoo".to_string(), "bar".to_string()])
        );
        assert_eq!(
            match_graphite_template("*.*.baz", "foo.bar.baz"),
            Some(vec![
                "foo.bar.baz".to_string(),
                "foo".to_string(),
                "bar".to_string()
            ])
        );
        assert_eq!(match_graphite_template("*.bar", "foo.bar.baz"), None);
    }

    #[test]
    fn apply_graphite_sets_labels_from_captures() {
        use crate::config::Action;
        use crate::regex::AnchoredRegex;
        use std::collections::BTreeMap;

        let mut labels_map = BTreeMap::new();
        labels_map.insert("job".to_string(), "${1}-zz".to_string());
        let cfg = RelabelConfig {
            source_labels: vec![],
            separator: ";".into(),
            target_label: "".into(),
            regex: AnchoredRegex::compile("(.*)").unwrap(),
            modulus: 0,
            replacement: "$1".into(),
            action: Action::Graphite,
            if_expr: None,
            graphite_match: Some("foo.*.baz".to_string()),
            graphite_labels: labels_map,
        };
        let mut l = vec![Label {
            name: "__name__".into(),
            value: "foo.bar.baz".into(),
        }];
        apply_graphite(&cfg, &mut l);
        assert_eq!(get_label_value(&l, "job"), "bar-zz");

        // Mismatch leaves labels untouched.
        let mut l2 = vec![Label {
            name: "__name__".into(),
            value: "foo.bar.bazz".into(),
        }];
        apply_graphite(&cfg, &mut l2);
        assert_eq!(get_label_value(&l2, "job"), "");
    }
}
