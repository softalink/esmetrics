//! Minimal regexp syntax validation.
//!
//! The Go parser validates label-filter regexps via `regexp.Compile`
//! (`regexp_cache.go` / `labelFilterExpr.toLabelFilter`). This crate stays
//! dependency-free, so instead of compiling regexps it validates the basic
//! RE2 syntax: balanced groups, well-formed character classes, escapes and
//! repetition operators. Anything else is accepted.

use crate::ParseError;

/// Validates the syntax of the regexp `s`.
///
/// Loose port of the validation performed by Go's `CompileRegexpAnchored`:
/// only syntax errors are detected; the regexp is not compiled.
pub(crate) fn validate_regexp(s: &str) -> Result<(), ParseError> {
    let err = |msg: &str| {
        Err(ParseError::new(format!(
            "error parsing regexp: {msg}: {s:?}"
        )))
    };

    #[derive(PartialEq, Clone, Copy)]
    enum Last {
        /// Nothing repeatable precedes (start, after `(`, `|`).
        None,
        /// A repeatable item precedes.
        Item,
        /// A repetition operator precedes (may be followed by a single `?`).
        Repeat,
        /// A non-greedy repetition (`*?` etc.) precedes.
        RepeatQ,
    }

    let mut chars = s.chars().peekable();
    let mut depth: usize = 0;
    let mut last = Last::None;
    while let Some(c) = chars.next() {
        match c {
            '(' => {
                depth += 1;
                last = Last::None;
                // Lenient handling of group headers such as `(?:`, `(?i)`,
                // `(?P<name>`: consume the `?` so it isn't treated as a
                // repetition operator.
                if chars.peek() == Some(&'?') {
                    chars.next();
                }
            }
            ')' => {
                if depth == 0 {
                    return err("unexpected )");
                }
                depth -= 1;
                last = Last::Item;
            }
            '[' => {
                validate_char_class(&mut chars, s)?;
                last = Last::Item;
            }
            '\\' => {
                if chars.next().is_none() {
                    return err("trailing backslash at end of expression");
                }
                last = Last::Item;
            }
            '*' | '+' | '?' => {
                last = match last {
                    Last::Item => Last::Repeat,
                    Last::Repeat if c == '?' => Last::RepeatQ,
                    Last::None => return err("missing argument to repetition operator"),
                    _ => return err("invalid nested repetition operator"),
                };
            }
            '|' => {
                last = Last::None;
            }
            '{'
                // `{n}`, `{n,}` and `{n,m}` are repetitions; anything else is
                // a literal `{` (matching RE2 behavior).
                if try_consume_repetition(&mut chars) => {
                    last = match last {
                        Last::Item => Last::Repeat,
                        Last::None => return err("missing argument to repetition operator"),
                        _ => return err("invalid nested repetition operator"),
                    };
                }
            _ => {
                last = Last::Item;
            }
        }
    }
    if depth != 0 {
        return err("missing closing )");
    }
    Ok(())
}

/// Validates a character class after the opening `[`.
fn validate_char_class(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    s: &str,
) -> Result<(), ParseError> {
    let err = || {
        Err(ParseError::new(format!(
            "error parsing regexp: missing closing ]: {s:?}"
        )))
    };
    // Optional negation.
    if chars.peek() == Some(&'^') {
        chars.next();
    }
    // A `]` right after `[` or `[^` is a literal.
    if chars.peek() == Some(&']') {
        chars.next();
    }
    loop {
        let Some(c) = chars.next() else {
            return err();
        };
        match c {
            ']' => return Ok(()),
            '\\' => {
                if chars.next().is_none() {
                    return err();
                }
            }
            '['
                // Possible POSIX class such as `[:alpha:]`; otherwise `[` is
                // a literal inside a class.
                if chars.peek() == Some(&':') => {
                    chars.next();
                    loop {
                        let Some(c2) = chars.next() else {
                            return err();
                        };
                        if c2 == ':' && chars.peek() == Some(&']') {
                            chars.next();
                            break;
                        }
                    }
                }
            _ => {}
        }
    }
}

/// Attempts to consume `n}`, `n,}` or `n,m}` after a `{`. Returns true if a
/// valid repetition was consumed; on false the iterator is left unchanged.
fn try_consume_repetition(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let mut probe = chars.clone();
    let mut consumed = 0usize;
    let mut digits = 0usize;
    while probe.peek().is_some_and(char::is_ascii_digit) {
        probe.next();
        consumed += 1;
        digits += 1;
    }
    if digits == 0 {
        return false;
    }
    if probe.peek() == Some(&',') {
        probe.next();
        consumed += 1;
        while probe.peek().is_some_and(char::is_ascii_digit) {
            probe.next();
            consumed += 1;
        }
    }
    if probe.peek() != Some(&'}') {
        return false;
    }
    // Consume the validated repetition, including the closing brace.
    for _ in 0..=consumed {
        chars.next();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_regexps() {
        for s in [
            "",
            "x",
            "^x",
            "^x$",
            "^(a[bc]|d)$",
            "x.+y",
            "a.*",
            "foo|bar",
            "1.2.3.4",
            "a{3}",
            "a{3,}",
            "a{3,5}",
            "a{",
            "x{y}",
            "[^abc]",
            "[]a]",
            "(?i)foo",
            "(?:ab)+",
            "a+?",
            "\\d+",
            "[[:alpha:]]",
            "[a\\]b]",
        ] {
            assert!(validate_regexp(s).is_ok(), "expecting valid regexp {s:?}");
        }
    }

    #[test]
    fn invalid_regexps() {
        for s in [
            "x[",
            "x(",
            "x)",
            "x[a",
            "a\\",
            "*x",
            "+",
            "a**",
            "(a",
            "((a)",
            "[[:alpha:]",
        ] {
            assert!(
                validate_regexp(s).is_err(),
                "expecting invalid regexp {s:?}"
            );
        }
    }
}
