//! Anchored regex wrapper, matching the semantics of `regexp.Compile("^(?:" + pattern + ")$")`
//! in `lib/promrelabel/config.go`.

use crate::RelabelError;
use std::borrow::Cow;

/// A regex that is always anchored to match the whole input string, plus the
/// original (unanchored) pattern it was compiled from.
#[derive(Debug, Clone)]
pub struct AnchoredRegex {
    re: ::regex::Regex,
    pub original: String,
}

impl AnchoredRegex {
    /// Compiles `pattern`, anchoring it as `^(?:<pattern>)$`.
    pub fn compile(pattern: &str) -> Result<AnchoredRegex, RelabelError> {
        let anchored = format!("^(?:{pattern})$");
        let re = ::regex::Regex::new(&anchored).map_err(|e| RelabelError {
            msg: format!("cannot parse regex {pattern:?}: {e}"),
        })?;
        Ok(AnchoredRegex {
            re,
            original: pattern.to_string(),
        })
    }

    /// Returns whether the whole input string matches.
    pub fn is_match(&self, s: &str) -> bool {
        self.re.is_match(s)
    }

    /// Replaces all (anchored, so at most one) matches of the whole string with `rep`.
    pub fn replace_all<'t>(&self, s: &'t str, rep: &str) -> Cow<'t, str> {
        self.re.replace_all(s, rep)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_is_anchored() {
        let re = AnchoredRegex::compile("foo.*").unwrap();
        assert!(re.is_match("foobar"));
        assert!(!re.is_match("xfoobar")); // anchored: must match whole string
    }
}
