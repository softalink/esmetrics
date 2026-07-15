//! The `if:` selector attached to a relabel config, ported from
//! `lib/promrelabel/if_expression.go`.
//!
//! An `if:` value is a metricsql-style series selector — either a bare
//! metric name (`up`), a label matcher (`{__name__="up", job="x"}`), or an
//! `or`-delimited list of filter groups inside the braces
//! (`{job="a" or job="b"}`). [`IfExpression::matches`] returns true when the
//! label set matches ANY of the or-groups, and each group matches only when
//! ALL of its filters match (upstream `ifExpression.Match` /
//! `matchLabelFilters`).

use crate::label::Label;
use crate::regex::AnchoredRegex;
use crate::RelabelError;
use esm_metricsql::{Expr, LabelFilter};

/// A compiled `if:` selector.
#[derive(Debug, Clone)]
pub struct IfExpression {
    /// Or-delimited groups of label filters; matches if any group matches.
    groups: Vec<Vec<CompiledFilter>>,
}

impl IfExpression {
    /// Parses `s` as a metricsql series selector and compiles its label
    /// filters. Ports `ifExpression.Parse` (if_expression.go:178-194).
    pub fn parse(s: &str) -> Result<IfExpression, RelabelError> {
        let expr = esm_metricsql::parse(s).map_err(|e| RelabelError {
            msg: format!("cannot parse `if` series selector {s:?}: {e}"),
        })?;
        let Expr::Metric(me) = expr else {
            return Err(RelabelError {
                msg: format!("expecting series selector for `if`; got {expr}"),
            });
        };
        let mut groups = Vec::with_capacity(me.label_filterss.len());
        for lfs in &me.label_filterss {
            let mut compiled = Vec::with_capacity(lfs.len());
            for lf in lfs {
                compiled.push(CompiledFilter::compile(lf)?);
            }
            groups.push(compiled);
        }
        Ok(IfExpression { groups })
    }

    /// Parses a list of `if:` selectors, OR-combining them. Ports
    /// `IfExpression.unmarshalFromInterface`'s `[]any` case
    /// (if_expression.go:104-116): each selector is parsed independently and
    /// the overall expression matches when ANY selector matches. Since each
    /// selector already contributes its own or-groups, flattening every
    /// selector's groups into one list preserves that OR semantics.
    pub fn parse_list(selectors: &[String]) -> Result<IfExpression, RelabelError> {
        let mut groups = Vec::new();
        for s in selectors {
            groups.extend(IfExpression::parse(s)?.groups);
        }
        Ok(IfExpression { groups })
    }

    /// Returns true if `labels` matches at least one or-group.
    /// Ports `ifExpression.Match` (if_expression.go:238-248). An expression
    /// with no groups (only reachable via an empty `if: []` list) matches
    /// everything, mirroring Go's `len(ie.ies) == 0` short-circuit to `true`.
    pub fn matches(&self, labels: &[Label]) -> bool {
        if self.groups.is_empty() {
            return true;
        }
        self.groups
            .iter()
            .any(|group| group.iter().all(|f| f.matches(labels)))
    }
}

/// A single compiled `label <op> "value"` filter. Ports `labelFilter`
/// (if_expression.go:276-299).
///
/// Upstream canonicalizes `__name__` to an empty-string label before
/// matching and special-cases the empty label to search by metric name
/// instead; this crate's [`Label`] already stores the metric name as an
/// ordinary label named `__name__`, so matching by `label` directly (no
/// canonicalization) is equivalent and simpler.
#[derive(Debug, Clone)]
struct CompiledFilter {
    label: String,
    value: String,
    is_negative: bool,
    is_regexp: bool,
    /// Anchored regex, compiled only for `=~`/`!~` filters.
    re: Option<AnchoredRegex>,
}

impl CompiledFilter {
    fn compile(lf: &LabelFilter) -> Result<CompiledFilter, RelabelError> {
        let re = if lf.is_regexp {
            Some(AnchoredRegex::compile(&lf.value)?)
        } else {
            None
        };
        Ok(CompiledFilter {
            label: lf.label.clone(),
            value: lf.value.clone(),
            is_negative: lf.is_negative,
            is_regexp: lf.is_regexp,
            re,
        })
    }

    fn matches(&self, labels: &[Label]) -> bool {
        match (self.is_negative, self.is_regexp) {
            (false, false) => self.equal_value(labels),
            (true, false) => !self.equal_value(labels),
            (false, true) => self.match_regexp(labels),
            (true, true) => !self.match_regexp(labels),
        }
    }

    /// Ports `labelFilter.equalValue` (if_expression.go:326-345): matches if
    /// any label named `self.label` has the exact value, or — when no such
    /// label exists — matches iff `self.value` is empty (`{missing=""}`
    /// matches a non-existing label).
    fn equal_value(&self, labels: &[Label]) -> bool {
        let mut name_matches = 0;
        for l in labels {
            if l.name != self.label {
                continue;
            }
            name_matches += 1;
            if l.value == self.value {
                return true;
            }
        }
        if name_matches == 0 {
            return self.value.is_empty();
        }
        false
    }

    /// Ports `labelFilter.matchRegexp` (if_expression.go:347-363): same
    /// missing-label special case as `equal_value`, but tests the regexp
    /// against the empty string instead of an empty `self.value`.
    fn match_regexp(&self, labels: &[Label]) -> bool {
        let re = self
            .re
            .as_ref()
            .expect("BUG: regexp filter has no compiled regex");
        let mut name_matches = 0;
        for l in labels {
            if l.name != self.label {
                continue;
            }
            name_matches += 1;
            if re.is_match(&l.value) {
                return true;
            }
        }
        if name_matches == 0 {
            return re.is_match("");
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Vec<Label> {
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect()
    }

    #[test]
    fn bare_metric_name_matches_by_name() {
        let ie = IfExpression::parse("up").unwrap();
        assert!(ie.matches(&labels(&[("__name__", "up")])));
        assert!(!ie.matches(&labels(&[("__name__", "down")])));
    }

    #[test]
    fn label_matcher_requires_all_filters() {
        let ie = IfExpression::parse(r#"{__name__="up", job="a"}"#).unwrap();
        assert!(ie.matches(&labels(&[("__name__", "up"), ("job", "a")])));
        assert!(!ie.matches(&labels(&[("__name__", "up"), ("job", "b")])));
    }

    #[test]
    fn regexp_filters_match() {
        let ie = IfExpression::parse(r#"{job=~"a|b"}"#).unwrap();
        assert!(ie.matches(&labels(&[("job", "a")])));
        assert!(ie.matches(&labels(&[("job", "b")])));
        assert!(!ie.matches(&labels(&[("job", "c")])));
    }

    #[test]
    fn negated_filters_match() {
        let ie = IfExpression::parse(r#"{job!="a"}"#).unwrap();
        assert!(!ie.matches(&labels(&[("job", "a")])));
        assert!(ie.matches(&labels(&[("job", "b")])));
    }

    #[test]
    fn or_group_matches_if_any_group_matches() {
        let ie = IfExpression::parse(r#"{env="prod" or env="staging"}"#).unwrap();
        assert!(ie.matches(&labels(&[("env", "prod")])));
        assert!(ie.matches(&labels(&[("env", "staging")])));
        assert!(!ie.matches(&labels(&[("env", "dev")])));
    }

    #[test]
    fn non_selector_expression_is_rejected() {
        assert!(IfExpression::parse("1 + 1").is_err());
    }
}
