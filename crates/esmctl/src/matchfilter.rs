//! Builds a per-metric MetricsQL match selector by combining the user's
//! `--vm-native-filter-match` with a specific metric name. Ports
//! `buildMatchWithFilter` from `vm_native.go` (using `esm-metricsql` in place
//! of `searchutil.ParseMetricSelector`).

use esm_metricsql::Expr;

/// Combines `filter` (a series selector) with `metric_name`, producing a new
/// selector that additionally pins `__name__` to `metric_name`. Ports
/// `buildMatchWithFilter`.
pub(crate) fn build_match_with_filter(filter: &str, metric_name: &str) -> Result<String, String> {
    let expr = esm_metricsql::parse(filter).map_err(|e| e.to_string())?;
    let Expr::Metric(me) = expr else {
        return Err(format!("expecting a series selector; got {filter:?}"));
    };

    if filter == metric_name || metric_name.is_empty() {
        return Ok(filter.to_string());
    }

    let name_filter = format!("__name__={}", quote(metric_name));

    let mut groups: Vec<String> = Vec::new();
    for lfs in &me.label_filterss {
        let mut parts: Vec<String> = Vec::new();
        for lf in lfs {
            // Skip the metric-name filter; it is re-added explicitly below
            // (ports the `len(tf.Key) == 0` skip, where storage encodes
            // `__name__` as the empty tag key).
            if lf.label.is_empty() || lf.label == "__name__" {
                continue;
            }
            let op = match (lf.is_negative, lf.is_regexp) {
                (true, true) => "!~",
                (true, false) => "!=",
                (false, true) => "=~",
                (false, false) => "=",
            };
            parts.push(format!("{}{}{}", lf.label, op, quote(&lf.value)));
        }
        parts.push(name_filter.clone());
        groups.push(parts.join(","));
    }

    Ok(format!("{{{}}}", groups.join(" or ")))
}

/// Go `%q` quoting for a label value.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_when_name_matches_filter() {
        assert_eq!(build_match_with_filter("foo", "foo").unwrap(), "foo");
    }

    #[test]
    fn empty_metric_name_returns_filter() {
        assert_eq!(
            build_match_with_filter("{job=\"a\"}", "").unwrap(),
            "{job=\"a\"}"
        );
    }

    #[test]
    fn adds_name_filter_to_selector() {
        let got = build_match_with_filter("{job=\"a\"}", "cpu").unwrap();
        assert_eq!(got, "{job=\"a\",__name__=\"cpu\"}");
    }

    #[test]
    fn preserves_operators() {
        let got = build_match_with_filter("{job!~\"a.*\"}", "cpu").unwrap();
        assert_eq!(got, "{job!~\"a.*\",__name__=\"cpu\"}");
    }

    #[test]
    fn drops_existing_name_filter() {
        let got = build_match_with_filter("bar{job=\"a\"}", "cpu").unwrap();
        assert_eq!(got, "{job=\"a\",__name__=\"cpu\"}");
    }
}
