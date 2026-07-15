//! The relabel apply engine: applies a single [`RelabelConfig`] to a label
//! set. Ports `applyRelabelConfig` (`lib/promrelabel/relabel.go`).
//!
//! This module implements the STANDARD actions (`replace`, `replace_all`,
//! `keep`, `drop`, `labelmap`, `labeldrop`, `labelkeep`, `hashmod`,
//! `lowercase`, `uppercase`) plus the equal/contains/metrics/`labelmap_all`
//! actions (`keepequal`, `dropequal`, `keep_if_equal`, `drop_if_equal`,
//! `keep_if_contains`, `drop_if_contains`, `keep_metrics`, `drop_metrics`,
//! `labelmap_all`), and the `graphite` action (see [`crate::graphite`]).
//! `if:` gating lives one layer up, in `ParsedConfigs::apply`, since it needs
//! to special-case `action: keep`/`action: drop` (see that function's doc
//! comment).

use crate::config::{Action, RelabelConfig};
use crate::label::{concat_source_values, get_label_value, set_label, Label};
use xxhash_rust::xxh64::xxh64;

/// Applies `cfg` to `labels` in place.
///
/// Returns `false` if the series should be dropped — only the `keep`/`drop`
/// family of actions can return `false`; every other action returns `true`.
pub fn apply_one(cfg: &RelabelConfig, labels: &mut Vec<Label>) -> bool {
    match cfg.action {
        Action::Replace => {
            apply_replace(cfg, labels);
            true
        }
        Action::ReplaceAll => {
            apply_replace_all(cfg, labels);
            true
        }
        Action::Keep => apply_keep(cfg, labels.as_slice()),
        Action::Drop => !apply_keep(cfg, labels.as_slice()),
        Action::Labelmap => {
            apply_labelmap(cfg, labels);
            true
        }
        Action::Labeldrop => {
            labels.retain(|l| !cfg.regex.is_match(&l.name));
            true
        }
        Action::Labelkeep => {
            labels.retain(|l| cfg.regex.is_match(&l.name));
            true
        }
        Action::Hashmod => {
            apply_hashmod(cfg, labels);
            true
        }
        Action::Lowercase => {
            apply_case(cfg, labels, str::to_lowercase);
            true
        }
        Action::Uppercase => {
            apply_case(cfg, labels, str::to_uppercase);
            true
        }
        Action::KeepEqual => apply_keep_equal(cfg, labels.as_slice()),
        Action::DropEqual => !apply_keep_equal(cfg, labels.as_slice()),
        Action::KeepIfEqual => apply_keep_if_equal(cfg, labels.as_slice()),
        Action::DropIfEqual => !apply_keep_if_equal(cfg, labels.as_slice()),
        Action::KeepIfContains => apply_keep_if_contains(cfg, labels.as_slice()),
        Action::DropIfContains => !apply_keep_if_contains(cfg, labels.as_slice()),
        Action::KeepMetrics => apply_keep_metrics(cfg, labels.as_slice()),
        Action::DropMetrics => !apply_keep_metrics(cfg, labels.as_slice()),
        Action::LabelmapAll => {
            apply_labelmap_all(cfg, labels);
            true
        }
        Action::Graphite => {
            crate::graphite::apply_graphite(cfg, labels);
            true
        }
    }
}

/// `replace`: sets `target_label` to `regex.replace_all(concat(source),
/// replacement)` when `regex` matches; `target_label` itself may reference
/// capture groups from the same match. Empty result removes the label.
fn apply_replace(cfg: &RelabelConfig, labels: &mut Vec<Label>) {
    let source = concat_source_values(labels, &cfg.source_labels, &cfg.separator);
    if !cfg.regex.is_match(&source) {
        return;
    }
    // Expand `{{labelName}}` references in the replacement BEFORE the regex
    // capture-group expansion, matching upstream's `hasLabelReferenceInReplacement`
    // fast-path guard (relabel.go:198-201) + `fillLabelReferences`
    // (relabel.go:637). `{{missing}}` fills to "". Only pay the scan/alloc
    // when the replacement actually contains a `{{`.
    let replacement = if cfg.replacement.contains("{{") {
        fill_label_references(&cfg.replacement, labels)
    } else {
        cfg.replacement.clone()
    };
    // `regex` is anchored (`^(?:...)$`), so it matches the whole source
    // exactly once; `replace_all` against that single match is equivalent
    // to upstream's `expandCaptureGroups(template, source, match)` for any
    // template — used here for both the value and a possibly-templated
    // target_label.
    let value = cfg
        .regex
        .replace_all(&source, replacement.as_str())
        .into_owned();
    let target_name = cfg
        .regex
        .replace_all(&source, &cfg.target_label)
        .into_owned();
    set_label(labels, &target_name, value);
}

/// Substitutes every `{{labelName}}` reference in `replacement` with that
/// label's value (a missing label yields ""). Ports `fillLabelReferences`
/// (relabel.go:637-660): text before an unterminated `{{` is emitted with the
/// literal `{{` preserved.
fn fill_label_references(replacement: &str, labels: &[Label]) -> String {
    let mut dst = String::new();
    let mut s = replacement;
    loop {
        let Some(open) = s.find("{{") else {
            dst.push_str(s);
            return dst;
        };
        dst.push_str(&s[..open]);
        s = &s[open + 2..];
        let Some(close) = s.find("}}") else {
            dst.push_str("{{");
            dst.push_str(s);
            return dst;
        };
        let label_name = &s[..close];
        s = &s[close + 2..];
        dst.push_str(get_label_value(labels, label_name));
    }
}

/// `replace_all`: replaces every (unanchored) regex match within
/// `concat(source)` with `replacement`, storing the result at
/// `target_label` — but only when the result actually differs from the
/// source, matching upstream's `valueStr != sourceStr` guard.
fn apply_replace_all(cfg: &RelabelConfig, labels: &mut Vec<Label>) {
    let source = concat_source_values(labels, &cfg.source_labels, &cfg.separator);
    // `AnchoredRegex` always anchors as `^(?:...)$`, which only ever
    // produces a single whole-string match — wrong for replace_all, which
    // must find every match inside the string. Recompile the raw pattern
    // unanchored. Compiling per apply_one call (rather than caching) is
    // acceptable for now per the task brief; `cfg.regex.original` was
    // already validated to compile (as the anchored form), so a compile
    // failure here should not happen in practice, but we still never
    // unwrap on it.
    let Ok(re) = regex::Regex::new(&cfg.regex.original) else {
        return;
    };
    let value = re.replace_all(&source, cfg.replacement.as_str());
    if value != source {
        set_label(labels, &cfg.target_label, value.into_owned());
    }
}

/// `keep`: keep the series iff `regex` matches `concat(source)`.
fn apply_keep(cfg: &RelabelConfig, labels: &[Label]) -> bool {
    let source = concat_source_values(labels, &cfg.source_labels, &cfg.separator);
    cfg.regex.is_match(&source)
}

/// `labelmap`: for every label whose NAME matches `regex`, add a new label
/// named `regex.replace_all(name, replacement)` with the old value.
fn apply_labelmap(cfg: &RelabelConfig, labels: &mut Vec<Label>) {
    let snapshot = labels.clone();
    for label in &snapshot {
        if !cfg.regex.is_match(&label.name) {
            continue;
        }
        let new_name = cfg
            .regex
            .replace_all(&label.name, &cfg.replacement)
            .into_owned();
        if new_name != label.name {
            set_label(labels, &new_name, label.value.clone());
        }
    }
}

/// `hashmod`: `target_label` = `xxh64(concat(source)) % modulus` as a decimal
/// string. Uses XXH64 with seed 0 to match upstream's
/// `xxhash.Sum64(concatLabelValues(...)) % modulus` (relabel.go:371,
/// `github.com/cespare/xxhash/v2`) exactly, so shard assignments are
/// bit-for-bit compatible with real vmagent/Prometheus hashmod sharding.
fn apply_hashmod(cfg: &RelabelConfig, labels: &mut Vec<Label>) {
    if cfg.modulus == 0 {
        // Upstream (`h := xxhash.Sum64(bb.B) % prc.Modulus`) would panic on
        // a zero modulus too; action-specific config validation (rejecting
        // `hashmod` without a modulus) is later work. Never panic here on
        // config content — no-op instead.
        return;
    }
    let source = concat_source_values(labels, &cfg.source_labels, &cfg.separator);
    let value = (xxh64(source.as_bytes(), 0) % cfg.modulus).to_string();
    set_label(labels, &cfg.target_label, value);
}

/// `lowercase`/`uppercase`: sets `target_label` to the Unicode-cased
/// `concat(source)`, matching Go's `strings.ToLower`/`ToUpper` (both are
/// Unicode-aware, not ASCII-only).
fn apply_case(cfg: &RelabelConfig, labels: &mut Vec<Label>, transform: fn(&str) -> String) {
    let source = concat_source_values(labels, &cfg.source_labels, &cfg.separator);
    set_label(labels, &cfg.target_label, transform(&source));
}

/// `keepequal`: keep iff `target_label`'s value equals `concat(source)`.
/// `dropequal` is the negation of this (`!apply_keep_equal`) — both branch
/// off the same comparison upstream (relabel.go:309-330).
fn apply_keep_equal(cfg: &RelabelConfig, labels: &[Label]) -> bool {
    let source = concat_source_values(labels, &cfg.source_labels, &cfg.separator);
    let target_value = get_label_value(labels, &cfg.target_label);
    source == target_value
}

/// `keep_if_equal`: keep iff every value in `source_labels` equals the
/// first one (relabel.go:285-308, `areEqualLabelValues`). `drop_if_equal`
/// is the negation.
///
/// Upstream's `areEqualLabelValues` panics (`logger.Panicf`) when
/// `source_labels` has fewer than 2 entries, treating that as an
/// unreachable config-validation bug (config.go rejects it before this
/// ever runs). Action-specific config validation is later work in this
/// crate, and this apply engine never panics on config content, so a
/// `source_labels` list with 0 or 1 entries here has nothing to disagree
/// with and is treated as trivially equal instead.
fn apply_keep_if_equal(cfg: &RelabelConfig, labels: &[Label]) -> bool {
    let Some(first_name) = cfg.source_labels.first() else {
        return true;
    };
    let first_value = get_label_value(labels, first_name);
    cfg.source_labels[1..]
        .iter()
        .all(|name| get_label_value(labels, name) == first_value)
}

/// `keep_if_contains`: keep iff `target_label`'s value contains each of
/// `source_labels`' values as a substring (relabel.go:259-284,
/// `containsAllLabelValues`). `drop_if_contains` is the negation. An empty
/// `source_labels` list has nothing to check, so it's vacuously satisfied —
/// matching Go's `for` loop over zero elements, which never hits the
/// `return false` and falls through to `return true`.
fn apply_keep_if_contains(cfg: &RelabelConfig, labels: &[Label]) -> bool {
    let target_value = get_label_value(labels, &cfg.target_label);
    cfg.source_labels
        .iter()
        .all(|name| target_value.contains(get_label_value(labels, name)))
}

/// `keep_metrics`/`drop_metrics`: sugar for `keep`/`drop` with
/// `source_labels: [__name__]`. Upstream rewrites the action at parse time
/// (config.go:363-380) into a plain `keep`/`drop` with `source_labels`
/// forced to `["__name__"]`, and validation there requires the YAML
/// `source_labels` to be empty for these two actions — so `cfg.source_labels`
/// and `cfg.target_label` are ignored here exactly as upstream ignores the
/// user-supplied ones.
fn apply_keep_metrics(cfg: &RelabelConfig, labels: &[Label]) -> bool {
    cfg.regex.is_match(get_label_value(labels, "__name__"))
}

/// `labelmap_all`: like `labelmap`, but renames EVERY label whose name the
/// regex matches anywhere (unanchored replace-all), not just the ones that
/// match — and unlike `labelmap`, the rename happens in place on
/// `label.name` rather than adding a new label alongside the original
/// (relabel.go:384-390, `replaceStringSubmatchesFast`). Renames are not
/// deduped against other labels in the set — upstream doesn't dedup here
/// either. See `apply_replace_all` above for why the anchored regex is
/// unusable for a replace-all and gets recompiled unanchored per call.
fn apply_labelmap_all(cfg: &RelabelConfig, labels: &mut [Label]) {
    let Ok(re) = regex::Regex::new(&cfg.regex.original) else {
        return;
    };
    for label in labels.iter_mut() {
        let new_name = re.replace_all(&label.name, cfg.replacement.as_str());
        if new_name != label.name {
            label.name = new_name.into_owned();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::label::get_label_value;
    use crate::regex::AnchoredRegex;

    fn labels(pairs: &[(&str, &str)]) -> Vec<Label> {
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect()
    }

    fn cfg(action: Action, src: &[&str], target: &str, regex: &str, repl: &str) -> RelabelConfig {
        RelabelConfig {
            source_labels: src.iter().map(|s| s.to_string()).collect(),
            separator: ";".into(),
            target_label: target.into(),
            regex: AnchoredRegex::compile(regex).unwrap(),
            modulus: 0,
            replacement: repl.into(),
            action,
            if_expr: None,
            graphite_match: None,
            graphite_labels: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn fill_label_references_matches_upstream() {
        // Ports the cases in Go's TestFillLabelReferences (relabel_test.go).
        let l = labels(&[("__name__", "foo"), ("bar", "baz")]);
        assert_eq!(fill_label_references("", &l), "");
        assert_eq!(fill_label_references("abc", &l), "abc");
        assert_eq!(fill_label_references("foo{{bar", &l), "foo{{bar");
        assert_eq!(fill_label_references("foo-$1", &l), "foo-$1");
        assert_eq!(fill_label_references("foo{{bar}}", &l), "foobaz");
        assert_eq!(fill_label_references("{{bar}}", &l), "baz");
        assert_eq!(fill_label_references("{{bar}}-aa", &l), "baz-aa");
        assert_eq!(
            fill_label_references("{{bar}}-aa{{__name__}}.{{bar}}{{missing}}", &l),
            "baz-aafoo.baz"
        );
    }

    #[test]
    fn replace_expands_label_references_in_replacement() {
        // relabel_test.go "replacement-with-label-refs, no regex":
        // replacement `{{__name__}}.{{foo}}` -> `qwe.bar`.
        let mut l = labels(&[("__name__", "qwe"), ("foo", "bar"), ("baz", "aaa")]);
        apply_one(
            &cfg(Action::Replace, &[], "abc", "(.*)", "{{__name__}}.{{foo}}"),
            &mut l,
        );
        assert_eq!(get_label_value(&l, "abc"), "qwe.bar");
    }

    #[test]
    fn replace_expands_label_references_then_capture_groups() {
        // relabel_test.go "with regex": replacement `{{__name__}}.{{foo}}.$1`
        // over source [baz]="aaa" with regex `a(.+)` -> `qwe.bar.aa`.
        let mut l = labels(&[("__name__", "qwe"), ("foo", "bar"), ("baz", "aaa")]);
        apply_one(
            &cfg(
                Action::Replace,
                &["baz"],
                "abc",
                "a(.+)",
                "{{__name__}}.{{foo}}.$1",
            ),
            &mut l,
        );
        assert_eq!(get_label_value(&l, "abc"), "qwe.bar.aa");
    }

    #[test]
    fn replace_sets_target() {
        let mut l = labels(&[("__name__", "http_requests"), ("code", "200")]);
        assert!(apply_one(
            &cfg(Action::Replace, &["code"], "code_class", "(.)..", "${1}xx"),
            &mut l
        ));
        assert_eq!(get_label_value(&l, "code_class"), "2xx");
    }

    #[test]
    fn keep_drops_non_matching() {
        let mut l = labels(&[("__name__", "up")]);
        assert!(apply_one(
            &cfg(Action::Keep, &["__name__"], "", "up", "$1"),
            &mut l
        )); // matches -> keep
        assert!(!apply_one(
            &cfg(Action::Keep, &["__name__"], "", "down", "$1"),
            &mut l
        )); // no match -> drop
    }

    #[test]
    fn labeldrop_removes_matching_labels() {
        let mut l = labels(&[
            ("__name__", "up"),
            ("tmp_a", "1"),
            ("tmp_b", "2"),
            ("keep", "3"),
        ]);
        apply_one(&cfg(Action::Labeldrop, &[], "", "tmp_.*", "$1"), &mut l);
        assert!(l.iter().all(|x| !x.name.starts_with("tmp_")));
        assert_eq!(get_label_value(&l, "keep"), "3");
    }

    #[test]
    fn hashmod_is_deterministic() {
        let mut c = cfg(Action::Hashmod, &["__name__"], "shard", "(.*)", "$1");
        c.modulus = 8;
        let mut l = labels(&[("__name__", "metric")]);
        apply_one(&c, &mut l);
        let v = get_label_value(&l, "shard").to_string();
        apply_one(&c, &mut labels(&[("__name__", "metric")])); // stable
        assert!(v.parse::<u64>().unwrap() < 8);
        // Pin the exact shard for a known input so a future hash-fn change
        // (e.g. swapping xxh64 for anything else) is caught. XXH64(b"metric",
        // seed=0) = 12615766905168682282; % 8 = 2, matching cespare/xxhash's
        // Sum64 used by upstream vmagent.
        assert_eq!(v, "2");
    }

    #[test]
    fn replace_all_replaces_every_match() {
        let mut l = labels(&[("path", "/a/b/c")]);
        assert!(apply_one(
            &cfg(Action::ReplaceAll, &["path"], "flat", "/", "_"),
            &mut l
        ));
        assert_eq!(get_label_value(&l, "flat"), "_a_b_c");
    }

    #[test]
    fn replace_all_is_noop_when_unchanged() {
        let mut l = labels(&[("path", "abc")]);
        assert!(apply_one(
            &cfg(Action::ReplaceAll, &["path"], "flat", "/", "_"),
            &mut l
        ));
        assert_eq!(get_label_value(&l, "flat"), "");
    }

    #[test]
    fn labelmap_adds_renamed_labels() {
        let mut l = labels(&[("__meta_kubernetes_node_label_zone", "us-east")]);
        apply_one(
            &cfg(
                Action::Labelmap,
                &[],
                "",
                "__meta_kubernetes_node_label_(.+)",
                "$1",
            ),
            &mut l,
        );
        assert_eq!(get_label_value(&l, "zone"), "us-east");
        // original label is untouched by labelmap
        assert_eq!(
            get_label_value(&l, "__meta_kubernetes_node_label_zone"),
            "us-east"
        );
    }

    #[test]
    fn labelkeep_removes_non_matching_labels() {
        let mut l = labels(&[("__name__", "up"), ("job", "x"), ("instance", "y")]);
        apply_one(
            &cfg(Action::Labelkeep, &[], "", "__name__|job", "$1"),
            &mut l,
        );
        assert!(l.iter().all(|x| x.name == "__name__" || x.name == "job"));
        assert_eq!(l.len(), 2);
    }

    #[test]
    fn drop_removes_matching_series() {
        let mut l = labels(&[("__name__", "debug_metric")]);
        assert!(!apply_one(
            &cfg(Action::Drop, &["__name__"], "", "debug_.*", "$1"),
            &mut l
        ));
        let mut l2 = labels(&[("__name__", "prod_metric")]);
        assert!(apply_one(
            &cfg(Action::Drop, &["__name__"], "", "debug_.*", "$1"),
            &mut l2
        ));
    }

    #[test]
    fn lowercase_and_uppercase_transform_target() {
        let mut l = labels(&[("Env", "PROD")]);
        apply_one(
            &cfg(Action::Lowercase, &["Env"], "env_lc", "(.*)", "$1"),
            &mut l,
        );
        assert_eq!(get_label_value(&l, "env_lc"), "prod");
        apply_one(
            &cfg(Action::Uppercase, &["Env"], "env_uc", "(.*)", "$1"),
            &mut l,
        );
        assert_eq!(get_label_value(&l, "env_uc"), "PROD");
    }

    #[test]
    fn keep_metrics_filters_by_name() {
        let mut l = labels(&[("__name__", "node_cpu")]);
        assert!(apply_one(
            &cfg(Action::KeepMetrics, &[], "", "node_.*", "$1"),
            &mut l
        ));
        assert!(!apply_one(
            &cfg(Action::KeepMetrics, &[], "", "http_.*", "$1"),
            &mut labels(&[("__name__", "node_cpu")])
        ));
    }

    #[test]
    fn keepequal_compares_target_to_source() {
        // target_label "a" equals concat(source ["b"]) -> keep
        let mut l = labels(&[("a", "x"), ("b", "x")]);
        assert!(apply_one(
            &cfg(Action::KeepEqual, &["b"], "a", "(.*)", "$1"),
            &mut l
        ));
        let mut l2 = labels(&[("a", "x"), ("b", "y")]);
        assert!(!apply_one(
            &cfg(Action::KeepEqual, &["b"], "a", "(.*)", "$1"),
            &mut l2
        ));
    }

    #[test]
    fn drop_metrics_drops_by_name() {
        assert!(!apply_one(
            &cfg(Action::DropMetrics, &[], "", "up", "$1"),
            &mut labels(&[("__name__", "up")])
        ));
        assert!(apply_one(
            &cfg(Action::DropMetrics, &[], "", "up", "$1"),
            &mut labels(&[("__name__", "down")])
        ));
    }

    #[test]
    fn dropequal_drops_when_target_matches_source() {
        let mut l = labels(&[("a", "x"), ("b", "x")]);
        assert!(!apply_one(
            &cfg(Action::DropEqual, &["b"], "a", "(.*)", "$1"),
            &mut l
        ));
        let mut l2 = labels(&[("a", "x"), ("b", "y")]);
        assert!(apply_one(
            &cfg(Action::DropEqual, &["b"], "a", "(.*)", "$1"),
            &mut l2
        ));
    }

    #[test]
    fn keep_if_equal_keeps_when_all_source_values_match() {
        let mut l = labels(&[("foo", "x"), ("bar", "x")]);
        assert!(apply_one(
            &cfg(Action::KeepIfEqual, &["foo", "bar"], "", "(.*)", "$1"),
            &mut l
        ));
        let mut l2 = labels(&[("foo", "x"), ("bar", "y")]);
        assert!(!apply_one(
            &cfg(Action::KeepIfEqual, &["foo", "bar"], "", "(.*)", "$1"),
            &mut l2
        ));
    }

    #[test]
    fn drop_if_equal_drops_when_all_source_values_match() {
        let mut l = labels(&[("foo", "x"), ("bar", "x")]);
        assert!(!apply_one(
            &cfg(Action::DropIfEqual, &["foo", "bar"], "", "(.*)", "$1"),
            &mut l
        ));
        let mut l2 = labels(&[("foo", "x"), ("bar", "y")]);
        assert!(apply_one(
            &cfg(Action::DropIfEqual, &["foo", "bar"], "", "(.*)", "$1"),
            &mut l2
        ));
    }

    #[test]
    fn keep_if_contains_keeps_when_target_contains_every_source_value() {
        // target_label "tags" contains both "req1" and "req2" values as substrings
        let mut l = labels(&[("tags", "prod,api,west"), ("req1", "prod"), ("req2", "api")]);
        assert!(apply_one(
            &cfg(
                Action::KeepIfContains,
                &["req1", "req2"],
                "tags",
                "(.*)",
                "$1"
            ),
            &mut l
        ));
        let mut l2 = labels(&[
            ("tags", "prod,api,west"),
            ("req1", "prod"),
            ("req2", "missing"),
        ]);
        assert!(!apply_one(
            &cfg(
                Action::KeepIfContains,
                &["req1", "req2"],
                "tags",
                "(.*)",
                "$1"
            ),
            &mut l2
        ));
    }

    #[test]
    fn drop_if_contains_drops_when_target_contains_every_source_value() {
        let mut l = labels(&[("tags", "prod,api,west"), ("req1", "prod"), ("req2", "api")]);
        assert!(!apply_one(
            &cfg(
                Action::DropIfContains,
                &["req1", "req2"],
                "tags",
                "(.*)",
                "$1"
            ),
            &mut l
        ));
        let mut l2 = labels(&[
            ("tags", "prod,api,west"),
            ("req1", "prod"),
            ("req2", "missing"),
        ]);
        assert!(apply_one(
            &cfg(
                Action::DropIfContains,
                &["req1", "req2"],
                "tags",
                "(.*)",
                "$1"
            ),
            &mut l2
        ));
    }

    #[test]
    fn labelmap_all_renames_every_label_via_unanchored_replace() {
        // Unlike `labelmap` (full-match, adds a new label alongside the
        // original), `labelmap_all` replaces every occurrence of the regex
        // inside each label NAME in place, so "foo_bar_baz" loses both
        // underscores and the original name is gone afterward.
        let mut l = labels(&[("foo_bar_baz", "1"), ("qux", "2")]);
        assert!(apply_one(
            &cfg(Action::LabelmapAll, &[], "", "_", "-"),
            &mut l
        ));
        assert_eq!(get_label_value(&l, "foo-bar-baz"), "1");
        assert!(l.iter().all(|x| x.name != "foo_bar_baz"));
        // No underscore in "qux" -> unanchored replace is a no-op.
        assert_eq!(get_label_value(&l, "qux"), "2");
    }
}
