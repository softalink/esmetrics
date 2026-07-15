//! Human-readable failure reporting for `esmalert-tool unittest`.
//!
//! Port of the presentation half of upstream vmalert-tool's per-file
//! "FAILED"/"SUCCESS" printing (`unittest.go:144-155`). Upstream's per-diff
//! detail (the `testGroupName:`/`groupname:`/`alertname:`/`exp:`/`got:` text)
//! is already built by [`crate::runner::check_alert_test_case`] and
//! [`crate::recording::check_metricsql_expr_test_case`] into each
//! [`crate::runner::GroupResult`] diff string; this module's only job is to
//! group those diff strings under a per-file/per-group heading for
//! [`crate::run_unittest`] to print.

/// Formats every diff from one `(file, group)` pair's failed test-group run
/// as a single human-readable block, e.g.:
///
/// ```text
/// --- FAIL: alerts_test.yml, group: t1
///     testGroupName: t1, groupname: g, alertname: InstanceDown, time: ...
///         exp: [...]
///         got: [...]
/// ```
///
/// Returns an empty string when `diffs` is empty — a passing group has
/// nothing to report; callers should skip calling this in that case anyway.
pub fn format_group_failures(file: &str, group: &str, diffs: &[String]) -> String {
    if diffs.is_empty() {
        return String::new();
    }
    let mut out = format!("--- FAIL: {file}, group: {group}\n");
    for diff in diffs {
        for line in diff.lines() {
            out.push_str("    ");
            out.push_str(line);
            out.push('\n');
        }
    }
    out.pop(); // Drop the trailing newline so callers can `println!` the result directly.
    out
}

#[cfg(test)]
mod tests {
    use super::format_group_failures;

    #[test]
    fn formats_a_header_and_indents_every_diff_line() {
        let out = format_group_failures(
            "alerts_test.yml",
            "t1",
            &["line1\nline2".to_string(), "another diff".to_string()],
        );
        assert!(
            out.starts_with("--- FAIL: alerts_test.yml, group: t1\n"),
            "unexpected header, got: {out:?}"
        );
        assert!(out.contains("    line1\n"), "got: {out:?}");
        assert!(out.contains("    line2\n"), "got: {out:?}");
        assert!(out.ends_with("    another diff"), "got: {out:?}");
    }

    #[test]
    fn empty_diffs_yields_an_empty_string() {
        assert_eq!(format_group_failures("f", "g", &[]), "");
    }
}
