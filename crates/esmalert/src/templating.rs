//! Shared Go-template preamble and validation for alerting/recording rules.
//!
//! Every annotation and label value in a rule is a Go template rendered with
//! vmalert's alert-specific data (`$value`, `$labels`, ...). Upstream declares
//! those template variables via a fixed header block prepended to each
//! template before parsing/executing (`app/vmalert/notifier/alert.go:93-106`,
//! `tplHeaders`). Without the preamble, a template like `{{ $value }}` fails
//! validation as an undefined variable, so nearly every real rule file would
//! be rejected.
//!
//! The renderer (Task 12) reuses [`TPL_HEADERS`] so validation and rendering
//! agree on exactly which variables are declared.

use esm_gotemplate::{Template, TemplateError};

/// The vmalert template preamble: 11 `{{ $var := .Field }}` declarations,
/// concatenated in upstream's order (`tplHeaders`, `alert.go:93-106`). This
/// is prepended to every user annotation/label template so the alert
/// variables resolve during validation and rendering.
pub const TPL_HEADERS: &str = "{{ $value := .Value }}{{ $type := .Type }}{{ $labels := .Labels }}{{ $expr := .Expr }}{{ $externalLabels := .ExternalLabels }}{{ $externalURL := .ExternalURL }}{{ $alertID := .AlertID }}{{ $groupID := .GroupID }}{{ $activeAt := .ActiveAt }}{{ $for := .For }}{{ $isPartial := .IsPartial }}";

/// Validates a single user template (an annotation or label value) by
/// prepending [`TPL_HEADERS`] and running `parse_and_validate`.
///
/// The deferred method-dispatch limitation still holds: a template using Go
/// time/duration method syntax (`.Sub`/`.Add`/`.UnixMilli`) will error here —
/// that surfaced error is the intended behavior, not something to work around.
pub fn validate_template(user_text: &str) -> Result<(), TemplateError> {
    Template::parse_and_validate(&format!("{TPL_HEADERS}{user_text}")).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_alert_variables() {
        validate_template("value is {{ $value }} on {{ $labels.instance }}").unwrap();
    }

    #[test]
    fn accepts_plain_text() {
        validate_template("just a string").unwrap();
    }

    #[test]
    fn rejects_malformed_template() {
        assert!(validate_template("{{ nope }").is_err());
    }

    #[test]
    fn rejects_unknown_function() {
        assert!(validate_template("{{ bogusFunc 1 }}").is_err());
    }
}
