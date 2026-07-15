//! The working label representation used by the apply engine, plus the
//! small label-manipulation helpers shared across relabel actions. Ports
//! `getLabelValue`, `concatLabelValues`, and `setLabelValue` from
//! `lib/promrelabel/relabel.go`.

/// A single label (name/value pair) being relabeled. `__name__` is
/// represented as an ordinary label here, matching upstream's `prompb.Label`.
/// `Serialize` (not `Deserialize` — nothing in this crate deserializes a
/// `Label` directly) so callers can embed it in a serde-serializable
/// reporting shape (e.g. esmagent's `/targets` snapshot).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

/// Returns the value of the label named `name`, or `""` if absent.
/// Ports `getLabelValue` (relabel.go).
pub(crate) fn get_label_value<'a>(labels: &'a [Label], name: &str) -> &'a str {
    labels
        .iter()
        .find(|l| l.name == name)
        .map(|l| l.value.as_str())
        .unwrap_or("")
}

/// Joins the values of `source_labels` (in declared order) with `separator`.
/// A missing label contributes an empty string. Returns `""` when
/// `source_labels` is empty. Ports `concatLabelValues` (relabel.go).
pub(crate) fn concat_source_values(
    labels: &[Label],
    source_labels: &[String],
    separator: &str,
) -> String {
    if source_labels.is_empty() {
        return String::new();
    }
    source_labels
        .iter()
        .map(|name| get_label_value(labels, name))
        .collect::<Vec<_>>()
        .join(separator)
}

/// Sets `name` to `value`, replacing an existing label in place or pushing a
/// new one. An empty `value` removes the label instead of storing it.
///
/// Ports `setLabelValue` (relabel.go), folding in the empty-value removal
/// that upstream defers to a separate `removeEmptyLabels` pass run once
/// after a whole `relabel_configs` list has been applied. Per-rule removal
/// is the contract for this crate's `apply_one` (see task brief); the
/// deferred-vs-immediate distinction only matters for labels that are
/// re-read by a later rule in the same list, which is out of scope here —
/// that orchestration lives in the `ParsedConfigs`/`Apply` layer built in
/// later work.
pub(crate) fn set_label(labels: &mut Vec<Label>, name: &str, value: String) {
    if value.is_empty() {
        remove_label(labels, name);
        return;
    }
    if let Some(existing) = labels.iter_mut().find(|l| l.name == name) {
        existing.value = value;
    } else {
        labels.push(Label {
            name: name.to_string(),
            value,
        });
    }
}

/// Removes the label named `name`, if present.
pub(crate) fn remove_label(labels: &mut Vec<Label>, name: &str) {
    labels.retain(|l| l.name != name);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_label_value_returns_empty_string_when_missing() {
        let labels = vec![Label {
            name: "a".into(),
            value: "1".into(),
        }];
        assert_eq!(get_label_value(&labels, "missing"), "");
    }

    #[test]
    fn concat_source_values_joins_with_separator() {
        let labels = vec![
            Label {
                name: "a".into(),
                value: "1".into(),
            },
            Label {
                name: "b".into(),
                value: "2".into(),
            },
        ];
        let joined = concat_source_values(&labels, &["a".to_string(), "b".to_string()], ";");
        assert_eq!(joined, "1;2");
    }

    #[test]
    fn set_label_with_empty_value_removes_the_label() {
        let mut labels = vec![Label {
            name: "a".into(),
            value: "1".into(),
        }];
        set_label(&mut labels, "a", String::new());
        assert!(labels.is_empty());
    }
}
