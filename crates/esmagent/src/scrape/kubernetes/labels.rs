//! `__meta_kubernetes_*` label/annotation helpers.
//!
//! Mirrors upstream vmagent's `registerLabelsAndAnnotations`
//! (`common_types.go`) and `SanitizeLabelName`
//! (`lib/promscrape/discoveryutil/util.go`).
//!
//! Upstream sanitizes the *whole* constructed label name
//! (`<prefix>_label_<labelname>`), not just the `<labelname>` portion. The
//! prefix and the `_label_`/`_labelpresent_`/`_annotation_`/
//! `_annotationpresent_` separators are already valid label-name
//! characters, so sanitizing the full string only ever rewrites the
//! characters that came from the k8s label/annotation name itself.

use std::collections::BTreeMap;

use super::object::ObjectMeta;

/// Replaces every character not in `[a-zA-Z0-9_]` with `_`.
pub fn sanitize_label_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Populates `out` with `<prefix>_label_*` / `<prefix>_labelpresent_*` and
/// `<prefix>_annotation_*` / `<prefix>_annotationpresent_*` entries for
/// every label and annotation on `meta`.
pub fn register_labels_and_annotations(
    prefix: &str,
    meta: &ObjectMeta,
    out: &mut BTreeMap<String, String>,
) {
    for (name, value) in &meta.labels {
        out.insert(
            sanitize_label_name(&format!("{prefix}_label_{name}")),
            value.clone(),
        );
        out.insert(
            sanitize_label_name(&format!("{prefix}_labelpresent_{name}")),
            "true".into(),
        );
    }
    for (name, value) in &meta.annotations {
        out.insert(
            sanitize_label_name(&format!("{prefix}_annotation_{name}")),
            value.clone(),
        );
        out.insert(
            sanitize_label_name(&format!("{prefix}_annotationpresent_{name}")),
            "true".into(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::kubernetes::object::ObjectMeta;

    #[test]
    fn sanitizes_invalid_label_chars() {
        assert_eq!(
            sanitize_label_name("app.kubernetes.io/name"),
            "app_kubernetes_io_name"
        );
        assert_eq!(sanitize_label_name("plain_ok9"), "plain_ok9");
    }

    #[test]
    fn registers_labels_annotations_and_present_markers() {
        let mut meta = ObjectMeta::default();
        meta.labels
            .insert("app.kubernetes.io/name".into(), "web".into());
        meta.annotations
            .insert("prometheus.io/scrape".into(), "true".into());
        let mut out = std::collections::BTreeMap::new();
        register_labels_and_annotations("__meta_kubernetes_pod", &meta, &mut out);
        assert_eq!(
            out["__meta_kubernetes_pod_label_app_kubernetes_io_name"],
            "web"
        );
        assert_eq!(
            out["__meta_kubernetes_pod_labelpresent_app_kubernetes_io_name"],
            "true"
        );
        assert_eq!(
            out["__meta_kubernetes_pod_annotation_prometheus_io_scrape"],
            "true"
        );
        assert_eq!(
            out["__meta_kubernetes_pod_annotationpresent_prometheus_io_scrape"],
            "true"
        );
    }
}
