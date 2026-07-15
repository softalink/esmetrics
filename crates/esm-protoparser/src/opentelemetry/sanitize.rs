//! Metric-name / label-name sanitization for OTLP ingestion.
//!
//! Port of upstream VictoriaMetrics v1.146.0
//! `lib/protoparser/opentelemetry/stream/sanitize.go`, with its flags
//! **fixed at their defaults** for this port:
//! `-opentelemetry.usePrometheusNaming=false` and
//! `-opentelemetry.convertMetricNamesToPrometheus=false`.
//!
//! ## What the flags would change (not ported — see below)
//!
//! `sanitizerContext.sanitizeMetricName` and `.sanitizeLabelName` both start
//! with an early return that is *always* taken at these fixed defaults:
//!
//! ```go
//! func (sctx *sanitizerContext) sanitizeLabelName(labelName string) string {
//!     if !*usePrometheusNaming {
//!         return labelName
//!     }
//!     return sctx.sanitizePrometheusLabelName(labelName)
//! }
//!
//! func (sctx *sanitizerContext) sanitizeMetricName(mm *pb.MetricMetadata) string {
//!     if !*usePrometheusNaming && !*convertMetricNamesToPrometheus {
//!         return mm.Name
//!     }
//!     return sctx.sanitizePrometheusMetricName(mm)
//! }
//! ```
//!
//! The unreachable `true`-branch logic (`sanitizePrometheusLabelName`,
//! `sanitizePrometheusMetricName`, the OTel-unit-to-Prometheus-unit maps
//! `unitMap`/`perUnitMap`, and the metric-name-token splitting/rejoining
//! machinery) is intentionally **not** ported, per the "simplicity first"
//! principle — dead code behind a permanently-false condition adds risk
//! without adding behavior. For the historical record, if
//! `usePrometheusNaming` were ever turned on, it would additionally:
//! - Rewrite label names via `promrelabel.SanitizeLabelName` (replacing
//!   characters outside `[a-zA-Z0-9_]` with `_`), prefixing `key_`/`key`
//!   when the sanitized name starts with a digit or a single underscore.
//! - Rewrite metric names into `token_token_..._token` form, splitting on
//!   `/_.-: `, inserting a unit suffix (translated through `unitMap`/
//!   `perUnitMap`, e.g. OTel `"ms"` → `"milliseconds"`), and appending
//!   `_total` for monotonic sums or `_ratio` for unitless (`"1"`) gauges.

/// Go: `sanitizerContext.sanitizeMetricName`. Always the identity function
/// at this port's fixed flag defaults (`usePrometheusNaming = false` and
/// `convertMetricNamesToPrometheus = false` both take the early-return
/// branch) — see the module doc for what the disabled branch would do.
pub fn sanitize_metric_name(name: &str) -> &str {
    name
}

/// Go: `sanitizerContext.sanitizeLabelName`. Always the identity function at
/// this port's fixed flag defaults (`usePrometheusNaming = false`) — see the
/// module doc for what the disabled branch would do.
pub fn sanitize_label_name(name: &str) -> &str {
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_metric_name_is_identity_at_fixed_defaults() {
        assert_eq!(sanitize_metric_name("my.metric/name"), "my.metric/name");
        assert_eq!(sanitize_metric_name(""), "");
    }

    #[test]
    fn sanitize_label_name_is_identity_at_fixed_defaults() {
        assert_eq!(sanitize_label_name("_private"), "_private");
        assert_eq!(sanitize_label_name("9lives"), "9lives");
    }
}
