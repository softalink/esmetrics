//! `esmauth_*` counters. Port of the `vmauth_*` counters from
//! `app/vmauth/main.go:672-677` and `auth_config.go:1053-1058`, renamed to
//! the `esm_*` convention (`vmauth_` -> `esmauth_`).
//!
//! # Security
//!
//! Metric label VALUES must never contain a raw secret (bearer token,
//! password, or `auth_token`). [`user_requests`] takes an already-safe
//! display name — see `proxy::user_label`, which mirrors upstream's
//! `UserInfo.name()` (`auth_config.go:1191-1209`): it returns the
//! configured `name`, else `username`, else a one-way hash of the bearer/auth
//! token (never the token itself).

use esm_common::metrics::{get_or_create_counter, Counter};

/// Port of `invalidAuthTokenRequests` (main.go:673). Not incremented by this
/// crate directly — `esm-auth::auth` only does map lookups; the caller that
/// owns the HTTP-level auth decision (the `esmauth` binary) increments this
/// on a failed [`crate::auth::AuthMap::lookup`].
pub fn invalid_auth_token_requests() -> &'static Counter {
    get_or_create_counter(r#"esmauth_http_request_errors_total{reason="invalid_auth_token"}"#)
}

/// Port of `missingRouteRequests` (main.go:674). Incremented by
/// [`crate::proxy::Proxy::proxy`] when [`crate::route::select_route`]
/// returns `None`.
pub fn missing_route_requests() -> &'static Counter {
    get_or_create_counter(r#"esmauth_http_request_errors_total{reason="missing_route"}"#)
}

/// Port of `rejectSlowClientRequests` (main.go:676). Not incremented by
/// this crate directly — slow-client rejection belongs to the request-body
/// buffering/queueing layer built in the `esmauth` binary (T8).
pub fn reject_slow_client_requests() -> &'static Counter {
    get_or_create_counter(r#"esmauth_http_request_errors_total{reason="reject_slow_client"}"#)
}

/// Port of `clientCanceledRequests` (main.go:675). Incremented by
/// [`crate::proxy::Proxy::proxy`] on a best-effort client-cancel detection
/// (a write failure while streaming the response back to the client).
pub fn client_canceled_requests() -> &'static Counter {
    get_or_create_counter(r#"esmauth_http_request_errors_total{reason="client_canceled"}"#)
}

/// Port of `concurrentRequestsLimitReached` (main.go:765). Not incremented
/// by this crate directly — concurrency limiting lives in the `esmauth`
/// binary (T8), which owns the limiter.
pub fn concurrent_requests_limit_reached() -> &'static Counter {
    get_or_create_counter("esmauth_concurrent_requests_limit_reached_total")
}

/// Port of `configReloadRequests` (main.go:672). Not incremented by this
/// crate directly — config reload is an `esmauth` binary (T8) concern.
pub fn config_reload_requests() -> &'static Counter {
    get_or_create_counter(r#"esmauth_http_requests_total{path="/-/reload"}"#)
}

/// Per-user request counter. Port of `UserInfo.requests`
/// (`auth_config.go:1053`, `vmauth_user_requests_total` + `getMetricLabels`).
/// `username` is `None` for the anonymous/unauthorized fallback user, which
/// mirrors upstream's `getMetricLabels` fast path: when the user has no
/// `name()`, the counter is registered with no labels at all rather than an
/// empty `username=""` label.
///
/// `username`, when `Some`, MUST already be a safe display value (see the
/// module security note) — never a raw bearer token or password.
pub fn user_requests(username: Option<&str>) -> &'static Counter {
    match username {
        Some(name) if !name.is_empty() => get_or_create_counter(&format!(
            r#"esmauth_user_requests_total{{username="{}"}}"#,
            escape_label(name)
        )),
        _ => get_or_create_counter("esmauth_user_requests_total"),
    }
}

/// Escapes `\`, `"`, and newlines in a label value (standard Prometheus label
/// escaping), defense-in-depth against a configured `name`/`username`
/// containing characters that would otherwise break the Prometheus exposition
/// line. Order matters: backslashes are escaped first so the later `\n`
/// substitution doesn't double-escape a backslash it just introduced.
fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_requests_with_name_uses_username_label() {
        let a = user_requests(Some("test_metrics_alice"));
        a.inc();
        let b = user_requests(Some("test_metrics_alice"));
        assert_eq!(b.get(), 1);
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn user_requests_without_name_uses_unlabeled_counter() {
        let a = user_requests(None);
        let b = user_requests(Some(""));
        assert!(
            std::ptr::eq(a, b),
            "empty and None must share the same counter"
        );
    }

    #[test]
    fn escape_label_escapes_quotes_and_backslashes() {
        assert_eq!(escape_label(r#"a"b\c"#), r#"a\"b\\c"#);
    }

    #[test]
    fn escape_label_escapes_newlines() {
        assert_eq!(escape_label("a\nb"), "a\\nb");

        // A label value containing a raw newline must not break the
        // exposition line: the counter's registered name (which embeds the
        // escaped label) must not contain a literal '\n' byte.
        let counter_name = format!(
            r#"esmauth_user_requests_total{{username="{}"}}"#,
            escape_label("evil\nesmauth_other_metric 1")
        );
        assert!(
            !counter_name.contains('\n'),
            "escaped label leaked a raw newline: {counter_name:?}"
        );
    }

    #[test]
    fn all_error_reason_counters_are_distinct() {
        invalid_auth_token_requests().inc();
        missing_route_requests().inc();
        reject_slow_client_requests().inc();
        client_canceled_requests().inc();
        // Each is registered under a distinct Prometheus key (different
        // `reason=` label value), so incrementing one must not affect the
        // others' independently-tracked totals from this point forward.
        let before = missing_route_requests().get();
        invalid_auth_token_requests().inc();
        assert_eq!(missing_route_requests().get(), before);
    }
}
