//! Route table, JSON envelopes, and auth-key gating for esmalert's read-only
//! JSON API. Faithful (narrowed) subset of upstream `app/vmalert/web.go:
//! 138-224` — see that file (checked into this repo under
//! `.upstream/VictoriaMetrics/app/vmalert/web.go`) for the reference
//! endpoint/JSON shapes this ports.
//!
//! # Deviations from upstream, and why
//!
//! Upstream keys single-object lookups (`/api/v1/rule`, `/api/v1/alert`,
//! `/api/v1/group`) by numeric group/rule/alert IDs computed from a
//! `manager` that this crate's [`crate::manager::Manager`] doesn't build
//! (see its doc comment: numeric ID bookkeeping is explicitly out of
//! scope). This port instead looks groups up by `name` (already a stable,
//! unique key here — see `Manager`'s doc comment) and rules up by
//! [`RuleView::id`] (the same `config::rule_identity_hash` upstream's
//! `HashRule` computes) within that group. [`AlertView`] carries no unique
//! id at all yet, so `/api/v1/alert` matches by `(group, alertname)` and
//! returns the first match; a group with multiple concurrently-firing
//! instances of the same alert (distinct label sets) only exposes one via
//! this endpoint. `/api/v1/notifiers` returns an empty list: `Manager`
//! exposes no notifier accessor yet (its `deps` field, which would hold the
//! `Notifiers`, is private), so there is no metadata to report — the task
//! brief's explicitly allowed fallback for this endpoint.
//!
//! Single-object endpoints also drop upstream's `ApiRuleWithUpdates` (state
//! update history isn't tracked anywhere in this crate yet) and return the
//! bare `RuleView`/`AlertView`/`GroupView` instead — matching upstream's own
//! "no envelope" shape for these three endpoints (see `marshalJson` call
//! sites in `web.go`), just with a narrower payload.

use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_channel::Sender;
use esm_http::{Method, Request, ResponseWriter};
use serde::Serialize;

use crate::manager::{GroupView, Manager};
use crate::rule::AlertView;

/// Shared state every request handler needs. Built once in
/// [`super::serve`] and shared (via `Arc`) across every connection thread.
pub(super) struct Context {
    pub(super) mgr: Arc<Mutex<Manager>>,
    pub(super) reload_auth_key: Option<String>,
    pub(super) metrics_auth_key: Option<String>,
    pub(super) reload_tx: Sender<()>,
}

/// Top-level request router. Never panics: every fallible step (query-param
/// parsing, JSON encoding, the reload send) is handled explicitly and turned
/// into a JSON error response rather than an `unwrap`/`expect`.
pub(super) fn handle(ctx: &Context, req: &mut Request<'_>, w: &mut ResponseWriter<'_>) {
    let path = strip_vmalert_prefix(req.path()).to_string();
    match (req.method(), path.as_str()) {
        (Method::Get, "/api/v1/rules") => handle_rules(ctx, w),
        (Method::Get, "/api/v1/alerts") => handle_alerts(ctx, w),
        (Method::Get, "/api/v1/rule") => handle_rule(ctx, req, w),
        (Method::Get, "/api/v1/alert") => handle_alert(ctx, req, w),
        (Method::Get, "/api/v1/group") => handle_group(ctx, req, w),
        (Method::Get, "/api/v1/notifiers") => handle_notifiers(w),
        (Method::Get, "/metrics") => handle_metrics(ctx, req, w),
        (Method::Get, "/-/healthy") => handle_healthy(w),
        (Method::Post, "/-/reload") => handle_reload(ctx, req, w),
        _ => write_error(w, 404, "unsupported path"),
    }
}

/// Upstream serves `/api/v1/*` under both a bare path and a `/vmalert`-
/// prefixed alias (`web.go:138-223`, e.g. `"/vmalert/api/v1/rules",
/// "/api/v1/rules"`). Normalizing here lets every match arm above handle
/// both without duplicating the route table.
fn strip_vmalert_prefix(path: &str) -> &str {
    path.strip_prefix("/vmalert").unwrap_or(path)
}

// -- list endpoints -----------------------------------------------------

#[derive(Serialize)]
struct RulesResponse {
    status: &'static str,
    data: RulesData,
}

#[derive(Serialize)]
struct RulesData {
    groups: Vec<GroupView>,
}

fn handle_rules(ctx: &Context, w: &mut ResponseWriter<'_>) {
    let groups = lock_mgr(&ctx.mgr).groups_snapshot();
    write_json(
        w,
        200,
        &RulesResponse {
            status: "success",
            data: RulesData { groups },
        },
    );
}

#[derive(Serialize)]
struct AlertsResponse {
    status: &'static str,
    data: AlertsData,
}

#[derive(Serialize)]
struct AlertsData {
    alerts: Vec<AlertView>,
}

fn handle_alerts(ctx: &Context, w: &mut ResponseWriter<'_>) {
    let alerts = lock_mgr(&ctx.mgr).alerts_snapshot();
    write_json(
        w,
        200,
        &AlertsResponse {
            status: "success",
            data: AlertsData { alerts },
        },
    );
}

/// See the module doc's "Deviations from upstream" section: `Manager`
/// exposes no notifier metadata yet, so this always reports an empty list
/// rather than fabricating targets.
#[derive(Serialize)]
struct NotifiersResponse {
    status: &'static str,
    data: NotifiersData,
}

#[derive(Serialize)]
struct NotifiersData {
    notifiers: Vec<()>,
}

fn handle_notifiers(w: &mut ResponseWriter<'_>) {
    write_json(
        w,
        200,
        &NotifiersResponse {
            status: "success",
            data: NotifiersData {
                notifiers: Vec::new(),
            },
        },
    );
}

// -- single-object lookups -----------------------------------------------

fn handle_group(ctx: &Context, req: &Request<'_>, w: &mut ResponseWriter<'_>) {
    let Some(name) = query_param(req, "group") else {
        write_error(w, 400, "missing required parameter \"group\"");
        return;
    };
    let groups = lock_mgr(&ctx.mgr).groups_snapshot();
    match groups.into_iter().find(|g| g.name == name) {
        Some(group) => write_json(w, 200, &group),
        None => write_error(w, 404, &format!("group {name:?} not found")),
    }
}

fn handle_rule(ctx: &Context, req: &Request<'_>, w: &mut ResponseWriter<'_>) {
    let Some(group_name) = query_param(req, "group") else {
        write_error(w, 400, "missing required parameter \"group\"");
        return;
    };
    let Some(rule_id_str) = query_param(req, "rule_id") else {
        write_error(w, 400, "missing required parameter \"rule_id\"");
        return;
    };
    let Ok(rule_id) = rule_id_str.parse::<u64>() else {
        write_error(w, 400, "invalid \"rule_id\": not a valid u64");
        return;
    };

    let groups = lock_mgr(&ctx.mgr).groups_snapshot();
    let Some(group) = groups.into_iter().find(|g| g.name == group_name) else {
        write_error(w, 404, &format!("group {group_name:?} not found"));
        return;
    };
    match group.rules.into_iter().find(|r| r.id == rule_id) {
        Some(rule) => write_json(w, 200, &rule),
        None => write_error(
            w,
            404,
            &format!("rule_id {rule_id} not found in group {group_name:?}"),
        ),
    }
}

fn handle_alert(ctx: &Context, req: &Request<'_>, w: &mut ResponseWriter<'_>) {
    let Some(group_name) = query_param(req, "group") else {
        write_error(w, 400, "missing required parameter \"group\"");
        return;
    };
    let Some(alert_name) = query_param(req, "alert") else {
        write_error(w, 400, "missing required parameter \"alert\"");
        return;
    };

    let alerts = lock_mgr(&ctx.mgr).alerts_snapshot();
    match alerts
        .into_iter()
        .find(|a| a.group == group_name && a.alertname == alert_name)
    {
        Some(alert) => write_json(w, 200, &alert),
        None => write_error(
            w,
            404,
            &format!("alert {alert_name:?} not found in group {group_name:?}"),
        ),
    }
}

// -- operational endpoints -----------------------------------------------

fn handle_metrics(ctx: &Context, req: &Request<'_>, w: &mut ResponseWriter<'_>) {
    if !check_auth_key(req, ctx.metrics_auth_key.as_deref(), w) {
        return;
    }
    let mut body = String::new();
    esm_common::metrics::write_prometheus(&mut body);
    w.set_content_type("text/plain; charset=utf-8");
    w.write_body(body.as_bytes());
}

fn handle_healthy(w: &mut ResponseWriter<'_>) {
    w.set_content_type("text/plain; charset=utf-8");
    w.write_body(b"OK");
}

/// Signals `reload_tx` and returns 200 on acceptance. The actual config
/// re-read + `Manager::reload` call is owned by `main.rs` (Task 19) — see
/// the module doc on [`super`].
fn handle_reload(ctx: &Context, req: &Request<'_>, w: &mut ResponseWriter<'_>) {
    if !check_auth_key(req, ctx.reload_auth_key.as_deref(), w) {
        return;
    }
    match ctx.reload_tx.send(()) {
        Ok(()) => {
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"OK");
        }
        Err(_) => write_error(w, 500, "reload channel closed; no reload owner listening"),
    }
}

// -- shared helpers -------------------------------------------------------

/// Locks `mgr`, recovering from poison rather than propagating a panic: a
/// panic while some other request held the lock must not cascade into every
/// later request also panicking (see the crate-level "never panic in a
/// handler" contract).
fn lock_mgr(mgr: &Mutex<Manager>) -> MutexGuard<'_, Manager> {
    mgr.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Reads a single decoded query-string parameter by key (first match).
fn query_param(req: &Request<'_>, key: &str) -> Option<String> {
    req.query_params()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

/// Gates an endpoint on an auth key (mirrors esmauth's `check_auth_key` /
/// upstream's `httpserver.CheckAuthFlag`). `configured: None` (no key set)
/// leaves the endpoint open. Otherwise the request must carry the matching
/// key via the `?authKey=` query arg — the exact param name esmauth
/// (`crates/esmauth/src/lib.rs:484`) and upstream vmalert
/// (`httpserver.CheckAuthFlag`) use, so a reload/metrics-gating script
/// written for one works against the other. A missing or mismatched key gets
/// a 401 and `false`, so the caller skips the gated work. The response never
/// echoes the configured or provided key.
fn check_auth_key(req: &Request<'_>, configured: Option<&str>, w: &mut ResponseWriter<'_>) -> bool {
    let Some(expected) = configured else {
        return true;
    };
    if query_param(req, "authKey").as_deref() == Some(expected) {
        return true;
    }
    write_error(w, 401, "missing or invalid authKey");
    false
}

#[derive(Serialize)]
struct ErrorResponse<'a> {
    error: &'a str,
    #[serde(rename = "errorType")]
    error_type: u16,
}

/// Writes a JSON error envelope: `{"error":"<msg>","errorType":<status>}`,
/// matching upstream's `errJson` shape (`web.go:705`).
fn write_error(w: &mut ResponseWriter<'_>, status: u16, msg: &str) {
    let body = serde_json::to_string(&ErrorResponse {
        error: msg,
        error_type: status,
    })
    // `ErrorResponse` can only fail to serialize on an allocation failure;
    // fall back to a hand-written body rather than unwrap/panic.
    .unwrap_or_else(|_| format!("{{\"error\":\"internal error\",\"errorType\":{status}}}"));
    w.write_json(status, &body);
}

/// Writes a 200 JSON response, or a 500 JSON error if `value` somehow fails
/// to serialize (never a panic — see the module's "never panic" contract).
fn write_json<T: Serialize>(w: &mut ResponseWriter<'_>, status: u16, value: &T) {
    match serde_json::to_string(value) {
        Ok(body) => w.write_json(status, &body),
        Err(_) => write_error(w, 500, "failed to encode JSON response"),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::time::Duration;

    use crossbeam_channel::unbounded;

    use crate::config::parse_config_str;
    use crate::datasource::{DsError, QueryResult};
    use crate::manager::{Manager, ManagerDeps};
    use crate::rule::Querier;
    use crate::web::{serve, ServerHandle, WebConfig};

    /// A `Querier` that always returns one present sample, so a `for: 0`
    /// alerting rule fires on its first evaluation (mirrors
    /// `manager::tests::PresentQuerier`, duplicated here to keep this test
    /// module self-contained).
    struct PresentQuerier;
    impl Querier for PresentQuerier {
        fn query(&self, _expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: vec![crate::datasource::Metric {
                    labels: vec![("instance".into(), "h1".into())],
                    timestamps: vec![0],
                    values: vec![1.0],
                }],
                is_partial: None,
            })
        }
    }

    struct EmptyQuerier;
    impl Querier for EmptyQuerier {
        fn query(&self, _expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            Ok(QueryResult {
                data: vec![],
                is_partial: None,
            })
        }
    }

    /// Starts a `Manager` running one alerting rule (`alert: A`) against a
    /// `Querier` that always has data present, so the rule fires almost
    /// immediately (long interval + zero `max_start_delay` + immediate
    /// first eval — same convention as
    /// `manager::tests::alerts_snapshot_reflects_live_firing_alert_from_running_group`).
    fn manager_with_one_firing_alert() -> Manager {
        let deps = ManagerDeps::for_test(std::sync::Arc::new(PresentQuerier));
        let yaml =
            "groups:\n  - name: g1\n    interval: 3600s\n    rules:\n      - alert: A\n        expr: up\n";
        Manager::start(parse_config_str(yaml).unwrap(), deps).expect("manager start")
    }

    fn manager_with_no_alerts() -> Manager {
        let deps = ManagerDeps::for_test(std::sync::Arc::new(EmptyQuerier));
        let yaml = "groups:\n  - name: g1\n    interval: 3600s\n    rules:\n      - alert: A\n        expr: up\n";
        Manager::start(parse_config_str(yaml).unwrap(), deps).expect("manager start")
    }

    fn start_server(
        mgr: Manager,
        cfg: WebConfig,
    ) -> (ServerHandle, crossbeam_channel::Receiver<()>) {
        let (reload_tx, reload_rx) = unbounded();
        let handle = serve(
            std::sync::Arc::new(std::sync::Mutex::new(mgr)),
            cfg,
            reload_tx,
        )
        .expect("serve failed to bind");
        (handle, reload_rx)
    }

    fn default_cfg() -> WebConfig {
        WebConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            reload_auth_key: None,
            metrics_auth_key: None,
            read_timeout: Duration::from_secs(5),
        }
    }

    const CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(10);

    /// Minimal raw-HTTP/1.1 client, mirroring
    /// `esmauth/tests/reload_e2e_test.rs`'s `send_request` helper so this
    /// in-process test doesn't need a full HTTP client dependency.
    fn send_request(addr: SocketAddr, method: &str, target: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).expect("connect failed");
        stream
            .set_read_timeout(Some(CLIENT_READ_TIMEOUT))
            .expect("set_read_timeout failed");
        let req =
            format!("{method} {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).expect("write failed");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("read failed (or timed out)");
        let (head, body) = response
            .split_once("\r\n\r\n")
            .unwrap_or_else(|| panic!("malformed response: {response:?}"));
        let status_line = head.lines().next().unwrap_or_default();
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        (status, body.to_string())
    }

    fn http_get(addr: SocketAddr, target: &str) -> (u16, String) {
        send_request(addr, "GET", target)
    }

    fn http_post(addr: SocketAddr, target: &str) -> (u16, String) {
        send_request(addr, "POST", target)
    }

    #[test]
    fn rules_endpoint_returns_groups() {
        let mgr = manager_with_no_alerts();
        let (handle, _reload_rx) = start_server(mgr, default_cfg());
        let addr = handle.local_addr();

        let (status, body) = http_get(addr, "/api/v1/rules");
        assert_eq!(status, 200, "body: {body}");
        let json: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(json["status"], "success");
        let rule_name = json["data"]["groups"][0]["rules"][0]["name"]
            .as_str()
            .expect("rules[0].name must be a string");
        assert_eq!(rule_name, "A");

        // Also reachable under the /vmalert-prefixed alias.
        let (status, body) = http_get(addr, "/vmalert/api/v1/rules");
        assert_eq!(status, 200, "body: {body}");

        handle.stop();
    }

    #[test]
    fn alerts_endpoint_returns_firing_alert() {
        let mgr = manager_with_one_firing_alert();
        let (handle, _reload_rx) = start_server(mgr, default_cfg());
        let addr = handle.local_addr();

        // Poll: the group's background thread needs its first eval to
        // complete before the alert shows up in the live snapshot.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let (found, last_body) = loop {
            let (status, body) = http_get(addr, "/api/v1/alerts");
            assert_eq!(status, 200, "body: {body}");
            let json: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
            if json["data"]["alerts"]
                .as_array()
                .is_some_and(|a| !a.is_empty())
            {
                break (true, body);
            }
            if std::time::Instant::now() >= deadline {
                break (false, body);
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(found, "alert never appeared: {last_body}");
        let json: serde_json::Value = serde_json::from_str(&last_body).unwrap();
        assert_eq!(json["data"]["alerts"][0]["alertname"], "A");
        assert_eq!(json["data"]["alerts"][0]["group"], "g1");
        assert_eq!(json["data"]["alerts"][0]["state"], "firing");

        handle.stop();
    }

    #[test]
    fn healthy_endpoint_returns_200() {
        let mgr = manager_with_no_alerts();
        let (handle, _reload_rx) = start_server(mgr, default_cfg());
        let addr = handle.local_addr();

        let (status, body) = http_get(addr, "/-/healthy");
        assert_eq!(status, 200);
        assert_eq!(body, "OK");

        handle.stop();
    }

    #[test]
    fn metrics_endpoint_open_when_no_key_configured() {
        let mgr = manager_with_no_alerts();
        let (handle, _reload_rx) = start_server(mgr, default_cfg());
        let addr = handle.local_addr();

        let (status, _body) = http_get(addr, "/metrics");
        assert_eq!(status, 200);

        handle.stop();
    }

    #[test]
    fn metrics_endpoint_requires_auth_key_when_set() {
        let mgr = manager_with_no_alerts();
        let mut cfg = default_cfg();
        cfg.metrics_auth_key = Some("secret".to_string());
        let (handle, _reload_rx) = start_server(mgr, cfg);
        let addr = handle.local_addr();

        let (status, body) = http_get(addr, "/metrics");
        assert_eq!(status, 401, "body: {body}");

        let (status, _body) = http_get(addr, "/metrics?authKey=secret");
        assert_eq!(status, 200);

        handle.stop();
    }

    #[test]
    fn reload_requires_auth_key_when_set() {
        let mgr = manager_with_no_alerts();
        let mut cfg = default_cfg();
        cfg.reload_auth_key = Some("secret".to_string());
        let (handle, reload_rx) = start_server(mgr, cfg);
        let addr = handle.local_addr();

        let (status, body) = http_post(addr, "/-/reload");
        assert_eq!(status, 401, "body: {body}");
        assert!(
            reload_rx.try_recv().is_err(),
            "reload must not fire without the key"
        );

        let (status, body) = http_post(addr, "/-/reload?authKey=secret");
        assert_eq!(status, 200, "body: {body}");
        reload_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reload_tx must fire when the correct key is provided");

        handle.stop();
    }

    #[test]
    fn reload_open_when_no_key_configured() {
        let mgr = manager_with_no_alerts();
        let (handle, reload_rx) = start_server(mgr, default_cfg());
        let addr = handle.local_addr();

        let (status, body) = http_post(addr, "/-/reload");
        assert_eq!(status, 200, "body: {body}");
        reload_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reload_tx must fire");

        handle.stop();
    }

    #[test]
    fn unsupported_path_returns_404_json_error() {
        let mgr = manager_with_no_alerts();
        let (handle, _reload_rx) = start_server(mgr, default_cfg());
        let addr = handle.local_addr();

        let (status, body) = http_get(addr, "/nope");
        assert_eq!(status, 404);
        let json: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(json["errorType"], 404);

        handle.stop();
    }
}
