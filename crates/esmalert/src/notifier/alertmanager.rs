//! Alertmanager v2 notifier client. Port of `notifier.AlertManager`
//! (`notifier/alertmanager.go`) and the JSON body shape from
//! `notifier/alertmanager_request.qtpl.go` (`streamamRequest`), narrowed to
//! what esmalert's rule evaluator needs to push alerts. Uses
//! `reqwest::blocking` (rustls-tls backend, matching
//! `datasource::client::Datasource`) — no tokio.

use std::time::Duration;

use reqwest::blocking::Client;
use serde_json::{Map, Value};
use url::Url;

use crate::config::Header;
use crate::datasource::{AuthConfig, TlsConfig};
use crate::rule::Alert;

use super::NotifyError;

/// Path Alertmanager's v2 API exposes for posting alerts. Port of
/// `alertManagerPath` (`alertmanager.go:168`).
const ALERTS_PATH: &str = "/api/v2/alerts";

/// Cap on how much of a non-2xx response body is captured in a
/// [`NotifyError`], so a chatty or malicious Alertmanager can't bloat error
/// messages (and, incidentally, can't smuggle anything but a short prefix
/// through).
const ERROR_BODY_SNIPPET_LEN: usize = 200;

/// A blocking Alertmanager v2 client. Port of `notifier.AlertManager`
/// (`alertmanager.go:22-35`), narrowed to the fields needed to build and
/// send requests (upstream's metrics/`lastError`/relabel-config fields are
/// out of scope for this task).
pub struct AlertManager {
    client: Client,
    url: Url,
    headers: Vec<Header>,
    auth: AuthConfig,
}

impl AlertManager {
    /// Builds a client posting to `<base_url>` + `path.Join("/",
    /// path_prefix, "/api/v2/alerts")` (port of how `alertsPath` is built in
    /// `notifier/config.go:218`, applied to `base_url` instead of a
    /// discovered target).
    pub fn new(
        base_url: &str,
        path_prefix: &str,
        headers: Vec<Header>,
        auth: AuthConfig,
        tls: TlsConfig,
        timeout: Duration,
    ) -> Result<Self, NotifyError> {
        let url = build_url(base_url, path_prefix)?;
        let client = build_client(&tls, timeout)?;
        Ok(AlertManager {
            client,
            url,
            headers,
            auth,
        })
    }

    /// Posts `alerts` as a JSON array to `/api/v2/alerts`. Port of
    /// `AlertManager.send` (`alertmanager.go:104-163`): builds the request
    /// body ([`alert_json`]), attaches headers/auth, and treats any non-2xx
    /// response as an error carrying the status and a short body snippet
    /// (never the request's auth credentials — those never appear in the
    /// response body or in this error).
    pub fn send(&self, alerts: &[Alert], external_url: &str) -> Result<(), NotifyError> {
        let body: Vec<Value> = alerts.iter().map(|a| alert_json(a, external_url)).collect();
        // `reqwest`'s `blocking` feature set here doesn't include `json`
        // (see `datasource::client`'s doc comment on the shared rustls-tls
        // backend), so the body is serialized and attached manually rather
        // than via `RequestBuilder::json`.
        let payload = serde_json::to_vec(&Value::Array(body))
            .map_err(|e| NotifyError::new(format!("cannot serialize alert payload: {e}")))?;
        let mut req = self
            .client
            .post(self.url.clone())
            .header("Content-Type", "application/json")
            .body(payload);
        for h in &self.headers {
            req = req.header(h.key.as_str(), h.value.as_str());
        }
        if let Some((user, pass)) = &self.auth.basic {
            req = req.basic_auth(user, Some(pass));
        } else if let Some(token) = &self.auth.bearer {
            req = req.bearer_auth(token);
        }
        let resp = req.send()?;
        let status = resp.status();
        if !status.is_success() {
            let body_bytes = resp.bytes().unwrap_or_default();
            let snippet: String = String::from_utf8_lossy(&body_bytes)
                .chars()
                .take(ERROR_BODY_SNIPPET_LEN)
                .collect();
            return Err(NotifyError::new(format!(
                "alertmanager returned status {status}: {snippet}"
            )));
        }
        Ok(())
    }
}

/// Builds the alerts endpoint: `base_url`'s existing path (if any) plus
/// [`join_alerts_path`]'s result, mirroring
/// `datasource::client::Datasource::build_url`'s existing-path handling.
fn build_url(base_url: &str, path_prefix: &str) -> Result<Url, NotifyError> {
    let mut url = Url::parse(base_url)
        .map_err(|e| NotifyError::new(format!("invalid alertmanager url {base_url:?}: {e}")))?;
    let existing_path = url.path().trim_end_matches('/').to_string();
    url.set_path(&format!("{existing_path}{}", join_alerts_path(path_prefix)));
    Ok(url)
}

/// Port of `path.Join("/", path_prefix, alertManagerPath)`
/// (`notifier/config.go:218`), narrowed to the leading/trailing-slash
/// normalization real `path_prefix` values need (no `.`/`..` segment
/// resolution, since upstream's `path_prefix` is a plain URL path prefix,
/// never a filesystem-style relative path).
fn join_alerts_path(path_prefix: &str) -> String {
    let trimmed = path_prefix.trim_matches('/');
    if trimmed.is_empty() {
        ALERTS_PATH.to_string()
    } else {
        format!("/{trimmed}{ALERTS_PATH}")
    }
}

/// Builds the `reqwest::blocking::Client`, applying `tls` the same way
/// `datasource::client::build_client` does (duplicated rather than shared —
/// see that function's module for the repo's established convention of
/// duplicating small already-verified helpers per module) plus the
/// notifier's request timeout, which the datasource client doesn't set.
fn build_client(tls: &TlsConfig, timeout: Duration) -> Result<Client, NotifyError> {
    let mut builder = Client::builder().timeout(timeout);
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file)
            .map_err(|e| NotifyError::new(format!("cannot read CA file {ca_file:?}: {e}")))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| NotifyError::new(format!("invalid CA certificate in {ca_file:?}: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file)
            .map_err(|e| NotifyError::new(format!("cannot read cert file {cert_file:?}: {e}")))?;
        let mut key_pem = std::fs::read(key_file)
            .map_err(|e| NotifyError::new(format!("cannot read key file {key_file:?}: {e}")))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| NotifyError::new(format!("invalid client cert/key: {e}")))?;
        builder = builder.identity(identity);
    }
    builder
        .build()
        .map_err(|e| NotifyError::new(format!("cannot build http client: {e}")))
}

/// Builds one alert's JSON object. Port of `streamamRequest`'s per-alert
/// loop body (`alertmanager_request.qtpl.go:11-29`):
/// - `startsAt` is always written, from the alert's `active_at` (upstream's
///   `Alert.Start`).
/// - `endsAt` is written only when the alert has a resolved time set
///   (upstream's `!alert.End.IsZero()` check); this port has no separate
///   `Alert.End` field — `resolved_at` is the closest analog, since
///   upstream's `alertsToSend` (`rule/alerting.go:879-885`) only ever
///   populates `End` from either `now + resolveDuration` (still-active
///   alerts) or `ResolvedAt` (resolved alerts), and `send`'s signature here
///   has no `resolveDuration` parameter to derive the former. Omitting
///   `endsAt` for still-active alerts is a valid Alertmanager v2 request
///   (Alertmanager treats a missing `endsAt` as "still firing").
/// - `generatorURL` is `external_url` as-is. Upstream derives a per-alert
///   source link via `AlertURLGenerator` (a `-external.url`-based
///   templated link to the alert's source); `Alert` doesn't carry a source
///   field in this port yet, so `external_url` is used unmodified.
fn alert_json(alert: &Alert, external_url: &str) -> Value {
    let mut obj = Map::new();
    obj.insert(
        "startsAt".to_string(),
        Value::String(rfc3339_millis(alert.active_at)),
    );
    obj.insert(
        "generatorURL".to_string(),
        Value::String(external_url.to_string()),
    );
    if let Some(resolved_at) = alert.resolved_at {
        obj.insert(
            "endsAt".to_string(),
            Value::String(rfc3339_millis(resolved_at)),
        );
    }
    obj.insert(
        "labels".to_string(),
        Value::Object(
            alert
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect(),
        ),
    );
    obj.insert(
        "annotations".to_string(),
        Value::Object(
            alert
                .annotations
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect(),
        ),
    );
    Value::Object(obj)
}

/// Formats a Unix-milliseconds timestamp as RFC3339 seconds-precision UTC.
/// Duplicated (not shared) from the equivalent algorithm in
/// `datasource::client::rfc3339_millis` / `rule::alert::format_active_at` —
/// this repo's established convention (see those functions' doc comments)
/// is to duplicate small already-verified helpers per module.
fn rfc3339_millis(ts_ms: i64) -> String {
    let unix_secs = ts_ms.div_euclid(1000).max(0) as u64;
    let (y, mo, d, h, mi, s) = civil_from_unix(unix_secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn civil_from_unix(unix_secs: u64) -> (i64, u64, u64, u64, u64, u64) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, d, h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::AlertState;
    use esm_http::{Request, ResponseWriter, Server};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Captured {
        path: String,
        body: String,
    }

    /// Starts a stub server on an ephemeral port that records the path and
    /// body of the last request it receives, then responds with `status`.
    fn start_capture_server(status: u16) -> (Server, Arc<Mutex<Option<Captured>>>) {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        let captured: Arc<Mutex<Option<Captured>>> = Arc::new(Mutex::new(None));
        let captured_for_handler = Arc::clone(&captured);
        server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                let path = req.path().to_string();
                let mut body_bytes = Vec::new();
                req.read_body_to(&mut body_bytes, 1 << 20).ok();
                let body = String::from_utf8_lossy(&body_bytes).to_string();
                *captured_for_handler.lock().unwrap() = Some(Captured { path, body });
                w.write_json(status, "{}");
            },
        ));
        (server, captured)
    }

    fn build_alert(
        labels: BTreeMap<String, String>,
        annotations: BTreeMap<String, String>,
    ) -> Alert {
        Alert {
            state: AlertState::Firing,
            active_at: 0,
            value: 1.0,
            labels,
            annotations,
            ..Default::default()
        }
    }

    #[test]
    fn posts_alerts_to_alertmanager_v2() {
        let (server, captured) = start_capture_server(200);
        let addr = server.local_addr();
        let am = AlertManager::new(
            &format!("http://{addr}"),
            "",
            vec![],
            AuthConfig::default(),
            TlsConfig::default(),
            Duration::from_secs(5),
        )
        .expect("build alertmanager");
        let alert = build_alert(
            [("alertname".to_string(), "A".to_string())]
                .into_iter()
                .collect(),
            [("summary".to_string(), "boom".to_string())]
                .into_iter()
                .collect(),
        );

        am.send(&[alert], "http://vm").expect("send failed");

        let req = captured
            .lock()
            .unwrap()
            .clone()
            .expect("no request captured");
        assert_eq!(req.path, "/api/v2/alerts");
        assert!(req.body.contains("\"alertname\":\"A\""));
        assert!(req.body.contains("\"summary\":\"boom\""));
        assert!(req.body.contains("\"generatorURL\":\"http://vm\""));

        server.stop();
    }

    #[test]
    fn ends_at_omitted_unless_alert_resolved() {
        let (server, captured) = start_capture_server(200);
        let addr = server.local_addr();
        let am = AlertManager::new(
            &format!("http://{addr}"),
            "",
            vec![],
            AuthConfig::default(),
            TlsConfig::default(),
            Duration::from_secs(5),
        )
        .expect("build alertmanager");

        let firing = build_alert(BTreeMap::new(), BTreeMap::new());
        am.send(&[firing], "http://vm").expect("send failed");
        let req = captured
            .lock()
            .unwrap()
            .clone()
            .expect("no request captured");
        assert!(
            !req.body.contains("endsAt"),
            "still-active alert should omit endsAt: {}",
            req.body
        );

        let resolved = Alert {
            state: AlertState::Inactive,
            active_at: 0,
            resolved_at: Some(1_700_000_000_000),
            value: 0.0,
            ..Default::default()
        };
        am.send(&[resolved], "http://vm").expect("send failed");
        let req2 = captured
            .lock()
            .unwrap()
            .clone()
            .expect("no request captured");
        assert!(
            req2.body.contains("\"endsAt\":\"2023-11-14T22:13:20Z\""),
            "resolved alert should carry endsAt: {}",
            req2.body
        );

        server.stop();
    }

    #[test]
    fn fan_out_collects_errors_without_aborting_other_targets() {
        let (ok_server, ok_captured) = start_capture_server(200);
        let ok_addr = ok_server.local_addr();
        let (fail_server, _fail_captured) = start_capture_server(500);
        let fail_addr = fail_server.local_addr();

        let ok_am = AlertManager::new(
            &format!("http://{ok_addr}"),
            "",
            vec![],
            AuthConfig::default(),
            TlsConfig::default(),
            Duration::from_secs(5),
        )
        .expect("build ok target");
        let fail_am = AlertManager::new(
            &format!("http://{fail_addr}"),
            "",
            vec![],
            AuthConfig::default(),
            TlsConfig::default(),
            Duration::from_secs(5),
        )
        .expect("build failing target");

        let notifiers = super::super::Notifiers(vec![ok_am, fail_am]);
        let alert = build_alert(
            [("alertname".to_string(), "A".to_string())]
                .into_iter()
                .collect(),
            BTreeMap::new(),
        );

        let errors = notifiers.send(&[alert], "http://vm");
        assert_eq!(errors.len(), 1, "expected exactly one target to fail");
        assert_eq!(errors[0].0, 1, "the failing target is index 1");
        assert!(
            ok_captured.lock().unwrap().is_some(),
            "the ok target should still have been attempted despite the other's failure"
        );

        ok_server.stop();
        fail_server.stop();
    }
}
