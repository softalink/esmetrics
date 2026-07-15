//! Kuma MADS HTTP client: the `server` URL derivation, bearer/basic auth, TLS,
//! and the single `POST <server>/v3/discovery:monitoringassignments` fetch the
//! refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/kuma/api.go`'s `getAPIServerPath`
//! (derives the API-server origin + request path from `server`, preserving any
//! base path and query — see [`derive_api_server_path`]) and
//! `updateTargetsLabels` (POST a `DiscoveryRequest` JSON body, parse the
//! `DiscoveryResponse`). Simpler than the k8s SD: one POST endpoint, no
//! watch/list split.
//!
//! ## Poll-based simplification
//!
//! Upstream keeps `version_info`/`nonce` across refreshes and honors a
//! `304 Not Modified` to skip re-parsing. This poll-based port sends an empty
//! `version_info`/`nonce` on every refresh and takes the full response each
//! time — the request-body *shape* stays faithful (`node.id`, `type_url`,
//! `version_info`, `response_nonce`), only the incremental-ACK optimization is
//! dropped. Matches the "keep simple" latitude in the task brief.

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};
use url::Url;

use crate::client::TlsConfig;
use crate::scrape::config::{KumaSdConfig, ScrapeError};

use super::labels::{parse_targets_labels, KumaTarget};

/// Per-request client-side timeout. The single POST is capped so a hung Kuma
/// control plane can't stall the refresh thread — and thus a
/// [`super::KumaDiscovery`] `Drop`/`stop` — indefinitely. Mirrors the
/// PuppetDB/DigitalOcean client's rationale.
const KUMA_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The MADS request suffix appended to the derived base path. Port of
/// `getAPIServerPath`'s `"v3/discovery:monitoringassignments"`.
const MADS_PATH: &str = "v3/discovery:monitoringassignments";

/// The xDS `type_url` for a Kuma `MonitoringAssignment`. Port of
/// `updateTargetsLabels`'s `TypeURL`.
const MONITORING_ASSIGNMENT_TYPE_URL: &str =
    "type.googleapis.com/kuma.observability.v1.MonitoringAssignment";

/// Fallback `node.id` when `client_id` is unset and no hostname is available.
/// Port of `newAPIConfig`'s final `"vmagent"` fallback.
const DEFAULT_CLIENT_ID: &str = "vmagent";

/// Resolved Kuma API access: the origin to POST to (`scheme://host[:port]`),
/// the request path (base path + [`MADS_PATH`] + any query), an HTTP client
/// with TLS applied, the resolved `node.id`, and the resolved auth.
///
/// `Debug` is hand-written to redact the bearer token and the basic password —
/// defense-in-depth against a future `{:?}` in a log line (mirrors
/// `PuppetdbApi`, which shows the basic username but never the password).
pub struct KumaApi {
    api_server: String,
    api_path: String,
    http: HttpClient,
    client_id: String,
    bearer: Option<String>,
    basic: Option<(String, String)>,
}

impl std::fmt::Debug for KumaApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KumaApi")
            .field("api_server", &self.api_server)
            .field("api_path", &self.api_path)
            .field("client_id", &self.client_id)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .finish()
    }
}

/// Derives `(api_server, api_path)` from `server`, mirroring
/// `getAPIServerPath` EXACTLY:
/// - a `server` with no `://` gets an `http://` scheme prepended;
/// - `api_server` is `scheme://host[:port]` (the URL's origin);
/// - `api_path` is the URL's base path with a trailing `/` ensured, then
///   `v3/discovery:monitoringassignments`, then `?<query>` if `server`
///   carried a query.
///
/// An empty or unparseable `server` errors (upstream `getAPIServerPath`'s
/// two failure cases, exercised by `api_test.go`'s `TestGetAPIConfigFailure`).
pub fn derive_api_server_path(server: &str) -> Result<(String, String), ScrapeError> {
    if server.is_empty() {
        return Err(ScrapeError::new("kuma_sd: `server` is missing"));
    }
    let with_scheme = if server.contains("://") {
        server.to_string()
    } else {
        format!("http://{server}")
    };
    let parsed = Url::parse(&with_scheme)
        .map_err(|e| ScrapeError::new(format!("cannot parse server {server:?}: {e}")))?;

    let Some(host) = parsed.host_str() else {
        return Err(ScrapeError::new(format!(
            "host is missing in server {server:?}"
        )));
    };
    let api_server = match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    };

    // `url` normalizes an empty path to "/"; both cases fold to the same
    // result here (a trailing "/" is ensured before appending MADS_PATH),
    // matching `getAPIServerPath` whether `psu.Path` is "" or "/".
    let mut api_path = parsed.path().to_string();
    if !api_path.ends_with('/') {
        api_path.push('/');
    }
    api_path.push_str(MADS_PATH);
    if let Some(query) = parsed.query() {
        api_path.push('?');
        api_path.push_str(query);
    }
    Ok((api_server, api_path))
}

/// Resolves `client_id`: the configured value, else the OS hostname
/// (`$HOSTNAME`/`$COMPUTERNAME`), else [`DEFAULT_CLIENT_ID`]. Port of
/// `newAPIConfig`'s `clientID` derivation (the hostname source differs — see
/// the module doc / report; `node.id` is informational for a poll-based port).
fn resolve_client_id(client_id: &str) -> String {
    if !client_id.is_empty() {
        return client_id.to_string();
    }
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(h) = std::env::var(var) {
            if !h.is_empty() {
                return h;
            }
        }
    }
    DEFAULT_CLIENT_ID.to_string()
}

/// Builds a [`KumaApi`] from `cfg`, mirroring `newAPIConfig`: derives the
/// API-server origin + request path from `server` (the sole `server`
/// validator — errors on an empty/unparseable URL), applies TLS, resolves
/// `client_id`, and takes the bearer token (else basic credentials) from
/// `cfg.auth`. Fails only on genuinely bad config (bad `server`, bad TLS
/// material) — never because the Kuma control plane is unreachable; the fetch
/// happens later on the refresh thread.
pub fn new_kuma_api(cfg: &KumaSdConfig) -> Result<KumaApi, ScrapeError> {
    let (api_server, api_path) = derive_api_server_path(&cfg.server)?;
    let http = build_client(&cfg.tls)?;
    Ok(KumaApi {
        api_server,
        api_path,
        http,
        client_id: resolve_client_id(&cfg.client_id),
        bearer: cfg.auth.bearer.clone(),
        basic: cfg.auth.basic.clone(),
    })
}

/// Builds a `reqwest::blocking::Client` applying `tls` (CA / client identity /
/// insecure-skip-verify), mirroring the PuppetDB/DigitalOcean client builder.
fn build_client(tls: &TlsConfig) -> Result<HttpClient, ScrapeError> {
    let mut builder = HttpClient::builder();
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file)
            .map_err(|e| ScrapeError::new(format!("cannot read CA file {ca_file:?}: {e}")))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| ScrapeError::new(format!("invalid CA certificate in {ca_file:?}: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file)
            .map_err(|e| ScrapeError::new(format!("cannot read cert file {cert_file:?}: {e}")))?;
        let mut key_pem = std::fs::read(key_file)
            .map_err(|e| ScrapeError::new(format!("cannot read key file {key_file:?}: {e}")))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| ScrapeError::new(format!("invalid client cert/key: {e}")))?;
        builder = builder.identity(identity);
    }
    builder
        .build()
        .map_err(|e| ScrapeError::new(format!("cannot build kuma http client: {e}")))
}

impl KumaApi {
    /// Issues the `POST <api_server><api_path>` with a `DiscoveryRequest` JSON
    /// body (empty `version_info`/`response_nonce` — see the module doc), the
    /// resolved auth, and a per-call timeout, returning the parsed targets on
    /// a 2xx status. Port of `updateTargetsLabels`.
    pub fn fetch_targets(&self) -> Result<Vec<KumaTarget>, ScrapeError> {
        let url = format!("{}{}", self.api_server, self.api_path);
        let body = serde_json::json!({
            "version_info": "",
            "node": { "id": self.client_id },
            "resource_names": [],
            "type_url": MONITORING_ASSIGNMENT_TYPE_URL,
            "response_nonce": "",
        })
        .to_string();
        let mut req = self
            .http
            .post(&url)
            .timeout(KUMA_HTTP_TIMEOUT)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .body(body);
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req
            .send()
            .map_err(|e| ScrapeError::new(format!("kuma request to {MADS_PATH:?} failed: {e}")))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .map_err(|e| ScrapeError::new(format!("kuma response from {MADS_PATH:?}: {e}")))?;
        if !status.is_success() {
            return Err(ScrapeError::new(format!(
                "kuma request to {MADS_PATH:?} failed: status {status}"
            )));
        }
        let (targets, _version, _nonce) = parse_targets_labels(&bytes)
            .map_err(|msg| ScrapeError::new(format!("{MADS_PATH:?}: {msg}")))?;
        Ok(targets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    /// Port of upstream `api_test.go`'s `TestGetAPIServerPathSuccess`: every
    /// `(server, expected_api_server, expected_api_path)` case, EXACTLY.
    #[test]
    fn derive_api_server_path_matches_upstream() {
        let cases = [
            // url without path
            (
                "http://localhost:5676",
                "http://localhost:5676",
                "/v3/discovery:monitoringassignments",
            ),
            // url with a bare path / trailing slash
            (
                "http://localhost:5676/",
                "http://localhost:5676",
                "/v3/discovery:monitoringassignments",
            ),
            (
                "https://foo.bar:1234/a/b",
                "https://foo.bar:1234",
                "/a/b/v3/discovery:monitoringassignments",
            ),
            // url with query args (preserved)
            (
                "https://foo.bar:1234/a/b?c=d&arg2=value2",
                "https://foo.bar:1234",
                "/a/b/v3/discovery:monitoringassignments?c=d&arg2=value2",
            ),
            // missing scheme -> http:// prepended
            (
                "foo.bar",
                "http://foo.bar",
                "/v3/discovery:monitoringassignments",
            ),
            (
                "foo.bar:1234/a/b",
                "http://foo.bar:1234",
                "/a/b/v3/discovery:monitoringassignments",
            ),
            (
                "foo.bar:1234/a/b?c=d&arg2=value2",
                "http://foo.bar:1234",
                "/a/b/v3/discovery:monitoringassignments?c=d&arg2=value2",
            ),
        ];
        for (server, want_api_server, want_api_path) in cases {
            let (api_server, api_path) = derive_api_server_path(server)
                .unwrap_or_else(|e| panic!("server {server:?} should derive: {e}"));
            assert_eq!(api_server, want_api_server, "api_server for {server:?}");
            assert_eq!(api_path, want_api_path, "api_path for {server:?}");
        }
    }

    /// Port of `api_test.go`'s `TestGetAPIConfigFailure`: an empty or
    /// unparseable `server` is rejected.
    #[test]
    fn derive_api_server_path_rejects_bad_server() {
        assert!(derive_api_server_path("").is_err());
        assert!(derive_api_server_path(":").is_err());
    }

    fn cfg(server: &str) -> KumaSdConfig {
        KumaSdConfig {
            server: server.to_string(),
            ..KumaSdConfig::default()
        }
    }

    #[test]
    fn new_kuma_api_validates_server() {
        assert!(new_kuma_api(&cfg("http://localhost:5676")).is_ok());
        assert!(new_kuma_api(&cfg("")).is_err());
        assert!(new_kuma_api(&cfg(":")).is_err());
    }

    #[test]
    fn bearer_token_is_redacted_in_debug() {
        let mut c = cfg("http://localhost:5676");
        c.auth = AuthConfig {
            bearer: Some("super-secret-token".into()),
            ..AuthConfig::default()
        };
        let api = new_kuma_api(&c).unwrap();
        assert_eq!(api.bearer.as_deref(), Some("super-secret-token"));
        assert!(!format!("{api:?}").contains("super-secret-token"));
    }

    #[test]
    fn basic_auth_password_is_redacted_in_debug() {
        let mut c = cfg("http://localhost:5676");
        c.auth = AuthConfig {
            basic: Some(("kuma-user".into(), "kuma-pass".into())),
            ..AuthConfig::default()
        };
        let api = new_kuma_api(&c).unwrap();
        let dbg = format!("{api:?}");
        assert!(dbg.contains("kuma-user"));
        assert!(!dbg.contains("kuma-pass"), "{dbg}");
    }

    #[test]
    fn resolve_client_id_prefers_configured_value() {
        assert_eq!(resolve_client_id("my-id"), "my-id");
        // Empty -> hostname or the vmagent fallback (never empty).
        assert!(!resolve_client_id("").is_empty());
    }
}
