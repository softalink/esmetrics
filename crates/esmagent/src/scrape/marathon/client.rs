//! Marathon HTTP API client: per-server scheme normalization, auth
//! (bearer-token, else HTTP basic), TLS, and the `/v2/apps` listing the
//! refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/marathon/api.go`'s `newAPIConfig` and
//! `apps.go`'s `GetAppsList` (v1.146.0). Two deliberate deviations, both noted
//! below and in the module doc / PORTING notes:
//!
//! - **Auth.** Upstream v1.146.0's `marathon.SDConfig` has no `auth_token`
//!   field: authentication is entirely via `promauth.HTTPClientConfig`
//!   (bearer-token / basic-auth / TLS). There is NO Marathon-specific
//!   `Authorization: token=<t>` header in this version (that was Prometheus's
//!   older shape). This port matches upstream: bearer, else basic — mirroring
//!   `scrape::digitalocean`/`scrape::nomad`.
//! - **Server selection.** Upstream picks ONE random server per refresh
//!   (`ac.cs[rand.Intn(...)]`) with no failover. This port tries each server
//!   in `servers` order until one responds, which is strictly more robust and
//!   deterministic (better for tests); a single-server config behaves
//!   identically.

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use crate::client::TlsConfig;
use crate::scrape::config::MarathonSdConfig;
pub use crate::scrape::config::ScrapeError;

use super::labels::{parse_app_list, AppList};

/// Per-request client-side timeout, capped so a hung Marathon server can't
/// stall the refresh thread — and thus a [`super::MarathonDiscovery`]
/// `Drop`/`stop` — indefinitely. Mirrors the DigitalOcean/Nomad rationale.
const MARATHON_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The apps listing path, matching `apps.go`'s
/// `"/v2/apps/?embed=apps.tasks"`.
const APPS_API_PATH: &str = "/v2/apps/?embed=apps.tasks";

/// Resolved Marathon API access: the normalized server base URLs (tried in
/// order), an HTTP client with TLS applied, and the resolved auth to attach
/// per request.
///
/// `Debug` is hand-written to redact the bearer token / basic password —
/// defense-in-depth against a future `{:?}` in a log line.
pub struct MarathonApi {
    base_urls: Vec<String>,
    http: HttpClient,
    bearer: Option<String>,
    basic: Option<(String, String)>,
}

impl std::fmt::Debug for MarathonApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarathonApi")
            .field("base_urls", &self.base_urls)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .finish()
    }
}

/// Builds a [`MarathonApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `cfg.auth.bearer` becomes the bearer token; failing that, `cfg.auth.basic`
///   becomes HTTP basic credentials.
/// - each `server` gets a scheme prepended when absent (`https` if TLS is
///   configured, else `http`) and a trailing `/` stripped.
///
/// Fails only on genuinely bad config (bad TLS material) — never because a
/// Marathon server is unreachable; listing happens later on the refresh thread.
pub fn new_marathon_api(cfg: &MarathonSdConfig) -> Result<MarathonApi, ScrapeError> {
    let http = build_client(&cfg.tls)?;
    let tls_configured = cfg.tls != TlsConfig::default();
    let base_urls = cfg
        .servers
        .iter()
        .map(|s| normalize_server(s, tls_configured))
        .collect();
    Ok(MarathonApi {
        base_urls,
        http,
        bearer: cfg.auth.bearer.clone(),
        basic: cfg.auth.basic.clone(),
    })
}

/// `<scheme>://<server>`, prepending a scheme when `server` has none (`https`
/// if TLS is configured, else `http`) and stripping a trailing `/`.
fn normalize_server(server: &str, tls_configured: bool) -> String {
    let with_scheme = if server.contains("://") {
        server.to_string()
    } else {
        let scheme = if tls_configured { "https" } else { "http" };
        format!("{scheme}://{server}")
    };
    with_scheme.trim_end_matches('/').to_string()
}

/// Builds a `reqwest::blocking::Client` applying `tls` (CA / client identity /
/// insecure-skip-verify), mirroring the DigitalOcean/Nomad client builders.
fn build_client(tls: &TlsConfig) -> Result<HttpClient, ScrapeError> {
    let mut builder = HttpClient::builder();
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file).map_err(|e| ScrapeError {
            msg: format!("cannot read CA file {ca_file:?}: {e}"),
        })?;
        let cert = reqwest::Certificate::from_pem(&pem).map_err(|e| ScrapeError {
            msg: format!("invalid CA certificate in {ca_file:?}: {e}"),
        })?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file).map_err(|e| ScrapeError {
            msg: format!("cannot read cert file {cert_file:?}: {e}"),
        })?;
        let mut key_pem = std::fs::read(key_file).map_err(|e| ScrapeError {
            msg: format!("cannot read key file {key_file:?}: {e}"),
        })?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem).map_err(|e| ScrapeError {
            msg: format!("invalid client cert/key: {e}"),
        })?;
        builder = builder.identity(identity);
    }
    builder.build().map_err(|e| ScrapeError {
        msg: format!("cannot build marathon http client: {e}"),
    })
}

impl MarathonApi {
    /// Issues a GET against `<base_url><path>` with the resolved auth and a
    /// per-call timeout, returning the response body bytes on a 2xx status.
    fn get(&self, base_url: &str, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{base_url}{path}");
        let mut req = self
            .http
            .get(&url)
            .timeout(MARATHON_HTTP_TIMEOUT)
            .header("Accept", "application/json");
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("marathon request to {url:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("marathon response from {url:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("marathon request to {url:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Lists all Marathon apps, trying each configured server in order until
    /// one responds. Port of `GetAppsList` (with the server-failover deviation
    /// noted in the module doc). Returns the last server's error when every
    /// server fails (or a "no servers" error when `servers` is empty).
    pub fn get_apps(&self) -> Result<AppList, ScrapeError> {
        let mut last_err: Option<ScrapeError> = None;
        for base_url in &self.base_urls {
            match self.get(base_url, APPS_API_PATH) {
                Ok(data) => {
                    return parse_app_list(&data).map_err(|msg| ScrapeError {
                        msg: format!("{base_url}{APPS_API_PATH}: {msg}"),
                    });
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| ScrapeError {
            msg: "marathon: no servers configured".to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    fn cfg(servers: &[&str]) -> MarathonSdConfig {
        MarathonSdConfig {
            servers: servers.iter().map(|s| s.to_string()).collect(),
            ..MarathonSdConfig::default()
        }
    }

    #[test]
    fn prepends_http_scheme_when_absent() {
        let api = new_marathon_api(&cfg(&["marathon:8080"])).unwrap();
        assert_eq!(api.base_urls, vec!["http://marathon:8080".to_string()]);
    }

    #[test]
    fn preserves_scheme_and_strips_trailing_slash() {
        let api = new_marathon_api(&cfg(&["https://marathon.example/"])).unwrap();
        assert_eq!(api.base_urls, vec!["https://marathon.example".to_string()]);
    }

    #[test]
    fn keeps_every_server_in_order() {
        let api = new_marathon_api(&cfg(&["a:1", "https://b:2", "c:3"])).unwrap();
        assert_eq!(
            api.base_urls,
            vec![
                "http://a:1".to_string(),
                "https://b:2".to_string(),
                "http://c:3".to_string(),
            ]
        );
    }

    #[test]
    fn bearer_token_is_redacted_in_debug() {
        let mut c = cfg(&["m:8080"]);
        c.auth = AuthConfig {
            bearer: Some("super-secret-token".into()),
            ..AuthConfig::default()
        };
        let api = new_marathon_api(&c).unwrap();
        assert_eq!(api.bearer.as_deref(), Some("super-secret-token"));
        assert!(!format!("{api:?}").contains("super-secret-token"));
    }

    #[test]
    fn basic_password_is_redacted_in_debug() {
        let mut c = cfg(&["m:8080"]);
        c.auth = AuthConfig {
            basic: Some(("user".into(), "hunter2".into())),
            ..AuthConfig::default()
        };
        let api = new_marathon_api(&c).unwrap();
        assert_eq!(api.basic, Some(("user".to_string(), "hunter2".to_string())));
        assert!(!format!("{api:?}").contains("hunter2"));
    }
}
