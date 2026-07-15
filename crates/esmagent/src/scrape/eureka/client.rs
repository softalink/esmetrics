//! Eureka HTTP API client: server/scheme normalization, bearer/basic auth,
//! TLS, and the single `GET <server>/apps` listing the refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/eureka/api.go`'s `newAPIConfig`
//! (server default `localhost:8080/eureka/v2`, scheme prepend) and
//! `getAPIResponse(cfg, "/apps")`. Simpler than EC2/Consul: a single
//! XML endpoint with optional bearer/basic auth, no region/agent bootstrap.
//!
//! ## `Accept: application/xml` (deliberate deviation)
//!
//! Upstream relies on `discoveryutil.Client`'s defaults and the Eureka server
//! returning XML; a real Eureka server returns JSON unless the request asks for
//! XML. Since this port parses the XML representation (matching upstream's
//! `encoding/xml` unmarshal), it sends `Accept: application/xml` explicitly so
//! the server is guaranteed to answer with the XML this client parses.

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use crate::client::TlsConfig;
use crate::scrape::config::EurekaSdConfig;
pub use crate::scrape::config::ScrapeError;

use super::labels::{parse_applications, Applications};

/// Per-request client-side timeout. The single `/apps` GET is capped so a hung
/// Eureka server can't stall the refresh thread — and thus a
/// [`super::EurekaDiscovery`] `Drop`/`stop` — indefinitely. Mirrors the
/// EC2/DigitalOcean client's rationale.
const EUREKA_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Default Eureka API server when `server` is unset. Port of `api.go`'s
/// `"localhost:8080/eureka/v2"`.
const DEFAULT_API_SERVER: &str = "localhost:8080/eureka/v2";

/// The applications listing path appended to the server base URL. Port of
/// `eureka.go`'s `getAPIResponse(cfg, "/apps")`.
const APPS_API_PATH: &str = "/apps";

/// Resolved Eureka API access: base URL, an HTTP client with TLS applied, and
/// the resolved auth (bearer token, else HTTP basic credentials) to attach per
/// request.
///
/// `Debug` is hand-written to redact the bearer token and the basic password —
/// defense-in-depth against a future `{:?}` in a log line.
pub struct EurekaApi {
    base_url: String,
    http: HttpClient,
    bearer: Option<String>,
    basic: Option<(String, String)>,
}

impl std::fmt::Debug for EurekaApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EurekaApi")
            .field("base_url", &self.base_url)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .finish()
    }
}

/// Builds an [`EurekaApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `cfg.auth.bearer` becomes the bearer token; failing that, `cfg.auth.basic`
///   becomes HTTP basic credentials.
/// - server defaults to `localhost:8080/eureka/v2`; a scheme is prepended when
///   absent (`https` when TLS is configured, else `http`).
///
/// Fails only on genuinely bad config (bad TLS material) — never because the
/// Eureka server is unreachable; listing happens later on the refresh thread.
pub fn new_eureka_api(cfg: &EurekaSdConfig) -> Result<EurekaApi, ScrapeError> {
    let http = build_client(&cfg.tls)?;
    let base_url = normalize_server(cfg);
    Ok(EurekaApi {
        base_url,
        http,
        bearer: cfg.auth.bearer.clone(),
        basic: cfg.auth.basic.clone(),
    })
}

/// `<scheme>://<server>`, defaulting the server to [`DEFAULT_API_SERVER`] and
/// choosing a scheme (when `server` has none) of `https` if TLS is configured,
/// else `http`. A trailing `/` is stripped. Port of `newAPIConfig`'s apiServer
/// normalization.
fn normalize_server(cfg: &EurekaSdConfig) -> String {
    let server = if cfg.server.is_empty() {
        DEFAULT_API_SERVER.to_string()
    } else {
        cfg.server.clone()
    };
    let with_scheme = if server.contains("://") {
        server
    } else {
        let scheme = if cfg.tls != TlsConfig::default() {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://{server}")
    };
    with_scheme.trim_end_matches('/').to_string()
}

/// Builds a `reqwest::blocking::Client` applying `tls` (CA / client identity /
/// insecure-skip-verify), mirroring the EC2/DigitalOcean client's builder.
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
        msg: format!("cannot build eureka http client: {e}"),
    })
}

impl EurekaApi {
    /// Issues a GET against `<base_url><path>` with the resolved auth, an
    /// `Accept: application/xml` header (see the module doc), and a per-call
    /// timeout, returning the response body bytes on a 2xx status.
    fn get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self
            .http
            .get(&url)
            .timeout(EUREKA_HTTP_TIMEOUT)
            .header("Accept", "application/xml");
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("eureka request to {path:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("eureka response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("eureka request to {path:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// GETs `<server>/apps`, returning the parsed applications. Port of
    /// `GetLabels`'s `getAPIResponse` + `parseAPIResponse`.
    pub fn list_applications(&self) -> Result<Applications, ScrapeError> {
        let data = self.get(APPS_API_PATH).map_err(|e| ScrapeError {
            msg: format!("cannot fetch data from eureka api: {}", e.msg),
        })?;
        parse_applications(&data).map_err(|msg| ScrapeError { msg })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    fn cfg() -> EurekaSdConfig {
        EurekaSdConfig::default()
    }

    #[test]
    fn defaults_server_and_scheme() {
        let api = new_eureka_api(&cfg()).unwrap();
        assert_eq!(api.base_url, "http://localhost:8080/eureka/v2");
    }

    #[test]
    fn explicit_server_without_scheme_gets_http() {
        let mut c = cfg();
        c.server = "eureka.local:8761".into();
        let api = new_eureka_api(&c).unwrap();
        assert_eq!(api.base_url, "http://eureka.local:8761");
    }

    #[test]
    fn server_with_scheme_is_preserved_and_trimmed() {
        let mut c = cfg();
        c.server = "https://eureka.example/eureka/".into();
        let api = new_eureka_api(&c).unwrap();
        assert_eq!(api.base_url, "https://eureka.example/eureka");
    }

    #[test]
    fn bearer_token_is_redacted_in_debug() {
        let mut c = cfg();
        c.auth = AuthConfig {
            bearer: Some("super-secret-token".into()),
            ..AuthConfig::default()
        };
        let api = new_eureka_api(&c).unwrap();
        assert_eq!(api.bearer.as_deref(), Some("super-secret-token"));
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret-token"), "{dbg}");
    }

    #[test]
    fn basic_auth_password_is_redacted_in_debug() {
        let mut c = cfg();
        c.auth = AuthConfig {
            basic: Some(("eu-user".into(), "eu-pass".into())),
            ..AuthConfig::default()
        };
        let api = new_eureka_api(&c).unwrap();
        assert_eq!(
            api.basic,
            Some(("eu-user".to_string(), "eu-pass".to_string()))
        );
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("eu-pass"), "{dbg}");
    }
}
