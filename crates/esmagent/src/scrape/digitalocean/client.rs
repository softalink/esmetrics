//! DigitalOcean HTTP API client: endpoint/scheme normalization, bearer-token
//! auth, TLS, and the paginated `/v2/droplets` listing the refresh loop
//! issues.
//!
//! Port of `lib/promscrape/discovery/digitalocean/api.go`'s `newAPIConfig`
//! (endpoint default `https://api.digitalocean.com`, port default 80) and
//! `getDroplets` (follow `links.pages.next` until exhausted). Simpler than
//! Consul/EC2: a single bearer-auth REST endpoint with cursor pagination, no
//! datacenter/region/agent bootstrap step.

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use crate::client::TlsConfig;
use crate::scrape::config::DigitaloceanSdConfig;
pub use crate::scrape::config::ScrapeError;

use super::labels::{next_url_path, parse_api_response, Droplet};

/// Per-request client-side timeout. A refresh pages through `/v2/droplets`
/// with several sequential GETs; each is capped so a hung DigitalOcean API
/// can't stall the refresh thread — and thus a [`super::DigitaloceanDiscovery`]
/// `Drop`/`stop` — indefinitely. Mirrors the Consul/EC2 client's rationale.
const DO_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Default DigitalOcean API endpoint, matching `api.go`'s `newAPIConfig`.
const DEFAULT_API_SERVER: &str = "https://api.digitalocean.com";

/// The droplets listing path, matching `api.go`'s `dropletsAPIPath`.
const DROPLETS_API_PATH: &str = "/v2/droplets";

/// Resolved DigitalOcean API access: base URL, an HTTP client with TLS
/// applied, and the resolved auth (bearer token, else HTTP basic credentials)
/// to attach per request.
///
/// `Debug` is hand-written to redact the bearer token and the basic password —
/// defense-in-depth against a future `{:?}` in a log line (mirrors `ConsulApi`,
/// which shows the basic username but never the password).
pub struct DigitaloceanApi {
    base_url: String,
    http: HttpClient,
    bearer: Option<String>,
    basic: Option<(String, String)>,
}

impl std::fmt::Debug for DigitaloceanApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DigitaloceanApi")
            .field("base_url", &self.base_url)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .finish()
    }
}

/// Builds a [`DigitaloceanApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `cfg.auth.bearer` becomes the bearer token; failing that,
///   `cfg.auth.basic` becomes HTTP basic credentials (mirrors `ConsulApi`).
/// - server defaults to `https://api.digitalocean.com`; a scheme is prepended
///   when absent (`https` when TLS is configured, else `http`).
///
/// Fails only on genuinely bad config (bad TLS material) — never because the
/// DigitalOcean API is unreachable; listing happens later on the refresh
/// thread.
pub fn new_digitalocean_api(cfg: &DigitaloceanSdConfig) -> Result<DigitaloceanApi, ScrapeError> {
    let http = build_client(&cfg.tls)?;
    let base_url = normalize_server(cfg);
    Ok(DigitaloceanApi {
        base_url,
        http,
        bearer: cfg.auth.bearer.clone(),
        basic: cfg.auth.basic.clone(),
    })
}

/// `<scheme>://<server>`, defaulting the server to
/// [`DEFAULT_API_SERVER`] and choosing a scheme (when `server` has none) of
/// `https` if TLS is configured, else `http`. A trailing `/` is stripped.
/// Port of `newAPIConfig`'s apiServer normalization.
fn normalize_server(cfg: &DigitaloceanSdConfig) -> String {
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

/// Builds a `reqwest::blocking::Client` applying `tls` (CA / client identity
/// / insecure-skip-verify), mirroring `scrape::consul::client`'s builder.
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
        msg: format!("cannot build digitalocean http client: {e}"),
    })
}

impl DigitaloceanApi {
    /// Issues a GET against `<base_url><path>` with the resolved auth and a
    /// per-call timeout, returning the response body bytes on a 2xx status.
    fn get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self
            .http
            .get(&url)
            .timeout(DO_HTTP_TIMEOUT)
            .header("Accept", "application/json");
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("digitalocean request to {path:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("digitalocean response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("digitalocean request to {path:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Lists every droplet, following `links.pages.next` until exhausted and
    /// accumulating results. Port of `getDroplets`.
    pub fn list_droplets(&self) -> Result<Vec<Droplet>, ScrapeError> {
        let mut droplets = Vec::new();
        let mut next_path = DROPLETS_API_PATH.to_string();
        loop {
            let data = self.get(&next_path).map_err(|e| ScrapeError {
                msg: format!("cannot fetch data from digitalocean list api: {}", e.msg),
            })?;
            let mut resp = parse_api_response(&data)?;
            droplets.append(&mut resp.droplets);
            match next_url_path(&resp.links.pages.next)? {
                Some(path) => next_path = path,
                None => return Ok(droplets),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    fn cfg() -> DigitaloceanSdConfig {
        DigitaloceanSdConfig::default()
    }

    #[test]
    fn defaults_endpoint_to_digitalocean() {
        let api = new_digitalocean_api(&cfg()).unwrap();
        assert_eq!(api.base_url, "https://api.digitalocean.com");
    }

    #[test]
    fn explicit_server_without_scheme_gets_http() {
        let mut c = cfg();
        c.server = "do.local:8080".into();
        let api = new_digitalocean_api(&c).unwrap();
        assert_eq!(api.base_url, "http://do.local:8080");
    }

    #[test]
    fn trailing_slash_is_stripped() {
        let mut c = cfg();
        c.server = "https://do.example/".into();
        let api = new_digitalocean_api(&c).unwrap();
        assert_eq!(api.base_url, "https://do.example");
    }

    #[test]
    fn bearer_token_is_redacted_in_debug() {
        let mut c = cfg();
        c.auth = AuthConfig {
            bearer: Some("super-secret-token".into()),
            ..AuthConfig::default()
        };
        let api = new_digitalocean_api(&c).unwrap();
        assert_eq!(api.bearer.as_deref(), Some("super-secret-token"));
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret-token"), "{dbg}");
    }

    #[test]
    fn basic_auth_password_is_redacted_in_debug() {
        let mut c = cfg();
        c.auth = AuthConfig {
            basic: Some(("do-user".into(), "do-pass".into())),
            ..AuthConfig::default()
        };
        let api = new_digitalocean_api(&c).unwrap();
        assert_eq!(
            api.basic,
            Some(("do-user".to_string(), "do-pass".to_string()))
        );
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("do-pass"), "{dbg}");
    }
}
