//! PuppetDB HTTP API client: URL validation/normalization, bearer/basic auth,
//! TLS, and the single `POST /pdb/query/v4` PQL query the refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/puppetdb/api.go`'s `newAPIConfig`
//! (required `url` with an `http`/`https` scheme and a host; required `query`)
//! and `resource.go`'s `getResourceList` (POST the PQL as a JSON
//! `{"query": "<pql>"}` body to `<url>/pdb/query/v4`, parse the resource
//! array). Simpler than Consul/EC2: one POST endpoint, no pagination and no
//! bootstrap step.

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};
use url::Url;

use crate::client::TlsConfig;
use crate::scrape::config::{PuppetdbSdConfig, ScrapeError};

use super::labels::{parse_resources, Resource};

/// Per-request client-side timeout. The single query POST is capped so a hung
/// PuppetDB server can't stall the refresh thread — and thus a
/// [`super::PuppetdbDiscovery`] `Drop`/`stop` — indefinitely. Mirrors the
/// DigitalOcean/Nomad client's rationale.
const PUPPETDB_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The PuppetDB query endpoint, matching `resource.go`'s
/// `GetAPIResponseWithReqParams("/pdb/query/v4", ...)`.
const QUERY_API_PATH: &str = "/pdb/query/v4";

/// Resolved PuppetDB API access: base URL, an HTTP client with TLS applied,
/// and the resolved auth (bearer token, else HTTP basic credentials) to attach
/// to the query POST.
///
/// `Debug` is hand-written to redact the bearer token and the basic password —
/// defense-in-depth against a future `{:?}` in a log line (mirrors
/// `DigitaloceanApi`, which shows the basic username but never the password).
pub struct PuppetdbApi {
    base_url: String,
    http: HttpClient,
    bearer: Option<String>,
    basic: Option<(String, String)>,
}

impl std::fmt::Debug for PuppetdbApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PuppetdbApi")
            .field("base_url", &self.base_url)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .finish()
    }
}

/// Builds a [`PuppetdbApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `url` must be present, parse cleanly, use an `http`/`https` scheme, and
///   have a host — otherwise this errors.
/// - `query` must be present.
/// - `cfg.auth.bearer` becomes the bearer token; failing that, `cfg.auth.basic`
///   becomes HTTP basic credentials.
///
/// The URL/query checks are redundant with [`super::build_puppetdb_sd_config`]
/// (which rejects an empty `url`/`query` at config-parse time), but this is the
/// sole validator of the URL's scheme/host and it runs for a
/// programmatically-built config too — mirroring upstream's `Test_newAPIConfig`
/// contract. Fails only on genuinely bad config (bad URL/query, bad TLS
/// material) — never because the PuppetDB server is unreachable; the query
/// happens later on the refresh thread.
pub fn new_puppetdb_api(cfg: &PuppetdbSdConfig) -> Result<PuppetdbApi, ScrapeError> {
    let base_url = validate_and_normalize_url(&cfg.url)?;
    if cfg.query.is_empty() {
        return Err(ScrapeError::new("query missing"));
    }
    let http = build_client(&cfg.tls)?;
    Ok(PuppetdbApi {
        base_url,
        http,
        bearer: cfg.auth.bearer.clone(),
        basic: cfg.auth.basic.clone(),
    })
}

/// Validates `raw` the way `newAPIConfig` does — non-empty, parseable, an
/// `http`/`https` scheme, and a non-empty host — returning the URL with any
/// trailing `/` stripped.
fn validate_and_normalize_url(raw: &str) -> Result<String, ScrapeError> {
    if raw.is_empty() {
        return Err(ScrapeError::new("URL is missing"));
    }
    let parsed =
        Url::parse(raw).map_err(|e| ScrapeError::new(format!("cannot parse {raw:?}: {e}")))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(ScrapeError::new(format!(
            "URL {raw:?} scheme must be 'http' or 'https'"
        )));
    }
    if parsed.host_str().unwrap_or("").is_empty() {
        return Err(ScrapeError::new(format!("host is missing in URL {raw:?}")));
    }
    Ok(raw.trim_end_matches('/').to_string())
}

/// Builds a `reqwest::blocking::Client` applying `tls` (CA / client identity /
/// insecure-skip-verify), mirroring the DigitalOcean/Nomad client's builder.
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
        msg: format!("cannot build puppetdb http client: {e}"),
    })
}

impl PuppetdbApi {
    /// Issues the `POST <base_url>/pdb/query/v4` with a JSON
    /// `{"query": "<query>"}` body, the resolved auth, and a per-call timeout,
    /// returning the parsed resource list on a 2xx status. Port of
    /// `getResourceList`.
    pub fn get_resources(&self, query: &str) -> Result<Vec<Resource>, ScrapeError> {
        let url = format!("{}{QUERY_API_PATH}", self.base_url);
        let body = serde_json::json!({ "query": query }).to_string();
        let mut req = self
            .http
            .post(&url)
            .timeout(PUPPETDB_HTTP_TIMEOUT)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .body(body);
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("puppetdb request to {QUERY_API_PATH:?} failed: {e}"),
        })?;
        let status = resp.status();
        let bytes = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("puppetdb response from {QUERY_API_PATH:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("puppetdb request to {QUERY_API_PATH:?} failed: status {status}"),
            });
        }
        parse_resources(&bytes).map_err(|msg| ScrapeError {
            msg: format!("{QUERY_API_PATH:?}: {msg}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    const QUERY: &str = r#"resources { type = "Class" and title = "Prometheus::Node_exporter" }"#;

    fn cfg(url: &str, query: &str) -> PuppetdbSdConfig {
        PuppetdbSdConfig {
            url: url.to_string(),
            query: query.to_string(),
            ..PuppetdbSdConfig::default()
        }
    }

    /// Port of upstream `api_test.go`'s `Test_newAPIConfig`: a valid
    /// http(s) URL + non-empty query succeeds; an empty URL, empty query, or
    /// non-http(s) scheme is rejected.
    #[test]
    fn new_api_config_validation_matches_upstream() {
        assert!(new_puppetdb_api(&cfg("https://puppetdb.example.com", QUERY)).is_ok());
        assert!(new_puppetdb_api(&cfg("", QUERY)).is_err());
        assert!(new_puppetdb_api(&cfg("https://puppetdb.example.com", "")).is_err());
        assert!(new_puppetdb_api(&cfg("ftp://invalid.url", QUERY)).is_err());
    }

    #[test]
    fn trailing_slash_is_stripped() {
        let api = new_puppetdb_api(&cfg("https://puppetdb.example.com/", QUERY)).unwrap();
        assert_eq!(api.base_url, "https://puppetdb.example.com");
    }

    #[test]
    fn bearer_token_is_redacted_in_debug() {
        let mut c = cfg("https://puppetdb.example.com", QUERY);
        c.auth = AuthConfig {
            bearer: Some("super-secret-token".into()),
            ..AuthConfig::default()
        };
        let api = new_puppetdb_api(&c).unwrap();
        assert_eq!(api.bearer.as_deref(), Some("super-secret-token"));
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret-token"), "{dbg}");
    }

    #[test]
    fn basic_auth_password_is_redacted_in_debug() {
        let mut c = cfg("https://puppetdb.example.com", QUERY);
        c.auth = AuthConfig {
            basic: Some(("pdb-user".into(), "pdb-pass".into())),
            ..AuthConfig::default()
        };
        let api = new_puppetdb_api(&c).unwrap();
        assert_eq!(
            api.basic,
            Some(("pdb-user".to_string(), "pdb-pass".to_string()))
        );
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("pdb-pass"), "{dbg}");
    }
}
