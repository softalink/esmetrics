//! Vultr HTTP API client: endpoint/scheme normalization, bearer-token auth,
//! TLS, and the cursor-paginated `/v2/instances` listing the refresh loop
//! issues.
//!
//! Port of `lib/promscrape/discovery/vultr/api.go`'s `newAPIConfig` (endpoint
//! `https://api.vultr.com`, port default 80, `bearer_token` required) and
//! `instance.go`'s `getInstances` (GET `/v2/instances?per_page=100`, following
//! `meta.links.next` as an opaque `cursor` param until it is empty). Same
//! single-bearer-REST shape as `scrape::digitalocean`, differing only in the
//! pagination style (opaque cursor vs. next-URL).

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use crate::client::TlsConfig;
pub use crate::scrape::config::ScrapeError;
use crate::scrape::config::VultrSdConfig;

use super::labels::{parse_api_response, Instance};

/// Per-request client-side timeout. A refresh pages through `/v2/instances`
/// with several sequential GETs; each is capped so a hung Vultr API can't
/// stall the refresh thread — and thus a [`super::VultrDiscovery`]
/// `Drop`/`stop` — indefinitely. Mirrors the DigitalOcean client's rationale.
const VULTR_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Default Vultr API endpoint, matching `api.go`'s `newAPIConfig`.
const DEFAULT_API_SERVER: &str = "https://api.vultr.com";

/// The instances listing path, matching `instance.go`'s `getInstances`.
const INSTANCES_API_PATH: &str = "/v2/instances";

/// Instances requested per page. Matches upstream's hardcoded `per_page=100`.
const PER_PAGE: u32 = 100;

/// Resolved Vultr API access: base URL, an HTTP client with TLS applied, and
/// the resolved bearer token to attach per request.
///
/// `Debug` is hand-written to redact the bearer token — defense-in-depth
/// against a future `{:?}` in a log line (mirrors `DigitaloceanApi`).
pub struct VultrApi {
    base_url: String,
    http: HttpClient,
    bearer: String,
}

impl std::fmt::Debug for VultrApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VultrApi")
            .field("base_url", &self.base_url)
            .field("bearer", &"<redacted>")
            .finish()
    }
}

/// Builds a [`VultrApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `cfg.auth.bearer` becomes the bearer token; a missing token is an error
///   (upstream: `missing bearer_token option`).
/// - server defaults to `https://api.vultr.com`; a scheme is prepended when
///   absent (`https` when TLS is configured, else `http`).
///
/// Fails on genuinely bad config (missing bearer token, bad TLS material) —
/// never because the Vultr API is unreachable; listing happens later on the
/// refresh thread.
pub fn new_vultr_api(cfg: &VultrSdConfig) -> Result<VultrApi, ScrapeError> {
    let bearer = cfg.auth.bearer.clone().ok_or_else(|| ScrapeError {
        msg: "vultr_sd_config: missing `bearer_token` option".to_string(),
    })?;
    let http = build_client(&cfg.tls)?;
    let base_url = normalize_server(cfg);
    Ok(VultrApi {
        base_url,
        http,
        bearer,
    })
}

/// `<scheme>://<server>`, defaulting the server to [`DEFAULT_API_SERVER`] and
/// choosing a scheme (when `server` has none) of `https` if TLS is configured,
/// else `http`. A trailing `/` is stripped. Port of `newAPIConfig`'s apiServer
/// normalization (mirrors `scrape::digitalocean::client::normalize_server`).
fn normalize_server(cfg: &VultrSdConfig) -> String {
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
/// insecure-skip-verify), mirroring `scrape::digitalocean::client`'s builder.
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
        msg: format!("cannot build vultr http client: {e}"),
    })
}

/// Builds the `/v2/instances` request path with `per_page` and, when paging, a
/// url-encoded `cursor`. Port of `getInstances`' path construction.
fn build_list_path(cursor: Option<&str>) -> String {
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    ser.append_pair("per_page", &PER_PAGE.to_string());
    if let Some(c) = cursor {
        ser.append_pair("cursor", c);
    }
    format!("{INSTANCES_API_PATH}?{}", ser.finish())
}

impl VultrApi {
    /// Issues a GET against `<base_url><path>` with bearer auth and a per-call
    /// timeout, returning the response body bytes on a 2xx status.
    fn get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}{path}", self.base_url);
        let req = self
            .http
            .get(&url)
            .timeout(VULTR_HTTP_TIMEOUT)
            .header("Accept", "application/json")
            .bearer_auth(&self.bearer);
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("vultr request to {path:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("vultr response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("vultr request to {path:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Lists every instance, following `meta.links.next` as an opaque `cursor`
    /// until it is empty and accumulating results. Port of `getInstances`.
    pub fn list_instances(&self) -> Result<Vec<Instance>, ScrapeError> {
        let mut instances = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let path = build_list_path(cursor.as_deref());
            let data = self.get(&path).map_err(|e| ScrapeError {
                msg: format!("cannot get vultr response from {path:?}: {}", e.msg),
            })?;
            let mut resp = parse_api_response(&data)?;
            instances.append(&mut resp.instances);
            let next = resp.meta.links.next;
            if next.is_empty() {
                return Ok(instances);
            }
            cursor = Some(next);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    fn cfg() -> VultrSdConfig {
        VultrSdConfig {
            auth: AuthConfig {
                bearer: Some("tok".into()),
                ..AuthConfig::default()
            },
            ..VultrSdConfig::default()
        }
    }

    #[test]
    fn defaults_endpoint_to_vultr() {
        let api = new_vultr_api(&cfg()).unwrap();
        assert_eq!(api.base_url, "https://api.vultr.com");
    }

    #[test]
    fn missing_bearer_token_is_an_error() {
        let c = VultrSdConfig::default();
        let err = new_vultr_api(&c).unwrap_err();
        assert!(err.msg.contains("bearer_token"), "{}", err.msg);
    }

    #[test]
    fn explicit_server_without_scheme_gets_http() {
        let mut c = cfg();
        c.server = "vultr.local:8080".into();
        let api = new_vultr_api(&c).unwrap();
        assert_eq!(api.base_url, "http://vultr.local:8080");
    }

    #[test]
    fn trailing_slash_is_stripped() {
        let mut c = cfg();
        c.server = "https://vultr.example/".into();
        let api = new_vultr_api(&c).unwrap();
        assert_eq!(api.base_url, "https://vultr.example");
    }

    #[test]
    fn bearer_token_is_redacted_in_debug() {
        let mut c = cfg();
        c.auth = AuthConfig {
            bearer: Some("super-secret-token".into()),
            ..AuthConfig::default()
        };
        let api = new_vultr_api(&c).unwrap();
        assert_eq!(api.bearer, "super-secret-token");
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret-token"), "{dbg}");
    }

    #[test]
    fn list_path_carries_per_page_and_cursor() {
        assert_eq!(build_list_path(None), "/v2/instances?per_page=100");
        // Cursor tokens are opaque and url-encoded.
        assert_eq!(
            build_list_path(Some("a b/c=")),
            "/v2/instances?per_page=100&cursor=a+b%2Fc%3D"
        );
    }
}
