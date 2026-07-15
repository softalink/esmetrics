//! Hetzner HTTP API client: per-role endpoint/auth resolution, TLS, and the
//! listing calls the refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/hetzner/api.go`'s `newAPIConfig` (per-role
//! `apiServer` + auth requirement) plus `hcloud.go`'s `getHCloudServers` /
//! `getHCloudNetworks` (both follow `meta.pagination.next_page` until
//! exhausted) and `robot.go`'s `getRobotServers` (a single `/server` GET):
//!
//! - `role: hcloud` → `https://api.hetzner.cloud`, **Bearer token** auth,
//!   paginated `/v1/servers` + `/v1/networks`.
//! - `role: robot` → `https://robot-ws.your-server.de`, **HTTP Basic** auth,
//!   `/server`.
//!
//! The endpoint is overridable (via `cfg.server`) so the stub tests can point
//! it at an in-process server. Every GET is timeout-bounded so a hung Hetzner
//! API can't stall the refresh thread — and thus a [`super::HetznerDiscovery`]
//! `Drop`/`stop` — indefinitely.

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use super::hcloud::{
    parse_hcloud_networks_list, parse_hcloud_server_list, HCloudNetwork, HCloudServer,
};
use super::robot::{parse_robot_servers, RobotServer};
use super::{HetznerSdConfig, ROLE_HCLOUD, ROLE_ROBOT};
use crate::client::TlsConfig;
pub use crate::scrape::config::ScrapeError;

/// Per-request client-side timeout. Mirrors the DigitalOcean/Consul client's
/// rationale: a paginated refresh issues several sequential GETs, each capped
/// so a hung Hetzner API can't stall the refresh thread.
const HETZNER_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Default Hetzner Cloud API endpoint (`role: hcloud`), matching `api.go`.
const DEFAULT_HCLOUD_API_SERVER: &str = "https://api.hetzner.cloud";

/// Default Hetzner Robot API endpoint (`role: robot`), matching `api.go`.
const DEFAULT_ROBOT_API_SERVER: &str = "https://robot-ws.your-server.de";

/// Resolved Hetzner API access: role, base URL, an HTTP client with TLS
/// applied, and the resolved auth to attach per request. `bearer` is used for
/// `role: hcloud`, `basic` for `role: robot`.
///
/// `Debug` is hand-written to redact the bearer token and the basic password.
pub struct HetznerApi {
    role: String,
    base_url: String,
    http: HttpClient,
    bearer: Option<String>,
    basic: Option<(String, String)>,
}

impl std::fmt::Debug for HetznerApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HetznerApi")
            .field("role", &self.role)
            .field("base_url", &self.base_url)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .finish()
    }
}

/// Builds a [`HetznerApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `role: hcloud` requires a bearer token (`cfg.auth.bearer`); `role: robot`
///   requires HTTP basic credentials (`cfg.auth.basic`).
/// - the endpoint defaults to the role's Hetzner API base, overridden by a
///   non-empty `cfg.server`; a scheme is prepended when absent (`https` when
///   TLS is configured, else `http`), and a trailing `/` is stripped.
///
/// Fails on genuinely bad config (unknown role, missing required auth, bad TLS
/// material) — never because the Hetzner API is unreachable; listing happens
/// later on the refresh thread.
pub fn new_hetzner_api(cfg: &HetznerSdConfig) -> Result<HetznerApi, ScrapeError> {
    let default_server = match cfg.role.as_str() {
        ROLE_HCLOUD => {
            if cfg.auth.bearer.is_none() {
                return Err(ScrapeError {
                    msg: "authorization (bearer_token) must be set when role is `hcloud`".into(),
                });
            }
            DEFAULT_HCLOUD_API_SERVER
        }
        ROLE_ROBOT => {
            if cfg.auth.basic.is_none() {
                return Err(ScrapeError {
                    msg: "basic_auth must be set when role is `robot`".into(),
                });
            }
            DEFAULT_ROBOT_API_SERVER
        }
        other => {
            return Err(ScrapeError {
                msg: format!("unexpected role={other:?}; must be one of `robot` or `hcloud`"),
            });
        }
    };

    let http = build_client(&cfg.tls)?;
    let base_url = normalize_server(&cfg.server, default_server, &cfg.tls);
    Ok(HetznerApi {
        role: cfg.role.clone(),
        base_url,
        http,
        bearer: cfg.auth.bearer.clone(),
        basic: cfg.auth.basic.clone(),
    })
}

/// `<scheme>://<server>`, defaulting to `default_server` and choosing a scheme
/// (when `server` has none) of `https` if TLS is configured, else `http`. A
/// trailing `/` is stripped.
fn normalize_server(server: &str, default_server: &str, tls: &TlsConfig) -> String {
    let server = if server.is_empty() {
        default_server.to_string()
    } else {
        server.to_string()
    };
    let with_scheme = if server.contains("://") {
        server
    } else {
        let scheme = if tls != &TlsConfig::default() {
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
        msg: format!("cannot build hetzner http client: {e}"),
    })
}

impl HetznerApi {
    /// Issues a GET against `<base_url><path>` with the resolved auth and a
    /// per-call timeout, returning the response body bytes on a 2xx status.
    /// Port of `discoveryutil.Client.GetAPIResponse`.
    fn get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self
            .http
            .get(&url)
            .timeout(HETZNER_HTTP_TIMEOUT)
            .header("Accept", "application/json");
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("hetzner request to {path:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("hetzner response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("hetzner request to {path:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Lists every hcloud server, following `meta.pagination.next_page` until
    /// exhausted. Port of `getHCloudServers`.
    pub fn list_hcloud_servers(&self) -> Result<Vec<HCloudServer>, ScrapeError> {
        let mut servers = Vec::new();
        let mut page = 1;
        loop {
            let data = self
                .get(&format!("/v1/servers?page={page}"))
                .map_err(|e| ScrapeError {
                    msg: format!("cannot query hcloud api for servers: {}", e.msg),
                })?;
            let (mut page_servers, next_page) = parse_hcloud_server_list(&data)?;
            servers.append(&mut page_servers);
            if next_page <= page {
                return Ok(servers);
            }
            page = next_page;
        }
    }

    /// Lists every hcloud network, following `meta.pagination.next_page` until
    /// exhausted. Port of `getHCloudNetworks`.
    pub fn list_hcloud_networks(&self) -> Result<Vec<HCloudNetwork>, ScrapeError> {
        let mut networks = Vec::new();
        let mut page = 1;
        loop {
            let data = self
                .get(&format!("/v1/networks?page={page}"))
                .map_err(|e| ScrapeError {
                    msg: format!("cannot query hcloud api for networks: {}", e.msg),
                })?;
            let (mut page_networks, next_page) = parse_hcloud_networks_list(&data)?;
            networks.append(&mut page_networks);
            if next_page <= page {
                return Ok(networks);
            }
            page = next_page;
        }
    }

    /// Lists every robot server via a single `/server` GET. Port of
    /// `getRobotServers`.
    pub fn list_robot_servers(&self) -> Result<Vec<RobotServer>, ScrapeError> {
        let data = self.get("/server").map_err(|e| ScrapeError {
            msg: format!("cannot query hetzner robot api for servers: {}", e.msg),
        })?;
        parse_robot_servers(&data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    fn hcloud_cfg() -> HetznerSdConfig {
        HetznerSdConfig {
            role: ROLE_HCLOUD.into(),
            auth: AuthConfig {
                bearer: Some("tok".into()),
                ..AuthConfig::default()
            },
            ..HetznerSdConfig::default()
        }
    }

    fn robot_cfg() -> HetznerSdConfig {
        HetznerSdConfig {
            role: ROLE_ROBOT.into(),
            auth: AuthConfig {
                basic: Some(("u".into(), "p".into())),
                ..AuthConfig::default()
            },
            ..HetznerSdConfig::default()
        }
    }

    #[test]
    fn hcloud_defaults_endpoint() {
        let api = new_hetzner_api(&hcloud_cfg()).unwrap();
        assert_eq!(api.base_url, "https://api.hetzner.cloud");
    }

    #[test]
    fn robot_defaults_endpoint() {
        let api = new_hetzner_api(&robot_cfg()).unwrap();
        assert_eq!(api.base_url, "https://robot-ws.your-server.de");
    }

    #[test]
    fn explicit_server_without_scheme_gets_http() {
        let mut c = hcloud_cfg();
        c.server = "hetzner.local:8080".into();
        let api = new_hetzner_api(&c).unwrap();
        assert_eq!(api.base_url, "http://hetzner.local:8080");
    }

    #[test]
    fn trailing_slash_is_stripped() {
        let mut c = hcloud_cfg();
        c.server = "https://hetzner.example/".into();
        let api = new_hetzner_api(&c).unwrap();
        assert_eq!(api.base_url, "https://hetzner.example");
    }

    #[test]
    fn hcloud_without_bearer_is_rejected() {
        let mut c = hcloud_cfg();
        c.auth = AuthConfig::default();
        let err = new_hetzner_api(&c).unwrap_err();
        assert!(err.msg.contains("hcloud"), "{}", err.msg);
    }

    #[test]
    fn robot_without_basic_is_rejected() {
        let mut c = robot_cfg();
        c.auth = AuthConfig::default();
        let err = new_hetzner_api(&c).unwrap_err();
        assert!(err.msg.contains("robot"), "{}", err.msg);
    }

    #[test]
    fn unknown_role_is_rejected() {
        let mut c = hcloud_cfg();
        c.role = "bogus".into();
        let err = new_hetzner_api(&c).unwrap_err();
        assert!(err.msg.contains("role"), "{}", err.msg);
    }

    #[test]
    fn secrets_are_redacted_in_debug() {
        let mut c = hcloud_cfg();
        c.auth = AuthConfig {
            bearer: Some("super-secret".into()),
            basic: Some(("user".into(), "pw-secret".into())),
        };
        let api = new_hetzner_api(&c).unwrap();
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret"), "{dbg}");
        assert!(!dbg.contains("pw-secret"), "{dbg}");
    }
}
