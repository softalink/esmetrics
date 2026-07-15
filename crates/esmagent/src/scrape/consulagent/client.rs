//! Consul Agent HTTP API client: auth resolution (config/env token, basic
//! auth), server/scheme normalization, TLS, and the three queries the refresh
//! loop issues against the LOCAL agent (`/v1/agent/self`, `/v1/agent/services`,
//! `/v1/agent/health/service/name/<svc>`).
//!
//! Port of `lib/promscrape/discovery/consulagent/api.go`'s `newAPIConfig`
//! (reusing the `consul` package's `GetToken`/`GetAgentInfo`) and the request
//! slices of `watch.go`. Unlike the plain Consul port, discovery targets the
//! local agent's own endpoints rather than the cluster catalog/health APIs.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use crate::client::TlsConfig;
use crate::scrape::config::{ConsulagentSdConfig, ScrapeError};

use super::labels::{parse_agent, parse_service_nodes, Agent, Service, ServiceNode};

/// Per-request client-side timeout. A refresh does several sequential GETs
/// (agent self + service list + one per service); each is capped so a hung
/// agent can't stall the refresh thread — and thus a
/// [`super::ConsulagentDiscovery`] `Drop`/`stop` — indefinitely. Mirrors the
/// Consul client's per-call timeout rationale.
const CONSULAGENT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolved Consul Agent API access: base URL, an HTTP client with TLS
/// applied, the auth to attach per request, and the configured tag separator.
///
/// `Debug` is hand-written to redact the bearer token / basic password —
/// defense-in-depth against a future `{:?}` in a log line.
pub struct ConsulagentApi {
    base_url: String,
    http: HttpClient,
    bearer: Option<String>,
    basic: Option<(String, String)>,
    pub tag_separator: String,
}

impl std::fmt::Debug for ConsulagentApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsulagentApi")
            .field("base_url", &self.base_url)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .field("tag_separator", &self.tag_separator)
            .finish()
    }
}

/// Builds a [`ConsulagentApi`] from `cfg`, mirroring `newAPIConfig`:
/// - token from `cfg.token`, else `CONSUL_HTTP_TOKEN_FILE`, else
///   `CONSUL_HTTP_TOKEN` (see [`resolve_token`]) becomes the bearer token;
///   setting both a token and an inline `bearer_token` is rejected.
/// - `username`/`password` become HTTP basic auth; setting both `username`
///   and an inline `basic_auth` is rejected.
/// - server defaults to `localhost:8500`; a scheme is prepended when absent
///   (`cfg.scheme`, else `https` when TLS is configured, else `http`).
/// - `tag_separator` defaults to `,`.
///
/// Fails only on genuinely bad config (unreadable token file, bad TLS
/// material, conflicting auth) — never because the agent is unreachable;
/// agent-info resolution and listing happen later on the refresh thread.
pub fn new_consulagent_api(cfg: &ConsulagentSdConfig) -> Result<ConsulagentApi, ScrapeError> {
    let token = resolve_token(cfg)?;
    let bearer = match (token, &cfg.auth.bearer) {
        (Some(t), Some(_)) if !t.is_empty() => {
            return Err(ScrapeError {
                msg: "cannot set both token and bearer_token configs".to_string(),
            });
        }
        (Some(t), _) if !t.is_empty() => Some(t),
        (_, inline) => inline.clone(),
    };

    let basic = match (&cfg.username, &cfg.auth.basic) {
        (Some(u), Some(_)) if !u.is_empty() => {
            return Err(ScrapeError {
                msg: "cannot set both username and basic_auth configs".to_string(),
            });
        }
        (Some(u), _) if !u.is_empty() => {
            Some((u.clone(), cfg.password.clone().unwrap_or_default()))
        }
        (_, inline) => inline.clone(),
    };

    let http = build_client(&cfg.tls)?;
    let base_url = normalize_server(cfg);
    let tag_separator = cfg.tag_separator.clone().unwrap_or_else(|| ",".to_string());

    Ok(ConsulagentApi {
        base_url,
        http,
        bearer,
        basic,
        tag_separator,
    })
}

/// Resolves the Consul ACL token, mirroring `GetToken`: `cfg.token` wins;
/// else `CONSUL_HTTP_TOKEN_FILE` (its contents; an unreadable file is an
/// error); else `CONSUL_HTTP_TOKEN` (possibly empty — an empty token is
/// allowed, for a Consul with ACLs disabled). Returns `None` when no source
/// yields a token.
fn resolve_token(cfg: &ConsulagentSdConfig) -> Result<Option<String>, ScrapeError> {
    if let Some(t) = cfg.token.as_ref().filter(|t| !t.is_empty()) {
        return Ok(Some(t.clone()));
    }
    if let Ok(file) = std::env::var("CONSUL_HTTP_TOKEN_FILE") {
        if !file.is_empty() {
            let data = std::fs::read_to_string(&file).map_err(|e| ScrapeError {
                msg: format!(
                    "cannot read consul token file {file:?}; probably `token` arg is missing \
                     in `consulagent_sd_config`? error: {e}"
                ),
            })?;
            return Ok(Some(data));
        }
    }
    match std::env::var("CONSUL_HTTP_TOKEN") {
        Ok(t) => Ok(Some(t)),
        Err(_) => Ok(None),
    }
}

/// `<scheme>://<server>`, defaulting the server to `localhost:8500` and
/// choosing a scheme (when `server` has none) of `cfg.scheme`, else `https`
/// if TLS is configured, else `http`. A trailing `/` is stripped.
fn normalize_server(cfg: &ConsulagentSdConfig) -> String {
    let server = if cfg.server.is_empty() {
        "localhost:8500"
    } else {
        cfg.server.as_str()
    };
    let with_scheme = if server.contains("://") {
        server.to_string()
    } else {
        let scheme = match cfg.scheme.as_deref().filter(|s| !s.is_empty()) {
            Some(s) => s.to_string(),
            None => {
                if cfg.tls != TlsConfig::default() {
                    "https".to_string()
                } else {
                    "http".to_string()
                }
            }
        };
        format!("{scheme}://{server}")
    };
    with_scheme.trim_end_matches('/').to_string()
}

/// Builds a `reqwest::blocking::Client` applying `tls` (CA / client identity
/// / insecure-skip-verify), mirroring the Consul client builder.
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
        msg: format!("cannot build consulagent http client: {e}"),
    })
}

impl ConsulagentApi {
    /// Issues a GET against `<base_url><path>` with the resolved auth and a
    /// per-call timeout, returning the response body bytes on a 2xx status.
    fn get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self
            .http
            .get(&url)
            .timeout(CONSULAGENT_HTTP_TIMEOUT)
            .header("Accept", "application/json");
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("consulagent request to {path:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("consulagent response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("consulagent request to {path:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Resolves the local agent info via `/v1/agent/self`. Port of
    /// `GetAgentInfo` — the agent's member address / datacenter / node name /
    /// metadata drive both service-datacenter filtering and the emitted
    /// `__meta_consulagent_*` labels.
    pub fn get_agent(&self) -> Result<Agent, ScrapeError> {
        let data = self.get("/v1/agent/self")?;
        parse_agent(&data).map_err(|msg| ScrapeError {
            msg: format!("cannot obtain consul datacenter: {msg}"),
        })
    }

    /// GETs `/v1/agent/services<query_args>`, returning the raw
    /// `{ "<service-id>": {Service...}, ... }` map (service ID -> its
    /// definition). Port of `getServiceNames`'s request.
    pub fn list_agent_services(
        &self,
        query_args: &str,
    ) -> Result<BTreeMap<String, Service>, ScrapeError> {
        let path = format!("/v1/agent/services{query_args}");
        let data = self.get(&path)?;
        serde_json::from_slice(&data).map_err(|e| ScrapeError {
            msg: format!("cannot parse response from {path:?}: {e}"),
        })
    }

    /// GETs `/v1/agent/health/service/name/<svc>`, returning the parsed
    /// service nodes. Port of `watchForServiceNodesUpdates`'s request (no
    /// query args). `service` is URL-path-escaped by the caller only if
    /// needed; agent service names are plain identifiers here.
    pub fn get_agent_service_nodes(&self, service: &str) -> Result<Vec<ServiceNode>, ScrapeError> {
        let path = format!("/v1/agent/health/service/name/{service}");
        let data = self.get(&path)?;
        parse_service_nodes(&data).map_err(|msg| ScrapeError {
            msg: format!("{path:?}: {msg}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    fn cfg() -> ConsulagentSdConfig {
        ConsulagentSdConfig::default()
    }

    #[test]
    fn defaults_server_and_scheme() {
        let api = new_consulagent_api(&cfg()).unwrap();
        assert_eq!(api.base_url, "http://localhost:8500");
        assert_eq!(api.tag_separator, ",");
    }

    #[test]
    fn explicit_server_and_scheme() {
        let mut c = cfg();
        c.server = "consul:8500".into();
        c.scheme = Some("https".into());
        let api = new_consulagent_api(&c).unwrap();
        assert_eq!(api.base_url, "https://consul:8500");
    }

    #[test]
    fn token_becomes_bearer_and_is_redacted_in_debug() {
        let mut c = cfg();
        c.token = Some("super-secret".into());
        let api = new_consulagent_api(&c).unwrap();
        assert_eq!(api.bearer.as_deref(), Some("super-secret"));
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret"), "{dbg}");
    }

    #[test]
    fn token_and_inline_bearer_conflict() {
        let mut c = cfg();
        c.token = Some("t".into());
        c.auth = AuthConfig {
            bearer: Some("b".into()),
            ..AuthConfig::default()
        };
        assert!(new_consulagent_api(&c).is_err());
    }

    #[test]
    fn username_password_becomes_basic() {
        let mut c = cfg();
        c.username = Some("alice".into());
        c.password = Some("hunter2".into());
        let api = new_consulagent_api(&c).unwrap();
        assert_eq!(
            api.basic,
            Some(("alice".to_string(), "hunter2".to_string()))
        );
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("hunter2"), "{dbg}");
    }
}
