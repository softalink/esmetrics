//! Nomad HTTP API client: auth resolution (`NOMAD_TOKEN` env / inline
//! `bearer_token` / inline `basic_auth`), server/scheme normalization, TLS,
//! and the two queries the refresh loop issues (`/v1/services`,
//! `/v1/service/<name>`).
//!
//! Port of `lib/promscrape/discovery/nomad/api.go`'s `newAPIConfig` and the
//! (non-blocking) request slices of `watch.go`. This port issues plain
//! polls, not Nomad blocking queries — see [`super`]'s module doc — so the
//! `index`/`wait`/`X-Nomad-Index` long-poll machinery from `api.go` is
//! intentionally omitted.
//!
//! ## Auth header
//!
//! Upstream feeds the resolved token into `promauth.HTTPClientConfig` as its
//! `BearerToken`, i.e. it is sent as a standard `Authorization: Bearer
//! <token>` header — NOT Nomad's native `X-Nomad-Token` header. This port
//! matches upstream exactly (bearer), for parity with vmagent.

use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use crate::client::TlsConfig;
use crate::scrape::config::{NomadSdConfig, ScrapeError};

use super::labels::{parse_service_names, parse_services, Service};

/// Per-request client-side timeout. A refresh does several sequential GETs
/// (service list + one per service); each is capped so a hung Nomad server
/// can't stall the refresh thread — and thus a [`super::NomadDiscovery`]
/// `Drop`/`stop` — indefinitely. Mirrors the Consul client's rationale.
const NOMAD_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Default Nomad API server when `server` and `NOMAD_ADDR` are both unset.
/// Port of `api.go`'s `"localhost:4646"`.
const DEFAULT_NOMAD_ADDR: &str = "localhost:4646";

/// Resolved Nomad API access: base URL, an HTTP client with TLS applied, the
/// auth to attach per request, and the configured tag separator.
///
/// `Debug` is hand-written to redact the bearer token / basic password —
/// defense-in-depth against a future `{:?}` in a log line.
pub struct NomadApi {
    base_url: String,
    http: HttpClient,
    bearer: Option<String>,
    basic: Option<(String, String)>,
    pub tag_separator: String,
}

impl std::fmt::Debug for NomadApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NomadApi")
            .field("base_url", &self.base_url)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("basic", &self.basic.as_ref().map(|(u, _)| u))
            .field("tag_separator", &self.tag_separator)
            .finish()
    }
}

/// Builds a [`NomadApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `NOMAD_TOKEN` env var (when set) becomes the bearer token; setting both
///   `NOMAD_TOKEN` and an inline `bearer_token` is rejected.
/// - inline `basic_auth` becomes HTTP basic auth.
/// - server defaults to `NOMAD_ADDR`, else `localhost:4646`; a scheme is
///   prepended when absent (`https` when TLS is configured, else `http`).
/// - `tag_separator` defaults to `,`.
///
/// Fails only on genuinely bad config (bad TLS material, conflicting auth) —
/// never because the Nomad server is unreachable; listing happens later on
/// the refresh thread.
pub fn new_nomad_api(cfg: &NomadSdConfig) -> Result<NomadApi, ScrapeError> {
    let env_token = std::env::var("NOMAD_TOKEN").ok().filter(|t| !t.is_empty());
    let bearer = match (env_token, &cfg.auth.bearer) {
        (Some(_), Some(_)) => {
            return Err(ScrapeError {
                msg: "cannot set both NOMAD_TOKEN and bearer_token".to_string(),
            });
        }
        (Some(t), _) => Some(t),
        (_, inline) => inline.clone(),
    };
    let basic = cfg.auth.basic.clone();

    let http = build_client(&cfg.tls)?;
    let base_url = normalize_server(cfg);
    let tag_separator = cfg.tag_separator.clone().unwrap_or_else(|| ",".to_string());

    Ok(NomadApi {
        base_url,
        http,
        bearer,
        basic,
        tag_separator,
    })
}

/// `<scheme>://<server>`, defaulting the server to `NOMAD_ADDR` (else
/// `localhost:4646`) and choosing a scheme (when `server` has none) of
/// `https` if TLS is configured, else `http`. A trailing `/` is stripped.
/// Port of `api.go`'s server/scheme resolution.
fn normalize_server(cfg: &NomadSdConfig) -> String {
    let server = if cfg.server.is_empty() {
        std::env::var("NOMAD_ADDR")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_NOMAD_ADDR.to_string())
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
/// / insecure-skip-verify), mirroring the Consul client's builder.
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
        msg: format!("cannot build nomad http client: {e}"),
    })
}

impl NomadApi {
    /// Issues a GET against `<base_url><path>` with the resolved auth and a
    /// per-call timeout, returning the response body bytes on a 2xx status.
    fn get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self
            .http
            .get(&url)
            .timeout(NOMAD_HTTP_TIMEOUT)
            .header("Accept", "application/json");
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("nomad request to {path:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("nomad response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("nomad request to {path:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// GETs `/v1/services<query_args>`, returning the flat list of service
    /// names across every namespace. Port of `getBlockingServiceNames`'s
    /// listing (minus the blocking-query index).
    pub fn list_service_names(&self, query_args: &str) -> Result<Vec<String>, ScrapeError> {
        let path = format!("/v1/services{query_args}");
        let data = self.get(&path)?;
        parse_service_names(&data).map_err(|msg| ScrapeError {
            msg: format!("{path:?}: {msg}"),
        })
    }

    /// GETs `/v1/service/<name><query_args>`, returning the parsed service
    /// registrations. Port of `watchForServiceAddressUpdates`'s per-service
    /// fetch.
    pub fn get_service(
        &self,
        service_name: &str,
        query_args: &str,
    ) -> Result<Vec<Service>, ScrapeError> {
        let path = format!("/v1/service/{service_name}{query_args}");
        let data = self.get(&path)?;
        parse_services(&data).map_err(|msg| ScrapeError {
            msg: format!("{path:?}: {msg}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AuthConfig;

    fn cfg() -> NomadSdConfig {
        NomadSdConfig::default()
    }

    #[test]
    fn defaults_server_and_scheme() {
        // Ensure NOMAD_ADDR doesn't leak in from the host env.
        std::env::remove_var("NOMAD_ADDR");
        let api = new_nomad_api(&cfg()).unwrap();
        assert_eq!(api.base_url, "http://localhost:4646");
        assert_eq!(api.tag_separator, ",");
    }

    #[test]
    fn explicit_server_keeps_no_scheme_default_http() {
        let mut c = cfg();
        c.server = "nomad:4646".into();
        let api = new_nomad_api(&c).unwrap();
        assert_eq!(api.base_url, "http://nomad:4646");
    }

    #[test]
    fn server_with_scheme_is_preserved() {
        let mut c = cfg();
        c.server = "https://nomad.example:4646".into();
        let api = new_nomad_api(&c).unwrap();
        assert_eq!(api.base_url, "https://nomad.example:4646");
    }

    #[test]
    fn inline_bearer_becomes_bearer_and_is_redacted_in_debug() {
        let mut c = cfg();
        c.auth = AuthConfig {
            bearer: Some("super-secret".into()),
            ..AuthConfig::default()
        };
        let api = new_nomad_api(&c).unwrap();
        assert_eq!(api.bearer.as_deref(), Some("super-secret"));
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret"), "{dbg}");
    }

    #[test]
    fn nomad_token_env_and_inline_bearer_conflict() {
        let mut c = cfg();
        c.auth = AuthConfig {
            bearer: Some("inline".into()),
            ..AuthConfig::default()
        };
        std::env::set_var("NOMAD_TOKEN", "env-token-esmagent-nomad-conflict");
        let res = new_nomad_api(&c);
        std::env::remove_var("NOMAD_TOKEN");
        assert!(res.is_err());
    }

    #[test]
    fn basic_auth_is_applied_and_password_redacted() {
        let mut c = cfg();
        c.auth = AuthConfig {
            basic: Some(("alice".into(), "hunter2".into())),
            ..AuthConfig::default()
        };
        let api = new_nomad_api(&c).unwrap();
        assert_eq!(
            api.basic,
            Some(("alice".to_string(), "hunter2".to_string()))
        );
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("hunter2"), "{dbg}");
    }
}
