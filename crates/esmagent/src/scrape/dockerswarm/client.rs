//! Docker Swarm API client: `host` parsing into a transport (Unix socket vs
//! HTTP), the `filters` query-arg builder, and the per-role endpoint fetches.
//!
//! Port of `lib/promscrape/discovery/dockerswarm/api.go`'s `newAPIConfig` /
//! `getAPIResponse` / `getFiltersQueryArg` and the per-endpoint fetches in
//! `services.go`/`tasks.go`/`nodes.go`/`network.go`. Docker Swarm uses the
//! same UNVERSIONED API paths as Docker (`/services`, `/tasks`, `/nodes`,
//! `/networks`).
//!
//! The Unix-socket transport is REUSED verbatim from the docker provider
//! ([`crate::scrape::docker::transport::unix_socket_get`]); only the HTTP arm
//! (auth/TLS + `reqwest::blocking`) and the role-scoped `filters` application
//! are dockerswarm-specific.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::blocking::Client as HttpClient;

use crate::client::{AuthConfig, TlsConfig};
use crate::scrape::config::ScrapeError;
use crate::scrape::docker::transport::unix_socket_get;

use super::network::{parse_networks, Network};
use super::nodes::{parse_nodes, Node};
use super::services::{parse_services, Service};
use super::tasks::{parse_tasks, Task};
use super::{DockerswarmFilter, DockerswarmSdConfig, Role};

/// Per-request client-side timeout. Each refresh issues several sequential
/// GETs; each is capped so a hung dockerd can't stall the refresh thread — and
/// thus a [`super::DockerswarmDiscovery`] `Drop`/`stop` — indefinitely.
/// Mirrors the docker provider's timeout.
const DOCKERSWARM_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The transport used to reach the Docker Swarm API: a Unix socket (docker's
/// custom HTTP/1.1 client) or an HTTP endpoint (`reqwest::blocking`, with
/// auth/TLS applied).
enum Transport {
    Unix {
        socket_path: String,
    },
    Http {
        base_url: String,
        http: HttpClient,
        bearer: Option<String>,
        basic: Option<(String, String)>,
    },
}

/// Resolved Docker Swarm API access: a [`Transport`], the escaped `filters`
/// query arg (applied only to the role's own endpoint), and the discovery
/// [`Role`]. `Debug` is hand-written to redact the bearer token / basic
/// password.
pub struct DockerswarmApi {
    transport: Transport,
    filters_query_arg: String,
    role: Role,
}

impl std::fmt::Debug for DockerswarmApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("DockerswarmApi");
        s.field("role", &self.role);
        match &self.transport {
            Transport::Unix { socket_path } => {
                s.field("transport", &"unix")
                    .field("socket_path", socket_path);
            }
            Transport::Http { base_url, .. } => {
                s.field("transport", &"http")
                    .field("base_url", base_url)
                    .field("auth", &"<redacted>");
            }
        }
        s.finish()
    }
}

/// Builds a [`DockerswarmApi`] from `cfg`, mirroring `newAPIConfig`. Fails on
/// genuinely bad config (empty/invalid `host`, invalid `role`, bad TLS
/// material) — never because the API is unreachable; fetching happens later on
/// the refresh thread.
pub fn new_dockerswarm_api(cfg: &DockerswarmSdConfig) -> Result<DockerswarmApi, ScrapeError> {
    let role = Role::parse(&cfg.role)?;
    let transport = parse_host(&cfg.host, &cfg.auth, &cfg.tls)?;
    Ok(DockerswarmApi {
        transport,
        filters_query_arg: get_filters_query_arg(&cfg.filters),
        role,
    })
}

/// Parses the Docker Swarm `host` into a [`Transport`] (same scheme handling
/// as the docker provider):
/// - `unix://<path>` -> Unix-socket transport at `<path>`.
/// - `tcp://host:port` -> HTTP transport at `http://host:port`.
/// - `http(s)://…` -> HTTP transport as-is.
/// - anything else (incl. empty) -> error.
fn parse_host(host: &str, auth: &AuthConfig, tls: &TlsConfig) -> Result<Transport, ScrapeError> {
    if let Some(path) = host.strip_prefix("unix://") {
        if path.is_empty() {
            return Err(ScrapeError {
                msg: "dockerswarm host unix:// requires a socket path".to_string(),
            });
        }
        return Ok(Transport::Unix {
            socket_path: path.to_string(),
        });
    }

    let base_url = if let Some(rest) = host.strip_prefix("tcp://") {
        format!("http://{rest}")
    } else if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else if host.is_empty() {
        return Err(ScrapeError {
            msg: "dockerswarm_sd_config requires a non-empty `host`".to_string(),
        });
    } else {
        return Err(ScrapeError {
            msg: format!(
                "invalid dockerswarm host {host:?}: expected a unix://, tcp://, http:// or https:// URL"
            ),
        });
    };

    let http = build_client(tls)?;
    Ok(Transport::Http {
        base_url: base_url.trim_end_matches('/').to_string(),
        http,
        bearer: auth.bearer.clone(),
        basic: auth.basic.clone(),
    })
}

/// Builds a `reqwest::blocking::Client` applying `tls`. Mirrors the docker
/// provider's builder.
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
        msg: format!("cannot build dockerswarm http client: {e}"),
    })
}

impl DockerswarmApi {
    /// This API's discovery role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Issues a GET against `path`, appending the escaped `filters` query arg
    /// only when `apply_filters` is set (the role's own endpoint), mirroring
    /// `getAPIResponse`'s conditional `filtersQueryArg`. Returns the body bytes
    /// on a 2xx status.
    fn get_api_response(&self, path: &str, apply_filters: bool) -> Result<Vec<u8>, ScrapeError> {
        let full_path = self.with_filters(path, apply_filters);
        match &self.transport {
            Transport::Unix { socket_path } => {
                let resp = unix_socket_get(socket_path, &full_path, DOCKERSWARM_HTTP_TIMEOUT)?;
                if !(200..300).contains(&resp.status) {
                    return Err(ScrapeError {
                        msg: format!(
                            "dockerswarm request to {path:?} failed: status {}",
                            resp.status
                        ),
                    });
                }
                Ok(resp.body)
            }
            Transport::Http {
                base_url,
                http,
                bearer,
                basic,
            } => {
                let url = format!("{base_url}{full_path}");
                let mut req = http
                    .get(&url)
                    .timeout(DOCKERSWARM_HTTP_TIMEOUT)
                    .header("Accept", "application/json");
                if let Some(token) = bearer {
                    req = req.bearer_auth(token);
                } else if let Some((user, pass)) = basic {
                    req = req.basic_auth(user, Some(pass));
                }
                let resp = req.send().map_err(|e| ScrapeError {
                    msg: format!("dockerswarm request to {path:?} failed: {e}"),
                })?;
                let status = resp.status();
                let body = resp.bytes().map_err(|e| ScrapeError {
                    msg: format!("dockerswarm response from {path:?}: {e}"),
                })?;
                if !status.is_success() {
                    return Err(ScrapeError {
                        msg: format!("dockerswarm request to {path:?} failed: status {status}"),
                    });
                }
                Ok(body.to_vec())
            }
        }
    }

    /// Appends the escaped `filters` query arg to `path` when `apply` is set,
    /// choosing `?` or `&` as upstream's `getAPIResponse` does.
    fn with_filters(&self, path: &str, apply: bool) -> String {
        if !apply || self.filters_query_arg.is_empty() {
            return path.to_string();
        }
        let sep = if path.contains('?') { '&' } else { '?' };
        format!("{path}{sep}filters={}", self.filters_query_arg)
    }

    /// Fetches and parses `GET /networks` (never filtered). Port of
    /// `getNetworks`.
    pub fn get_networks(&self) -> Result<Vec<Network>, ScrapeError> {
        let data = self
            .get_api_response("/networks", false)
            .map_err(|e| ScrapeError {
                msg: format!("cannot query dockerswarm api for networks: {}", e.msg),
            })?;
        parse_networks(&data)
    }

    /// Fetches and parses `GET /services` (filtered only for the `services`
    /// role). Port of `getServices`.
    pub fn get_services(&self) -> Result<Vec<Service>, ScrapeError> {
        let data = self
            .get_api_response("/services", self.role == Role::Services)
            .map_err(|e| ScrapeError {
                msg: format!("cannot query dockerswarm api for services: {}", e.msg),
            })?;
        parse_services(&data)
    }

    /// Fetches and parses `GET /nodes` (filtered only for the `nodes` role).
    /// Port of `getNodes`.
    pub fn get_nodes(&self) -> Result<Vec<Node>, ScrapeError> {
        let data = self
            .get_api_response("/nodes", self.role == Role::Nodes)
            .map_err(|e| ScrapeError {
                msg: format!("cannot query dockerswarm api for nodes: {}", e.msg),
            })?;
        parse_nodes(&data)
    }

    /// Fetches and parses `GET /tasks` (filtered only for the `tasks` role).
    /// Port of `getTasks`.
    pub fn get_tasks(&self) -> Result<Vec<Task>, ScrapeError> {
        let data = self
            .get_api_response("/tasks", self.role == Role::Tasks)
            .map_err(|e| ScrapeError {
                msg: format!("cannot query dockerswarm api for tasks: {}", e.msg),
            })?;
        parse_tasks(&data)
    }
}

/// Builds the escaped `filters` query arg. Port of `getFiltersQueryArg`:
/// `{name: [values...], ...}` (keys sorted via `BTreeMap`, matching Go's map
/// JSON key ordering; values in listed order), then URL-query-escaped.
fn get_filters_query_arg(filters: &[DockerswarmFilter]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let mut m: BTreeMap<&str, &Vec<String>> = BTreeMap::new();
    for f in filters {
        m.insert(f.name.as_str(), &f.values);
    }
    let json = serde_json::to_string(&m).unwrap_or_default();
    query_escape(&json)
}

/// Go `url.QueryEscape`-compatible escaping: every byte except
/// `A-Za-z0-9-_.~` is percent-encoded, and a space becomes `+`.
fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(host: &str, role: &str) -> DockerswarmSdConfig {
        DockerswarmSdConfig {
            host: host.to_string(),
            role: role.to_string(),
            ..DockerswarmSdConfig::default()
        }
    }

    #[test]
    fn unix_host_parses_to_socket_path() {
        let api = new_dockerswarm_api(&cfg("unix:///var/run/docker.sock", "nodes")).unwrap();
        match api.transport {
            Transport::Unix { socket_path } => assert_eq!(socket_path, "/var/run/docker.sock"),
            _ => panic!("expected unix transport"),
        }
    }

    #[test]
    fn tcp_host_maps_to_http() {
        let api = new_dockerswarm_api(&cfg("tcp://dockerd.local:2375", "tasks")).unwrap();
        match api.transport {
            Transport::Http { base_url, .. } => assert_eq!(base_url, "http://dockerd.local:2375"),
            _ => panic!("expected http transport"),
        }
    }

    #[test]
    fn empty_host_is_rejected() {
        assert!(new_dockerswarm_api(&cfg("", "nodes")).is_err());
    }

    #[test]
    fn invalid_host_scheme_is_rejected() {
        assert!(new_dockerswarm_api(&cfg("ftp://nope", "nodes")).is_err());
    }

    #[test]
    fn invalid_role_is_rejected() {
        assert!(new_dockerswarm_api(&cfg("tcp://d:2375", "bogus")).is_err());
    }

    /// Port of upstream `api_test.go::TestGetFiltersQueryArg`.
    #[test]
    fn filters_query_arg_matches_upstream() {
        assert_eq!(get_filters_query_arg(&[]), "");
        let filters = vec![
            DockerswarmFilter {
                name: "name".into(),
                values: vec!["foo".into(), "bar".into()],
            },
            DockerswarmFilter {
                name: "xxx".into(),
                values: vec!["aa".into()],
            },
        ];
        assert_eq!(
            get_filters_query_arg(&filters),
            "%7B%22name%22%3A%5B%22foo%22%2C%22bar%22%5D%2C%22xxx%22%3A%5B%22aa%22%5D%7D"
        );
    }

    #[test]
    fn bearer_token_redacted_in_debug() {
        let mut c = cfg("http://dockerd:2375", "nodes");
        c.auth = AuthConfig {
            bearer: Some("super-secret".into()),
            ..AuthConfig::default()
        };
        let api = new_dockerswarm_api(&c).unwrap();
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret"), "{dbg}");
    }
}
