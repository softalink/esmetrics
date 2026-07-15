//! Docker Engine API client: `host` parsing into a transport (Unix socket vs
//! HTTP), the `filters` query-arg builder, and the `/networks` +
//! `/containers/json` fetches the refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/docker/api.go`'s `newAPIConfig` /
//! `getAPIResponse` / `getFiltersQueryArg` and `container.go`/`network.go`'s
//! `getContainers`/`getNetworks`. Docker uses UNVERSIONED API paths
//! (`/containers/json`, `/networks`) — no `/vX.Y/` prefix — matching upstream.
//!
//! Two transport arms mirror upstream's `discoveryutil.NewClient` special
//! case: a `unix://…` host uses the hand-rolled [`super::transport`] HTTP/1.1
//! client over a Unix socket, while `tcp://host:port` (mapped to
//! `http://host:port`) and `http(s)://…` hosts use `reqwest::blocking` with
//! the config's auth/TLS.

use std::time::Duration;

use reqwest::blocking::Client as HttpClient;

use crate::client::{AuthConfig, TlsConfig};
use crate::scrape::config::{DockerSdConfig, ScrapeError};

use super::labels::{parse_containers, Container};
use super::network::{parse_networks, Network};
use super::transport::unix_socket_get;

/// Per-request client-side timeout. Each refresh issues two sequential GETs
/// (`/networks`, `/containers/json`); each is capped so a hung dockerd can't
/// stall the refresh thread — and thus a [`super::DockerDiscovery`]
/// `Drop`/`stop` — indefinitely. Mirrors the DigitalOcean/Consul rationale.
const DOCKER_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// One `filters` entry (`docker.go`'s `Filter`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DockerFilter {
    pub name: String,
    pub values: Vec<String>,
}

/// The transport used to reach the Docker API: a Unix socket (custom HTTP/1.1
/// client) or an HTTP endpoint (`reqwest::blocking`, with auth/TLS applied).
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

/// Resolved Docker API access: a [`Transport`] plus the escaped `filters`
/// query arg appended to every request. `Debug` is hand-written to redact the
/// bearer token / basic password (defense-in-depth against a future `{:?}`).
pub struct DockerApi {
    transport: Transport,
    filters_query_arg: String,
}

impl std::fmt::Debug for DockerApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("DockerApi");
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

/// Builds a [`DockerApi`] from `cfg`, mirroring `newAPIConfig`. Fails only on
/// genuinely bad config (empty/invalid `host`, bad TLS material) — never
/// because the Docker API is unreachable; fetching happens later on the
/// refresh thread.
pub fn new_docker_api(cfg: &DockerSdConfig) -> Result<DockerApi, ScrapeError> {
    let transport = parse_host(&cfg.host, &cfg.auth, &cfg.tls)?;
    Ok(DockerApi {
        transport,
        filters_query_arg: get_filters_query_arg(&cfg.filters),
    })
}

/// Parses the Docker `host` into a [`Transport`]:
/// - `unix://<path>` -> Unix-socket transport at `<path>`.
/// - `tcp://host:port` -> HTTP transport at `http://host:port`.
/// - `http(s)://…` -> HTTP transport as-is.
/// - anything else (incl. empty) -> error.
fn parse_host(host: &str, auth: &AuthConfig, tls: &TlsConfig) -> Result<Transport, ScrapeError> {
    if let Some(path) = host.strip_prefix("unix://") {
        if path.is_empty() {
            return Err(ScrapeError {
                msg: "docker host unix:// requires a socket path".to_string(),
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
            msg: "docker_sd_config requires a non-empty `host`".to_string(),
        });
    } else {
        return Err(ScrapeError {
            msg: format!(
                "invalid docker host {host:?}: expected a unix://, tcp://, http:// or https:// URL"
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

/// Builds a `reqwest::blocking::Client` applying `tls`. Mirrors
/// `scrape::digitalocean::client`'s builder.
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
        msg: format!("cannot build docker http client: {e}"),
    })
}

impl DockerApi {
    /// Issues a GET against `path` (with the escaped `filters` query arg
    /// appended, like upstream's `getAPIResponse`) and returns the body bytes
    /// on a 2xx status.
    fn get_api_response(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let full_path = self.with_filters(path);
        match &self.transport {
            Transport::Unix { socket_path } => {
                let resp = unix_socket_get(socket_path, &full_path, DOCKER_HTTP_TIMEOUT)?;
                if !(200..300).contains(&resp.status) {
                    return Err(ScrapeError {
                        msg: format!("docker request to {path:?} failed: status {}", resp.status),
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
                    .timeout(DOCKER_HTTP_TIMEOUT)
                    .header("Accept", "application/json");
                if let Some(token) = bearer {
                    req = req.bearer_auth(token);
                } else if let Some((user, pass)) = basic {
                    req = req.basic_auth(user, Some(pass));
                }
                let resp = req.send().map_err(|e| ScrapeError {
                    msg: format!("docker request to {path:?} failed: {e}"),
                })?;
                let status = resp.status();
                let body = resp.bytes().map_err(|e| ScrapeError {
                    msg: format!("docker response from {path:?}: {e}"),
                })?;
                if !status.is_success() {
                    return Err(ScrapeError {
                        msg: format!("docker request to {path:?} failed: status {status}"),
                    });
                }
                Ok(body.to_vec())
            }
        }
    }

    /// Appends the escaped `filters` query arg to `path`, choosing `?` or `&`
    /// as upstream's `getAPIResponse` does.
    fn with_filters(&self, path: &str) -> String {
        if self.filters_query_arg.is_empty() {
            return path.to_string();
        }
        let sep = if path.contains('?') { '&' } else { '?' };
        format!("{path}{sep}filters={}", self.filters_query_arg)
    }

    /// Fetches and parses `GET /networks`. Port of `getNetworks`.
    pub fn get_networks(&self) -> Result<Vec<Network>, ScrapeError> {
        let data = self
            .get_api_response("/networks")
            .map_err(|e| ScrapeError {
                msg: format!("cannot query docker api for networks: {}", e.msg),
            })?;
        parse_networks(&data)
    }

    /// Fetches and parses `GET /containers/json`. Port of `getContainers`.
    pub fn get_containers(&self) -> Result<Vec<Container>, ScrapeError> {
        let data = self
            .get_api_response("/containers/json")
            .map_err(|e| ScrapeError {
                msg: format!("cannot query docker api for containers: {}", e.msg),
            })?;
        parse_containers(&data)
    }
}

/// Builds the escaped `filters` query arg. Port of `getFiltersQueryArg`:
/// `{name: {value: true, ...}, ...}` (keys/values de-duplicated and sorted via
/// `BTreeMap`/`BTreeSet`, matching Go's `map` JSON key ordering), then
/// URL-query-escaped.
fn get_filters_query_arg(filters: &[DockerFilter]) -> String {
    use std::collections::{BTreeMap, BTreeSet};
    if filters.is_empty() {
        return String::new();
    }
    let mut m: BTreeMap<&str, BTreeMap<&str, bool>> = BTreeMap::new();
    for f in filters {
        let entry = m.entry(f.name.as_str()).or_default();
        let mut seen = BTreeSet::new();
        for v in &f.values {
            if seen.insert(v.as_str()) {
                entry.insert(v.as_str(), true);
            }
        }
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

    fn cfg_with_host(host: &str) -> DockerSdConfig {
        DockerSdConfig {
            host: host.to_string(),
            ..DockerSdConfig::default()
        }
    }

    #[test]
    fn unix_host_parses_to_socket_path() {
        let api = new_docker_api(&cfg_with_host("unix:///var/run/docker.sock")).unwrap();
        match api.transport {
            Transport::Unix { socket_path } => assert_eq!(socket_path, "/var/run/docker.sock"),
            _ => panic!("expected unix transport"),
        }
    }

    #[test]
    fn tcp_host_maps_to_http() {
        let api = new_docker_api(&cfg_with_host("tcp://dockerd.local:2375")).unwrap();
        match api.transport {
            Transport::Http { base_url, .. } => assert_eq!(base_url, "http://dockerd.local:2375"),
            _ => panic!("expected http transport"),
        }
    }

    #[test]
    fn http_host_kept_and_trailing_slash_stripped() {
        let api = new_docker_api(&cfg_with_host("https://dockerd.example/")).unwrap();
        match api.transport {
            Transport::Http { base_url, .. } => assert_eq!(base_url, "https://dockerd.example"),
            _ => panic!("expected http transport"),
        }
    }

    #[test]
    fn empty_host_is_rejected() {
        assert!(new_docker_api(&cfg_with_host("")).is_err());
    }

    #[test]
    fn invalid_host_scheme_is_rejected() {
        assert!(new_docker_api(&cfg_with_host("ftp://nope")).is_err());
    }

    /// Port of upstream `api_test.go::TestGetFiltersQueryArg`.
    #[test]
    fn filters_query_arg_matches_upstream() {
        assert_eq!(get_filters_query_arg(&[]), "");
        let filters = vec![
            DockerFilter {
                name: "name".into(),
                values: vec!["foo".into(), "bar".into()],
            },
            DockerFilter {
                name: "xxx".into(),
                values: vec!["aa".into()],
            },
        ];
        assert_eq!(
            get_filters_query_arg(&filters),
            "%7B%22name%22%3A%7B%22bar%22%3Atrue%2C%22foo%22%3Atrue%7D%2C%22xxx%22%3A%7B%22aa%22%3Atrue%7D%7D"
        );
    }

    #[test]
    fn bearer_token_redacted_in_debug() {
        let mut cfg = cfg_with_host("http://dockerd:2375");
        cfg.auth = AuthConfig {
            bearer: Some("super-secret".into()),
            ..AuthConfig::default()
        };
        let api = new_docker_api(&cfg).unwrap();
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret"), "{dbg}");
    }
}
