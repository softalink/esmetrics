//! Kubernetes API client: auth resolution (in-cluster vs. explicit
//! `api_server`), the shared `reqwest::blocking` HTTP client, and the
//! LIST/WATCH URL builders consumed by the watcher (a later task).
//!
//! Port of `lib/promscrape/discoveryutils/kubernetes/api.go`'s
//! `newAPIConfig` (auth resolution) and `api_watcher.go`'s per-role
//! path/query construction (upstream v1.146.0).

use std::sync::Arc;
use std::time::Duration;

use reqwest::blocking::{Client as HttpClient, Response};

use crate::client::TlsConfig;
use crate::scrape::config::{K8sSelector, KubernetesSdConfig, ScrapeError};
use crate::scrape::kubernetes::kubeconfig;
use crate::scrape::kubernetes::oauth2::{self, OAuth2TokenSource};

/// Page size (`limit=`) sent on LIST requests. Without a `limit`, the
/// apiserver returns the whole collection in one response and never emits a
/// `metadata.continue` token, so the watcher's pagination code would never
/// activate. Matches upstream's chunked LIST default.
const LIST_PAGE_LIMIT: u32 = 1000;

/// Resolved Kubernetes API access: the normalized base URL, an HTTP client
/// with TLS already applied, and the auth to attach per request. Built by
/// [`resolve_api_config`]; requests go through [`ApiConfig::get`] or the
/// [`ApiConfig::list_url`]/[`ApiConfig::watch_url`] builders.
///
/// `Debug` is hand-written (see the `impl` below) rather than derived: it
/// must never print `bearer_token`/`basic`'s secret contents, even though
/// nothing in this crate logs an `ApiConfig` today — defense-in-depth
/// against a future `{:?}` in a log line or panic message.
pub struct ApiConfig {
    pub api_server: String,
    http: HttpClient,
    /// Path to a token file that is re-read on every [`ApiConfig::get`]
    /// call (projected service-account tokens rotate) — set for the
    /// in-cluster case, and also settable via a kubeconfig entry that
    /// specifies both `token` and `tokenFile`. Not mutually exclusive with
    /// `bearer_token`/`basic`: [`ApiConfig::get`] resolves auth
    /// deterministically by precedence — `bearer_token_file` first when
    /// present, then `bearer_token`, then `basic`.
    bearer_token_file: Option<String>,
    bearer_token: Option<String>,
    basic: Option<(String, String)>,
    /// OAuth2 client-credentials token source (shared across watcher threads
    /// via the enclosing `Arc<ApiConfig>`, so one token cache is reused). When
    /// set, [`ApiConfig::get`] uses it and skips the static bearer/basic
    /// chain. Composes with any auth mode but in practice pairs with an
    /// explicit `api_server`.
    oauth2: Option<Arc<OAuth2TokenSource>>,
}

/// Redacts every secret field: `bearer_token` shows only whether it is set,
/// `basic` shows only the username (never the password), and
/// `bearer_token_file` shows the path verbatim — a filesystem path isn't a
/// secret, and printing it is useful for diagnosing auth misconfiguration.
/// `api_server` is never sensitive and is printed as-is.
impl std::fmt::Debug for ApiConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiConfig")
            .field("api_server", &self.api_server)
            .field("bearer_token_file", &self.bearer_token_file)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("basic", &self.basic.as_ref().map(|(user, _pass)| user))
            .field("oauth2", &self.oauth2.as_ref().map(|_| "<set>"))
            .finish()
    }
}

/// Overridable filesystem paths and env var names for in-cluster
/// service-account credentials. [`Default`] resolves to the real in-cluster
/// locations; tests point this at temp files and custom env var names so
/// they don't collide with a real k8s environment (or trip when the test
/// runner itself happens to run inside a pod).
pub struct InClusterPaths {
    pub host_env: String,
    pub port_env: String,
    pub ca_file: String,
    pub token_file: String,
    /// Projected service-account namespace file, read best-effort by
    /// `KubernetesDiscovery` when `namespaces.own_namespace` is set (not used
    /// by [`resolve_api_config`] itself). Overridable for the same reason as
    /// `ca_file`/`token_file`: so a unit test can point it at a temp file
    /// instead of the real in-cluster path.
    pub namespace_file: String,
}

impl Default for InClusterPaths {
    fn default() -> Self {
        InClusterPaths {
            host_env: "KUBERNETES_SERVICE_HOST".to_string(),
            port_env: "KUBERNETES_SERVICE_PORT".to_string(),
            ca_file: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt".to_string(),
            token_file: "/var/run/secrets/kubernetes.io/serviceaccount/token".to_string(),
            namespace_file: "/var/run/secrets/kubernetes.io/serviceaccount/namespace".to_string(),
        }
    }
}

/// Resolves `cfg`'s auth into an [`ApiConfig`], mirroring upstream
/// `newAPIConfig`:
/// - `api_server` and `kubeconfig_file` set together is rejected —
///   config-level validation already catches this too (see
///   `scrape::config::validate`), but this is defensive since
///   `resolve_api_config` can be called directly.
/// - `kubeconfig_file` set (alone) -> the file is parsed
///   ([`kubeconfig::load_kube_config`]): its `current-context` cluster
///   supplies the server + TLS (CA/client identity, possibly inline base64)
///   and the user supplies token/token-file/basic auth; a cluster `proxy-url`
///   is applied to the HTTP client.
/// - explicit non-empty `api_server` -> `cfg.auth`/`cfg.tls` are used
///   directly; the bearer/basic values are inline (no token file).
/// - empty `api_server` -> in-cluster: `paths.host_env`/`paths.port_env`
///   build `https://<host>:<port>`, `paths.ca_file` becomes the client's
///   trusted CA, and `paths.token_file` is re-read on every request.
///
/// On both non-kubeconfig paths, `cfg.proxy_url` (when set) is applied to the
/// API-server HTTP client (an invalid URL surfaces here via
/// `reqwest::Proxy::all`). The kubeconfig path ignores `cfg.proxy_url` — its
/// cluster `proxy-url` takes precedence.
pub fn resolve_api_config(
    cfg: &KubernetesSdConfig,
    paths: &InClusterPaths,
) -> Result<ApiConfig, ScrapeError> {
    if cfg.api_server.is_some() && cfg.kubeconfig_file.is_some() {
        return Err(ScrapeError {
            msg: "`api_server` and `kubeconfig_file` cannot be set simultaneously".to_string(),
        });
    }
    // Build the OAuth2 token source once (if configured) so it's shared across
    // every watcher thread via the enclosing `Arc<ApiConfig>`. It composes with
    // whichever auth mode resolves below.
    let oauth2 = match &cfg.oauth2 {
        Some(o) => Some(Arc::new(oauth2::new_token_source(o)?)),
        None => None,
    };

    if let Some(path) = cfg.kubeconfig_file.as_deref() {
        return resolve_from_kubeconfig(path, oauth2);
    }

    match cfg.api_server.as_deref() {
        Some(api_server) if !api_server.is_empty() => {
            let http = build_client(&cfg.tls, cfg.proxy_url.as_deref())?;
            Ok(ApiConfig {
                api_server: normalize_api_server(api_server, &cfg.tls),
                http,
                bearer_token_file: None,
                bearer_token: cfg.auth.bearer.clone(),
                basic: cfg.auth.basic.clone(),
                oauth2,
            })
        }
        _ => resolve_in_cluster(paths, cfg.proxy_url.as_deref(), oauth2),
    }
}

/// Resolves the in-cluster case: env-var host/port, cluster CA, projected
/// token file. Named env vars kept in the error message so a misconfigured
/// deployment (or a job that should have set `api_server` explicitly) gets
/// upstream's exact guidance.
fn resolve_in_cluster(
    paths: &InClusterPaths,
    proxy_url: Option<&str>,
    oauth2: Option<Arc<OAuth2TokenSource>>,
) -> Result<ApiConfig, ScrapeError> {
    let host = std::env::var(&paths.host_env).map_err(|_| ScrapeError {
        msg: format!(
            "cannot find {} env var; it must be defined when running in k8s; \
             probably `kubernetes_sd_config->api_server` is missing?",
            paths.host_env
        ),
    })?;
    let port = std::env::var(&paths.port_env).map_err(|_| ScrapeError {
        msg: format!(
            "cannot find {} env var; it must be defined when running in k8s; \
             probably `kubernetes_sd_config->api_server` is missing?",
            paths.port_env
        ),
    })?;

    let tls = TlsConfig {
        ca_file: Some(paths.ca_file.clone()),
        ..TlsConfig::default()
    };
    let http = build_client(&tls, proxy_url)?;

    Ok(ApiConfig {
        api_server: format!("https://{host}:{port}"),
        http,
        bearer_token_file: Some(paths.token_file.clone()),
        bearer_token: None,
        basic: None,
        oauth2,
    })
}

/// Resolves the `kubeconfig_file` case: parse the file, then build an
/// [`ApiConfig`] from the resolved cluster + user. TLS material (CA / client
/// identity) is in-memory PEM bytes (kubeconfig `*-data` fields are
/// base64-inline), so [`build_client_from_materials`] is used instead of the
/// file-path [`build_client`]. A cluster `proxy-url` becomes the client's
/// proxy.
///
/// Both `token` and `token_file` are passed through when present —
/// [`ApiConfig::get`] already prefers `bearer_token_file`, matching upstream's
/// precedence. `tls_server_name` from the kubeconfig is parsed but not applied
/// (same `reqwest::blocking` SNI limitation as [`build_client`]).
fn resolve_from_kubeconfig(
    path: &str,
    oauth2: Option<Arc<OAuth2TokenSource>>,
) -> Result<ApiConfig, ScrapeError> {
    let kc = kubeconfig::load_kube_config(path)?;
    let http = build_client_from_materials(
        kc.ca_pem.as_deref(),
        kc.identity_pem.as_deref(),
        kc.insecure_skip_verify,
        kc.proxy_url.as_deref(),
    )?;
    // The kubeconfig server carries its own scheme; `tls_hint` only steers
    // `normalize_api_server`'s http-vs-https choice for the (unusual)
    // scheme-less case, and reflects whether the kubeconfig configured TLS.
    let tls_hint = TlsConfig {
        insecure_skip_verify: kc.ca_pem.is_some()
            || kc.identity_pem.is_some()
            || kc.insecure_skip_verify,
        ..TlsConfig::default()
    };
    Ok(ApiConfig {
        api_server: normalize_api_server(&kc.server, &tls_hint),
        http,
        bearer_token_file: kc.token_file,
        bearer_token: kc.token,
        basic: kc.basic,
        oauth2,
    })
}

/// Adds a scheme (`https` if `tls` configures anything, else `http`) when
/// `api_server` doesn't already have one, and strips a trailing `/`.
fn normalize_api_server(api_server: &str, tls: &TlsConfig) -> String {
    let with_scheme = if api_server.contains("://") {
        api_server.to_string()
    } else {
        let scheme = if *tls != TlsConfig::default() {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://{api_server}")
    };
    with_scheme.trim_end_matches('/').to_string()
}

/// Builds a `reqwest::blocking::Client` from `tls`, mirroring
/// `crate::client::build_client`'s shape (duplicated rather than shared —
/// that function is private to its module and bundles in a client-wide
/// `send_timeout` that doesn't apply here; [`ApiConfig::get`] applies a
/// per-call timeout instead).
pub(super) fn build_client(
    tls: &TlsConfig,
    proxy_url: Option<&str>,
) -> Result<HttpClient, ScrapeError> {
    let ca_pem = match &tls.ca_file {
        Some(ca_file) => Some(std::fs::read(ca_file).map_err(|e| ScrapeError {
            msg: format!("cannot read CA file {ca_file:?}: {e}"),
        })?),
        None => None,
    };
    let identity_pem = match (&tls.cert_file, &tls.key_file) {
        (Some(cert_file), Some(key_file)) => {
            let mut identity_pem = std::fs::read(cert_file).map_err(|e| ScrapeError {
                msg: format!("cannot read cert file {cert_file:?}: {e}"),
            })?;
            let mut key_pem = std::fs::read(key_file).map_err(|e| ScrapeError {
                msg: format!("cannot read key file {key_file:?}: {e}"),
            })?;
            identity_pem.push(b'\n');
            identity_pem.append(&mut key_pem);
            Some(identity_pem)
        }
        _ => None,
    };
    // `tls.server_name` has no direct equivalent in reqwest's blocking
    // `ClientBuilder`; not wired here — same documented gap as
    // `crate::client::build_client`. `proxy_url` is the standalone
    // `kubernetes_sd_config.proxy_url` field, applied on the explicit-
    // `api_server` and in-cluster auth paths (the kubeconfig path threads its
    // own cluster `proxy-url` through `build_client_from_materials` directly).
    build_client_from_materials(
        ca_pem.as_deref(),
        identity_pem.as_deref(),
        tls.insecure_skip_verify,
        proxy_url,
    )
}

/// Builds a `reqwest::blocking::Client` from in-memory materials: `ca_pem`
/// (an extra trusted root), `identity_pem` (a combined client cert+key for
/// mTLS), `insecure` (skip server-cert verification), and an optional
/// all-schemes HTTP `proxy_url`. Used by the kubeconfig path (whose CA /
/// client identity arrive as base64-inline PEM bytes) and, via
/// [`build_client`], by the file-path path (which reads the files first).
fn build_client_from_materials(
    ca_pem: Option<&[u8]>,
    identity_pem: Option<&[u8]>,
    insecure: bool,
    proxy_url: Option<&str>,
) -> Result<HttpClient, ScrapeError> {
    let mut builder = HttpClient::builder();
    if insecure {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(pem) = ca_pem {
        let cert = reqwest::Certificate::from_pem(pem).map_err(|e| ScrapeError {
            msg: format!("invalid CA certificate: {e}"),
        })?;
        builder = builder.add_root_certificate(cert);
    }
    if let Some(pem) = identity_pem {
        let identity = reqwest::Identity::from_pem(pem).map_err(|e| ScrapeError {
            msg: format!("invalid client cert/key: {e}"),
        })?;
        builder = builder.identity(identity);
    }
    if let Some(p) = proxy_url {
        let proxy = reqwest::Proxy::all(p).map_err(|e| ScrapeError {
            msg: format!("invalid proxy_url {p:?}: {e}"),
        })?;
        builder = builder.proxy(proxy);
    }
    builder.build().map_err(|e| ScrapeError {
        msg: format!("cannot build http client: {e}"),
    })
}

impl ApiConfig {
    /// Builds a LIST url: `resourceVersion=0&resourceVersionMatch=NotOlderThan`
    /// plus `limit=<LIST_PAGE_LIMIT>` (so the apiserver chunks the response
    /// and returns a `metadata.continue` token, activating the watcher's
    /// pagination), plus the label/field selectors for `role`, plus
    /// `continue=<cont>` when paginating. Mirrors upstream `api_watcher.go`'s
    /// list request.
    pub fn list_url(
        &self,
        role: &str,
        namespace: Option<&str>,
        selectors: &[K8sSelector],
        cont: Option<&str>,
    ) -> String {
        let path = resource_path(role, namespace);
        let mut pairs = vec![
            ("resourceVersion".to_string(), "0".to_string()),
            (
                "resourceVersionMatch".to_string(),
                "NotOlderThan".to_string(),
            ),
            ("limit".to_string(), LIST_PAGE_LIMIT.to_string()),
        ];
        append_selectors(&mut pairs, role, selectors);
        if let Some(c) = cont {
            pairs.push(("continue".to_string(), c.to_string()));
        }
        format!("{}{path}?{}", self.api_server, build_query(&pairs))
    }

    /// Builds a WATCH url: `watch=1&allowWatchBookmarks=true&timeoutSeconds=<n>&resourceVersion=<rv>`
    /// plus the same selectors as [`ApiConfig::list_url`]. Mirrors upstream
    /// `api_watcher.go`'s watch request.
    pub fn watch_url(
        &self,
        role: &str,
        namespace: Option<&str>,
        selectors: &[K8sSelector],
        resource_version: &str,
        timeout_secs: u64,
    ) -> String {
        let path = resource_path(role, namespace);
        let mut pairs = vec![
            ("watch".to_string(), "1".to_string()),
            ("allowWatchBookmarks".to_string(), "true".to_string()),
            ("timeoutSeconds".to_string(), timeout_secs.to_string()),
            ("resourceVersion".to_string(), resource_version.to_string()),
        ];
        append_selectors(&mut pairs, role, selectors);
        format!("{}{path}?{}", self.api_server, build_query(&pairs))
    }

    /// Issues a GET against `url` with the resolved auth applied, re-reading
    /// `bearer_token_file` on every call so a rotated projected
    /// service-account token is picked up (upstream re-reads it too). An
    /// unreadable token file is surfaced as an error that names only the
    /// file path — never the token contents. Always sends
    /// `Accept: application/json` and applies `timeout` to this request only
    /// (the shared client itself carries no default timeout).
    pub fn get(&self, url: &str, timeout: Duration) -> Result<Response, ScrapeError> {
        let mut req = self
            .http
            .get(url)
            .timeout(timeout)
            .header("Accept", "application/json");

        if let Some(oauth2) = &self.oauth2 {
            // OAuth2 takes precedence over the static auth chain (a config
            // won't sanely combine oauth2 with a static bearer). The token is
            // never logged.
            req = req.bearer_auth(oauth2.token()?);
        } else if let Some(token_file) = &self.bearer_token_file {
            let token = std::fs::read_to_string(token_file).map_err(|e| ScrapeError {
                msg: format!("cannot read bearer token file {token_file:?}: {e}"),
            })?;
            req = req.bearer_auth(token.trim());
        } else if let Some(token) = &self.bearer_token {
            req = req.bearer_auth(token);
        } else if let Some((user, pass)) = &self.basic {
            req = req.basic_auth(user, Some(pass));
        }

        req.send().map_err(|e| ScrapeError {
            msg: format!("request to k8s API failed: {e}"),
        })
    }
}

/// Resource path for `role`, matching upstream `api_watcher.go`. `node` and
/// `namespace` are cluster-scoped, so any `namespace` argument is ignored for
/// them.
fn resource_path(role: &str, namespace: Option<&str>) -> String {
    match role {
        "node" => "/api/v1/nodes".to_string(),
        "namespace" => "/api/v1/namespaces".to_string(),
        "pod" => namespaced_path("/api/v1", "pods", namespace),
        "service" => namespaced_path("/api/v1", "services", namespace),
        "endpoints" => namespaced_path("/api/v1", "endpoints", namespace),
        "ingress" => namespaced_path("/apis/networking.k8s.io/v1", "ingresses", namespace),
        "endpointslice" => {
            namespaced_path("/apis/discovery.k8s.io/v1", "endpointslices", namespace)
        }
        other => namespaced_path("/api/v1", &format!("{other}s"), namespace),
    }
}

fn namespaced_path(api_root: &str, resource: &str, namespace: Option<&str>) -> String {
    match namespace {
        Some(ns) => format!("{api_root}/namespaces/{ns}/{resource}"),
        None => format!("{api_root}/{resource}"),
    }
}

/// Appends `labelSelector`/`fieldSelector` params gathered from `selectors`
/// whose `.role` matches `role`. Multiple selectors for the same role join
/// their label/field expressions with `,` (k8s' native selector syntax for
/// combining terms).
fn append_selectors(pairs: &mut Vec<(String, String)>, role: &str, selectors: &[K8sSelector]) {
    let labels: Vec<&str> = selectors
        .iter()
        .filter(|s| s.role == role)
        .filter_map(|s| s.label.as_deref())
        .filter(|s| !s.is_empty())
        .collect();
    if !labels.is_empty() {
        pairs.push(("labelSelector".to_string(), labels.join(",")));
    }

    let fields: Vec<&str> = selectors
        .iter()
        .filter(|s| s.role == role)
        .filter_map(|s| s.field.as_deref())
        .filter(|s| !s.is_empty())
        .collect();
    if !fields.is_empty() {
        pairs.push(("fieldSelector".to_string(), fields.join(",")));
    }
}

/// Joins `pairs` into a `k=v&k=v` query string, percent-encoding each value
/// with [`percent_encode_query`]. Keys are all fixed literals this module
/// controls, so only values are encoded.
fn build_query(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={}", percent_encode_query(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Minimal RFC 3986 percent-encoder for a query parameter value: ASCII
/// alphanumerics and `-_.~` pass through unencoded; everything else
/// (including `=`, `,`, spaces, and any UTF-8 multi-byte sequence) is
/// escaped byte-by-byte as `%XX`. Hand-rolled instead of adding a new
/// dependency — see the task brief.
fn percent_encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A parseable self-signed CA cert PEM — reqwest only needs it to parse
    /// as a valid certificate, not to chain to anything real.
    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUOX5mzV8aYJYKqPeHt3a8Crdyp3owDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjA3MTIxNzUxMDZaFw0zNjA3MDkx
NzUxMDZaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQC+XHup0ybbgyxqU43fJn3IqZU/4Mlmg2gUkZLypNpFBB4qxLwf
HHCRgzILyoLvffxGIga5Inuo1V8XSnkl/DL/+FQ0llcSbQs4SBtEplcd2e1p2oID
cVr2ddPgtvP+ocMrqeOXiA47t6g2wvoiK4L5DpZ4XZx64zQOVSkOHdNFMDNa1ZdE
+4B4u3oKTJwOKgfOQZ5WPJuokxP3ePM4Z/EB9VShj7cwm2IysyVHoXEOx18qbVVv
r17rtmU8qsC2Ly33xOSgpxCN+Vm6jS2Z8XkCFal5VtGsP1JskMWRWhha9+saJr1Q
X0c+/3nU2GbXyKLR347Wbcr7ZCfYti+FoIBfAgMBAAGjUzBRMB0GA1UdDgQWBBSs
1lvwDmd+7tJeyN6PkD3qnZji3jAfBgNVHSMEGDAWgBSs1lvwDmd+7tJeyN6PkD3q
nZji3jAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQCjoLqMp0yI
b1faNMxakz8qhaGs3PUSg/ZutqTIbf769jhMx4irhwun8vNTV4btP5MiB7wEARWv
wp4XCpSFsWMERhhnenvDqXI1mHxaCxpkO/WGmea7FA2XmmbmMs3BMT8hZzhrgtMy
xN+yJ7lzidRQSx9Gf9P3SomrdxNMQdXtrEIuE+53h12vIfyj/QPoYltMj5wyWt3o
0bYi5MIRJJK+La5YT+39S0QZMjD5c6GYHcFr6pvYHLDYzVyH0uFEk845sdibU5bB
kWeGBk85k+1kh1l2JsV1aNRmX3tmA4JKmUqSJ4JggRnKHSoNRl5TU60PgIGlDE7q
WQQWLIPlW2xC
-----END CERTIFICATE-----
";

    /// Builds a `KubernetesSdConfig` with `role` set and every other field
    /// at its default (no `api_server`, no auth, no tls).
    fn k8s_cfg_role(role: &str) -> KubernetesSdConfig {
        KubernetesSdConfig {
            role: role.to_string(),
            ..KubernetesSdConfig::default()
        }
    }

    /// Resolves an `ApiConfig` for an explicit `api_server` with no auth —
    /// used by URL-builder tests that don't care about auth resolution.
    fn api_config_for_test(api_server: &str) -> ApiConfig {
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = Some(api_server.to_string());
        resolve_api_config(&cfg, &InClusterPaths::default()).unwrap()
    }

    #[test]
    fn in_cluster_resolution_from_overridable_paths() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.crt");
        std::fs::write(&ca, TEST_CA_PEM).unwrap();
        let tok = dir.path().join("token");
        std::fs::write(&tok, "tok-123\n").unwrap();
        std::env::set_var("ESM_TEST_K8S_HOST", "10.0.0.1");
        std::env::set_var("ESM_TEST_K8S_PORT", "6443");
        let paths = InClusterPaths {
            host_env: "ESM_TEST_K8S_HOST".into(),
            port_env: "ESM_TEST_K8S_PORT".into(),
            ca_file: ca.to_string_lossy().into(),
            token_file: tok.to_string_lossy().into(),
            ..InClusterPaths::default()
        };
        let cfg = k8s_cfg_role("pod");
        let ac = resolve_api_config(&cfg, &paths).unwrap();
        assert_eq!(ac.api_server, "https://10.0.0.1:6443");
        assert_eq!(ac.bearer_token_file.as_deref(), Some(tok.to_str().unwrap()));
    }

    #[test]
    fn missing_in_cluster_env_var_names_it_in_the_error() {
        std::env::remove_var("ESM_TEST_MISSING_HOST");
        let paths = InClusterPaths {
            host_env: "ESM_TEST_MISSING_HOST".into(),
            port_env: "ESM_TEST_MISSING_PORT".into(),
            ..InClusterPaths::default()
        };
        let cfg = k8s_cfg_role("pod");
        let err = resolve_api_config(&cfg, &paths).unwrap_err();
        assert!(err.msg.contains("ESM_TEST_MISSING_HOST"), "{}", err.msg);
    }

    #[test]
    fn list_and_watch_urls_include_namespace_and_selectors() {
        let ac = api_config_for_test("https://api:6443"); // explicit api_server, no auth
        let sel = vec![K8sSelector {
            role: "pod".into(),
            label: Some("app=web".into()),
            field: None,
        }];
        let lu = ac.list_url("pod", Some("prod"), &sel, None);
        assert!(lu.starts_with("https://api:6443/api/v1/namespaces/prod/pods?"));
        assert!(lu.contains("resourceVersion=0"));
        assert!(lu.contains("limit=1000"));
        assert!(lu.contains("labelSelector=app%3Dweb"));
        let wu = ac.watch_url("pod", None, &sel, "42", 300);
        assert!(wu.starts_with("https://api:6443/api/v1/pods?"));
        assert!(
            wu.contains("watch=1")
                && wu.contains("resourceVersion=42")
                && wu.contains("timeoutSeconds=300")
        );
    }

    #[test]
    fn node_role_ignores_namespace_in_path() {
        let ac = api_config_for_test("https://api:6443");
        let lu = ac.list_url("node", Some("prod"), &[], None);
        assert!(lu.starts_with("https://api:6443/api/v1/nodes?"));
    }

    #[test]
    fn ingress_role_uses_networking_api_group() {
        let ac = api_config_for_test("https://api:6443");
        let lu = ac.list_url("ingress", Some("prod"), &[], None);
        assert!(
            lu.starts_with("https://api:6443/apis/networking.k8s.io/v1/namespaces/prod/ingresses?")
        );
        let lu_all = ac.list_url("ingress", None, &[], None);
        assert!(lu_all.starts_with("https://api:6443/apis/networking.k8s.io/v1/ingresses?"));
    }

    #[test]
    fn endpoints_endpointslice_namespace_urls() {
        let ac = api_config_for_test("https://api:6443");
        let lu = ac.list_url("endpoints", Some("prod"), &[], None);
        assert!(lu.starts_with("https://api:6443/api/v1/namespaces/prod/endpoints?"));
        let lu2 = ac.list_url("endpointslice", Some("prod"), &[], None);
        assert!(lu2.starts_with(
            "https://api:6443/apis/discovery.k8s.io/v1/namespaces/prod/endpointslices?"
        ));
        let lu3 = ac.list_url("endpointslice", None, &[], None);
        assert!(lu3.starts_with("https://api:6443/apis/discovery.k8s.io/v1/endpointslices?"));
        let lu4 = ac.list_url("namespace", None, &[], None);
        assert!(lu4.starts_with("https://api:6443/api/v1/namespaces?"));
        let wu = ac.watch_url("endpointslice", Some("d"), &[], "7", 60);
        assert!(wu
            .starts_with("https://api:6443/apis/discovery.k8s.io/v1/namespaces/d/endpointslices?"));
        assert!(wu.contains("watch=1"));
    }

    #[test]
    fn list_url_pagination_sets_continue() {
        let ac = api_config_for_test("https://api:6443");
        let lu = ac.list_url("service", None, &[], Some("abc123"));
        assert!(lu.contains("continue=abc123"));
    }

    #[test]
    fn debug_redacts_bearer_and_basic_auth_secrets() {
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = Some("https://api:6443".into());
        cfg.auth.bearer = Some("super-secret-token".into());
        let ac = resolve_api_config(&cfg, &InClusterPaths::default()).unwrap();
        let dbg = format!("{ac:?}");
        assert!(!dbg.contains("super-secret-token"), "{dbg}");
        assert!(dbg.contains("api:6443"), "{dbg}");

        let mut cfg2 = k8s_cfg_role("pod");
        cfg2.api_server = Some("https://api:6443".into());
        cfg2.auth.basic = Some(("alice".into(), "hunter2".into()));
        let ac2 = resolve_api_config(&cfg2, &InClusterPaths::default()).unwrap();
        let dbg2 = format!("{ac2:?}");
        assert!(!dbg2.contains("hunter2"), "{dbg2}");
        assert!(dbg2.contains("alice"), "{dbg2}"); // username is not secret
    }

    #[test]
    fn resolve_api_config_with_oauth2_sets_source_and_redacts_secret() {
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = Some("https://api:6443".into());
        cfg.oauth2 = Some(oauth2::OAuth2Config {
            client_id: "id".into(),
            client_secret: Some("super-secret-xyz".into()),
            token_url: "https://idp/token".into(),
            ..oauth2::OAuth2Config::default()
        });
        let ac = resolve_api_config(&cfg, &InClusterPaths::default()).unwrap();
        assert!(ac.oauth2.is_some());
        let dbg = format!("{ac:?}");
        assert!(!dbg.contains("super-secret-xyz"), "{dbg}");
        assert!(dbg.contains("oauth2"), "{dbg}");
    }

    #[test]
    fn kubeconfig_file_resolves_server_ca_and_token() {
        use base64::Engine as _;
        let ca_data = base64::engine::general_purpose::STANDARD.encode(TEST_CA_PEM.as_bytes());
        // Flow mappings keep each YAML node on one line (block indentation via
        // `\`-continued literals is fragile — the continuation strips leading
        // whitespace).
        let yaml = format!(
            "current-context: ctx\n\
             clusters:\n- {{name: c1, cluster: {{server: 'https://k8s.example:6443/', certificate-authority-data: '{ca_data}'}}}}\n\
             users:\n- {{name: u1, user: {{token: my-token}}}}\n\
             contexts:\n- {{name: ctx, context: {{cluster: c1, user: u1}}}}\n"
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kubeconfig");
        std::fs::write(&path, yaml).unwrap();

        let mut cfg = k8s_cfg_role("pod");
        cfg.kubeconfig_file = Some(path.to_string_lossy().into());
        // resolve_api_config returning Ok proves the reqwest client built
        // successfully from the base64 CA (a bad CA would surface here).
        let ac = resolve_api_config(&cfg, &InClusterPaths::default()).unwrap();
        assert_eq!(ac.api_server, "https://k8s.example:6443");
        assert_eq!(ac.bearer_token.as_deref(), Some("my-token"));
        assert!(ac.bearer_token_file.is_none());
    }

    #[test]
    fn api_server_proxy_url_builds_ok_and_invalid_proxy_errors() {
        // Valid proxy_url on an explicit api_server config: the reqwest client
        // accepts the proxy and resolve_api_config returns Ok.
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = Some("https://api:6443".into());
        cfg.proxy_url = Some("http://proxy:3128".into());
        let ac = resolve_api_config(&cfg, &InClusterPaths::default()).unwrap();
        assert_eq!(ac.api_server, "https://api:6443");

        // An unparseable proxy URL surfaces an error at resolve time.
        let mut bad = k8s_cfg_role("pod");
        bad.api_server = Some("https://api:6443".into());
        bad.proxy_url = Some("http://[::bad".into());
        let err = resolve_api_config(&bad, &InClusterPaths::default()).unwrap_err();
        assert!(err.msg.contains("proxy_url"), "{}", err.msg);
    }

    #[test]
    fn in_cluster_proxy_url_is_applied() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.crt");
        std::fs::write(&ca, TEST_CA_PEM).unwrap();
        let tok = dir.path().join("token");
        std::fs::write(&tok, "tok-123\n").unwrap();
        std::env::set_var("ESM_TEST_K8S_PROXY_HOST", "10.0.0.9");
        std::env::set_var("ESM_TEST_K8S_PROXY_PORT", "6443");
        let paths = InClusterPaths {
            host_env: "ESM_TEST_K8S_PROXY_HOST".into(),
            port_env: "ESM_TEST_K8S_PROXY_PORT".into(),
            ca_file: ca.to_string_lossy().into(),
            token_file: tok.to_string_lossy().into(),
            ..InClusterPaths::default()
        };
        // Valid proxy on the in-cluster path builds fine.
        let mut cfg = k8s_cfg_role("pod");
        cfg.proxy_url = Some("http://proxy:3128".into());
        let ac = resolve_api_config(&cfg, &paths).unwrap();
        assert_eq!(ac.api_server, "https://10.0.0.9:6443");
        // An invalid proxy on the in-cluster path errors.
        let mut bad = k8s_cfg_role("pod");
        bad.proxy_url = Some("http://[::bad".into());
        let err = resolve_api_config(&bad, &paths).unwrap_err();
        assert!(err.msg.contains("proxy_url"), "{}", err.msg);
    }

    #[test]
    fn api_server_and_kubeconfig_file_together_is_rejected() {
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = Some("https://k8s:6443".into());
        cfg.kubeconfig_file = Some("/x".into());
        let err = resolve_api_config(&cfg, &InClusterPaths::default()).unwrap_err();
        assert!(err.msg.contains("api_server"), "{}", err.msg);
        assert!(err.msg.contains("kubeconfig_file"), "{}", err.msg);
    }

    #[test]
    fn api_server_without_scheme_is_normalized_to_https_when_tls_configured() {
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = Some("k8s.internal:6443/".into());
        cfg.tls = TlsConfig {
            insecure_skip_verify: true,
            ..TlsConfig::default()
        };
        let ac = resolve_api_config(&cfg, &InClusterPaths::default()).unwrap();
        assert_eq!(ac.api_server, "https://k8s.internal:6443");
    }

    #[test]
    fn api_server_without_scheme_or_tls_normalizes_to_http() {
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = Some("k8s.internal:6443".into());
        let ac = resolve_api_config(&cfg, &InClusterPaths::default()).unwrap();
        assert_eq!(ac.api_server, "http://k8s.internal:6443");
    }

    #[test]
    fn get_reads_bearer_token_file_fresh_and_rejects_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("token");
        std::fs::write(&tok, "first-token\n").unwrap();
        let mut cfg = k8s_cfg_role("pod");
        cfg.api_server = None;
        std::env::set_var("ESM_TEST_GET_HOST", "127.0.0.1");
        std::env::set_var("ESM_TEST_GET_PORT", "1"); // unroutable port; we never actually connect
        let ca = dir.path().join("ca.crt");
        std::fs::write(&ca, TEST_CA_PEM).unwrap();
        let paths = InClusterPaths {
            host_env: "ESM_TEST_GET_HOST".into(),
            port_env: "ESM_TEST_GET_PORT".into(),
            ca_file: ca.to_string_lossy().into(),
            token_file: tok.to_string_lossy().into(),
            ..InClusterPaths::default()
        };
        let ac = resolve_api_config(&cfg, &paths).unwrap();

        // Remove the token file: get() must surface an error naming only
        // the path, never a token value (there is none to leak here, but
        // this proves the read failure is propagated rather than panicking
        // or silently proceeding unauthenticated).
        std::fs::remove_file(&tok).unwrap();
        let err = ac
            .get("http://127.0.0.1:1/x", Duration::from_millis(50))
            .unwrap_err();
        // The error names the token-file path via `{path:?}` (Debug), which on
        // Windows escapes each `\` as `\\`; compare against that same Debug
        // rendering so the substring check is platform-agnostic (the raw path
        // is not a substring of the backslash-escaped message on Windows).
        let shown = format!("{:?}", tok.to_string_lossy());
        assert!(err.msg.contains(&shown), "{}", err.msg);
        assert!(!err.msg.contains("first-token"));
    }
}
