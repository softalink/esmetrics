//! kubeconfig-file authentication for Kubernetes service discovery.
//!
//! Port of `lib/promscrape/discovery/kubernetes/kubeconfig.go` (upstream
//! v1.146.0): parses a kubeconfig YAML document, resolves its
//! `current-context` to a cluster + user, and flattens the result into a
//! [`KubeConfig`] the API client ([`super::client`]) turns into an
//! `ApiConfig`.
//!
//! ## Deviations from upstream
//!
//! - **Local file only.** Upstream reads the kubeconfig via
//!   `fscore.ReadFileOrHTTP`, which also accepts `http(s)://` URLs.
//!   [`load_kube_config`] reads a local filesystem path only.
//! - **TLS material is read into bytes here, not deferred to the client.**
//!   Upstream sets `CAFile`/`CertFile`/`KeyFile` *paths* on a `promauth.TLSConfig`
//!   and the HTTP client reads them later. This crate's `reqwest::blocking`
//!   client is built from in-memory PEM bytes (kubeconfig `*-data` fields are
//!   base64-inline), so when a `certificate-authority`/`client-certificate`/
//!   `client-key` *path* is given, its bytes are read at load time; when the
//!   corresponding `*-data` field is present it is preferred (upstream sets
//!   both the path and the decoded data — data wins in the client).
//! - **`exec` and `act-as*` impersonation auth are rejected**, mirroring
//!   upstream's `AuthInfo.validate`.
//!
//! Secrets (token, client key, password) are never logged or formatted into
//! error messages.

use base64::Engine as _;
use serde::Deserialize;

use crate::scrape::config::ScrapeError;

const STD_B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// A parsed kubeconfig document. Field names / renames mirror
/// `clientcmd/api/v1/types.go` (the shape upstream's `Config` targets).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub clusters: Vec<ConfigCluster>,
    #[serde(rename = "users")]
    pub auth_infos: Vec<ConfigAuthInfo>,
    pub contexts: Vec<ConfigContext>,
    #[serde(rename = "current-context")]
    pub current_context: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ConfigCluster {
    pub name: String,
    pub cluster: Option<Cluster>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ConfigAuthInfo {
    pub name: String,
    #[serde(rename = "user")]
    pub auth_info: Option<AuthInfo>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ConfigContext {
    pub name: String,
    pub context: Option<Context>,
}

/// How to reach a cluster. `omitempty` string fields upstream map to
/// `#[serde(default)]` empty strings here (absent == empty).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Cluster {
    pub server: String,
    #[serde(rename = "tls-server-name")]
    pub tls_server_name: String,
    #[serde(rename = "insecure-skip-tls-verify")]
    pub insecure_skip_tls_verify: bool,
    #[serde(rename = "certificate-authority")]
    pub certificate_authority: String,
    #[serde(rename = "certificate-authority-data")]
    pub certificate_authority_data: String,
    #[serde(rename = "proxy-url")]
    pub proxy_url: String,
}

/// Identity material for a cluster user. `exec`/`act-as*` are parsed only so
/// their presence can be rejected (see [`AuthInfo::validate`]).
///
/// `Debug` is hand-written (see the `impl` below) rather than derived: this
/// raw parse struct holds `client-key-data`, `token`, and `password` in the
/// clear.
#[derive(Default, Deserialize)]
#[serde(default)]
pub struct AuthInfo {
    #[serde(rename = "client-certificate")]
    pub client_certificate: String,
    #[serde(rename = "client-certificate-data")]
    pub client_certificate_data: String,
    #[serde(rename = "client-key")]
    pub client_key: String,
    #[serde(rename = "client-key-data")]
    pub client_key_data: String,
    /// Presence rejected — exec credential plugins are not supported.
    pub exec: Option<serde_yaml_ng::Value>,
    pub token: String,
    #[serde(rename = "tokenFile")]
    pub token_file: String,
    #[serde(rename = "act-as")]
    pub impersonate: String,
    #[serde(rename = "act-as-uid")]
    pub impersonate_uid: String,
    #[serde(rename = "act-as-groups")]
    pub impersonate_groups: Vec<String>,
    #[serde(rename = "act-as-user-extra")]
    pub impersonate_user_extra: Vec<String>,
    pub username: String,
    pub password: String,
}

/// Redacts `client-key-data`, `token`, and `password` (the client's private
/// key and credentials); everything else, including `client-certificate*`
/// (a public certificate) and `username`, is printed as-is.
impl std::fmt::Debug for AuthInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthInfo")
            .field("client_certificate", &self.client_certificate)
            .field("client_certificate_data", &self.client_certificate_data)
            .field("client_key", &self.client_key)
            .field(
                "client_key_data",
                &if self.client_key_data.is_empty() {
                    "<empty>"
                } else {
                    "<redacted>"
                },
            )
            .field("exec", &self.exec)
            .field(
                "token",
                &if self.token.is_empty() {
                    "<empty>"
                } else {
                    "<redacted>"
                },
            )
            .field("token_file", &self.token_file)
            .field("impersonate", &self.impersonate)
            .field("impersonate_uid", &self.impersonate_uid)
            .field("impersonate_groups", &self.impersonate_groups)
            .field("impersonate_user_extra", &self.impersonate_user_extra)
            .field("username", &self.username)
            .field(
                "password",
                &if self.password.is_empty() {
                    "<empty>"
                } else {
                    "<redacted>"
                },
            )
            .finish()
    }
}

impl AuthInfo {
    /// Rejects the unsupported auth mechanisms, mirroring upstream
    /// `AuthInfo.validate`: exec plugins, every `act-as*` impersonation
    /// field, and a password without a username.
    fn validate(&self) -> Result<(), ScrapeError> {
        if self.exec.is_some() {
            return Err(unsupported_field_error("exec"));
        }
        if !self.impersonate_uid.is_empty() {
            return Err(unsupported_field_error("act-as-uid"));
        }
        if !self.impersonate.is_empty() {
            return Err(unsupported_field_error("act-as"));
        }
        if !self.impersonate_groups.is_empty() {
            return Err(unsupported_field_error("act-as-groups"));
        }
        if !self.impersonate_user_extra.is_empty() {
            return Err(unsupported_field_error("act-as-user-extra"));
        }
        if !self.password.is_empty() && self.username.is_empty() {
            return Err(err("username cannot be empty, if password defined"));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Context {
    pub cluster: String,
    #[serde(rename = "user")]
    pub auth_info: String,
}

/// Flattened, resolved kubeconfig: the concrete materials the API client
/// needs. TLS bytes (`ca_pem`/`identity_pem`) are populated only for an
/// `https://` server, matching upstream's TLS-config gate.
///
/// `Debug` is hand-written (see the `impl` below) rather than derived: it
/// must never print `token`/`basic`'s password/`identity_pem`'s secret
/// contents.
#[derive(Default)]
pub struct KubeConfig {
    pub server: String,
    pub ca_pem: Option<Vec<u8>>,
    pub identity_pem: Option<Vec<u8>>,
    pub token: Option<String>,
    pub token_file: Option<String>,
    pub basic: Option<(String, String)>,
    pub insecure_skip_verify: bool,
    pub tls_server_name: Option<String>,
    pub proxy_url: Option<String>,
}

/// Redacts every secret field, mirroring `ApiConfig`'s `Debug` impl in
/// [`super::client`]: `token` shows only whether it is set, `basic` shows
/// only the username (never the password), and `identity_pem` shows only
/// presence (it embeds the client's private key). `ca_pem` is not secret —
/// a CA certificate is public — but its raw bytes are printed as presence
/// only to keep the output readable. `server`, `token_file`,
/// `tls_server_name`, `proxy_url`, and `insecure_skip_verify` are never
/// sensitive and are printed as-is. This is defense-in-depth against a
/// future `{:?}` in a log line or panic message.
impl std::fmt::Debug for KubeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeConfig")
            .field("server", &self.server)
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "<present>"))
            .field(
                "identity_pem",
                &self.identity_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("token_file", &self.token_file)
            .field("basic", &self.basic.as_ref().map(|(user, _pass)| user))
            .field("insecure_skip_verify", &self.insecure_skip_verify)
            .field("tls_server_name", &self.tls_server_name)
            .field("proxy_url", &self.proxy_url)
            .finish()
    }
}

fn err(msg: impl Into<String>) -> ScrapeError {
    ScrapeError { msg: msg.into() }
}

fn unsupported_field_error(field: &str) -> ScrapeError {
    err(format!(
        "field {field:?} is not supported yet; if you feel it is needed please open a feature request \
         at https://github.com/VictoriaMetrics/VictoriaMetrics/issues/new"
    ))
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Reads and parses `path` as a kubeconfig, then resolves its
/// `current-context` into a [`KubeConfig`]. Errors mirror upstream
/// `newKubeConfig`'s wording (`cannot read`/`cannot parse`/`cannot build
/// kubeConfig from`).
///
/// Local filesystem paths only — see the module doc's deviation note.
pub fn load_kube_config(path: &str) -> Result<KubeConfig, ScrapeError> {
    let data = std::fs::read(path).map_err(|e| err(format!("cannot read {path:?}: {e}")))?;
    let cfg: Config =
        serde_yaml_ng::from_slice(&data).map_err(|e| err(format!("cannot parse {path:?}: {e}")))?;
    cfg.build_kube_config()
        .map_err(|e| err(format!("cannot build kubeConfig from {path:?}: {}", e.msg)))
}

impl Config {
    /// Resolves `current-context` → cluster + user and flattens into a
    /// [`KubeConfig`]. Faithful port of upstream `Config.buildKubeConfig`.
    fn build_kube_config(&self) -> Result<KubeConfig, ScrapeError> {
        let context_name = &self.current_context;
        let context = self
            .contexts
            .iter()
            .find(|c| c.name == *context_name)
            .and_then(|c| c.context.as_ref())
            .ok_or_else(|| err(format!("missing context {context_name:?}")))?;

        let cluster_name = &context.cluster;
        let cluster = self
            .clusters
            .iter()
            .find(|c| c.name == *cluster_name)
            .and_then(|c| c.cluster.as_ref())
            .ok_or_else(|| {
                err(format!(
                    "missing cluster config {cluster_name:?} at context {context_name:?}"
                ))
            })?;

        let server = &cluster.server;
        if server.is_empty() {
            return Err(err(format!(
                "missing kubernetes server address for config {cluster_name:?} at context {context_name:?}"
            )));
        }

        let auth_name = &context.auth_info;
        let auth = self
            .auth_infos
            .iter()
            .find(|a| a.name == *auth_name)
            .and_then(|a| a.auth_info.as_ref());
        if !auth_name.is_empty() && auth.is_none() {
            return Err(err(format!("missing auth config {auth_name:?}")));
        }

        let mut kc = KubeConfig {
            server: server.clone(),
            proxy_url: non_empty(&cluster.proxy_url),
            ..KubeConfig::default()
        };

        if let Some(au) = auth {
            au.validate()
                .map_err(|e| err(format!("invalid auth config {auth_name:?}: {}", e.msg)))?;

            // Upstream gates the entire TLS block on an `https://` server.
            if server.starts_with("https://") {
                kc.insecure_skip_verify = cluster.insecure_skip_tls_verify;
                kc.tls_server_name = non_empty(&cluster.tls_server_name);
                kc.ca_pem = resolve_ca(cluster, cluster_name, context_name)?;
                kc.identity_pem = resolve_identity(au, auth_name)?;
            }

            if !au.username.is_empty() || !au.password.is_empty() {
                kc.basic = Some((au.username.clone(), au.password.clone()));
            }
            kc.token = non_empty(&au.token);
            kc.token_file = non_empty(&au.token_file);
        }

        Ok(kc)
    }
}

/// CA bytes for an `https://` cluster: base64 `certificate-authority-data`
/// wins when present (mirrors upstream, where the decoded data overrides the
/// file in the client); otherwise the `certificate-authority` file is read.
fn resolve_ca(
    cluster: &Cluster,
    cluster_name: &str,
    context_name: &str,
) -> Result<Option<Vec<u8>>, ScrapeError> {
    if !cluster.certificate_authority_data.is_empty() {
        let ca = STD_B64
            .decode(cluster.certificate_authority_data.as_bytes())
            .map_err(|e| {
                err(format!(
                    "cannot base64-decode certificate-authority-data from config {cluster_name:?} at context {context_name:?}: {e}"
                ))
            })?;
        return Ok(Some(ca));
    }
    if !cluster.certificate_authority.is_empty() {
        let path = &cluster.certificate_authority;
        let ca = std::fs::read(path).map_err(|e| {
            err(format!(
                "cannot read certificate-authority file {path:?}: {e}"
            ))
        })?;
        return Ok(Some(ca));
    }
    Ok(None)
}

/// Combined client cert + key PEM for an `https://` cluster, assembled ONLY
/// when both a cert source and a key source are present (matching upstream,
/// which needs both for a client identity). Each source is base64 `*-data`
/// when present, else the corresponding file path read from disk.
fn resolve_identity(au: &AuthInfo, auth_name: &str) -> Result<Option<Vec<u8>>, ScrapeError> {
    let cert = resolve_material(
        &au.client_certificate_data,
        &au.client_certificate,
        "client-certificate-data",
        "client-certificate",
        auth_name,
    )?;
    let key = resolve_material(
        &au.client_key_data,
        &au.client_key,
        "client-key-data",
        "client-key",
        auth_name,
    )?;
    match (cert, key) {
        (Some(mut c), Some(mut k)) => {
            c.push(b'\n');
            c.append(&mut k);
            Ok(Some(c))
        }
        _ => Ok(None),
    }
}

/// One PEM material: prefer base64 `data`, else read the file at `path`.
/// `data_field` names the base64 field for the decode-error message;
/// `path_field` names the plain (non-`-data`) field for the file-read error
/// message — they differ (e.g. `client-certificate-data` vs.
/// `client-certificate`), so a file-read failure must not be blamed on the
/// `-data` field.
fn resolve_material(
    data: &str,
    path: &str,
    data_field: &str,
    path_field: &str,
    auth_name: &str,
) -> Result<Option<Vec<u8>>, ScrapeError> {
    if !data.is_empty() {
        let bytes = STD_B64.decode(data.as_bytes()).map_err(|e| {
            err(format!(
                "cannot base64-decode {data_field} from {auth_name:?}: {e}"
            ))
        })?;
        return Ok(Some(bytes));
    }
    if !path.is_empty() {
        let bytes = std::fs::read(path)
            .map_err(|e| err(format!("cannot read {path_field} file {path:?}: {e}")))?;
        return Ok(Some(bytes));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A parseable self-signed CA cert PEM (same fixture as `client.rs`'s
    /// tests). Only needs to parse as a certificate, not chain to anything.
    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    fn b64(s: &str) -> String {
        STD_B64.encode(s.as_bytes())
    }

    /// Builds a kubeconfig YAML from a cluster flow-mapping body and a user
    /// flow-mapping body (both given without their surrounding braces). Flow
    /// mappings keep each node on one line, sidestepping the block-indentation
    /// pitfalls of `\`-continued string literals.
    fn kc_yaml(cluster_body: &str, user_body: &str) -> String {
        format!(
            "current-context: ctx\n\
             clusters:\n- {{name: c1, cluster: {{{cluster_body}}}}}\n\
             users:\n- {{name: u1, user: {{{user_body}}}}}\n\
             contexts:\n- {{name: ctx, context: {{cluster: c1, user: u1}}}}\n"
        )
    }

    /// (a) Full valid kubeconfig: https server, CA-from-data, a user with
    /// client-certificate-data + client-key-data -> server, ca_pem and
    /// identity_pem all resolved.
    #[test]
    fn resolves_https_server_ca_and_client_identity() {
        let cert = "-----BEGIN CERTIFICATE-----\nCERT\n-----END CERTIFICATE-----\n";
        let key = "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n";
        let yaml = kc_yaml(
            &format!(
                "server: 'https://k8s.example:6443', certificate-authority-data: '{}'",
                b64(TEST_CA_PEM)
            ),
            &format!(
                "client-certificate-data: '{}', client-key-data: '{}'",
                b64(cert),
                b64(key)
            ),
        );
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let kc = cfg.build_kube_config().unwrap();
        assert_eq!(kc.server, "https://k8s.example:6443");
        assert_eq!(kc.ca_pem.as_deref(), Some(TEST_CA_PEM.as_bytes()));
        let expected_identity = format!("{cert}\n{key}");
        assert_eq!(
            kc.identity_pem.as_deref(),
            Some(expected_identity.as_bytes())
        );
        assert!(kc.token.is_none() && kc.basic.is_none());
    }

    /// (b) A token user resolves the bearer token.
    #[test]
    fn resolves_token_user() {
        let yaml = kc_yaml("server: 'https://k8s:6443'", "token: sekret-token");
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let kc = cfg.build_kube_config().unwrap();
        assert_eq!(kc.token.as_deref(), Some("sekret-token"));
        assert!(kc.basic.is_none());
    }

    /// (c) username + password resolves basic auth.
    #[test]
    fn resolves_basic_auth() {
        let yaml = kc_yaml(
            "server: 'https://k8s:6443'",
            "username: alice, password: hunter2",
        );
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let kc = cfg.build_kube_config().unwrap();
        assert_eq!(kc.basic, Some(("alice".to_string(), "hunter2".to_string())));
    }

    /// (d) missing current-context -> error.
    #[test]
    fn missing_current_context_errors() {
        let yaml = "current-context: nope\nclusters:\n- {name: c1, cluster: {server: 'https://k8s:6443'}}\ncontexts:\n- {name: ctx, context: {cluster: c1}}\n";
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let e = cfg.build_kube_config().unwrap_err();
        assert!(e.msg.contains("missing context"), "{}", e.msg);
        assert!(e.msg.contains("nope"), "{}", e.msg);
    }

    /// (e) exec user -> "exec" unsupported error.
    #[test]
    fn exec_user_is_rejected() {
        let yaml = kc_yaml("server: 'https://k8s:6443'", "exec: {command: get-token}");
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let e = cfg.build_kube_config().unwrap_err();
        assert!(e.msg.contains("\"exec\""), "{}", e.msg);
        assert!(e.msg.contains("not supported yet"), "{}", e.msg);
    }

    /// act-as impersonation is rejected too.
    #[test]
    fn impersonation_is_rejected() {
        let yaml = kc_yaml("server: 'https://k8s:6443'", "act-as: someoneelse");
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let e = cfg.build_kube_config().unwrap_err();
        assert!(e.msg.contains("\"act-as\""), "{}", e.msg);
    }

    /// (f) password without username -> error.
    #[test]
    fn password_without_username_errors() {
        let yaml = kc_yaml("server: 'https://k8s:6443'", "password: hunter2");
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let e = cfg.build_kube_config().unwrap_err();
        assert!(e.msg.contains("username cannot be empty"), "{}", e.msg);
        // The password value must never leak into the error.
        assert!(!e.msg.contains("hunter2"), "{}", e.msg);
    }

    /// (g) an http:// server skips TLS material assembly.
    #[test]
    fn http_server_skips_tls_material() {
        let yaml = kc_yaml(
            &format!(
                "server: 'http://k8s:8080', certificate-authority-data: '{}'",
                b64(TEST_CA_PEM)
            ),
            "token: t",
        );
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let kc = cfg.build_kube_config().unwrap();
        assert_eq!(kc.server, "http://k8s:8080");
        assert!(kc.ca_pem.is_none(), "CA must not be assembled for http");
        assert!(kc.identity_pem.is_none());
        assert!(!kc.insecure_skip_verify);
        assert_eq!(kc.token.as_deref(), Some("t"));
    }

    /// (h) invalid base64 CA data -> decode error.
    #[test]
    fn invalid_base64_ca_data_errors() {
        let yaml = kc_yaml(
            "server: 'https://k8s:6443', certificate-authority-data: '!!!not-base64!!!'",
            "token: t",
        );
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let e = cfg.build_kube_config().unwrap_err();
        assert!(
            e.msg
                .contains("cannot base64-decode certificate-authority-data"),
            "{}",
            e.msg
        );
    }

    /// A missing cluster reference produces upstream's wording.
    #[test]
    fn missing_cluster_config_errors() {
        let yaml = "current-context: ctx\nusers:\n- {name: u1, user: {token: t}}\ncontexts:\n- {name: ctx, context: {cluster: nope, user: u1}}\n";
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let e = cfg.build_kube_config().unwrap_err();
        assert!(e.msg.contains("missing cluster config"), "{}", e.msg);
        assert!(e.msg.contains("nope"), "{}", e.msg);
    }

    /// An empty server address is rejected.
    #[test]
    fn empty_server_errors() {
        let yaml = kc_yaml("server: ''", "token: t");
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let e = cfg.build_kube_config().unwrap_err();
        assert!(
            e.msg.contains("missing kubernetes server address"),
            "{}",
            e.msg
        );
    }

    /// proxy-url on the cluster is carried through.
    #[test]
    fn proxy_url_is_carried_through() {
        let yaml = kc_yaml(
            "server: 'https://k8s:6443', proxy-url: 'http://proxy:3128'",
            "token: t",
        );
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        let kc = cfg.build_kube_config().unwrap();
        assert_eq!(kc.proxy_url.as_deref(), Some("http://proxy:3128"));
    }

    /// load_kube_config reads and parses a file end to end.
    #[test]
    fn load_kube_config_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kubeconfig");
        let yaml = kc_yaml("server: 'https://k8s:6443'", "token: t");
        std::fs::write(&path, yaml).unwrap();
        let kc = load_kube_config(path.to_str().unwrap()).unwrap();
        assert_eq!(kc.server, "https://k8s:6443");
        assert_eq!(kc.token.as_deref(), Some("t"));
    }

    /// `KubeConfig`'s hand-written `Debug` must never print the bearer
    /// token, the basic-auth password, or the client identity's private-key
    /// bytes, while non-secret fields (server, username) still show up.
    #[test]
    fn debug_redacts_kubeconfig_secrets() {
        let kc = KubeConfig {
            server: "https://k8s.example:6443".to_string(),
            identity_pem: Some(
                b"-----BEGIN PRIVATE KEY-----\nsuper-secret-key-bytes\n-----END PRIVATE KEY-----\n"
                    .to_vec(),
            ),
            token: Some("super-secret-token".to_string()),
            basic: Some(("alice".to_string(), "hunter2".to_string())),
            ..KubeConfig::default()
        };
        let dbg = format!("{kc:?}");
        assert!(!dbg.contains("super-secret-token"), "{dbg}");
        assert!(!dbg.contains("hunter2"), "{dbg}");
        assert!(!dbg.contains("super-secret-key-bytes"), "{dbg}");
        assert!(dbg.contains("https://k8s.example:6443"), "{dbg}");
        assert!(dbg.contains("alice"), "{dbg}"); // username is not secret
    }
}
