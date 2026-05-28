//! `esm-auth` — Phase 7 with vmauth-compatible config.
//!
//! Reverse proxy that loads a `vmauth`-style YAML configuration: per-user
//! Basic + Bearer credentials, per-user `url_map` URL rewriting, IP filters.

#![allow(clippy::print_stderr)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::single_match_else)]
#![allow(clippy::collapsible_if)]
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use clap::Parser;
use regex::Regex;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "esm-auth",
    about = "vmauth-compatible reverse proxy (multi-user, URL maps, IP filters).",
    version
)]
struct Cli {
    /// HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:8427")]
    http_listen_addr: SocketAddr,
    /// Path to a vmauth-compatible YAML config.
    #[arg(long)]
    auth_file: Option<PathBuf>,
    /// Legacy single-user mode: bearer token (only used when --auth-file is absent).
    #[arg(long)]
    bearer_token: Option<String>,
    /// Legacy single-user mode: upstream URL.
    #[arg(long)]
    upstream_url: Option<String>,
}

/// vmauth `auth.yml` schema. Subset — covers users with credentials, URL
/// maps, default URL prefix, and IP allow lists.
#[derive(Debug, Deserialize, Default)]
struct AuthConfig {
    #[serde(default)]
    users: Vec<User>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct User {
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    bearer_token: Option<String>,
    #[serde(default)]
    name: Option<String>,
    /// One default upstream URL prefix.
    #[serde(default)]
    url_prefix: Option<String>,
    /// Per-route URL maps.
    #[serde(default)]
    url_map: Vec<UrlMap>,
    /// CIDR/IP allow list. If non-empty, the client's source IP must
    /// belong to this list.
    #[serde(default)]
    ip_filters: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct UrlMap {
    /// Regex prefixes that route to this URL.
    src_paths: Vec<String>,
    /// Upstream URL prefix.
    url_prefix: String,
}

struct CompiledUser {
    user: User,
    compiled_paths: Vec<Vec<Regex>>, // one inner Vec per url_map entry
}

struct AppState {
    users: Vec<CompiledUser>,
    /// Indexes for fast credential lookup.
    bearer_index: BTreeMap<String, usize>,
    basic_index: BTreeMap<String, usize>, // "user:pass" → user idx
    client: reqwest::Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let config = if let Some(path) = &cli.auth_file {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        serde_yaml_ng::from_str(&raw).context("parse auth.yml")?
    } else {
        let upstream = cli
            .upstream_url
            .clone()
            .context("provide either --auth-file or --upstream-url + --bearer-token")?;
        let token = cli
            .bearer_token
            .clone()
            .context("--bearer-token required in legacy single-user mode")?;
        AuthConfig {
            users: vec![User {
                bearer_token: Some(token),
                url_prefix: Some(upstream),
                ..Default::default()
            }],
        }
    };

    let mut compiled = Vec::new();
    let mut bearer_index = BTreeMap::new();
    let mut basic_index = BTreeMap::new();
    for (idx, user) in config.users.iter().enumerate() {
        let mut compiled_paths = Vec::new();
        for um in &user.url_map {
            let mut regs = Vec::with_capacity(um.src_paths.len());
            for p in &um.src_paths {
                let r = Regex::new(p).with_context(|| format!("compile src_paths regex {p:?}"))?;
                regs.push(r);
            }
            compiled_paths.push(regs);
        }
        compiled.push(CompiledUser { user: user.clone(), compiled_paths });
        if let Some(t) = &user.bearer_token {
            bearer_index.insert(t.clone(), idx);
        }
        if let (Some(u), Some(p)) = (&user.username, &user.password) {
            basic_index.insert(format!("{u}:{p}"), idx);
        }
    }

    let state = Arc::new(AppState {
        users: compiled,
        bearer_index,
        basic_index,
        client: reqwest::Client::builder()
            .user_agent("esm-auth/0.0.0")
            .build()
            .context("build HTTP client")?,
    });

    let app = Router::new().fallback(proxy).with_state(state);
    let listener = tokio::net::TcpListener::bind(cli.http_listen_addr).await?;
    tracing::info!(addr = %cli.http_listen_addr, "esm-auth listening");

    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async {
            let _ = esm_platform::signal::wait_for_shutdown().await;
        })
        .await
        .context("axum serve")?;
    Ok(())
}

fn init_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .finish();
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("warning: tracing init failed: {e}");
    }
}

async fn proxy(
    State(state): State<Arc<AppState>>,
    ConnectInfo(client_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: Request,
) -> Result<Response, AppError> {
    let user_idx = match identify_user(&headers, &state) {
        Some(idx) => idx,
        None => {
            let mut resp = (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
            resp.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"esm-auth\""),
            );
            return Ok(resp);
        }
    };
    let user = &state.users[user_idx];

    // IP filter.
    if !user.user.ip_filters.is_empty() && !ip_allowed(&user.user.ip_filters, client_addr.ip()) {
        return Ok((StatusCode::FORBIDDEN, "ip not in allow list").into_response());
    }

    // URL routing.
    let uri: &Uri = req.uri();
    let path_and_query = uri.path_and_query().map_or("/", axum::http::uri::PathAndQuery::as_str);
    let upstream_prefix = pick_upstream(user, path_and_query);
    let Some(upstream_prefix) = upstream_prefix else {
        return Ok((StatusCode::NOT_FOUND, "no matching url_map entry").into_response());
    };
    let target = format!("{}{}", upstream_prefix.trim_end_matches('/'), path_and_query);

    let method = req.method().clone();
    let mut builder = state.client.request(method, &target);
    for (k, v) in &headers {
        if k == axum::http::header::HOST || k == axum::http::header::AUTHORIZATION {
            continue;
        }
        builder = builder.header(k.as_str(), v.as_bytes());
    }
    let body_bytes = axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024)
        .await
        .map_err(|e| AppError(anyhow::anyhow!("read request body: {e}")))?;
    let upstream_resp = builder
        .body(body_bytes.to_vec())
        .send()
        .await
        .map_err(|e| AppError(anyhow::anyhow!("upstream request: {e}")))?;

    let status = upstream_resp.status();
    let upstream_headers = upstream_resp.headers().clone();
    let upstream_body = upstream_resp
        .bytes()
        .await
        .map_err(|e| AppError(anyhow::anyhow!("read upstream body: {e}")))?;
    let mut resp = Response::builder().status(status);
    for (k, v) in &upstream_headers {
        if matches!(k.as_str(), "transfer-encoding" | "connection") {
            continue;
        }
        resp = resp.header(k.as_str(), v.as_bytes());
    }
    resp.body(Body::from(upstream_body)).map_err(|e| AppError(anyhow::anyhow!("build resp: {e}")))
}

fn identify_user(headers: &HeaderMap, state: &AppState) -> Option<usize> {
    let auth = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    if let Some(token) = auth.strip_prefix("Bearer ") {
        return state.bearer_index.get(token).copied();
    }
    if let Some(creds) = auth.strip_prefix("Basic ") {
        // Decode base64 manually to avoid adding a crate.
        let decoded = base64_decode(creds.trim())?;
        return state.basic_index.get(&decoded).copied();
    }
    None
}

fn pick_upstream(user: &CompiledUser, path: &str) -> Option<String> {
    for (i, um) in user.user.url_map.iter().enumerate() {
        if user.compiled_paths[i].iter().any(|r| r.is_match(path)) {
            return Some(um.url_prefix.clone());
        }
    }
    user.user.url_prefix.clone()
}

fn ip_allowed(allowed: &[String], ip: IpAddr) -> bool {
    let ip_str = ip.to_string();
    for entry in allowed {
        if entry == &ip_str {
            return true;
        }
        if let Some((net_str, prefix_str)) = entry.split_once('/')
            && let (Ok(net), Ok(prefix_len)) = (net_str.parse::<IpAddr>(), prefix_str.parse::<u8>())
            && cidr_contains(net, prefix_len, ip)
        {
            return true;
        }
    }
    false
}

/// Returns true if `ip` falls in the CIDR range `network/prefix_len`.
/// Mismatched address families never match. `prefix_len` is clamped to the
/// address-family width.
fn cidr_contains(network: IpAddr, prefix_len: u8, ip: IpAddr) -> bool {
    match (network, ip) {
        (IpAddr::V4(net), IpAddr::V4(target)) => {
            let prefix = prefix_len.min(32);
            if prefix == 0 {
                return true;
            }
            let mask: u32 = if prefix == 32 { u32::MAX } else { (!0u32) << (32 - prefix) };
            (u32::from(net) & mask) == (u32::from(target) & mask)
        }
        (IpAddr::V6(net), IpAddr::V6(target)) => {
            let prefix = prefix_len.min(128);
            if prefix == 0 {
                return true;
            }
            let net_bits = u128::from(net);
            let target_bits = u128::from(target);
            let mask: u128 = if prefix == 128 { u128::MAX } else { (!0u128) << (128 - prefix) };
            (net_bits & mask) == (target_bits & mask)
        }
        _ => false,
    }
}

fn base64_decode(s: &str) -> Option<String> {
    const TABLE: [i8; 256] = build_base64_table();
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut buf: u32 = 0;
    let mut bits = 0;
    for &b in bytes {
        if b == b'=' {
            break;
        }
        let v = TABLE[b as usize];
        if v < 0 {
            return None;
        }
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    String::from_utf8(out).ok()
}

const fn build_base64_table() -> [i8; 256] {
    let mut t = [-1i8; 256];
    let mut i = 0;
    while i < 26 {
        t[(b'A' + i) as usize] = i as i8;
        t[(b'a' + i) as usize] = (i + 26) as i8;
        i += 1;
    }
    let mut j = 0;
    while j < 10 {
        t[(b'0' + j) as usize] = (j + 52) as i8;
        j += 1;
    }
    t[b'+' as usize] = 62;
    t[b'/' as usize] = 63;
    t[b'-' as usize] = 62; // url-safe
    t[b'_' as usize] = 63; // url-safe
    t
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn cidr_v4_basic() {
        assert!(cidr_contains(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)),
            8,
            IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
        ));
        assert!(!cidr_contains(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)),
            8,
            IpAddr::V4(Ipv4Addr::new(11, 0, 0, 1)),
        ));
        // 192.168.1.0/24 contains 192.168.1.42 but not 192.168.2.42.
        assert!(cidr_contains(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0)),
            24,
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42)),
        ));
        assert!(!cidr_contains(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0)),
            24,
            IpAddr::V4(Ipv4Addr::new(192, 168, 2, 42)),
        ));
        // /0 always matches.
        assert!(cidr_contains(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            0,
            IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
        ));
        // /32 matches exactly.
        assert!(cidr_contains(
            IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
            32,
            IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
        ));
        assert!(!cidr_contains(
            IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
            32,
            IpAddr::V4(Ipv4Addr::new(1, 2, 3, 5)),
        ));
    }

    #[test]
    fn cidr_v6_basic() {
        assert!(cidr_contains(
            IpAddr::V6("2001:db8::".parse::<Ipv6Addr>().unwrap()),
            32,
            IpAddr::V6("2001:db8:1::1".parse::<Ipv6Addr>().unwrap()),
        ));
        assert!(!cidr_contains(
            IpAddr::V6("2001:db8::".parse::<Ipv6Addr>().unwrap()),
            32,
            IpAddr::V6("2001:db9::1".parse::<Ipv6Addr>().unwrap()),
        ));
    }

    #[test]
    fn cidr_family_mismatch() {
        assert!(!cidr_contains(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)),
            8,
            IpAddr::V6("::1".parse::<Ipv6Addr>().unwrap()),
        ));
    }

    #[test]
    fn ip_allowed_combines_exact_and_cidr() {
        let allow = ["192.168.1.42".to_string(), "10.0.0.0/8".to_string()];
        assert!(ip_allowed(&allow, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42))));
        assert!(ip_allowed(&allow, IpAddr::V4(Ipv4Addr::new(10, 5, 5, 5))));
        assert!(!ip_allowed(&allow, IpAddr::V4(Ipv4Addr::new(11, 0, 0, 0))));
    }
}

#[derive(Debug)]
struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::warn!(error = %self.0, "proxy error");
        (StatusCode::BAD_GATEWAY, format!("proxy error: {}", self.0)).into_response()
    }
}
