//! GCE Compute API client: auth resolution (static bearer token, or the GCE
//! metadata-server access token cached to expiry), project/zone auto-detection
//! via the metadata server, and the paginated `zones.list` / `instances.list`
//! GETs the refresh loop issues.
//!
//! Port of the SCOPED subset of `lib/promscrape/discovery/gce/api.go` +
//! `zone.go` + `instance.go` this task supports. Upstream builds a
//! `google.DefaultClient` (which honors `GOOGLE_APPLICATION_CREDENTIALS`, the
//! gcloud SDK creds, and the metadata server). This port supports only:
//! (1) a static `bearer_token` from config; (2) the metadata-server access
//! token. The service-account JSON key file (RS256-JWT -> token exchange) is
//! intentionally NOT ported — see the module doc in [`super`].

use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;

use crate::scrape::config::ScrapeError;

use super::GceSdConfig;

/// Per-request timeout for Compute API calls (`zones.list` /
/// `instances.list`). Each page GET is bounded so a hung endpoint can't stall
/// the refresh thread — and thus [`super::GceDiscovery`]'s `Drop`/`stop` —
/// indefinitely. Matches upstream's 30s intent for the compute client.
const COMPUTE_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// SHORT per-request timeout for metadata-server calls. A non-GCE host has
/// nothing answering on `metadata.google.internal`, so every metadata GET is
/// bounded tightly to fail fast rather than hang.
const METADATA_HTTP_TIMEOUT: Duration = Duration::from_secs(2);

/// Default Compute API base, matching upstream `instance.go` / `zone.go`.
const DEFAULT_COMPUTE_BASE: &str = "https://compute.googleapis.com/compute/v1";

/// Default metadata-server base, matching upstream `api.go`'s
/// `getGCEMetadata`.
const DEFAULT_METADATA_BASE: &str = "http://metadata.google.internal/computeMetadata/v1";

/// Seconds shaved off the metadata token's `expires_in` so it is refreshed
/// slightly early (avoiding a race where a request goes out with a token that
/// expires in transit). Mirrors `scrape::kubernetes::oauth2`'s margin.
const TOKEN_SAFETY_MARGIN_SECS: u64 = 30;

/// Where the Compute API bearer token comes from, resolved once at
/// [`new_gce_api`]: a static token from config (never expires) or the GCE
/// metadata server (fetched + cached on demand in the request path).
enum Auth {
    Static(String),
    Metadata,
}

/// A cached metadata access token and the monotonic instant it should be
/// considered expired at (already reduced by [`TOKEN_SAFETY_MARGIN_SECS`]).
/// [`Instant`] is used deliberately — a monotonic clock isn't affected by
/// wall-clock adjustments.
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// Resolved GCE Compute API access: HTTP clients (one for the Compute API, one
/// short-timeout one for the metadata server), the auth source (+ metadata
/// token cache), and the effective Compute / metadata base URLs.
///
/// `Debug` is hand-written to redact a static bearer token and never print the
/// cached metadata token — defense-in-depth against a future `{:?}` in a log
/// line (mirrors `Ec2Api` / `OAuth2TokenSource`).
pub struct GceApi {
    http: HttpClient,
    metadata_http: HttpClient,
    auth: Auth,
    token_cache: Mutex<Option<CachedToken>>,
    compute_base: String,
    metadata_base: String,
}

impl std::fmt::Debug for GceApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let auth = match &self.auth {
            Auth::Static(_) => "Static(<redacted>)",
            Auth::Metadata => "Metadata",
        };
        f.debug_struct("GceApi")
            .field("auth", &auth)
            .field("compute_base", &self.compute_base)
            .field("metadata_base", &self.metadata_base)
            .finish()
    }
}

/// Builds a [`GceApi`] from `cfg`, resolving auth in priority order: a static
/// `bearer_token` from the config, else the GCE metadata-server access token.
///
/// Fails only on a genuinely bad HTTP-client build — never because GCE /
/// metadata is unreachable (token fetch, project/zone resolution, and the
/// first listing happen later on the refresh thread).
pub fn new_gce_api(cfg: &GceSdConfig) -> Result<GceApi, ScrapeError> {
    let http = HttpClient::builder().build().map_err(|e| ScrapeError {
        msg: format!("cannot build gce http client: {e}"),
    })?;
    let metadata_http = HttpClient::builder()
        .timeout(METADATA_HTTP_TIMEOUT)
        .build()
        .map_err(|e| ScrapeError {
            msg: format!("cannot build gce metadata http client: {e}"),
        })?;

    let auth = match cfg.bearer_token.as_deref().filter(|t| !t.is_empty()) {
        Some(t) => Auth::Static(t.to_string()),
        None => Auth::Metadata,
    };

    Ok(GceApi {
        http,
        metadata_http,
        auth,
        token_cache: Mutex::new(None),
        compute_base: normalize_base(cfg.endpoint.as_deref(), DEFAULT_COMPUTE_BASE),
        metadata_base: normalize_base(cfg.metadata_url.as_deref(), DEFAULT_METADATA_BASE),
    })
}

/// Resolves a base URL: the override (given a scheme if it lacks one, trailing
/// `/` trimmed) if set and non-empty, else `default`.
fn normalize_base(override_url: Option<&str>, default: &str) -> String {
    match override_url.filter(|s| !s.is_empty()) {
        Some(u) => {
            let with_scheme = if u.contains("://") {
                u.to_string()
            } else {
                format!("https://{u}")
            };
            with_scheme.trim_end_matches('/').to_string()
        }
        None => default.to_string(),
    }
}

/// The subset of a GCE metadata token response this port consumes. Port of
/// the metadata token JSON `{access_token, expires_in, token_type}`.
#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    expires_in: u64,
    #[allow(dead_code)]
    token_type: Option<String>,
}

impl GceApi {
    /// Returns a valid Compute API bearer token: the static one as-is, or the
    /// metadata token, refreshing it when the cache is empty or the cached
    /// token has reached its (safety-margin-adjusted) expiry. The cache mutex
    /// is held only to read/store — never across the bounded metadata fetch.
    fn get_token(&self) -> Result<String, ScrapeError> {
        match &self.auth {
            Auth::Static(t) => Ok(t.clone()),
            Auth::Metadata => {
                {
                    let guard = self.token_cache.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(c) = guard.as_ref() {
                        if Instant::now() < c.expires_at {
                            return Ok(c.access_token.clone());
                        }
                    }
                }
                let (access_token, expires_in) = self.fetch_metadata_token()?;
                let expires_at = if expires_in == 0 {
                    Instant::now()
                } else {
                    Instant::now()
                        + Duration::from_secs(expires_in.saturating_sub(TOKEN_SAFETY_MARGIN_SECS))
                };
                let mut guard = self.token_cache.lock().unwrap_or_else(|e| e.into_inner());
                *guard = Some(CachedToken {
                    access_token: access_token.clone(),
                    expires_at,
                });
                Ok(access_token)
            }
        }
    }

    /// Fetches the default service account's access token from the metadata
    /// server. Port of the metadata token path of `api.go`.
    fn fetch_metadata_token(&self) -> Result<(String, u64), ScrapeError> {
        let data = self
            .metadata_get("instance/service-accounts/default/token")
            .map_err(|e| ScrapeError {
                msg: format!(
                    "cannot obtain GCE metadata access token (is this running on GCE?): {}",
                    e.msg
                ),
            })?;
        let parsed: TokenResponse = serde_json::from_slice(&data).map_err(|e| ScrapeError {
            msg: format!("cannot parse GCE metadata token response: {e}"),
        })?;
        if parsed.access_token.is_empty() {
            return Err(ScrapeError {
                msg: "GCE metadata token response is missing access_token".to_string(),
            });
        }
        Ok((parsed.access_token, parsed.expires_in))
    }

    /// Auto-detects the current GCE project via the metadata server. Port of
    /// `getCurrentProject` (`project/project-id`).
    pub fn get_current_project(&self) -> Result<String, ScrapeError> {
        let data = self.metadata_get("project/project-id")?;
        let project = String::from_utf8_lossy(&data).trim().to_string();
        if project.is_empty() {
            return Err(ScrapeError {
                msg: "GCE metadata returned an empty project id".to_string(),
            });
        }
        Ok(project)
    }

    /// Auto-detects the current GCE zone via the metadata server. Port of
    /// `getCurrentZone`: `instance/zone` returns
    /// `projects/<num>/zones/<zone>`; the zone is the 4th `/`-separated part.
    pub fn get_current_zone(&self) -> Result<String, ScrapeError> {
        let data = self.metadata_get("instance/zone")?;
        let raw = String::from_utf8_lossy(&data);
        let parts: Vec<&str> = raw.trim().split('/').collect();
        if parts.len() != 4 {
            return Err(ScrapeError {
                msg: format!(
                    "unexpected GCE zone metadata; want `projects/<num>/zones/<zone>`, got {raw:?}"
                ),
            });
        }
        Ok(parts[3].to_string())
    }

    /// One metadata-server GET with the required `Metadata-Flavor: Google`
    /// header, bounded by [`METADATA_HTTP_TIMEOUT`]. Port of `getGCEMetadata`.
    fn metadata_get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}/{path}", self.metadata_base);
        let resp = self
            .metadata_http
            .get(&url)
            .header("Metadata-Flavor", "Google")
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("GCE metadata request to {path:?} failed: {e}"),
            })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("GCE metadata response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("GCE metadata request to {path:?}: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Lists every zone name for `project`, following `nextPageToken` until
    /// exhausted. Port of `getZonesForProject` (used for `zone: '*'`). No
    /// filter is passed — GCE's `zones.list` does not support it.
    pub fn list_zones(&self, project: &str) -> Result<Vec<String>, ScrapeError> {
        let base = format!("{}/projects/{project}/zones", self.compute_base);
        let mut zones = Vec::new();
        let mut page_token = String::new();
        loop {
            let url = build_query_url(&base, "", &page_token);
            let data = self.compute_get(&url)?;
            let zl = super::labels::parse_zone_list(&data).map_err(|msg| ScrapeError {
                msg: format!("cannot parse zone list from {url:?}: {msg}"),
            })?;
            zones.extend(zl.items.into_iter().map(|z| z.name));
            if zl.next_page_token.is_empty() {
                return Ok(zones);
            }
            page_token = zl.next_page_token;
        }
    }

    /// Lists every instance for `project`/`zone` matching `filter`, following
    /// `nextPageToken` until exhausted. Port of
    /// `getInstancesForProjectAndZone`.
    pub fn list_instances(
        &self,
        project: &str,
        zone: &str,
        filter: &str,
    ) -> Result<Vec<super::labels::Instance>, ScrapeError> {
        let base = format!(
            "{}/projects/{project}/zones/{zone}/instances",
            self.compute_base
        );
        let mut insts = Vec::new();
        let mut page_token = String::new();
        loop {
            let url = build_query_url(&base, filter, &page_token);
            let data = self.compute_get(&url)?;
            let il = super::labels::parse_instance_list(&data).map_err(|msg| ScrapeError {
                msg: format!("cannot parse instance list from {url:?}: {msg}"),
            })?;
            insts.extend(il.items);
            if il.next_page_token.is_empty() {
                return Ok(insts);
            }
            page_token = il.next_page_token;
        }
    }

    /// One Compute API GET with the `Authorization: Bearer <token>` header and
    /// a [`COMPUTE_HTTP_TIMEOUT`] cap, returning the body bytes on a 2xx.
    fn compute_get(&self, url: &str) -> Result<Vec<u8>, ScrapeError> {
        let token = self.get_token()?;
        let resp = self
            .http
            .get(url)
            .timeout(COMPUTE_HTTP_TIMEOUT)
            .bearer_auth(token)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("gce api request to {url:?} failed: {e}"),
            })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("gce api response from {url:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!(
                    "gce api request to {url:?}: status {status}; body: {:?}",
                    String::from_utf8_lossy(&body)
                ),
            });
        }
        Ok(body.to_vec())
    }
}

/// Appends the non-empty `filter` / `pageToken` query args to `base`. Port of
/// `appendNonEmptyQueryArg` (used for both args).
fn build_query_url(base: &str, filter: &str, page_token: &str) -> String {
    let mut url = base.to_string();
    for (name, value) in [("filter", filter), ("pageToken", page_token)] {
        if value.is_empty() {
            continue;
        }
        let sep = if url.contains('?') { '&' } else { '?' };
        url.push(sep);
        url.push_str(&format!("{}={}", query_escape(name), query_escape(value)));
    }
    url
}

/// Go `url.QueryEscape`-equivalent: unreserved (`A-Za-z0-9-_.~`) pass through,
/// space becomes `+`, everything else is `%XX` (UTF-8 byte-wise). Local copy
/// of the EC2/Nomad ports' helper.
fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> GceSdConfig {
        GceSdConfig::default()
    }

    #[test]
    fn static_bearer_token_is_used_and_redacted_in_debug() {
        let mut c = cfg();
        c.bearer_token = Some("super-secret-token".into());
        let api = new_gce_api(&c).unwrap();
        assert!(matches!(api.auth, Auth::Static(_)));
        assert_eq!(api.get_token().unwrap(), "super-secret-token");
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret-token"), "{dbg}");
    }

    #[test]
    fn no_bearer_token_selects_metadata_auth() {
        let api = new_gce_api(&cfg()).unwrap();
        assert!(matches!(api.auth, Auth::Metadata));
    }

    #[test]
    fn bases_default_when_unset() {
        let api = new_gce_api(&cfg()).unwrap();
        assert_eq!(api.compute_base, DEFAULT_COMPUTE_BASE);
        assert_eq!(api.metadata_base, DEFAULT_METADATA_BASE);
    }

    #[test]
    fn base_overrides_are_normalized() {
        let mut c = cfg();
        c.endpoint = Some("127.0.0.1:8080/compute/v1/".into());
        c.metadata_url = Some("http://127.0.0.1:8080/md".into());
        let api = new_gce_api(&c).unwrap();
        // Bare host gets https and the trailing slash trimmed.
        assert_eq!(api.compute_base, "https://127.0.0.1:8080/compute/v1");
        // Explicit scheme is kept.
        assert_eq!(api.metadata_base, "http://127.0.0.1:8080/md");
    }

    #[test]
    fn build_query_url_appends_only_non_empty_args() {
        assert_eq!(build_query_url("http://h/i", "", ""), "http://h/i");
        assert_eq!(
            build_query_url("http://h/i", "status=RUNNING", ""),
            "http://h/i?filter=status%3DRUNNING"
        );
        assert_eq!(
            build_query_url("http://h/i", "", "tok"),
            "http://h/i?pageToken=tok"
        );
        assert_eq!(
            build_query_url("http://h/i", "f", "tok"),
            "http://h/i?filter=f&pageToken=tok"
        );
    }
}
