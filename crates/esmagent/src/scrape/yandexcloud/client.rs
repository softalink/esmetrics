//! Yandex Cloud API client: auth resolution (a static
//! `yandex_passport_oauth_token` exchanged for an IAM token at the IAM tokens
//! endpoint, or the compute metadata-server IAM token, cached to near-expiry),
//! service-endpoint resolution (`GET <api_endpoint>/endpoints`), and the
//! paginated resource-manager (organizations/clouds/folders) + compute
//! (instances) GETs the refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/yandexcloud/api.go` (auth + endpoints) and
//! `yandexcloud.go`'s enumeration methods. Upstream supports the OAuth-token
//! exchange, the GCE-style metadata token, and a disabled EC2 IMDSv1 fallback.
//! This port supports the first two; the service-account authorized-key JSON
//! (JWT -> IAM exchange) path is intentionally NOT ported — a set
//! `service_account_key_file` is rejected at build time (see the module doc in
//! [`super`]).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;

use crate::client::TlsConfig;
use crate::scrape::config::ScrapeError;

use super::labels::{parse_instances_page, Instance};
use super::YandexcloudSdConfig;

/// Per-request timeout for resource-manager / compute API calls. Each page GET
/// is bounded so a hung endpoint can't stall the refresh thread — and thus
/// [`super::YandexcloudDiscovery`]'s `Drop`/`stop` — indefinitely. Matches the
/// GCE/DigitalOcean 30s intent.
const API_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// SHORT per-request timeout for compute metadata-server calls. A non-Yandex
/// host has nothing answering on `169.254.169.254`, so the metadata token GET
/// is bounded tightly to fail fast rather than hang. Mirrors GCE's metadata
/// timeout.
const METADATA_HTTP_TIMEOUT: Duration = Duration::from_secs(2);

/// Default Yandex Cloud API endpoint, matching upstream `api.go`'s
/// `newAPIConfig` (`https://api.cloud.yandex.net`).
const DEFAULT_API_ENDPOINT: &str = "https://api.cloud.yandex.net";

/// Default compute metadata-server base. The token path
/// [`METADATA_TOKEN_PATH`] is appended. Overridable for tests via
/// `metadata_url`.
const DEFAULT_METADATA_BASE: &str = "http://169.254.169.254";

/// GCE-style compute metadata token path, matching upstream
/// `getGCEInstanceCreds`.
const METADATA_TOKEN_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";

/// Refresh the cached IAM token this long before its stated expiry — matches
/// upstream `getFreshAPICredentials`'s `10*time.Second` margin.
const CREDS_REFRESH_MARGIN: Duration = Duration::from_secs(10);

/// Where the IAM token comes from, resolved once at [`new_yandexcloud_api`]:
/// a static Passport OAuth token exchanged at the IAM tokens endpoint, or the
/// compute metadata server.
enum Auth {
    /// `yandex_passport_oauth_token` exchanged for an IAM token.
    OAuth(String),
    /// Compute metadata-server IAM token (no token configured).
    Metadata,
}

/// A cached IAM token plus its stated expiry (`None` = unknown expiry -> always
/// refetch). [`SystemTime`] mirrors `scrape::ec2`'s cache; the OAuth exchange
/// returns an RFC3339 `expiresAt` and the metadata token an `expires_in`
/// seconds value, both convertible to a wall-clock instant.
struct CachedCreds {
    token: String,
    expiration: Option<SystemTime>,
}

/// The four Yandex Cloud service endpoints this port uses, resolved from
/// `GET <api_endpoint>/endpoints`. Port of `api.go`'s `serviceEndpoints` map,
/// narrowed to the ids we call.
pub struct ServiceEndpoints {
    map: BTreeMap<String, String>,
}

impl ServiceEndpoints {
    fn get(&self, id: &str) -> Result<&str, ScrapeError> {
        self.map
            .get(id)
            .map(String::as_str)
            .ok_or_else(|| ScrapeError {
                msg: format!("yandexcloud API endpoints list is missing service {id:?}"),
            })
    }
}

/// Resolved Yandex Cloud API access: HTTP clients (one for the API, one
/// short-timeout one for the metadata server), the auth source (+ IAM token
/// cache), and the effective `api_endpoint` / metadata base URLs.
///
/// `Debug` is hand-written to redact a static OAuth token and never print the
/// cached IAM token — defense-in-depth against a future `{:?}` in a log line
/// (mirrors `GceApi` / `Ec2Api`).
pub struct YandexcloudApi {
    http: HttpClient,
    metadata_http: HttpClient,
    auth: Auth,
    creds: Mutex<Option<CachedCreds>>,
    api_endpoint: String,
    metadata_base: String,
}

impl std::fmt::Debug for YandexcloudApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let auth = match &self.auth {
            Auth::OAuth(_) => "OAuth(<redacted>)",
            Auth::Metadata => "Metadata",
        };
        f.debug_struct("YandexcloudApi")
            .field("auth", &auth)
            .field("api_endpoint", &self.api_endpoint)
            .field("metadata_base", &self.metadata_base)
            .finish()
    }
}

/// Builds a [`YandexcloudApi`] from `cfg`, resolving auth in priority order: a
/// static `yandex_passport_oauth_token` (exchanged for an IAM token), else the
/// compute metadata-server IAM token.
///
/// Fails only on a genuinely bad HTTP-client build (bad TLS material) — never
/// because Yandex Cloud / metadata is unreachable (token fetch, endpoint
/// resolution, and listing happen later on the refresh thread).
pub fn new_yandexcloud_api(cfg: &YandexcloudSdConfig) -> Result<YandexcloudApi, ScrapeError> {
    let http = build_client(&cfg.tls)?;
    let metadata_http = HttpClient::builder()
        .timeout(METADATA_HTTP_TIMEOUT)
        .build()
        .map_err(|e| ScrapeError {
            msg: format!("cannot build yandexcloud metadata http client: {e}"),
        })?;

    let auth = match cfg
        .yandex_passport_oauth_token
        .as_deref()
        .filter(|t| !t.is_empty())
    {
        Some(t) => Auth::OAuth(t.to_string()),
        None => Auth::Metadata,
    };

    Ok(YandexcloudApi {
        http,
        metadata_http,
        auth,
        creds: Mutex::new(None),
        api_endpoint: normalize_endpoint(cfg.api_endpoint.as_deref(), DEFAULT_API_ENDPOINT),
        metadata_base: normalize_endpoint(cfg.metadata_url.as_deref(), DEFAULT_METADATA_BASE),
    })
}

/// Resolves a base URL: the override (given `https` if it lacks a scheme,
/// trailing `/` trimmed) if set and non-empty, else `default`.
fn normalize_endpoint(override_url: Option<&str>, default: &str) -> String {
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
        msg: format!("cannot build yandexcloud http client: {e}"),
    })
}

/// `GET <api_endpoint>/endpoints` response. Port of `api.go`'s `endpoints`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EndpointsResponse {
    endpoints: Vec<EndpointEntry>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EndpointEntry {
    id: String,
    address: String,
}

/// Metadata token response `{access_token, expires_in, token_type}`. Port of
/// `gceAPICredentials`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GceApiCredentials {
    access_token: String,
    expires_in: u64,
    token_type: String,
}

/// IAM token exchange response `{iamToken, expiresAt}`. Port of `iamToken`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct IamTokenResponse {
    #[serde(rename = "iamToken")]
    iam_token: String,
    #[serde(rename = "expiresAt")]
    expires_at: String,
}

/// A resource-manager / organization-manager list page. All three list
/// endpoints share this shape (`{<items>, nextPageToken}`); only the item `id`
/// is read, so a single flattened page type with a renamed items field per call
/// isn't worth it — each list method deserializes into its own typed page.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct OrganizationsPage {
    organizations: Vec<IdEntry>,
    #[serde(rename = "nextPageToken")]
    next_page_token: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CloudsPage {
    clouds: Vec<IdEntry>,
    #[serde(rename = "nextPageToken")]
    next_page_token: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FoldersPage {
    folders: Vec<IdEntry>,
    #[serde(rename = "nextPageToken")]
    next_page_token: String,
}

/// A resource-manager entry — only `id` is consumed by the enumeration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct IdEntry {
    id: String,
}

impl YandexcloudApi {
    /// Resolves the four service endpoints from `GET <api_endpoint>/endpoints`,
    /// mapping each returned `{id, address}` to `<scheme>://<address>` where
    /// `scheme` is the `api_endpoint`'s scheme. Port of `getServiceEndpoints`.
    pub fn resolve_service_endpoints(&self) -> Result<ServiceEndpoints, ScrapeError> {
        let scheme = self.api_endpoint.split("://").next().unwrap_or("https");
        let url = format!("{}/endpoints", self.api_endpoint);
        let data = self.plain_get(&url)?;
        let parsed: EndpointsResponse = serde_json::from_slice(&data).map_err(|e| ScrapeError {
            msg: format!("cannot parse yandexcloud API endpoints list from {url:?}: {e}"),
        })?;
        let mut map = BTreeMap::new();
        for ep in parsed.endpoints {
            map.insert(ep.id, format!("{scheme}://{}", ep.address));
        }
        Ok(ServiceEndpoints { map })
    }

    /// Lists every organization id, following `nextPageToken`. Port of
    /// `getOrganizations`.
    pub fn list_organizations(&self, eps: &ServiceEndpoints) -> Result<Vec<String>, ScrapeError> {
        let base = format!(
            "{}/organization-manager/v1/organizations",
            eps.get("organization-manager")?
        );
        let mut ids = Vec::new();
        let mut page_token = String::new();
        loop {
            let url = append_page_token(&base, &page_token);
            let data = self.api_get(&url, eps)?;
            let page: OrganizationsPage = parse_page(&data, &url)?;
            ids.extend(page.organizations.into_iter().map(|o| o.id));
            if page.next_page_token.is_empty() {
                return Ok(ids);
            }
            page_token = page.next_page_token;
        }
    }

    /// Lists cloud ids for the given organizations (or org-less), following
    /// `nextPageToken`. Port of `getClouds`.
    pub fn list_clouds(
        &self,
        eps: &ServiceEndpoints,
        org_ids: &[String],
    ) -> Result<Vec<String>, ScrapeError> {
        let base = format!(
            "{}/resource-manager/v1/clouds",
            eps.get("resource-manager")?
        );
        // An empty org list still performs one org-less listing (upstream
        // appends an empty organization).
        let orgs: Vec<&str> = if org_ids.is_empty() {
            vec![""]
        } else {
            org_ids.iter().map(String::as_str).collect()
        };
        let mut ids = Vec::new();
        for org in orgs {
            let org_url = if org.is_empty() {
                base.clone()
            } else {
                format!("{base}?organizationId={}", query_escape(org))
            };
            let mut page_token = String::new();
            loop {
                let url = append_page_token(&org_url, &page_token);
                let data = self.api_get(&url, eps)?;
                let page: CloudsPage = parse_page(&data, &url)?;
                ids.extend(page.clouds.into_iter().map(|c| c.id));
                if page.next_page_token.is_empty() {
                    break;
                }
                page_token = page.next_page_token;
            }
        }
        Ok(ids)
    }

    /// Lists folder ids for the given clouds, following `nextPageToken`. Port of
    /// `getFolders`.
    pub fn list_folders(
        &self,
        eps: &ServiceEndpoints,
        cloud_ids: &[String],
    ) -> Result<Vec<String>, ScrapeError> {
        let base = format!(
            "{}/resource-manager/v1/folders",
            eps.get("resource-manager")?
        );
        let mut ids = Vec::new();
        for cloud in cloud_ids {
            let cloud_url = format!("{base}?cloudId={}", query_escape(cloud));
            let mut page_token = String::new();
            loop {
                let url = append_page_token(&cloud_url, &page_token);
                let data = self.api_get(&url, eps)?;
                let page: FoldersPage = parse_page(&data, &url)?;
                ids.extend(page.folders.into_iter().map(|fld| fld.id));
                if page.next_page_token.is_empty() {
                    break;
                }
                page_token = page.next_page_token;
            }
        }
        Ok(ids)
    }

    /// Lists every compute instance for `folder_id`, following `nextPageToken`.
    /// Port of `getInstances`.
    pub fn list_instances(
        &self,
        eps: &ServiceEndpoints,
        folder_id: &str,
    ) -> Result<Vec<Instance>, ScrapeError> {
        let base = format!(
            "{}/compute/v1/instances?folderId={}",
            eps.get("compute")?,
            query_escape(folder_id)
        );
        let mut insts = Vec::new();
        let mut page_token = String::new();
        loop {
            let url = append_page_token(&base, &page_token);
            let data = self.api_get(&url, eps)?;
            let page = parse_instances_page(&data).map_err(|msg| ScrapeError {
                msg: format!("cannot parse instances response from {url:?}: {msg}"),
            })?;
            insts.extend(page.instances);
            if page.next_page_token.is_empty() {
                return Ok(insts);
            }
            page_token = page.next_page_token;
        }
    }

    /// One authenticated API GET (`Authorization: Bearer <iam-token>`), bounded
    /// by [`API_HTTP_TIMEOUT`], returning the body bytes on a 2xx. Port of
    /// `getAPIResponse`.
    fn api_get(&self, url: &str, eps: &ServiceEndpoints) -> Result<Vec<u8>, ScrapeError> {
        let token = self.fresh_token(eps)?;
        let resp = self
            .http
            .get(url)
            .timeout(API_HTTP_TIMEOUT)
            .bearer_auth(token)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("cannot query yandexcloud api url {url:?}: {e}"),
            })?;
        read_body(resp, url)
    }

    /// One unauthenticated GET (used for `/endpoints`, which needs no token),
    /// bounded by [`API_HTTP_TIMEOUT`].
    fn plain_get(&self, url: &str) -> Result<Vec<u8>, ScrapeError> {
        let resp = self
            .http
            .get(url)
            .timeout(API_HTTP_TIMEOUT)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("cannot query {url:?}: {e}"),
            })?;
        read_body(resp, url)
    }

    /// Returns a valid IAM bearer token, refreshing the cache when empty or
    /// within [`CREDS_REFRESH_MARGIN`] of expiry. The cache mutex is held only
    /// to read/store — never across the bounded network fetch. Port of
    /// `getFreshAPICredentials`.
    fn fresh_token(&self, eps: &ServiceEndpoints) -> Result<String, ScrapeError> {
        {
            let guard = self.creds.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(c) = guard.as_ref() {
                let fresh = match c.expiration {
                    None => false,
                    Some(exp) => exp > SystemTime::now() + CREDS_REFRESH_MARGIN,
                };
                if fresh {
                    return Ok(c.token.clone());
                }
            }
        }
        let creds = match &self.auth {
            Auth::OAuth(token) => self.exchange_oauth_token(token, eps)?,
            Auth::Metadata => self.fetch_metadata_token()?,
        };
        let token = creds.token.clone();
        let mut guard = self.creds.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(creds);
        Ok(token)
    }

    /// Exchanges the static Passport OAuth token for an IAM token at
    /// `<iam>/iam/v1/tokens`. Port of `getIAMToken`.
    fn exchange_oauth_token(
        &self,
        oauth_token: &str,
        eps: &ServiceEndpoints,
    ) -> Result<CachedCreds, ScrapeError> {
        let url = format!("{}/iam/v1/tokens", eps.get("iam")?);
        let body = serde_json::json!({ "yandexPassportOauthToken": oauth_token }).to_string();
        let resp = self
            .http
            .post(&url)
            .timeout(API_HTTP_TIMEOUT)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("cannot send request to yandex cloud iam api {url:?}: {e}"),
            })?;
        let data = read_body(resp, &url)?;
        let parsed: IamTokenResponse = serde_json::from_slice(&data).map_err(|e| ScrapeError {
            msg: format!("cannot parse iam token from {url:?}: {e}"),
        })?;
        if parsed.iam_token.is_empty() {
            return Err(ScrapeError {
                msg: format!("yandexcloud IAM token response from {url:?} is missing iamToken"),
            });
        }
        Ok(CachedCreds {
            token: parsed.iam_token,
            expiration: parse_rfc3339(&parsed.expires_at),
        })
    }

    /// Fetches the compute metadata-server IAM token. Port of
    /// `getGCEInstanceCreds`.
    fn fetch_metadata_token(&self) -> Result<CachedCreds, ScrapeError> {
        let url = format!("{}{METADATA_TOKEN_PATH}", self.metadata_base);
        let resp = self
            .metadata_http
            .get(&url)
            .header("Metadata-Flavor", "Google")
            .send()
            .map_err(|e| ScrapeError {
                msg: format!(
                    "cannot obtain yandexcloud metadata token from {url:?} \
                     (is this running in Yandex Cloud?): {e}"
                ),
            })?;
        let data = read_body(resp, &url)?;
        let parsed: GceApiCredentials = serde_json::from_slice(&data).map_err(|e| ScrapeError {
            msg: format!("cannot unmarshal metadata token from {url:?}: {e}"),
        })?;
        if parsed.token_type != "Bearer" {
            return Err(ScrapeError {
                msg: format!(
                    "unsupported metadata token type from {url:?}: {:?}; supported: \"Bearer\"",
                    parsed.token_type
                ),
            });
        }
        if parsed.access_token.is_empty() {
            return Err(ScrapeError {
                msg: format!(
                    "yandexcloud metadata token response from {url:?} is missing access_token"
                ),
            });
        }
        Ok(CachedCreds {
            token: parsed.access_token,
            expiration: Some(SystemTime::now() + Duration::from_secs(parsed.expires_in)),
        })
    }
}

/// Reads a response body, erroring on a non-2xx status. Port of
/// `readResponseBody`.
fn read_body(resp: reqwest::blocking::Response, url: &str) -> Result<Vec<u8>, ScrapeError> {
    let status = resp.status();
    let body = resp.bytes().map_err(|e| ScrapeError {
        msg: format!("cannot read response from {url:?}: {e}"),
    })?;
    if !status.is_success() {
        return Err(ScrapeError {
            msg: format!(
                "unexpected status code for {url:?}; got {status}; response body: {:?}",
                String::from_utf8_lossy(&body)
            ),
        });
    }
    Ok(body.to_vec())
}

/// Deserializes a resource-manager list page, wrapping the parse error with the
/// source URL.
fn parse_page<T: for<'de> Deserialize<'de>>(data: &[u8], url: &str) -> Result<T, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot parse response from {url:?}: {e}"),
    })
}

/// Appends `&pageToken=...` (or `?pageToken=...` when the base has no query) if
/// `page_token` is non-empty. All list URLs already carry their scoping query
/// arg, so this matches upstream's `nextLink` construction.
fn append_page_token(base: &str, page_token: &str) -> String {
    if page_token.is_empty() {
        return base.to_string();
    }
    let sep = if base.contains('?') { '&' } else { '?' };
    format!("{base}{sep}pageToken={}", query_escape(page_token))
}

/// Go `url.QueryEscape`-equivalent: unreserved (`A-Za-z0-9-_.~`) pass through,
/// space becomes `+`, everything else is `%XX` (UTF-8 byte-wise). Local copy of
/// the GCE/EC2 ports' helper.
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

/// Parses an RFC3339 UTC timestamp `YYYY-MM-DDTHH:MM:SS[.frac]Z` (the form the
/// IAM tokens endpoint returns for `expiresAt`) to a [`SystemTime`]. Returns
/// `None` on any parse failure, so a malformed expiry degrades to "refetch on
/// next use" rather than a panic. Local copy of `scrape::ec2`'s helper, tolerant
/// of a fractional-seconds component.
fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() < 20 || bytes[10] != b'T' || *bytes.last()? != b'Z' {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hh: i64 = s.get(11..13)?.parse().ok()?;
    let mm: i64 = s.get(14..16)?.parse().ok()?;
    let ss: i64 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hh * 3600 + mm * 60 + ss;
    if secs < 0 {
        return None;
    }
    Some(UNIX_EPOCH + Duration::from_secs(secs as u64))
}

/// Converts a Gregorian `(year, month, day)` to a count of days since the Unix
/// epoch. Howard Hinnant's `days_from_civil` algorithm (local copy of
/// `scrape::ec2`'s helper).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> YandexcloudSdConfig {
        YandexcloudSdConfig {
            service: "compute".to_string(),
            ..YandexcloudSdConfig::default()
        }
    }

    #[test]
    fn oauth_token_selected_and_redacted_in_debug() {
        let mut c = cfg();
        c.yandex_passport_oauth_token = Some("super-secret-oauth".into());
        let api = new_yandexcloud_api(&c).unwrap();
        assert!(matches!(api.auth, Auth::OAuth(_)));
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret-oauth"), "{dbg}");
    }

    #[test]
    fn no_token_selects_metadata_auth() {
        let api = new_yandexcloud_api(&cfg()).unwrap();
        assert!(matches!(api.auth, Auth::Metadata));
    }

    #[test]
    fn endpoints_default_and_override() {
        let api = new_yandexcloud_api(&cfg()).unwrap();
        assert_eq!(api.api_endpoint, DEFAULT_API_ENDPOINT);
        assert_eq!(api.metadata_base, DEFAULT_METADATA_BASE);

        let mut c = cfg();
        c.api_endpoint = Some("127.0.0.1:8080/".into());
        c.metadata_url = Some("http://127.0.0.1:9090".into());
        let api = new_yandexcloud_api(&c).unwrap();
        assert_eq!(api.api_endpoint, "https://127.0.0.1:8080");
        assert_eq!(api.metadata_base, "http://127.0.0.1:9090");
    }

    #[test]
    fn append_page_token_only_when_present() {
        assert_eq!(
            append_page_token("http://h/f?cloudId=c", ""),
            "http://h/f?cloudId=c"
        );
        assert_eq!(
            append_page_token("http://h/f?cloudId=c", "tok"),
            "http://h/f?cloudId=c&pageToken=tok"
        );
        assert_eq!(
            append_page_token("http://h/orgs", "t k"),
            "http://h/orgs?pageToken=t+k"
        );
    }

    #[test]
    fn parse_rfc3339_handles_plain_and_fractional() {
        assert!(parse_rfc3339("2020-04-27T09:19:26Z").is_some());
        assert!(parse_rfc3339("2020-04-27T09:19:26.123456789Z").is_some());
        assert!(parse_rfc3339("not-a-time").is_none());
    }
}
