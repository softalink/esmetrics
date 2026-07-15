//! EC2 query-API client: credential resolution (static config, environment,
//! IMDSv2 instance role), region resolution via IMDS, SigV4-signed
//! `DescribeInstances` / `DescribeAvailabilityZones` GETs, and the
//! `Filter.N.*` query-string builder.
//!
//! Port of the SCOPED subset of `lib/awsapi/config.go` this task supports:
//! the static/env/instance-role credential chain and the EC2 API request
//! path. STS `AssumeRole`/`role_arn`, web-identity tokens, and the shared
//! `~/.aws` config/credentials files are intentionally NOT ported — see the
//! module doc in [`super`].

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;
use url::Url;

use crate::scrape::config::ScrapeError;

use super::sigv4::{format_amz_time, signed_get_headers, AwsCredentials};
use super::{Ec2Filter, Ec2SdConfig};

/// Per-request timeout for EC2 API calls. `DescribeInstances` can paginate,
/// so each page GET is bounded so a hung endpoint can't stall the refresh
/// thread — and thus [`super::Ec2Discovery`]'s `Drop`/`stop` — indefinitely.
const EC2_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// SHORT per-request timeout for IMDS calls. A non-AWS host has nothing
/// listening on the link-local metadata address, so every IMDS GET is
/// bounded tightly to fail fast rather than hang.
const IMDS_HTTP_TIMEOUT: Duration = Duration::from_secs(2);

/// IMDS link-local base URL.
const IMDS_BASE: &str = "http://169.254.169.254";

/// IMDSv2 session-token TTL (seconds).
const IMDS_TOKEN_TTL: &str = "21600";

/// Refresh IMDS credentials this long before their stated expiration.
const IMDS_CREDS_REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// EC2 query API version, matching upstream `awsapi/config.go`. Must stay at
/// `2016-11-15` or later: the 2013-10-15 schema predates the
/// `ipv6AddressesSet` element (`__meta_ec2_ipv6_addresses`) and the
/// `DescribeAvailabilityZones` `zoneId` field (`__meta_ec2_availability_zone_id`),
/// so those labels would silently never populate against real AWS. Only
/// affects the live AWS request, not signing or parsing — the SigV4 known
/// vector test in `sigv4.rs` hardcodes its own `2013-10-15` URL literal
/// (matching upstream's `sign_test.go` vector) independent of this const.
const EC2_API_VERSION: &str = "2016-11-15";

/// Where the API credentials come from, resolved once at [`new_ec2_api`]:
/// static keys (config or environment; never expire) or the IMDSv2 instance
/// role (fetched + cached on demand in the signing path).
enum CredsSource {
    Static(AwsCredentials),
    Imds,
}

/// A cached IMDS credential set plus its stated expiration (`None` =
/// non-expiring, only for static creds — IMDS creds always carry one).
struct CachedCreds {
    creds: AwsCredentials,
    expiration: Option<SystemTime>,
}

/// Resolved EC2 API access: HTTP clients (one for the API, one short-timeout
/// one for IMDS), the credential source (+ IMDS cache), and the effective
/// endpoint override.
///
/// `Debug` is hand-written to redact static secret keys / session tokens —
/// defense-in-depth against a future `{:?}` in a log line (mirrors
/// `ConsulApi`).
pub struct Ec2Api {
    http: HttpClient,
    imds_http: HttpClient,
    source: CredsSource,
    cached: Mutex<Option<CachedCreds>>,
    endpoint_override: Option<String>,
}

impl std::fmt::Debug for Ec2Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source = match &self.source {
            CredsSource::Static(c) => {
                format!("Static(access_key={:?}, secret=<redacted>)", c.access_key)
            }
            CredsSource::Imds => "Imds".to_string(),
        };
        f.debug_struct("Ec2Api")
            .field("source", &source)
            .field("endpoint_override", &self.endpoint_override)
            .finish()
    }
}

/// Builds an [`Ec2Api`] from `cfg`, resolving the credential source in
/// priority order: static `access_key`/`secret_key` from the config, else
/// `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ optional
/// `AWS_SESSION_TOKEN`) from the environment, else the IMDSv2 instance role.
///
/// Fails only on a genuinely bad HTTP-client build — never because AWS/IMDS
/// is unreachable (credential and region resolution happen later on the
/// refresh thread).
pub fn new_ec2_api(cfg: &Ec2SdConfig) -> Result<Ec2Api, ScrapeError> {
    let http = HttpClient::builder().build().map_err(|e| ScrapeError {
        msg: format!("cannot build ec2 http client: {e}"),
    })?;
    let imds_http = HttpClient::builder()
        .timeout(IMDS_HTTP_TIMEOUT)
        .build()
        .map_err(|e| ScrapeError {
            msg: format!("cannot build ec2 imds http client: {e}"),
        })?;

    let source = resolve_creds_source(cfg);
    let endpoint_override = cfg
        .endpoint
        .as_deref()
        .filter(|e| !e.is_empty())
        .map(normalize_endpoint);

    Ok(Ec2Api {
        http,
        imds_http,
        source,
        cached: Mutex::new(None),
        endpoint_override,
    })
}

/// Resolves the credential source per the supported chain (see
/// [`new_ec2_api`]).
fn resolve_creds_source(cfg: &Ec2SdConfig) -> CredsSource {
    let secret = cfg.secret_key.as_deref().unwrap_or_default();
    if !cfg.access_key.is_empty() && !secret.is_empty() {
        return CredsSource::Static(AwsCredentials {
            access_key: cfg.access_key.clone(),
            secret_key: secret.to_string(),
            token: cfg.session_token.clone().filter(|t| !t.is_empty()),
        });
    }
    let env_ak = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
    let env_sk = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
    if !env_ak.is_empty() && !env_sk.is_empty() {
        return CredsSource::Static(AwsCredentials {
            access_key: env_ak,
            secret_key: env_sk,
            token: std::env::var("AWS_SESSION_TOKEN")
                .ok()
                .filter(|t| !t.is_empty()),
        });
    }
    CredsSource::Imds
}

/// `buildAPIEndpoint`: a bare host gets an `https://` scheme; a trailing `/`
/// is ensured.
fn normalize_endpoint(endpoint: &str) -> String {
    let mut e = endpoint.to_string();
    if !e.contains("://") {
        e = format!("https://{e}");
    }
    if !e.ends_with('/') {
        e.push('/');
    }
    e
}

impl Ec2Api {
    /// Returns fresh API credentials, refreshing IMDS creds when they are
    /// within [`IMDS_CREDS_REFRESH_MARGIN`] of expiry. Static creds are
    /// returned as-is. Port of `getFreshAPICredentials`'s scoped subset.
    fn get_fresh_credentials(&self) -> Result<AwsCredentials, ScrapeError> {
        match &self.source {
            CredsSource::Static(c) => Ok(c.clone()),
            CredsSource::Imds => {
                let mut guard = self.cached.lock().unwrap();
                if let Some(cc) = guard.as_ref() {
                    let fresh = match cc.expiration {
                        None => true,
                        Some(exp) => exp > SystemTime::now() + IMDS_CREDS_REFRESH_MARGIN,
                    };
                    if fresh {
                        return Ok(cc.creds.clone());
                    }
                }
                let cc = self.fetch_imds_credentials()?;
                let creds = cc.creds.clone();
                *guard = Some(cc);
                Ok(creds)
            }
        }
    }

    /// Fetches instance-role credentials via IMDSv2. Port of
    /// `getInstanceRoleCredentials` + `parseMetadataSecurityCredentials`.
    fn fetch_imds_credentials(&self) -> Result<CachedCreds, ScrapeError> {
        let role_list = self.imds_get("meta-data/iam/security-credentials/")?;
        let role_list = String::from_utf8_lossy(&role_list);
        let role = role_list.lines().next().unwrap_or("").trim();
        if role.is_empty() {
            return Err(ScrapeError {
                msg: "no IAM instance role found via IMDS".to_string(),
            });
        }
        let data = self.imds_get(&format!("meta-data/iam/security-credentials/{role}"))?;
        let msc: MetadataSecurityCredentials =
            serde_json::from_slice(&data).map_err(|e| ScrapeError {
                msg: format!("cannot parse IMDS security credentials: {e}"),
            })?;
        if msc.access_key_id.is_empty() || msc.secret_access_key.is_empty() {
            return Err(ScrapeError {
                msg: "IMDS returned empty access/secret key".to_string(),
            });
        }
        Ok(CachedCreds {
            creds: AwsCredentials {
                access_key: msc.access_key_id,
                secret_key: msc.secret_access_key,
                token: Some(msc.token).filter(|t| !t.is_empty()),
            },
            expiration: parse_rfc3339(&msc.expiration),
        })
    }

    /// Resolves the AWS region via IMDS: the instance-identity document's
    /// `region`, falling back to `meta-data/placement/region`. Port of
    /// `getDefaultRegion`'s IMDS path (the `AWS_REGION` env var is checked by
    /// the caller in [`super`]).
    pub fn resolve_region_via_imds(&self) -> Result<String, ScrapeError> {
        if let Ok(data) = self.imds_get("dynamic/instance-identity/document") {
            if let Ok(doc) = serde_json::from_slice::<IdentityDocument>(&data) {
                if !doc.region.is_empty() {
                    return Ok(doc.region);
                }
            }
        }
        let data = self.imds_get("meta-data/placement/region")?;
        let region = String::from_utf8_lossy(&data).trim().to_string();
        if region.is_empty() {
            return Err(ScrapeError {
                msg: "IMDS returned empty region".to_string(),
            });
        }
        Ok(region)
    }

    /// One IMDSv2 GET: PUT for a session token (short TTL), then GET
    /// `/latest/<path>` with the token header. Both calls are bounded by
    /// [`IMDS_HTTP_TIMEOUT`]. Port of `getMetadataByPath`.
    fn imds_get(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let token_url = format!("{IMDS_BASE}/latest/api/token");
        let token_resp = self
            .imds_http
            .put(&token_url)
            .header("X-aws-ec2-metadata-token-ttl-seconds", IMDS_TOKEN_TTL)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("cannot obtain IMDSv2 session token: {e}"),
            })?;
        if !token_resp.status().is_success() {
            return Err(ScrapeError {
                msg: format!(
                    "IMDSv2 session token request: status {}",
                    token_resp.status()
                ),
            });
        }
        let token = token_resp.text().map_err(|e| ScrapeError {
            msg: format!("cannot read IMDSv2 session token: {e}"),
        })?;

        let url = format!("{IMDS_BASE}/latest/{path}");
        let resp = self
            .imds_http
            .get(&url)
            .header("X-aws-ec2-metadata-token", token)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("IMDS request to {path:?} failed: {e}"),
            })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("IMDS response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("IMDS request to {path:?}: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Issues `DescribeInstances` for `region` with optional filter and
    /// pagination-token query strings, returning the raw XML body.
    pub fn describe_instances(
        &self,
        region: &str,
        filters_qs: &str,
        next_token: &str,
    ) -> Result<Vec<u8>, ScrapeError> {
        self.ec2_api_response(region, "DescribeInstances", filters_qs, next_token)
    }

    /// Issues `DescribeAvailabilityZones` for `region`, returning the raw XML
    /// body.
    pub fn describe_availability_zones(
        &self,
        region: &str,
        filters_qs: &str,
    ) -> Result<Vec<u8>, ScrapeError> {
        self.ec2_api_response(region, "DescribeAvailabilityZones", filters_qs, "")
    }

    /// Signs and issues one EC2 query-API GET. Port of `GetEC2APIResponse`.
    fn ec2_api_response(
        &self,
        region: &str,
        action: &str,
        filters_qs: &str,
        next_token: &str,
    ) -> Result<Vec<u8>, ScrapeError> {
        let creds = self.get_fresh_credentials()?;
        let endpoint = self.effective_endpoint(region);
        let mut api_url = format!("{endpoint}?Action={}", query_escape(action));
        if !filters_qs.is_empty() {
            api_url.push('&');
            api_url.push_str(filters_qs);
        }
        if !next_token.is_empty() {
            api_url.push_str(&format!("&NextToken={}", query_escape(next_token)));
        }
        api_url.push_str(&format!("&Version={EC2_API_VERSION}"));

        let url = Url::parse(&api_url).map_err(|e| ScrapeError {
            msg: format!("cannot build ec2 api url {api_url:?}: {e}"),
        })?;
        let (amz_date, date_stamp) = format_amz_time(SystemTime::now());
        let headers = signed_get_headers(&url, "ec2", region, &creds, &amz_date, &date_stamp);

        let mut req = self.http.get(url.clone()).timeout(EC2_HTTP_TIMEOUT);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().map_err(|e| ScrapeError {
            msg: format!("ec2 api request to {url} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("ec2 api response from {url}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!(
                    "ec2 api request to {url}: status {status}; body: {:?}",
                    String::from_utf8_lossy(&body)
                ),
            });
        }
        Ok(body.to_vec())
    }

    /// `https://ec2.<region>.amazonaws.com/` unless a config endpoint
    /// override was set. Port of `buildAPIEndpoint`.
    fn effective_endpoint(&self, region: &str) -> String {
        match &self.endpoint_override {
            Some(e) => e.clone(),
            None => format!("https://ec2.{region}.amazonaws.com/"),
        }
    }
}

/// Builds the `Filter.N.Name`/`Filter.N.Value.M` query string from `filters`.
/// Port of `GetFiltersQueryString`.
pub fn filters_query_string(filters: &[Ec2Filter]) -> String {
    let mut args: Vec<String> = Vec::new();
    for (i, f) in filters.iter().enumerate() {
        args.push(format!("Filter.{}.Name={}", i + 1, query_escape(&f.name)));
        for (j, v) in f.values.iter().enumerate() {
            args.push(format!(
                "Filter.{}.Value.{}={}",
                i + 1,
                j + 1,
                query_escape(v)
            ));
        }
    }
    args.join("&")
}

/// Go `url.QueryEscape`-equivalent: unreserved (`A-Za-z0-9-_.~`) pass
/// through, space becomes `+`, everything else is `%XX` (UTF-8 byte-wise).
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

/// Parses an RFC3339 UTC timestamp of the shape `YYYY-MM-DDTHH:MM:SSZ` (the
/// form IMDS returns for credential expiration) to a [`SystemTime`]. Returns
/// `None` on any parse failure, so a malformed expiration degrades to
/// "refetch on next signing" rather than a panic.
fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    let s = s.trim();
    // "2020-04-27T09:19:26Z" — take the first 19 chars as the naive datetime,
    // require a 'T' separator and a trailing 'Z' (UTC).
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

/// Converts a Gregorian `(year, month, day)` to a count of days since the
/// Unix epoch. Howard Hinnant's `days_from_civil` algorithm.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// IMDS security-credentials JSON. Port of `MetadataSecurityCredentials`.
#[derive(Debug, Default, Deserialize)]
struct MetadataSecurityCredentials {
    #[serde(rename = "AccessKeyId", default)]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey", default)]
    secret_access_key: String,
    #[serde(rename = "Token", default)]
    token: String,
    #[serde(rename = "Expiration", default)]
    expiration: String,
}

/// Instance-identity document, narrowed to `region`. Port of
/// `IdentityDocument`.
#[derive(Debug, Default, Deserialize)]
struct IdentityDocument {
    #[serde(default)]
    region: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Ec2SdConfig {
        Ec2SdConfig::default()
    }

    #[test]
    fn static_config_creds_win_and_are_redacted_in_debug() {
        let mut c = cfg();
        c.access_key = "AKIDEXAMPLE".into();
        c.secret_key = Some("super-secret-key".into());
        c.session_token = Some("tok".into());
        let api = new_ec2_api(&c).unwrap();
        match &api.source {
            CredsSource::Static(creds) => {
                assert_eq!(creds.access_key, "AKIDEXAMPLE");
                assert_eq!(creds.secret_key, "super-secret-key");
                assert_eq!(creds.token.as_deref(), Some("tok"));
            }
            _ => panic!("expected static creds"),
        }
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret-key"), "{dbg}");
        assert!(!dbg.contains("tok\""), "{dbg}");
    }

    #[test]
    fn falls_back_to_imds_when_no_static_or_env_creds() {
        // Guard against env creds leaking in from the runner.
        let has_env = !std::env::var("AWS_ACCESS_KEY_ID")
            .unwrap_or_default()
            .is_empty()
            && !std::env::var("AWS_SECRET_ACCESS_KEY")
                .unwrap_or_default()
                .is_empty();
        if has_env {
            return;
        }
        let api = new_ec2_api(&cfg()).unwrap();
        assert!(matches!(api.source, CredsSource::Imds));
    }

    #[test]
    fn endpoint_override_is_normalized() {
        // A bare host gets an https scheme and a trailing slash.
        let mut c = cfg();
        c.endpoint = Some("127.0.0.1:8123".into());
        let api = new_ec2_api(&c).unwrap();
        assert_eq!(
            api.effective_endpoint("us-east-1"),
            "https://127.0.0.1:8123/"
        );

        // An explicit http:// endpoint keeps its scheme.
        let mut c2 = cfg();
        c2.endpoint = Some("http://127.0.0.1:8123".into());
        let api2 = new_ec2_api(&c2).unwrap();
        assert_eq!(
            api2.effective_endpoint("us-east-1"),
            "http://127.0.0.1:8123/"
        );
    }

    #[test]
    fn effective_endpoint_defaults_to_regional_host() {
        let api = new_ec2_api(&cfg()).unwrap();
        assert_eq!(
            api.effective_endpoint("eu-west-2"),
            "https://ec2.eu-west-2.amazonaws.com/"
        );
    }

    #[test]
    fn filters_query_string_builds_indexed_params() {
        let filters = vec![
            Ec2Filter {
                name: "instance-state-name".into(),
                values: vec!["running".into(), "pending".into()],
            },
            Ec2Filter {
                name: "tag:env".into(),
                values: vec!["prod".into()],
            },
        ];
        assert_eq!(
            filters_query_string(&filters),
            "Filter.1.Name=instance-state-name&Filter.1.Value.1=running&Filter.1.Value.2=pending&Filter.2.Name=tag%3Aenv&Filter.2.Value.1=prod"
        );
    }

    #[test]
    fn parse_rfc3339_parses_utc_timestamp() {
        // 2020-04-27T09:19:26Z == unix 1587979166.
        let t = parse_rfc3339("2020-04-27T09:19:26Z").expect("parse");
        assert_eq!(
            t.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            1_587_979_166
        );
        assert!(parse_rfc3339("not-a-time").is_none());
        assert!(parse_rfc3339("2020-04-27 09:19:26").is_none());
    }
}
