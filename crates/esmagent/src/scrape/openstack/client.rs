//! OpenStack API client: Keystone v3 token acquisition + caching, service
//! catalog → Nova/compute endpoint resolution, and the paginated Nova GETs
//! (`servers/detail`, `os-hypervisors/detail`) the refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/openstack/api.go` (`newAPIConfig` /
//! `getCreds` / `getFreshAPICredentials` / `getAPIResponse`) plus the
//! pagination loops from `instance.go` / `hypervisor.go`. The Keystone token is
//! obtained via `POST <identity_endpoint>/auth/tokens` (the auth body built by
//! [`super::auth`]), the `X-Subject-Token` response header carries the token,
//! and the service catalog is parsed to find the compute endpoint for the
//! configured `region` + `availability`. The token + compute URL are cached
//! until the token's `expires_at` (converted to a monotonic deadline), with a
//! 401-triggered re-auth as a safety net against clock skew.

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client as HttpClient;

use super::auth::{
    build_auth_request_body, get_compute_endpoint_url, read_credentials_from_env, AuthResponse,
};
use super::hypervisor::{parse_hypervisor_detail, Hypervisor};
use super::instance::{parse_servers_detail, Server};
use super::OpenstackSdConfig;
use crate::client::TlsConfig;
use crate::scrape::config::ScrapeError;

/// Per-request client-side timeout for every Keystone POST and Nova GET, so a
/// hung endpoint can't stall the refresh thread — and thus a
/// [`super::OpenstackDiscovery`] `Drop`/`stop` — indefinitely. Matches
/// upstream's 30s intent.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Refresh margin subtracted from a token's lifetime, mirroring upstream
/// `getFreshAPICredentials`'s `time.Until(expiration) > 10*time.Second` check.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(10);

/// Upper bound on how long a token is cached regardless of its stated
/// `expires_at`, so a bogus far-future timestamp can't pin a stale token. A
/// 401 also forces re-auth.
const TOKEN_CACHE_CAP: Duration = Duration::from_secs(24 * 3600);

/// A cached Keystone token + resolved compute URL, valid until `expires_at`
/// (a monotonic deadline). [`Instant`] is used deliberately — a monotonic
/// clock isn't affected by wall-clock adjustments.
struct CachedCreds {
    token: String,
    compute_url: String,
    expires_at: Instant,
}

/// Resolved OpenStack API access: the HTTP client, the Keystone endpoint, the
/// prebuilt auth-request body, the compute-endpoint filters, and the token
/// cache.
///
/// `Debug` is hand-written to never print the auth body (it holds the password
/// / application-credential secret) or the cached token.
pub struct OpenstackApi {
    http: HttpClient,
    /// Keystone base URL (trailing `/` and a `.0` version suffix trimmed).
    endpoint: String,
    /// Prebuilt Keystone auth-request JSON body (holds secrets).
    auth_token_req: Vec<u8>,
    region: String,
    availability: String,
    all_tenants: bool,
    port: u16,
    creds: Mutex<Option<CachedCreds>>,
}

impl std::fmt::Debug for OpenstackApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenstackApi")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("availability", &self.availability)
            .field("all_tenants", &self.all_tenants)
            .field("port", &self.port)
            .field("auth_token_req", &"<redacted>")
            .finish()
    }
}

impl OpenstackApi {
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Builds an [`OpenstackApi`] from `cfg`, mirroring `newAPIConfig`:
/// - `availability` defaults to `public`;
/// - when `identity_endpoint` is unset, credentials + endpoint are read from
///   the `OS_*` environment variables ([`read_credentials_from_env`]);
/// - a `v2.0` identity endpoint is rejected, and a trailing `.0` (e.g. `v3.0`)
///   is trimmed for Prometheus-config compatibility;
/// - the Keystone auth-request body is built once (failing on missing/invalid
///   auth fields).
///
/// Fails on genuinely bad config (unsupported `v2.0` endpoint, missing auth,
/// bad TLS material) — never because Keystone is unreachable; the first token
/// fetch happens later on the refresh thread.
pub fn new_openstack_api(cfg: &OpenstackSdConfig) -> Result<OpenstackApi, ScrapeError> {
    let http = build_client(&cfg.tls)?;

    let mut availability = cfg.availability.clone();
    if availability.is_empty() {
        availability = "public".to_string();
    }

    // Only the identity endpoint + auth fields come from the env-fallback copy;
    // region/availability/all_tenants/port stay from the original cfg
    // (matching upstream, which sets those before the sdcAuth swap).
    let auth_src = if cfg.identity_endpoint.is_empty() {
        read_credentials_from_env()
    } else {
        cfg.clone()
    };

    if auth_src.identity_endpoint.ends_with("v2.0") {
        return Err(ScrapeError::new("identity_endpoint v2.0 is not supported"));
    }
    // Trim a trailing `.0` (v3.0 -> v3) and any trailing slash.
    let endpoint = auth_src
        .identity_endpoint
        .strip_suffix(".0")
        .unwrap_or(&auth_src.identity_endpoint)
        .trim_end_matches('/')
        .to_string();

    let auth_token_req = build_auth_request_body(&auth_src)?;

    Ok(OpenstackApi {
        http,
        endpoint,
        auth_token_req,
        region: cfg.region.clone(),
        availability,
        all_tenants: cfg.all_tenants,
        port: cfg.port,
        creds: Mutex::new(None),
    })
}

/// Builds a `reqwest::blocking::Client` applying `tls`, mirroring
/// `scrape::hetzner::client`'s builder, with the shared [`HTTP_TIMEOUT`].
fn build_client(tls: &TlsConfig) -> Result<HttpClient, ScrapeError> {
    let mut builder = HttpClient::builder().timeout(HTTP_TIMEOUT);
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
        msg: format!("cannot build openstack http client: {e}"),
    })
}

impl OpenstackApi {
    /// Returns a valid `(token, compute_url)`, reusing the cache until the
    /// token's monotonic deadline, else re-authenticating via [`Self::get_creds`].
    ///
    /// Uses a double-checked pattern: lock → return a clone if the cached token
    /// is still valid → otherwise DROP the guard and perform the Keystone auth
    /// POST WITHOUT holding the lock → re-lock to store the fresh creds. The
    /// cache mutex is never held across the HTTP round-trip, so a hung Keystone
    /// can't block other threads' cache reads. A concurrent double-auth is
    /// harmless (last writer wins).
    fn get_fresh_credentials(&self) -> Result<(String, String), ScrapeError> {
        {
            let guard = self.creds.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(c) = guard.as_ref() {
                if Instant::now() < c.expires_at {
                    return Ok((c.token.clone(), c.compute_url.clone()));
                }
            }
        }
        let creds = self.get_creds()?;
        let out = (creds.token.clone(), creds.compute_url.clone());
        let mut guard = self.creds.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(creds);
        Ok(out)
    }

    /// Invalidates the cached credentials, forcing a re-auth on the next call.
    fn invalidate_credentials(&self) {
        let mut guard = self.creds.lock().unwrap_or_else(|e| e.into_inner());
        *guard = None;
    }

    /// POSTs the auth body to `<endpoint>/auth/tokens`, reads the
    /// `X-Subject-Token` header + parses the catalog for the compute endpoint,
    /// and converts the token's `expires_at` into a monotonic deadline. Port of
    /// `getCreds`.
    fn get_creds(&self) -> Result<CachedCreds, ScrapeError> {
        let url = format!("{}/auth/tokens", self.endpoint);
        let resp = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(self.auth_token_req.clone())
            .send()
            .map_err(|e| {
                ScrapeError::new(format!(
                    "failed query openstack identity api at url {url}: {e}"
                ))
            })?;
        let status = resp.status();
        let token = resp
            .headers()
            .get("X-Subject-Token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = resp
            .bytes()
            .map_err(|e| ScrapeError::new(format!("cannot read response from {url:?}: {e}")))?;
        if status.as_u16() != 201 {
            return Err(ScrapeError::new(format!(
                "auth failed, bad status code: {}, want: 201",
                status.as_u16()
            )));
        }
        if token.is_empty() {
            return Err(ScrapeError::new(
                "auth failed, response without X-Subject-Token",
            ));
        }
        let ar: AuthResponse = serde_json::from_slice(&body).map_err(|e| {
            ScrapeError::new(format!("cannot parse auth credentials response: {e}"))
        })?;
        let compute_url =
            get_compute_endpoint_url(&ar.token.catalog, &self.availability, &self.region).map_err(
                |e| {
                    ScrapeError::new(format!(
                        "cannot get computeEndpoint, account doesn't have enough permissions, \
                     availability: {}, region: {}; error: {}",
                        self.availability, self.region, e.msg
                    ))
                },
            )?;
        Ok(CachedCreds {
            token,
            compute_url,
            expires_at: deadline_from_expires_at(&ar.token.expires_at),
        })
    }

    /// One Nova GET with `X-Auth-Token`, retrying once (after invalidating the
    /// cache) on a 401 so a token expired at the server side triggers a
    /// re-auth. Port of `getAPIResponse` + the client's token refresh.
    fn api_get(&self, url: &str) -> Result<Vec<u8>, ScrapeError> {
        let (token, _) = self.get_fresh_credentials()?;
        let (status, body) = self.do_get(url, &token)?;
        if status == 401 {
            self.invalidate_credentials();
            let (token2, _) = self.get_fresh_credentials()?;
            let (status2, body2) = self.do_get(url, &token2)?;
            if status2 != 200 {
                return Err(unexpected_status(url, status2, &body2));
            }
            return Ok(body2);
        }
        if status != 200 {
            return Err(unexpected_status(url, status, &body));
        }
        Ok(body)
    }

    /// Issues a single Nova GET, returning `(status, body)`.
    fn do_get(&self, url: &str, token: &str) -> Result<(u16, Vec<u8>), ScrapeError> {
        let resp = self
            .http
            .get(url)
            .header("X-Auth-Token", token)
            .send()
            .map_err(|e| ScrapeError::new(format!("cannot query openstack api url {url}: {e}")))?;
        let status = resp.status().as_u16();
        let body = resp
            .bytes()
            .map_err(|e| ScrapeError::new(format!("cannot read response from {url:?}: {e}")))?;
        Ok((status, body.to_vec()))
    }

    /// Lists every Nova server via paginated `servers/detail`, following
    /// `servers_links[0].href` until exhausted. Port of `getServers`.
    pub fn get_servers(&self) -> Result<Vec<Server>, ScrapeError> {
        let (_, compute_url) = self.get_fresh_credentials()?;
        let mut next = format!(
            "{}/servers/detail?all_tenants={}",
            compute_url.trim_end_matches('/'),
            self.all_tenants
        );
        let mut servers = Vec::new();
        loop {
            let data = self.api_get(&next)?;
            let sd = parse_servers_detail(&data).map_err(ScrapeError::new)?;
            servers.extend(sd.servers);
            if sd.links.is_empty() {
                return Ok(servers);
            }
            next = sd.links[0].href.clone();
        }
    }

    /// Lists every Nova hypervisor via paginated `os-hypervisors/detail`,
    /// following `hypervisors_links[0].href` until exhausted. Port of
    /// `getHypervisors`.
    pub fn get_hypervisors(&self) -> Result<Vec<Hypervisor>, ScrapeError> {
        let (_, compute_url) = self.get_fresh_credentials()?;
        let mut next = format!(
            "{}/os-hypervisors/detail",
            compute_url.trim_end_matches('/')
        );
        let mut hvs = Vec::new();
        loop {
            let data = self.api_get(&next)?;
            let d = parse_hypervisor_detail(&data).map_err(ScrapeError::new)?;
            hvs.extend(d.hypervisors);
            if d.links.is_empty() {
                return Ok(hvs);
            }
            next = d.links[0].href.clone();
        }
    }
}

fn unexpected_status(url: &str, status: u16, body: &[u8]) -> ScrapeError {
    ScrapeError::new(format!(
        "unexpected status code for {url:?}; got {status}; want 200; response body: {:?}",
        String::from_utf8_lossy(body)
    ))
}

/// Converts a token's absolute RFC3339 `expires_at` into a conservative
/// monotonic deadline: `now + (ttl - margin)`, capped by [`TOKEN_CACHE_CAP`].
/// An unparseable or already-past timestamp yields `now` (forcing a re-auth on
/// the next call — the freshly-obtained token is still used for the current
/// request).
fn deadline_from_expires_at(expires_at: &str) -> Instant {
    let now = Instant::now();
    let Some(exp_unix) = parse_rfc3339_to_unix(expires_at) else {
        return now;
    };
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let ttl = exp_unix - now_unix - TOKEN_REFRESH_MARGIN.as_secs() as i64;
    if ttl <= 0 {
        return now;
    }
    let ttl = (ttl as u64).min(TOKEN_CACHE_CAP.as_secs());
    now + Duration::from_secs(ttl)
}

/// Minimal RFC3339 → Unix-seconds parser handling `Z`/`z` and `±hh:mm`
/// (or `±hhmm`) offsets and an optional fractional-seconds part. Returns
/// `None` on any structural mismatch.
fn parse_rfc3339_to_unix(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let num = |lo: usize, hi: usize| -> Option<i64> {
        std::str::from_utf8(&b[lo..hi]).ok()?.parse::<i64>().ok()
    };
    if b[4] != b'-'
        || b[7] != b'-'
        || (b[10] != b'T' && b[10] != b't' && b[10] != b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let year = num(0, 4)?;
    let month = num(5, 7)?;
    let day = num(8, 10)?;
    let hour = num(11, 13)?;
    let minute = num(14, 16)?;
    let sec = num(17, 19)?;

    let mut i = 19;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
    }
    let mut offset_secs: i64 = 0;
    if i < b.len() {
        match b[i] {
            b'Z' | b'z' => {}
            b'+' | b'-' => {
                let sign = if b[i] == b'-' { -1 } else { 1 };
                if i + 3 > b.len() {
                    return None;
                }
                let oh = std::str::from_utf8(&b[i + 1..i + 3])
                    .ok()?
                    .parse::<i64>()
                    .ok()?;
                let mm_start = if i + 3 < b.len() && b[i + 3] == b':' {
                    i + 4
                } else {
                    i + 3
                };
                let om = if mm_start + 2 <= b.len() {
                    std::str::from_utf8(&b[mm_start..mm_start + 2])
                        .ok()?
                        .parse::<i64>()
                        .ok()?
                } else {
                    0
                };
                offset_secs = sign * (oh * 3600 + om * 60);
            }
            _ => return None,
        }
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86400 + hour * 3600 + minute * 60 + sec - offset_secs)
}

/// Days since 1970-01-01 for a civil (proleptic Gregorian) date — Howard
/// Hinnant's `days_from_civil` algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_epoch_is_zero() {
        assert_eq!(parse_rfc3339_to_unix("1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn rfc3339_known_utc_value() {
        assert_eq!(
            parse_rfc3339_to_unix("2021-01-01T00:00:00Z"),
            Some(1609459200)
        );
    }

    #[test]
    fn rfc3339_fractional_and_offset() {
        // 2021-01-01T00:00:00 at -01:00 is 01:00:00 UTC -> +3600.
        assert_eq!(
            parse_rfc3339_to_unix("2021-01-01T00:00:00.123456-01:00"),
            Some(1609462800)
        );
        // Microsecond fraction with Z, matching a real Keystone timestamp shape.
        assert_eq!(
            parse_rfc3339_to_unix("2015-11-06T15:32:17.893769Z"),
            Some(1446823937)
        );
    }

    #[test]
    fn rfc3339_bad_input_is_none() {
        assert_eq!(parse_rfc3339_to_unix("not-a-date"), None);
        assert_eq!(parse_rfc3339_to_unix(""), None);
    }

    #[test]
    fn deadline_past_timestamp_forces_refresh() {
        let d = deadline_from_expires_at("2000-01-01T00:00:00Z");
        assert!(d <= Instant::now());
    }

    #[test]
    fn deadline_future_timestamp_is_ahead() {
        let d = deadline_from_expires_at("2999-01-01T00:00:00Z");
        assert!(d > Instant::now());
    }
}
