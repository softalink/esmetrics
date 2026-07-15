//! OVHcloud HTTP API client: endpoint→base-URL resolution, the OVH request
//! signing scheme, the `/auth/time` server-clock sync, and the per-service
//! list + detail GETs the refresh loop issues.
//!
//! Port of `lib/promscrape/discovery/ovhcloud/api.go` (`availableEndpoints`
//! map, `newAPIConfig`), `common.go` (`getAuthHeaders`, `getServerTime`,
//! `getOVHTimestamp`), and the listing calls in `dedicated_server.go` / `vps.go`.
//!
//! ## Signing
//!
//! Every data request carries these headers (`common.go`):
//! - `X-Ovh-Application: <application_key>`
//! - `X-Ovh-Timestamp: <ts>` where `ts = now - timeDelta` (`timeDelta` learned
//!   from `/auth/time`, defaulting to `now` when that call fails)
//! - `X-Ovh-Consumer: <consumer_key>`
//! - `X-Ovh-Signature: "$1$" + sha1_hex(secret+consumer+GET+fullURL+body+ts)`
//!   — the six fields joined by literal `+`, with an empty body (so a `++`
//!   appears). See [`ovh_signature`].
//!
//! The base URL is overridable (`cfg.api_url_override`) so the stub tests can
//! point it at an in-process server, mirroring upstream's
//! `mock_server_test.go`. Every GET is timeout-bounded so a hung OVH API can't
//! stall the refresh thread — and thus an [`super::OvhcloudDiscovery`]
//! `Drop`/`stop` — indefinitely.

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::blocking::{Client as HttpClient, Response};
use sha1::{Digest, Sha1};

use super::common::parse_ip_list;
use super::dedicated_server::DedicatedServer;
use super::vps::VirtualPrivateServer;
use super::OvhcloudSdConfig;
pub use crate::scrape::config::ScrapeError;

/// Per-request client-side timeout. A refresh issues a list GET then, per
/// instance, one detail GET and one `/ips` GET; each is capped so a hung OVH
/// API can't stall the refresh thread. Mirrors the DigitalOcean/Hetzner client.
const OVH_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// `endpoint` name → API base URL. Port of `api.go`'s `availableEndpoints`.
pub(crate) fn endpoint_base_url(endpoint: &str) -> Option<&'static str> {
    match endpoint {
        "ovh-eu" => Some("https://eu.api.ovh.com/1.0"),
        "ovh-ca" => Some("https://ca.api.ovh.com/1.0"),
        "ovh-us" => Some("https://api.us.ovhcloud.com/1.0"),
        "kimsufi-eu" => Some("https://eu.api.kimsufi.com/1.0"),
        "kimsufi-ca" => Some("https://ca.api.kimsufi.com/1.0"),
        "soyoustart-eu" => Some("https://eu.api.soyoustart.com/1.0"),
        "soyoustart-ca" => Some("https://ca.api.soyoustart.com/1.0"),
        _ => None,
    }
}

/// Resolved OVHcloud API access: base URL, an HTTP client, and the signing
/// material. `time_delta` caches the learned `now - serverTime` offset.
///
/// `Debug` is hand-written to redact the application secret and consumer key.
/// The application key is not secret (it is the public `X-Ovh-Application`
/// identifier) and is shown, mirroring how `ConsulApi`/`HetznerApi` surface the
/// basic username but never the password.
pub struct OvhcloudApi {
    base_url: String,
    http: HttpClient,
    application_key: String,
    application_secret: String,
    consumer_key: String,
    time_delta: Mutex<Option<i64>>,
}

impl std::fmt::Debug for OvhcloudApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OvhcloudApi")
            .field("base_url", &self.base_url)
            .field("application_key", &self.application_key)
            .field("application_secret", &"<redacted>")
            .field("consumer_key", &"<redacted>")
            .finish()
    }
}

/// Builds an [`OvhcloudApi`] from `cfg`, mirroring `newAPIConfig`. The base URL
/// is `cfg.api_url_override` when set (tests), else the `endpoint`'s mapped URL.
///
/// Fails on a genuinely bad config (unknown `endpoint`) — never because the OVH
/// API is unreachable; listing happens later on the refresh thread.
pub fn new_ovhcloud_api(cfg: &OvhcloudSdConfig) -> Result<OvhcloudApi, ScrapeError> {
    let base_url = if cfg.api_url_override.is_empty() {
        endpoint_base_url(&cfg.endpoint)
            .ok_or_else(|| {
                ScrapeError::new(format!(
                    "unsupported `endpoint` for ovhcloud sd: {}",
                    cfg.endpoint
                ))
            })?
            .to_string()
    } else {
        cfg.api_url_override.trim_end_matches('/').to_string()
    };

    let http = HttpClient::builder().build().map_err(|e| ScrapeError {
        msg: format!("cannot build ovhcloud http client: {e}"),
    })?;

    Ok(OvhcloudApi {
        base_url,
        http,
        application_key: cfg.application_key.clone(),
        application_secret: cfg.application_secret.clone(),
        consumer_key: cfg.consumer_key.clone(),
        time_delta: Mutex::new(None),
    })
}

/// Computes the OVH `X-Ovh-Signature` value. Port of `common.go`'s
/// `getAuthHeaders` signature line: `"$1$" + sha1_hex(<fields>)` where the
/// fields are `application_secret`, `consumer_key`, HTTP method, the full URL
/// (`endpoint + path`), the request body, and the timestamp — joined by literal
/// `+` (Go's `fmt.Fprintf(h, "%s+%s+%s+%s+%s+%d", ...)`). The body is empty for
/// every SD request, so a `++` appears between the URL and timestamp.
fn ovh_signature(
    application_secret: &str,
    consumer_key: &str,
    method: &str,
    full_url: &str,
    body: &str,
    timestamp: i64,
) -> String {
    let payload =
        format!("{application_secret}+{consumer_key}+{method}+{full_url}+{body}+{timestamp}");
    let mut hasher = Sha1::new();
    hasher.update(payload.as_bytes());
    format!("$1${}", to_hex(&hasher.finalize()))
}

/// Lowercase hex of `bytes`. Local copy of the `scrape::ec2::sigv4` helper (a
/// few lines; avoids coupling the two SD modules).
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Seconds since the Unix epoch, saturating at 0 before the epoch.
fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl OvhcloudApi {
    /// The base URL, exposed for tests.
    #[cfg(test)]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Issues a GET with only the general headers (`X-Ovh-Application`,
    /// `Accept`, `User-Agent`) — used for the unauthenticated `/auth/time`
    /// call. Port of `setGeneralHeaders`.
    fn get_unsigned(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let url = format!("{}{path}", self.base_url);
        let req = self
            .http
            .get(&url)
            .timeout(OVH_HTTP_TIMEOUT)
            .header("X-Ovh-Application", &self.application_key)
            .header("Accept", "application/json")
            .header("User-Agent", "esmetrics/esmagent");
        Self::send(req, path)
    }

    /// Issues a GET with the full signed OVH header set. Port of `getAuthHeaders`.
    fn get_signed(&self, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let timestamp = self.ovh_timestamp();
        let full_url = format!("{}{path}", self.base_url);
        let signature = ovh_signature(
            &self.application_secret,
            &self.consumer_key,
            "GET",
            &full_url,
            "",
            timestamp,
        );
        let req = self
            .http
            .get(&full_url)
            .timeout(OVH_HTTP_TIMEOUT)
            .header("X-Ovh-Application", &self.application_key)
            .header("Accept", "application/json")
            .header("User-Agent", "esmetrics/esmagent")
            .header("X-Ovh-Timestamp", timestamp.to_string())
            .header("X-Ovh-Consumer", &self.consumer_key)
            .header("X-Ovh-Signature", signature);
        Self::send(req, path)
    }

    /// Sends `req`, returning the body bytes on a 2xx status.
    fn send(req: reqwest::blocking::RequestBuilder, path: &str) -> Result<Vec<u8>, ScrapeError> {
        let resp: Response = req.send().map_err(|e| ScrapeError {
            msg: format!("ovhcloud request to {path:?} failed: {e}"),
        })?;
        let status = resp.status();
        let body = resp.bytes().map_err(|e| ScrapeError {
            msg: format!("ovhcloud response from {path:?}: {e}"),
        })?;
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!("ovhcloud request to {path:?} failed: status {status}"),
            });
        }
        Ok(body.to_vec())
    }

    /// Returns the OVH server timestamp (`now - timeDelta`). Port of
    /// `getOVHTimestamp`: learns `timeDelta` from `/auth/time` on first use and
    /// caches it; on failure it warns and falls back to the local `now`.
    fn ovh_timestamp(&self) -> i64 {
        {
            let guard = self.time_delta.lock().unwrap();
            if let Some(delta) = *guard {
                return now_unix() - delta;
            }
        }
        match self.server_time() {
            Ok(server_time) => {
                let delta = now_unix() - server_time;
                *self.time_delta.lock().unwrap() = Some(delta);
                now_unix() - delta
            }
            Err(e) => {
                log::warn!("cannot get OVH server time, using current timestamp: {e}");
                now_unix()
            }
        }
    }

    /// Fetches the OVH server clock from `/auth/time` (a plain integer body).
    /// Port of `getServerTime`.
    fn server_time(&self) -> Result<i64, ScrapeError> {
        let body = self.get_unsigned("/auth/time").map_err(|e| ScrapeError {
            msg: format!("failed to get server time from /auth/time: {}", e.msg),
        })?;
        let text = String::from_utf8_lossy(&body);
        text.trim().parse::<i64>().map_err(|e| ScrapeError {
            msg: format!("cannot parse ovh /auth/time response {text:?}: {e}"),
        })
    }

    /// Lists VPS service names (`GET /vps`). Port of `getVPSList`.
    pub fn list_vps(&self) -> Result<Vec<String>, ScrapeError> {
        let data = self.get_signed("/vps")?;
        parse_string_list(&data, "/vps")
    }

    /// Fetches one VPS's detail (`GET /vps/<name>`) and its IPs
    /// (`GET /vps/<name>/ips`), attaching the parsed IPs. Port of `getVPSDetails`.
    pub fn get_vps_details(&self, name: &str) -> Result<VirtualPrivateServer, ScrapeError> {
        let detail_path = format!("/vps/{name}");
        let detail = self.get_signed(&detail_path)?;
        let mut vps: VirtualPrivateServer =
            serde_json::from_slice(&detail).map_err(|e| ScrapeError {
                msg: format!("cannot unmarshal response from {detail_path:?}: {e}"),
            })?;
        vps.ips = self.fetch_ips(&format!("{detail_path}/ips"))?;
        Ok(vps)
    }

    /// Lists dedicated-server service names (`GET /dedicated/server`). Port of
    /// `getDedicatedServerList`.
    pub fn list_dedicated_servers(&self) -> Result<Vec<String>, ScrapeError> {
        let data = self.get_signed("/dedicated/server")?;
        parse_string_list(&data, "/dedicated/server")
    }

    /// Fetches one dedicated server's detail (`GET /dedicated/server/<name>`)
    /// and its IPs (`.../ips`), attaching the parsed IPs. Port of
    /// `getDedicatedServerDetails`.
    pub fn get_dedicated_server_details(&self, name: &str) -> Result<DedicatedServer, ScrapeError> {
        let detail_path = format!("/dedicated/server/{name}");
        let detail = self.get_signed(&detail_path)?;
        let mut server: DedicatedServer =
            serde_json::from_slice(&detail).map_err(|e| ScrapeError {
                msg: format!("cannot unmarshal response from {detail_path:?}: {e}"),
            })?;
        server.ips = self.fetch_ips(&format!("{detail_path}/ips"))?;
        Ok(server)
    }

    /// GETs a `.../ips` endpoint (a JSON array of address/CIDR strings) and
    /// parses it via [`parse_ip_list`].
    fn fetch_ips(&self, path: &str) -> Result<Vec<std::net::IpAddr>, ScrapeError> {
        let data = self.get_signed(path)?;
        let ips: Vec<String> = serde_json::from_slice(&data).map_err(|e| ScrapeError {
            msg: format!("cannot unmarshal response from {path:?}: {e}"),
        })?;
        parse_ip_list(&ips)
    }
}

/// Parses a JSON array-of-strings body (the service listing shape).
fn parse_string_list(data: &[u8], path: &str) -> Result<Vec<String>, ScrapeError> {
    serde_json::from_slice(data).map_err(|e| ScrapeError {
        msg: format!("cannot unmarshal response from {path:?}: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> OvhcloudSdConfig {
        OvhcloudSdConfig {
            endpoint: "ovh-eu".into(),
            application_key: "app".into(),
            application_secret: "secret".into(),
            consumer_key: "consumer".into(),
            service: "vps".into(),
            ..OvhcloudSdConfig::default()
        }
    }

    /// Load-bearing correctness test: the `X-Ovh-Signature` for fixed inputs
    /// must equal `"$1$" + sha1_hex("<secret>+<consumer>+GET+<url>++<ts>")`.
    /// The expected hex is precomputed (Python `hashlib.sha1`) over exactly that
    /// string, pinning both the field order/joiner and the empty-body `++`.
    #[test]
    fn signature_matches_precomputed_sha1() {
        let sig = ovh_signature(
            "secret",
            "consumer",
            "GET",
            "https://eu.api.ovh.com/1.0/vps",
            "",
            1_700_000_000,
        );
        assert_eq!(sig, "$1$0289d03f93f99aafa310a3b5451763ed782abe64");
    }

    /// The signed payload joins the six fields with `+` and leaves a `++` where
    /// the empty body sits — asserted indirectly by reproducing the digest the
    /// signer must have hashed.
    #[test]
    fn signature_uses_plus_joined_fields_with_empty_body() {
        let expected = {
            let payload = "s+c+GET+https://x/vps++42";
            let mut h = Sha1::new();
            h.update(payload.as_bytes());
            format!("$1${}", to_hex(&h.finalize()))
        };
        let sig = ovh_signature("s", "c", "GET", "https://x/vps", "", 42);
        assert_eq!(sig, expected);
    }

    #[test]
    fn endpoint_maps_to_base_url() {
        let api = new_ovhcloud_api(&cfg()).unwrap();
        assert_eq!(api.base_url(), "https://eu.api.ovh.com/1.0");
    }

    #[test]
    fn unknown_endpoint_is_rejected() {
        let mut c = cfg();
        c.endpoint = "nope".into();
        let err = new_ovhcloud_api(&c).unwrap_err();
        assert!(err.msg.contains("endpoint"), "{}", err.msg);
    }

    #[test]
    fn override_wins_over_endpoint_and_strips_trailing_slash() {
        let mut c = cfg();
        c.api_url_override = "http://127.0.0.1:8080/".into();
        let api = new_ovhcloud_api(&c).unwrap();
        assert_eq!(api.base_url(), "http://127.0.0.1:8080");
    }

    #[test]
    fn secrets_are_redacted_in_debug() {
        let mut c = cfg();
        c.application_secret = "super-secret".into();
        c.consumer_key = "consumer-secret".into();
        let api = new_ovhcloud_api(&c).unwrap();
        let dbg = format!("{api:?}");
        assert!(!dbg.contains("super-secret"), "{dbg}");
        assert!(!dbg.contains("consumer-secret"), "{dbg}");
        // The application key is the public identifier and may show.
        assert!(dbg.contains("app"), "{dbg}");
    }
}
