//! OAuth2 client-credentials authentication for Kubernetes service
//! discovery's API-server client.
//!
//! Port of `lib/promauth/config.go`'s `OAuth2Config` (upstream v1.146.0): a
//! standard OAuth2 *client-credentials* grant. [`new_token_source`] validates
//! the config and builds a dedicated `reqwest::blocking` client (from the
//! token endpoint's own `tls_config`/`proxy_url`); [`OAuth2TokenSource::token`]
//! POSTs `grant_type=client_credentials` (+ `client_id`/`client_secret`/
//! `scope`/`endpoint_params`) to `token_url`, parses `{access_token,
//! expires_in, token_type}`, and caches the token until shortly before it
//! expires. The API client attaches it as `Authorization: Bearer
//! <access_token>` (see [`super::client::ApiConfig`]).
//!
//! ## Deviations from upstream
//!
//! - **Body-only credentials (`AuthStyleInParams`).** `client_id`/
//!   `client_secret` are sent in the POST body. Upstream's golang.org/x/oauth2
//!   autodetects the endpoint's preferred style (retrying with HTTP Basic on
//!   the first request); that fallback is not ported.
//! - **`headers` for the token request is not ported** (a minor deferral).
//!
//! Secrets (`client_secret`) and the fetched `access_token` are never logged
//! or formatted into error messages.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;

use crate::client::TlsConfig;
use crate::scrape::config::ScrapeError;
use crate::scrape::kubernetes::client::build_client;

/// Seconds shaved off the server-reported `expires_in` so a token is
/// refreshed slightly early, avoiding a race where a request is sent with a
/// token that expires in transit.
const SAFETY_MARGIN_SECS: u64 = 10;

/// Bounds the token-endpoint POST in [`OAuth2TokenSource::fetch`]. Without
/// this, an unresponsive IdP (blackholed TCP, no response) would hang the
/// request forever; matches this crate's other fixed HTTP timeouts (e.g.
/// `LIST_HTTP_TIMEOUT`, `REMOTE_WRITE_SEND_TIMEOUT`).
const OAUTH2_TOKEN_TIMEOUT: Duration = Duration::from_secs(30);

/// OAuth2 client-credentials configuration. `client_secret` and
/// `client_secret_file` are mutually exclusive (exactly one required); see
/// [`validate`].
///
/// `Debug` is hand-written (below) rather than derived: it must never print
/// `client_secret` — this crate requires secret-holding structs to redact
/// their `Debug`, matching [`super::client::ApiConfig`] and
/// [`super::kubeconfig`].
#[derive(Clone, Default, PartialEq)]
pub struct OAuth2Config {
    pub client_id: String,
    pub client_secret: Option<String>,
    pub client_secret_file: Option<String>,
    pub scopes: Vec<String>,
    pub token_url: String,
    pub endpoint_params: BTreeMap<String, String>,
    /// TLS for the *token endpoint* client (independent of the API server's).
    pub tls: TlsConfig,
    /// HTTP proxy for the *token endpoint* client (independent of the API
    /// server's `proxy_url`).
    pub proxy_url: Option<String>,
}

/// Redacts `client_secret` (shows only whether it is set); `client_id`,
/// `client_secret_file` (a path), `scopes`, `token_url`, `endpoint_params`,
/// and `proxy_url` are not secret and are printed as-is.
impl std::fmt::Debug for OAuth2Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2Config")
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("client_secret_file", &self.client_secret_file)
            .field("scopes", &self.scopes)
            .field("token_url", &self.token_url)
            .field("endpoint_params", &self.endpoint_params)
            .field("tls", &self.tls)
            .field("proxy_url", &self.proxy_url)
            .finish()
    }
}

/// Validates `cfg`, mirroring upstream `OAuth2Config.validate` wording:
/// `client_id` non-empty; exactly one of `client_secret`/`client_secret_file`;
/// `token_url` non-empty.
pub fn validate(cfg: &OAuth2Config) -> Result<(), ScrapeError> {
    if cfg.client_id.is_empty() {
        return Err(ScrapeError {
            msg: "client_id cannot be empty".to_string(),
        });
    }
    match (&cfg.client_secret, &cfg.client_secret_file) {
        (None, None) => {
            return Err(ScrapeError {
                msg: "ClientSecret or ClientSecretFile must be set".to_string(),
            });
        }
        (Some(_), Some(_)) => {
            return Err(ScrapeError {
                msg: "ClientSecret and ClientSecretFile cannot be set simultaneously".to_string(),
            });
        }
        _ => {}
    }
    if cfg.token_url.is_empty() {
        return Err(ScrapeError {
            msg: "token_url cannot be empty".to_string(),
        });
    }
    Ok(())
}

/// A cached access token and the monotonic instant it should be considered
/// expired at (already reduced by [`SAFETY_MARGIN_SECS`]). [`Instant`] is used
/// deliberately — a monotonic clock isn't affected by wall-clock adjustments.
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// A token source for one [`OAuth2Config`]: owns a dedicated blocking HTTP
/// client for the token endpoint and a mutex-guarded cache. Cloning the outer
/// `Arc<OAuth2TokenSource>` (as [`super::client::ApiConfig`] does across
/// watcher threads) shares one cache. The mutex is only held to read/write
/// the cache, never across the network fetch (see [`OAuth2TokenSource::token`]),
/// so a concurrent cache miss on multiple threads may issue redundant fetches
/// rather than serializing behind one refresh; each fetch is also bounded by
/// [`OAUTH2_TOKEN_TIMEOUT`], so an unresponsive IdP can't stall a watcher
/// thread indefinitely.
pub struct OAuth2TokenSource {
    http: HttpClient,
    cfg: OAuth2Config,
    cache: Mutex<Option<CachedToken>>,
}

/// Redacts everything sensitive: `cfg`'s own `Debug` hides `client_secret`,
/// and the cache is shown as presence-only (never the `access_token`).
impl std::fmt::Debug for OAuth2TokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cached = self.cache.try_lock().map(|g| g.is_some()).unwrap_or(false);
        f.debug_struct("OAuth2TokenSource")
            .field("cfg", &self.cfg)
            .field("cached", &cached)
            .finish()
    }
}

/// Validates `cfg` and builds an [`OAuth2TokenSource`]. The token endpoint's
/// HTTP client is built from `cfg.tls` + `cfg.proxy_url` (reusing
/// [`super::client::build_client`]); no network call is made here — the first
/// token is fetched lazily on the first [`OAuth2TokenSource::token`] call.
pub fn new_token_source(cfg: &OAuth2Config) -> Result<OAuth2TokenSource, ScrapeError> {
    validate(cfg)?;
    let http = build_client(&cfg.tls, cfg.proxy_url.as_deref())?;
    Ok(OAuth2TokenSource {
        http,
        cfg: cfg.clone(),
        cache: Mutex::new(None),
    })
}

/// The subset of an OAuth2 token response this port consumes.
#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    expires_in: Option<u64>,
    #[allow(dead_code)]
    token_type: Option<String>,
}

impl OAuth2TokenSource {
    /// Returns a valid access token, fetching a fresh one only when the cache
    /// is empty or the cached token has reached its (safety-margin-adjusted)
    /// expiry. The cache mutex is held only to read and to store the result —
    /// never across the network fetch itself (bounded by
    /// [`OAUTH2_TOKEN_TIMEOUT`]) — so a hung IdP cannot block other watcher
    /// threads that only need to read the cache. A concurrent cache miss on
    /// multiple threads may cause redundant fetches; the last writer's result
    /// is cached, which is harmless.
    pub fn token(&self) -> Result<String, ScrapeError> {
        {
            let guard = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = guard.as_ref() {
                if Instant::now() < cached.expires_at {
                    return Ok(cached.access_token.clone());
                }
            }
        }
        let (access_token, expires_in) = self.fetch()?;
        // `expires_in == 0` (absent or zero) => treat as non-cacheable: set
        // `expires_at` to now so the very next call re-fetches.
        let expires_at = if expires_in == 0 {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(expires_in.saturating_sub(SAFETY_MARGIN_SECS))
        };
        let mut guard = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(CachedToken {
            access_token: access_token.clone(),
            expires_at,
        });
        Ok(access_token)
    }

    /// Performs the token POST and returns `(access_token, expires_in)`. Never
    /// logs `client_secret` or the returned `access_token`.
    fn fetch(&self) -> Result<(String, u64), ScrapeError> {
        let client_secret = self.resolve_client_secret()?;
        let mut params: Vec<(String, String)> = vec![
            ("grant_type".to_string(), "client_credentials".to_string()),
            ("client_id".to_string(), self.cfg.client_id.clone()),
            ("client_secret".to_string(), client_secret),
        ];
        if !self.cfg.scopes.is_empty() {
            params.push(("scope".to_string(), self.cfg.scopes.join(" ")));
        }
        for (k, v) in &self.cfg.endpoint_params {
            params.push((k.clone(), v.clone()));
        }

        let resp = self
            .http
            .post(&self.cfg.token_url)
            .timeout(OAUTH2_TOKEN_TIMEOUT)
            .form(&params)
            .send()
            .map_err(|e| ScrapeError {
                msg: format!("oauth2 token request to token_url failed: {e}"),
            })?;

        let status = resp.status();
        if !status.is_success() {
            return Err(ScrapeError {
                msg: format!(
                    "oauth2 token endpoint returned non-success status {}",
                    status.as_u16()
                ),
            });
        }

        let body = resp.text().map_err(|e| ScrapeError {
            msg: format!("cannot read oauth2 token response body: {e}"),
        })?;
        // The parse error names the JSON structure problem, not the token
        // value; a well-formed response with a present `access_token` parses
        // cleanly, so this never surfaces the secret.
        let parsed: TokenResponse = serde_json::from_str(&body).map_err(|e| ScrapeError {
            msg: format!("cannot parse oauth2 token response: {e}"),
        })?;
        if parsed.access_token.is_empty() {
            return Err(ScrapeError {
                msg: "oauth2 token response is missing access_token".to_string(),
            });
        }
        Ok((parsed.access_token, parsed.expires_in.unwrap_or(0)))
    }

    /// Resolves the client secret: the inline value if set, else the
    /// `client_secret_file`'s contents read fresh (matching upstream's lazy
    /// per-request file read). Errors name only the file path, never the
    /// secret.
    fn resolve_client_secret(&self) -> Result<String, ScrapeError> {
        if let Some(s) = &self.cfg.client_secret {
            return Ok(s.clone());
        }
        if let Some(path) = &self.cfg.client_secret_file {
            return std::fs::read_to_string(path)
                .map(|s| s.trim().to_string())
                .map_err(|e| ScrapeError {
                    msg: format!("cannot read client_secret_file {path:?}: {e}"),
                });
        }
        // Unreachable: `validate` (run by `new_token_source`) guarantees one
        // of the two is set.
        Err(ScrapeError {
            msg: "ClientSecret or ClientSecretFile must be set".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use esm_http::{Request, ResponseWriter, Server};

    /// A running stub OAuth2 token endpoint. `hits` counts every received
    /// request (so a test can assert cache hits issue no HTTP call), and
    /// `last_body` records the most recent request's form body.
    struct TokenStub {
        server: Server,
        hits: Arc<AtomicUsize>,
        last_body: Arc<Mutex<String>>,
    }

    impl TokenStub {
        fn url(&self) -> String {
            format!("http://{}/token", self.server.local_addr())
        }
        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
        fn last_body(&self) -> String {
            self.last_body.lock().unwrap().clone()
        }
        fn stop(&self) {
            self.server.stop();
        }
    }

    /// Starts a stub token endpoint that answers every request with `status` +
    /// `json`, recording the request count and last form body.
    fn start_token_stub(status: u16, json: String) -> TokenStub {
        let server = Server::bind("127.0.0.1:0").expect("bind token stub");
        let hits = Arc::new(AtomicUsize::new(0));
        let last_body = Arc::new(Mutex::new(String::new()));
        let hits_h = Arc::clone(&hits);
        let body_h = Arc::clone(&last_body);
        server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                hits_h.fetch_add(1, Ordering::SeqCst);
                let mut buf = Vec::new();
                let _ = req.read_body_to(&mut buf, 1 << 20);
                *body_h.lock().unwrap() = String::from_utf8_lossy(&buf).to_string();
                w.write_json(status, &json);
            },
        ));
        TokenStub {
            server,
            hits,
            last_body,
        }
    }

    /// A minimal valid config (inline `client_secret`) pointed at `url`.
    fn cfg(url: &str) -> OAuth2Config {
        OAuth2Config {
            client_id: "id".to_string(),
            client_secret: Some("sec".to_string()),
            token_url: url.to_string(),
            ..OAuth2Config::default()
        }
    }

    #[test]
    fn token_fetches_and_returns_access_token() {
        let stub = start_token_stub(
            200,
            r#"{"access_token":"tok-1","expires_in":3600,"token_type":"Bearer"}"#.to_string(),
        );
        let ts = new_token_source(&cfg(&stub.url())).unwrap();
        assert_eq!(ts.token().unwrap(), "tok-1");
        assert_eq!(stub.hits(), 1);
        stub.stop();
    }

    #[test]
    fn token_is_cached_within_expiry_no_second_request() {
        let stub = start_token_stub(
            200,
            r#"{"access_token":"tok-1","expires_in":3600,"token_type":"Bearer"}"#.to_string(),
        );
        let ts = new_token_source(&cfg(&stub.url())).unwrap();
        let t1 = ts.token().unwrap();
        let t2 = ts.token().unwrap();
        assert_eq!(t1, "tok-1");
        assert_eq!(t2, "tok-1");
        // The second call must be served from cache: exactly one HTTP request.
        assert_eq!(stub.hits(), 1);
        stub.stop();
    }

    #[test]
    fn token_without_expires_in_is_refetched_each_call() {
        let stub = start_token_stub(200, r#"{"access_token":"tok-1"}"#.to_string());
        let ts = new_token_source(&cfg(&stub.url())).unwrap();
        assert_eq!(ts.token().unwrap(), "tok-1");
        assert_eq!(ts.token().unwrap(), "tok-1");
        assert_eq!(stub.hits(), 2);
        stub.stop();
    }

    #[test]
    fn non_success_status_is_an_error() {
        let stub = start_token_stub(401, r#"{"error":"invalid_client"}"#.to_string());
        let ts = new_token_source(&cfg(&stub.url())).unwrap();
        let err = ts.token().unwrap_err();
        assert!(err.msg.contains("401"), "{}", err.msg);
        stub.stop();
    }

    #[test]
    fn scopes_and_endpoint_params_appear_in_post_body() {
        let stub = start_token_stub(
            200,
            r#"{"access_token":"tok-1","expires_in":3600}"#.to_string(),
        );
        let mut c = cfg(&stub.url());
        c.scopes = vec!["a".to_string(), "b".to_string()];
        c.endpoint_params
            .insert("resource".to_string(), "r1".to_string());
        let ts = new_token_source(&c).unwrap();
        ts.token().unwrap();
        let body = stub.last_body();
        assert!(body.contains("grant_type=client_credentials"), "{body}");
        assert!(body.contains("client_id=id"), "{body}");
        // form-urlencoded joins scopes with a space -> "a+b".
        assert!(body.contains("scope=a"), "{body}");
        assert!(body.contains("resource=r1"), "{body}");
        stub.stop();
    }

    #[test]
    fn missing_access_token_is_an_error() {
        let stub = start_token_stub(200, r#"{"expires_in":3600}"#.to_string());
        let ts = new_token_source(&cfg(&stub.url())).unwrap();
        assert!(ts.token().is_err());
        stub.stop();
    }

    #[test]
    fn client_secret_file_is_read_at_fetch_time() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, "file-secret\n").unwrap();
        let stub = start_token_stub(
            200,
            r#"{"access_token":"tok-1","expires_in":3600}"#.to_string(),
        );
        let mut c = cfg(&stub.url());
        c.client_secret = None;
        c.client_secret_file = Some(secret.to_string_lossy().to_string());
        let ts = new_token_source(&c).unwrap();
        assert_eq!(ts.token().unwrap(), "tok-1");
        let body = stub.last_body();
        assert!(body.contains("client_secret=file-secret"), "{body}");
        stub.stop();
    }

    #[test]
    fn validate_rejects_bad_configs() {
        let base = cfg("https://idp/token");

        let mut empty_id = base.clone();
        empty_id.client_id = String::new();
        assert!(validate(&empty_id)
            .unwrap_err()
            .msg
            .contains("client_id cannot be empty"));

        let mut no_secret = base.clone();
        no_secret.client_secret = None;
        assert!(validate(&no_secret)
            .unwrap_err()
            .msg
            .contains("ClientSecret or ClientSecretFile must be set"));

        let mut both_secrets = base.clone();
        both_secrets.client_secret_file = Some("/f".to_string());
        assert!(validate(&both_secrets)
            .unwrap_err()
            .msg
            .contains("cannot be set simultaneously"));

        let mut empty_url = base.clone();
        empty_url.token_url = String::new();
        assert!(validate(&empty_url)
            .unwrap_err()
            .msg
            .contains("token_url cannot be empty"));
    }

    #[test]
    fn debug_redacts_client_secret() {
        let c = OAuth2Config {
            client_id: "id".to_string(),
            client_secret: Some("super-secret-xyz".to_string()),
            token_url: "https://idp/token".to_string(),
            ..OAuth2Config::default()
        };
        let d = format!("{c:?}");
        assert!(!d.contains("super-secret-xyz"), "{d}");
    }
}
