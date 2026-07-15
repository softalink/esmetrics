//! Blocking Prometheus-API datasource client. Port of `datasource.Client`
//! (`client.go`) and its Prometheus instant/range request builders
//! (`client_prom.go:255-295`), narrowed to what esmalert's rule evaluator
//! needs. Uses `reqwest::blocking` (rustls-tls backend, matching
//! `esmauth`'s workspace dependency) — no tokio.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::blocking::Client;
use url::Url;

use crate::config::Header;

use super::auth::{AuthConfig, TlsConfig};
use super::prom_json::parse_prom_response;
use super::{DsError, QueryResult};

const API_QUERY_PATH: &str = "/api/v1/query";

/// Default per-request round-trip timeout for datasource queries. Without a
/// timeout, a connected-but-stalled datasource blocks a group-eval thread
/// inside [`Datasource::query`] forever, so `Manager::shutdown`'s
/// group-thread join (and `Manager::reload`, which holds the manager mutex
/// across those joins) would hang. Mirrors the remote-write client's
/// `DEFAULT_SEND_TIMEOUT` convention.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
// Not called by `main`'s wiring (Task 19): the rule evaluator and the
// `query` template builtin both only ever issue instant queries. Kept for
// API completeness/future range-query use.
#[allow(dead_code)]
const API_QUERY_RANGE_PATH: &str = "/api/v1/query_range";

/// A blocking Prometheus-API datasource client.
pub struct Datasource {
    client: Client,
    base_url: Url,
    auth: AuthConfig,
    /// Per-group extra URL query params (`Group::params`); take priority
    /// over any same-named param already set, matching upstream
    /// `setReqParams`.
    params: BTreeMap<String, Vec<String>>,
    /// Per-group extra headers (`Group::headers`), applied to every request.
    headers: Vec<Header>,
    eval_interval: Duration,
}

impl Datasource {
    pub fn new(
        url: &str,
        auth: AuthConfig,
        tls: TlsConfig,
        params: BTreeMap<String, Vec<String>>,
        headers: Vec<Header>,
        eval_interval: Duration,
        timeout: Duration,
    ) -> Result<Self, DsError> {
        let base_url = Url::parse(url)
            .map_err(|e| DsError::new(format!("invalid datasource url {url:?}: {e}")))?;
        let client = build_client(&tls, timeout)?;
        Ok(Datasource {
            client,
            base_url,
            auth,
            params,
            headers,
            eval_interval,
        })
    }

    /// Instant query: `GET <url>/api/v1/query?query=<expr>&time=<ts>[&step=<step>]`.
    /// `ts` is unix millis; formatted as RFC3339 per upstream.
    pub fn query(&self, expr: &str, ts: i64) -> Result<QueryResult, DsError> {
        let mut base = vec![("time".to_string(), rfc3339_millis(ts))];
        if let Some(step) = self.step_param() {
            base.push(("step".to_string(), step));
        }
        let url = self.build_url(API_QUERY_PATH, base, expr);
        let body = self.execute(url)?;
        parse_prom_response(&body)
    }

    /// Range query: `GET <url>/api/v1/query_range?query=<expr>&start=<from>&end=<to>[&step=<step>]`.
    /// `from`/`to` are unix millis; formatted as RFC3339 per upstream.
    ///
    /// Not called by `main`'s wiring (Task 19): the rule evaluator and the
    /// `query` template builtin both only ever issue instant queries.
    #[allow(dead_code)]
    pub fn query_range(&self, expr: &str, from: i64, to: i64) -> Result<QueryResult, DsError> {
        let mut base = vec![
            ("start".to_string(), rfc3339_millis(from)),
            ("end".to_string(), rfc3339_millis(to)),
        ];
        if let Some(step) = self.step_param() {
            base.push(("step".to_string(), step));
        }
        let url = self.build_url(API_QUERY_RANGE_PATH, base, expr);
        let body = self.execute(url)?;
        parse_prom_response(&body)
    }

    /// Appends `suffix` to the base URL's existing path (Go: `r.URL.Path +=
    /// suffix`), then sets the query string to `base` plus the group's
    /// extra params (priority over same-named entries in `base`) plus
    /// `query=<expr>` last, mirroring `setReqParams`'s precedence.
    fn build_url(&self, suffix: &str, mut base: Vec<(String, String)>, expr: &str) -> Url {
        let mut url = self.base_url.clone();
        let existing_path = url.path().trim_end_matches('/').to_string();
        url.set_path(&format!("{existing_path}{suffix}"));

        for (k, vs) in &self.params {
            base.retain(|(bk, _)| bk != k);
            for v in vs {
                base.push((k.clone(), v.clone()));
            }
        }
        base.push(("query".to_string(), expr.to_string()));

        url.query_pairs_mut().clear().extend_pairs(&base);
        url
    }

    fn step_param(&self) -> Option<String> {
        (!self.eval_interval.is_zero()).then(|| format!("{}s", self.eval_interval.as_secs()))
    }

    fn execute(&self, url: Url) -> Result<Vec<u8>, DsError> {
        let mut req = self.client.get(url);
        for h in &self.headers {
            req = req.header(h.key.as_str(), h.value.as_str());
        }
        if let Some((user, pass)) = &self.auth.basic {
            req = req.basic_auth(user, Some(pass));
        } else if let Some(token) = &self.auth.bearer {
            req = req.bearer_auth(token);
        }
        let resp = req.send()?;
        let status = resp.status();
        let body = resp.bytes()?.to_vec();
        if !status.is_success() {
            return Err(DsError::new(format!("unexpected response status {status}")));
        }
        Ok(body)
    }
}

fn build_client(tls: &TlsConfig, timeout: Duration) -> Result<Client, DsError> {
    let mut builder = Client::builder().timeout(timeout);
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &tls.ca_file {
        let pem = std::fs::read(ca_file)
            .map_err(|e| DsError::new(format!("cannot read CA file {ca_file:?}: {e}")))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| DsError::new(format!("invalid CA certificate in {ca_file:?}: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        let mut identity_pem = std::fs::read(cert_file)
            .map_err(|e| DsError::new(format!("cannot read cert file {cert_file:?}: {e}")))?;
        let mut key_pem = std::fs::read(key_file)
            .map_err(|e| DsError::new(format!("cannot read key file {key_file:?}: {e}")))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| DsError::new(format!("invalid client cert/key: {e}")))?;
        builder = builder.identity(identity);
    }
    // `tls.server_name` (SNI/hostname override independent of the request
    // URL's host) has no direct equivalent in reqwest's blocking
    // `ClientBuilder`; not wired here — see task report for the gap.
    builder
        .build()
        .map_err(|e| DsError::new(format!("cannot build http client: {e}")))
}

/// Formats a Unix-milliseconds timestamp as RFC3339 seconds-precision UTC
/// (Go's `time.RFC3339` has no fractional seconds), matching what upstream
/// vmalert sends for `time`/`start`/`end`. No chrono dependency: the same
/// civil-date algorithm (Howard Hinnant's `civil_from_days`) as
/// `esm-backup::timeutil::rfc3339_from_unix`, duplicated locally rather than
/// depending on the unrelated backup crate for one helper.
fn rfc3339_millis(ts_ms: i64) -> String {
    let unix_secs = ts_ms.div_euclid(1000).max(0) as u64;
    let (y, mo, d, h, mi, s) = civil_from_unix(unix_secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn civil_from_unix(unix_secs: u64) -> (i64, u64, u64, u64, u64, u64) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, d, h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use esm_http::{Request, ResponseWriter, Server, ServerConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn rfc3339_formats_known_millis_timestamp() {
        assert_eq!(rfc3339_millis(1_700_000_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(rfc3339_millis(0), "1970-01-01T00:00:00Z");
    }

    const CANNED_VECTOR_BODY: &str = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"h1"},"value":[1700000000,"1"]}]}}"#;

    fn stub_server(hits: Arc<AtomicUsize>) -> Server {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                hits.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.path(), "/api/v1/query");
                let params: Vec<_> = req.query_params().collect();
                assert!(
                    params.iter().any(|(k, v)| k == "query" && v == "up"),
                    "missing query=up in {params:?}"
                );
                assert!(
                    params.iter().any(|(k, _)| k == "time"),
                    "missing time param in {params:?}"
                );
                w.write_json(200, CANNED_VECTOR_BODY);
            },
        ));
        server
    }

    #[test]
    fn query_round_trips_against_stub_server() {
        let hits = Arc::new(AtomicUsize::new(0));
        let server = stub_server(Arc::clone(&hits));
        let addr = server.local_addr();

        let ds = Datasource::new(
            &format!("http://{addr}"),
            AuthConfig::default(),
            TlsConfig::default(),
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(15),
            DEFAULT_QUERY_TIMEOUT,
        )
        .expect("build datasource");

        let result = ds.query("up", 1_700_000_000_000).expect("query failed");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].values, vec![1.0]);
        assert!(result.data[0]
            .labels
            .iter()
            .any(|(k, v)| k == "instance" && v == "h1"));

        server.stop();
    }

    #[test]
    fn query_range_hits_range_path_with_start_end() {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        let addr = server.local_addr();
        server.serve(Arc::new(|req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            assert_eq!(req.path(), "/api/v1/query_range");
            let params: Vec<_> = req.query_params().collect();
            assert!(params.iter().any(|(k, _)| k == "start"));
            assert!(params.iter().any(|(k, _)| k == "end"));
            let body = r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1000,"1"]]}]}}"#;
            w.write_json(200, body);
        }));

        let ds = Datasource::new(
            &format!("http://{addr}"),
            AuthConfig::default(),
            TlsConfig::default(),
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(0),
            DEFAULT_QUERY_TIMEOUT,
        )
        .expect("build datasource");

        let result = ds
            .query_range("up", 1_700_000_000_000, 1_700_000_060_000)
            .expect("query_range failed");
        assert_eq!(result.data.len(), 1);

        server.stop();
    }

    #[test]
    fn group_params_and_headers_reach_the_server() {
        let server = Server::bind_with_config(
            "127.0.0.1:0",
            ServerConfig {
                capture_all_headers: true,
                ..ServerConfig::default()
            },
        )
        .expect("bind stub server");
        let addr = server.local_addr();
        server.serve(Arc::new(
            |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                let params: Vec<_> = req.query_params().collect();
                assert!(params.iter().any(|(k, v)| k == "extra" && v == "yes"));
                assert!(
                    req.all_headers()
                        .iter()
                        .any(|(k, v)| k.eq_ignore_ascii_case("x-custom") && v == "hdr"),
                    "missing custom header in {:?}",
                    req.all_headers()
                );
                w.write_json(200, CANNED_VECTOR_BODY);
            },
        ));

        let mut params = BTreeMap::new();
        params.insert("extra".to_string(), vec!["yes".to_string()]);
        let headers = vec![Header {
            key: "X-Custom".to_string(),
            value: "hdr".to_string(),
        }];

        let ds = Datasource::new(
            &format!("http://{addr}"),
            AuthConfig::default(),
            TlsConfig::default(),
            params,
            headers,
            Duration::from_secs(0),
            DEFAULT_QUERY_TIMEOUT,
        )
        .expect("build datasource");

        ds.query("up", 0).expect("query failed");
        server.stop();
    }

    #[test]
    fn non_success_status_is_an_error() {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        let addr = server.local_addr();
        server.serve(Arc::new(
            |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                w.write_json(500, r#"{"error":"boom"}"#);
            },
        ));

        let ds = Datasource::new(
            &format!("http://{addr}"),
            AuthConfig::default(),
            TlsConfig::default(),
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(0),
            DEFAULT_QUERY_TIMEOUT,
        )
        .expect("build datasource");

        assert!(ds.query("up", 0).is_err());
        server.stop();
    }

    #[test]
    fn query_does_not_hang_when_endpoint_never_responds() {
        // A bound-but-never-accepted TCP listener: the kernel completes the
        // handshake into the accept backlog and buffers the request bytes,
        // but no response ever comes — the stalled-datasource case the
        // request timeout must guard against. Without `.timeout()` on the
        // client, `req.send()` would block the group-eval thread forever,
        // and `Manager::shutdown`'s group-thread join would deadlock. The
        // listener is kept alive (bound to `_listener`) for the whole test.
        use std::net::TcpListener;
        use std::time::Instant;

        let _listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled listener");
        let addr = _listener.local_addr().expect("local addr");

        let short_timeout = Duration::from_millis(300);
        let ds = Datasource::new(
            &format!("http://{addr}"),
            AuthConfig::default(),
            TlsConfig::default(),
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(0),
            short_timeout,
        )
        .expect("build datasource");

        let start = Instant::now();
        let result = ds.query("up", 0);
        assert!(
            result.is_err(),
            "query against a never-responding endpoint should error, got {result:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "query hung despite request timeout: took {:?}",
            start.elapsed()
        );
    }
}
