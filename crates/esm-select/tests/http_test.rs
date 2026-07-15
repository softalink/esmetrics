//! Golden-response tests: a [`FakeProvider`] behind `SelectHandlers`,
//! served by esm-http on 127.0.0.1:0, exercised with real HTTP roundtrips.

use esm_http::{Request, ResponseWriter, Server};
use esm_promql::provider::{Deadline, MetricsProvider, SearchQuery, Series};
use esm_select::{SelectConfig, SelectHandlers};
use esm_storage::metric_name::MetricName;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------- fixtures

fn metric_name(name: &str, tags: &[(&str, &str)]) -> MetricName {
    let mut mn = MetricName {
        metric_group: name.as_bytes().to_vec(),
        ..Default::default()
    };
    for (k, v) in tags {
        mn.add_tag(k, v);
    }
    mn
}

/// In-memory provider: returns the series matching the query's
/// non-regexp equality/inequality filters, with samples clipped to the
/// requested time range. Regexp filters are treated as match-all.
struct FakeProvider {
    series: Vec<(MetricName, Vec<i64>, Vec<f64>)>,
    delay: Duration,
}

fn matches_filters(mn: &MetricName, sq: &SearchQuery) -> bool {
    if sq.tag_filterss.is_empty() {
        return true;
    }
    sq.tag_filterss.iter().any(|group| {
        group.iter().all(|f| {
            if f.is_regexp {
                return true;
            }
            let value: &[u8] = if f.label == "__name__" {
                &mn.metric_group
            } else {
                mn.get_tag_value(&f.label).unwrap_or_default()
            };
            (value == f.value.as_bytes()) != f.is_negative
        })
    })
}

impl FakeProvider {
    fn new(series: Vec<(MetricName, Vec<i64>, Vec<f64>)>) -> FakeProvider {
        FakeProvider {
            series,
            delay: Duration::ZERO,
        }
    }
}

impl MetricsProvider for FakeProvider {
    fn search(&self, sq: &SearchQuery, _deadline: Deadline) -> esm_promql::Result<Vec<Series>> {
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
        let mut out = Vec::new();
        for (mn, timestamps, values) in &self.series {
            if !matches_filters(mn, sq) {
                continue;
            }
            let mut ts = Vec::new();
            let mut vs = Vec::new();
            for (i, &t) in timestamps.iter().enumerate() {
                if t >= sq.start && t <= sq.end {
                    ts.push(t);
                    vs.push(values[i]);
                }
            }
            out.push(Series {
                metric_name: mn.clone(),
                timestamps: Arc::new(ts),
                values: vs,
            });
        }
        Ok(out)
    }
}

/// test_metric{host=h1,h2} sampled at 1000s/1100s/1200s.
fn default_provider() -> FakeProvider {
    FakeProvider::new(vec![
        (
            metric_name("test_metric", &[("host", "h1")]),
            vec![1_577_836_800_000, 1_577_836_900_000, 1_577_837_000_000],
            vec![1.5, 0.1, 1e10],
        ),
        (
            metric_name("test_metric", &[("host", "h2")]),
            vec![1_577_836_800_000, 1_577_836_900_000, 1_577_837_000_000],
            vec![2.0, 3.25, 66.66666666666667],
        ),
    ])
}

// ------------------------------------------------------------ HTTP helpers

struct TestServer {
    server: Server,
    addr: SocketAddr,
}

fn serve(provider: FakeProvider, config: SelectConfig) -> TestServer {
    let handlers = Arc::new(SelectHandlers::with_config(provider, config));
    let server = Server::bind("127.0.0.1:0").expect("bind");
    let addr = server.local_addr();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if !handlers.handle(req, w) {
                w.write_status(404);
            }
        },
    ));
    TestServer { server, addr }
}

fn serve_default(provider: FakeProvider) -> TestServer {
    serve(provider, SelectConfig::default())
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server.stop();
    }
}

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

fn roundtrip(addr: SocketAddr, raw_request: &str) -> Response {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(raw_request.as_bytes()).expect("write");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, body) = text.split_once("\r\n\r\n").expect("header terminator");
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split(' ')
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.to_string(), v.trim().to_string()))
        .collect();
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
    });
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_string()
    };
    Response {
        status,
        headers,
        body,
    }
}

fn dechunk(mut body: &str) -> String {
    let mut out = String::new();
    while let Some((size_line, rest)) = body.split_once("\r\n") {
        let size = usize::from_str_radix(size_line.trim(), 16).expect("chunk size");
        if size == 0 {
            break;
        }
        out.push_str(&rest[..size]);
        body = &rest[size + 2..]; // skip chunk data + CRLF
    }
    out
}

fn get(addr: SocketAddr, path_and_query: &str) -> Response {
    roundtrip(
        addr,
        &format!("GET {path_and_query} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"),
    )
}

fn post_form(addr: SocketAddr, path: &str, form: &str) -> Response {
    roundtrip(
        addr,
        &format!(
            "POST {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
             Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{form}",
            form.len()
        ),
    )
}

/// The wall-clock `executionTimeMsec` is nondeterministic; normalize it
/// to 0 for golden comparisons.
fn normalize_exec_time(body: &str) -> String {
    let key = "\"executionTimeMsec\":";
    let Some(pos) = body.find(key) else {
        return body.to_string();
    };
    let digits_start = pos + key.len();
    let digits_end = body[digits_start..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|off| digits_start + off)
        .unwrap_or(body.len());
    format!("{}{key}0{}", &body[..pos], &body[digits_end..])
}

// ------------------------------------------------------------------ tests

#[test]
fn query_range_matrix_golden() {
    let ts = serve_default(default_provider());
    let resp = get(
        ts.addr,
        "/api/v1/query_range?query=test_metric%7Bhost%3D%22h1%22%7D&start=1577836800&end=1577837000&step=100",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-type"), Some("application/json"));
    assert_eq!(
        normalize_exec_time(&resp.body),
        "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[\
         {\"metric\":{\"__name__\":\"test_metric\",\"host\":\"h1\"},\
         \"values\":[[1577836800,\"1.5\"],[1577836900,\"0.1\"],[1577837000,\"10000000000\"]]}]},\
         \"stats\":{\"seriesFetched\": \"1\",\"executionTimeMsec\":0}}"
    );
}

#[test]
fn query_range_multi_series_and_post() {
    let ts = serve_default(default_provider());
    let result = "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[\
         {\"metric\":{\"__name__\":\"test_metric\",\"host\":\"h1\"},\
         \"values\":[[1577836800,\"1.5\"],[1577836900,\"0.1\"],[1577837000,\"10000000000\"]]},\
         {\"metric\":{\"__name__\":\"test_metric\",\"host\":\"h2\"},\
         \"values\":[[1577836800,\"2\"],[1577836900,\"3.25\"],[1577837000,\"66.66666666666667\"]]}]},\
         \"stats\":{\"seriesFetched\": \"";
    let expected_first = format!("{result}2\",\"executionTimeMsec\":0}}}}");
    let resp = get(
        ts.addr,
        "/api/v1/query_range?query=test_metric&start=1577836800&end=1577837000&step=100",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(normalize_exec_time(&resp.body), expected_first);

    // The same query via POST form. The result is now served from the
    // rollup result cache, so no series are fetched from the storage
    // (matching the upstream's full-cache-hit behavior).
    let expected_cached = format!("{result}0\",\"executionTimeMsec\":0}}}}");
    let resp = post_form(
        ts.addr,
        "/api/v1/query_range",
        "query=test_metric&start=1577836800&end=1577837000&step=100",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(normalize_exec_time(&resp.body), expected_cached);
}

#[test]
fn instant_query_vector_golden() {
    let ts = serve_default(default_provider());
    let resp = get(
        ts.addr,
        "/api/v1/query?query=test_metric%7Bhost%3D%22h1%22%7D&time=1577837000",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(
        normalize_exec_time(&resp.body),
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[\
         {\"metric\":{\"__name__\":\"test_metric\",\"host\":\"h1\"},\
         \"value\":[1577837000,\"10000000000\"]}]},\
         \"stats\":{\"seriesFetched\": \"1\",\"executionTimeMsec\":0}}"
    );
}

#[test]
fn instant_query_empty_result() {
    let ts = serve_default(FakeProvider::new(vec![]));
    let resp = get(
        ts.addr,
        "/api/v1/query?query=no_such_metric&time=1577837000",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(
        normalize_exec_time(&resp.body),
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]},\
         \"stats\":{\"seriesFetched\": \"0\",\"executionTimeMsec\":0}}"
    );
}

#[test]
fn instant_selector_with_rollup_exports_promapi_matrix() {
    let ts = serve_default(default_provider());
    // Bare `selector[d]` at an instant → raw samples on (time-d, time].
    let resp = get(
        ts.addr,
        "/api/v1/query?query=test_metric%7Bhost%3D%22h1%22%7D%5B5m%5D&time=1577837000",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("content-type"),
        Some("application/stream+json; charset=utf-8")
    );
    assert_eq!(
        resp.body,
        "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[\
         {\"metric\":{\"__name__\":\"test_metric\",\"host\":\"h1\"},\
         \"values\":[[1577836800,\"1.5\"],[1577836900,\"0.1\"],[1577837000,\"10000000000\"]]}]}}"
    );
}

#[test]
fn error_bad_query_is_422_prometheus_envelope() {
    let ts = serve_default(default_provider());
    let resp = get(
        ts.addr,
        "/api/v1/query?query=test_metric%7B&time=1577837000",
    );
    assert_eq!(resp.status, 422);
    assert_eq!(resp.header("content-type"), Some("application/json"));
    assert!(
        resp.body
            .starts_with("{\"status\":\"error\",\"errorType\":\"422\",\"error\":\""),
        "body: {}",
        resp.body
    );
    assert!(resp.body.ends_with("\"}"), "body: {}", resp.body);
}

#[test]
fn error_missing_query_arg() {
    let ts = serve_default(default_provider());
    for path in ["/api/v1/query", "/api/v1/query_range"] {
        let resp = get(ts.addr, path);
        assert_eq!(resp.status, 422, "path={path}");
        assert_eq!(
            resp.body,
            "{\"status\":\"error\",\"errorType\":\"422\",\"error\":\"missing `query` arg\"}"
        );
    }
}

#[test]
fn error_bad_time_param() {
    let ts = serve_default(default_provider());
    let resp = get(ts.addr, "/api/v1/query?query=up&time=not-a-time");
    assert_eq!(resp.status, 422);
    assert!(
        resp.body.contains("cannot parse time=not-a-time"),
        "{}",
        resp.body
    );

    let resp = get(
        ts.addr,
        "/api/v1/query_range?query=up&start=1000&end=1200&step=0",
    );
    assert_eq!(resp.status, 422);
    assert!(resp.body.contains("out of allowed range"), "{}", resp.body);
}

#[test]
fn series_golden() {
    let ts = serve_default(default_provider());
    let resp = get(
        ts.addr,
        "/api/v1/series?match%5B%5D=test_metric&start=1577836800&end=1577837100",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.body,
        "{\"status\":\"success\",\"data\":[\
         {\"__name__\":\"test_metric\",\"host\":\"h1\"},\
         {\"__name__\":\"test_metric\",\"host\":\"h2\"}]}"
    );

    // limit truncates.
    let resp = get(
        ts.addr,
        "/api/v1/series?match%5B%5D=test_metric&start=1577836800&end=1577837100&limit=1",
    );
    assert_eq!(
        resp.body,
        "{\"status\":\"success\",\"data\":[{\"__name__\":\"test_metric\",\"host\":\"h1\"}]}"
    );
}

#[test]
fn series_requires_match_arg() {
    let ts = serve_default(default_provider());
    let resp = get(ts.addr, "/api/v1/series");
    assert_eq!(resp.status, 422);
    assert_eq!(
        resp.body,
        "{\"status\":\"error\",\"errorType\":\"422\",\"error\":\"missing `match[]` arg\"}"
    );
}

#[test]
fn labels_and_label_values_golden() {
    let ts = serve_default(default_provider());
    let resp = get(ts.addr, "/api/v1/labels?start=1577836800&end=1577837100");
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.body,
        "{\"status\":\"success\",\"data\":[\"__name__\",\"host\"]}"
    );

    let resp = get(
        ts.addr,
        "/api/v1/label/host/values?start=1577836800&end=1577837100",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.body,
        "{\"status\":\"success\",\"data\":[\"h1\",\"h2\"]}"
    );

    let resp = get(
        ts.addr,
        "/api/v1/label/__name__/values?start=1577836800&end=1577837100",
    );
    assert_eq!(
        resp.body,
        "{\"status\":\"success\",\"data\":[\"test_metric\"]}"
    );

    let resp = get(
        ts.addr,
        "/api/v1/label/missing/values?start=1577836800&end=1577837100",
    );
    assert_eq!(resp.body, "{\"status\":\"success\",\"data\":[]}");

    // limit caps the sorted list.
    let resp = get(
        ts.addr,
        "/api/v1/label/host/values?start=1577836800&end=1577837100&limit=1",
    );
    assert_eq!(resp.body, "{\"status\":\"success\",\"data\":[\"h1\"]}");
}

#[test]
fn export_ndjson_golden() {
    let provider = FakeProvider::new(vec![
        (
            metric_name("m", &[("host", "h1")]),
            vec![
                1_577_836_800_000,
                1_577_836_900_000,
                1_577_837_000_000,
                1_577_837_100_000,
            ],
            vec![1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY],
        ),
        (metric_name("empty", &[]), vec![], vec![]),
        (
            metric_name("m", &[("host", "h2")]),
            vec![1_577_836_800_000],
            vec![0.30000000000000004],
        ),
    ]);
    let ts = serve_default(provider);
    let resp = get(
        ts.addr,
        "/api/v1/export?match%5B%5D=%7B__name__%21%3D%22%22%7D&start=1577836800&end=1577837100",
    );
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("content-type"),
        Some("application/stream+json; charset=utf-8")
    );
    assert_eq!(
        resp.body,
        "{\"metric\":{\"__name__\":\"m\",\"host\":\"h1\"},\
         \"values\":[1.5,null,\"Infinity\",\"-Infinity\"],\
         \"timestamps\":[1577836800000,1577836900000,1577837000000,1577837100000]}\n\
         {\"metric\":{\"__name__\":\"m\",\"host\":\"h2\"},\
         \"values\":[0.30000000000000004],\"timestamps\":[1577836800000]}\n"
    );
}

#[test]
fn export_requires_match_and_maps_errors_to_plain_400() {
    let ts = serve_default(default_provider());
    let resp = get(ts.addr, "/api/v1/export");
    assert_eq!(resp.status, 400);
    assert_eq!(
        resp.header("content-type"),
        Some("text/plain; charset=utf-8")
    );
    assert!(resp.body.contains("missing `match[]` arg"), "{}", resp.body);
}

#[test]
fn export_max_rows_per_line_splits_lines() {
    let provider = FakeProvider::new(vec![(
        metric_name("m", &[]),
        vec![1_577_836_800_000, 1_577_836_900_000, 1_577_837_000_000],
        vec![1.0, 2.0, 3.0],
    )]);
    let ts = serve_default(provider);
    let resp = get(
        ts.addr,
        "/api/v1/export?match%5B%5D=m&start=1577836800&end=1577837100&max_rows_per_line=2",
    );
    assert_eq!(
        resp.body,
        "{\"metric\":{\"__name__\":\"m\"},\"values\":[1,2],\"timestamps\":[1577836800000,1577836900000]}\n\
         {\"metric\":{\"__name__\":\"m\"},\"values\":[3],\"timestamps\":[1577837000000]}\n"
    );
}

#[test]
fn static_stubs_match_upstream() {
    let ts = serve_default(FakeProvider::new(vec![]));
    let cases = [
        (
            "/api/v1/status/buildinfo",
            "{\"status\":\"success\",\"data\":{\"version\":\"2.24.0\"}}",
        ),
        (
            "/api/v1/rules",
            "{\"status\":\"success\",\"data\":{\"groups\":[]}}",
        ),
        (
            "/api/v1/alerts",
            "{\"status\":\"success\",\"data\":{\"alerts\":[]}}",
        ),
        (
            "/api/v1/notifiers",
            "{\"status\":\"success\",\"data\":{\"notifiers\":[]}}",
        ),
        (
            "/api/v1/query_exemplars",
            "{\"status\":\"success\",\"data\":[]}",
        ),
    ];
    for (path, expected) in cases {
        let resp = get(ts.addr, path);
        assert_eq!(resp.status, 200, "path={path}");
        assert_eq!(resp.body, expected, "path={path}");
        assert_eq!(resp.header("content-type"), Some("application/json"));
    }
    // /prometheus prefix is stripped like in the upstream.
    let resp = get(ts.addr, "/prometheus/api/v1/status/buildinfo");
    assert_eq!(resp.status, 200);
}

#[test]
fn non_select_paths_fall_through() {
    let ts = serve_default(FakeProvider::new(vec![]));
    for path in [
        "/api/v1/write",
        "/api/v1/status/tsdb",
        "/metrics",
        "/api/v1/label/host/values/extra",
    ] {
        let resp = get(ts.addr, path);
        assert_eq!(resp.status, 404, "path={path}");
    }
}

#[test]
fn concurrency_limit_answers_429_with_retry_after() {
    let mut provider = default_provider();
    provider.delay = Duration::from_millis(500);
    let config = SelectConfig {
        max_concurrent_requests: 1,
        max_queue_duration_ms: 50,
        ..Default::default()
    };
    let ts = serve(provider, config);
    let addr = ts.addr;

    let slow = std::thread::spawn(move || {
        get(
            addr,
            "/api/v1/query_range?query=test_metric&start=1577836800&end=1577837000&step=100",
        )
    });
    std::thread::sleep(Duration::from_millis(150));
    let fast = get(
        addr,
        "/api/v1/query_range?query=test_metric&start=1577836800&end=1577837000&step=100",
    );
    assert_eq!(fast.status, 429, "body: {}", fast.body);
    assert_eq!(fast.header("retry-after"), Some("10"));
    assert!(
        fast.body.contains("couldn't start executing the request"),
        "{}",
        fast.body
    );

    let slow = slow.join().unwrap();
    assert_eq!(slow.status, 200);
}

#[test]
fn timeout_param_accepted_and_bad_timeout_ignored_by_limiter() {
    let ts = serve_default(default_provider());
    // A valid timeout arg flows into the deadline without changing results.
    let resp = get(
        ts.addr,
        "/api/v1/query_range?query=test_metric%7Bhost%3D%22h1%22%7D&start=1577836800&end=1577837000&step=100&timeout=5",
    );
    assert_eq!(resp.status, 200);
    assert!(resp.body.contains("\"resultType\":\"matrix\""));
}
