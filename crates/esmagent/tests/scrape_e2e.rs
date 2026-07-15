//! End-to-end test for esmagent's scrape engine: drives the real
//! [`esmagent::run`] pipeline (scrape manager -> per-target worker ->
//! `esm-relabel` target/metric relabel -> auto-metrics -> the SAME
//! global-relabel -> `Fanout` -> `RemoteWriteCtx` forwarding path pushed
//! data uses) against an in-process stub `/metrics` target and an
//! in-process stub remote-write destination. Mirrors `tests/e2e.rs`'s style
//! (in-process mock servers, bounded polling for every capture, no
//! sleep-only assertions) and `src/lib_tests.rs`'s
//! `targets_route_serves_scraped_targets_and_becomes_healthy` test (same
//! `run`+`/api/v1/targets` pattern, extended here to also verify the
//! forwarded payload and target-death staleness).
//!
//! Scenario:
//! 1. A stub `/metrics` target serves two Prometheus exposition series.
//! 2. A stub remote-write destination captures every `/api/v1/write` body
//!    it receives (still answering `204`).
//! 3. `esmagent::run` is started against a one-job `-promscrape.config`
//!    (a static target pointing at the `/metrics` stub, a short
//!    `scrape_interval` so the test doesn't need multi-second waits, and a
//!    target-relabel rule that adds a constant `env=e2e` label — proving
//!    target-relabel is wired into the scrape path, not just pushed data).
//! 4. The destination stub is polled until it has received the two scraped
//!    series and the `up=1` auto-metric, all carrying the target labels
//!    (`job`, `instance`) plus the relabel-added `env` label
//!    (snappy+protobuf decoded).
//! 5. `GET /api/v1/targets` on `app.local_addr()` confirms the target is
//!    reported `health: "up"`.
//! 6. The `/metrics` stub is fully dropped (not just `.stop()`'d — per
//!    `scrapework_tests.rs`'s finding, only `drop` frees the listening
//!    port so the next connection is refused instead of hanging). The next
//!    scrape tick fails; the destination stub is polled until it has
//!    received `up=0` AND a `STALE_NAN` marker for one of the two
//!    previously-scraped series, proving target-death staleness reaches
//!    the forwarding pipeline end to end.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};

use esmagent::flags::Flags;

/// Polls `check` until it returns `true` or `timeout` elapses. Bounds every
/// wait in this file so a wiring bug fails the test fast instead of hanging
/// the suite (duplicated from `tests/e2e.rs`'s `wait_until` — private to
/// its own module, same rationale as this crate's other per-file test
/// helper duplication).
fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Serves a fixed Prometheus exposition body on `/metrics`; any other path
/// gets 404. Mirrors `scrape::scrapework_tests`'s `start_metrics_stub`.
fn start_metrics_stub(body: &'static str) -> Server {
    let server = Server::bind("127.0.0.1:0").expect("bind metrics stub");
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.path() == "/metrics" {
                w.set_content_type("text/plain; version=0.0.4");
                w.write_body(body.as_bytes());
            } else {
                w.write_status(404);
            }
        },
    ));
    server
}

/// A remote-write destination stub: captures every request body it
/// accepts and always answers `204`. Mirrors `tests/e2e.rs`'s `DestStub`,
/// minus the `up`-gating (this test never fails the destination).
struct DestStub {
    server: Server,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
}

fn start_dest_stub() -> DestStub {
    let server = Server::bind("127.0.0.1:0").expect("bind destination stub");
    let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let bodies_for_handler = Arc::clone(&bodies);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).ok();
            bodies_for_handler.lock().unwrap().push(body);
            w.write_status(204);
        },
    ));
    DestStub { server, bodies }
}

/// One decoded remote-write sample: its metric name (from `__name__`), its
/// full label set, and its value.
struct DecodedSample {
    name: String,
    labels: HashMap<String, String>,
    value: f64,
}

/// Snappy-decompresses and protobuf-decodes every captured body, flattening
/// every `(series, sample)` pair across all of them into one `Vec`. Order
/// not significant to callers, which only check membership.
fn decode_bodies(bodies: &[Vec<u8>]) -> Vec<DecodedSample> {
    let mut out = Vec::new();
    for body in bodies {
        let raw = snap::raw::Decoder::new()
            .decompress_vec(body)
            .expect("snappy decompress captured body");
        let wr = esm_protoparser::prompb::unmarshal_write_request(&raw)
            .expect("decode captured write request");
        for ts in &wr.timeseries {
            let labels: HashMap<String, String> = ts
                .labels
                .iter()
                .map(|l| {
                    (
                        String::from_utf8_lossy(l.name).into_owned(),
                        String::from_utf8_lossy(l.value).into_owned(),
                    )
                })
                .collect();
            let name = labels.get("__name__").cloned().unwrap_or_default();
            for s in &ts.samples {
                out.push(DecodedSample {
                    name: name.clone(),
                    labels: labels.clone(),
                    value: s.value,
                });
            }
        }
    }
    out
}

/// Whether `samples` contains an entry named `name` whose value satisfies
/// `value_ok` and whose labels contain every `(key, value)` pair in
/// `want_labels`.
fn has_sample(
    samples: &[DecodedSample],
    name: &str,
    value_ok: impl Fn(f64) -> bool,
    want_labels: &[(&str, &str)],
) -> bool {
    samples.iter().any(|s| {
        s.name == name
            && value_ok(s.value)
            && want_labels
                .iter()
                .all(|(k, v)| s.labels.get(*k).map(|got| got == v).unwrap_or(false))
    })
}

/// Minimal raw HTTP/1.1 GET client, mirroring `src/lib_tests.rs`'s
/// `http_get` (private to that module, so duplicated here).
fn http_get(addr: SocketAddr, target: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect to esmagent");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set_read_timeout");
    let req = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response: {response:?}"));
    let status_line = head.lines().next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, body.to_string())
}

const JOB_NAME: &str = "esmagent_e2e_scrape";

#[test]
fn scrape_engine_forwards_series_and_reports_target_death() {
    let dest = start_dest_stub();
    let dest_addr = dest.server.local_addr();

    let mut metrics_server = Some(start_metrics_stub(
        "foo_metric{code=\"200\"} 5\nbar_metric 7\n",
    ));
    let metrics_addr = metrics_server.as_ref().unwrap().local_addr();

    let tmp = tempfile::tempdir().expect("tmp data path");
    let scrape_cfg_path = tmp.path().join("scrape.yml");
    // `scrape_interval`/`scrape_timeout` are both set explicitly (rather
    // than relying on the 60s/10s global defaults) so the test doesn't
    // need multi-second waits; `scrape_timeout` must stay <=
    // `scrape_interval` per `scrape::config::validate`. The
    // `relabel_configs` rule (no `source_labels`, default `(.*)` regex, an
    // explicit `replacement`) is the standard "add a constant label"
    // pattern — it proves target-relabel runs on the scrape path, not just
    // pushed data.
    std::fs::write(
        &scrape_cfg_path,
        format!(
            "scrape_configs:\n\
             \x20 - job_name: {JOB_NAME}\n\
             \x20   scrape_interval: 1s\n\
             \x20   scrape_timeout: 500ms\n\
             \x20   static_configs:\n\
             \x20     - targets: ['{metrics_addr}']\n\
             \x20   relabel_configs:\n\
             \x20     - target_label: env\n\
             \x20       replacement: e2e\n"
        ),
    )
    .expect("write scrape config");

    let flags = Flags {
        remote_write_urls: vec![format!("http://{dest_addr}/api/v1/write")],
        remote_write_tmp_data_path: tmp.path().to_string_lossy().to_string(),
        remote_write_max_block_size: 1,
        http_listen_addr: "127.0.0.1:0".to_string(),
        promscrape_config: Some(scrape_cfg_path.to_string_lossy().to_string()),
        ..Flags::default()
    };

    let app = esmagent::run(&flags).expect("esmagent::run should succeed");
    let agent_addr = app.local_addr();

    let instance = metrics_addr.to_string();
    let target_labels: &[(&str, &str)] = &[
        ("job", JOB_NAME),
        ("instance", instance.as_str()),
        ("env", "e2e"),
    ];

    // --- Step 1: the destination receives the scraped series and up=1,
    // all carrying the target + relabel-added labels. ---
    assert!(
        wait_until(Duration::from_secs(15), || {
            let samples = decode_bodies(&dest.bodies.lock().unwrap());
            has_sample(&samples, "foo_metric", |v| v == 5.0, target_labels)
                && has_sample(&samples, "bar_metric", |v| v == 7.0, target_labels)
                && has_sample(&samples, "up", |v| v == 1.0, target_labels)
        }),
        "destination never received the scraped series + up=1"
    );

    // --- Step 2: `/api/v1/targets` reports the target as up. ---
    assert!(
        wait_until(Duration::from_secs(10), || {
            let (status, body) = http_get(agent_addr, "/api/v1/targets");
            if status != 200 {
                return false;
            }
            let json: serde_json::Value =
                serde_json::from_str(&body).expect("targets response must be valid JSON");
            json["data"]["activeTargets"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|t| {
                    t["scrapeUrl"]
                        .as_str()
                        .is_some_and(|u| u.contains(&instance))
                        && t["health"] == "up"
                })
        }),
        "/api/v1/targets never reported the target as up"
    );

    // --- Step 3: kill the target; the next scrape tick reports up=0 and
    // stale-marks the previously-scraped series, both forwarded to the
    // destination. Fully drop (not `.stop()`) so the port refuses instead
    // of hanging the connection — see `scrapework_tests.rs`. ---
    drop(metrics_server.take().expect("metrics stub already dropped"));

    assert!(
        wait_until(Duration::from_secs(15), || {
            let samples = decode_bodies(&dest.bodies.lock().unwrap());
            let up_is_zero = has_sample(&samples, "up", |v| v == 0.0, target_labels);
            let stale_marker = samples.iter().any(|s| {
                (s.name == "foo_metric" || s.name == "bar_metric")
                    && s.value.to_bits() == esm_common::decimal::STALE_NAN.to_bits()
            });
            up_is_zero && stale_marker
        }),
        "destination never received up=0 and a stale marker after the target died"
    );

    app.stop();
    dest.server.stop();
    let _ = std::fs::remove_dir_all(tmp.path());
}
