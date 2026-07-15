//! End-to-end test for `esmagent`'s **pushed-ingestion** forwarding path:
//! drives the real [`esmagent::run`] pipeline (an `esm-http` server ->
//! `esm_insert` router -> `ForwardingSink` -> `Fanout` -> two
//! per-destination `RemoteWriteCtx` [relabel -> block accumulation ->
//! `PersistentQueue` -> `Client` retry pool]) against two in-process stub
//! remote-write destinations. No real upstream VictoriaMetrics-compatible
//! service or scrape target is needed here — `-promscrape.config` is unset,
//! so the scrape engine stays disabled and every series in this file comes
//! from pushed ingestion (see `tests/scrape_e2e.rs` for the scrape ->
//! relabel -> forward path, and `crates/esmagent/README.md` for the full
//! picture: esmagent both accepts pushed data and actively scrapes).
//!
//! Mirrors `esmalert/tests/e2e.rs`'s style: in-process mock servers, bounded
//! polling for every capture, no sleep-only assertions.
//!
//! Scenario:
//! 1. `esmagent::run` is started against two destination stubs (A, B) and a
//!    temp `-remoteWrite.tmpDataPath`. A Prometheus remote-write payload
//!    (two series) is POSTed to esmagent's `/api/v1/write`; both stubs are
//!    asserted to receive it (snappy+protobuf decoded).
//! 2. Destination B is made to fail (its stub starts answering every
//!    request with `500`, a retryable status per `client.rs`'s
//!    classification). More data is posted: A keeps receiving normally
//!    while B's un-delivered blocks accumulate as files under its
//!    `tmpDataPath` subdirectory (`esmagent::queue_dir_name`), proving
//!    per-destination failure isolation.
//! 3. B recovers (its stub starts answering `204` again); its backlog
//!    drains and is delivered.
//! 4. Durability: B is taken down again and three more distinct series are
//!    pushed, then `esmagent` is restarted (`App::stop` + a fresh
//!    `esmagent::run` against the *same* `tmpDataPath`) while B is still
//!    down. Per `client.rs`'s documented shutdown semantics (module doc,
//!    "stop lands mid-backoff" paragraph), even the one block the single
//!    worker was actively retrying when `stop` landed is re-queued onto the
//!    durable queue rather than dropped, so all three pushed series survive
//!    the restart and are delivered once B comes back up.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_http::{Request, ResponseWriter, Server};
use esm_protoparser::prompb::{Label, Sample, TimeSeries};
use esm_protoparser::prompb_encode::encode_and_compress;

use esmagent::flags::Flags;

/// Polls `check` until it returns `true` or `timeout` elapses. Bounds every
/// wait in this file so a wiring bug fails the test fast instead of hanging
/// the suite (mirrors `esmalert/tests/e2e.rs`'s `wait_until`).
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

/// One in-process destination stub: captures every request body it accepts,
/// gated by `up` (an [`AtomicBool`] the test flips directly, no port
/// rebind needed to simulate a failing destination). While `up` is `false`
/// it answers `500` (retryable, per `client::SendOutcome`) without
/// capturing anything — this "kills" the destination logically (the task
/// brief's "return errors" option) rather than tearing down the TCP
/// listener, so "bringing it back" is a single deterministic flag flip
/// instead of a racy same-port rebind.
struct DestStub {
    server: Server,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    up: Arc<AtomicBool>,
}

fn start_dest_stub() -> DestStub {
    let server = Server::bind("127.0.0.1:0").expect("bind destination stub");
    let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let up = Arc::new(AtomicBool::new(true));
    let bodies_for_handler = Arc::clone(&bodies);
    let up_for_handler = Arc::clone(&up);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if !up_for_handler.load(Ordering::SeqCst) {
                w.write_status(500);
                return;
            }
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).ok();
            bodies_for_handler.lock().unwrap().push(body);
            w.write_status(204);
        },
    ));
    DestStub { server, bodies, up }
}

/// Snappy-decompresses and protobuf-decodes every captured body, returning
/// every `__name__` label value found across all of them (order not
/// significant to callers, which only check membership).
fn decoded_series_names(bodies: &[Vec<u8>]) -> Vec<String> {
    let mut names = Vec::new();
    for body in bodies {
        let raw = snap::raw::Decoder::new()
            .decompress_vec(body)
            .expect("snappy decompress captured body");
        let wr = esm_protoparser::prompb::unmarshal_write_request(&raw)
            .expect("decode captured write request");
        for ts in &wr.timeseries {
            for l in &ts.labels {
                if l.name == b"__name__" {
                    names.push(String::from_utf8_lossy(l.value).into_owned());
                }
            }
        }
    }
    names
}

/// Builds a snappy-compressed Prometheus remote-write `WriteRequest` body
/// with one series per `(name, timestamp, value)` entry, each carrying only
/// a `__name__` label — enough for [`decoded_series_names`] to identify it,
/// and (with `-remoteWrite.maxBlockSize=1`, see `base_flags`) enough to
/// force each series into its own on-disk queue block.
fn build_write_request(series: &[(&str, i64, f64)]) -> Vec<u8> {
    let timeseries: Vec<TimeSeries<'_>> = series
        .iter()
        .map(|(name, ts, value)| TimeSeries {
            labels: vec![Label {
                name: b"__name__",
                value: name.as_bytes(),
            }],
            samples: vec![Sample {
                value: *value,
                timestamp: *ts,
            }],
        })
        .collect();
    encode_and_compress(&timeseries).expect("encode+compress write request")
}

/// Minimal raw HTTP/1.1 client: POSTs `body` to esmagent's `/api/v1/write`
/// and returns the response status code. Mirrors
/// `esm-insert/tests/promremotewrite_write.rs`'s `post` helper.
fn post_remote_write(addr: SocketAddr, body: &[u8]) -> u16 {
    let mut stream = TcpStream::connect(addr).expect("connect to esmagent");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set_read_timeout");
    let head = format!(
        "POST /api/v1/write HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Length: {}\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .expect("write request head");
    stream.write_all(body).expect("write request body");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let text = String::from_utf8_lossy(&raw);
    let status_line = text.lines().next().unwrap_or_default();
    status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Whether `dir` currently contains at least one regular file (i.e. the
/// destination's `PersistentQueue` has un-delivered blocks durably on disk).
fn dir_has_files(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .any(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        })
        .unwrap_or(false)
}

/// Base config shared by the initial run and the restart in step 4: two
/// destinations (`addr_a`, `addr_b`), a durable queue under `tmp_data_path`,
/// and tight tuning so the test doesn't need multi-second waits:
/// `-remoteWrite.maxBlockSize=1` flushes every pushed series into its own
/// block synchronously (no dependency on the periodic flush thread);
/// `-remoteWrite.queues=1` per destination keeps at most one block
/// "in flight" at a time, which is what makes step 4's survivor set
/// deterministic (see the module doc); short retry bounds keep the
/// dead-destination retry loop from stalling the test.
fn base_flags(addr_a: SocketAddr, addr_b: SocketAddr, tmp_data_path: &str) -> Flags {
    Flags {
        remote_write_urls: vec![
            format!("http://{addr_a}/api/v1/write"),
            format!("http://{addr_b}/api/v1/write"),
        ],
        remote_write_tmp_data_path: tmp_data_path.to_string(),
        remote_write_max_block_size: 1,
        remote_write_queues: 1,
        remote_write_flush_interval: Duration::from_secs(3600),
        remote_write_retry_min_interval: Duration::from_millis(20),
        remote_write_retry_max_interval: Duration::from_millis(100),
        http_listen_addr: "127.0.0.1:0".to_string(),
        ..Flags::default()
    }
}

#[test]
fn forwarding_pipeline_isolates_failures_queues_and_survives_restart() {
    let dest_a = start_dest_stub();
    let dest_b = start_dest_stub();
    let addr_a = dest_a.server.local_addr();
    let addr_b = dest_b.server.local_addr();

    let tmp = tempfile::tempdir().expect("tmp data path");
    let flags = base_flags(addr_a, addr_b, &tmp.path().to_string_lossy());
    let queue_dir_b = tmp
        .path()
        .join(esmagent::queue_dir_name(&flags.remote_write_urls[1], 1));

    let app = esmagent::run(&flags).expect("esmagent::run should succeed");
    let agent_addr = app.local_addr();

    // --- Step 1: both destinations receive the pushed batch. ---
    let body1 = build_write_request(&[
        ("esmagent_e2e_alpha", 1_000, 1.0),
        ("esmagent_e2e_beta", 1_000, 2.0),
    ]);
    assert_eq!(post_remote_write(agent_addr, &body1), 204);

    assert!(
        wait_until(Duration::from_secs(5), || dest_a
            .bodies
            .lock()
            .unwrap()
            .len()
            >= 2),
        "destination A never received the initial batch"
    );
    assert!(
        wait_until(Duration::from_secs(5), || dest_b
            .bodies
            .lock()
            .unwrap()
            .len()
            >= 2),
        "destination B never received the initial batch"
    );
    let names_a = decoded_series_names(&dest_a.bodies.lock().unwrap());
    let names_b = decoded_series_names(&dest_b.bodies.lock().unwrap());
    for name in ["esmagent_e2e_alpha", "esmagent_e2e_beta"] {
        assert!(
            names_a.contains(&name.to_string()),
            "A missing {name}: {names_a:?}"
        );
        assert!(
            names_b.contains(&name.to_string()),
            "B missing {name}: {names_b:?}"
        );
    }

    // --- Step 2: destination B fails; A keeps receiving while B's blocks
    // pile up durably on disk. ---
    dest_b.up.store(false, Ordering::SeqCst);
    let a_delivered_before = dest_a.bodies.lock().unwrap().len();

    let body2 = build_write_request(&[
        ("esmagent_e2e_gamma", 2_000, 3.0),
        ("esmagent_e2e_delta", 2_000, 4.0),
    ]);
    assert_eq!(post_remote_write(agent_addr, &body2), 204);

    assert!(
        wait_until(Duration::from_secs(5), || dest_a
            .bodies
            .lock()
            .unwrap()
            .len()
            >= a_delivered_before + 2),
        "destination A stopped receiving while B was down (failure isolation broke)"
    );
    assert!(
        wait_until(Duration::from_secs(5), || dir_has_files(&queue_dir_b)),
        "destination B's queue directory never accumulated blocks while B was down"
    );

    // --- Step 3: B recovers; its backlog drains and is delivered. ---
    dest_b.up.store(true, Ordering::SeqCst);
    assert!(
        wait_until(Duration::from_secs(5), || {
            let names = decoded_series_names(&dest_b.bodies.lock().unwrap());
            names.contains(&"esmagent_e2e_gamma".to_string())
                && names.contains(&"esmagent_e2e_delta".to_string())
        }),
        "destination B never received its backlog after recovering"
    );

    // --- Step 4: durability across a restart. ---
    dest_b.up.store(false, Ordering::SeqCst);
    let body3 = build_write_request(&[
        ("esmagent_e2e_durable_1", 3_000, 5.0),
        ("esmagent_e2e_durable_2", 3_000, 6.0),
        ("esmagent_e2e_durable_3", 3_000, 7.0),
    ]);
    assert_eq!(post_remote_write(agent_addr, &body3), 204);
    assert!(
        wait_until(Duration::from_secs(5), || dir_has_files(&queue_dir_b)),
        "durable-batch blocks never landed on disk before the restart"
    );

    // Stops the HTTP server and every destination's pipeline. Per
    // `client.rs`'s documented shutdown semantics, destination B's single
    // worker re-queues whatever block it was actively retrying
    // (`esmagent_e2e_durable_1`, pushed and therefore popped first) back
    // onto the durable queue instead of dropping it, so all three durable
    // series — including `_durable_1` — survive the restart.
    app.stop();

    let app2 = esmagent::run(&flags).expect("esmagent::run (restart) should succeed");

    dest_b.up.store(true, Ordering::SeqCst);
    assert!(
        wait_until(Duration::from_secs(5), || {
            let names = decoded_series_names(&dest_b.bodies.lock().unwrap());
            names.contains(&"esmagent_e2e_durable_1".to_string())
                && names.contains(&"esmagent_e2e_durable_2".to_string())
                && names.contains(&"esmagent_e2e_durable_3".to_string())
        }),
        "queued-but-undelivered blocks (including the in-flight one) did not survive the esmagent restart"
    );

    app2.stop();
    dest_a.server.stop();
    dest_b.server.stop();
}

/// Exercises the global stream-aggregation stage (`-streamAggr.config`): a
/// `sum_samples` aggregation over a 1s interval matching every series. Two
/// samples of the same series are pushed; the aggregated output
/// (`foo:1s_sum_samples`) must be delivered to the destination, and the raw
/// input `foo` must NOT be (it was consumed by the aggregator and
/// `-streamAggr.keepInput` is off). A generous `staleness_interval` keeps the
/// entry from being pruned before its first flush.
#[test]
fn stream_aggregation_aggregates_and_drops_matched_input() {
    let dest = start_dest_stub();
    let dest_addr = dest.server.local_addr();
    let tmp = tempfile::tempdir().expect("tmp data path");

    let cfg_path = tmp.path().join("streamaggr.yml");
    std::fs::write(
        &cfg_path,
        "- interval: 1s\n  staleness_interval: 1h\n  outputs: [sum_samples]\n",
    )
    .expect("write streamAggr config");

    let flags = Flags {
        remote_write_urls: vec![format!("http://{dest_addr}/api/v1/write")],
        remote_write_tmp_data_path: tmp.path().to_string_lossy().into_owned(),
        remote_write_max_block_size: 1,
        remote_write_queues: 1,
        remote_write_flush_interval: Duration::from_millis(50),
        remote_write_retry_min_interval: Duration::from_millis(20),
        remote_write_retry_max_interval: Duration::from_millis(100),
        http_listen_addr: "127.0.0.1:0".to_string(),
        stream_aggr_config: Some(cfg_path.to_string_lossy().into_owned()),
        ..Flags::default()
    };
    let app = esmagent::run(&flags).expect("esmagent::run should succeed");
    let agent_addr = app.local_addr();

    // Two samples of the same series `foo` → sum_samples = 3.
    let body = build_write_request(&[("foo", 1000, 1.0), ("foo", 2000, 2.0)]);
    assert_eq!(post_remote_write(agent_addr, &body), 204);

    // The aggregated series is delivered after the ~1s interval flush.
    assert!(
        wait_until(Duration::from_secs(8), || {
            let bodies = dest.bodies.lock().unwrap();
            decoded_series_names(&bodies)
                .iter()
                .any(|n| n == "foo:1s_sum_samples")
        }),
        "aggregated series never delivered; got {:?}",
        decoded_series_names(&dest.bodies.lock().unwrap())
    );

    // The raw input must have been consumed by the aggregator, not forwarded.
    let names = decoded_series_names(&dest.bodies.lock().unwrap());
    assert!(
        !names.iter().any(|n| n == "foo"),
        "raw input `foo` must be dropped when aggregated (keepInput off); got {names:?}"
    );

    app.stop();
    dest.server.stop();
}

/// Per-URL stream aggregation (`-remoteWrite.streamAggr.config`, positional):
/// destination A aggregates (sum_samples over 1s) while destination B does
/// not. Pushing `foo` twice must deliver the aggregated `foo:1s_sum_samples`
/// to A only (raw `foo` consumed there) and the raw `foo` samples to B —
/// proving per-destination aggregation isolation.
#[test]
fn per_url_stream_aggregation_isolates_destinations() {
    let dest_a = start_dest_stub();
    let dest_b = start_dest_stub();
    let addr_a = dest_a.server.local_addr();
    let addr_b = dest_b.server.local_addr();
    let tmp = tempfile::tempdir().expect("tmp data path");

    let cfg_a = tmp.path().join("streamaggr_a.yml");
    std::fs::write(
        &cfg_a,
        "- interval: 1s\n  staleness_interval: 1h\n  outputs: [sum_samples]\n",
    )
    .expect("write per-URL streamAggr config");

    let flags = Flags {
        remote_write_urls: vec![
            format!("http://{addr_a}/api/v1/write"),
            format!("http://{addr_b}/api/v1/write"),
        ],
        remote_write_tmp_data_path: tmp.path().to_string_lossy().into_owned(),
        remote_write_max_block_size: 1,
        remote_write_queues: 1,
        remote_write_flush_interval: Duration::from_millis(50),
        remote_write_retry_min_interval: Duration::from_millis(20),
        remote_write_retry_max_interval: Duration::from_millis(100),
        http_listen_addr: "127.0.0.1:0".to_string(),
        // Positional: destination A aggregates, destination B (empty) does not.
        remote_write_stream_aggr_config: vec![cfg_a.to_string_lossy().into_owned(), String::new()],
        ..Flags::default()
    };
    let app = esmagent::run(&flags).expect("esmagent::run should succeed");
    let agent_addr = app.local_addr();

    let body = build_write_request(&[("foo", 1000, 1.0), ("foo", 2000, 2.0)]);
    assert_eq!(post_remote_write(agent_addr, &body), 204);

    // Destination A: aggregated series delivered.
    assert!(
        wait_until(Duration::from_secs(8), || {
            decoded_series_names(&dest_a.bodies.lock().unwrap())
                .iter()
                .any(|n| n == "foo:1s_sum_samples")
        }),
        "destination A never received the aggregated series"
    );
    // Destination B: raw series delivered (no aggregation configured).
    assert!(
        wait_until(Duration::from_secs(8), || {
            decoded_series_names(&dest_b.bodies.lock().unwrap())
                .iter()
                .any(|n| n == "foo")
        }),
        "destination B never received the raw series"
    );

    // A must NOT carry the raw `foo` (it was consumed by the aggregator);
    // B must NOT carry the aggregated series (no aggregation there).
    let names_a = decoded_series_names(&dest_a.bodies.lock().unwrap());
    let names_b = decoded_series_names(&dest_b.bodies.lock().unwrap());
    assert!(
        !names_a.iter().any(|n| n == "foo"),
        "destination A leaked raw input despite aggregation: {names_a:?}"
    );
    assert!(
        !names_b.iter().any(|n| n == "foo:1s_sum_samples"),
        "destination B received an aggregated series it should not: {names_b:?}"
    );

    app.stop();
    dest_a.server.stop();
    dest_b.server.stop();
}
