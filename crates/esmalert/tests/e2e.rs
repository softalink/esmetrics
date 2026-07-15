//! End-to-end test for `esmalert`: drives a real [`esmalert::rule::Group`]
//! (one alerting rule) against three in-process `esm-http` stub servers —
//! a datasource (`GET /api/v1/query`), a remote-write capture
//! (`POST /api/v1/write`), and an Alertmanager v2 capture
//! (`POST /api/v2/alerts`) — through the crate's real `Datasource`,
//! `RwClient`, and `AlertManager` clients. No real `esmetrics` instance is
//! needed.
//!
//! Mirrors `esmauth/tests/reload_e2e_test.rs`'s style (in-process mock
//! servers, no wall-clock sleeps beyond a bounded defensive poll) and reuses
//! the same evaluation pattern this crate's own unit tests already use
//! (`rule::group_tests`, `rule::alerting::tests::pending_then_firing_after_for`):
//! call [`esmalert::rule::Group::eval_once`] directly at successive
//! caller-chosen timestamps rather than depending on `Group::start`'s
//! interval-loop thread and real time.
//!
//! Scenario:
//! 1. The datasource stub always answers with one sample (`instance="h1"`),
//!    so the rule's expression is "present" at every evaluation. At `t0` the
//!    alert is new -> `Pending`. At `t1 = t0 + 3s` (`for: 2s` has elapsed)
//!    it -> `Firing`.
//! 2. After every `eval_once`, `RwClient::flush_now` synchronously drains
//!    the remote-write queue to the write-capture stub; its captured body is
//!    decoded (snappy + `unmarshal_write_request`) to find the
//!    `ALERTS_FOR_STATE` series, whose sample value is `active_at` in whole
//!    unix seconds (see `rule::alert::alert_for_time_series`'s doc comment).
//! 3. Only once `Firing` (never while `Pending`, see
//!    `AlertingRule::alerts_to_send`) does `eval_once` POST to the
//!    Alertmanager stub; its captured body is asserted to carry the rule's
//!    identity labels.
//! 4. Finally, a **fresh** `AlertingRule` (no prior state) is built and its
//!    `restore()` is called against a small in-memory `Querier` that returns
//!    exactly the `ALERTS_FOR_STATE` sample decoded in step 2 — proving the
//!    write -> decode -> restore round trip: the fresh rule's recovered
//!    `active_at` must equal the original alert's `active_at` (the decoded
//!    series value, in whole seconds, times 1000).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use esm_gotemplate::{default_funcs, EvalContext};
use esm_http::{Request, ResponseWriter, Server};

use esmalert::datasource::{
    AuthConfig, Datasource, DsError, Metric, QueryResult, TlsConfig, DEFAULT_QUERY_TIMEOUT,
};
use esmalert::notifier::{AlertManager, Notifiers};
use esmalert::remotewrite::{RwClient, RwConfig, DEFAULT_SEND_TIMEOUT};
use esmalert::rule::{AlertingRule, Group, Querier, RuleKind};

/// Rule identity used throughout: matches what the datasource/restore
/// querier and the Alertmanager capture assertions all key off of.
const RULE_NAME: &str = "HighLoad";
const GROUP_NAME: &str = "g";
/// `for:` duration for the alerting rule; short so `t1` doesn't need to be
/// far from `t0`, but non-zero so the Pending phase is actually exercised.
const FOR_MS: i64 = 2_000;

/// Polls `check` until it returns `true` or `timeout` elapses. Bounds every
/// wait in this file so a wiring bug fails the test fast instead of hanging
/// the suite (mirrors `esmauth/tests/reload_e2e_test.rs`'s `wait_until`).
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

/// Starts the datasource stub: always answers `GET /api/v1/query` with one
/// present sample (`instance="h1"`), regardless of the query expression or
/// timestamp — the state transition under test comes entirely from the
/// caller driving `eval_once` at successive `ts` values, not from the
/// queried data changing.
fn start_datasource_stub() -> Server {
    let server = Server::bind("127.0.0.1:0").expect("bind datasource stub");
    server.serve(Arc::new(
        |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            assert_eq!(req.path(), "/api/v1/query");
            let body = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"h1"},"value":[1700000000,"1"]}]}}"#;
            w.write_json(200, body);
        },
    ));
    server
}

/// Starts the remote-write capture stub: records the raw (snappy-compressed
/// protobuf) body of the most recent `POST /api/v1/write`.
fn start_write_capture_stub() -> (Server, Arc<Mutex<Option<Vec<u8>>>>) {
    let server = Server::bind("127.0.0.1:0").expect("bind write-capture stub");
    let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let captured_for_handler = Arc::clone(&captured);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            assert_eq!(req.path(), "/api/v1/write");
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).ok();
            *captured_for_handler.lock().unwrap() = Some(body);
            w.write_json(204, "{}");
        },
    ));
    (server, captured)
}

/// Starts the Alertmanager v2 capture stub: records the JSON body of the
/// most recent `POST /api/v2/alerts`.
fn start_alertmanager_capture_stub() -> (Server, Arc<Mutex<Option<String>>>) {
    let server = Server::bind("127.0.0.1:0").expect("bind alertmanager-capture stub");
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured_for_handler = Arc::clone(&captured);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            assert_eq!(req.path(), "/api/v2/alerts");
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).ok();
            *captured_for_handler.lock().unwrap() =
                Some(String::from_utf8_lossy(&body).to_string());
            w.write_json(200, "{}");
        },
    ));
    (server, captured)
}

fn test_ctx() -> EvalContext {
    EvalContext {
        external_url: "http://esmalert.local".into(),
        path_prefix: "".into(),
        query_fn: Arc::new(|_| Ok(vec![])),
    }
}

/// Decodes a captured remote-write body (snappy + protobuf) into
/// `(labels-without-__name__, sample-value)` for the series named
/// `metric_name`. Panics (via `expect`) if the series isn't present — every
/// call site expects it to be, given the eval that preceded it.
fn decode_series(body: &[u8], metric_name: &str) -> (BTreeMap<String, String>, f64) {
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(body)
        .expect("snappy decompress remote-write body");
    let wr = esm_protoparser::prompb::unmarshal_write_request(&decompressed)
        .expect("decode remote-write request");
    for ts in &wr.timeseries {
        let is_match = ts
            .labels
            .iter()
            .any(|l| l.name == b"__name__" && l.value == metric_name.as_bytes());
        if !is_match {
            continue;
        }
        // A state transition now also emits a StaleNaN marker series carrying
        // the same `__name__` (e.g. the Pending->Firing eval writes a stale
        // `ALERTS{alertstate="pending"}` alongside the live firing one). Skip
        // those NaN-valued markers so this returns the live series.
        if ts.samples.first().is_some_and(|s| s.value.is_nan()) {
            continue;
        }
        let labels: BTreeMap<String, String> = ts
            .labels
            .iter()
            .filter(|l| l.name != b"__name__")
            .map(|l| {
                (
                    String::from_utf8_lossy(l.name).to_string(),
                    String::from_utf8_lossy(l.value).to_string(),
                )
            })
            .collect();
        let value = ts.samples.first().expect("series has a sample").value;
        return (labels, value);
    }
    panic!("series {metric_name:?} not found in write request: {wr:?}");
}

/// An in-memory [`Querier`] that always returns one fixed `Metric` — used to
/// drive [`AlertingRule::restore`] against exactly the `ALERTS_FOR_STATE`
/// data decoded from the captured remote-write body, without depending on
/// the datasource stub or any HTTP round trip for the restore step.
struct FixedRestoreQuerier(Metric);

impl Querier for FixedRestoreQuerier {
    fn query(&self, _expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
        Ok(QueryResult {
            data: vec![self.0.clone()],
            is_partial: None,
        })
    }
}

#[test]
fn alert_transitions_pending_to_firing_and_write_restore_round_trips() {
    let ds_server = start_datasource_stub();
    let ds_addr = ds_server.local_addr();
    let (write_server, write_captured) = start_write_capture_stub();
    let write_addr = write_server.local_addr();
    let (am_server, am_captured) = start_alertmanager_capture_stub();
    let am_addr = am_server.local_addr();

    let datasource = Datasource::new(
        &format!("http://{ds_addr}"),
        AuthConfig::default(),
        TlsConfig::default(),
        BTreeMap::new(),
        Vec::new(),
        Duration::from_secs(15),
        DEFAULT_QUERY_TIMEOUT,
    )
    .expect("build datasource client");

    let rw = RwClient::start(RwConfig {
        url: format!("http://{write_addr}"),
        // Large enough that the background flush thread never fires on its
        // own during this test; every flush below is driven synchronously
        // via `flush_now`, so the test has no dependency on timer timing.
        flush_interval: Duration::from_secs(3600),
        max_batch_size: 100,
        max_queue_size: 100,
        concurrency: 1,
        send_timeout: DEFAULT_SEND_TIMEOUT,
        auth: AuthConfig::default(),
        tls: TlsConfig::default(),
        headers: vec![],
    })
    .expect("start remote-write client");

    let am = AlertManager::new(
        &format!("http://{am_addr}"),
        "",
        vec![],
        AuthConfig::default(),
        TlsConfig::default(),
        Duration::from_secs(5),
    )
    .expect("build alertmanager client");
    let notifiers = Notifiers(vec![am]);

    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    let mut group = Group {
        name: GROUP_NAME.to_string(),
        // Unused by this test: every evaluation is driven explicitly via
        // `eval_once` at a chosen `ts`, never through `Group::start`'s
        // wall-clock interval loop.
        interval: Duration::from_secs(9_999),
        concurrency: 1,
        rules: vec![RuleKind::Alerting(AlertingRule {
            name: RULE_NAME.to_string(),
            group_name: GROUP_NAME.to_string(),
            expr: "up".to_string(),
            r#for: Duration::from_millis(FOR_MS as u64),
            ..Default::default()
        })],
        ..Default::default()
    };

    // A round unix-seconds timestamp, so `active_at`'s whole-seconds
    // ALERTS_FOR_STATE encoding round-trips exactly (no fractional part to
    // lose).
    let t0: i64 = 1_700_000_000_000;
    let t1: i64 = t0 + 3_000; // FOR_MS (2s) elapsed -> Firing.

    // --- Eval 1 (t0): the sample is newly seen -> Pending. ---
    group.eval_once(
        &datasource,
        t0,
        &funcs,
        &ctx,
        Some(&rw),
        Some(&notifiers),
        "http://esmalert.local",
    );
    let snap = group.snapshot();
    assert_eq!(snap.alerts.len(), 1, "expected exactly one active alert");
    assert_eq!(snap.alerts[0].state, "pending");

    rw.flush_now().expect("flush_now after pending eval");
    // Pending alerts are excluded from `alerts_to_send`
    // (`AlertingRule::alerts_to_send`'s doc comment), so no Alertmanager POST
    // is expected yet.
    assert!(
        am_captured.lock().unwrap().is_none(),
        "alertmanager must not be notified while the alert is still Pending"
    );

    // --- Eval 2 (t1 = t0 + 3s >= for:2s): promotes to Firing. ---
    group.eval_once(
        &datasource,
        t1,
        &funcs,
        &ctx,
        Some(&rw),
        Some(&notifiers),
        "http://esmalert.local",
    );
    let snap = group.snapshot();
    assert_eq!(snap.alerts.len(), 1);
    assert_eq!(
        snap.alerts[0].state, "firing",
        "alert should have transitioned Pending -> Firing after `for` elapsed"
    );

    rw.flush_now().expect("flush_now after firing eval");

    // Alertmanager must now have received a POST for the firing alert.
    let am_found = wait_until(Duration::from_secs(2), || {
        am_captured.lock().unwrap().is_some()
    });
    assert!(am_found, "alertmanager stub never received a POST");
    let am_body = am_captured.lock().unwrap().clone().unwrap();
    assert!(
        am_body.contains(&format!("\"alertname\":\"{RULE_NAME}\"")),
        "alertmanager payload missing alertname: {am_body}"
    );
    assert!(
        am_body.contains("\"instance\":\"h1\""),
        "alertmanager payload missing instance label: {am_body}"
    );

    // The remote-write capture must reflect the *second* (Firing) eval's
    // ALERTS/ALERTS_FOR_STATE series (each `flush_now` fully drains the
    // queue, and the capture stub keeps only the most recent body).
    let write_found = wait_until(Duration::from_secs(2), || {
        write_captured.lock().unwrap().is_some()
    });
    assert!(write_found, "remote-write stub never received a POST");
    let write_body = write_captured.lock().unwrap().clone().unwrap();

    let (alerts_labels, alerts_value) = decode_series(&write_body, "ALERTS");
    assert_eq!(
        alerts_labels.get("alertstate").map(String::as_str),
        Some("firing"),
        "ALERTS series should reflect the Firing state: {alerts_labels:?}"
    );
    assert_eq!(alerts_value, 1.0);

    let (for_state_labels, for_state_value) = decode_series(&write_body, "ALERTS_FOR_STATE");
    assert_eq!(
        for_state_labels.get("alertname").map(String::as_str),
        Some(RULE_NAME)
    );
    // The value is `active_at` in whole unix seconds; `active_at` was set at
    // t0 (the Pending eval) and must not have moved when the alert was
    // promoted to Firing at t1.
    let expected_active_at_seconds = t0 / 1000;
    assert_eq!(for_state_value as i64, expected_active_at_seconds);

    // --- Restore round trip: a FRESH AlertingRule, no prior state, restored
    // from exactly the ALERTS_FOR_STATE data just decoded above. ---
    let mut fresh_rule = AlertingRule {
        name: RULE_NAME.to_string(),
        group_name: GROUP_NAME.to_string(),
        expr: "up".to_string(),
        ..Default::default()
    };
    assert!(
        fresh_rule.alerts.is_empty(),
        "precondition: the fresh rule starts with no alert state"
    );

    let restore_metric = Metric {
        labels: for_state_labels.into_iter().collect(),
        timestamps: vec![t1],
        values: vec![for_state_value],
    };
    let restore_querier = FixedRestoreQuerier(restore_metric);
    fresh_rule
        .restore(&restore_querier, t1 + 1_000, Duration::from_secs(3_600))
        .expect("restore failed");

    assert_eq!(
        fresh_rule.alerts.len(),
        1,
        "restore should have seeded exactly one alert"
    );
    let restored = fresh_rule.alerts.values().next().unwrap();
    assert_eq!(
        restored.active_at, t0,
        "restored active_at must equal the original alert's active_at \
         (ALERTS_FOR_STATE value in whole seconds * 1000)"
    );

    rw.shutdown();
    ds_server.stop();
    write_server.stop();
    am_server.stop();
}
