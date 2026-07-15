//! Unit tests for [`super::Group`] and [`super::GroupHandle`], split into
//! this `#[path]`-included sibling file to keep `group.rs` under the
//! crate's 800-line file-size convention. Logically still `group::tests`
//! (`super::*` resolves to `group`).

use super::*;
use crate::datasource::{AuthConfig, DsError, Metric, QueryResult, TlsConfig};
use crate::notifier::AlertManager;
use crate::remotewrite::RwConfig;
use crate::rule::AlertState;
use esm_gotemplate::default_funcs;
use esm_http::{Request, ResponseWriter, Server};
use std::sync::Mutex;

struct Scripted {
    present: bool,
}
impl Querier for Scripted {
    fn query(&self, _: &str, _: i64) -> Result<QueryResult, DsError> {
        Ok(QueryResult {
            data: if self.present {
                vec![Metric {
                    labels: vec![("instance".into(), "h1".into())],
                    timestamps: vec![0],
                    values: vec![1.0],
                }]
            } else {
                vec![]
            },
            is_partial: None,
        })
    }
}

/// Returns two distinct metrics, used to trip a recording rule's
/// `limit` (or duplicate) exec error without needing to construct a
/// `DsError` (whose constructor is private to the datasource module).
struct TwoMetrics;
impl Querier for TwoMetrics {
    fn query(&self, _: &str, _: i64) -> Result<QueryResult, DsError> {
        Ok(QueryResult {
            data: vec![
                Metric {
                    labels: vec![("instance".into(), "h1".into())],
                    timestamps: vec![0],
                    values: vec![1.0],
                },
                Metric {
                    labels: vec![("instance".into(), "h2".into())],
                    timestamps: vec![0],
                    values: vec![1.0],
                },
            ],
            is_partial: None,
        })
    }
}

fn test_ctx() -> EvalContext {
    EvalContext {
        external_url: "".into(),
        path_prefix: "".into(),
        query_fn: Arc::new(|_| Ok(vec![])),
    }
}

#[test]
fn snapshot_reflects_firing_alert_after_eval() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(60),
        concurrency: 1,
        rules: vec![RuleKind::Alerting(AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            ..Default::default()
        })],
        ..Default::default()
    };
    g.eval_once(
        &Scripted { present: true },
        5_000,
        &funcs,
        &ctx,
        None,
        None,
        "http://vm",
    );

    let snap = g.snapshot();
    assert_eq!(snap.alerts.len(), 1, "one active alert expected");
    let a = &snap.alerts[0];
    assert_eq!(a.state, "firing"); // for=0 -> fires immediately
    assert_eq!(a.alertname, "A");
    assert_eq!(a.group, "g");
    assert_eq!(a.active_at, 5_000);
    assert_eq!(a.labels.get("instance").map(String::as_str), Some("h1"));
    // The alerting rule itself evaluated successfully.
    assert_eq!(snap.rules[0].health, RuleHealth::Ok);
    assert!(snap.rules[0].last_error.is_none());
}

#[test]
fn snapshot_reports_ok_health_after_successful_recording_eval() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(60),
        concurrency: 1,
        rules: vec![RuleKind::Recording(RecordingRule {
            name: "r".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };
    g.eval_once(
        &Scripted { present: true },
        0,
        &funcs,
        &ctx,
        None,
        None,
        "http://vm",
    );

    let snap = g.snapshot();
    assert_eq!(snap.rules.len(), 1);
    assert_eq!(snap.rules[0].health, RuleHealth::Ok);
    assert!(snap.rules[0].last_error.is_none());
    assert_eq!(snap.rules[0].record.as_deref(), Some("r"));
    assert!(snap.alerts.is_empty(), "recording rules produce no alerts");
}

#[test]
fn snapshot_reports_err_health_when_rule_exec_fails() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    // limit=1 with a querier returning 2 series -> RecordingRule::exec
    // returns an "exec exceeded limit" error.
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(60),
        concurrency: 1,
        limit: 1,
        rules: vec![RuleKind::Recording(RecordingRule {
            name: "r".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };
    g.eval_once(&TwoMetrics, 0, &funcs, &ctx, None, None, "http://vm");

    let snap = g.snapshot();
    assert_eq!(snap.rules[0].health, RuleHealth::Err);
    let msg = snap.rules[0]
        .last_error
        .as_deref()
        .expect("errored rule should record last_error");
    assert!(
        msg.contains("limit"),
        "last_error should describe the exec failure, got {msg:?}"
    );
}

#[test]
fn eval_once_returns_eval_errors_for_failed_rules() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    // limit=1 with a querier returning 2 series -> RecordingRule::exec
    // returns an "exec exceeded limit" error, which `eval_once` must now
    // surface to its caller (the unittest runner relies on this) in
    // addition to logging it and recording it on the rule's health.
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(60),
        concurrency: 1,
        limit: 1,
        rules: vec![RuleKind::Recording(RecordingRule {
            name: "r".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };
    let errs = g.eval_once(&TwoMetrics, 0, &funcs, &ctx, None, None, "http://vm");
    assert_eq!(errs.len(), 1, "the failing rule's error must be returned");
    assert!(
        errs[0].to_string().contains("limit"),
        "returned error should describe the exec failure, got {}",
        errs[0]
    );
}

#[test]
fn eval_once_returns_no_errors_when_all_rules_succeed() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(60),
        concurrency: 1,
        rules: vec![RuleKind::Recording(RecordingRule {
            name: "r".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };
    let errs = g.eval_once(
        &Scripted { present: true },
        0,
        &funcs,
        &ctx,
        None,
        None,
        "http://vm",
    );
    assert!(errs.is_empty(), "no errors expected, got {errs:?}");
}

#[test]
fn eval_once_pushes_recording_results_to_rw() {
    let server = Server::bind("127.0.0.1:0").expect("bind stub server");
    let addr = server.local_addr();
    let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let captured_for_handler = Arc::clone(&captured);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).ok();
            *captured_for_handler.lock().unwrap() = Some(body);
            w.write_json(204, "{}");
        },
    ));

    let rw = RwClient::start(RwConfig {
        url: format!("http://{addr}"),
        flush_interval: Duration::from_secs(3600),
        max_batch_size: 100,
        max_queue_size: 100,
        concurrency: 1,
        send_timeout: Duration::from_secs(30),
        auth: AuthConfig::default(),
        tls: TlsConfig::default(),
        headers: vec![],
    })
    .expect("start rw client");

    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(60),
        concurrency: 1,
        rules: vec![RuleKind::Recording(RecordingRule {
            name: "job:up".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };

    g.eval_once(
        &Scripted { present: true },
        0,
        &funcs,
        &ctx,
        Some(&rw),
        None,
        "http://vm",
    );
    rw.flush_now().expect("flush_now failed");

    let body = captured
        .lock()
        .unwrap()
        .clone()
        .expect("remote-write received a request");
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .expect("snappy decompress");
    let wr = esm_protoparser::prompb::unmarshal_write_request(&decompressed).expect("decode");
    assert_eq!(wr.timeseries.len(), 1);
    assert!(wr.timeseries[0]
        .labels
        .iter()
        .any(|l| l.name == b"__name__" && l.value == b"job:up"));

    rw.shutdown();
    server.stop();
}

#[test]
fn concurrency_one_runs_all_rules() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(1),
        concurrency: 1,
        rules: vec![RuleKind::Alerting(AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            ..Default::default()
        })],
        ..Default::default()
    };
    g.eval_once(
        &Scripted { present: true },
        0,
        &funcs,
        &ctx,
        None,
        None,
        "http://vm",
    );
    if let RuleKind::Alerting(ar) = &g.rules[0] {
        assert_eq!(ar.alerts.len(), 1);
    } else {
        panic!("expected alerting rule");
    }
}

#[test]
fn eval_once_sets_resolve_duration_end_before_notifying() {
    let server = Server::bind("127.0.0.1:0").expect("bind stub alertmanager");
    let addr = server.local_addr();
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured_for_handler = Arc::clone(&captured);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            let mut body = Vec::new();
            req.read_body_to(&mut body, 1 << 20).ok();
            *captured_for_handler.lock().unwrap() =
                Some(String::from_utf8_lossy(&body).to_string());
            w.write_json(200, "{}");
        },
    ));

    let am = AlertManager::new(
        &format!("http://{addr}"),
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
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(60),
        concurrency: 1,
        rules: vec![RuleKind::Alerting(AlertingRule {
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            ..Default::default()
        })],
        ..Default::default()
    };

    // interval=60s, resend_delay=0 (default) -> resolve_duration =
    // max(60s,0)*4 = 240s; ts=0 -> endsAt = epoch + 240s.
    g.eval_once(
        &Scripted { present: true },
        0,
        &funcs,
        &ctx,
        None,
        Some(&notifiers),
        "http://vm",
    );

    let body = captured
        .lock()
        .unwrap()
        .clone()
        .expect("alertmanager received a request");
    assert!(
        body.contains("\"endsAt\":\"1970-01-01T00:04:00Z\""),
        "still-firing alert should carry a resolveDuration-derived endsAt: {body}"
    );

    server.stop();
}

#[test]
fn apply_update_preserves_live_alerts_when_rule_identity_is_unchanged() {
    // Same `id` across old/new (as it would be if only `annotations`
    // changed — not part of a rule's identity, see
    // `config::rule_identity_hash`): live alert state must carry over.
    let mut old = Group {
        name: "g".into(),
        rules: vec![RuleKind::Alerting(AlertingRule {
            id: 1,
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };
    if let RuleKind::Alerting(ar) = &mut old.rules[0] {
        ar.alerts.insert(
            1,
            Alert {
                state: AlertState::Firing,
                active_at: 42,
                ..Default::default()
            },
        );
    }

    let new_group = Group {
        name: "g".into(),
        rules: vec![RuleKind::Alerting(AlertingRule {
            id: 1,
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            annotations: [("summary".to_string(), "updated".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        })],
        ..Default::default()
    };

    old.apply_update(new_group);

    if let RuleKind::Alerting(ar) = &old.rules[0] {
        assert_eq!(ar.annotations.get("summary").unwrap(), "updated");
        assert_eq!(
            ar.alerts.len(),
            1,
            "live alert state should be preserved when rule identity is unchanged"
        );
        assert_eq!(ar.alerts.get(&1).unwrap().active_at, 42);
    } else {
        panic!("expected alerting rule");
    }
}

#[test]
fn apply_update_resets_alert_state_when_rule_identity_changes() {
    // A changed `expr` gives the rule a different identity `id` (per
    // `config::rule_identity_hash`/upstream's `HashRule`), so it must
    // NOT inherit the old rule's live alert state, even though the name
    // is unchanged.
    let mut old = Group {
        name: "g".into(),
        rules: vec![RuleKind::Alerting(AlertingRule {
            id: 1,
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };
    if let RuleKind::Alerting(ar) = &mut old.rules[0] {
        ar.alerts.insert(
            1,
            Alert {
                state: AlertState::Firing,
                active_at: 42,
                ..Default::default()
            },
        );
    }

    let new_group = Group {
        name: "g".into(),
        rules: vec![RuleKind::Alerting(AlertingRule {
            id: 2,
            name: "A".into(),
            group_name: "g".into(),
            expr: "up > 1".into(),
            ..Default::default()
        })],
        ..Default::default()
    };

    old.apply_update(new_group);

    if let RuleKind::Alerting(ar) = &old.rules[0] {
        assert_eq!(ar.expr, "up > 1", "rule body should reflect the update");
        assert!(
            ar.alerts.is_empty(),
            "a changed rule identity must not inherit stale alert state"
        );
    } else {
        panic!("expected alerting rule");
    }
}

#[test]
fn start_and_stop_does_not_hang() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);
    let g = Group {
        name: "g".into(),
        // A long interval and zero start delay: the loop enters its
        // steady-state wait immediately and should return as soon as
        // `stop()` is called, without waiting out any real interval.
        interval: Duration::from_secs(3600),
        concurrency: 1,
        rules: vec![],
        ..Default::default()
    };
    let q: Arc<dyn Querier + Send + Sync> = Arc::new(Scripted { present: false });
    let handle = g.start(
        q,
        funcs,
        ctx,
        None,
        None,
        "http://vm".to_string(),
        None,
        Duration::from_secs(0),
    );
    handle.stop();
}

#[test]
fn hot_reload_republishes_snapshot_dropping_removed_rule_alerts() {
    let ctx = test_ctx();
    let funcs = default_funcs(&ctx);

    // A group whose single alerting rule is already firing.
    let mut g = Group {
        name: "g".into(),
        interval: Duration::from_secs(3600),
        concurrency: 1,
        rules: vec![RuleKind::Alerting(AlertingRule {
            id: 1,
            name: "A".into(),
            group_name: "g".into(),
            expr: "up".into(),
            r#for: Duration::from_secs(0),
            ..Default::default()
        })],
        ..Default::default()
    };
    g.eval_once(
        &Scripted { present: true },
        0,
        &funcs,
        &ctx,
        None,
        None,
        "http://vm",
    );
    let live = Arc::new(Mutex::new(g.snapshot()));
    assert_eq!(
        live.lock().unwrap().alerts.len(),
        1,
        "precondition: the alert is firing before the reload"
    );

    // Reload with the alerting rule removed (a different id -> not matched),
    // leaving only a recording rule.
    let new_group = Group {
        name: "g".into(),
        interval: Duration::from_secs(3600),
        concurrency: 1,
        rules: vec![RuleKind::Recording(RecordingRule {
            id: 2,
            name: "r".into(),
            expr: "up".into(),
            ..Default::default()
        })],
        ..Default::default()
    };

    let (update_tx, update_rx) = crossbeam_channel::unbounded::<Group>();
    let (_stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(1);
    update_tx.send(new_group).expect("queue the reload");

    // A short bounded wait: the queued update is consumed and the snapshot
    // republished immediately, before this returns `Elapsed` — no dependence
    // on any scheduled tick.
    let outcome = wait_or_apply_updates(
        &mut g,
        &stop_rx,
        &update_rx,
        &live,
        Duration::from_millis(20),
    );
    assert!(matches!(outcome, WaitOutcome::Elapsed));

    let snap = live.lock().unwrap();
    assert!(
        snap.alerts.is_empty(),
        "the removed rule's ghost alert must be dropped from the snapshot at reload time, not a tick later"
    );
    assert_eq!(snap.rules.len(), 1);
    assert_eq!(snap.rules[0].record.as_deref(), Some("r"));
}
