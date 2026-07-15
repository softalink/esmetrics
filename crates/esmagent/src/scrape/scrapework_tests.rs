//! Tests for [`super::Scraper`]/[`super::ScrapeResult`] — split out of
//! `scrapework.rs` to keep that file under the 800-line cap. Still `mod
//! tests` from `scrapework.rs`'s point of view (see the `#[path]` attribute
//! there), so `super::*` below is `scrapework`'s items.

use super::*;
use crate::scrape::autometrics::AUTO_METRIC_COUNT;
use esm_http::{Request, ResponseWriter, Server};
use std::sync::Arc;

/// Scrapes once via a throwaway [`Scraper`] (fresh `last_series`, so
/// `scrape_series_added` isn't exercised by this helper — see the
/// dedicated `scrape_series_added_*` test for that).
fn scrape_once(
    client: &reqwest::blocking::Client,
    scrape_url: &str,
    cfg: ScrapeConfigResolved,
) -> ScrapeResult {
    Scraper::new(cfg).scrape(client, scrape_url)
}

/// Returns the scraped (non-auto-metric) series, i.e. `r.series` minus
/// the [`AUTO_METRIC_COUNT`] auto-metrics [`Scraper::scrape`] always
/// appends at the end.
fn scraped_only(r: &ScrapeResult) -> &[OwnedSeries] {
    &r.series[..r.series.len() - AUTO_METRIC_COUNT]
}

/// Finds an auto-metric series by its `__name__` value.
fn find_auto<'a>(r: &'a ScrapeResult, name: &str) -> &'a OwnedSeries {
    r.series
        .iter()
        .rev()
        .find(|s| {
            s.labels
                .iter()
                .any(|l| l.name == "__name__" && l.value == name)
        })
        .unwrap_or_else(|| panic!("auto-metric {name:?} not found in result.series"))
}

/// Serves `body` (status 200, `text/plain`) on `/metrics`; any other
/// path gets 404. Mirrors `crate::client`'s stub-server test pattern.
/// Returns the `host:port` string and the running [`Server`] (stopped
/// on drop of the returned guard via `Server::stop`, called by the
/// caller once done).
fn start_metrics_stub(body: &'static str) -> (String, Server) {
    let server = Server::bind("127.0.0.1:0").expect("bind stub server");
    let addr = server.local_addr().to_string();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.path() == "/metrics" {
                w.set_content_type("text/plain");
                w.write_body(body.as_bytes());
            } else {
                w.write_status(404);
            }
        },
    ));
    (addr, server)
}

/// Serves the current contents of `body` (status 200, `text/plain`) on
/// `/metrics`, re-reading it on every request — lets a test change what a
/// running target returns mid-test by mutating `*body.lock().unwrap()`.
/// Any other path gets 404.
fn start_dynamic_stub(body: std::sync::Arc<std::sync::Mutex<String>>) -> (String, Server) {
    let server = Server::bind("127.0.0.1:0").expect("bind stub server");
    let addr = server.local_addr().to_string();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.path() == "/metrics" {
                w.set_content_type("text/plain");
                let b = body.lock().unwrap().clone();
                w.write_body(b.as_bytes());
            } else {
                w.write_status(404);
            }
        },
    ));
    (addr, server)
}

/// Serves a fixed `status` (no body) on every path.
fn start_status_stub(status: u16) -> (String, Server) {
    let server = Server::bind("127.0.0.1:0").expect("bind stub server");
    let addr = server.local_addr().to_string();
    server.serve(Arc::new(
        move |_req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            w.write_status(status);
        },
    ));
    (addr, server)
}

fn label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn base_cfg(target_labels: Vec<Label>, external_labels: Vec<Label>) -> ScrapeConfigResolved {
    ScrapeConfigResolved {
        metric_relabel: ParsedConfigs::parse("[]").unwrap(),
        honor_labels: false,
        honor_timestamps: true,
        external_labels,
        target_labels,
        sample_limit: 0,
        label_limit: 0,
        scrape_timeout: Duration::from_secs(5),
        max_scrape_size: 0,
        enable_compression: true,
        auth: AuthConfig::default(),
        tls: TlsConfig::default(),
    }
}

#[test]
fn scrape_once_parses_and_merges_target_labels() {
    let (addr, server) = start_metrics_stub("up_metric{code=\"200\"} 5\nother 7\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let cfg = base_cfg(
        vec![label("job", "node"), label("instance", "h1")],
        vec![label("env", "prod")],
    );

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up, "expected scrape to succeed, got error: {:?}", r.error);
    assert_eq!(r.samples_scraped, 2);
    assert_eq!(r.samples_post_relabel, 2);
    assert_eq!(scraped_only(&r).len(), 2);

    let up = scraped_only(&r)
        .iter()
        .find(|s| {
            s.labels
                .iter()
                .any(|l| l.name == "__name__" && l.value == "up_metric")
        })
        .expect("up_metric series present");
    assert!(up
        .labels
        .iter()
        .any(|l| l.name == "code" && l.value == "200"));
    assert!(up
        .labels
        .iter()
        .any(|l| l.name == "job" && l.value == "node"));
    assert!(up
        .labels
        .iter()
        .any(|l| l.name == "instance" && l.value == "h1"));
    assert!(up
        .labels
        .iter()
        .any(|l| l.name == "env" && l.value == "prod"));
    assert_eq!(up.samples.len(), 1);
    assert_eq!(up.samples[0].value, 5.0);

    server.stop();
}

#[test]
fn scrape_once_reports_down_on_failure_status() {
    let (addr, server) = start_status_stub(500);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let cfg = base_cfg(vec![], vec![]);

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(!r.up);
    assert!(r.error.is_some());
    assert!(scraped_only(&r).is_empty());
    assert_eq!(r.samples_scraped, 0);

    server.stop();
}

#[test]
fn scrape_once_reports_down_on_connection_failure() {
    // Nothing listening on this port.
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let cfg = base_cfg(vec![], vec![]);

    let r = scrape_once(&client, "http://127.0.0.1:1/metrics", cfg);

    assert!(!r.up);
    assert!(r.error.is_some());
}

#[test]
fn honor_labels_true_keeps_scraped_value_on_conflict() {
    let (addr, server) = start_metrics_stub("m{job=\"scraped\"} 1\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg(vec![label("job", "node")], vec![]);
    cfg.honor_labels = true;

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up);
    let s = &scraped_only(&r)[0];
    assert!(s
        .labels
        .iter()
        .any(|l| l.name == "job" && l.value == "scraped"));
    assert!(!s.labels.iter().any(|l| l.name == "exported_job"));

    server.stop();
}

#[test]
fn honor_labels_false_renames_scraped_value_on_conflict() {
    let (addr, server) = start_metrics_stub("m{job=\"scraped\"} 1\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let cfg = base_cfg(vec![label("job", "node")], vec![]);

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up);
    let s = &scraped_only(&r)[0];
    assert!(s
        .labels
        .iter()
        .any(|l| l.name == "job" && l.value == "node"));
    assert!(s
        .labels
        .iter()
        .any(|l| l.name == "exported_job" && l.value == "scraped"));

    server.stop();
}

#[test]
fn metric_relabel_drop_removes_the_row() {
    let (addr, server) = start_metrics_stub("keep_me 1\ndrop_me 2\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg(vec![], vec![]);
    cfg.metric_relabel =
        ParsedConfigs::parse("- source_labels: [__name__]\n  regex: \"drop_me\"\n  action: drop\n")
            .unwrap();

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up);
    assert_eq!(r.samples_scraped, 2);
    assert_eq!(r.samples_post_relabel, 1);
    assert_eq!(scraped_only(&r).len(), 1);
    assert!(scraped_only(&r)[0]
        .labels
        .iter()
        .any(|l| l.name == "__name__" && l.value == "keep_me"));

    server.stop();
}

#[test]
fn metric_relabel_sees_target_labels_but_not_external_labels() {
    // Locks the pipeline order (upstream addRow):
    //   target_labels merged BEFORE metric_relabel, external_labels
    //   merged AFTER. A metric_relabel that drops on a TARGET label
    //   (job) must fire; one keyed on an EXTERNAL label name (env)
    //   must NOT match, because external labels don't exist yet at
    //   relabel time.
    let (addr, server) = start_metrics_stub("m 1\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg(vec![label("job", "node")], vec![label("env", "prod")]);
    cfg.metric_relabel = ParsedConfigs::parse(
        // Drop if job==node (a target label) OR if env==prod (an
        // external label). Only the first clause can ever match here,
        // proving external labels are added post-relabel.
        "- source_labels: [job]\n  regex: node\n  action: drop\n\
         - source_labels: [env]\n  regex: prod\n  action: drop\n",
    )
    .unwrap();

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up);
    assert_eq!(r.samples_scraped, 1);
    // The series is dropped ONLY because the job target label was
    // visible to metric_relabel. If target labels were merged after
    // relabel (the wrong order), the first drop clause could not match
    // and the row would survive.
    assert_eq!(
        r.samples_post_relabel, 0,
        "target-label drop rule did not fire"
    );
    assert!(scraped_only(&r).is_empty());

    server.stop();
}

#[test]
fn external_label_is_added_after_relabel_and_survives() {
    // Companion to the drop test: with a metric_relabel keyed only on
    // the external label name (which isn't present at relabel time),
    // the row survives AND the external label still ends up on the
    // emitted series (merged in the post-relabel step).
    let (addr, server) = start_metrics_stub("m 1\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg(vec![label("job", "node")], vec![label("env", "prod")]);
    cfg.metric_relabel =
        ParsedConfigs::parse("- source_labels: [env]\n  regex: prod\n  action: drop\n").unwrap();

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up);
    assert_eq!(
        r.samples_post_relabel, 1,
        "row wrongly dropped on an absent external label"
    );
    let s = &scraped_only(&r)[0];
    assert!(s
        .labels
        .iter()
        .any(|l| l.name == "job" && l.value == "node"));
    assert!(s
        .labels
        .iter()
        .any(|l| l.name == "env" && l.value == "prod"));

    server.stop();
}

#[test]
fn finalize_strips_tmp_labels_introduced_by_relabel() {
    // A metric_relabel that writes a __tmp_* label must not leak that
    // label into the emitted series (FinalizeLabels), while __name__
    // is preserved.
    let (addr, server) = start_metrics_stub("m 1\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg(vec![], vec![]);
    cfg.metric_relabel = ParsedConfigs::parse(
        "- source_labels: [__name__]\n  target_label: __tmp_keep\n  action: replace\n",
    )
    .unwrap();

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up);
    let s = &scraped_only(&r)[0];
    assert!(!s.labels.iter().any(|l| l.name == "__tmp_keep"));
    assert!(s
        .labels
        .iter()
        .any(|l| l.name == "__name__" && l.value == "m"));

    server.stop();
}

#[test]
fn max_scrape_size_cap_fails_the_scrape() {
    let (addr, server) = start_metrics_stub("m 1\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg(vec![], vec![]);
    cfg.max_scrape_size = 2; // body is longer than 2 bytes

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(!r.up);
    let err = r.error.expect("expected an error");
    assert!(err.contains("max_scrape_size"), "unexpected error: {err}");

    server.stop();
}

#[test]
fn honor_timestamps_false_uses_scrape_time_not_embedded_timestamp() {
    let (addr, server) = start_metrics_stub("m 1 999999999000\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg(vec![], vec![]);
    cfg.honor_timestamps = false;

    let before = now_millis();
    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);
    let after = now_millis();

    assert!(r.up);
    let ts = scraped_only(&r)[0].samples[0].timestamp;
    assert_ne!(ts, 999_999_999_000);
    assert!(
        ts >= before && ts <= after,
        "ts {ts} not in [{before}, {after}]"
    );

    server.stop();
}

#[test]
fn honor_timestamps_true_keeps_embedded_timestamp() {
    let (addr, server) = start_metrics_stub("m 1 999999999000\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let cfg = base_cfg(vec![], vec![]); // honor_timestamps: true by default

    let r = scrape_once(&client, &format!("http://{addr}/metrics"), cfg);

    assert!(r.up);
    assert_eq!(scraped_only(&r)[0].samples[0].timestamp, 999_999_999_000);

    server.stop();
}

#[test]
fn emits_auto_metrics_including_up() {
    let (addr, server) = start_metrics_stub("a 1\nb 2\n");
    let cfg = base_cfg(vec![label("job", "node"), label("instance", "h1")], vec![]);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(r.up, "expected scrape to succeed, got error: {:?}", r.error);
    let names: Vec<&str> = r
        .series
        .iter()
        .filter_map(|s| {
            s.labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.as_str())
        })
        .collect();
    for expected in [
        "up",
        "scrape_duration_seconds",
        "scrape_response_size_bytes",
        "scrape_samples_post_metric_relabeling",
        "scrape_samples_scraped",
        "scrape_series_added",
        "scrape_timeout_seconds",
    ] {
        assert!(
            names.contains(&expected),
            "missing auto-metric {expected:?} in {names:?}"
        );
    }
    assert_eq!(r.series.len(), 2 + AUTO_METRIC_COUNT);

    let up = find_auto(&r, "up");
    assert_eq!(up.samples[0].value, 1.0);
    assert!(up
        .labels
        .iter()
        .any(|l| l.name == "job" && l.value == "node"));
    assert!(up
        .labels
        .iter()
        .any(|l| l.name == "instance" && l.value == "h1"));

    let scraped = find_auto(&r, "scrape_samples_scraped");
    assert_eq!(scraped.samples[0].value, 2.0);
    let post_relabel = find_auto(&r, "scrape_samples_post_metric_relabeling");
    assert_eq!(post_relabel.samples[0].value, 2.0);
    let timeout = find_auto(&r, "scrape_timeout_seconds");
    assert_eq!(timeout.samples[0].value, 5.0); // base_cfg's Duration::from_secs(5)
    let size = find_auto(&r, "scrape_response_size_bytes");
    assert_eq!(size.samples[0].value, r.response_size as f64);

    server.stop();
}

#[test]
fn up_is_zero_on_failure() {
    let (addr, server) = start_status_stub(500);
    let cfg = base_cfg(vec![label("job", "node")], vec![]);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(!r.up);
    assert!(scraped_only(&r).is_empty());
    // All seven auto-metrics are still emitted on failure, `up`
    // carrying the target labels at value 0.
    assert_eq!(r.series.len(), AUTO_METRIC_COUNT);
    let up = find_auto(&r, "up");
    assert_eq!(up.samples[0].value, 0.0);
    assert!(up
        .labels
        .iter()
        .any(|l| l.name == "job" && l.value == "node"));

    assert_eq!(
        find_auto(&r, "scrape_samples_scraped").samples[0].value,
        0.0
    );
    assert_eq!(
        find_auto(&r, "scrape_samples_post_metric_relabeling").samples[0].value,
        0.0
    );
    assert_eq!(find_auto(&r, "scrape_series_added").samples[0].value, 0.0);
    assert_eq!(
        find_auto(&r, "scrape_response_size_bytes").samples[0].value,
        0.0
    );

    server.stop();
}

#[test]
fn scrape_series_added_counts_only_new_series_since_last_scrape() {
    let (addr, server) = start_metrics_stub("a 1\n");
    let cfg = base_cfg(vec![], vec![]);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut sw = Scraper::new(cfg);

    // First scrape: both series ("a") are new.
    let r1 = sw.scrape(&client, &format!("http://{addr}/metrics"));
    assert!(r1.up);
    assert_eq!(find_auto(&r1, "scrape_series_added").samples[0].value, 1.0);

    // Second scrape of the same body: nothing new.
    let r2 = sw.scrape(&client, &format!("http://{addr}/metrics"));
    assert!(r2.up);
    assert_eq!(find_auto(&r2, "scrape_series_added").samples[0].value, 0.0);

    server.stop();
}

#[test]
fn scrape_series_added_counts_a_series_added_on_a_later_scrape() {
    let server = Server::bind("127.0.0.1:0").expect("bind stub server");
    let addr = server.local_addr().to_string();
    let body = Arc::new(std::sync::Mutex::new("a 1\n"));
    let body_for_handler = Arc::clone(&body);
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.path() == "/metrics" {
                w.set_content_type("text/plain");
                let b = *body_for_handler.lock().unwrap();
                w.write_body(b.as_bytes());
            } else {
                w.write_status(404);
            }
        },
    ));

    let cfg = base_cfg(vec![], vec![]);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut sw = Scraper::new(cfg);

    let r1 = sw.scrape(&client, &format!("http://{addr}/metrics"));
    assert!(r1.up);
    assert_eq!(find_auto(&r1, "scrape_series_added").samples[0].value, 1.0);

    // Second scrape adds a brand-new series ("b") alongside "a".
    *body.lock().unwrap() = "a 1\nb 2\n";
    let r2 = sw.scrape(&client, &format!("http://{addr}/metrics"));
    assert!(r2.up);
    assert_eq!(
        find_auto(&r2, "scrape_series_added").samples[0].value,
        1.0,
        "only \"b\" is new on the second scrape"
    );

    server.stop();
}

#[test]
fn series_gone_gets_stale_marker() {
    let body = Arc::new(std::sync::Mutex::new("a 1\nb 2\n".to_string()));
    let (addr, server) = start_dynamic_stub(body.clone());
    let cfg = base_cfg(vec![], vec![]);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut sw = Scraper::new(cfg);

    let r1 = sw.scrape(&client, &format!("http://{addr}/metrics"));
    assert!(r1.up, "expected first scrape to succeed: {:?}", r1.error);

    // "b" disappears on the second scrape.
    *body.lock().unwrap() = "a 1\n".to_string();
    let r2 = sw.scrape(&client, &format!("http://{addr}/metrics"));
    assert!(r2.up, "expected second scrape to succeed: {:?}", r2.error);

    let stale = r2.series.iter().find(|s| {
        s.labels
            .iter()
            .any(|l| l.name == "__name__" && l.value == "b")
    });
    assert!(
        stale.is_some(),
        "expected a stale marker for vanished series \"b\""
    );
    let stale = stale.unwrap();
    assert_eq!(stale.samples.len(), 1);
    assert_eq!(
        stale.samples[0].value.to_bits(),
        esm_common::decimal::STALE_NAN.to_bits()
    );

    // "a" is still present, so it must NOT get a stale marker.
    let a_series: Vec<_> = r2
        .series
        .iter()
        .filter(|s| {
            s.labels
                .iter()
                .any(|l| l.name == "__name__" && l.value == "a")
        })
        .collect();
    assert_eq!(
        a_series.len(),
        1,
        "\"a\" should appear exactly once, not stale-marked"
    );
    assert_ne!(
        a_series[0].samples[0].value.to_bits(),
        esm_common::decimal::STALE_NAN.to_bits()
    );

    server.stop();
}

#[test]
fn mark_stale_all_emits_for_tracked_series() {
    let (addr, server) = start_metrics_stub("a 1\nb 2\n");
    let cfg = base_cfg(vec![], vec![]);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));
    assert!(r.up, "expected scrape to succeed: {:?}", r.error);

    let stale = sw.mark_stale_all(now_millis());

    let names: Vec<&str> = stale
        .iter()
        .filter_map(|s| {
            s.labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.as_str())
        })
        .collect();
    assert!(
        names.contains(&"a"),
        "missing stale marker for \"a\": {names:?}"
    );
    assert!(
        names.contains(&"b"),
        "missing stale marker for \"b\": {names:?}"
    );
    assert!(
        names.contains(&"up"),
        "missing stale auto-metric \"up\": {names:?}"
    );
    assert_eq!(
        stale.len(),
        2 + AUTO_METRIC_COUNT,
        "expected 2 scraped series + {AUTO_METRIC_COUNT} auto-metrics, all stale"
    );
    for s in &stale {
        assert_eq!(
            s.samples[0].value.to_bits(),
            esm_common::decimal::STALE_NAN.to_bits(),
            "series {:?} was not stale-marked",
            s.labels
        );
    }

    server.stop();
}

#[test]
fn failed_scrape_marks_all_stale_once() {
    let (addr, server) = start_metrics_stub("a 1\nb 2\n");
    let cfg = base_cfg(vec![], vec![]);
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let mut sw = Scraper::new(cfg);
    let url = format!("http://{addr}/metrics");

    let r1 = sw.scrape(&client, &url);
    assert!(r1.up, "expected first scrape to succeed: {:?}", r1.error);
    // Fully drop the server (not just `.stop()`) so the listening socket
    // closes and subsequent connections are refused promptly instead of
    // hanging in the OS accept backlog until the client timeout.
    drop(server);

    // First failure: everything tracked from r1 ("a", "b") should be
    // stale-marked exactly once.
    let r2 = sw.scrape(&client, &url);
    assert!(!r2.up, "expected second scrape to fail (server dropped)");
    let stale_names: Vec<&str> = r2
        .series
        .iter()
        .filter_map(|s| {
            s.labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.as_str())
        })
        .collect();
    assert!(stale_names.contains(&"a"));
    assert!(stale_names.contains(&"b"));
    for s in &r2.series {
        if s.labels
            .iter()
            .any(|l| l.name == "__name__" && (l.value == "a" || l.value == "b"))
        {
            assert_eq!(
                s.samples[0].value.to_bits(),
                esm_common::decimal::STALE_NAN.to_bits()
            );
        }
    }
    assert_eq!(
        r2.series.len(),
        2 + AUTO_METRIC_COUNT,
        "expected exactly the 2 stale scraped series + auto-metrics"
    );

    // Second consecutive failure: no NEW stale markers — only the
    // AUTO_METRIC_COUNT auto-metrics, no scraped/stale series.
    let r3 = sw.scrape(&client, &url);
    assert!(
        !r3.up,
        "expected third scrape to fail (server still dropped)"
    );
    assert_eq!(
        r3.series.len(),
        AUTO_METRIC_COUNT,
        "a second consecutive failure must not re-emit stale markers"
    );
}

#[test]
fn failed_scrape_does_not_clobber_last_series() {
    let body = Arc::new(std::sync::Mutex::new("a 1\nb 2\n".to_string()));
    let (addr, server) = start_dynamic_stub(body.clone());
    let cfg = base_cfg(vec![], vec![]);
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let mut sw = Scraper::new(cfg);
    let url = format!("http://{addr}/metrics");

    let r1 = sw.scrape(&client, &url);
    assert!(r1.up, "expected first scrape to succeed: {:?}", r1.error);

    // Fail the scrape (server dropped so the port is freed and the
    // connection is refused), then recover with the SAME body as r1.
    drop(server);
    let r2 = sw.scrape(&client, &url);
    assert!(!r2.up, "expected second scrape to fail (server dropped)");
    // last_series must still be {a, b} from r1, not clobbered by the
    // failure — both get stale-marked once here.
    assert_eq!(r2.series.len(), 2 + AUTO_METRIC_COUNT);

    let (addr2, server2) = start_dynamic_stub(body.clone());
    // Re-scrape the identical body against the restarted server. Since
    // last_series was never clobbered by the failure, this recovered
    // scrape must see BOTH "a" and "b" as already-known (not spuriously
    // re-added), and must NOT emit any stale markers (nothing vanished
    // relative to the still-intact last_series set).
    let r3 = sw.scrape(&client, &format!("http://{addr2}/metrics"));
    assert!(
        r3.up,
        "expected recovered scrape to succeed: {:?}",
        r3.error
    );
    assert_eq!(
        find_auto(&r3, "scrape_series_added").samples[0].value,
        0.0,
        "recovered scrape must not over-count series_added"
    );
    assert_eq!(
        r3.series.len(),
        2 + AUTO_METRIC_COUNT,
        "recovered scrape must not carry any stale markers"
    );
    for s in &r3.series {
        assert_ne!(
            s.samples[0].value.to_bits(),
            esm_common::decimal::STALE_NAN.to_bits(),
            "recovered scrape spuriously stale-marked {:?}",
            s.labels
        );
    }

    server2.stop();
}
