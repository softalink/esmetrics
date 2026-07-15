//! Tests for `sample_limit`/`label_limit` enforcement in [`super::Scraper`]
//! (Task 7) — split out of `scrapework_tests.rs` to keep both files under
//! the 800-line cap. Still `mod limits_tests` from `scrapework.rs`'s point
//! of view (see the `#[path]` attribute there), so `super::*` below is
//! `scrapework`'s items.

use super::*;
use crate::scrape::autometrics::AUTO_METRIC_COUNT;
use esm_http::{Request, ResponseWriter, Server};
use std::sync::Arc;

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

/// Like [`start_metrics_stub`] but serves an owned (heap) body — needed for
/// bodies generated at runtime (e.g. one larger than the parser's read
/// block size).
fn start_owned_metrics_stub(body: String) -> (String, Server) {
    let server = Server::bind("127.0.0.1:0").expect("bind stub server");
    let addr = server.local_addr().to_string();
    let body = Arc::new(body);
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

fn base_cfg() -> ScrapeConfigResolved {
    ScrapeConfigResolved {
        metric_relabel: ParsedConfigs::parse("[]").unwrap(),
        honor_labels: false,
        honor_timestamps: true,
        external_labels: vec![],
        target_labels: vec![],
        sample_limit: 0,
        label_limit: 0,
        scrape_timeout: Duration::from_secs(5),
        max_scrape_size: 0,
        enable_compression: true,
        auth: AuthConfig::default(),
        tls: TlsConfig::default(),
    }
}

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

fn has_scraped_series(r: &ScrapeResult, name: &str) -> bool {
    r.series.iter().any(|s| {
        s.labels
            .iter()
            .any(|l| l.name == "__name__" && l.value == name)
    })
}

#[test]
fn sample_limit_exceeded_drops_all_samples_and_bumps_counter() {
    // Stub returns 3 samples ("a", "b", "c"); sample_limit=2 -> the whole
    // scrape fails, none of the 3 scraped series are forwarded.
    let (addr, server) = start_metrics_stub("a 1\nb 2\nc 3\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg();
    cfg.sample_limit = 2;
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(!r.up, "sample_limit exceeded must fail the scrape");
    let err = r.error.as_ref().expect("expected an error");
    assert!(err.contains("sample_limit=2"), "unexpected error: {err}");
    for name in ["a", "b", "c"] {
        assert!(
            !has_scraped_series(&r, name),
            "series {name:?} must be dropped when sample_limit is exceeded"
        );
    }
    // samples_scraped/samples_post_relabel reflect the real (over-limit)
    // counts computed before the drop, matching upstream
    // (`scrapework.go:548-559`): the count is known before the limit
    // check runs.
    assert_eq!(r.samples_scraped, 3);
    assert_eq!(r.samples_post_relabel, 3);

    let limit = find_auto(&r, "scrape_samples_limit");
    assert_eq!(limit.samples[0].value, 2.0);
    let up = find_auto(&r, "up");
    assert_eq!(up.samples[0].value, 0.0);

    assert_eq!(sw.scrapes_skipped_by_sample_limit(), 1);
    assert_eq!(sw.scrapes_skipped_by_label_limit(), 0);

    server.stop();
}

#[test]
fn sample_limit_not_exceeded_forwards_all_samples() {
    let (addr, server) = start_metrics_stub("a 1\nb 2\nc 3\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg();
    cfg.sample_limit = 5;
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(r.up, "expected scrape to succeed, got error: {:?}", r.error);
    for name in ["a", "b", "c"] {
        assert!(
            has_scraped_series(&r, name),
            "series {name:?} must be forwarded when under sample_limit"
        );
    }
    let limit = find_auto(&r, "scrape_samples_limit");
    assert_eq!(limit.samples[0].value, 5.0);
    assert_eq!(sw.scrapes_skipped_by_sample_limit(), 0);

    server.stop();
}

#[test]
fn label_limit_exceeded_drops_all_samples_and_bumps_counter() {
    // "wide" has 3 labels total (__name__ + code + region); label_limit=2
    // -> the whole scrape fails, even the well-formed "narrow" series
    // (which alone would be under the limit) is dropped too.
    let (addr, server) = start_metrics_stub("narrow 1\nwide{code=\"200\",region=\"us\"} 2\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg();
    cfg.label_limit = 2;
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(!r.up, "label_limit exceeded must fail the scrape");
    let err = r.error.as_ref().expect("expected an error");
    assert!(err.contains("label_limit=2"), "unexpected error: {err}");
    for name in ["narrow", "wide"] {
        assert!(
            !has_scraped_series(&r, name),
            "series {name:?} must be dropped when label_limit is exceeded"
        );
    }
    // Upstream never reaches the `samplesPostRelabeling = len(...)`
    // assignment on this path (`addRows` returns before it runs) —
    // faithfully left at 0.
    assert_eq!(r.samples_post_relabel, 0);

    let limit = find_auto(&r, "scrape_labels_limit");
    assert_eq!(limit.samples[0].value, 2.0);
    let up = find_auto(&r, "up");
    assert_eq!(up.samples[0].value, 0.0);

    assert_eq!(sw.scrapes_skipped_by_label_limit(), 1);
    assert_eq!(sw.scrapes_skipped_by_sample_limit(), 0);

    server.stop();
}

#[test]
fn label_limit_not_exceeded_forwards_all_samples() {
    let (addr, server) = start_metrics_stub("narrow 1\nwide{code=\"200\",region=\"us\"} 2\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg();
    cfg.label_limit = 3; // "wide" has exactly 3 labels: __name__, code, region
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(r.up, "expected scrape to succeed, got error: {:?}", r.error);
    for name in ["narrow", "wide"] {
        assert!(
            has_scraped_series(&r, name),
            "series {name:?} must be forwarded when under label_limit"
        );
    }
    let limit = find_auto(&r, "scrape_labels_limit");
    assert_eq!(limit.samples[0].value, 3.0);
    assert_eq!(sw.scrapes_skipped_by_label_limit(), 0);

    server.stop();
}

#[test]
fn both_limits_disabled_omit_both_conditional_auto_metrics() {
    let (addr, server) = start_metrics_stub("a 1\n");
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let cfg = base_cfg(); // sample_limit = label_limit = 0 (disabled)
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(r.up);
    assert!(!r.series.iter().any(|s| s
        .labels
        .iter()
        .any(|l| l.name == "__name__" && l.value == "scrape_samples_limit")));
    assert!(!r.series.iter().any(|s| s
        .labels
        .iter()
        .any(|l| l.name == "__name__" && l.value == "scrape_labels_limit")));
    assert_eq!(r.series.len(), 1 + AUTO_METRIC_COUNT);

    server.stop();
}

#[test]
fn sample_limit_failure_marks_prior_series_stale_without_clobbering_last_series() {
    // Composition with Task 6 staleness: a successful scrape sees {a, b,
    // c}; the next scrape exceeds sample_limit and fails. That failure
    // must still mark the previously-seen series stale (up=0 behaves like
    // any other failed scrape for staleness purposes), and must NOT
    // clobber `last_series` — a subsequent successful scrape of the same
    // {a, b, c} body must not spuriously report them as newly added.
    let body = Arc::new(std::sync::Mutex::new("a 1\nb 2\nc 3\n".to_string()));
    let (addr, server) = {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        let addr = server.local_addr().to_string();
        let body_for_handler = Arc::clone(&body);
        server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                if req.path() == "/metrics" {
                    w.set_content_type("text/plain");
                    let b = body_for_handler.lock().unwrap().clone();
                    w.write_body(b.as_bytes());
                } else {
                    w.write_status(404);
                }
            },
        ));
        (addr, server)
    };
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg();
    cfg.sample_limit = 2;
    let mut sw = Scraper::new(cfg);
    let url = format!("http://{addr}/metrics");

    // First scrape: sample_limit not yet configured to trip (still 3
    // samples > limit=2) -- use a smaller body for the first, successful
    // scrape instead, then grow it to trip the limit on the second.
    *body.lock().unwrap() = "a 1\nb 2\n".to_string();
    let r1 = sw.scrape(&client, &url);
    assert!(r1.up, "expected first scrape to succeed: {:?}", r1.error);
    assert_eq!(find_auto(&r1, "scrape_series_added").samples[0].value, 2.0);

    // Second scrape: grows to 3 samples, tripping sample_limit=2 ->
    // fails. Because up flips to 0, "a" and "b" (from last_series) must
    // be stale-marked.
    *body.lock().unwrap() = "a 1\nb 2\nc 3\n".to_string();
    let r2 = sw.scrape(&client, &url);
    assert!(!r2.up, "expected second scrape to fail (sample_limit)");
    for name in ["a", "b"] {
        let stale = r2
            .series
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value == name)
            })
            .unwrap_or_else(|| panic!("expected a stale marker for {name:?}"));
        assert_eq!(
            stale.samples[0].value.to_bits(),
            esm_common::decimal::STALE_NAN.to_bits()
        );
    }
    // "c" was never in last_series (this is the scrape that would have
    // introduced it, but the whole scrape failed) so it must not appear
    // at all -- not as data, not as a stale marker.
    assert!(!has_scraped_series(&r2, "c"));

    // Third scrape: shrink back to the original {a, b} body (under the
    // limit again) -- since last_series was never clobbered by the
    // sample_limit failure, this recovered scrape must see BOTH "a" and
    // "b" as already-known (scrape_series_added == 0), not spuriously
    // re-added.
    *body.lock().unwrap() = "a 1\nb 2\n".to_string();
    let r3 = sw.scrape(&client, &url);
    assert!(
        r3.up,
        "expected recovered scrape to succeed: {:?}",
        r3.error
    );
    assert_eq!(
        find_auto(&r3, "scrape_series_added").samples[0].value,
        0.0,
        "recovered scrape must not over-count series_added; last_series was clobbered"
    );

    server.stop();
}

#[test]
fn label_limit_failure_counts_all_rows_across_multiple_read_blocks() {
    // Regression: a `label_limit` breach must NOT cut the body read short.
    // The over-limit series is placed EARLY (line 2); the body is padded to
    // well beyond the parser's 64KB read block so the abort, if it halted
    // the read loop, would leave `scrape_samples_scraped` truncated to only
    // the first block's rows. `scrape_samples_scraped` must equal the total
    // row count of the whole body.
    //
    // Each padded line is `metric_padded_name_<i> 1\n` (>= 24 bytes); 6000
    // of them is ~150KB, comfortably spanning multiple 64KB blocks.
    const PAD_ROWS: usize = 6000;
    let mut body = String::with_capacity(PAD_ROWS * 26 + 64);
    // Line 1: a normal single-label row.
    body.push_str("first_ok 1\n");
    // Line 2 (early): the over-limit series -- 3 labels total
    // (__name__, code, region) vs label_limit=2.
    body.push_str("wide{code=\"200\",region=\"us\"} 2\n");
    for i in 0..PAD_ROWS {
        body.push_str("metric_padded_name_");
        body.push_str(&i.to_string());
        body.push_str(" 1\n");
    }
    let total_rows = 2 + PAD_ROWS; // first_ok + wide + PAD_ROWS padding

    // Sanity: the body must actually exceed one 64KB read block, otherwise
    // this test wouldn't exercise the multi-block path it's guarding.
    assert!(
        body.len() > 64 * 1024,
        "test body ({} bytes) must exceed the 64KB read block to be meaningful",
        body.len()
    );

    let (addr, server) = start_owned_metrics_stub(body);
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg();
    cfg.label_limit = 2;
    let mut sw = Scraper::new(cfg);

    let r = sw.scrape(&client, &format!("http://{addr}/metrics"));

    assert!(!r.up, "label_limit breach must fail the scrape");
    // The whole body was parsed even though the abort fired on line 2:
    // scrape_samples_scraped == every row, not a truncated block count.
    assert_eq!(
        r.samples_scraped, total_rows,
        "scrape_samples_scraped must count all parsed rows across every block"
    );
    assert_eq!(
        find_auto(&r, "scrape_samples_scraped").samples[0].value,
        total_rows as f64
    );
    // Failure path invariants still hold: nothing forwarded, counter bumped,
    // post-relabel left at 0.
    assert_eq!(r.samples_post_relabel, 0);
    assert!(!has_scraped_series(&r, "first_ok"));
    assert!(!has_scraped_series(&r, "wide"));
    assert_eq!(sw.scrapes_skipped_by_label_limit(), 1);

    server.stop();
}
