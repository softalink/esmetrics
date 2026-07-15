//! Tests for [`ScrapeConfigResolved::enable_compression`] — split out of
//! `scrapework_tests.rs` to keep that file under the repo's 800-line cap.
//! Still `mod compression_tests` from `scrapework.rs`'s point of view (see
//! the `#[path]` attribute there), so `super::*` below is `scrapework`'s
//! items.

use super::*;
use esm_http::{Request, ResponseWriter, Server, ServerConfig};
use std::sync::{Arc, Mutex};

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

/// Serves `body` on `/metrics`, binding with `capture_all_headers: true`
/// (see `esm_http::ServerConfig`'s doc — off by default, so
/// `Request::all_headers()` would otherwise always be empty), and records
/// into `saw_gzip_ae` whether the request carried an `Accept-Encoding`
/// header naming `gzip`.
fn start_metrics_stub_capturing_accept_encoding(
    body: &'static str,
    saw_gzip_ae: Arc<Mutex<bool>>,
) -> (String, Server) {
    let server = Server::bind_with_config(
        "127.0.0.1:0",
        ServerConfig {
            capture_all_headers: true,
            ..ServerConfig::default()
        },
    )
    .expect("bind stub server");
    let addr = server.local_addr().to_string();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            if req.path() == "/metrics" {
                let has_gzip_ae = req
                    .all_headers()
                    .iter()
                    .any(|(n, v)| n.eq_ignore_ascii_case("accept-encoding") && v.contains("gzip"));
                *saw_gzip_ae.lock().unwrap() = has_gzip_ae;
                w.set_content_type("text/plain");
                w.write_body(body.as_bytes());
            } else {
                w.write_status(404);
            }
        },
    ));
    (addr, server)
}

#[test]
fn enable_compression_true_sends_accept_encoding_gzip() {
    let saw_gzip_ae = Arc::new(Mutex::new(false));
    let (addr, server) =
        start_metrics_stub_capturing_accept_encoding("m 1\n", Arc::clone(&saw_gzip_ae));
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let cfg = base_cfg(); // enable_compression: true, base_cfg's default

    let r = Scraper::new(cfg).scrape(&client, &format!("http://{addr}/metrics"));

    assert!(r.up, "expected scrape to succeed, got error: {:?}", r.error);
    assert!(
        *saw_gzip_ae.lock().unwrap(),
        "expected Accept-Encoding: gzip to be sent when enable_compression=true"
    );

    server.stop();
}

#[test]
fn enable_compression_false_omits_accept_encoding_gzip() {
    let saw_gzip_ae = Arc::new(Mutex::new(false));
    let (addr, server) =
        start_metrics_stub_capturing_accept_encoding("m 1\n", Arc::clone(&saw_gzip_ae));
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let mut cfg = base_cfg();
    cfg.enable_compression = false;

    let r = Scraper::new(cfg).scrape(&client, &format!("http://{addr}/metrics"));

    assert!(
        r.up,
        "expected an uncompressed 200 response to still parse fine: {:?}",
        r.error
    );
    assert!(
        !*saw_gzip_ae.lock().unwrap(),
        "expected Accept-Encoding to be omitted when enable_compression=false"
    );

    server.stop();
}
