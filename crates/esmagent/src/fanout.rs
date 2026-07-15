//! [`Fanout`]: the [`SeriesConsumer`] that dispatches every pushed series to
//! every configured destination's [`RemoteWriteCtx`].
//!
//! Port of the fan-out loop in `app/vmagent/remotewrite/remotewrite.go`'s
//! `tryPush`: each destination gets the same input series and applies its
//! own per-URL relabel independently, so one destination's relabel config
//! never affects what another destination receives.

use crate::rwctx::RemoteWriteCtx;
use crate::series::OwnedSeries;
use crate::sink::SeriesConsumer;

/// Owns every destination's [`RemoteWriteCtx`] and forwards each pushed
/// batch of series to all of them.
pub struct Fanout {
    ctxs: Vec<RemoteWriteCtx>,
}

impl Fanout {
    pub fn new(ctxs: Vec<RemoteWriteCtx>) -> Fanout {
        Fanout { ctxs }
    }

    /// Stops every destination's pipeline (see [`RemoteWriteCtx::stop`]).
    pub fn stop(self) {
        for ctx in self.ctxs {
            ctx.stop();
        }
    }
}

impl SeriesConsumer for Fanout {
    /// Forwards `series` to every destination. Each [`RemoteWriteCtx::push`]
    /// applies its own per-URL relabel to its own copy, so this does not
    /// need to (and must not) mutate or relabel `series` itself.
    fn push(&self, series: &[OwnedSeries]) {
        for ctx in &self.ctxs {
            ctx.push(series);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{AuthConfig, ClientConfig, TlsConfig};
    use crate::rwctx::RwCtxConfig;
    use esm_http::{Request, ResponseWriter, Server};
    use esm_protoparser::prompb::Sample;
    use esm_relabel::{Label, ParsedConfigs};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Stub remote-write endpoint that captures every request body it
    /// receives (after snappy+protobuf decoding is left to the caller) and
    /// always answers `204`.
    struct CaptureStub {
        server: Server,
        bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    fn start_capture_stub() -> CaptureStub {
        let server = Server::bind("127.0.0.1:0").expect("bind stub server");
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let bodies_for_handler = Arc::clone(&bodies);
        server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                let mut body = Vec::new();
                req.read_body_to(&mut body, 8 << 20).ok();
                bodies_for_handler.lock().unwrap().push(body);
                w.write_status(204);
            },
        ));
        CaptureStub { server, bodies }
    }

    /// Polls `check` until it returns `true` or `timeout` elapses, sleeping
    /// briefly between polls. Returns whether `check` was ever satisfied.
    fn wait_until(mut check: impl FnMut() -> bool, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            if check() {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn base_client_config(url: String) -> ClientConfig {
        ClientConfig {
            url,
            queues: 1,
            retry_min: Duration::from_millis(10),
            retry_max: Duration::from_millis(50),
            send_timeout: Duration::from_secs(2),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        }
    }

    /// Snappy-decompresses and protobuf-decodes `body` into `(name, value)`
    /// label pairs of its single time series.
    fn decode_labels(body: &[u8]) -> Vec<(String, String)> {
        let raw = snap::raw::Decoder::new().decompress_vec(body).unwrap();
        let wr = esm_protoparser::prompb::unmarshal_write_request(&raw).unwrap();
        assert_eq!(wr.timeseries.len(), 1, "expected exactly one series");
        wr.timeseries[0]
            .labels
            .iter()
            .map(|l| {
                (
                    String::from_utf8_lossy(l.name).into_owned(),
                    String::from_utf8_lossy(l.value).into_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn fanout_delivers_to_two_destinations_with_per_url_relabel() {
        let stub_a = start_capture_stub();
        let stub_b = start_capture_stub();
        let addr_a = stub_a.server.local_addr();
        let addr_b = stub_b.server.local_addr();

        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        // ctx B's per-URL relabel: unconditionally add {dc="b"} by matching
        // the always-present __name__ label with `.*` and replacing.
        let relabel_b = ParsedConfigs::parse(
            "- source_labels: [__name__]\n  regex: \".*\"\n  target_label: dc\n  replacement: b\n  action: replace\n",
        )
        .unwrap();

        // Tiny max_block_size so `push` enqueues a full block immediately,
        // no reliance on the periodic flush thread for this test.
        let ctx_a = RemoteWriteCtx::start(RwCtxConfig {
            client: base_client_config(format!("http://{addr_a}/")),
            url_relabel: None,
            queue_dir: dir_a.path().to_path_buf(),
            max_disk_bytes: 10_000_000,
            max_block_size: 1,
            flush_interval: Duration::from_secs(3600),
            stream_aggr_config: None,
            stream_aggr_keep_input: false,
            stream_aggr_dedup_interval_ms: 0,
        })
        .expect("start ctx a");

        let ctx_b = RemoteWriteCtx::start(RwCtxConfig {
            client: base_client_config(format!("http://{addr_b}/")),
            url_relabel: Some(relabel_b),
            queue_dir: dir_b.path().to_path_buf(),
            max_disk_bytes: 10_000_000,
            max_block_size: 1,
            flush_interval: Duration::from_secs(3600),
            stream_aggr_config: None,
            stream_aggr_keep_input: false,
            stream_aggr_dedup_interval_ms: 0,
        })
        .expect("start ctx b");

        let fanout = Fanout::new(vec![ctx_a, ctx_b]);

        let series = OwnedSeries {
            labels: vec![Label {
                name: "__name__".to_string(),
                value: "up".to_string(),
            }],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1000,
            }],
        };
        fanout.push(&[series]);

        assert!(
            wait_until(
                || !stub_a.bodies.lock().unwrap().is_empty(),
                Duration::from_secs(5),
            ),
            "destination A never received the series"
        );
        assert!(
            wait_until(
                || !stub_b.bodies.lock().unwrap().is_empty(),
                Duration::from_secs(5),
            ),
            "destination B never received the series"
        );

        let body_a = stub_a.bodies.lock().unwrap()[0].clone();
        let body_b = stub_b.bodies.lock().unwrap()[0].clone();
        let labels_a = decode_labels(&body_a);
        let labels_b = decode_labels(&body_b);

        assert!(
            !labels_a.iter().any(|(n, _)| n == "dc"),
            "destination A must not carry destination B's per-URL relabel: {labels_a:?}"
        );
        assert!(
            labels_b.iter().any(|(n, v)| n == "dc" && v == "b"),
            "destination B must carry dc=b from its per-URL relabel: {labels_b:?}"
        );

        fanout.stop();
        stub_a.server.stop();
        stub_b.server.stop();
    }
}
