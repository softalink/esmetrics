//! esmetrics server logic, lib-ified so integration tests can start the
//! server in-process (bind `127.0.0.1:0`, discover the port, hit endpoints).

mod esmui;
pub mod flags;
pub mod signal;
pub mod wiring;

use std::borrow::Cow;
use std::io;
use std::sync::Arc;

use esm_http::{Request, ResponseWriter, Server};
use esm_insert::ingestserver::IngestServer;
use esm_insert::opentsdbhttp::OpentsdbHttpHandlers;
use esm_insert::InsertHandlers;
use esm_select::{SelectConfig, SelectHandlers};
use esm_storage::{OpenOptions, Storage};
use esm_streamaggr::{Aggregators, Options, PushFunc};

use crate::flags::Flags;

/// EsMetrics favicon (shield + pulse), served at `/favicon.ico`.
const FAVICON_ICO: &[u8] = include_bytes!("../../../assets/favicon.ico");
/// EsMetrics logo, served at `/logo.svg`.
const LOGO_SVG: &[u8] = include_bytes!("../../../assets/logo.svg");
use crate::wiring::{StorageProvider, StorageSink, StreamAggSink};

/// A running esmetrics instance: HTTP server + storage, plus the optional
/// Graphite/OpenTSDB TCP+UDP ingest listeners and the optional dedicated
/// OpenTSDB HTTP `/api/put` listener. Stop with [`App::stop`], which shuts
/// everything down and closes storage cleanly.
pub struct App {
    pub server: Server,
    graphite_server: Option<IngestServer>,
    opentsdb_server: Option<IngestServer>,
    opentsdbhttp_server: Option<Server>,
    storage: Option<Arc<Storage>>,
    /// Global stream aggregation (`-streamAggr.config`), stopped and flushed
    /// (into the still-open storage) before storage is closed.
    stream_aggregators: Option<Arc<Aggregators>>,
}

impl App {
    /// The bound listen address (real port when bound to `:0`).
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.server.local_addr()
    }

    /// The bound Graphite TCP/UDP address, if `-graphiteListenAddr` was set
    /// (real port when bound to a `:0`-style ephemeral address).
    pub fn graphite_addr(&self) -> Option<std::net::SocketAddr> {
        self.graphite_server.as_ref().map(IngestServer::local_addr)
    }

    /// The bound OpenTSDB telnet TCP/UDP address, if `-opentsdbListenAddr`
    /// was set (real port when bound to a `:0`-style ephemeral address).
    pub fn opentsdb_addr(&self) -> Option<std::net::SocketAddr> {
        self.opentsdb_server.as_ref().map(IngestServer::local_addr)
    }

    /// The bound dedicated OpenTSDB HTTP `/api/put` address, if
    /// `-opentsdbHTTPListenAddr` was set (real port when bound to a
    /// `:0`-style ephemeral address).
    pub fn opentsdb_http_addr(&self) -> Option<std::net::SocketAddr> {
        self.opentsdbhttp_server.as_ref().map(Server::local_addr)
    }

    /// Stops the HTTP server, the Graphite/OpenTSDB ingest listeners and the
    /// dedicated OpenTSDB HTTP listener (if enabled), and closes the storage
    /// (joining all its background threads). Mirrors the upstream's
    /// graceful shutdown order.
    pub fn stop(mut self) {
        self.server.stop();
        if let Some(server) = self.graphite_server.take() {
            server.stop();
        }
        if let Some(server) = self.opentsdb_server.take() {
            server.stop();
        }
        if let Some(server) = self.opentsdbhttp_server.take() {
            server.stop();
        }
        // Stop stream aggregation before closing storage: the final flush
        // writes aggregated output into the still-open storage, and dropping
        // the last handle releases the push callback's storage reference so
        // the `try_unwrap` below can reclaim it.
        if let Some(aggs) = self.stream_aggregators.take() {
            aggs.must_stop();
            drop(aggs);
        }
        if let Some(storage) = self.storage.take() {
            match Arc::try_unwrap(storage) {
                Ok(s) => s.must_close(),
                Err(_) => {
                    // In-flight requests still hold provider/sink clones for
                    // a moment; this only happens if stop() races a request.
                    log::warn!("storage still referenced at shutdown; skipping close");
                }
            }
        }
    }
}

/// Builds the select config from the command-line flags
/// (upstream flag → vmselect config).
fn select_config(flags: &Flags) -> SelectConfig {
    SelectConfig {
        max_concurrent_requests: flags.search_max_concurrent_requests,
        ..SelectConfig::default()
    }
}

/// Opens storage, binds the HTTP server and starts serving. The caller owns
/// shutdown via [`App::stop`].
pub fn run(flags: &Flags) -> io::Result<App> {
    if flags.search_max_workers_per_query > 0 {
        esm_common::query_workers::set_max_workers(flags.search_max_workers_per_query);
    }

    let storage = Arc::new(Storage::must_open(
        &flags.storage_data_path,
        OpenOptions {
            retention_msecs: flags.retention_msecs,
            ..Default::default()
        },
    ));

    // Global stream aggregation (`-streamAggr.config`), applied to every
    // ingestion path before storage.
    let (stream_agg_sink, stream_aggregators) = build_stream_agg(flags, Arc::clone(&storage))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    // Shared between the HTTP `/write`-style handlers and the optional
    // Graphite/OpenTSDB TCP+UDP ingest listeners below, so all ingestion
    // paths feed the same storage instance.
    let sink = Arc::new(StorageSink {
        storage: Arc::clone(&storage),
        stream_agg: stream_agg_sink,
    });
    let insert = InsertHandlers::new(Arc::clone(&sink));
    let select = SelectHandlers::with_config(
        StorageProvider {
            storage: Arc::clone(&storage),
        },
        select_config(flags),
    );

    let addr = normalize_listen_addr(&flags.http_listen_addr);
    let server = Server::bind(&addr)?;
    let handler_storage = Arc::clone(&storage);
    let snapshot_auth_key = flags.snapshot_auth_key.clone();
    server.serve(Arc::new(
        move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
            request_handler(
                req,
                w,
                &insert,
                &select,
                &handler_storage,
                &snapshot_auth_key,
            );
        },
    ));

    let graphite_server = if !flags.graphite_listen_addr.is_empty() {
        let addr = normalize_listen_addr(&flags.graphite_listen_addr);
        Some(esm_insert::ingestserver::serve_graphite(
            &addr,
            Arc::clone(&sink),
        )?)
    } else {
        None
    };
    let opentsdb_server = if !flags.opentsdb_listen_addr.is_empty() {
        let addr = normalize_listen_addr(&flags.opentsdb_listen_addr);
        Some(esm_insert::ingestserver::serve_opentsdb_telnet(
            &addr,
            Arc::clone(&sink),
        )?)
    } else {
        None
    };

    let opentsdbhttp_server = if !flags.opentsdb_http_listen_addr.is_empty() {
        let addr = normalize_listen_addr(&flags.opentsdb_http_listen_addr);
        let opentsdbhttp_server = Server::bind(&addr)?;
        let handlers = Arc::new(OpentsdbHttpHandlers::new(Arc::clone(&sink)));
        opentsdbhttp_server.serve(Arc::new(
            move |req: &mut Request<'_>, w: &mut ResponseWriter<'_>| {
                handlers.handle(req, w);
            },
        ));
        Some(opentsdbhttp_server)
    } else {
        None
    };

    Ok(App {
        server,
        graphite_server,
        opentsdb_server,
        opentsdbhttp_server,
        storage: Some(storage),
        stream_aggregators,
    })
}

/// Builds the global stream-aggregation stage from `-streamAggr.config` (and
/// the `-streamAggr.*` option flags). Returns the sink stage plus the shared
/// [`Aggregators`] handle (for shutdown flush). `(None, None)` when the flag
/// is unset.
fn build_stream_agg(
    flags: &Flags,
    storage: Arc<Storage>,
) -> Result<(Option<StreamAggSink>, Option<Arc<Aggregators>>), String> {
    let Some(path) = &flags.stream_aggr_config else {
        return Ok((None, None));
    };
    let yaml = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read -streamAggr.config {path:?}: {e}"))?;

    let storage_cb = Arc::clone(&storage);
    let push_func: PushFunc =
        Arc::new(move |tss| wiring::write_aggregated_to_storage(&storage_cb, tss));
    let opts = Options {
        dedup_interval_ms: flags.stream_aggr_dedup_interval_ms,
        keep_input: flags.stream_aggr_keep_input,
        drop_input_labels: flags.stream_aggr_drop_input_labels.clone(),
        ignore_old_samples: flags.stream_aggr_ignore_old_samples,
        ignore_first_intervals: flags.stream_aggr_ignore_first_intervals,
        flush_on_shutdown: flags.stream_aggr_flush_on_shutdown,
        enable_windows: flags.stream_aggr_enable_windows,
        ..Options::default()
    };
    let aggs = Arc::new(
        Aggregators::load_from_data(&yaml, push_func, &opts)
            .map_err(|e| format!("invalid -streamAggr.config {path:?}: {}", e.msg))?,
    );
    Ok((
        Some(StreamAggSink {
            aggregators: Arc::clone(&aggs),
            keep_input: flags.stream_aggr_keep_input,
        }),
        Some(aggs),
    ))
}

/// Go's `net.Listen` accepts `":8428"` (all interfaces); `std::net` needs an
/// explicit host.
fn normalize_listen_addr(addr: &str) -> Cow<'_, str> {
    if addr.starts_with(':') {
        Cow::Owned(format!("0.0.0.0{addr}"))
    } else {
        Cow::Borrowed(addr)
    }
}

/// Top-level request router (mirrors `requestHandler` in the upstream
/// `app/victoria-metrics/main.go` plus httpserver's built-in paths).
fn request_handler(
    req: &mut Request<'_>,
    w: &mut ResponseWriter<'_>,
    insert: &InsertHandlers<Arc<StorageSink>>,
    select: &SelectHandlers<StorageProvider>,
    storage: &Arc<Storage>,
    snapshot_auth_key: &str,
) {
    let path = req.path();
    if path.starts_with("/snapshot") || path == "/api/v1/admin/tsdb/snapshot" {
        handle_snapshot_request(req, w, storage, snapshot_auth_key);
        return;
    }
    match path {
        "/health" => {
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"OK");
            return;
        }
        // Port of the upstream `github.com/VictoriaMetrics/metrics` library's
        // `/metrics` handler (`metrics.WritePrometheus`), scoped to the
        // counters registered via `esm_common::metrics` — see that module's
        // doc for what's ported (counters only) vs skipped (gauges,
        // histograms, summaries, process metrics, HELP/TYPE lines).
        "/metrics" => {
            w.set_content_type("text/plain; charset=utf-8");
            let mut body = String::new();
            esm_common::metrics::write_prometheus(&mut body);
            w.write_body(body.as_bytes());
            return;
        }
        "/" => {
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(
                b"EsMetrics - secure, fast, memory-safe time-series database (Softalink LLC)\n\
                  Web UI: /esmui/\n",
            );
            return;
        }
        "/favicon.ico" => {
            w.set_content_type("image/x-icon");
            w.set_header("Cache-Control", "max-age=3600");
            w.write_body(FAVICON_ICO);
            return;
        }
        "/logo.svg" => {
            w.set_content_type("image/svg+xml");
            w.set_header("Cache-Control", "max-age=3600");
            w.write_body(LOGO_SVG);
            return;
        }
        // upstream: /internal/force_flush makes recently ingested data visible
        // to search immediately (used by tests and the bench harness).
        "/internal/force_flush" => {
            storage.force_flush();
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"OK");
            return;
        }
        "/internal/force_merge" => {
            if let Err(e) = storage.force_merge_partitions("") {
                log::warn!("force_merge failed: {e}");
            }
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"OK");
            return;
        }
        _ => {}
    }
    if esmui::handle(req, w) {
        return;
    }
    if insert.handle(req, w) {
        return;
    }
    if select.handle(req, w) {
        return;
    }
    w.set_status(404);
    w.set_content_type("text/plain; charset=utf-8");
    // Re-read the path: the borrow of `req` must not span the mutable
    // handler calls above (paths decode lazily and borrow the request).
    w.write_body(format!("unsupported path requested: {:?}\n", req.path()).as_bytes());
}

/// Handles /snapshot/{create,list,delete,delete_all} and the Prometheus
/// /api/v1/admin/tsdb/snapshot alias. Go: app/vmstorage RequestHandler.
fn handle_snapshot_request(
    req: &mut Request<'_>,
    w: &mut ResponseWriter<'_>,
    storage: &Arc<Storage>,
    auth_key: &str,
) {
    if !auth_key.is_empty() {
        let supplied = query_param(req, "authKey");
        if supplied.as_deref() != Some(auth_key) {
            w.set_status(401);
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"The provided authKey doesn't match -snapshotAuthKey\n");
            return;
        }
    }

    let path = req.path();
    let prometheus_compatible = path == "/api/v1/admin/tsdb/snapshot";
    let action = if prometheus_compatible {
        "/create"
    } else {
        path.strip_prefix("/snapshot").unwrap_or("")
    };

    match action {
        "/create" => {
            let name = storage.must_create_snapshot();
            if prometheus_compatible {
                w.write_json(
                    200,
                    &format!("{{\"status\":\"success\",\"data\":{{\"name\":\"{name}\"}}}}"),
                );
            } else {
                w.write_json(
                    200,
                    &format!("{{\"status\":\"ok\",\"snapshot\":\"{name}\"}}"),
                );
            }
        }
        "/list" => {
            let names = storage.must_list_snapshots();
            let quoted: Vec<String> = names.iter().map(|n| format!("\"{n}\"")).collect();
            w.write_json(
                200,
                &format!("{{\"status\":\"ok\",\"snapshots\":[{}]}}", quoted.join(",")),
            );
        }
        "/delete" => {
            let name = query_param(req, "snapshot").unwrap_or_default();
            match storage.delete_snapshot(&name) {
                Ok(()) => w.write_json(200, "{\"status\":\"ok\"}"),
                Err(e) => write_json_error(w, &e),
            }
        }
        "/delete_all" => {
            for name in storage.must_list_snapshots() {
                if let Err(e) = storage.delete_snapshot(&name) {
                    write_json_error(w, &e);
                    return;
                }
            }
            w.write_json(200, "{\"status\":\"ok\"}");
        }
        _ => {
            w.set_status(404);
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(format!("unsupported path requested: {path:?}\n").as_bytes());
        }
    }
}

fn write_json_error(w: &mut ResponseWriter<'_>, msg: &str) {
    let quoted = serde_json::to_string(msg).unwrap_or_else(|_| "\"internal error\"".to_string());
    w.write_json(500, &format!("{{\"status\":\"error\",\"msg\":{quoted}}}"));
}

fn query_param(req: &Request<'_>, key: &str) -> Option<String> {
    req.query_params()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

#[cfg(test)]
mod tests {
    use super::normalize_listen_addr;

    #[test]
    fn normalizes_go_style_listen_addrs() {
        assert_eq!(normalize_listen_addr(":8428"), "0.0.0.0:8428");
        assert_eq!(normalize_listen_addr("127.0.0.1:8428"), "127.0.0.1:8428");
        assert_eq!(normalize_listen_addr("localhost:80"), "localhost:80");
    }

    #[test]
    fn select_config_maps_flags() {
        use crate::flags::Flags;

        // 0 stays 0: esm-select resolves it to min(2 × cpus, 16) itself.
        let auto = super::select_config(&Flags::default());
        assert_eq!(auto.max_concurrent_requests, 0);

        let capped = super::select_config(&Flags {
            search_max_concurrent_requests: 2,
            ..Flags::default()
        });
        assert_eq!(capped.max_concurrent_requests, 2);
        // Everything else keeps the upstream defaults.
        assert_eq!(capped.max_queue_duration_ms, 10_000);
    }
}
