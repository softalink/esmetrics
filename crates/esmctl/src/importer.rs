//! The destination importer: batches time series and POSTs them to
//! `/api/v1/import` as newline-delimited VM JSON. Ports `app/vmctl/vm/vm.go`
//! and `vm/timeseries.go`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use esm_common::decimal::{round_to_decimal_digits, round_to_significant_figures};
use reqwest::blocking::Client;

use crate::auth::AuthConfig;
use crate::backoff::{Backoff, RetryError};
use crate::transport::RateLimitedReader;

/// gzip-compresses `data` (level 1, matching upstream's `gzip.NewWriterLevel`).
fn gzip(data: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(data)?;
    enc.finish()
}

/// One series to import.
pub(crate) struct Series {
    pub(crate) name: String,
    pub(crate) labels: Vec<(String, String)>,
    pub(crate) timestamps: Vec<i64>,
    pub(crate) values: Vec<f64>,
}

/// Configuration for the [`Importer`].
pub(crate) struct ImporterConfig {
    pub(crate) addr: String,
    pub(crate) auth: AuthConfig,
    pub(crate) http: Client,
    pub(crate) concurrency: usize,
    pub(crate) batch_size: usize,
    pub(crate) significant_figures: i32,
    pub(crate) round_digits: i32,
    pub(crate) extra_labels: Vec<String>,
    pub(crate) backoff: Arc<Backoff>,
    /// `-vm-compress`: gzip the import request body.
    pub(crate) compress: bool,
    /// `-vm-rate-limit`: bytes/second across all import workers (`0` = off).
    pub(crate) rate_limit: i64,
}

struct Shared {
    import_path: String,
    auth: AuthConfig,
    http: Client,
    backoff: Arc<Backoff>,
    batch_size: usize,
    significant_figures: i32,
    round_digits: i32,
    compress: bool,
    rate_limit: i64,
    /// Shared byte-rate limiter across all import workers (ports `im.rl`).
    rl: Arc<crate::transport::Limiter>,
    first_error: Mutex<Option<String>>,
    aborted: AtomicBool,
    /// Import statistics, aggregated across workers. Ports `vm.stats` plus the
    /// `vmctl_importer_*` counters (exposed here only as the end-of-run summary
    /// — a one-shot CLI has no scrape endpoint to publish counters to).
    stats: Stats,
}

/// Aggregated importer counters. Ports the `stats` struct in `vm/stats.go`.
struct Stats {
    start: Instant,
    samples: AtomicU64,
    bytes: AtomicU64,
    requests: AtomicU64,
    retries: AtomicU64,
    idle_nanos: AtomicU64,
}

/// Batches incoming [`Series`] and imports them concurrently. Ports
/// `vm.Importer`.
pub(crate) struct Importer {
    tx: Option<Sender<Series>>,
    handles: Vec<JoinHandle<()>>,
    shared: Arc<Shared>,
}

impl Importer {
    /// Builds an importer, pinging `/health` and starting the worker pool.
    /// Ports `NewImporter`.
    pub(crate) fn new(cfg: ImporterConfig) -> Result<Importer, String> {
        if cfg.concurrency < 1 {
            return Err("concurrency can't be lower than 1".to_string());
        }
        let addr = cfg.addr.trim_end_matches('/').to_string();
        let import_path = crate::native::add_extra_labels_to_import_path(
            &format!("{addr}/api/v1/import"),
            &cfg.extra_labels,
        )?;

        // Ping /health.
        let ping = cfg.auth.apply(cfg.http.get(format!("{addr}/health")));
        let resp = ping
            .send()
            .map_err(|e| format!("ping to {addr:?} failed: {e}"))?;
        if resp.status().as_u16() != 200 {
            return Err(format!(
                "ping to {addr:?} failed: bad status {}",
                resp.status()
            ));
        }

        let batch_size = if cfg.batch_size < 1 {
            100_000
        } else {
            cfg.batch_size
        };
        let shared = Arc::new(Shared {
            import_path,
            auth: cfg.auth,
            http: cfg.http,
            backoff: cfg.backoff,
            batch_size,
            significant_figures: cfg.significant_figures,
            round_digits: cfg.round_digits,
            compress: cfg.compress,
            rate_limit: cfg.rate_limit,
            rl: Arc::new(crate::transport::Limiter::new(cfg.rate_limit)),
            first_error: Mutex::new(None),
            aborted: AtomicBool::new(false),
            stats: Stats {
                start: Instant::now(),
                samples: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                requests: AtomicU64::new(0),
                retries: AtomicU64::new(0),
                idle_nanos: AtomicU64::new(0),
            },
        });

        let (tx, rx) = std::sync::mpsc::channel::<Series>();
        let rx = Arc::new(Mutex::new(rx));
        let mut handles = Vec::with_capacity(cfg.concurrency);
        for _ in 0..cfg.concurrency {
            let rx = Arc::clone(&rx);
            let shared = Arc::clone(&shared);
            handles.push(
                std::thread::Builder::new()
                    .name("esmctl-importer".into())
                    .spawn(move || worker(&rx, &shared))
                    .expect("spawn importer worker"),
            );
        }

        Ok(Importer {
            tx: Some(tx),
            handles,
            shared,
        })
    }

    /// Queues a series for import. Returns `Err` if a worker has already
    /// recorded a fatal import error. Ports `Importer.Input`.
    pub(crate) fn input(&self, ts: Series) -> Result<(), String> {
        if crate::signal::is_cancelled() {
            return Err("execution cancelled".to_string());
        }
        if self.shared.aborted.load(Ordering::SeqCst) {
            return Err(self
                .shared
                .first_error
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "import process aborted".to_string()));
        }
        match &self.tx {
            Some(tx) => tx.send(ts).map_err(|_| "importer is closed".to_string()),
            None => Err("importer is closed".to_string()),
        }
    }

    /// Flushes and stops all workers, prints the import stats summary, and
    /// returns the first import error if any. Ports `Importer.Close` plus the
    /// `log.Print(im.Stats())` the processors emit.
    pub(crate) fn close(mut self) -> Result<(), String> {
        self.tx = None; // drop sender → workers drain and exit
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
        log::info!("{}", self.shared.stats.summary());
        let err = self.shared.first_error.lock().unwrap().clone();
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Stats {
    /// Renders the human-readable end-of-run summary. Ports `stats.String`.
    fn summary(&self) -> String {
        let total = self.start.elapsed();
        let secs = total.as_secs_f64();
        let samples = self.samples.load(Ordering::SeqCst);
        let bytes = self.bytes.load(Ordering::SeqCst);
        let samples_per_s = if samples > 0 && secs > 0.0 {
            samples as f64 / secs
        } else {
            0.0
        };
        let bytes_per_s = if bytes > 0 && secs > 0.0 {
            (bytes as f64 / secs) as i64
        } else {
            0
        };
        let idle = Duration::from_nanos(self.idle_nanos.load(Ordering::SeqCst));
        format!(
            "EsMetrics importer stats:\n  \
             idle duration: {idle:?};\n  \
             time spent while importing: {total:?};\n  \
             total samples: {samples};\n  \
             samples/s: {samples_per_s:.2};\n  \
             total bytes: {};\n  \
             bytes/s: {};\n  \
             import requests: {};\n  \
             import requests retries: {};",
            byte_count_si(bytes as i64),
            byte_count_si(bytes_per_s),
            self.requests.load(Ordering::SeqCst),
            self.retries.load(Ordering::SeqCst),
        )
    }
}

/// Formats a byte count using SI (decimal) units. Ports `byteCountSI`.
fn byte_count_si(b: i64) -> String {
    const UNIT: i64 = 1000;
    if b < UNIT {
        return format!("{b} B");
    }
    let mut div = UNIT;
    let mut exp = 0;
    let mut n = b / UNIT;
    while n >= UNIT {
        div *= UNIT;
        n /= UNIT;
        exp += 1;
    }
    let value = b as f64 / div as f64;
    let unit = ['k', 'M', 'G', 'T', 'P', 'E'][exp];
    format!("{value:.1} {unit}B")
}

fn worker(rx: &Mutex<Receiver<Series>>, shared: &Shared) {
    let mut batch: Vec<Series> = Vec::new();
    let mut data_points = 0usize;
    loop {
        let wait_start = Instant::now();
        let next = { rx.lock().unwrap().recv() };
        shared
            .stats
            .idle_nanos
            .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        match next {
            Ok(mut ts) => {
                round_series(&mut ts, shared.significant_figures, shared.round_digits);
                data_points += ts.timestamps.len();
                batch.push(ts);
                if data_points >= shared.batch_size {
                    flush(&mut batch, shared);
                    data_points = 0;
                }
            }
            Err(_) => {
                // Channel closed: flush the final partial batch and exit.
                flush(&mut batch, shared);
                return;
            }
        }
    }
}

fn flush(batch: &mut Vec<Series>, shared: &Shared) {
    if batch.is_empty() || shared.aborted.load(Ordering::SeqCst) {
        batch.clear();
        return;
    }
    let total_samples: usize = batch.iter().map(|s| s.timestamps.len()).sum();
    let body = encode_batch(batch);
    let body_len = body.len();
    // The Ctrl-C handler aborts the retry waits between attempts.
    let (attempts, res) = shared
        .backoff
        .retry(crate::signal::cancel_flag(), || import_once(shared, &body));
    shared.stats.retries.fetch_add(attempts, Ordering::Relaxed);
    match res {
        Ok(()) => {
            shared.stats.requests.fetch_add(1, Ordering::Relaxed);
            shared
                .stats
                .bytes
                .fetch_add(body_len as u64, Ordering::Relaxed);
            shared
                .stats
                .samples
                .fetch_add(total_samples as u64, Ordering::Relaxed);
        }
        Err(e) => {
            let mut slot = shared.first_error.lock().unwrap();
            if slot.is_none() {
                *slot = Some(e);
            }
            shared.aborted.store(true, Ordering::SeqCst);
        }
    }
    batch.clear();
}

fn import_once(shared: &Shared, body: &[u8]) -> Result<(), RetryError> {
    let mut rb = shared.auth.apply(shared.http.post(&shared.import_path));

    // Optionally gzip the JSON body (ports the `-vm-compress` path).
    let body_bytes: Vec<u8> = if shared.compress {
        rb = rb.header("Content-Encoding", "gzip");
        gzip(body).map_err(|e| RetryError::retryable(format!("gzip failed: {e}")))?
    } else {
        body.to_vec()
    };

    // Optionally throttle the upload through the shared byte-rate limiter.
    rb = if shared.rate_limit > 0 {
        let reader =
            RateLimitedReader::new(std::io::Cursor::new(body_bytes), Arc::clone(&shared.rl));
        rb.body(reqwest::blocking::Body::new(reader))
    } else {
        rb.body(body_bytes)
    };

    let resp = rb
        .send()
        .map_err(|e| RetryError::retryable(format!("import request error: {e}")))?;
    let status = resp.status().as_u16();
    if status == 204 {
        return Ok(());
    }
    let text = resp.text().unwrap_or_default();
    if status == 400 {
        // Bad request: unrecoverable, fast-fail (ports ErrBadRequest).
        return Err(RetryError::fatal(format!("bad request: {status}: {text}")));
    }
    Err(RetryError::retryable(format!(
        "unexpected response code {status}: {text}"
    )))
}

/// Serializes a batch to newline-delimited VM import JSON, splitting series
/// with more than 10K samples across lines. Ports `TimeSeries.write`.
fn encode_batch(batch: &[Series]) -> Vec<u8> {
    let mut out = String::new();
    for ts in batch {
        let mut i = 0;
        let n = ts.timestamps.len();
        while i < n {
            let end = (i + 10_000).min(n);
            out.push_str("{\"metric\":{\"__name__\":");
            push_json_string(&mut out, &ts.name);
            for (k, v) in &ts.labels {
                out.push(',');
                push_json_string(&mut out, k);
                out.push(':');
                push_json_string(&mut out, v);
            }
            out.push_str("},\"timestamps\":[");
            for (j, t) in ts.timestamps[i..end].iter().enumerate() {
                if j > 0 {
                    out.push(',');
                }
                out.push_str(&t.to_string());
            }
            out.push_str("],\"values\":[");
            for (j, v) in ts.values[i..end].iter().enumerate() {
                if j > 0 {
                    out.push(',');
                }
                out.push_str(&format_value(*v));
            }
            out.push_str("]}\n");
            i = end;
        }
    }
    out.into_bytes()
}

fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Go `%v` float formatting: shortest round-tripping form (Rust's default
/// `{}` for f64 matches for finite values).
fn format_value(v: f64) -> String {
    if v.is_nan() {
        "null".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "1e999".to_string()
        } else {
            "-1e999".to_string()
        }
    } else {
        format!("{v}")
    }
}

fn round_series(ts: &mut Series, significant_figures: i32, round_digits: i32) {
    if significant_figures > 0 {
        for v in ts.values.iter_mut() {
            *v = round_to_significant_figures(*v, significant_figures);
        }
    }
    if round_digits < 100 {
        for v in ts.values.iter_mut() {
            *v = round_to_decimal_digits(*v, round_digits);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(name: &str, labels: &[(&str, &str)], ts: &[i64], vals: &[f64]) -> Series {
        Series {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            timestamps: ts.to_vec(),
            values: vals.to_vec(),
        }
    }

    #[test]
    fn encodes_import_json_line() {
        let b = vec![series(
            "cpu",
            &[("host", "h1")],
            &[1000, 2000],
            &[1.5, 66.0],
        )];
        let got = String::from_utf8(encode_batch(&b)).unwrap();
        assert_eq!(
            got,
            "{\"metric\":{\"__name__\":\"cpu\",\"host\":\"h1\"},\"timestamps\":[1000,2000],\"values\":[1.5,66]}\n"
        );
    }

    #[test]
    fn splits_long_series() {
        let n = 25_000;
        let ts: Vec<i64> = (0..n).collect();
        let vals: Vec<f64> = (0..n).map(|x| x as f64).collect();
        let b = vec![series("m", &[], &ts, &vals)];
        let got = String::from_utf8(encode_batch(&b)).unwrap();
        // 25000 samples → 3 lines (10k + 10k + 5k).
        assert_eq!(got.lines().count(), 3);
    }

    #[test]
    fn escapes_json_strings() {
        let mut s = String::new();
        push_json_string(&mut s, "a\"b\\c");
        assert_eq!(s, "\"a\\\"b\\\\c\"");
    }

    #[test]
    fn byte_count_si_formats_decimal_units() {
        assert_eq!(byte_count_si(0), "0 B");
        assert_eq!(byte_count_si(512), "512 B");
        assert_eq!(byte_count_si(1000), "1.0 kB");
        assert_eq!(byte_count_si(1_500_000), "1.5 MB");
        assert_eq!(byte_count_si(2_000_000_000), "2.0 GB");
    }

    #[test]
    fn stats_summary_reports_counts() {
        let s = Stats {
            start: Instant::now(),
            samples: AtomicU64::new(1000),
            bytes: AtomicU64::new(2000),
            requests: AtomicU64::new(3),
            retries: AtomicU64::new(1),
            idle_nanos: AtomicU64::new(0),
        };
        let out = s.summary();
        assert!(out.contains("total samples: 1000;"));
        assert!(out.contains("total bytes: 2.0 kB;"));
        assert!(out.contains("import requests: 3;"));
        assert!(out.contains("import requests retries: 1;"));
    }
}
