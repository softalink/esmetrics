//! The `remote-read` migration source: reads time series from a Prometheus
//! remote-read endpoint (`/api/v1/read`) in SAMPLES mode and streams them to
//! the importer. Ports `app/vmctl/remoteread/remoteread.go` and the
//! `remoteReadProcessor` from `remoteread.go`.
//!
//! Only the default SAMPLES response type is supported; STREAMED_XOR_CHUNKS
//! (`--remote-read-use-stream`) would require porting Prometheus's XOR chunk
//! decoder and is rejected at startup.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use reqwest::blocking::Client;

use crate::importer::{Importer, Series};
use crate::proto::{self, Field};
use crate::stepper::split_date_range;
use crate::timeparse::parse_time_msec;

/// A remote-read time filter (millisecond bounds). Ports `remoteread.Filter`.
struct Filter {
    start_ms: i64,
    end_ms: i64,
}

/// A `label=~value` matcher.
pub(crate) struct Matcher {
    pub(crate) name: String,
    pub(crate) value: String,
}

/// Prometheus default chunked-read frame-size limit (`DefaultChunkedReadLimit`).
const CHUNKED_READ_LIMIT: u64 = 50 * 1024 * 1024;

/// Remote-read HTTP client. Ports `remoteread.Client`.
pub(crate) struct RemoteReadClient {
    pub(crate) addr: String,
    pub(crate) disable_path_append: bool,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) matchers: Vec<Matcher>,
    pub(crate) use_stream: bool,
    pub(crate) http: Client,
}

impl RemoteReadClient {
    fn read(
        &self,
        filter: &Filter,
        mut callback: impl FnMut(Series) -> Result<(), String>,
    ) -> Result<(), String> {
        // Build the ReadRequest protobuf (a single Query). In stream mode add
        // accepted_response_types = [STREAMED_XOR_CHUNKS].
        let mut query = Vec::new();
        proto::put_int64(&mut query, 1, filter.start_ms);
        proto::put_int64(&mut query, 2, filter.end_ms - 1);
        for m in &self.matchers {
            let mut lm = Vec::new();
            proto::put_int64(&mut lm, 1, 2); // LabelMatcher_RE = 2
            proto::put_string(&mut lm, 2, &m.name);
            proto::put_string(&mut lm, 3, &m.value);
            proto::put_message(&mut query, 3, &lm);
        }
        let mut req = Vec::new();
        proto::put_message(&mut req, 1, &query);
        if self.use_stream {
            // ReadRequest.accepted_response_types = [STREAMED_XOR_CHUNKS = 1].
            proto::put_int64(&mut req, 2, 1);
        }

        let compressed = snap::raw::Encoder::new()
            .compress_vec(&req)
            .map_err(|e| format!("snappy encode failed: {e}"))?;

        let url = if self.disable_path_append {
            self.addr.clone()
        } else {
            format!("{}/api/v1/read", self.addr)
        };
        let content_type = if self.use_stream {
            "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
        } else {
            "application/x-protobuf"
        };
        let mut rb = self
            .http
            .post(&url)
            .header("Content-Encoding", "snappy")
            .header("Accept-Encoding", "snappy")
            .header("Content-Type", content_type)
            .header("X-Prometheus-Remote-Read-Version", "0.1.0")
            .body(compressed);
        if !self.user.is_empty() {
            rb = rb.basic_auth(&self.user, Some(&self.password));
        }
        for (k, v) in &self.headers {
            rb = rb.header(k.as_str(), v.as_str());
        }

        let mut resp = rb
            .send()
            .map_err(|e| format!("error while sending request to {url:?}: {e}"))?;
        let status = resp.status().as_u16();
        if status != 200 && status != 204 {
            let body = resp.text().unwrap_or_default();
            return Err(format!(
                "unexpected response code {status} for {url:?}: {body}"
            ));
        }

        if self.use_stream {
            // STREAMED_XOR_CHUNKS: newline-of-frames, each an uncompressed
            // `prompb.ChunkedReadResponse`. Ports `processStreamResponse`.
            while let Some(frame) = crate::chunkenc::read_frame(&mut resp, CHUNKED_READ_LIMIT)? {
                for series in decode_chunked_read_response(&frame)? {
                    callback(series)?;
                }
            }
            return Ok(());
        }

        // SAMPLES: a single snappy-compressed `prompb.ReadResponse`.
        let mut body = Vec::new();
        resp.copy_to(&mut body)
            .map_err(|e| format!("error reading response: {e}"))?;
        let uncompressed = snap::raw::Decoder::new()
            .decompress_vec(&body)
            .map_err(|e| format!("error decoding response: {e}"))?;

        for series in decode_read_response(&uncompressed) {
            callback(series)?;
        }
        Ok(())
    }
}

/// Decodes a `prompb.ChunkedReadResponse` frame into importer series. Nesting:
/// ChunkedSeries → (Labels, Chunks); each XOR Chunk's `data` is decoded to
/// samples. Ports `processStreamResponse` + `parseSamples`.
fn decode_chunked_read_response(data: &[u8]) -> Result<Vec<Series>, String> {
    let mut out = Vec::new();
    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        // ChunkedReadResponse.chunked_series = 1
        if let Field::LenDelim(1, cs) = f {
            out.push(decode_chunked_series(cs)?);
        }
    }
    Ok(out)
}

fn decode_chunked_series(data: &[u8]) -> Result<Series, String> {
    let mut name = String::new();
    let mut labels: Vec<(String, String)> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        match f {
            Field::LenDelim(1, label) => {
                if let Some((n, v)) = decode_label(label) {
                    if n == "__name__" {
                        name = v;
                    } else {
                        labels.push((n, v));
                    }
                }
            }
            Field::LenDelim(2, chunk) => {
                // Chunk { type=3 (XOR=1), data=4 }
                let (chunk_type, chunk_data) = decode_chunk(chunk);
                if chunk_type == 1 {
                    for (t, v) in crate::chunkenc::decode_xor_chunk(&chunk_data)? {
                        timestamps.push(t);
                        values.push(v);
                    }
                }
                // Non-XOR chunks (histograms) are skipped.
            }
            _ => {}
        }
    }
    Ok(Series {
        name,
        labels,
        timestamps,
        values,
    })
}

/// Returns `(encoding, data)` for a `prompb.Chunk`.
fn decode_chunk(data: &[u8]) -> (u64, Vec<u8>) {
    let mut chunk_type = 0u64;
    let mut chunk_data = Vec::new();
    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        match f {
            Field::Varint(3, t) => chunk_type = t,
            Field::LenDelim(4, d) => chunk_data = d.to_vec(),
            _ => {}
        }
    }
    (chunk_type, chunk_data)
}

/// Decodes a `prompb.ReadResponse` into importer series. Handles the message
/// nesting Results → Timeseries → (Labels, Samples).
fn decode_read_response(data: &[u8]) -> Vec<Series> {
    let mut out = Vec::new();
    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        if let Field::LenDelim(1, qr) = f {
            decode_query_result(qr, &mut out);
        }
    }
    out
}

fn decode_query_result(data: &[u8], out: &mut Vec<Series>) {
    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        if let Field::LenDelim(1, ts) = f {
            if let Some(series) = decode_timeseries(ts) {
                out.push(series);
            }
        }
    }
}

fn decode_timeseries(data: &[u8]) -> Option<Series> {
    let mut name = String::new();
    let mut labels: Vec<(String, String)> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        match f {
            Field::LenDelim(1, label) => {
                if let Some((n, v)) = decode_label(label) {
                    if n == "__name__" {
                        name = v;
                    } else {
                        labels.push((n, v));
                    }
                }
            }
            Field::LenDelim(2, sample) => {
                if let Some((v, t)) = decode_sample(sample) {
                    values.push(v);
                    timestamps.push(t);
                }
            }
            _ => {}
        }
    }
    Some(Series {
        name,
        labels,
        timestamps,
        values,
    })
}

fn decode_label(data: &[u8]) -> Option<(String, String)> {
    let mut name = String::new();
    let mut value = String::new();
    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        match f {
            Field::LenDelim(1, b) => name = String::from_utf8_lossy(b).into_owned(),
            Field::LenDelim(2, b) => value = String::from_utf8_lossy(b).into_owned(),
            _ => {}
        }
    }
    Some((name, value))
}

fn decode_sample(data: &[u8]) -> Option<(f64, i64)> {
    let mut value = 0.0;
    let mut timestamp = 0i64;
    let mut r = proto::Reader::new(data);
    while let Some(f) = r.next() {
        match f {
            Field::Fixed64(1, bits) => value = f64::from_bits(bits),
            Field::Varint(2, v) => timestamp = v as i64,
            _ => {}
        }
    }
    Some((value, timestamp))
}

/// Configuration for the remote-read migration.
pub(crate) struct RemoteReadConfig {
    pub(crate) client: Arc<RemoteReadClient>,
    pub(crate) time_start: String,
    pub(crate) time_end: String,
    pub(crate) chunk: String,
    pub(crate) time_reverse: bool,
    pub(crate) concurrency: usize,
    pub(crate) assume_yes: bool,
}

/// Converts a nanosecond time bound to a millisecond bound, preserving
/// sub-second precision. Matches Go's `time.Time.UnixMilli()` (`ns / 1e6`,
/// truncating toward zero) that `remoteReadProcessor.run` uses to fill
/// `remoteread.Filter{StartTimestampMs, EndTimestampMs}`.
fn ns_to_msec(ns: i64) -> i64 {
    ns / 1_000_000
}

/// Runs the remote-read migration. Ports `remoteReadProcessor.run`.
pub(crate) fn run(cfg: &RemoteReadConfig, importer: Importer) -> Result<(), String> {
    let start_ns = parse_time_msec(&cfg.time_start)
        .map_err(|e| format!("failed to parse remote-read-filter-time-start: {e}"))?
        * 1_000_000;
    let end_ns = if cfg.time_end.is_empty() {
        crate::timeparse::now_ms() * 1_000_000
    } else {
        parse_time_msec(&cfg.time_end)
            .map_err(|e| format!("failed to parse remote-read-filter-time-end: {e}"))?
            * 1_000_000
    };

    let ranges = split_date_range(start_ns, end_ns, &cfg.chunk, cfg.time_reverse)
        .map_err(|e| format!("failed to create date ranges: {e}"))?;
    log::info!(
        "Selected time range will be split into {} ranges according to {:?} step",
        ranges.len(),
        cfg.chunk
    );
    if !crate::prompt(cfg.assume_yes, "Continue?") {
        return Ok(());
    }

    let importer = Arc::new(importer);
    let queue: VecDeque<Filter> = ranges
        .into_iter()
        .map(|(s, e)| Filter {
            start_ms: ns_to_msec(s),
            end_ms: ns_to_msec(e),
        })
        .collect();
    run_workers(cfg, &importer, queue)?;

    match Arc::try_unwrap(importer) {
        Ok(im) => im.close(),
        Err(_) => Err("importer still referenced at shutdown".to_string()),
    }?;
    log::info!("Import finished!");
    Ok(())
}

fn run_workers(
    cfg: &RemoteReadConfig,
    importer: &Arc<Importer>,
    queue: VecDeque<Filter>,
) -> Result<(), String> {
    let cc = cfg.concurrency.max(1);
    let queue = Arc::new(Mutex::new(queue));
    let first_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let stop = Arc::new(AtomicBool::new(false));

    std::thread::scope(|scope| {
        for _ in 0..cc {
            let queue = Arc::clone(&queue);
            let first_error = Arc::clone(&first_error);
            let stop = Arc::clone(&stop);
            let importer = Arc::clone(importer);
            let client = Arc::clone(&cfg.client);
            scope.spawn(move || loop {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let filter = { queue.lock().unwrap().pop_front() };
                let Some(filter) = filter else { return };
                let res = client.read(&filter, |series| importer.input(series));
                if let Err(e) = res {
                    let mut slot = first_error.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                    stop.store(true, Ordering::SeqCst);
                    return;
                }
            });
        }
    });

    let err = first_error.lock().unwrap().take();
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sub-second time bounds must survive the ns→ms conversion. The previous
    /// `ns / 1e9 * 1000` truncated to whole seconds first (1.5 s → 1000 ms);
    /// the `UnixMilli()`-equivalent `ns / 1e6` keeps 1500 ms.
    #[test]
    fn ns_bounds_preserve_millisecond_precision() {
        assert_eq!(ns_to_msec(1_500_000_000), 1500);
        assert_eq!(ns_to_msec(1_234_567_000_000), 1_234_567);
        // Whole-second bounds are unchanged.
        assert_eq!(ns_to_msec(2_000_000_000), 2000);
    }

    /// Encodes a minimal ReadResponse and checks the decoder recovers it.
    #[test]
    fn decodes_read_response() {
        // Build TimeSeries { labels: [__name__=up, job=x], samples: [{1.5, 1000}] }
        let mut label1 = Vec::new();
        proto::put_string(&mut label1, 1, "__name__");
        proto::put_string(&mut label1, 2, "up");
        let mut label2 = Vec::new();
        proto::put_string(&mut label2, 1, "job");
        proto::put_string(&mut label2, 2, "x");
        let mut sample = Vec::new();
        sample.push((1 << 3) | 1); // field 1, fixed64
        sample.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
        proto::put_int64(&mut sample, 2, 1000);

        let mut ts = Vec::new();
        proto::put_message(&mut ts, 1, &label1);
        proto::put_message(&mut ts, 1, &label2);
        proto::put_message(&mut ts, 2, &sample);

        let mut qr = Vec::new();
        proto::put_message(&mut qr, 1, &ts);
        let mut resp = Vec::new();
        proto::put_message(&mut resp, 1, &qr);

        let series = decode_read_response(&resp);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].name, "up");
        assert_eq!(series[0].labels, vec![("job".to_string(), "x".to_string())]);
        assert_eq!(series[0].timestamps, vec![1000]);
        assert_eq!(series[0].values, vec![1.5]);
    }
}
