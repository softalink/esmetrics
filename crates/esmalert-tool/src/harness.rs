//! In-process esmetrics harness for `esmalert-tool`.
//!
//! Stands up a real esmetrics server (storage + HTTP), ingests
//! [`InputSample`]s via Prometheus remote-write, and is queryable through an
//! `esmalert::datasource::Datasource` pointed at [`Harness::base_url`].
//!
//! Faithful to upstream vmalert-tool's unittest harness
//! (`app/vmalert-tool/unittest/unittest.go`), which stands up
//! vmstorage+vminsert+vmselect behind an `httptest` server; here the
//! equivalent server-run entry point (`esmetrics::run`) already exists, so
//! standing up a real server in-process is straightforward.
//!
//! ## Flush
//!
//! `esmetrics` exposes `GET /internal/force_flush` (`esmetrics::lib.rs`,
//! mirroring upstream `vmstorage.DebugFlush()`), which flushes the data
//! table and every partition's index so newly ingested rows and their
//! tag-index entries become searchable immediately. [`Harness::flush`] hits
//! that endpoint. It's still not instantaneous end-to-end (ingestion itself
//! lands on background insert-server plumbing before storage sees it), so
//! callers should poll [`esmalert::datasource::Datasource::query`] with a
//! short bounded retry rather than assuming the very first query after
//! `flush()` already observes the data — see this crate's round-trip test
//! below for the pattern.

// Scaffold stage: this harness isn't wired into `main()` yet — the runner
// that consumes it (executing test-file rule groups against it) lands in a
// later task.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use reqwest::blocking::Client;

use esm_protoparser::prompb::{Label, Sample, TimeSeries};
use esm_protoparser::prompb_encode::encode_and_compress;
use esmalert::series::Series;
use esmetrics::flags::Flags;
use esmetrics::App;

use crate::input::InputSample;
use crate::ToolError;

/// Process-wide, monotonically-incrementing counter used to derive a unique
/// temp storage directory per [`Harness`] (never random/time-based, so runs
/// stay reproducible and diagnosable).
static SEQ: AtomicU64 = AtomicU64::new(0);

/// A running in-process esmetrics server, used to round-trip-test esmalert
/// rule files: ingest [`InputSample`]s via remote-write, then query them
/// back through a real `esmalert::datasource::Datasource`.
pub struct Harness {
    app: Option<App>,
    base_url: String,
    temp_dir: PathBuf,
    client: Client,
}

impl Harness {
    /// Starts a fresh in-process esmetrics server backed by a unique temp
    /// storage directory, bound to an ephemeral localhost port.
    pub fn start() -> Result<Harness, ToolError> {
        let temp_dir = std::env::temp_dir().join(format!(
            "esmalert-tool-harness-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let flags = Flags {
            http_listen_addr: "127.0.0.1:0".to_string(),
            storage_data_path: temp_dir.to_string_lossy().into_owned(),
            // `esm_storage::Storage::must_open` treats a `<= 0` value as "use
            // the max retention window" (100 years). Test rule files ingest
            // samples at arbitrary fixed timestamps unrelated to wall-clock
            // "now" (e.g. `1_700_000_000_000`), so the default 31-day
            // retention would silently drop them as "too old" once the
            // wall-clock date drifts far enough past the fixture timestamps.
            retention_msecs: 0,
            ..Flags::default()
        };
        let app = esmetrics::run(&flags).map_err(|e| {
            ToolError::new(format!("failed to start in-process esmetrics server: {e}"))
        })?;
        let base_url = format!("http://{}", app.local_addr());
        let client = Client::builder()
            .build()
            .map_err(|e| ToolError::new(format!("failed to build http client: {e}")))?;

        Ok(Harness {
            app: Some(app),
            base_url,
            temp_dir,
            client,
        })
    }

    /// The base URL of the in-process server, e.g. `http://127.0.0.1:54321`.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Ingests `samples` via Prometheus remote-write to `/api/v1/write`.
    /// Samples are grouped by unique label set into one series each (samples
    /// sorted by timestamp within a series), matching how
    /// `esm_protoparser::prompb::TimeSeries` represents a batch.
    pub fn ingest(&self, samples: &[InputSample]) -> Result<(), ToolError> {
        let series = group_into_series(samples);
        let borrowed: Vec<TimeSeries<'_>> = series.iter().map(to_borrowed).collect();
        let compressed = encode_and_compress(&borrowed)
            .map_err(|e| ToolError::new(format!("failed to encode/compress samples: {e}")))?;

        let url = format!("{}/api/v1/write", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(compressed)
            .send()
            .map_err(|e| ToolError::new(format!("remote-write POST to {url} failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(ToolError::new(format!(
                "remote-write POST to {url} returned status {status}"
            )));
        }
        Ok(())
    }

    /// Forces newly ingested rows (and their tag-index entries) to become
    /// searchable via `GET /internal/force_flush`. Always returns `Ok`: a
    /// failed/unreachable flush is treated as "not yet visible" rather than
    /// a hard error, since callers are expected to poll the subsequent query
    /// with a bounded retry anyway (storage visibility can lag ingestion by
    /// a little even after a successful flush call).
    pub fn flush(&self) -> Result<(), ToolError> {
        let url = format!("{}/internal/force_flush", self.base_url);
        let _ = self.client.get(&url).send();
        Ok(())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(app) = self.app.take() {
            app.stop();
        }
        std::fs::remove_dir_all(&self.temp_dir).ok();
    }
}

/// Groups `samples` by unique label set (preserving first-seen label-set
/// order), sorting each group's samples by timestamp.
fn group_into_series(samples: &[InputSample]) -> Vec<Series> {
    let mut series: Vec<Series> = Vec::new();
    for sample in samples {
        let target = series.iter_mut().find(|s| s.labels == sample.labels);
        let entry = match target {
            Some(s) => s,
            None => {
                series.push(Series {
                    labels: sample.labels.clone(),
                    samples: Vec::new(),
                });
                series.last_mut().expect("just pushed")
            }
        };
        entry.samples.push(Sample {
            value: sample.value,
            timestamp: sample.timestamp_ms,
        });
    }
    for s in &mut series {
        s.samples.sort_by_key(|sample| sample.timestamp);
    }
    series
}

/// Borrows an owned [`Series`] as an `esm_protoparser::prompb::TimeSeries`
/// for the duration of one [`encode_and_compress`] call (mirrors
/// `esmalert::remotewrite::client::to_borrowed`).
fn to_borrowed(s: &Series) -> TimeSeries<'_> {
    TimeSeries {
        labels: s
            .labels
            .iter()
            .map(|(name, value)| Label {
                name: name.as_bytes(),
                value: value.as_bytes(),
            })
            .collect(),
        samples: s.samples.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::Harness;
    use crate::input::InputSample;

    #[test]
    fn ingest_then_query_roundtrips_through_in_process_server() {
        let h = Harness::start().unwrap();
        let samples = vec![InputSample {
            labels: vec![("__name__".into(), "up".into()), ("job".into(), "x".into())],
            timestamp_ms: 1_700_000_000_000,
            value: 1.0,
        }];
        h.ingest(&samples).unwrap();
        h.flush().unwrap();

        let ds = esmalert::datasource::Datasource::new(
            h.base_url(),
            Default::default(),
            Default::default(),
            Default::default(),
            vec![],
            std::time::Duration::from_secs(60),
            esmalert::datasource::DEFAULT_QUERY_TIMEOUT,
        )
        .unwrap();

        // Storage visibility can lag ingestion by a little even after a
        // successful flush (see the module doc), so poll with a bounded
        // retry rather than asserting on the first query.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut found = false;
        while std::time::Instant::now() < deadline {
            let res = ds.query("up", 1_700_000_000_000).unwrap();
            if res.data.iter().any(|m| m.values.contains(&1.0)) {
                found = true;
                break;
            }
            h.flush().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(found, "expected up==1.0 to become queryable");
    }
}
