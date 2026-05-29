//! In-process query read-path profiler. Measures the per-series read
//! (`search_by_metric_name` → decode + merge) that dominates heavy TSBS
//! aggregations (double-groupby reads ~10k series over a 12h window). Run:
//!
//! ```text
//! cargo test -p esm-single --release --test profile_query -- --ignored --nocapture
//! ```
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::print_stderr,
    clippy::unwrap_used
)]

use std::time::Instant;

use esm_storage::{Sample, Storage, TimeRange};

const SERIES: usize = 1000;
const STEP_MS: i64 = 10_000; // 10s, like TSBS cpu-only
const T0: i64 = 1_704_067_200_000;

/// Ingest `SERIES` series, each `samples_per` long, into one flushed part.
fn build(dir: &std::path::Path, samples_per: usize) -> Storage {
    let mut s = Storage::open(dir).unwrap();
    let mut batch: Vec<Sample> = Vec::with_capacity(SERIES * samples_per);
    for i in 0..SERIES {
        let name = format!("cpu_usage_user_{i}").into_bytes();
        for k in 0..samples_per {
            batch.push(Sample {
                metric_name: name.clone(),
                timestamp_ms: T0 + k as i64 * STEP_MS,
                value: (k as i64 * 7 + i as i64) % 100,
            });
        }
    }
    let idx: Vec<usize> = (0..batch.len()).collect();
    s.ingest_selected(&batch, &idx).unwrap();
    s.flush().unwrap();
    s
}

#[test]
#[ignore = "manual profiler; run with --release --ignored --nocapture"]
fn profile_query_read() {
    // 24h/series @10s = 8640 samples -> >1 block (MAX 8192), like real data.
    let samples_per = 8640usize;
    let tmp = tempfile::tempdir().unwrap();
    let store = build(&tmp.path().join("d"), samples_per);

    // Heavy-agg read shape: each series read once over a 12h window.
    let window = TimeRange { min_timestamp_ms: T0, max_timestamp_ms: T0 + 12 * 3600 * 1000 };
    let names: Vec<Vec<u8>> =
        (0..SERIES).map(|i| format!("cpu_usage_user_{i}").into_bytes()).collect();

    // Warm OS cache.
    let mut got = 0usize;
    for n in &names {
        got += store.search_by_metric_name(n, window).unwrap().len();
    }

    let iters = 5;
    let t = Instant::now();
    let mut sink = 0i64;
    for _ in 0..iters {
        for n in &names {
            let s = store.search_by_metric_name(n, window).unwrap();
            sink = sink.wrapping_add(s.len() as i64);
        }
    }
    let el = t.elapsed();
    let per_iter = el / iters;
    let samples = got; // per iteration
    eprintln!("--- query read profile ({SERIES} series, {samples} samples in 12h window) ---");
    eprintln!(
        "search_by_metric_name x{SERIES}: {per_iter:?}/iter  ({:.0} samples/s)  [sink={sink}]",
        samples as f64 / per_iter.as_secs_f64()
    );
}
