//! Q3 crossover microbench: at what candidate count does the per-part scan
//! (`scan_tsids`, the wide path) overtake per-series `search_by_tsid` (the
//! selective path)? Models ONE shard of a TSBS scale-1000 load: ~312 series
//! (10k / 32 shards), ~5 time-disjoint parts, a multi-hour window. Run:
//!
//! ```text
//! cargo test -p esm-storage --release --test selective_scan_compare -- --ignored --nocapture
//! ```
#![allow(
    clippy::print_stderr,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::unwrap_used
)]

use std::time::Instant;

use esm_storage::timeseries::Tsid;
use esm_storage::{Sample, Storage, TimeRange};

const SERIES: usize = 312; // 10k series / 32 shards
const DAYS_SAMPLES: usize = 25920; // 3 days @10s
const STEP_MS: i64 = 10_000;
const T0: i64 = 1_704_067_200_000;
const PARTS: usize = 6;

fn build(dir: &std::path::Path) -> Storage {
    let mut s = Storage::open(dir).unwrap();
    let chunk = DAYS_SAMPLES / PARTS;
    for part in 0..PARTS {
        let mut batch: Vec<Sample> = Vec::with_capacity(SERIES * chunk);
        for i in 0..SERIES {
            let name = format!("cpu_usage_user_{i}").into_bytes();
            for k in (part * chunk)..((part + 1) * chunk) {
                batch.push(Sample {
                    metric_name: name.clone(),
                    timestamp_ms: T0 + k as i64 * STEP_MS,
                    value: (k as i64 * 7 + i as i64) % 100,
                });
            }
        }
        let idx: Vec<usize> = (0..batch.len()).collect();
        s.ingest_selected(&batch, &idx).unwrap();
        s.flush_no_merge().unwrap();
    }
    s
}

#[test]
#[ignore = "manual research microbench; run with --release --ignored --nocapture"]
fn selective_scan_compare() {
    let tmp = tempfile::tempdir().unwrap();
    let store = build(&tmp.path().join("d"));
    // 8h window (cpu-max-all shape); overlaps a couple of the half-day parts.
    let lo = T0 + 30 * 3600 * 1000;
    let window = TimeRange { min_timestamp_ms: lo, max_timestamp_ms: lo + 8 * 3600 * 1000 };
    let all: Vec<Tsid> = (0..SERIES)
        .map(|i| store.lookup_tsid(&format!("cpu_usage_user_{i}").into_bytes()).unwrap())
        .collect();

    let iters = 50u32;
    eprintln!("shard model: {SERIES} series, {PARTS} parts, 8h window, {iters} iters/measure");
    eprintln!(
        "{:>6} {:>14} {:>14} {:>10}",
        "cands", "per-series ms", "per-part ms", "ratio(ps/pp)"
    );
    for &cands in &[5usize, 8, 16, 40, 80, 160, 312] {
        // Deterministic spread across the id space (mimics scattered hosts).
        let stride = SERIES / cands.max(1);
        let mut picked: Vec<Tsid> = (0..cands).map(|j| all[(j * stride) % SERIES]).collect();
        picked.sort_unstable();
        picked.dedup();

        // warm
        for &t in &picked {
            let _ = store.search_by_tsid(t, window).unwrap();
        }
        let _ = store.scan_tsids(&picked, window).unwrap();

        // (A) per-series selective path
        let t = Instant::now();
        let mut sink_a = 0i64;
        for _ in 0..iters {
            for &ts in &picked {
                sink_a += store.search_by_tsid(ts, window).unwrap().len() as i64;
            }
        }
        let per_series = t.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);

        // (B) per-part scan (wide path)
        let t = Instant::now();
        let mut sink_b = 0i64;
        for _ in 0..iters {
            sink_b += store.scan_tsids(&picked, window).unwrap().iter().map(Vec::len).sum::<usize>()
                as i64;
        }
        let per_part = t.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);

        assert_eq!(sink_a, sink_b, "the two paths must return the same sample count");
        eprintln!(
            "{:>6} {:>14.3} {:>14.3} {:>10.2}",
            picked.len(),
            per_series,
            per_part,
            per_series / per_part
        );
    }
}
