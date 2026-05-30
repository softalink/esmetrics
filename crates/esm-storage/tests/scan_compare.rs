//! Research microbench: per-series reads (current) vs a VM-style per-part
//! streaming scan (open each overlapping part once, iterate all blocks),
//! for a wide query that touches every series (like double-groupby-all).
//! Run:
//!
//! ```text
//! cargo test -p esm-storage --release --test scan_compare -- --ignored --nocapture
//! ```
#![allow(
    clippy::print_stderr,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::unwrap_used
)]

use std::collections::BTreeMap;
use std::time::Instant;

use esm_storage::timeseries::{BlockStreamReader, Tsid};
use esm_storage::{Sample, Storage, TimeRange};

const SERIES: usize = 1000;
const DAYS_SAMPLES: usize = 25920; // 3 days @10s
const STEP_MS: i64 = 10_000;
const T0: i64 = 1_704_067_200_000;
const PARTS: usize = 6; // ~half-day each, time-disjoint (like TSBS bulk load)

/// Build SERIES series spanning 3 days, flushed into PARTS time-disjoint parts
/// (no merge), mimicking a bulk-loaded shard.
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
fn scan_compare() {
    let tmp = tempfile::tempdir().unwrap();
    let store = build(&tmp.path().join("d"));
    // 12h window in the middle (overlaps ~2 of the 6 half-day parts).
    let lo = T0 + 30 * 3600 * 1000;
    let window = TimeRange { min_timestamp_ms: lo, max_timestamp_ms: lo + 12 * 3600 * 1000 };
    let names: Vec<Vec<u8>> =
        (0..SERIES).map(|i| format!("cpu_usage_user_{i}").into_bytes()).collect();
    let tsids: Vec<Tsid> = names.iter().map(|n| store.lookup_tsid(n).unwrap()).collect();
    let part_paths: Vec<_> = std::fs::read_dir(tmp.path().join("d").join("ts_parts"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    eprintln!("{SERIES} series, {} parts, 12h window", part_paths.len());

    let iters = 10u32;
    // warm
    let mut got = 0usize;
    for n in &names {
        got += store.search_by_metric_name(n, window).unwrap().len();
    }

    // (A) per-series: current path (open overlapping parts PER series).
    let t = Instant::now();
    let mut sink_a = 0i64;
    for _ in 0..iters {
        for n in &names {
            sink_a += store.search_by_metric_name(n, window).unwrap().len() as i64;
        }
    }
    let per_series = t.elapsed();

    // (B) per-part scan: open each overlapping part ONCE, iterate every block,
    // keep in-window samples bucketed by tsid. (All series are candidates.)
    let candidates: std::collections::HashSet<Tsid> = tsids.iter().copied().collect();
    let t = Instant::now();
    let mut sink_b = 0i64;
    for _ in 0..iters {
        let mut out: BTreeMap<Tsid, Vec<(i64, i64)>> = BTreeMap::new();
        for p in &part_paths {
            let mut r = BlockStreamReader::open(p).unwrap();
            // prune by part time range
            if r.part_header.max_timestamp < window.min_timestamp_ms
                || r.part_header.min_timestamp > window.max_timestamp_ms
            {
                continue;
            }
            while let Some(h) = r.next_block_header().unwrap() {
                if !candidates.contains(&h.tsid) {
                    continue;
                }
                if h.max_timestamp < window.min_timestamp_ms
                    || h.min_timestamp > window.max_timestamp_ms
                {
                    continue;
                }
                let (ts_arr, v_arr) = r.read_data_block_for(&h).unwrap();
                let e = out.entry(h.tsid).or_default();
                for (ts, v) in ts_arr.iter().zip(v_arr.iter()) {
                    if *ts >= window.min_timestamp_ms && *ts <= window.max_timestamp_ms {
                        e.push((*ts, *v));
                    }
                }
            }
        }
        sink_b += out.values().map(Vec::len).sum::<usize>() as i64;
    }
    let per_part = t.elapsed();

    let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0 / f64::from(iters);
    eprintln!("returned ~{got} samples/iter");
    eprintln!("(A) per-series search : {:.1} ms/iter  [sink={sink_a}]", ms(per_series));
    eprintln!("(B) per-part scan     : {:.1} ms/iter  [sink={sink_b}]", ms(per_part));
    eprintln!("    speedup (A/B) = {:.2}x", ms(per_series) / ms(per_part));
}
