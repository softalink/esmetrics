//! Microbenchmark: split the per-series read path into its components
//! (part-open, decode, full search) to locate the ~73% of read time that is
//! NOT raw decode. Run:
//!
//! ```text
//! cargo test -p esm-storage --release --test read_path_split -- --ignored --nocapture
//! ```
#![allow(
    clippy::print_stderr,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::unwrap_used
)]

use std::time::Instant;

use esm_storage::timeseries::BlockStreamReader;
use esm_storage::{Sample, Storage, TimeRange};

const SERIES: usize = 1000;
const SAMPLES_PER: usize = 8640; // 24h @10s -> >1 block
const STEP_MS: i64 = 10_000;
const T0: i64 = 1_704_067_200_000;

fn build(dir: &std::path::Path) -> Storage {
    let mut s = Storage::open(dir).unwrap();
    let mut batch: Vec<Sample> = Vec::with_capacity(SERIES * SAMPLES_PER);
    for i in 0..SERIES {
        let name = format!("cpu_usage_user_{i}").into_bytes();
        for k in 0..SAMPLES_PER {
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
#[ignore = "manual microbench; run with --release --ignored --nocapture"]
fn read_path_split() {
    let tmp = tempfile::tempdir().unwrap();
    let store = build(&tmp.path().join("d"));
    let window = TimeRange { min_timestamp_ms: T0, max_timestamp_ms: T0 + 12 * 3600 * 1000 };
    let names: Vec<Vec<u8>> =
        (0..SERIES).map(|i| format!("cpu_usage_user_{i}").into_bytes()).collect();
    let part_dir = tmp.path().join("d").join("ts_parts");
    let part_path = std::fs::read_dir(&part_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.is_dir())
        .unwrap();

    // Cache header + metaindex once (as the storage now does in PartMeta).
    let probe = BlockStreamReader::open(&part_path).unwrap();
    let header = probe.part_header.clone();
    let metaindex = probe.metaindex();
    drop(probe);
    let tsids: Vec<_> = names.iter().map(|n| store.lookup_tsid(n).unwrap()).collect();

    let iters = 5u32;
    let warm = |store: &Storage| {
        let mut g = 0usize;
        for n in &names {
            g += store.search_by_metric_name(n, window).unwrap().len();
        }
        g
    };
    let returned = warm(&store);

    // (A) Full search_by_metric_name (open + seek + decode + materialize + merge).
    let t = Instant::now();
    let mut sink = 0i64;
    for _ in 0..iters {
        for n in &names {
            sink += store.search_by_metric_name(n, window).unwrap().len() as i64;
        }
    }
    let full = t.elapsed();

    // (B) Part-open only: open_with_index + seek_to_tsid, no block read.
    let t = Instant::now();
    for _ in 0..iters {
        for &tsid in &tsids {
            let mut r =
                BlockStreamReader::open_with_index(&part_path, header.clone(), metaindex.clone())
                    .unwrap();
            r.seek_to_tsid(tsid);
            std::hint::black_box(&r);
        }
    }
    let open_only = t.elapsed();

    // (C) Open + seek + decode every matching block (no StoredSample build/merge).
    let t = Instant::now();
    let mut decoded = 0usize;
    for _ in 0..iters {
        for &tsid in &tsids {
            let mut r =
                BlockStreamReader::open_with_index(&part_path, header.clone(), metaindex.clone())
                    .unwrap();
            r.seek_to_tsid(tsid);
            while let Some(h) = r.next_block_header().unwrap() {
                if h.tsid > tsid {
                    break;
                }
                if h.tsid != tsid || h.max_timestamp < window.min_timestamp_ms {
                    continue;
                }
                if h.min_timestamp > window.max_timestamp_ms {
                    break;
                }
                let (ts, _v) = r.read_data_block_for(&h).unwrap();
                decoded += ts.len();
            }
        }
    }
    let open_decode = t.elapsed();

    let perns =
        |d: std::time::Duration, n: usize| d.as_secs_f64() * 1e9 / f64::from(iters) / n as f64;
    eprintln!(
        "returned {returned} samples/iter, decoded ~{} /iter [sink={sink}]",
        decoded / iters as usize
    );
    eprintln!(
        "(A) full search          : {:.1} ns/returned-sample  ({:.1}M/s)",
        perns(full, returned),
        1000.0 / perns(full, returned)
    );
    eprintln!(
        "(B) open+seek only       : {:.2} us/series",
        open_only.as_secs_f64() * 1e6 / f64::from(iters) / SERIES as f64
    );
    eprintln!(
        "(C) open+seek+decode     : {:.1} ns/returned-sample  ({:.1}M/s)",
        perns(open_decode, returned),
        1000.0 / perns(open_decode, returned)
    );
    eprintln!(
        "    -> materialize+merge = (A)-(C) = {:.1} ns/returned-sample",
        perns(full, returned) - perns(open_decode, returned)
    );
}
