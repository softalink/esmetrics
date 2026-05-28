//! Microbenchmarks for the ingest + query hot paths.
//!
//! Establishes the rolling baseline that the per-PR perf gate (PLAN.md §7.3)
//! compares against, and provides a starting point for the nightly
//! vs-upstream comparison.

#![allow(clippy::unwrap_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

use criterion::{Criterion, criterion_group, criterion_main};
use esm_storage::{Sample, Storage, TimeRange};

fn bench_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest");
    group.sample_size(20);

    for &count in &[1_000usize, 10_000usize] {
        group.bench_function(format!("samples_{count}"), |b| {
            b.iter_with_setup(
                || {
                    let tmp = tempfile::tempdir().unwrap();
                    let s = Storage::open(tmp.path().join("d")).unwrap();
                    (tmp, s)
                },
                |(_tmp, mut s)| {
                    let mut samples = Vec::with_capacity(count);
                    for i in 0..count {
                        samples.push(Sample {
                            metric_name: format!("metric_{}", i % 64).into_bytes(),
                            timestamp_ms: i as i64,
                            value: i as i64,
                        });
                    }
                    s.ingest(&samples).unwrap();
                    s.flush().unwrap();
                    s
                },
            );
        });
    }
    group.finish();
}

fn bench_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("query");
    group.sample_size(50);

    group.bench_function("search_by_name_one_metric", |b| {
        // Setup once outside the timed loop.
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Storage::open(tmp.path().join("d")).unwrap();
        let mut samples = Vec::with_capacity(10_000);
        for i in 0..10_000 {
            samples.push(Sample { metric_name: b"metric_a".to_vec(), timestamp_ms: i, value: i });
        }
        s.ingest(&samples).unwrap();
        s.flush().unwrap();

        b.iter(|| {
            let _ = s.search_by_metric_name(
                b"metric_a",
                TimeRange { min_timestamp_ms: 0, max_timestamp_ms: 10_000 },
            );
        });
    });

    group.finish();
}

criterion_group!(benches, bench_ingest, bench_query);
criterion_main!(benches);
