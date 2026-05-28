//! End-to-end PromQL evaluator bench.
//!
//! Seeds a temp data dir with N series × M samples each, then measures
//! instant-query latency for a small library of representative
//! expressions. The intent is to gate against perf regressions during
//! Tier A6/A7 work — not to be a head-to-head VM comparison (that's
//! gated by the conformance harness).

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::expect_used)]

use criterion::{Criterion, criterion_group, criterion_main};
use esm_promql::EvalContext;
use esm_promql::evaluator::evaluate;
use esm_promql::parser::parse;
use esm_storage::{Sample, Storage};

fn seed_storage(num_series: usize, samples_per_series: usize) -> (tempfile::TempDir, Storage) {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut storage = Storage::open(tmp.path().join("d")).expect("open");
    let now_ms = 1_700_000_000_000_i64;
    let step = 15_000_i64;
    let mut batch = Vec::with_capacity(num_series * samples_per_series);
    for s in 0..num_series {
        let name = format!(r#"http_requests_total{{job="api",inst="{s}"}}"#).into_bytes();
        for i in 0..samples_per_series {
            batch.push(Sample {
                metric_name: name.clone(),
                timestamp_ms: now_ms - (samples_per_series as i64 - i as i64) * step,
                value: (i * 7 + s) as i64,
            });
        }
    }
    storage.ingest(&batch).expect("ingest");
    storage.flush().expect("flush");
    (tmp, storage)
}

fn bench_instant(c: &mut Criterion) {
    let (_tmp, storage) = seed_storage(50, 200);
    let now_ms = 1_700_000_000_000_i64;
    let ctx = EvalContext::instant(now_ms);

    let exprs: &[(&str, &str)] = &[
        ("selector", r#"http_requests_total{job="api"}"#),
        ("sum", "sum(http_requests_total)"),
        ("sum_by_inst", "sum by (inst) (http_requests_total)"),
        ("rate_5m", "rate(http_requests_total[5m])"),
        ("topk5", "topk(5, http_requests_total)"),
    ];

    for (name, src) in exprs {
        let expr = parse(src).expect("parse");
        c.bench_function(&format!("promql_instant/{name}"), |b| {
            b.iter(|| {
                let _ = evaluate(&expr, &storage, ctx).expect("eval");
            });
        });
    }
}

criterion_group!(benches, bench_instant);
criterion_main!(benches);
