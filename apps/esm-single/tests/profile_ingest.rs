//! In-process ingest phase profiler. Attributes ingest cost to parse vs
//! buffer-ingest vs flush without a sampling profiler (perf is unavailable in
//! some sandboxes). Run:
//!
//! ```text
//! cargo test -p esm-single --release --test profile_ingest -- --ignored --nocapture
//! ```
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::format_push_string,
    clippy::print_stderr
)]

use std::time::Instant;

/// Build a realistic cpu-only Influx batch: `lines` lines, 10 tags, 10 fields.
/// Cardinality matches TSBS cpu-only: a bounded host set (so most samples hit
/// an existing series, like the real workload), one timestamp step per cycle.
fn synth(lines: usize) -> String {
    const HOSTS: usize = 1000;
    let mut s = String::with_capacity(lines * 320);
    for i in 0..lines {
        let host = i % HOSTS;
        let ts = 1_704_067_200_000_000_000i64 + (i / HOSTS) as i64 * 10_000_000_000;
        s.push_str(&format!(
            "cpu,hostname=host_{host},region=us-west-1,datacenter=us-west-1a,rack=12,os=Ubuntu16,arch=x64,team=SF,service=3,service_version=1,service_environment=test \
usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i {ts}\n"
        ));
    }
    s
}

#[test]
#[ignore = "manual profiler; run with --release --ignored --nocapture"]
fn profile_ingest_phases() {
    let lines = 200_000usize;
    let samples = lines * 10;
    let body = synth(lines);

    // Phase 1: parse (arena-keyed).
    let t = Instant::now();
    let mut arena = Vec::with_capacity(body.len());
    let mut entries = Vec::new();
    esm_protocols::influx_line::parse_into(&body, 0, 1, &mut arena, &mut entries).unwrap();
    let parse = t.elapsed();
    assert_eq!(entries.len(), samples);

    // Phase 2: buffer-ingest (sub-flush-threshold, so no flush fires).
    let tmp = tempfile::tempdir().unwrap();
    let store = esm_storage::ShardedStorage::open(tmp.path().join("d"), 16).unwrap();
    let t = Instant::now();
    store.ingest_keyed(&arena, &entries).unwrap();
    let ingest = t.elapsed();

    // Phase 3: flush everything to disk (compress + write + merge).
    let t = Instant::now();
    store.flush().unwrap();
    let flush = t.elapsed();

    let lps = |d: std::time::Duration| lines as f64 / d.as_secs_f64();
    let sps = |d: std::time::Duration| samples as f64 / d.as_secs_f64();
    eprintln!("--- ingest phase profile ({lines} lines, {samples} samples, 16 shards) ---");
    eprintln!("parse_into : {parse:?}  ({:.0} lines/s, {:.0} samples/s)", lps(parse), sps(parse));
    eprintln!("ingest_keyed(buffer): {ingest:?}  ({:.0} samples/s)", sps(ingest));
    eprintln!("flush(to disk): {flush:?}  ({:.0} samples/s)", sps(flush));
    let total = parse + ingest + flush;
    eprintln!(
        "share: parse {:.0}% | buffer {:.0}% | flush {:.0}%",
        100.0 * parse.as_secs_f64() / total.as_secs_f64(),
        100.0 * ingest.as_secs_f64() / total.as_secs_f64(),
        100.0 * flush.as_secs_f64() / total.as_secs_f64(),
    );

    // Lever-1 isolation: a second ingest of the same keys hits every series in
    // name_to_tsid (no insert, no index update) — the steady-state path of a
    // real sustained load. Its cost is intern lookup (FNV over the full key +
    // probe + memcmp) + pending push. Comparing warm-vs-cold shows how much is
    // the first-seen interning vs the per-sample hash that a two-level intern
    // would target.
    let store2 = esm_storage::ShardedStorage::open(tmp.path().join("d2"), 16).unwrap();
    store2.ingest_keyed(&arena, &entries).unwrap(); // warm the maps
    let t = Instant::now();
    store2.ingest_keyed(&arena, &entries).unwrap(); // all-hits steady state
    let warm = t.elapsed();
    eprintln!("buffer (warm, all-hits): {warm:?}  ({:.0} samples/s)", sps(warm));
}
