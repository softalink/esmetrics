//! Minimal process-global counter registry.
//!
//! Port of a narrow slice of the upstream `github.com/VictoriaMetrics/metrics`
//! library (v1.146.0's `vendor/github.com/VictoriaMetrics/metrics`): just
//! enough to back `/metrics` counters for ingestion row/request accounting.
//! Counters only — no gauges, histograms, summaries, process metrics, or
//! `HELP`/`TYPE` exposition lines (see [`write_prometheus`]).
//!
//! Mirrors the upstream library's API shape: the registry key IS the full
//! Prometheus-style name including labels (e.g.
//! `esm_rows_inserted_total{type="promremotewrite"}`), exactly like Go's
//! `metrics.GetOrCreateCounter("vm_rows_inserted_total{type=\"promremotewrite\"}")`
//! — there is no separate label/name split at this layer.
//!
//! # Registry bound
//!
//! The registry leaks one `Counter` (via [`Box::leak`]) per distinct name
//! ever requested. This is safe here because the caller set is bounded to a
//! small, finite collection: static string literals (one per
//! protocol/listener) for most callers, plus, for `esm-auth`, one key per
//! *configured* user/config-name (see `esm-auth::metrics::user_requests`) —
//! never attacker-controlled request data. Registered names are always
//! derived from operator-supplied configuration (loaded at startup or
//! reload), so the registry cannot grow unbounded in practice.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// A process-wide monotonically increasing counter.
///
/// Go: `metrics.Counter` (the `n uint64` field, atomically updated).
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Increments the counter by 1. Go: `Counter.Inc`.
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Increments the counter by `delta`. Go: `Counter.Add`.
    pub fn inc_by(&self, delta: u64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }

    /// Returns the current value. Go: `Counter.Get`.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

type Registry = Mutex<HashMap<String, &'static Counter>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns the process-global counter registered under `name`, creating and
/// leaking it (see the module doc's "Registry bound" section) the first time
/// `name` is requested.
///
/// `name` is the FULL Prometheus-style metric name including labels, e.g.
/// `esm_rows_inserted_total{type="promremotewrite"}` — the labeled name IS the
/// registry key, matching Go's `metrics.GetOrCreateCounter`.
pub fn get_or_create_counter(name: &str) -> &'static Counter {
    let mut registry = registry().lock().unwrap();
    if let Some(counter) = registry.get(name) {
        return counter;
    }
    let counter: &'static Counter = Box::leak(Box::default());
    registry.insert(name.to_owned(), counter);
    counter
}

/// Writes every registered counter to `dst` in Prometheus text exposition
/// format, sorted by name: one `<name> <value>\n` line per counter, no
/// `HELP`/`TYPE` lines and no timestamps — matching the shape of Go
/// `Counter.marshalTo` (`fmt.Fprintf(w, "%s %d\n", prefix, v)`), which the
/// upstream `metrics` library's default `Set.WritePrometheus` also emits with
/// no `HELP`/`TYPE` preamble for plain counters.
pub fn write_prometheus(dst: &mut String) {
    let registry = registry().lock().unwrap();
    let mut names: Vec<&String> = registry.keys().collect();
    names.sort();
    for name in names {
        let counter = registry[name];
        dst.push_str(name);
        dst.push(' ');
        dst.push_str(&counter.get().to_string());
        dst.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn get_or_create_returns_the_same_counter_for_the_same_name() {
        let a = get_or_create_counter("test_metrics_same_counter_total");
        a.inc();
        let b = get_or_create_counter("test_metrics_same_counter_total");
        // Same underlying counter: the increment via `a` is visible via `b`.
        assert_eq!(b.get(), 1);
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn inc_and_inc_by_accumulate() {
        let c = get_or_create_counter("test_metrics_inc_by_total");
        assert_eq!(c.get(), 0);
        c.inc();
        c.inc_by(41);
        assert_eq!(c.get(), 42);
    }

    #[test]
    fn write_prometheus_emits_sorted_name_value_lines() {
        get_or_create_counter(r#"test_metrics_zzz_total{type="b"}"#).inc_by(2);
        get_or_create_counter(r#"test_metrics_aaa_total{type="a"}"#).inc_by(5);

        let mut out = String::new();
        write_prometheus(&mut out);

        let a_pos = out
            .find(r#"test_metrics_aaa_total{type="a"} 5"#)
            .expect("aaa counter line present");
        let z_pos = out
            .find(r#"test_metrics_zzz_total{type="b"} 2"#)
            .expect("zzz counter line present");
        assert!(a_pos < z_pos, "expected sorted-by-name output:\n{out}");

        // Exact line format: "<name> <value>\n", no HELP/TYPE/timestamp.
        for line in out.lines() {
            if line.starts_with("test_metrics_") {
                assert!(
                    line.split(' ').count() == 2,
                    "expected `<name> <value>`, got: {line:?}"
                );
            }
        }
    }

    #[test]
    fn concurrent_inc_from_threads_sums_correctly() {
        let counter = get_or_create_counter("test_metrics_concurrent_total");
        let start = counter.get();

        let threads: Vec<_> = (0..8)
            .map(|_| {
                thread::spawn(move || {
                    let counter = get_or_create_counter("test_metrics_concurrent_total");
                    for _ in 0..1000 {
                        counter.inc();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(counter.get(), start + 8 * 1000);
    }

    #[test]
    fn unregistered_name_is_absent_from_output() {
        let mut out = String::new();
        write_prometheus(&mut out);
        assert!(!out.contains("test_metrics_never_registered_total"));
    }

    #[test]
    fn arc_across_threads_shares_registration() {
        // Sanity check that the returned reference is genuinely 'static and
        // usable from a spawned thread without any lifetime gymnastics.
        let shared = Arc::new(());
        let _keep_alive = Arc::clone(&shared);
        let counter = get_or_create_counter("test_metrics_static_lifetime_total");
        thread::spawn(move || {
            counter.inc();
        })
        .join()
        .unwrap();
        assert_eq!(
            get_or_create_counter("test_metrics_static_lifetime_total").get(),
            1
        );
    }
}
