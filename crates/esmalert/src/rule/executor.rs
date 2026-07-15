//! Bounded-concurrency rule executor: runs every rule in a group exactly
//! once per tick, either sequentially (`concurrency <= 1`) or split across a
//! fixed pool of scoped threads (`concurrency > 1`).
//!
//! Port of `execConcurrently` (`app/vmalert/rule/group.go:735-770`).

use std::thread;

use esm_gotemplate::{EvalContext, Funcs};

use super::group::RuleKind;
use super::{Querier, RuleError};
use crate::series::Series;

/// Runs every rule in `rules` exactly once at `ts`, returning each result
/// tagged with its original index into `rules` (the returned `Vec`'s own
/// order isn't meaningful — callers key off the first tuple element, not
/// position).
///
/// `concurrency` (clamped to `[1, rules.len()]`) of `1` runs every rule
/// sequentially on the calling thread — the common case, and it skips the
/// `std::thread::scope` setup entirely. For `concurrency > 1`, `rules` is
/// split into that many contiguous, non-overlapping chunks via
/// `chunks_mut`; each chunk is exec'd sequentially by its own
/// `std::thread::scope` thread. Splitting into contiguous chunks (rather
/// than handing out work one rule at a time from a shared queue) is what
/// lets each rule be owned, as a plain `&mut`, by exactly one thread — no
/// `Mutex`/atomic index is needed to coordinate access to `rules` itself,
/// and no two threads can ever touch the same rule. The trade-off (vs.
/// upstream's semaphore-gated goroutine-per-rule pool) is that a group
/// whose rules have very uneven per-rule cost won't load-balance across
/// workers; not a concern here since group rule counts are small and this
/// port favors correctness/simplicity over that scheduling nuance.
///
/// `q` must be `Sync`: with `concurrency > 1`, multiple worker threads call
/// through the same `&dyn Querier` concurrently. [`super::Querier`] itself
/// has no `Sync` supertrait bound — adding one there would force every
/// existing mock implementation (e.g. `remoteread`'s `Cell`-based test
/// queriers) to become `Sync` too, breaking already-compiling code — so
/// the bound is applied only at this call boundary, via the trait-object
/// type `dyn Querier + Sync`. A caller with a `Sync` querier (the real
/// `Datasource`, or any test mock with no interior mutability) passes it
/// here unchanged; a caller with a non-`Sync` querier can still use the
/// `concurrency <= 1` path (only reachable by *not* calling this function
/// concurrently with itself, which no caller in this crate does).
pub fn exec_concurrently(
    rules: &mut [RuleKind],
    q: &(dyn Querier + Sync),
    ts: i64,
    concurrency: usize,
    funcs: &Funcs,
    ctx: &EvalContext,
    limit: i64,
) -> Vec<(usize, Result<Vec<Series>, RuleError>)> {
    let n = rules.len();
    if n == 0 {
        return Vec::new();
    }
    let workers = concurrency.clamp(1, n);
    if workers == 1 {
        return rules
            .iter_mut()
            .enumerate()
            .map(|(i, r)| (i, r.exec(q, ts, funcs, ctx, limit)))
            .collect();
    }

    let chunk_size = n.div_ceil(workers);
    let mut results: Vec<Option<Result<Vec<Series>, RuleError>>> = (0..n).map(|_| None).collect();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        let mut offset = 0usize;
        for chunk in rules.chunks_mut(chunk_size) {
            let start = offset;
            offset += chunk.len();
            handles.push(scope.spawn(move || {
                chunk
                    .iter_mut()
                    .enumerate()
                    .map(|(i, r)| (start + i, r.exec(q, ts, funcs, ctx, limit)))
                    .collect::<Vec<_>>()
            }));
        }
        for h in handles {
            if let Ok(chunk_results) = h.join() {
                for (idx, res) in chunk_results {
                    results[idx] = Some(res);
                }
            }
            // A worker thread panicking is never propagated as a panic
            // here (rule evaluation must never crash the group loop); the
            // affected rules' slots are left `None` and turned into a
            // `RuleError` below.
        }
    });

    results
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            (
                i,
                r.unwrap_or_else(|| Err(RuleError::new("rule evaluation worker thread panicked"))),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{DsError, Metric, QueryResult};
    use crate::rule::RecordingRule;
    use esm_gotemplate::default_funcs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingQ(Arc<AtomicUsize>);
    impl Querier for CountingQ {
        fn query(&self, _expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(QueryResult {
                data: vec![Metric {
                    labels: vec![],
                    timestamps: vec![0],
                    values: vec![1.0],
                }],
                is_partial: None,
            })
        }
    }

    fn test_ctx() -> EvalContext {
        EvalContext {
            external_url: String::new(),
            path_prefix: String::new(),
            query_fn: Arc::new(|_| Ok(vec![])),
        }
    }

    fn recording_rule(name: &str) -> RuleKind {
        RuleKind::Recording(RecordingRule {
            name: name.to_string(),
            expr: "up".into(),
            ..Default::default()
        })
    }

    fn rule_record_name(series: &[Series]) -> String {
        series
            .first()
            .and_then(|s| s.labels.iter().find(|(k, _)| k == "__name__"))
            .map(|(_, v)| v.clone())
            .expect("series carries a __name__ label")
    }

    #[test]
    fn concurrency_one_runs_every_rule_exactly_once_sequentially() {
        let calls = Arc::new(AtomicUsize::new(0));
        let q = CountingQ(Arc::clone(&calls));
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut rules: Vec<RuleKind> = (0..3).map(|i| recording_rule(&format!("r{i}"))).collect();

        let results = exec_concurrently(&mut rules, &q, 0, 1, &funcs, &ctx, 0);

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(results.len(), 3);
        for (idx, res) in results {
            let series = res.expect("rule exec should succeed");
            assert_eq!(rule_record_name(&series), format!("r{idx}"));
        }
    }

    #[test]
    fn concurrency_gt_one_runs_every_rule_exactly_once_and_maps_indices_correctly() {
        let calls = Arc::new(AtomicUsize::new(0));
        let q = CountingQ(Arc::clone(&calls));
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut rules: Vec<RuleKind> = (0..7).map(|i| recording_rule(&format!("r{i}"))).collect();

        let results = exec_concurrently(&mut rules, &q, 0, 4, &funcs, &ctx, 0);

        assert_eq!(
            calls.load(Ordering::SeqCst),
            7,
            "every rule must be queried exactly once, regardless of worker count"
        );
        assert_eq!(results.len(), 7);

        let mut seen = [false; 7];
        for (idx, res) in results {
            assert!(!seen[idx], "duplicate result for index {idx}");
            seen[idx] = true;
            let series = res.expect("rule exec should succeed");
            assert_eq!(
                rule_record_name(&series),
                format!("r{idx}"),
                "result at index {idx} must belong to rule {idx}"
            );
        }
        assert!(seen.iter().all(|&s| s), "every index must have a result");
    }

    #[test]
    fn concurrency_higher_than_rule_count_still_runs_each_rule_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let q = CountingQ(Arc::clone(&calls));
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut rules: Vec<RuleKind> = (0..2).map(|i| recording_rule(&format!("r{i}"))).collect();

        let results = exec_concurrently(&mut rules, &q, 0, 16, &funcs, &ctx, 0);

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn empty_rule_set_returns_empty_results() {
        let calls = Arc::new(AtomicUsize::new(0));
        let q = CountingQ(Arc::clone(&calls));
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let mut rules: Vec<RuleKind> = vec![];

        let results = exec_concurrently(&mut rules, &q, 0, 4, &funcs, &ctx, 0);

        assert!(results.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// A `Querier` that panics for one designated expression and returns a
    /// single sample for every other. Used to inject a real panic *inside a
    /// worker thread* (a rule's `exec` calls `q.query`, so panicking here
    /// panics that worker) — this exercises the `h.join()` `Err`-absorption
    /// path in [`exec_concurrently`] through the existing types, with no
    /// contortion needed.
    struct PanicOnExprQ {
        panic_expr: &'static str,
    }
    impl Querier for PanicOnExprQ {
        fn query(&self, expr: &str, _ts: i64) -> Result<QueryResult, DsError> {
            if expr == self.panic_expr {
                panic!("injected rule-evaluation panic for expr {expr:?}");
            }
            Ok(QueryResult {
                data: vec![Metric {
                    labels: vec![],
                    timestamps: vec![0],
                    values: vec![1.0],
                }],
                is_partial: None,
            })
        }
    }

    fn recording_rule_with_expr(name: &str, expr: &str) -> RuleKind {
        RuleKind::Recording(RecordingRule {
            name: name.to_string(),
            expr: expr.to_string(),
            ..Default::default()
        })
    }

    #[test]
    fn panicking_rule_becomes_ruleerror_and_leaves_siblings_intact() {
        // 4 rules, concurrency 4 -> chunk_size = div_ceil(4, 4) = 1, so each
        // rule is isolated in its own worker thread. Rule at index 2 panics
        // inside its worker; that worker's `join()` returns `Err`, which
        // `exec_concurrently` absorbs (leaving slot 2 `None` -> a
        // `RuleError`) rather than propagating the panic or corrupting the
        // other three slots.
        let ctx = test_ctx();
        let funcs = default_funcs(&ctx);
        let q = PanicOnExprQ { panic_expr: "BOOM" };
        let mut rules: Vec<RuleKind> = vec![
            recording_rule_with_expr("r0", "up"),
            recording_rule_with_expr("r1", "up"),
            recording_rule_with_expr("r2", "BOOM"),
            recording_rule_with_expr("r3", "up"),
        ];

        // Silence the default panic hook's stderr backtrace for the
        // intentional injected panic, so this test's output isn't alarming.
        // Restored immediately after the call. (The hook is process-global;
        // this briefly affects any panic racing in a sibling test, but the
        // window is tiny and only suppresses stderr printing, never changes
        // pass/fail behavior.)
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let results = exec_concurrently(&mut rules, &q, 0, 4, &funcs, &ctx, 0);
        std::panic::set_hook(prev_hook);

        assert_eq!(results.len(), 4);
        let mut by_index: std::collections::HashMap<usize, Result<Vec<Series>, RuleError>> =
            results.into_iter().collect();

        // The three non-panicking siblings return correct, correctly-indexed
        // results — the panic didn't abort the call or shift any index.
        for i in [0usize, 1, 3] {
            let series = by_index
                .remove(&i)
                .unwrap_or_else(|| panic!("missing result for index {i}"))
                .unwrap_or_else(|_| panic!("sibling rule {i} should have succeeded"));
            assert_eq!(rule_record_name(&series), format!("r{i}"));
        }

        // The panicked rule's slot surfaces as an `Err`, not a lost entry.
        let panicked = by_index.remove(&2).expect("index 2 must still have a slot");
        assert!(
            panicked.is_err(),
            "the panicking rule's slot must surface as a RuleError"
        );
    }
}
