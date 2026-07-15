//! Process-wide resolved `-search.maxWorkersPerQuery` value: the maximum
//! number of CPU cores a single query may use. Read by the promql rollup
//! workers (esm-promql) and the storage unpack fan-out (esm-storage), so a
//! single setting caps both layers.
//!
//! Resolution precedence: explicit flag value via [`set_max_workers`] >
//! `ESM_MAX_QUERY_WORKERS` env var (debug/benchmarking knob) > auto
//! `min(cpus, 32)` (the upstream `netstorage.MaxWorkers()` analog).

use std::sync::OnceLock;

static MAX_WORKERS: OnceLock<usize> = OnceLock::new();

/// Installs the `-search.maxWorkersPerQuery` flag value. Must be called
/// before the first query is served; later calls are ignored because the
/// resolved value may already have been observed.
pub fn set_max_workers(n: usize) {
    let n = n.max(1);
    if MAX_WORKERS.set(n).is_err() {
        let current = *MAX_WORKERS
            .get()
            .expect("set failed, so the cell is filled");
        if current != n {
            log::warn!(
                "-search.maxWorkersPerQuery={n} ignored: the per-query worker cap \
                 was already resolved to {current} before the flag was installed"
            );
        }
    }
}

/// The resolved per-query worker cap (always ≥ 1).
pub fn max_workers() -> usize {
    *MAX_WORKERS.get_or_init(|| {
        resolve_max_workers(
            std::env::var("ESM_MAX_QUERY_WORKERS").ok().as_deref(),
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1),
        )
    })
}

/// `min(cpus, 32)`: the auto default (upstream `netstorage.MaxWorkers()`).
pub fn auto_max_workers(cpus: usize) -> usize {
    cpus.clamp(1, 32)
}

/// Pure resolution used by [`max_workers`]: parsable positive env value,
/// else auto.
fn resolve_max_workers(env: Option<&str>, cpus: usize) -> usize {
    if let Some(n) = env
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 1)
    {
        return n;
    }
    auto_max_workers(cpus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_prefers_env_over_auto() {
        // Parsable positive env wins.
        assert_eq!(resolve_max_workers(Some("3"), 8), 3);
        // Unset, unparsable, or zero env falls back to auto.
        assert_eq!(resolve_max_workers(None, 8), 8);
        assert_eq!(resolve_max_workers(Some("abc"), 8), 8);
        assert_eq!(resolve_max_workers(Some("0"), 8), 8);
        assert_eq!(resolve_max_workers(Some("-2"), 8), 8);
    }

    #[test]
    fn auto_is_cpus_capped_at_32() {
        assert_eq!(auto_max_workers(1), 1);
        assert_eq!(auto_max_workers(8), 8);
        assert_eq!(auto_max_workers(48), 32);
        // Degenerate cpus=0 still yields a usable value.
        assert_eq!(auto_max_workers(0), 1);
    }

    #[test]
    fn set_wins_over_everything() {
        // Runs in the same process as the other tests but is the only test
        // touching the static, so the OnceLock observation is deterministic.
        set_max_workers(5);
        assert_eq!(max_workers(), 5);
        // A second set is ignored: the value may already have been observed.
        set_max_workers(7);
        assert_eq!(max_workers(), 5);
    }
}
