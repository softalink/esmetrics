//! The `vm-native` migration processor: explore metric names, split the time
//! range, and stream native export → import across a worker pool. Ports
//! `vm_native.go`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::backoff::{Backoff, RetryError};
use crate::civil::format_rfc3339;
use crate::matchfilter::build_match_with_filter;
use crate::native::{add_extra_labels_to_import_path, Filter, NativeClient};
use crate::stepper::split_date_range;
use crate::timeparse::parse_time_msec;

const NATIVE_EXPORT_ADDR: &str = "api/v1/export";
const NATIVE_IMPORT_ADDR: &str = "api/v1/import";

/// A metric name paired with the time ranges to migrate for it.
type MetricRanges = Vec<(String, Vec<(i64, i64)>)>;

/// Configuration for a `vm-native` migration.
pub(crate) struct VmNativeConfig {
    pub(crate) src: Arc<NativeClient>,
    pub(crate) dst: Arc<NativeClient>,
    pub(crate) filter_match: String,
    pub(crate) time_start: String,
    pub(crate) time_end: String,
    pub(crate) chunk: String,
    pub(crate) time_reverse: bool,
    pub(crate) concurrency: usize,
    pub(crate) is_native: bool,
    pub(crate) disable_per_metric: bool,
    pub(crate) inter_cluster: bool,
    pub(crate) backoff: Arc<Backoff>,
    pub(crate) rate_limit: i64,
    pub(crate) assume_yes: bool,
}

#[derive(Default)]
struct Stats {
    bytes: AtomicU64,
    requests: AtomicU64,
}

/// Runs the migration. Ports `vmNativeProcessor.run`.
pub(crate) fn run(cfg: &VmNativeConfig, cancel: &AtomicBool) -> Result<(), String> {
    if cfg.filter_match.is_empty() {
        return Err("flag \"vm-native-filter-match\" can't be empty".to_string());
    }

    let start_ns = parse_time_msec(&cfg.time_start)
        .map_err(|e| format!("failed to parse vm-native-filter-time-start: {e}"))?
        * 1_000_000;
    let end_ns = if cfg.time_end.is_empty() {
        crate::timeparse::now_ms() * 1_000_000
    } else {
        parse_time_msec(&cfg.time_end)
            .map_err(|e| format!("failed to parse vm-native-filter-time-end: {e}"))?
            * 1_000_000
    };

    let ranges = if cfg.chunk.is_empty() {
        vec![(start_ns, end_ns)]
    } else {
        split_date_range(start_ns, end_ns, &cfg.chunk, cfg.time_reverse)
            .map_err(|e| format!("failed to create date ranges: {e}"))?
    };

    let base_filter = Filter {
        matcher: cfg.filter_match.clone(),
        time_start: cfg.time_start.clone(),
        time_end: cfg.time_end.clone(),
    };

    let tenants = if cfg.inter_cluster {
        log::info!("Discovering tenants...");
        let t = cfg.src.get_source_tenants(&base_filter)?;
        log::info!("The following tenants were discovered: {t:?}");
        if !crate::prompt(cfg.assume_yes, "Continue?") {
            return Ok(());
        }
        t
    } else {
        vec![String::new()]
    };

    let stats = Arc::new(Stats::default());
    for tenant_id in &tenants {
        run_backfilling(cfg, cancel, tenant_id, &ranges, &stats)?;
    }

    log::info!(
        "Import finished! Total bytes: {}, requests: {}",
        stats.bytes.load(Ordering::SeqCst),
        stats.requests.load(Ordering::SeqCst)
    );
    Ok(())
}

fn run_backfilling(
    cfg: &VmNativeConfig,
    cancel: &AtomicBool,
    tenant_id: &str,
    ranges: &[(i64, i64)],
    stats: &Arc<Stats>,
) -> Result<(), String> {
    let mut export_addr = NATIVE_EXPORT_ADDR.to_string();
    let mut import_addr = NATIVE_IMPORT_ADDR.to_string();
    if cfg.is_native {
        export_addr.push_str("/native");
        import_addr.push_str("/native");
    }
    import_addr = add_extra_labels_to_import_path(&import_addr, &cfg.dst.extra_labels)?;

    let (src_url, dst_url) = if cfg.inter_cluster {
        (
            format!(
                "{}/select/{}/prometheus/{}",
                cfg.src.addr, tenant_id, export_addr
            ),
            format!(
                "{}/insert/{}/prometheus/{}",
                cfg.dst.addr, tenant_id, import_addr
            ),
        )
    } else {
        (
            format!("{}/{}", cfg.src.addr, export_addr),
            format!("{}/{}", cfg.dst.addr, import_addr),
        )
    };

    log::info!(
        "Initing import process from {src_url:?} to {dst_url:?} with filter {:?}",
        cfg.filter_match
    );
    if ranges.len() > 1 {
        log::info!(
            "Selected time range will be split into {} ranges according to {:?} step",
            ranges.len(),
            cfg.chunk
        );
    }

    // Build the metric → ranges map (per-metric exploration, or the whole
    // range under an empty metric name).
    let metrics_map: MetricRanges = if cfg.disable_per_metric {
        vec![(String::new(), ranges.to_vec())]
    } else {
        let map = explore(cfg, tenant_id, ranges)?;
        if map.is_empty() {
            log::info!(
                "no metrics found{}",
                if tenant_id.is_empty() {
                    String::new()
                } else {
                    format!(" for tenant id: {tenant_id}")
                }
            );
            return Ok(());
        }
        let total: usize = map.iter().map(|(_, r)| r.len()).sum();
        log::info!(
            "Found {} unique metric names to import. Total import/export requests to make {}",
            map.len(),
            total
        );
        if !cfg.inter_cluster && !crate::prompt(cfg.assume_yes, "Continue?") {
            return Ok(());
        }
        map
    };

    // Build the work queue of concrete filters.
    let mut queue: VecDeque<Filter> = VecDeque::new();
    for (metric, m_ranges) in &metrics_map {
        let matcher = build_match_with_filter(&cfg.filter_match, metric).map_err(|e| {
            format!(
                "failed to build filter {:?} for metric {metric:?}: {e}",
                cfg.filter_match
            )
        })?;
        for (s, e) in m_ranges {
            queue.push_back(Filter {
                matcher: matcher.clone(),
                time_start: format_rfc3339(*s),
                time_end: format_rfc3339(*e),
            });
        }
    }

    run_workers(cfg, cancel, &src_url, &dst_url, queue, stats)
}

fn explore(
    cfg: &VmNativeConfig,
    tenant_id: &str,
    ranges: &[(i64, i64)],
) -> Result<MetricRanges, String> {
    log::info!("Exploring metrics...");
    let base = Filter {
        matcher: cfg.filter_match.clone(),
        ..Default::default()
    };
    // Preserve first-seen metric order for deterministic output.
    let mut order: Vec<String> = Vec::new();
    let mut map: std::collections::HashMap<String, Vec<(i64, i64)>> =
        std::collections::HashMap::new();
    for r in ranges {
        let names =
            cfg.src
                .explore(&base, tenant_id, &format_rfc3339(r.0), &format_rfc3339(r.1))?;
        for name in names {
            let entry = map.entry(name.clone()).or_insert_with(|| {
                order.push(name.clone());
                Vec::new()
            });
            entry.push(*r);
        }
    }
    Ok(order
        .into_iter()
        .map(|m| {
            let ranges = map.remove(&m).unwrap_or_default();
            (m, ranges)
        })
        .collect())
}

fn run_workers(
    cfg: &VmNativeConfig,
    cancel: &AtomicBool,
    src_url: &str,
    dst_url: &str,
    queue: VecDeque<Filter>,
    stats: &Arc<Stats>,
) -> Result<(), String> {
    let cc = cfg.concurrency.max(1);
    let queue = Arc::new(Mutex::new(queue));
    let first_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    std::thread::scope(|scope| {
        for _ in 0..cc {
            let queue = Arc::clone(&queue);
            let first_error = Arc::clone(&first_error);
            let stats = Arc::clone(stats);
            let src = Arc::clone(&cfg.src);
            let dst = Arc::clone(&cfg.dst);
            let backoff = Arc::clone(&cfg.backoff);
            let disable_per_metric = cfg.disable_per_metric;
            let rate_limit = cfg.rate_limit;
            let src_url = src_url.to_string();
            let dst_url = dst_url.to_string();
            scope.spawn(move || loop {
                if cancel.load(Ordering::SeqCst) {
                    return;
                }
                let filter = { queue.lock().unwrap().pop_front() };
                let Some(filter) = filter else { return };

                let result = if disable_per_metric {
                    run_single(&src, &dst, &src_url, &dst_url, &filter, rate_limit, &stats)
                        .map_err(|e| e.msg)
                } else {
                    let (_, res) = backoff.retry(cancel, || {
                        run_single(&src, &dst, &src_url, &dst_url, &filter, rate_limit, &stats)
                    });
                    res
                };
                if let Err(e) = result {
                    let mut slot = first_error.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                    cancel.store(true, Ordering::SeqCst);
                    return;
                }
            });
        }
    });

    let err = first_error.lock().unwrap().take();
    match err {
        Some(e) => Err(format!("import process failed: {e}")),
        None => Ok(()),
    }
}

/// Streams one export→import request. Ports `runSingle`.
fn run_single(
    src: &NativeClient,
    dst: &NativeClient,
    src_url: &str,
    dst_url: &str,
    filter: &Filter,
    rate_limit: i64,
    stats: &Stats,
) -> Result<(), RetryError> {
    let resp = src.export(src_url, filter).map_err(RetryError::retryable)?;
    // Per-request byte-rate limiting (matches upstream's fresh Limiter per
    // runSingle call): throttle the bytes streamed from export into import.
    let written = if rate_limit > 0 {
        let limiter = Arc::new(crate::transport::Limiter::new(rate_limit));
        let reader = crate::transport::RateLimitedReader::new(resp, limiter);
        dst.import(dst_url, reader).map_err(RetryError::retryable)?
    } else {
        dst.import(dst_url, resp).map_err(RetryError::retryable)?
    };
    stats.bytes.fetch_add(written, Ordering::SeqCst);
    stats.requests.fetch_add(1, Ordering::SeqCst);
    Ok(())
}
