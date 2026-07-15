//! Tests for the parallel per-series unpack path
//! (`Storage::search_series_parallel`): result equivalence with the serial
//! `Search::next_series` loop and correctness under concurrent queries.

use std::path::PathBuf;
use std::sync::Arc;

use esm_storage::{
    marshal_metric_name_raw, MetricRow, OpenOptions, SeriesBlock, Storage, TagFilters, TimeRange,
    NO_DEADLINE,
};

const MSEC_PER_MINUTE: i64 = 60 * 1000;
const RETENTION_365D_MSECS: i64 = 365 * 24 * 3600 * 1000;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esm-storage-parallel-search-test-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn open_storage(path: &PathBuf) -> Storage {
    Storage::must_open(
        path,
        OpenOptions {
            retention_msecs: RETENTION_365D_MSECS,
            ..Default::default()
        },
    )
}

fn make_row(metric: &str, tags: &[(&str, &str)], timestamp: i64, value: f64) -> MetricRow {
    let mut raw = Vec::new();
    let mut labels: Vec<(&[u8], &[u8])> = vec![(b"__name__", metric.as_bytes())];
    for (k, v) in tags {
        labels.push((k.as_bytes(), v.as_bytes()));
    }
    marshal_metric_name_raw(&mut raw, &labels);
    MetricRow {
        metric_name_raw: raw,
        timestamp,
        value,
    }
}

fn metric_group_filter(metric_re: &str) -> TagFilters {
    let mut tfs = TagFilters::new();
    tfs.add(&[], metric_re.as_bytes(), false, true).unwrap();
    tfs
}

/// Reads all the series matching `tfss` on `tr` with the serial
/// `next_series` path, sorted by metric name.
fn read_series_serial(
    storage: &Storage,
    tfss: &[TagFilters],
    tr: TimeRange,
) -> Vec<(String, Vec<i64>, Vec<f64>)> {
    let mut search = storage
        .search(tfss, tr, 100_000, NO_DEADLINE)
        .expect("search must succeed");
    let mut out = Vec::new();
    let mut sb = SeriesBlock::default();
    while search
        .next_series(&mut sb)
        .expect("next_series must succeed")
    {
        out.push((
            sb.metric_name.to_string(),
            sb.timestamps.clone(),
            sb.values.clone(),
        ));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Reads the same result via the parallel unpack path.
fn read_series_parallel(
    storage: &Storage,
    tfss: &[TagFilters],
    tr: TimeRange,
) -> Vec<(String, Vec<i64>, Vec<f64>)> {
    let mut out: Vec<(String, Vec<i64>, Vec<f64>)> = storage
        .search_series_parallel(tfss, tr, 100_000, NO_DEADLINE)
        .expect("search_series_parallel must succeed")
        .into_iter()
        .map(|sb| (sb.metric_name.to_string(), sb.timestamps, sb.values))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Builds a storage with `num_metrics * num_hosts` series spread over
/// multiple flushed parts (plus overlapping duplicate timestamps) so the
/// parallel unpack exercises multi-block merge, duplicate-timestamp
/// collapsing and time-range trimming.
fn fill_storage(s: &Storage, base_ts: i64, num_metrics: usize, num_hosts: usize) {
    let mut rows = Vec::new();
    for m in 0..num_metrics {
        for h in 0..num_hosts {
            let metric = format!("parallel_metric_{m}");
            let host = format!("host-{h}");
            for i in 0..40 {
                rows.push(make_row(
                    &metric,
                    &[("host", &host)],
                    base_ts + i * MSEC_PER_MINUTE,
                    (m * 1000 + h * 10 + i as usize) as f64,
                ));
            }
        }
    }
    s.add_rows(&rows, 64);
    s.force_flush();

    // A second batch in a separate part, overlapping the tail of the first
    // one (same timestamps, different values → duplicate-timestamp path).
    let mut rows2 = Vec::new();
    for m in 0..num_metrics {
        for h in 0..num_hosts {
            let metric = format!("parallel_metric_{m}");
            let host = format!("host-{h}");
            for i in 30..70 {
                rows2.push(make_row(
                    &metric,
                    &[("host", &host)],
                    base_ts + i * MSEC_PER_MINUTE,
                    -((m * 1000 + h * 10 + i as usize) as f64),
                ));
            }
        }
    }
    s.add_rows(&rows2, 64);
    s.force_flush();
}

// The parallel path must return exactly the same series (names, timestamps,
// values) as the serial next_series loop, including on a trimmed time range.
#[test]
fn parallel_search_matches_serial_search() {
    let path = test_dir("equivalence");
    let s = open_storage(&path);
    let base_ts = now_ms() - 2 * 24 * 3600 * 1000;
    fill_storage(&s, base_ts, 10, 20);

    let tfss = vec![metric_group_filter("parallel_metric_.*")];
    // Full range and a trimmed range (cuts both the head and the tail).
    let trs = [
        TimeRange {
            min_timestamp: base_ts - MSEC_PER_MINUTE,
            max_timestamp: base_ts + 100 * MSEC_PER_MINUTE,
        },
        TimeRange {
            min_timestamp: base_ts + 10 * MSEC_PER_MINUTE,
            max_timestamp: base_ts + 50 * MSEC_PER_MINUTE,
        },
    ];
    for tr in trs {
        let serial = read_series_serial(&s, &tfss, tr);
        let parallel = read_series_parallel(&s, &tfss, tr);
        assert_eq!(serial.len(), 200, "expected all series to be found");
        assert_eq!(serial, parallel, "parallel result diverges on {tr:?}");
    }

    // A single-series query goes through the serial fast path.
    let mut tfs = metric_group_filter("parallel_metric_3");
    tfs.add(b"host", b"host-7", false, false).unwrap();
    let tfss_one = vec![tfs];
    assert_eq!(
        read_series_serial(&s, &tfss_one, trs[0]),
        read_series_parallel(&s, &tfss_one, trs[0]),
    );

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// No matching series → empty result, no pool interaction issues.
#[test]
fn parallel_search_empty_result() {
    let path = test_dir("empty");
    let s = open_storage(&path);
    let base_ts = now_ms() - 24 * 3600 * 1000;
    fill_storage(&s, base_ts, 1, 1);

    let tfss = vec![metric_group_filter("no_such_metric_.*")];
    let tr = TimeRange {
        min_timestamp: base_ts,
        max_timestamp: base_ts + 100 * MSEC_PER_MINUTE,
    };
    let out = s
        .search_series_parallel(&tfss, tr, 100_000, NO_DEADLINE)
        .expect("search must succeed");
    assert!(out.is_empty());

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// Many queries hammering the shared unpack pool concurrently must all get
// complete, correct results (per-query output isolation).
#[test]
fn parallel_search_concurrent_queries() {
    let path = test_dir("concurrent");
    let s = Arc::new(open_storage(&path));
    let base_ts = now_ms() - 2 * 24 * 3600 * 1000;
    fill_storage(&s, base_ts, 8, 16);

    let tr = TimeRange {
        min_timestamp: base_ts,
        max_timestamp: base_ts + 100 * MSEC_PER_MINUTE,
    };
    let tfss = vec![metric_group_filter("parallel_metric_.*")];
    let expected = Arc::new(read_series_serial(&s, &tfss, tr));

    let mut handles = Vec::new();
    for worker in 0..8 {
        let s = Arc::clone(&s);
        let expected = Arc::clone(&expected);
        handles.push(std::thread::spawn(move || {
            for round in 0..20 {
                let tfss = vec![metric_group_filter("parallel_metric_.*")];
                let got = read_series_parallel(&s, &tfss, tr);
                assert_eq!(
                    got, *expected,
                    "diverging result in worker {worker} round {round}"
                );
            }
        }));
    }
    for h in handles {
        h.join().expect("query thread must not panic");
    }

    Arc::try_unwrap(s)
        .unwrap_or_else(|_| panic!("storage still referenced"))
        .must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// An already-expired deadline must surface as an error, not hang the pool.
#[test]
fn parallel_search_expired_deadline() {
    let path = test_dir("deadline");
    let s = open_storage(&path);
    let base_ts = now_ms() - 24 * 3600 * 1000;
    fill_storage(&s, base_ts, 4, 8);

    let tfss = vec![metric_group_filter("parallel_metric_.*")];
    let tr = TimeRange {
        min_timestamp: base_ts,
        max_timestamp: base_ts + 100 * MSEC_PER_MINUTE,
    };
    let res = s.search_series_parallel(&tfss, tr, 100_000, 1);
    assert!(res.is_err(), "expired deadline must fail the search");

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}
