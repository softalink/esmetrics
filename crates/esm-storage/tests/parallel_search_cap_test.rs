//! With `-search.maxWorkersPerQuery`-style cap installed, the parallel
//! unpack must produce results identical to the serial path (spec §4:
//! results identical across cap values).

use std::path::PathBuf;

use esm_storage::{
    marshal_metric_name_raw, MetricRow, OpenOptions, SeriesBlock, Storage, TagFilters, TimeRange,
    NO_DEADLINE,
};

const MSEC_PER_MINUTE: i64 = 60 * 1000;
const RETENTION_365D_MSECS: i64 = 365 * 24 * 3600 * 1000;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esm-storage-parallel-search-cap-test-{name}-{}",
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

fn make_row(metric: &str, host: usize, timestamp: i64, value: f64) -> MetricRow {
    let mut raw = Vec::new();
    let host_tag = format!("host-{host}");
    let labels: Vec<(&[u8], &[u8])> = vec![
        (b"__name__", metric.as_bytes()),
        (b"host", host_tag.as_bytes()),
    ];
    marshal_metric_name_raw(&mut raw, &labels);
    MetricRow {
        metric_name_raw: raw,
        timestamp,
        value,
    }
}

#[test]
fn capped_parallel_unpack_matches_serial_path() {
    // Cap the whole process to 2 workers per query BEFORE any search runs.
    // Assumes available_parallelism() > 1: on a 1-CPU host the unpack pool
    // takes the serial fast path regardless of the cap, and this test
    // degenerates to comparing the serial path with itself (same latent
    // limitation as the sibling parallel_search_test.rs).
    esm_common::query_workers::set_max_workers(2);
    assert_eq!(esm_common::query_workers::max_workers(), 2);

    let dir = test_dir("equivalence");
    let storage = Storage::must_open(
        &dir,
        OpenOptions {
            retention_msecs: RETENTION_365D_MSECS,
            ..Default::default()
        },
    );
    let base_ts = now_ms() - 200 * MSEC_PER_MINUTE;

    // 40 series x 60 samples across two flushed parts so the parallel path
    // (total > MIN_PARALLEL_SERIES) actually engages under the cap.
    for part in 0..2 {
        let mut rows = Vec::new();
        for host in 0..40 {
            for i in 0..30 {
                let ts = base_ts + (part * 30 + i) * MSEC_PER_MINUTE;
                rows.push(make_row(
                    "cap_metric",
                    host,
                    ts,
                    (host * 1000 + i as usize) as f64,
                ));
            }
        }
        storage.add_rows(&rows, 64);
        storage.force_flush();
    }

    let mut tfs = TagFilters::new();
    tfs.add(&[], b"cap_metric", false, false).unwrap();
    let tr = TimeRange {
        min_timestamp: base_ts,
        max_timestamp: base_ts + 200 * MSEC_PER_MINUTE,
    };

    // Serial reference via Search::next_series.
    let mut search = storage
        .search(&[tfs.clone()], tr, 100_000, NO_DEADLINE)
        .expect("search must succeed");
    let mut serial: Vec<(String, Vec<i64>, Vec<f64>)> = Vec::new();
    let mut sb = SeriesBlock::default();
    while search
        .next_series(&mut sb)
        .expect("next_series must succeed")
    {
        serial.push((
            sb.metric_name.to_string(),
            sb.timestamps.clone(),
            sb.values.clone(),
        ));
    }
    serial.sort_by(|a, b| a.0.cmp(&b.0));
    drop(search);

    // Capped parallel path.
    let mut parallel: Vec<(String, Vec<i64>, Vec<f64>)> = storage
        .search_series_parallel(&[tfs], tr, 100_000, NO_DEADLINE)
        .expect("search_series_parallel must succeed")
        .into_iter()
        .map(|sb| (sb.metric_name.to_string(), sb.timestamps, sb.values))
        .collect();
    parallel.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(serial.len(), 40, "fixture must produce 40 series");
    assert_eq!(serial, parallel, "capped parallel unpack must equal serial");

    storage.must_close();
    let _ = std::fs::remove_dir_all(&dir);
}
