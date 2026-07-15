//! Port of the essential storage_test.go scenarios: open/close lifecycle,
//! AddRows → flush → search round-trips, month-boundary partition routing,
//! reopen persistence, concurrent ingestion, retention enforcement and
//! high-churn series creation with regex search.

use std::path::PathBuf;
use std::sync::Arc;

use esm_storage::{
    marshal_metric_name_raw, MetricName, MetricRow, OpenOptions, SeriesBlock, Storage, TagFilters,
    TimeRange, Tsid, MSEC_PER_DAY, NO_DEADLINE,
};

const MSEC_PER_MINUTE: i64 = 60 * 1000;
const RETENTION_365D_MSECS: i64 = 365 * 24 * 3600 * 1000;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esm-storage-storage-test-{name}-{}",
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

fn open_storage(path: &PathBuf, retention_msecs: i64) -> Storage {
    Storage::must_open(
        path,
        OpenOptions {
            retention_msecs,
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

fn metric_group_filter(metric: &str) -> TagFilters {
    let mut tfs = TagFilters::new();
    tfs.add(&[], metric.as_bytes(), false, false).unwrap();
    tfs
}

/// Reads all the series matching `tfss` on `tr`, sorted by metric name.
fn read_series(
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

// Open/close in a loop: all the background threads must be joined on close,
// so repeated cycles must not deadlock, panic or leak flock errors.
#[test]
fn storage_open_close_loop() {
    let path = test_dir("open-close-loop");
    for i in 0..5 {
        let s = open_storage(&path, RETENTION_365D_MSECS);
        if i % 2 == 0 {
            let ts = now_ms();
            s.add_rows(&[make_row("loop_metric", &[("host", "h1")], ts, 1.5)], 64);
        }
        s.must_close();
    }
    let _ = std::fs::remove_dir_all(&path);
}

// AddRows across two months → force_flush → search_tsids by tag filters →
// read the series back with exact values/timestamps. Also checks the
// month-boundary partition routing on disk.
#[test]
fn storage_add_rows_two_months_and_search() {
    let path = test_dir("two-months");
    let s = open_storage(&path, RETENTION_365D_MSECS);

    let now = now_ms();
    let cur_month = TimeRange::from_partition_timestamp(now);
    // 20 samples per series in the current month and 20 in the previous one.
    let cur_base = cur_month.min_timestamp;
    let prev_base = cur_month.min_timestamp - 20 * MSEC_PER_MINUTE - 1;

    let mut mrs = Vec::new();
    let mut expected: Vec<(i64, f64)> = Vec::new();
    for i in 0..20i64 {
        let ts_prev = prev_base + i * MSEC_PER_MINUTE;
        let ts_cur = cur_base + i * MSEC_PER_MINUTE;
        let v_prev = i as f64 * 1.25 - 10.0;
        let v_cur = i as f64 * 0.5 + 100.0;
        mrs.push(make_row(
            "cpu_usage",
            &[("host", "h1"), ("region", "r1")],
            ts_prev,
            v_prev,
        ));
        mrs.push(make_row(
            "cpu_usage",
            &[("host", "h1"), ("region", "r1")],
            ts_cur,
            v_cur,
        ));
        // A second series that must not leak into the h1 results.
        mrs.push(make_row(
            "cpu_usage",
            &[("host", "h2"), ("region", "r1")],
            ts_cur,
            -v_cur,
        ));
        expected.push((ts_prev, v_prev));
        expected.push((ts_cur, v_cur));
    }
    expected.sort_unstable_by_key(|(ts, _)| *ts);
    s.add_rows(&mrs, 64);
    s.force_flush();

    // Both monthly partitions must exist on disk.
    let prev_month_name = esm_storage::timestamp_to_partition_name(prev_base);
    let cur_month_name = esm_storage::timestamp_to_partition_name(cur_base);
    assert_ne!(prev_month_name, cur_month_name);
    assert!(path
        .join("data")
        .join("small")
        .join(&prev_month_name)
        .exists());
    assert!(path
        .join("data")
        .join("small")
        .join(&cur_month_name)
        .exists());
    assert!(path
        .join("data")
        .join("indexdb")
        .join(&prev_month_name)
        .exists());
    assert!(path
        .join("data")
        .join("indexdb")
        .join(&cur_month_name)
        .exists());

    let tr = TimeRange {
        min_timestamp: prev_base,
        max_timestamp: cur_base + 20 * MSEC_PER_MINUTE,
    };

    // search_tsids by metric group filter: 2 series (h1, h2).
    let tfss = vec![metric_group_filter("cpu_usage")];
    let tsids = s.search_tsids(&tfss, tr, 100, NO_DEADLINE).unwrap();
    assert_eq!(tsids.len(), 2, "unexpected number of TSIDs");
    assert!(
        tsids.windows(2).all(|w| w[0] < w[1]),
        "TSIDs must be sorted"
    );

    // search_tsids by (metric group, host=h1): 1 series.
    let mut tfs = metric_group_filter("cpu_usage");
    tfs.add(b"host", b"h1", false, false).unwrap();
    let tsids = s
        .search_tsids(&[tfs.clone()], tr, 100, NO_DEADLINE)
        .unwrap();
    assert_eq!(tsids.len(), 1);

    // Read the h1 series back: exact timestamps and values across both
    // months.
    let series = read_series(&s, &[tfs], tr);
    assert_eq!(series.len(), 1);
    let (name, timestamps, values) = &series[0];
    assert!(
        name.contains("cpu_usage") && name.contains("h1"),
        "bad name: {name}"
    );
    let got: Vec<(i64, f64)> = timestamps
        .iter()
        .copied()
        .zip(values.iter().copied())
        .collect();
    assert_eq!(got, expected);

    // Time-range trimming: only the current-month samples.
    let cur_tr = TimeRange {
        min_timestamp: cur_base,
        max_timestamp: cur_base + 20 * MSEC_PER_MINUTE,
    };
    let mut tfs = metric_group_filter("cpu_usage");
    tfs.add(b"host", b"h1", false, false).unwrap();
    let series = read_series(&s, &[tfs], cur_tr);
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].1.len(), 20);
    assert!(series[0].1.iter().all(|&ts| ts >= cur_base));

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// Close → reopen → the data must still be searchable (parts.json + file
// parts + per-partition indexdb persistence).
#[test]
fn storage_reopen_persistence() {
    let path = test_dir("reopen");
    let now = now_ms();
    let base = TimeRange::from_partition_timestamp(now).min_timestamp;

    let n_samples = 50i64;
    {
        let s = open_storage(&path, RETENTION_365D_MSECS);
        let mut mrs = Vec::new();
        for i in 0..n_samples {
            mrs.push(make_row(
                "mem_usage",
                &[("host", "h7")],
                base + i * MSEC_PER_MINUTE,
                i as f64 * 2.0,
            ));
        }
        s.add_rows(&mrs, 64);
        s.force_flush();
        s.must_close();
    }

    let s = open_storage(&path, RETENTION_365D_MSECS);
    let tr = TimeRange {
        min_timestamp: base,
        max_timestamp: base + n_samples * MSEC_PER_MINUTE,
    };
    let tfss = vec![metric_group_filter("mem_usage")];
    let tsids = s.search_tsids(&tfss, tr, 100, NO_DEADLINE).unwrap();
    assert_eq!(tsids.len(), 1, "series must survive a reopen");

    let series = read_series(&s, &tfss, tr);
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].1.len(), n_samples as usize);
    for (i, (&ts, &v)) in series[0].1.iter().zip(series[0].2.iter()).enumerate() {
        assert_eq!(ts, base + i as i64 * MSEC_PER_MINUTE);
        assert_eq!(v, i as f64 * 2.0);
    }

    // The metric names search must work after reopen too.
    let names = s.search_metric_names(&tfss, tr, 100, NO_DEADLINE).unwrap();
    assert_eq!(names.len(), 1);
    let mut mn = MetricName::default();
    mn.unmarshal(&names[0]).unwrap();
    assert_eq!(mn.metric_group, b"mem_usage");

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// Concurrent add_rows from 4 threads plus concurrent searches.
#[test]
fn storage_concurrent_add_rows_and_search() {
    let path = test_dir("concurrent");
    let s = Arc::new(open_storage(&path, RETENTION_365D_MSECS));
    let now = now_ms();
    let base = TimeRange::from_partition_timestamp(now).min_timestamp;

    const THREADS: usize = 4;
    const SERIES_PER_THREAD: usize = 25;
    const SAMPLES_PER_SERIES: i64 = 20;

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let s = Arc::clone(&s);
            scope.spawn(move || {
                for series in 0..SERIES_PER_THREAD {
                    let host = format!("host-{t}-{series}");
                    let mut mrs = Vec::new();
                    for i in 0..SAMPLES_PER_SERIES {
                        mrs.push(make_row(
                            "conc_metric",
                            &[("host", &host)],
                            base + i * MSEC_PER_MINUTE,
                            (t * 1000 + series) as f64 + i as f64,
                        ));
                    }
                    s.add_rows(&mrs, 64);
                }
            });
        }
        // A concurrent searcher: results may be partial but must never fail.
        let s2 = Arc::clone(&s);
        scope.spawn(move || {
            let tr = TimeRange {
                min_timestamp: base,
                max_timestamp: base + SAMPLES_PER_SERIES * MSEC_PER_MINUTE,
            };
            for _ in 0..10 {
                let tfss = vec![metric_group_filter("conc_metric")];
                let _ = s2.search_tsids(&tfss, tr, 100_000, NO_DEADLINE).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        });
    });

    s.force_flush();
    let tr = TimeRange {
        min_timestamp: base,
        max_timestamp: base + SAMPLES_PER_SERIES * MSEC_PER_MINUTE,
    };
    let tfss = vec![metric_group_filter("conc_metric")];
    let tsids = s.search_tsids(&tfss, tr, 100_000, NO_DEADLINE).unwrap();
    assert_eq!(tsids.len(), THREADS * SERIES_PER_THREAD);

    let series = read_series(&s, &tfss, tr);
    assert_eq!(series.len(), THREADS * SERIES_PER_THREAD);
    for (_, timestamps, _) in &series {
        assert_eq!(timestamps.len(), SAMPLES_PER_SERIES as usize);
    }

    match Arc::try_unwrap(s) {
        Ok(s) => s.must_close(),
        Err(_) => panic!("BUG: storage references leaked from the test threads"),
    }
    let _ = std::fs::remove_dir_all(&path);
}

// Retention: rows in a partition older than the retention are not
// searchable after retention enforcement drops the partition.
#[test]
fn storage_retention_drops_old_partitions() {
    let path = test_dir("retention");
    let now = now_ms();

    // Insert rows ~40 days old with a 60-day retention.
    let old_ts = now - 40 * MSEC_PER_DAY;
    let old_month = TimeRange::from_partition_timestamp(old_ts);
    assert!(
        old_month.max_timestamp < TimeRange::from_partition_timestamp(now).min_timestamp,
        "test requires the old rows to land in an older month"
    );
    {
        let s = open_storage(&path, 60 * MSEC_PER_DAY);
        let mut mrs = Vec::new();
        for i in 0..10i64 {
            mrs.push(make_row(
                "old_metric",
                &[("host", "h1")],
                old_ts + i * MSEC_PER_MINUTE,
                i as f64,
            ));
            mrs.push(make_row("new_metric", &[("host", "h1")], now - i, i as f64));
        }
        s.add_rows(&mrs, 64);
        s.force_flush();

        let tr = TimeRange {
            min_timestamp: old_ts,
            max_timestamp: now,
        };
        let tsids = s
            .search_tsids(&[metric_group_filter("old_metric")], tr, 100, NO_DEADLINE)
            .unwrap();
        assert_eq!(
            tsids.len(),
            1,
            "old rows must be searchable within retention"
        );
        s.must_close();
    }

    // Reopen with a 7-day retention: the old month is fully outside the
    // retention, so the enforcement must drop its partition.
    let s = open_storage(&path, 7 * MSEC_PER_DAY);
    s.debug_enforce_retention();

    let tr = TimeRange {
        min_timestamp: old_ts,
        max_timestamp: now,
    };
    let tsids = s
        .search_tsids(&[metric_group_filter("old_metric")], tr, 100, NO_DEADLINE)
        .unwrap();
    assert!(
        tsids.is_empty(),
        "old rows must not be searchable after enforcement"
    );
    let series = read_series(&s, &[metric_group_filter("old_metric")], tr);
    assert!(series.is_empty());

    // The new rows must still be there.
    let tsids = s
        .search_tsids(&[metric_group_filter("new_metric")], tr, 100, NO_DEADLINE)
        .unwrap();
    assert_eq!(tsids.len(), 1);

    // The dropped partition directories must be removed from disk.
    let old_name = esm_storage::timestamp_to_partition_name(old_ts);
    assert!(!path.join("data").join("small").join(&old_name).exists());
    assert!(!path.join("data").join("big").join(&old_name).exists());
    assert!(!path.join("data").join("indexdb").join(&old_name).exists());

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// New-series registration must hit both the global and the per-day index
// paths: a sub-day time range uses the per-day index, while a range covering
// the whole partition month uses the global index.
#[test]
fn storage_series_registered_in_global_and_per_day_indexes() {
    let path = test_dir("global-per-day");
    let s = open_storage(&path, RETENTION_365D_MSECS);

    let now = now_ms();
    let month = TimeRange::from_partition_timestamp(now);
    let day_start = (now / MSEC_PER_DAY) * MSEC_PER_DAY;
    let ts = day_start.max(month.min_timestamp);

    s.add_rows(&[make_row("reg_metric", &[("host", "h9")], ts, 42.5)], 64);
    s.force_flush();

    let tfss = vec![metric_group_filter("reg_metric")];

    // Per-day index path: the search range is a sub-range of the partition
    // month (adjust_time_range keeps it as is).
    let day_tr = TimeRange {
        min_timestamp: ts,
        max_timestamp: ts + MSEC_PER_MINUTE,
    };
    let tsids = s.search_tsids(&tfss, day_tr, 100, NO_DEADLINE).unwrap();
    assert_eq!(tsids.len(), 1, "per-day index lookup failed");

    // Global index path: the search range covers the whole partition month
    // (adjust_time_range switches to the global index time range).
    let tsids = s.search_tsids(&tfss, month, 100, NO_DEADLINE).unwrap();
    assert_eq!(tsids.len(), 1, "global index lookup failed");

    let series = read_series(&s, &tfss, day_tr);
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].1, vec![ts]);
    assert_eq!(series[0].2, vec![42.5]);

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// Multiple flush rounds create multiple file parts; a forced merge must
// consolidate them while preserving all the data (exercises mergeParts,
// swapSrcWithDstParts and parts.json rewrites).
#[test]
fn storage_force_merge_preserves_data() {
    let path = test_dir("force-merge");
    let now = now_ms();
    let base = TimeRange::from_partition_timestamp(now).min_timestamp;

    const ROUNDS: i64 = 5;
    const SAMPLES_PER_ROUND: i64 = 30;

    // Create several file parts via close/reopen cycles (close flushes the
    // in-memory parts to files).
    for round in 0..ROUNDS {
        let s = open_storage(&path, RETENTION_365D_MSECS);
        let mut mrs = Vec::new();
        for i in 0..SAMPLES_PER_ROUND {
            let k = round * SAMPLES_PER_ROUND + i;
            mrs.push(make_row(
                "merge_metric",
                &[("host", "h1")],
                base + k * MSEC_PER_MINUTE,
                k as f64 * 0.25,
            ));
        }
        s.add_rows(&mrs, 64);
        s.must_close();
    }

    let s = open_storage(&path, RETENTION_365D_MSECS);
    s.force_merge_partitions("")
        .expect("force merge must succeed");

    let tr = TimeRange {
        min_timestamp: base,
        max_timestamp: base + ROUNDS * SAMPLES_PER_ROUND * MSEC_PER_MINUTE,
    };
    let series = read_series(&s, &[metric_group_filter("merge_metric")], tr);
    assert_eq!(series.len(), 1);
    let (_, timestamps, values) = &series[0];
    assert_eq!(timestamps.len(), (ROUNDS * SAMPLES_PER_ROUND) as usize);
    for (k, (&ts, &v)) in timestamps.iter().zip(values.iter()).enumerate() {
        assert_eq!(ts, base + k as i64 * MSEC_PER_MINUTE);
        assert_eq!(v, k as f64 * 0.25);
    }

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// High-churn ingestion: 10k distinct series, then search by metric group
// regex.
#[test]
fn storage_high_churn_series_and_regex_search() {
    let path = test_dir("high-churn");
    let s = open_storage(&path, RETENTION_365D_MSECS);

    let now = now_ms();
    let base = TimeRange::from_partition_timestamp(now).min_timestamp;

    const GROUPS: usize = 10;
    const SERIES_PER_GROUP: usize = 1000;

    let mut mrs = Vec::with_capacity(GROUPS * SERIES_PER_GROUP);
    for g in 0..GROUPS {
        for i in 0..SERIES_PER_GROUP {
            mrs.push(make_row(
                &format!("churn_g{g}_total"),
                &[("host", &format!("host-{i}")), ("iter", &format!("{i}"))],
                base + (i as i64 % 60) * MSEC_PER_MINUTE,
                (g * SERIES_PER_GROUP + i) as f64,
            ));
        }
    }
    s.add_rows(&mrs, 64);
    s.force_flush();

    let tr = TimeRange {
        min_timestamp: base,
        max_timestamp: base + 60 * MSEC_PER_MINUTE,
    };

    // Regex on the metric group: churn_g[0-3]_total → 4 groups.
    let mut tfs = TagFilters::new();
    tfs.add(&[], b"churn_g[0-3]_total", false, true).unwrap();
    let tsids = s.search_tsids(&[tfs], tr, 100_000, NO_DEADLINE).unwrap();
    assert_eq!(tsids.len(), 4 * SERIES_PER_GROUP);

    // Exact group match.
    let tsids = s
        .search_tsids(
            &[metric_group_filter("churn_g7_total")],
            tr,
            100_000,
            NO_DEADLINE,
        )
        .unwrap();
    assert_eq!(tsids.len(), SERIES_PER_GROUP);

    // All the TSIDs must be distinct metricIDs.
    let mut metric_ids: Vec<u64> = tsids.iter().map(|t: &Tsid| t.metric_id).collect();
    metric_ids.sort_unstable();
    metric_ids.dedup();
    assert_eq!(metric_ids.len(), SERIES_PER_GROUP);

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// Regression guard for the "task #17" investigation (2026-07-05): a single
// force_flush (no force_merge) must make brand-new series immediately
// searchable — data rows AND tag-index entries — via both search_tsids and
// the search_series_parallel path the HTTP export/query APIs use. Three
// ingest-then-export e2e efforts misdiagnosed sample-timestamp/clock issues
// as a "tag index needs force_merge" contract; this pins the truth.
#[test]
fn flush_only_makes_new_series_searchable() {
    let path = test_dir("flush-only-visibility");
    let s = open_storage(&path, RETENTION_365D_MSECS);
    let now = now_ms();
    let tr = TimeRange {
        min_timestamp: now - 3_600_000,
        max_timestamp: now + 3_600_000,
    };

    for i in 0..20i64 {
        let a = format!("fov_a_{i}");
        let b = format!("fov_b_{i}");
        let (va, vb) = (100.0 + i as f64, 900.0 + i as f64);
        s.add_rows(&[make_row(&a, &[("host", "h")], now, va)], 64);
        s.add_rows(&[make_row(&b, &[("host", "h")], now, vb)], 64);
        s.force_flush();

        let tsids = s
            .search_tsids(&[metric_group_filter(&b)], tr, 100, NO_DEADLINE)
            .unwrap();
        assert_eq!(
            tsids.len(),
            1,
            "iter {i}: tag index must see {b} after flush"
        );

        let blocks = s
            .search_series_parallel(&[metric_group_filter(&b)], tr, 100, NO_DEADLINE)
            .unwrap();
        assert_eq!(blocks.len(), 1, "iter {i}: series search must see {b}");
        assert_eq!(
            blocks[0].values,
            vec![vb],
            "iter {i}: {b} must carry its own value, never {a}'s"
        );
    }

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

// Regression for the decoded-block cache collision behind "task #17": blocks
// with constant-encoded values (e.g. any single-sample block) have
// values_block_size == 0, so the block-stream writer never advances
// values_block_offset and EVERY such block in a part sits at values offset 0.
// The decoded-block cache keys by (part_id, values_block_offset); before the
// fix, querying series A and then series B (both single-sample, same
// in-memory part) returned A's cached samples for B.
#[test]
fn decoded_block_cache_does_not_leak_across_const_encoded_blocks() {
    let path = test_dir("decoded-cache-collision");
    let s = open_storage(&path, RETENTION_365D_MSECS);
    let now = now_ms();
    let tr = TimeRange {
        min_timestamp: now - 3_600_000,
        max_timestamp: now + 3_600_000,
    };

    s.add_rows(&[make_row("dcc_a", &[], now, 11.0)], 64);
    s.add_rows(&[make_row("dcc_b", &[], now, 22.0)], 64);
    s.force_flush();

    // Query A first: populates the decoded-block cache.
    let blocks_a = s
        .search_series_parallel(&[metric_group_filter("dcc_a")], tr, 100, NO_DEADLINE)
        .unwrap();
    assert_eq!(blocks_a.len(), 1);
    assert_eq!(blocks_a[0].values, vec![11.0], "A must carry its own value");

    // Query B second: must NOT hit A's cache entry.
    let blocks_b = s
        .search_series_parallel(&[metric_group_filter("dcc_b")], tr, 100, NO_DEADLINE)
        .unwrap();
    assert_eq!(blocks_b.len(), 1);
    assert_eq!(
        blocks_b[0].values,
        vec![22.0],
        "B must carry its own value, not A's cached block"
    );

    s.must_close();
    let _ = std::fs::remove_dir_all(&path);
}
