//! Snapshot creation and lifecycle tests: `Storage::must_create_snapshot`,
//! `must_list_snapshots`, and the resulting snapshot directories being
//! independently openable storage trees with the exact pre-snapshot data.

use std::path::PathBuf;
use std::sync::Arc;

use esm_storage::{
    marshal_metric_name_raw, MetricRow, OpenOptions, SeriesBlock, Storage, TagFilters, TimeRange,
    MSEC_PER_DAY, NO_DEADLINE,
};

const RETENTION_365D_MSECS: i64 = 365 * 24 * 3600 * 1000;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esm-storage-snapshot-test-{name}-{}",
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

const RETENTION: i64 = RETENTION_365D_MSECS;

/// Ingests `n` rows of a gauge, snapshots, and verifies the snapshot dir is
/// a complete, independently openable storage containing the same data.
#[test]
fn snapshot_is_openable_and_complete() {
    let dir = test_dir("openable");
    let storage = open_storage(&dir, RETENTION);
    let ts0 = now_ms() - 1000 * 60;
    let rows: Vec<_> = (0..1000)
        .map(|i| make_row("snap_gauge", &[("i", "x")], ts0 + i * 10, i as f64))
        .collect();
    storage.add_rows(&rows, 64);
    storage.force_flush();

    let name = storage.must_create_snapshot();
    // Names are valid per the snapshotutil format.
    assert_eq!(storage.must_list_snapshots(), vec![name.clone()]);

    // Rows added AFTER the snapshot must not leak into it.
    let late = make_row("snap_gauge", &[("i", "x")], ts0 + 100_000, 1.0);
    storage.add_rows(&[late], 64);
    storage.force_flush();

    // The snapshot is a valid storage tree of its own.
    let snap_path = dir.join("snapshots").join(&name);
    let snap_storage = open_storage(&snap_path, RETENTION);
    let tr = TimeRange {
        min_timestamp: ts0 - MSEC_PER_DAY,
        max_timestamp: ts0 + MSEC_PER_DAY,
    };
    let tfs = metric_group_filter("snap_gauge");
    let series = read_series(&snap_storage, &[tfs], tr);
    assert_eq!(series.len(), 1);
    assert_eq!(
        series[0].1.len(),
        1000,
        "snapshot must contain exactly the pre-snapshot rows"
    );
    snap_storage.must_close();

    storage.must_close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn list_and_delete_snapshots() {
    let dir = test_dir("list-delete");
    let storage = open_storage(&dir, RETENTION);
    let row = make_row("del_gauge", &[("a", "b")], now_ms(), 42.0);
    storage.add_rows(&[row], 64);
    storage.force_flush();

    assert!(storage.must_list_snapshots().is_empty());
    let s1 = storage.must_create_snapshot();
    let s2 = storage.must_create_snapshot();
    let listed = storage.must_list_snapshots();
    assert_eq!(listed.len(), 2);
    assert!(listed.contains(&s1) && listed.contains(&s2));
    // Sorted ascending (names sort chronologically).
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(listed, sorted);

    // Deleting a snapshot removes its dir and must not disturb live data
    // (hard links: the live parts keep their inodes).
    storage.delete_snapshot(&s1).unwrap();
    assert_eq!(storage.must_list_snapshots(), vec![s2.clone()]);
    assert!(!dir.join("snapshots").join(&s1).exists());

    // Unknown / invalid names are errors, not panics.
    assert!(storage.delete_snapshot(&s1).is_err());
    assert!(storage.delete_snapshot("../../etc").is_err());
    assert!(storage.delete_snapshot("").is_err());

    storage.delete_snapshot(&s2).unwrap();
    storage.must_close();

    // Live data survived both deletions.
    let reopened = open_storage(&dir, RETENTION);
    let tr = TimeRange {
        min_timestamp: now_ms() - MSEC_PER_DAY,
        max_timestamp: now_ms() + MSEC_PER_DAY,
    };
    let series = read_series(&reopened, &[metric_group_filter("del_gauge")], tr);
    assert_eq!(series.len(), 1);
    reopened.must_close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_under_concurrent_ingestion() {
    let dir = test_dir("concurrent");
    let storage = Arc::new(open_storage(&dir, RETENTION));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let ingest = {
        let storage = Arc::clone(&storage);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let ts0 = now_ms();
            let mut i = 0i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let row = make_row("conc_gauge", &[("w", "1")], ts0 + i, i as f64);
                storage.add_rows(&[row], 64);
                i += 1;
            }
        })
    };

    // Snapshots taken while ingesting must each be valid openable storages.
    let mut names = Vec::new();
    for _ in 0..3 {
        names.push(storage.must_create_snapshot());
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    ingest.join().unwrap();

    for name in &names {
        let snap = open_storage(&dir.join("snapshots").join(name), RETENTION);
        snap.must_close(); // opening validates parts.json vs part dirs
    }
    for name in &names {
        storage.delete_snapshot(name).unwrap();
    }
    match Arc::try_unwrap(storage) {
        Ok(s) => s.must_close(),
        Err(_) => panic!("storage still referenced"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[should_panic(expected = "restore-in-progress")]
fn must_open_panics_on_incomplete_restore() {
    let dir = test_dir("restore-guard");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("restore-in-progress"), b"").unwrap();
    let _ = open_storage(&dir, RETENTION); // must panic
}
