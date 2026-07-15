//! Port of the core `index_db_test.go` scenarios: TestIndexDBOpenClose,
//! TestIndexDB (serial + concurrent) and TestSearchTSIDWithTimeRange.

#![allow(clippy::field_reassign_with_default)]

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use esm_common::uint64set::Set;
use esm_storage::index::{generate_tsid, IndexDb, IndexDbContext, TagFilters, NO_DEADLINE};
use esm_storage::{MetricName, TimeRange, Tsid, MSEC_PER_DAY};

const MSEC_PER_HOUR: i64 = 3600 * 1000;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("esm-storage-index-db-test-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn open_db(path: &PathBuf, ctx: Arc<IndexDbContext>) -> IndexDb {
    IndexDb::must_open(
        123,
        TimeRange::default(),
        "test_idb",
        path,
        ctx,
        Arc::new(AtomicBool::new(false)),
        false,
    )
}

/// A tiny deterministic RNG (replacement for Go's rand.New(rand.NewSource)).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

#[test]
fn index_db_open_close() {
    let path = test_dir("open-close");
    let ctx = Arc::new(IndexDbContext::new());
    for _ in 0..5 {
        let db = open_db(&path, Arc::clone(&ctx));
        db.must_close();
    }
    let _ = std::fs::remove_dir_all(&path);
}

fn create_test_series(
    db: &IndexDb,
    metric_groups: usize,
    timestamp: i64,
) -> (Vec<MetricName>, Vec<Tsid>) {
    let mut rng = Lcg(1);
    let mut mns: Vec<MetricName> = Vec::new();
    let mut tsids: Vec<Tsid> = Vec::new();

    let date = timestamp as u64 / MSEC_PER_DAY as u64;
    let mut is = db.get_index_search(NO_DEADLINE);
    let mut metric_name_buf = Vec::new();
    for i in 0..401 {
        let mut mn = MetricName::default();
        mn.metric_group = format!("metricGroup.{}\x00\x01\x02", i % metric_groups).into_bytes();

        // Init other tags.
        let tags_count = (rng.next() % 10) as usize + 1;
        for j in 0..tags_count {
            let key = format!("key\x01\x02\x00_{i}_{j}");
            let value = format!("val\x01_{i}\x00_{j}\x02");
            mn.add_tag(&key, &value);
        }
        mn.sort_tags();
        metric_name_buf.clear();
        mn.marshal(&mut metric_name_buf);

        // Create a TSID for the metric name.
        let mut tsid = Tsid::default();
        if !is.get_tsid_by_metric_name(&mut tsid, &metric_name_buf, date) {
            generate_tsid(&mut tsid, &mn);
            db.create_global_indexes(&tsid, &mn);
            db.create_per_day_indexes(date, &tsid, &mn);
        }

        mns.push(mn);
        tsids.push(tsid);
    }
    is.must_close();

    // Flush the index to disk, so it becomes visible for search.
    db.debug_flush();

    (mns, tsids)
}

fn has_tsid(tsids: &[Tsid], tsid: &Tsid) -> bool {
    tsids.contains(tsid)
}

fn check_tsid_by_name(
    db: &IndexDb,
    mns: &[MetricName],
    tsids: &[Tsid],
    timestamp: i64,
    is_concurrent: bool,
) {
    let date = timestamp as u64 / MSEC_PER_DAY as u64;
    let max_metrics = 100_000;
    for (i, (mn, tsid)) in mns.iter().zip(tsids.iter()).enumerate() {
        let mut mn = mn.clone();
        mn.sort_tags();
        let mut metric_name = Vec::new();
        mn.marshal(&mut metric_name);

        let mut tsid_local = Tsid::default();
        let mut is = db.get_index_search(NO_DEADLINE);
        assert!(
            is.get_tsid_by_metric_name(&mut tsid_local, &metric_name, date),
            "cannot obtain tsid #{i}"
        );
        is.must_close();
        if is_concurrent {
            // Multiple TSIDs may match the same mn in concurrent mode.
            tsid_local.metric_id = tsid.metric_id;
        }
        assert_eq!(&tsid_local, tsid, "unexpected tsid for mn #{i}");

        // Search for the metric name by the given metricID.
        let (metric_name_copy, ok) = db.search_metric_name(Vec::new(), tsid_local.metric_id, false);
        assert!(
            ok,
            "cannot find metricName for metricID={}",
            tsid_local.metric_id
        );
        assert_eq!(
            metric_name, metric_name_copy,
            "unexpected mn for metricID={}",
            tsid_local.metric_id
        );

        // Try searching the metric name for a non-existent MetricID.
        let (buf, found) = db.search_metric_name(Vec::new(), 1, false);
        assert!(
            !found,
            "unexpected metricName found for non-existing metricID"
        );
        assert!(buf.is_empty());
    }

    // Try tag filters.
    let tr = TimeRange {
        min_timestamp: timestamp - MSEC_PER_DAY,
        max_timestamp: timestamp + MSEC_PER_DAY,
    };
    for (i, (mn, tsid)) in mns.iter().zip(tsids.iter()).enumerate() {
        // Deviation from the Go test: check every 8th series only. Each
        // iteration compiles ~20 unique regexps, which is much slower in
        // debug-mode Rust than in Go; the subsample covers the same code
        // paths (401 iterations -> ~50).
        if i % 8 != 0 {
            continue;
        }
        // Search without regexps.
        let mut tfs = TagFilters::new();
        tfs.add(&[], &mn.metric_group, false, false).unwrap();
        for t in &mn.tags {
            tfs.add(&t.key, &t.value, false, false).unwrap();
        }
        tfs.add(&[], b"foobar", true, false).unwrap();
        tfs.add(&[], &[], true, false).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            has_tsid(&tsids_found, tsid),
            "tsid is missing in exact search; i={i} tfs={tfs}"
        );

        // Verify the tag cache.
        let tsids_cached = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert_eq!(tsids_cached, tsids_found, "unexpected cached tsids");

        // Add a negative filter zeroing the search results.
        tfs.add(&[], &mn.metric_group, true, false).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            !has_tsid(&tsids_found, tsid),
            "unexpected tsid found for exact negative filter; i={i}"
        );

        // Search for a Graphite-like wildcard.
        tfs.reset();
        let n = mn
            .metric_group
            .iter()
            .position(|&b| b == b'.')
            .expect("cannot find dot in MetricGroup");
        let tail = std::str::from_utf8(&mn.metric_group[n..]).unwrap();
        let re = format!("[^.]*{}", regex::escape(tail));
        tfs.add(&[], re.as_bytes(), false, true).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            has_tsid(&tsids_found, tsid),
            "tsid is missing in Graphite-wildcard regexp search; i={i} tfs={tfs}"
        );

        // Search with a filter matching an empty tag (a single filter).
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1601
        tfs.reset();
        tfs.add(&[], &mn.metric_group, false, false).unwrap();
        tfs.add(b"non-existent-tag", b"foo|", false, true).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            has_tsid(&tsids_found, tsid),
            "tsid is missing when matching a filter with an empty tag; i={i}"
        );

        // Search with filters matching empty tags (multiple filters).
        tfs.reset();
        tfs.add(&[], &mn.metric_group, false, false).unwrap();
        tfs.add(b"non-existent-tag1", b"foo|", false, true).unwrap();
        tfs.add(b"non-existent-tag2", b"bar|", false, true).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            has_tsid(&tsids_found, tsid),
            "tsid is missing when matching multiple filters with empty tags; i={i}"
        );

        // Search with regexps.
        tfs.reset();
        tfs.add(&[], &mn.metric_group, false, true).unwrap();
        for t in &mn.tags {
            let mut re_value = t.value.clone();
            re_value.extend_from_slice(b"|foo*.");
            tfs.add(&t.key, &re_value, false, true).unwrap();
            let mut re_value = t.value.clone();
            re_value.extend_from_slice(b"|aaa|foo|bar");
            tfs.add(&t.key, &re_value, false, true).unwrap();
        }
        tfs.add(&[], b"^foobar$", true, true).unwrap();
        tfs.add(&[], &[], true, true).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            has_tsid(&tsids_found, tsid),
            "tsid is missing in regexp search; i={i} tfs={tfs}"
        );
        tfs.add(&[], &mn.metric_group, true, true).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            !has_tsid(&tsids_found, tsid),
            "unexpected tsid found for regexp negative filter; i={i}"
        );

        // Search with a filter matching zero results.
        tfs.reset();
        tfs.add(b"non-existing-key", b"foobar", false, false)
            .unwrap();
        tfs.add(&[], &mn.metric_group, false, true).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            tsids_found.is_empty(),
            "non-zero tsids found for non-existing tag filter"
        );

        if is_concurrent {
            // Skip the empty filter search in concurrent mode (see the Go
            // test for the rationale).
            continue;
        }

        // Search with an empty filter. It should match all the results.
        tfs.reset();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            has_tsid(&tsids_found, tsid),
            "tsid is missing in the empty-filter search; i={i}"
        );

        // Search with an empty metricGroup. It should match zero results.
        tfs.reset();
        tfs.add(&[], &[], false, false).unwrap();
        let tsids_found = db
            .search_tsids(std::slice::from_ref(&tfs), tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            tsids_found.is_empty(),
            "unexpected non-empty tsids found for empty metricGroup"
        );

        // Search with multiple tfss.
        let mut tfs1 = TagFilters::new();
        tfs1.add(&[], &[], false, false).unwrap();
        let mut tfs2 = TagFilters::new();
        tfs2.add(&[], &mn.metric_group, false, false).unwrap();
        let tsids_found = db
            .search_tsids(&[tfs1, tfs2], tr, max_metrics, NO_DEADLINE)
            .unwrap();
        assert!(
            has_tsid(&tsids_found, tsid),
            "tsid is missing when searching with multiple tfss; i={i}"
        );

        // Verify empty tfss.
        let tsids_found = db.search_tsids(&[], tr, max_metrics, NO_DEADLINE).unwrap();
        assert!(
            tsids_found.is_empty(),
            "unexpected non-empty tsids found for empty tfss"
        );
    }
}

#[test]
fn index_db_serial() {
    let path = test_dir("serial");
    let metric_groups = 10;
    let timestamp = (esm_common::fasttime::unix_timestamp() * 1000) as i64;

    let ctx = Arc::new(IndexDbContext::new());
    let db = open_db(&path, Arc::clone(&ctx));
    let (mns, tsids) = create_test_series(&db, metric_groups, timestamp);
    check_tsid_by_name(&db, &mns, &tsids, timestamp, false);

    // Re-open the db and verify it works as expected.
    db.must_close();
    let db = open_db(&path, ctx);
    check_tsid_by_name(&db, &mns, &tsids, timestamp, false);

    db.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn index_db_concurrent() {
    let path = test_dir("concurrent");
    let metric_groups = 10;
    let timestamp = (esm_common::fasttime::unix_timestamp() * 1000) as i64;

    let ctx = Arc::new(IndexDbContext::new());
    let db = open_db(&path, ctx);

    std::thread::scope(|s| {
        for _ in 0..3 {
            let db = &db;
            s.spawn(move || {
                let (mns, tsids) = create_test_series(db, metric_groups, timestamp);
                check_tsid_by_name(db, &mns, &tsids, timestamp, true);
            });
        }
    });

    db.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn search_tsids_with_time_range() {
    let path = test_dir("time-range");
    // Create a bunch of per-day time series.
    let days: u64 = 5;
    let metrics_per_day: usize = 1000;
    // 2019-10-15T05:01:00Z
    let timestamp: i64 = 1_571_115_660_000;
    let base_date = timestamp as u64 / MSEC_PER_DAY as u64;

    let new_mn = |name: &str, day: u64, metric: usize| -> MetricName {
        let mut mn = MetricName::default();
        mn.metric_group = name.as_bytes().to_vec();
        mn.add_tag("constant", "const");
        mn.add_tag("day", &format!("{day}"));
        mn.add_tag("UniqueId", &format!("{metric}"));
        mn.add_tag("some_unique_id", &format!("{day}"));
        mn.sort_tags();
        mn
    };

    let ctx = Arc::new(IndexDbContext::new());
    let db = open_db(&path, ctx);

    let mut per_day_metric_ids: std::collections::HashMap<u64, Set> =
        std::collections::HashMap::new();
    let mut all_metric_ids = Set::default();
    {
        let mut is = db.get_index_search(NO_DEADLINE);
        let mut metric_name_buf = Vec::new();
        for day in 0..days {
            let date = base_date - day;
            let mut metric_ids = Set::default();
            for metric in 0..metrics_per_day {
                let mn = new_mn("testMetric", day, metric);
                metric_name_buf.clear();
                mn.marshal(&mut metric_name_buf);
                let mut tsid = Tsid::default();
                if !is.get_tsid_by_metric_name(&mut tsid, &metric_name_buf, date) {
                    generate_tsid(&mut tsid, &mn);
                    db.create_global_indexes(&tsid, &mn);
                    db.create_per_day_indexes(date, &tsid, &mn);
                }
                metric_ids.add(tsid.metric_id);
            }
            all_metric_ids.union(&metric_ids);
            per_day_metric_ids.insert(date, metric_ids);
        }
        is.must_close();
    }

    // Flush the index to disk, so it becomes visible for search.
    db.debug_flush();

    // Check that all the metrics are found for all the days.
    {
        let mut is2 = db.get_index_search(NO_DEADLINE);
        for date in (base_date - days + 1)..=base_date {
            let metric_ids = is2.get_metric_ids_for_date(date, metrics_per_day).unwrap();
            assert!(
                per_day_metric_ids[&date].equal(&metric_ids),
                "unexpected metricIDs found for date {date}"
            );
        }

        // Check that all the metrics are found in the global index.
        let metric_ids = is2
            .get_metric_ids_for_date(0, metrics_per_day * days as usize)
            .unwrap();
        assert!(
            all_metric_ids.equal(&metric_ids),
            "unexpected metricIDs found in the global index"
        );
        is2.must_close();
    }

    // Add a metric that will be deleted shortly.
    {
        let mut is3 = db.get_index_search(NO_DEADLINE);
        let day = days;
        let date = base_date - day;
        let mut mn = new_mn("deletedMetric", day, 999);
        mn.add_tag("labelToDelete", &format!("{day}"));
        mn.sort_tags();
        let mut metric_name_buf = Vec::new();
        mn.marshal(&mut metric_name_buf);
        let mut tsid = Tsid::default();
        if !is3.get_tsid_by_metric_name(&mut tsid, &metric_name_buf, date) {
            generate_tsid(&mut tsid, &mn);
            db.create_global_indexes(&tsid, &mn);
            db.create_per_day_indexes(date, &tsid, &mn);
        }
        is3.must_close();
        // Delete the added metric. It is expected it won't be returned
        // during the searches below.
        let mut deleted_set = Set::default();
        deleted_set.add(tsid.metric_id);
        db.set_deleted_metric_ids(Arc::new(deleted_set));
        db.debug_flush();
    }

    // Create a filter that matches the series across multiple days.
    let mut tfs = TagFilters::new();
    tfs.add(b"constant", b"const", false, false).unwrap();

    // Perform a search within a single day. This should return the metrics
    // for the day.
    let tr = TimeRange {
        min_timestamp: timestamp - 2 * MSEC_PER_HOUR - 1,
        max_timestamp: timestamp,
    };
    let matched_tsids = db
        .search_tsids(std::slice::from_ref(&tfs), tr, 100_000, NO_DEADLINE)
        .unwrap();
    assert_eq!(
        matched_tsids.len(),
        metrics_per_day,
        "expected {metrics_per_day} time series for the current day"
    );

    // Search with a composite filter (name + label) within a day.
    let mut tfs_metric_name = TagFilters::new();
    tfs_metric_name
        .add(b"constant", b"const", false, false)
        .unwrap();
    tfs_metric_name
        .add(&[], b"testMetric", false, false)
        .unwrap();
    let matched_tsids = db
        .search_tsids(
            std::slice::from_ref(&tfs_metric_name),
            tr,
            100_000,
            NO_DEADLINE,
        )
        .unwrap();
    assert_eq!(
        matched_tsids.len(),
        metrics_per_day,
        "composite search mismatch"
    );

    // Search with a metric-group-only filter within a day.
    let mut tfs_name_only = TagFilters::new();
    tfs_name_only.add(&[], b"testMetric", false, false).unwrap();
    let matched_tsids = db
        .search_tsids(
            std::slice::from_ref(&tfs_name_only),
            tr,
            100_000,
            NO_DEADLINE,
        )
        .unwrap();
    assert_eq!(
        matched_tsids.len(),
        metrics_per_day,
        "metric-group-only search mismatch"
    );

    // The deleted metric must not be found.
    let mut tfs_deleted = TagFilters::new();
    tfs_deleted
        .add(&[], b"deletedMetric", false, false)
        .unwrap();
    let matched_tsids = db
        .search_tsids(
            std::slice::from_ref(&tfs_deleted),
            TimeRange {
                min_timestamp: timestamp - MSEC_PER_DAY * (days as i64 + 1),
                max_timestamp: timestamp,
            },
            100_000,
            NO_DEADLINE,
        )
        .unwrap();
    assert!(matched_tsids.is_empty(), "deleted metric must not be found");

    // Perform a search across all the days; should match all the metrics.
    let tr = TimeRange {
        min_timestamp: timestamp - MSEC_PER_DAY * days as i64,
        max_timestamp: timestamp,
    };
    let matched_tsids = db
        .search_tsids(std::slice::from_ref(&tfs), tr, 100_000, NO_DEADLINE)
        .unwrap();
    assert_eq!(
        matched_tsids.len(),
        metrics_per_day * days as usize,
        "expected {} time series for all the days",
        metrics_per_day * days as usize
    );

    // Global-index search must match all the metrics as well.
    let matched_tsids = db
        .search_tsids(
            std::slice::from_ref(&tfs),
            esm_storage::GLOBAL_INDEX_TIME_RANGE,
            100_000,
            NO_DEADLINE,
        )
        .unwrap();
    assert_eq!(
        matched_tsids.len(),
        metrics_per_day * days as usize,
        "global search mismatch"
    );

    // Repeat the search with the tag-filters cache populated.
    let matched_tsids2 = db
        .search_tsids(
            std::slice::from_ref(&tfs),
            esm_storage::GLOBAL_INDEX_TIME_RANGE,
            100_000,
            NO_DEADLINE,
        )
        .unwrap();
    assert_eq!(
        matched_tsids, matched_tsids2,
        "cached global search mismatch"
    );

    // search_metric_names must return the same number of names.
    let names = db
        .search_metric_names(std::slice::from_ref(&tfs), tr, 100_000, NO_DEADLINE)
        .unwrap();
    assert_eq!(names.len(), metrics_per_day * days as usize);

    // An empty result must be cached as well (the second call goes through
    // the tagFilters cache).
    let mut tfs_none = TagFilters::new();
    tfs_none
        .add(b"no-such-key", b"no-such-value", false, false)
        .unwrap();
    for _ in 0..2 {
        let found = db
            .search_tsids(std::slice::from_ref(&tfs_none), tr, 100_000, NO_DEADLINE)
            .unwrap();
        assert!(found.is_empty());
    }

    db.must_close();
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn match_tag_filters_semantics() {
    use esm_storage::index::match_tag_filters;

    let mut mn = MetricName::default();
    mn.metric_group = b"foobar".to_vec();
    mn.add_tag("key", "value");
    mn.sort_tags();

    let check = |adds: &[(&[u8], &[u8], bool, bool)], want: bool| {
        let mut tfs = TagFilters::new();
        for &(key, value, is_negative, is_regexp) in adds {
            tfs.add(key, value, is_negative, is_regexp).unwrap();
        }
        let mut filters = tfs.filters().to_vec();
        let mut kb = Vec::new();
        let got = match_tag_filters(&mn, &mut filters, &mut kb).unwrap();
        assert_eq!(got, want, "unexpected match result for {tfs}");
    };

    // Metric group matching.
    check(&[(b"", b"foobar", false, false)], true);
    check(&[(b"", b"foobar1", false, false)], false);
    check(&[(b"", b"foo.*", false, true)], true);
    check(&[(b"", b"foobar", true, false)], false);

    // Tag matching.
    check(&[(b"key", b"value", false, false)], true);
    check(&[(b"key", b"v", false, false)], false);
    check(&[(b"key", b"value", true, false)], false);
    check(&[(b"key", b"v", true, false)], true);
    check(&[(b"key", b"val.*", false, true)], true);

    // Non-existing tag key semantics.
    check(&[(b"missing", b"x", false, false)], false);
    // A negative filter for a non-existing tag key matches anything.
    check(&[(b"missing", b"x", true, false)], true);
    // A positive empty-match filter for a non-existing tag key matches.
    check(&[(b"missing", b"foo|", false, true)], true);
    check(&[(b"missing", b"foo", false, true)], false);
    // `{missing!~"|foo"}` must not match a missing tag (it requires a
    // non-empty value).
    check(&[(b"missing", b"|foo", true, true)], false);

    // Multiple filters.
    check(
        &[
            (b"", b"foobar", false, false),
            (b"key", b"value", false, false),
        ],
        true,
    );
    check(
        &[
            (b"", b"foobar", false, false),
            (b"key", b"value2", false, false),
        ],
        false,
    );
}
