//! Port of `table_test.go`.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use esm_mergeset::{Table, TableSearch, MAX_INMEMORY_BLOCK_SIZE};

use common::{
    open_table, open_table_with_flush_counter, remove_dir, test_dir, test_reopen_table,
    total_items_count, Rng,
};

#[test]
fn table_open_close() {
    let path = test_dir("table-open-close");
    remove_dir(&path);

    // Create a new table.
    let tb = open_table(&path);
    // Close it.
    tb.must_close();

    // Re-open the created table multiple times.
    for _ in 0..4 {
        let tb = open_table(&path);
        tb.must_close();
    }

    remove_dir(&path);
}

#[test]
fn table_add_items_too_long_item() {
    let path = test_dir("table-too-long-item");
    remove_dir(&path);

    let tb = open_table(&path);
    let item = vec![0u8; MAX_INMEMORY_BLOCK_SIZE + 1];
    tb.add_items(&[&item]);
    tb.debug_flush();
    assert_eq!(total_items_count(&tb), 0);
    tb.must_close();

    remove_dir(&path);
}

fn add_items_serial(r: &mut Rng, tb: &Table, items_count: usize) {
    for _ in 0..items_count {
        let mut item = r.random_bytes();
        item.truncate(MAX_INMEMORY_BLOCK_SIZE);
        tb.add_items(&[&item]);
    }
}

#[test]
fn table_add_items_serial() {
    let mut r = Rng::new(1);
    let path = test_dir("table-add-items-serial");
    remove_dir(&path);

    let (tb, flushes) = open_table_with_flush_counter(&path);

    const ITEMS_COUNT: usize = 10_000;
    add_items_serial(&mut r, &tb, ITEMS_COUNT);

    // Verify the items count after the pending items flush.
    tb.debug_flush();
    assert!(
        flushes.load(Ordering::Relaxed) > 0,
        "unexpected zero flushes"
    );
    assert_eq!(total_items_count(&tb), ITEMS_COUNT as u64);

    tb.must_close();

    // Re-open the table and make sure itemsCount remains the same.
    test_reopen_table(&path, ITEMS_COUNT as u64);

    // Add more items in order to verify merge between inmemory parts and
    // file-based parts.
    let tb = open_table(&path);
    const MORE_ITEMS_COUNT: usize = ITEMS_COUNT * 3;
    add_items_serial(&mut r, &tb, MORE_ITEMS_COUNT);
    tb.must_close();

    // Re-open the table and verify itemsCount again.
    test_reopen_table(&path, (ITEMS_COUNT + MORE_ITEMS_COUNT) as u64);

    remove_dir(&path);
}

fn add_items_concurrent(tb: &Table, items_count: usize) {
    const THREADS: usize = 6;
    std::thread::scope(|s| {
        for n in 0..THREADS {
            s.spawn(move || {
                let mut r = Rng::new(n as u64);
                let per_thread = items_count / THREADS + usize::from(n < items_count % THREADS);
                for _ in 0..per_thread {
                    let mut item = r.random_bytes();
                    item.truncate(MAX_INMEMORY_BLOCK_SIZE);
                    tb.add_items(&[&item]);
                }
            });
        }
    });
}

#[test]
fn table_add_items_concurrent() {
    let path = test_dir("table-add-items-concurrent");
    remove_dir(&path);

    let (tb, flushes) = open_table_with_flush_counter(&path);

    const ITEMS_COUNT: usize = 10_000;
    add_items_concurrent(&tb, ITEMS_COUNT);

    // Verify the items count after the pending items flush.
    tb.debug_flush();
    assert!(
        flushes.load(Ordering::Relaxed) > 0,
        "unexpected zero flushes"
    );
    assert_eq!(total_items_count(&tb), ITEMS_COUNT as u64);

    tb.must_close();

    // Re-open the table and make sure itemsCount remains the same.
    test_reopen_table(&path, ITEMS_COUNT as u64);

    // Add more items in order to verify merge between inmemory parts and
    // file-based parts.
    let tb = open_table(&path);
    const MORE_ITEMS_COUNT: usize = ITEMS_COUNT * 3;
    add_items_concurrent(&tb, MORE_ITEMS_COUNT);
    tb.must_close();

    // Re-open the table and verify itemsCount again.
    test_reopen_table(&path, (ITEMS_COUNT + MORE_ITEMS_COUNT) as u64);

    remove_dir(&path);
}

#[test]
fn table_create_snapshot_at() {
    let path = test_dir("table-create-snapshot-at");
    let snapshot1 = test_dir("table-create-snapshot-at-snapshot1");
    let snapshot2 = test_dir("table-create-snapshot-at-snapshot2");
    remove_dir(&path);
    remove_dir(&snapshot1);
    remove_dir(&snapshot2);

    let tb = open_table(&path);

    // Write many items into the table, so background merges start.
    // (Reduced from Go's 3e5 since the deferred block cache makes uncached
    // debug-build searches slower.)
    const ITEMS_COUNT: usize = 30_000;
    for i in 0..ITEMS_COUNT {
        let item = format!("item {i}");
        tb.add_items(&[item.as_bytes()]);
    }

    // Close and re-open the table in order to flush all the data to disk
    // before creating the snapshots.
    tb.must_close();
    let tb = open_table(&path);

    // Create multiple snapshots.
    tb.must_create_snapshot_at(&snapshot1);
    tb.must_create_snapshot_at(&snapshot2);

    // Verify the snapshots contain all the data.
    let tb1 = open_table(&snapshot1);
    let tb2 = open_table(&snapshot2);

    let mut ts = TableSearch::new(&tb, false);
    let mut ts1 = TableSearch::new(&tb1, false);
    let mut ts2 = TableSearch::new(&tb2, false);
    for i in 0..ITEMS_COUNT {
        let key = format!("item {i}");
        let key = key.as_bytes();
        ts.first_item_with_prefix(key)
            .unwrap_or_else(|e| panic!("cannot find item {i} in the original table: {e}"));
        assert_eq!(ts.item(), key, "unexpected item in the original table");
        ts1.first_item_with_prefix(key)
            .unwrap_or_else(|e| panic!("cannot find item {i} in snapshot1: {e}"));
        assert_eq!(ts1.item(), key, "unexpected item in snapshot1");
        ts2.first_item_with_prefix(key)
            .unwrap_or_else(|e| panic!("cannot find item {i} in snapshot2: {e}"));
        assert_eq!(ts2.item(), key, "unexpected item in snapshot2");
    }
    ts.must_close();
    ts1.must_close();
    ts2.must_close();

    // Close and remove the tables.
    tb2.must_close();
    tb1.must_close();
    tb.must_close();

    remove_dir(&snapshot2);
    remove_dir(&snapshot1);
    remove_dir(&path);
}

#[test]
fn table_add_items_while_searching() {
    // Adds items concurrently with searchers and verifies that neither side
    // crashes and all the flushed items stay visible.
    let path = test_dir("table-add-while-searching");
    remove_dir(&path);

    let tb = Arc::new(open_table(&path));

    // Seed some searchable data.
    for i in 0..1000 {
        let item = format!("seed {i:06}");
        tb.add_items(&[item.as_bytes()]);
    }
    tb.debug_flush();

    std::thread::scope(|s| {
        // Writers.
        for w in 0..2 {
            let tb = Arc::clone(&tb);
            s.spawn(move || {
                for i in 0..5000 {
                    let item = format!("writer{w} {i:06}");
                    tb.add_items(&[item.as_bytes()]);
                }
                tb.debug_flush();
            });
        }
        // Searchers.
        for _ in 0..3 {
            let tb = Arc::clone(&tb);
            s.spawn(move || {
                for _ in 0..30 {
                    let mut ts = TableSearch::new(&tb, false);
                    let mut r = Rng::new(42);
                    for _ in 0..50 {
                        let key = format!("seed {:06}", r.intn(1000));
                        ts.first_item_with_prefix(key.as_bytes())
                            .unwrap_or_else(|e| panic!("cannot find {key}: {e}"));
                        assert_eq!(ts.item(), key.as_bytes());
                    }
                    ts.must_close();
                }
            });
        }
    });

    tb.debug_flush();
    assert_eq!(total_items_count(&tb), 1000 + 2 * 5000);

    match Arc::try_unwrap(tb) {
        Ok(tb) => tb.must_close(),
        Err(_) => panic!("table is still referenced"),
    }
    remove_dir(&path);
}
