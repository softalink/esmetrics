//! Port of `table_search_test.go`.

mod common;

use std::sync::atomic::Ordering;

use esm_mergeset::{Table, TableSearch};

use common::{open_table, open_table_with_flush_counter, remove_dir, test_dir, Rng};

fn new_test_table(
    r: &mut Rng,
    path: &std::path::PathBuf,
    items_count: usize,
) -> (Table, Vec<Vec<u8>>) {
    let (tb, flushes) = open_table_with_flush_counter(path);
    let mut items: Vec<Vec<u8>> = Vec::with_capacity(items_count);
    for i in 0..items_count {
        let item = format!("{}:{i}", r.intn(1_000_000_000)).into_bytes();
        tb.add_items(&[&item]);
        items.push(item);
    }
    tb.debug_flush();
    if items_count > 0 {
        assert!(
            flushes.load(Ordering::Relaxed) > 0,
            "unexpected zero flushes for itemsCount={items_count}"
        );
    }
    items.sort();
    (tb, items)
}

fn test_table_search_serial(tb: &Table, items: &[Vec<u8>]) {
    let mut ts = TableSearch::new(tb, false);
    let keys: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"123".to_vec(),
        b"9".to_vec(),
        b"892".to_vec(),
        b"2384329".to_vec(),
        b"fdsjflfdf".to_vec(),
        items[0].clone(),
        items[items.len() - 1].clone(),
        items[items.len() / 2].clone(),
    ];
    for key in keys {
        let mut n = items.partition_point(|item| item[..] < key[..]);
        ts.seek(&key);
        while n < items.len() {
            let item = &items[n];
            assert!(
                ts.next_item(),
                "missing item {item:?} at position {n} when searching for {key:?}"
            );
            assert_eq!(
                ts.item(),
                &item[..],
                "unexpected item found at position {n} when searching for {key:?}"
            );
            n += 1;
        }
        assert!(
            !ts.next_item(),
            "superfluous item found at position {n} when searching for {key:?}"
        );
        assert!(
            ts.error().is_none(),
            "unexpected error when searching for {key:?}"
        );
    }
    ts.must_close();
}

#[test]
fn table_search_serial() {
    let path = test_dir("table-search-serial");
    remove_dir(&path);

    const ITEMS_COUNT: usize = 100_000;

    let items = {
        let mut r = Rng::new(1);
        let (tb, items) = new_test_table(&mut r, &path, ITEMS_COUNT);
        test_table_search_serial(&tb, &items);
        tb.must_close();
        items
    };

    // Re-open the table and verify the search works.
    {
        let tb = open_table(&path);
        test_table_search_serial(&tb, &items);
        tb.must_close();
    }

    remove_dir(&path);
}

#[test]
fn table_search_concurrent() {
    let path = test_dir("table-search-concurrent");
    remove_dir(&path);

    const ITEMS_COUNT: usize = 100_000;

    let items = {
        let mut r = Rng::new(2);
        let (tb, items) = new_test_table(&mut r, &path, ITEMS_COUNT);
        std::thread::scope(|s| {
            for _ in 0..5 {
                let tb = &tb;
                let items = &items;
                s.spawn(move || test_table_search_serial(tb, items));
            }
        });
        tb.must_close();
        items
    };

    // Re-open the table and verify the search works.
    {
        let tb = open_table(&path);
        std::thread::scope(|s| {
            for _ in 0..5 {
                let tb = &tb;
                let items = &items;
                s.spawn(move || test_table_search_serial(tb, items));
            }
        });
        tb.must_close();
    }

    remove_dir(&path);
}
