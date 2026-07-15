//! Integration test for the block caches (`lib/blockcache` port):
//! repeated searches over file parts must hit the process-global caches,
//! and cached entries must be dropped when the owning parts are dropped.
//!
//! Note: the block caches are process-global, so this binary contains a
//! single test in order to keep the cache counters deterministic.

mod common;

use common::{open_table, remove_dir, test_dir, Rng};
use esm_mergeset::{Table, TableMetrics, TableSearch};

fn metrics(tb: &Table) -> TableMetrics {
    let mut m = TableMetrics::default();
    tb.update_metrics(&mut m);
    m
}

/// Scans the whole table and asserts the result matches `items`.
fn search_all(tb: &Table, items: &[Vec<u8>]) {
    let mut ts = TableSearch::new(tb, false);
    ts.seek(b"");
    let mut found = 0usize;
    while ts.next_item() {
        assert_eq!(
            ts.item(),
            &items[found][..],
            "unexpected item at position {found}"
        );
        found += 1;
    }
    assert!(ts.error().is_none(), "unexpected error: {:?}", ts.error());
    assert_eq!(found, items.len(), "missing items");
    ts.must_close();
}

#[test]
fn table_search_uses_block_caches() {
    let path = test_dir("blockcache-integration");
    remove_dir(&path);

    // Build a table with enough items to span many blocks, then reopen it so
    // that all the parts are file-backed.
    let mut r = Rng::new(42);
    let tb = open_table(&path);
    let mut items: Vec<Vec<u8>> = (0..50_000u64)
        .map(|i| format!("{:09}:{i}", r.intn(1_000_000_000)).into_bytes())
        .collect();
    for item in &items {
        tb.add_items(&[item]);
    }
    tb.must_close(); // flushes everything to file parts
    items.sort();

    let tb = open_table(&path);
    let m0 = metrics(&tb);

    // Blocks enter the cache on the 3rd miss for their key
    // (missesBeforeCaching = 2), so passes 1-3 miss and pass 4 must be
    // served entirely from the caches.
    let mut prev = m0.clone();
    for pass in 1..=4u32 {
        search_all(&tb, &items);
        let m = metrics(&tb);
        assert!(
            m.data_blocks_cache_requests > prev.data_blocks_cache_requests,
            "pass {pass}: data-blocks cache requests did not increase"
        );
        assert!(
            m.index_blocks_cache_requests > prev.index_blocks_cache_requests,
            "pass {pass}: index-blocks cache requests did not increase"
        );
        if pass <= 3 {
            assert!(
                m.data_blocks_cache_misses > prev.data_blocks_cache_misses,
                "pass {pass}: expected data-blocks cache misses"
            );
        }
        if pass >= 3 {
            assert!(
                m.data_blocks_cache_size > 0,
                "pass {pass}: no data blocks were cached"
            );
            assert!(
                m.data_blocks_cache_size_bytes > 0,
                "pass {pass}: cached data blocks report zero size"
            );
            assert!(
                m.index_blocks_cache_size > 0,
                "pass {pass}: no index blocks were cached"
            );
        }
        if pass == 4 {
            assert_eq!(
                m.data_blocks_cache_misses, prev.data_blocks_cache_misses,
                "pass 4 must be served from the data-blocks cache"
            );
            assert_eq!(
                m.index_blocks_cache_misses, prev.index_blocks_cache_misses,
                "pass 4 must be served from the index-blocks cache"
            );
        }
        // The non-sparse search path must not touch the sparse cache.
        assert_eq!(m.data_blocks_sparse_cache_size, 0);
        prev = m;
    }

    // Closing the table drops its parts, which must remove all their cached
    // blocks from the process-global caches (no leaked entries).
    tb.must_close();
    let probe_path = test_dir("blockcache-integration-probe");
    remove_dir(&probe_path);
    let probe = open_table(&probe_path);
    let m = metrics(&probe);
    assert_eq!(
        m.data_blocks_cache_size, 0,
        "data-blocks cache leaks entries after part removal"
    );
    assert_eq!(m.data_blocks_cache_size_bytes, 0);
    assert_eq!(
        m.index_blocks_cache_size, 0,
        "index-blocks cache leaks entries after part removal"
    );
    assert_eq!(m.index_blocks_cache_size_bytes, 0);
    probe.must_close();

    remove_dir(&path);
    remove_dir(&probe_path);
}
