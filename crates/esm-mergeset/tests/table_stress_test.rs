//! Port of `TestTableAddItemsConcurrentStress`.
//!
//! Kept in its own integration-test binary because it overrides the
//! process-global raw-items shard parameters.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use esm_mergeset::{
    set_raw_items_shard_params_for_tests, FlushCallback, Item, PrepareBlockCallback, Table,
    MAX_INMEMORY_BLOCK_SIZE,
};

use common::{remove_dir, test_dir, test_reopen_table, total_items_count};

#[test]
fn table_add_items_concurrent_stress() {
    let path = test_dir("table-add-items-concurrent-stress");
    remove_dir(&path);

    const RAW_ITEMS_SHARDS_PER_TABLE: usize = 10;
    const MAX_BLOCKS_PER_SHARD: usize = 3;
    set_raw_items_shard_params_for_tests(RAW_ITEMS_SHARDS_PER_TABLE, MAX_BLOCKS_PER_SHARD);

    let flushes = Arc::new(AtomicU64::new(0));
    let flushes_clone = Arc::clone(&flushes);
    let flush_callback: FlushCallback = Arc::new(move || {
        flushes_clone.fetch_add(1, Ordering::Relaxed);
    });
    let prepare_block: PrepareBlockCallback =
        Arc::new(|_data: &mut Vec<u8>, _items: &mut Vec<Item>| {});

    let blocks_needed = RAW_ITEMS_SHARDS_PER_TABLE * MAX_BLOCKS_PER_SHARD * 10;

    let tb = Table::must_open(
        &path,
        Duration::ZERO,
        Some(flush_callback),
        Duration::ZERO,
        Some(prepare_block),
        Arc::new(AtomicBool::new(false)),
    );

    // Each item fills a whole raw block, so the shards overflow and the
    // ibsToFlush path is exercised.
    let items: Vec<Vec<u8>> = (0..blocks_needed)
        .map(|j| vec![j as u8; MAX_INMEMORY_BLOCK_SIZE - 10])
        .collect();
    let items_refs: Vec<&[u8]> = items.iter().map(|item| &item[..]).collect();
    tb.add_items(&items_refs);

    // Verify the items count after the pending items flush.
    tb.debug_flush();
    assert!(
        flushes.load(Ordering::Relaxed) > 0,
        "unexpected zero flushes"
    );
    assert_eq!(total_items_count(&tb), blocks_needed as u64);

    tb.must_close();

    // Re-open the table and make sure the items count remains the same.
    test_reopen_table(&path, blocks_needed as u64);

    remove_dir(&path);
}
