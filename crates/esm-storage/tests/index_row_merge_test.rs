//! Port of TestMergeTagToMetricIDsRows / TestRemoveDuplicateMetricIDs from
//! `index_db_test.go`.

use esm_encoding::marshal_uint64;
use esm_mergeset::Item;
use esm_storage::index::{
    check_items_sorted, marshal_common_prefix, merge_tag_to_metric_ids_rows,
    remove_duplicate_metric_ids, MAX_METRIC_IDS_PER_ROW, NS_PREFIX_DATE_TAG_TO_METRIC_IDS,
    NS_PREFIX_TAG_TO_METRIC_IDS,
};
use esm_storage::Tag;

fn f(items: &[Vec<u8>], expected_items: &[Vec<u8>]) {
    let mut data: Vec<u8> = Vec::new();
    let mut items_b: Vec<Item> = Vec::new();
    for item in items {
        data.extend_from_slice(item);
        items_b.push(Item {
            start: (data.len() - item.len()) as u32,
            end: data.len() as u32,
        });
    }
    assert!(
        check_items_sorted(&data, &items_b),
        "source items aren't sorted"
    );
    merge_tag_to_metric_ids_rows(&mut data, &mut items_b);
    assert_eq!(
        items_b.len(),
        expected_items.len(),
        "unexpected len(result_items_b)"
    );
    assert!(
        check_items_sorted(&data, &items_b),
        "result items aren't sorted"
    );
    // Each result item must be a prefix of the remaining data (items must be
    // laid out sequentially in the data buffer).
    let mut buf: &[u8] = &data;
    for it in &items_b {
        let item = it.bytes(&data);
        assert!(buf.starts_with(item), "unexpected prefix for result data");
        buf = &buf[item.len()..];
    }
    assert!(buf.is_empty(), "unexpected tail left in result data");
    let result_items: Vec<Vec<u8>> = items_b.iter().map(|it| it.bytes(&data).to_vec()).collect();
    assert_eq!(result_items, expected_items, "unexpected items");
}

fn xy(ns_prefix: u8, key: &str, value: &str, metric_ids: &[u64]) -> Vec<u8> {
    let mut dst = Vec::new();
    marshal_common_prefix(&mut dst, ns_prefix);
    if ns_prefix == NS_PREFIX_DATE_TAG_TO_METRIC_IDS {
        marshal_uint64(&mut dst, 1234567901233);
    }
    let t = Tag {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    };
    t.marshal(&mut dst);
    for &metric_id in metric_ids {
        marshal_uint64(&mut dst, metric_id);
    }
    dst
}

fn x(key: &str, value: &str, metric_ids: &[u64]) -> Vec<u8> {
    xy(NS_PREFIX_TAG_TO_METRIC_IDS, key, value, metric_ids)
}

fn y(key: &str, value: &str, metric_ids: &[u64]) -> Vec<u8> {
    xy(NS_PREFIX_DATE_TAG_TO_METRIC_IDS, key, value, metric_ids)
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[test]
fn merge_tag_to_metric_ids_rows_basic() {
    f(&[], &[]);
    f(&[b("foo")], &[b("foo")]);
    f(
        &[b("a"), b("b"), b("c"), b("def")],
        &[b("a"), b("b"), b("c"), b("def")],
    );
    f(
        &[b("\x00"), b("\x00b"), b("\x00c"), b("\x00def")],
        &[b("\x00"), b("\x00b"), b("\x00c"), b("\x00def")],
    );
}

#[test]
fn merge_tag_to_metric_ids_rows_first_last_preserved() {
    // The first and the last row must remain unchanged.
    f(
        &[
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
        ],
        &[x("", "", &[0]), x("", "", &[0]), x("", "", &[0])],
    );
    f(
        &[
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
            y("", "", &[0]),
            y("", "", &[0]),
            y("", "", &[0]),
        ],
        &[
            x("", "", &[0]),
            x("", "", &[0]),
            y("", "", &[0]),
            y("", "", &[0]),
        ],
    );
    f(
        &[
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
            b("xyz"),
        ],
        &[x("", "", &[0]), x("", "", &[0]), b("xyz")],
    );
    f(
        &[
            b("\x00asdf"),
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
        ],
        &[b("\x00asdf"), x("", "", &[0]), x("", "", &[0])],
    );
    f(
        &[
            b("\x00asdf"),
            y("", "", &[0]),
            y("", "", &[0]),
            y("", "", &[0]),
            y("", "", &[0]),
        ],
        &[b("\x00asdf"), y("", "", &[0]), y("", "", &[0])],
    );
    f(
        &[
            b("\x00asdf"),
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
            x("", "", &[0]),
            b("xyz"),
        ],
        &[b("\x00asdf"), x("", "", &[0]), b("xyz")],
    );
    f(
        &[
            b("\x00asdf"),
            x("", "", &[0]),
            x("", "", &[0]),
            y("", "", &[0]),
            y("", "", &[0]),
            b("xyz"),
        ],
        &[b("\x00asdf"), x("", "", &[0]), y("", "", &[0]), b("xyz")],
    );
}

#[test]
fn merge_tag_to_metric_ids_rows_merging() {
    f(
        &[
            b("\x00asdf"),
            x("", "", &[1]),
            x("", "", &[2]),
            x("", "", &[3]),
            x("", "", &[4]),
            b("xyz"),
        ],
        &[b("\x00asdf"), x("", "", &[1, 2, 3, 4]), b("xyz")],
    );
    f(
        &[
            b("\x00asdf"),
            x("", "", &[1]),
            x("", "", &[2]),
            x("", "", &[3]),
            x("", "", &[4]),
        ],
        &[b("\x00asdf"), x("", "", &[1, 2, 3]), x("", "", &[4])],
    );
    f(
        &[
            b("\x00asdf"),
            x("", "", &[1]),
            x("", "", &[2, 3, 4]),
            x("", "", &[2, 3, 4, 5]),
            x("", "", &[3, 5]),
            b("foo"),
        ],
        &[b("\x00asdf"), x("", "", &[1, 2, 3, 4, 5]), b("foo")],
    );
    f(
        &[
            b("\x00asdf"),
            x("", "", &[1]),
            x("", "a", &[2, 3, 4]),
            x("", "a", &[2, 3, 4, 5]),
            x("", "b", &[3, 5]),
            b("foo"),
        ],
        &[
            b("\x00asdf"),
            x("", "", &[1]),
            x("", "a", &[2, 3, 4, 5]),
            x("", "b", &[3, 5]),
            b("foo"),
        ],
    );
    f(
        &[
            b("\x00asdf"),
            x("", "", &[1]),
            x("x", "a", &[2, 3, 4]),
            x("y", "", &[2, 3, 4, 5]),
            x("y", "x", &[3, 5]),
            b("foo"),
        ],
        &[
            b("\x00asdf"),
            x("", "", &[1]),
            x("x", "a", &[2, 3, 4]),
            x("y", "", &[2, 3, 4, 5]),
            x("y", "x", &[3, 5]),
            b("foo"),
        ],
    );
    f(
        &[
            b("\x00asdf"),
            x("sdf", "aa", &[1, 1, 3]),
            x("sdf", "aa", &[1, 2]),
            b("foo"),
        ],
        &[b("\x00asdf"), x("sdf", "aa", &[1, 2, 3]), b("foo")],
    );
    f(
        &[
            b("\x00asdf"),
            x("sdf", "aa", &[1, 2, 2, 4]),
            x("sdf", "aa", &[1, 2, 3]),
            b("foo"),
        ],
        &[b("\x00asdf"), x("sdf", "aa", &[1, 2, 3, 4]), b("foo")],
    );
}

#[test]
fn merge_tag_to_metric_ids_rows_big_chunks() {
    // Construct big source chunks.
    let metric_ids: Vec<u64> = (0..MAX_METRIC_IDS_PER_ROW as u64 - 1).collect();
    f(
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            y("foo", "bar", &metric_ids),
            y("foo", "bar", &metric_ids),
            b("x"),
        ],
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            y("foo", "bar", &metric_ids),
            b("x"),
        ],
    );

    let metric_ids: Vec<u64> = (0..MAX_METRIC_IDS_PER_ROW as u64).collect();
    f(
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            b("x"),
        ],
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            b("x"),
        ],
    );

    let metric_ids: Vec<u64> = (0..3 * MAX_METRIC_IDS_PER_ROW as u64).collect();
    f(
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            b("x"),
        ],
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            b("x"),
        ],
    );
    f(
        &[
            b("\x00aa"),
            x("foo", "bar", &[0, 0, 1, 2, 3]),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            b("x"),
        ],
        &[
            b("\x00aa"),
            x("foo", "bar", &[0, 1, 2, 3]),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            b("x"),
        ],
    );
}

#[test]
fn merge_tag_to_metric_ids_rows_duplicates_and_fallback() {
    // Check the duplicate metricIDs removal.
    let metric_ids: Vec<u64> = vec![123; MAX_METRIC_IDS_PER_ROW - 1];
    f(
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &metric_ids),
            y("foo", "bar", &metric_ids),
            b("x"),
        ],
        &[
            b("\x00aa"),
            x("foo", "bar", &[123]),
            y("foo", "bar", &[123]),
            b("x"),
        ],
    );

    // Check the fallback to the original items after a merge which would
    // result in incorrect ordering.
    let metric_ids: Vec<u64> = vec![123; MAX_METRIC_IDS_PER_ROW - 3];
    f(
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &[123, 123, 125]),
            x("foo", "bar", &[123, 124]),
            b("x"),
        ],
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &[123, 123, 125]),
            x("foo", "bar", &[123, 124]),
            b("x"),
        ],
    );
    f(
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &[123, 123, 125]),
            x("foo", "bar", &[123, 124]),
            y("foo", "bar", &[123, 124]),
        ],
        &[
            b("\x00aa"),
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &[123, 123, 125]),
            x("foo", "bar", &[123, 124]),
            y("foo", "bar", &[123, 124]),
        ],
    );
    f(
        &[
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &[123, 123, 125]),
            x("foo", "bar", &[123, 124]),
        ],
        &[
            x("foo", "bar", &metric_ids),
            x("foo", "bar", &[123, 123, 125]),
            x("foo", "bar", &[123, 124]),
        ],
    );
}

#[test]
fn remove_duplicate_metric_ids_cases() {
    fn f(metric_ids: &[u64], expected: &[u64]) {
        let mut a = metric_ids.to_vec();
        remove_duplicate_metric_ids(&mut a);
        assert_eq!(
            a, expected,
            "unexpected result from remove_duplicate_metric_ids"
        );
    }
    f(&[], &[]);
    f(&[123], &[123]);
    f(&[123, 123], &[123]);
    f(&[123, 123, 123], &[123]);
    f(&[123, 1234, 1235], &[123, 1234, 1235]);
    f(&[0, 1, 1, 2], &[0, 1, 2]);
    f(&[0, 0, 0, 1, 1, 2], &[0, 1, 2]);
    f(&[0, 1, 1, 2, 2], &[0, 1, 2]);
    f(&[0, 1, 2, 2], &[0, 1, 2]);
}
