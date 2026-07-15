//! Port of `mergeTagToMetricIDsRows` and `tagToMetricIDsRowParser` from
//! `index_db.go`: merging of metricID postings within a mergeset block.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use esm_encoding::{marshal_uint64, unmarshal_uint64};
use esm_mergeset::{Item, PrepareBlockCallback};

use crate::metric_name::{Tag, TAG_SEPARATOR_CHAR};

use super::{
    marshal_common_prefix, unmarshal_common_prefix, NS_PREFIX_DATE_TAG_TO_METRIC_IDS,
    NS_PREFIX_TAG_TO_METRIC_IDS,
};

/// Limits the number of metricIDs in a tag->metricIDs row.
///
/// This reduces the overhead on index and metaindex in lib/mergeset.
pub const MAX_METRIC_IDS_PER_ROW: usize = 64;

/// The number of index blocks with tag->metricIDs rows processed by the
/// merge callback.
pub static INDEX_BLOCKS_WITH_METRIC_IDS_PROCESSED: AtomicU64 = AtomicU64::new(0);

/// The number of index blocks reverted because the merged rows became
/// unsorted (duplicate metricIDs across the source rows).
pub static INDEX_BLOCKS_WITH_METRIC_IDS_INCORRECT_ORDER: AtomicU64 = AtomicU64::new(0);

/// Parser for `nsPrefixTagToMetricIDs` / `nsPrefixDateTagToMetricIDs` rows.
/// Go: tagToMetricIDsRowParser.
#[derive(Default)]
pub struct TagToMetricIDsRowParser {
    /// The first byte parsed from the row after an `init` call.
    pub ns_prefix: u8,
    /// The parsed date for `nsPrefixDateTagToMetricIDs` rows.
    pub date: u64,
    /// The parsed metricIDs after a `parse_metric_ids` call.
    pub metric_ids: Vec<u64>,
    metric_ids_parsed: bool,
    /// The parsed tag after an `init` call.
    pub tag: Tag,
    /// The remaining unparsed metricIDs (owned copy of the row tail).
    tail: Vec<u8>,
}

impl TagToMetricIDsRowParser {
    /// Resets the parser. Go: tagToMetricIDsRowParser.Reset.
    pub fn reset(&mut self) {
        self.ns_prefix = 0;
        self.date = 0;
        self.metric_ids.clear();
        self.metric_ids_parsed = false;
        self.tag = Tag::default();
        self.tail.clear();
    }

    /// Initializes the parser from `b`, which must contain an encoded
    /// tag->metricIDs row. Go: tagToMetricIDsRowParser.Init.
    pub fn init(&mut self, b: &[u8], ns_prefix_expected: u8) -> Result<(), String> {
        let (mut tail, ns_prefix) = unmarshal_common_prefix(b)
            .map_err(|err| format!("invalid tag->metricIDs row {b:?}: {err}"))?;
        if ns_prefix != ns_prefix_expected {
            return Err(format!(
                "invalid prefix for tag->metricIDs row {b:?}; got {ns_prefix}; want {ns_prefix_expected}"
            ));
        }
        if ns_prefix == NS_PREFIX_DATE_TAG_TO_METRIC_IDS {
            // Unmarshal the date.
            if tail.len() < 8 {
                return Err(format!(
                    "cannot unmarshal date from (date, tag)->metricIDs row {b:?} from {} bytes; want at least 8 bytes",
                    tail.len()
                ));
            }
            self.date = unmarshal_uint64(tail);
            tail = &tail[8..];
        }
        self.ns_prefix = ns_prefix;
        let tail = self
            .tag
            .unmarshal(tail)
            .map_err(|err| format!("cannot unmarshal tag from tag->metricIDs row {b:?}: {err}"))?;
        self.init_only_tail(tail)
    }

    /// Marshals the row prefix (without the metricIDs tail) to `dst`.
    /// Go: tagToMetricIDsRowParser.MarshalPrefix.
    pub fn marshal_prefix(&self, dst: &mut Vec<u8>) {
        marshal_common_prefix(dst, self.ns_prefix);
        if self.ns_prefix == NS_PREFIX_DATE_TAG_TO_METRIC_IDS {
            marshal_uint64(dst, self.date);
        }
        self.tag.marshal(dst);
    }

    /// Initializes only the metricIDs tail.
    /// Go: tagToMetricIDsRowParser.InitOnlyTail.
    pub fn init_only_tail(&mut self, tail: &[u8]) -> Result<(), String> {
        if tail.is_empty() {
            return Err("missing metricID in the tag->metricIDs row".to_string());
        }
        if tail.len() % 8 != 0 {
            return Err(format!(
                "invalid tail length in the tag->metricIDs row; got {} bytes; must be multiple of 8 bytes",
                tail.len()
            ));
        }
        self.tail.clear();
        self.tail.extend_from_slice(tail);
        self.metric_ids_parsed = false;
        Ok(())
    }

    /// Returns true if the row prefixes (ns prefix, date, tag) of `self` and
    /// `x` are equal. Go: tagToMetricIDsRowParser.EqualPrefix.
    pub fn equal_prefix(&self, x: &TagToMetricIDsRowParser) -> bool {
        self.tag == x.tag && self.date == x.date && self.ns_prefix == x.ns_prefix
    }

    /// The number of metricIDs in the row tail.
    /// Go: tagToMetricIDsRowParser.MetricIDsLen.
    pub fn metric_ids_len(&self) -> usize {
        self.tail.len() / 8
    }

    /// Parses the metricIDs from the tail into `self.metric_ids`.
    /// Go: tagToMetricIDsRowParser.ParseMetricIDs.
    pub fn parse_metric_ids(&mut self) {
        if self.metric_ids_parsed {
            return;
        }
        self.metric_ids.clear();
        let mut tail: &[u8] = &self.tail;
        while tail.len() >= 8 {
            self.metric_ids.push(unmarshal_uint64(tail));
            tail = &tail[8..];
        }
        self.metric_ids_parsed = true;
    }

    /// Returns the number of series in the row that match `filter` (all when
    /// `filter` is None) and do not match `negative_filter`.
    /// Go: tagToMetricIDsRowParser.GetMatchingSeriesCount.
    pub fn get_matching_series_count(
        &mut self,
        filter: Option<&esm_common::uint64set::Set>,
        negative_filter: &esm_common::uint64set::Set,
    ) -> usize {
        if filter.is_none() && negative_filter.is_empty() {
            return self.metric_ids_len();
        }
        self.parse_metric_ids();
        let mut n = 0;
        for &metric_id in &self.metric_ids {
            if let Some(f) = filter {
                if !f.has(metric_id) {
                    continue;
                }
            }
            if !negative_filter.has(metric_id) {
                n += 1;
            }
        }
        n
    }
}

/// Returns the `PrepareBlockCallback` for the indexDB mergeset table.
/// Go: mergeTagToMetricIDsRows.
pub fn merge_tag_to_metric_ids_rows_callback() -> PrepareBlockCallback {
    Arc::new(|data: &mut Vec<u8>, items: &mut Vec<Item>| {
        merge_tag_to_metric_ids_rows(data, items);
    })
}

/// Merges tag->metricIDs rows with the same (prefix, tag) within a block.
/// Go: mergeTagToMetricIDsRows.
pub fn merge_tag_to_metric_ids_rows(data: &mut Vec<u8>, items: &mut Vec<Item>) {
    merge_tag_to_metric_ids_rows_internal(data, items, NS_PREFIX_TAG_TO_METRIC_IDS);
    merge_tag_to_metric_ids_rows_internal(data, items, NS_PREFIX_DATE_TAG_TO_METRIC_IDS);
}

fn merge_tag_to_metric_ids_rows_internal(data: &mut Vec<u8>, items: &mut Vec<Item>, ns_prefix: u8) {
    // Perform quick checks whether items contain rows starting from ns_prefix
    // based on the fact that items are sorted.
    if items.len() <= 2 {
        // The first and the last row must remain unchanged.
        return;
    }
    let first_item = items[0].bytes(data);
    if !first_item.is_empty() && first_item[0] > ns_prefix {
        return;
    }
    let last_item = items[items.len() - 1].bytes(data);
    if !last_item.is_empty() && last_item[0] < ns_prefix {
        return;
    }

    // items contain at least one row starting from ns_prefix.
    // Merge rows with a common tag.
    let mut mp = TagToMetricIDsRowParser::default();
    let mut mp_prev = TagToMetricIDsRowParser::default();
    let mut pending_metric_ids: Vec<u64> = Vec::new();
    let mut dst_data: Vec<u8> = Vec::with_capacity(data.len());
    let mut dst_items: Vec<Item> = Vec::with_capacity(items.len());

    let flush_pending = |dst_data: &mut Vec<u8>,
                         dst_items: &mut Vec<Item>,
                         pending: &mut Vec<u64>,
                         mp: &TagToMetricIDsRowParser| {
        if pending.is_empty() {
            // Nothing to flush.
            return;
        }
        pending.sort_unstable();
        remove_duplicate_metric_ids(pending);

        // Marshal the pending metricIDs.
        let start = dst_data.len();
        mp.marshal_prefix(dst_data);
        for &metric_id in pending.iter() {
            marshal_uint64(dst_data, metric_id);
        }
        dst_items.push(Item {
            start: start as u32,
            end: dst_data.len() as u32,
        });
        pending.clear();
    };

    let items_len = items.len();
    for (i, it) in items.iter().enumerate() {
        let item = it.bytes(data);
        if item.is_empty() || item[0] != ns_prefix || i == 0 || i == items_len - 1 {
            // Write rows not starting with ns_prefix as-is.
            // Additionally write the first and the last row as-is in order
            // to preserve the sort order for adjacent blocks.
            flush_pending(
                &mut dst_data,
                &mut dst_items,
                &mut pending_metric_ids,
                &mp_prev,
            );
            let start = dst_data.len();
            dst_data.extend_from_slice(item);
            dst_items.push(Item {
                start: start as u32,
                end: dst_data.len() as u32,
            });
            continue;
        }
        if let Err(err) = mp.init(item, ns_prefix) {
            panic!(
                "FATAL: cannot parse row starting with nsPrefix {ns_prefix} during merge: {err}"
            );
        }
        if mp.metric_ids_len() >= MAX_METRIC_IDS_PER_ROW {
            flush_pending(
                &mut dst_data,
                &mut dst_items,
                &mut pending_metric_ids,
                &mp_prev,
            );
            let start = dst_data.len();
            dst_data.extend_from_slice(item);
            dst_items.push(Item {
                start: start as u32,
                end: dst_data.len() as u32,
            });
            continue;
        }
        if !mp.equal_prefix(&mp_prev) {
            flush_pending(
                &mut dst_data,
                &mut dst_items,
                &mut pending_metric_ids,
                &mp_prev,
            );
        }
        mp.parse_metric_ids();
        pending_metric_ids.extend_from_slice(&mp.metric_ids);
        std::mem::swap(&mut mp, &mut mp_prev);
        if pending_metric_ids.len() >= MAX_METRIC_IDS_PER_ROW {
            flush_pending(
                &mut dst_data,
                &mut dst_items,
                &mut pending_metric_ids,
                &mp_prev,
            );
        }
    }
    assert!(
        pending_metric_ids.is_empty(),
        "BUG: pending_metric_ids must be empty at this point; got {} items",
        pending_metric_ids.len()
    );
    if !check_items_sorted(&dst_data, &dst_items) {
        // Items could become unsorted if the initial items contain duplicate
        // metricIDs:
        //
        //   item1: 1, 1, 5
        //   item2: 1, 4
        //
        // Items could become the following after the merge:
        //
        //   item1: 1, 5
        //   item2: 1, 4
        //
        // i.e. item1 > item2. Leave the original items unmerged, so they can
        // be merged next time. This case should be quite rare — if multiple
        // data points are simultaneously inserted into the same new time
        // series from multiple concurrent threads.
        INDEX_BLOCKS_WITH_METRIC_IDS_INCORRECT_ORDER.fetch_add(1, Ordering::Relaxed);
        assert!(
            check_items_sorted(data, items),
            "BUG: the original items weren't sorted"
        );
        INDEX_BLOCKS_WITH_METRIC_IDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    *data = dst_data;
    *items = dst_items;
    INDEX_BLOCKS_WITH_METRIC_IDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
}

/// Returns true if the items are sorted. Go: checkItemsSorted.
pub fn check_items_sorted(data: &[u8], items: &[Item]) -> bool {
    if items.is_empty() {
        return true;
    }
    let mut prev_item = items[0].bytes(data);
    for it in &items[1..] {
        let curr_item = it.bytes(data);
        if prev_item > curr_item {
            return false;
        }
        prev_item = curr_item;
    }
    true
}

/// Removes duplicates from the sorted metricIDs in place.
/// Go: removeDuplicateMetricIDs.
pub fn remove_duplicate_metric_ids(sorted_metric_ids: &mut Vec<u64>) {
    if sorted_metric_ids.len() < 2 {
        return;
    }
    sorted_metric_ids.dedup();
}

/// The tag row (nsPrefix 1/6) suffix parser helper: finds the tag separator
/// position in `tail` (the bytes right after the row prefix).
pub(crate) fn find_tag_separator(tail: &[u8]) -> Option<usize> {
    tail.iter().position(|&b| b == TAG_SEPARATOR_CHAR)
}
