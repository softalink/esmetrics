//! `InmemoryBlock` — the in-memory staging buffer for items before they are
//! marshalled into a storage block, plus the marshal/unmarshal codec.
//!
//! Format reference: `docs/format/mergeset-part.md` §5.
//! VM source: `lib/mergeset/encoding.go:19-179, 223-503`.

use std::ops::Range;

use esm_compress::int::{
    marshal_uint64, marshal_varuint64s, unmarshal_uint64, unmarshal_varuint64s_into,
};
use esm_compress::zstd_codec::{ZstdError, compress_zstd_level, decompress_zstd};
use thiserror::Error;

use super::{MarshalType, StorageBlock};

/// In-memory staging block. Holds up to [`super::MAX_INMEMORY_BLOCK_SIZE`]
/// bytes of items plus a shared `common_prefix` extracted at marshal time.
///
/// Items are stored as a flat `data: Vec<u8>` plus an `items: Vec<Range<u32>>`
/// of `(start, end)` offsets. Identical to VM's representation
/// (`Item.Start`/`Item.End` u32 pair) so the merge path can run without
/// allocation surprises.
#[derive(Debug, Default, Clone)]
pub struct InmemoryBlock {
    /// Common byte prefix shared by every item. Populated at marshal time.
    pub common_prefix: Vec<u8>,
    /// Backing storage for item bytes, concatenated.
    pub data: Vec<u8>,
    /// Offsets `(start, end)` into `data` for each item, in insertion order.
    pub items: Vec<Range<u32>>,
}

impl InmemoryBlock {
    /// Number of items currently in the block.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// `true` if no items are held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Append `item` to the block. Returns `false` if appending would exceed
    /// [`super::MAX_INMEMORY_BLOCK_SIZE`] — the caller must then flush this
    /// block and start a new one. Matches VM's `Add`
    /// (`lib/mergeset/encoding.go:161-179`).
    pub fn add(&mut self, item: &[u8]) -> bool {
        if self.data.len() + item.len() > super::MAX_INMEMORY_BLOCK_SIZE {
            return false;
        }
        // SAFETY of casts: `data.len()` is bounded by `MAX_INMEMORY_BLOCK_SIZE`
        // (64 KiB), well within u32. The lint is conservative for 64-bit
        // platforms but cannot use the runtime bound check above.
        #[allow(clippy::cast_possible_truncation)]
        let start = self.data.len() as u32;
        self.data.extend_from_slice(item);
        #[allow(clippy::cast_possible_truncation)]
        let end = self.data.len() as u32;
        self.items.push(start..end);
        true
    }

    /// Reset to an empty state, retaining allocated capacity for reuse.
    pub fn reset(&mut self) {
        self.common_prefix.clear();
        self.data.clear();
        self.items.clear();
    }

    /// Borrow the bytes of the `idx`-th item.
    #[must_use]
    pub fn item_bytes(&self, idx: usize) -> &[u8] {
        let r = &self.items[idx];
        &self.data[r.start as usize..r.end as usize]
    }

    /// `true` if items are in lex-sorted order. Compares the full item bytes
    /// (i.e. they include `common_prefix` because items are stored with the
    /// prefix intact prior to encoding).
    #[must_use]
    pub fn is_sorted(&self) -> bool {
        self.items
            .windows(2)
            .all(|w| range_slice(&self.data, &w[0]) <= range_slice(&self.data, &w[1]))
    }

    /// Sort items in-place by their full byte values, lex order.
    pub fn sort_items(&mut self) {
        let data_ptr: *const u8 = self.data.as_ptr();
        let data_len = self.data.len();
        // We cannot pass `self.data` into `sort_by` because the closure
        // borrows `items`. Use raw pointer access to look up bytes for the
        // comparison; the borrow checker has no way to express "data and
        // items have disjoint lifetimes during this sort" without raw
        // pointers. The pointer outlives the sort because `data` is owned
        // by `self`.
        self.items.sort_by(|a, b| {
            // SAFETY: `data_ptr` points at the start of `self.data`, which
            // is owned by `self` and not modified during `sort_by`. Each
            // range's [start, end) is guaranteed by `add` and `unmarshal_data`
            // to be within `0..data_len`.
            let lhs = unsafe {
                std::slice::from_raw_parts(
                    data_ptr.add(a.start as usize),
                    (a.end - a.start) as usize,
                )
            };
            let rhs = unsafe {
                std::slice::from_raw_parts(
                    data_ptr.add(b.start as usize),
                    (b.end - b.start) as usize,
                )
            };
            // Touch the length so clippy doesn't flag it as unused. (Used
            // for the debug-assert below.)
            debug_assert!(a.end as usize <= data_len && b.end as usize <= data_len);
            lhs.cmp(rhs)
        });
    }

    /// Recompute [`Self::common_prefix`] assuming items are already sorted.
    /// The longest common prefix of a sorted sequence equals the common
    /// prefix of its first and last elements (`lib/mergeset/encoding.go:106-120`).
    pub fn update_common_prefix_sorted(&mut self) {
        if self.items.len() <= 1 {
            // Per VM: a single-item or empty block has no useful common prefix.
            self.common_prefix.clear();
            return;
        }
        let first = range_slice(&self.data, &self.items[0]);
        // SAFETY of indexing: `items.len() > 1` checked above.
        let last_range = &self.items[self.items.len() - 1];
        let last = range_slice(&self.data, last_range);
        let cp_len = common_prefix_len(first, last);
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(&first[..cp_len]);
    }

    /// Marshal sorted items into `sb`. Assumes `items` is sorted; debug
    /// builds verify this.
    ///
    /// Appends:
    /// - the full bytes of the first item to `first_item_dst`,
    /// - the discovered common prefix to `common_prefix_dst`,
    /// - the items chunk to `sb.items_data` (cleared first),
    /// - the lens chunk to `sb.lens_data` (cleared first).
    ///
    /// Returns `(items_count, marshal_type)`.
    ///
    /// Mirrors VM's `MarshalSortedData` -> `marshalData`
    /// (`lib/mergeset/encoding.go:232-337`).
    ///
    /// # Errors
    /// Returns [`MarshalError`] if zstd compression fails or the block is empty.
    pub fn marshal_sorted_data(
        &mut self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> Result<(u32, MarshalType), MarshalError> {
        debug_assert!(self.is_sorted(), "marshal_sorted_data requires sorted items");
        if self.items.is_empty() {
            return Err(MarshalError::Empty);
        }
        self.update_common_prefix_sorted();
        self.marshal_data(sb, first_item_dst, common_prefix_dst, compress_level)
    }

    /// Marshal (possibly unsorted) items into `sb`. Sorts in-place first.
    /// VM's `MarshalUnsortedData` (`lib/mergeset/encoding.go:223-226`).
    ///
    /// # Errors
    /// Returns [`MarshalError`] if zstd compression fails or the block is empty.
    pub fn marshal_unsorted_data(
        &mut self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> Result<(u32, MarshalType), MarshalError> {
        if self.items.is_empty() {
            return Err(MarshalError::Empty);
        }
        self.sort_items();
        self.update_common_prefix_sorted();
        self.marshal_data(sb, first_item_dst, common_prefix_dst, compress_level)
    }

    fn marshal_data(
        &self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> Result<(u32, MarshalType), MarshalError> {
        let items_count = u32::try_from(self.items.len())
            .map_err(|_| MarshalError::TooManyItems(self.items.len()))?;
        let first_item = range_slice(&self.data, &self.items[0]);
        first_item_dst.extend_from_slice(first_item);
        common_prefix_dst.extend_from_slice(&self.common_prefix);

        let cp_len = self.common_prefix.len();
        let raw_data_minus_cp =
            self.data.len().saturating_sub(cp_len.saturating_mul(self.items.len()));
        if raw_data_minus_cp < 64 || self.items.len() < 2 {
            // Plain encoding is cheaper for tiny blocks; matches VM 278-282.
            self.marshal_data_plain(sb);
            return Ok((items_count, MarshalType::Plain));
        }

        // ----- zstd path -----
        // Layout produced here (consumed by `unmarshal_data`):
        //   items chunk = concat(item[i][cp_len + prefix_lens[i] ..])
        //   lens chunk  = varuints(prefix_lens_delta) || varuints(lens_delta)
        //                 then zstd-compressed.

        let n = self.items.len();
        let mut prefix_lens_delta = Vec::with_capacity(n - 1);
        let mut item_lens_delta = Vec::with_capacity(n - 1);
        let mut raw_items_chunk = Vec::new();

        let mut prev_item_stripped: &[u8] = &first_item[cp_len..];
        let mut prev_prefix_len: u64 = 0;
        let mut prev_item_len: u64 = prev_item_stripped.len() as u64;
        for it in &self.items[1..] {
            let item_full = range_slice(&self.data, it);
            let item_stripped = &item_full[cp_len..];
            let prefix_len_usize = common_prefix_len(prev_item_stripped, item_stripped);
            let prefix_len = prefix_len_usize as u64;
            raw_items_chunk.extend_from_slice(&item_stripped[prefix_len_usize..]);

            prefix_lens_delta.push(prefix_len ^ prev_prefix_len);
            prev_prefix_len = prefix_len;

            let item_len = item_stripped.len() as u64;
            item_lens_delta.push(item_len ^ prev_item_len);
            prev_item_len = item_len;

            prev_item_stripped = item_stripped;
        }

        // Compress items chunk.
        sb.items_data.clear();
        compress_zstd_level(&mut sb.items_data, &raw_items_chunk, compress_level)?;

        // Fall back to plain if compression isn't worth the bytes saved.
        // VM threshold: compressed >= 0.9 * raw_data_minus_cp. The f64
        // mantissa is sufficient for the heuristic since both sides are
        // bounded by MAX_INMEMORY_BLOCK_SIZE (64 KiB).
        let raw_for_ratio = raw_data_minus_cp;
        #[allow(clippy::cast_precision_loss)]
        let too_large = (sb.items_data.len() as f64) > 0.9 * (raw_for_ratio as f64);
        if too_large {
            self.marshal_data_plain(sb);
            return Ok((items_count, MarshalType::Plain));
        }

        // Build + compress lens chunk: varuints(prefix_lens_delta) || varuints(item_lens_delta).
        let mut raw_lens_chunk = Vec::new();
        marshal_varuint64s(&mut raw_lens_chunk, &prefix_lens_delta);
        marshal_varuint64s(&mut raw_lens_chunk, &item_lens_delta);
        sb.lens_data.clear();
        compress_zstd_level(&mut sb.lens_data, &raw_lens_chunk, compress_level)?;

        Ok((items_count, MarshalType::Zstd))
    }

    fn marshal_data_plain(&self, sb: &mut StorageBlock) {
        let cp_len = self.common_prefix.len();
        sb.items_data.clear();
        for it in &self.items[1..] {
            let item_full = range_slice(&self.data, it);
            sb.items_data.extend_from_slice(&item_full[cp_len..]);
        }
        sb.lens_data.clear();
        for it in &self.items[1..] {
            let stripped_len = u64::from(it.end - it.start) - cp_len as u64;
            marshal_uint64(&mut sb.lens_data, stripped_len);
        }
    }

    /// Reverse of [`Self::marshal_sorted_data`]. Populates `self` with the
    /// decoded items, sorted. The block is cleared first.
    ///
    /// Mirrors VM's `UnmarshalData` (`lib/mergeset/encoding.go:353-479`).
    ///
    /// # Errors
    /// See [`UnmarshalDataError`].
    pub fn unmarshal_data(
        &mut self,
        sb: &StorageBlock,
        first_item: &[u8],
        common_prefix: &[u8],
        items_count: u32,
        mt: MarshalType,
    ) -> Result<(), UnmarshalDataError> {
        self.reset();
        if items_count == 0 {
            return Err(UnmarshalDataError::ZeroItemsCount);
        }
        self.common_prefix.extend_from_slice(common_prefix);

        match mt {
            MarshalType::Plain => self.unmarshal_data_plain(sb, first_item, items_count)?,
            MarshalType::Zstd => self.unmarshal_data_zstd(sb, first_item, items_count)?,
        }

        if !self.is_sorted() {
            return Err(UnmarshalDataError::DecodedUnsorted);
        }
        Ok(())
    }

    fn unmarshal_data_plain(
        &mut self,
        sb: &StorageBlock,
        first_item: &[u8],
        items_count: u32,
    ) -> Result<(), UnmarshalDataError> {
        let cp = self.common_prefix.clone();
        // Reserve capacity to match the upper-bound layout.
        let cp_len = cp.len();
        let raw_total = first_item.len()
            + sb.items_data.len()
            + cp_len.saturating_mul((items_count as usize).saturating_sub(1));
        self.data.reserve(raw_total);

        // Item 0 = first_item as-is.
        self.push_item(first_item);

        // Items 1..N: read BE u64 lens from sb.lens_data, then read that
        // many bytes from sb.items_data, prepending the common prefix.
        let mut lens_cursor = &sb.lens_data[..];
        let mut items_cursor = &sb.items_data[..];

        let mut tmp = Vec::new();
        for _ in 1..items_count {
            let (stripped_len, n) = unmarshal_uint64(lens_cursor)
                .map_err(|e| UnmarshalDataError::PlainLens { source: e })?;
            lens_cursor = &lens_cursor[n..];
            let stripped_len = usize::try_from(stripped_len)
                .map_err(|_| UnmarshalDataError::ItemLengthOverflow(stripped_len))?;
            if items_cursor.len() < stripped_len {
                return Err(UnmarshalDataError::ItemsTruncated {
                    need: stripped_len,
                    have: items_cursor.len(),
                });
            }
            let (item_stripped, rest) = items_cursor.split_at(stripped_len);
            items_cursor = rest;

            tmp.clear();
            tmp.extend_from_slice(&cp);
            tmp.extend_from_slice(item_stripped);
            self.push_item(&tmp);
        }
        if !lens_cursor.is_empty() {
            return Err(UnmarshalDataError::TrailingLensBytes(lens_cursor.len()));
        }
        if !items_cursor.is_empty() {
            return Err(UnmarshalDataError::TrailingItemBytes(items_cursor.len()));
        }
        Ok(())
    }

    fn unmarshal_data_zstd(
        &mut self,
        sb: &StorageBlock,
        first_item: &[u8],
        items_count: u32,
    ) -> Result<(), UnmarshalDataError> {
        // Decompress lens chunk = varuints(prefix_lens_delta) || varuints(lens_delta).
        let n = items_count as usize;
        let mut lens_raw = Vec::new();
        decompress_zstd(&mut lens_raw, &sb.lens_data)?;

        let mut prefix_lens_delta = vec![0u64; n - 1];
        let mut item_lens_delta = vec![0u64; n - 1];

        let tail = unmarshal_varuint64s_into(&mut prefix_lens_delta, &lens_raw)
            .map_err(|e| UnmarshalDataError::ZstdPrefixLens { source: e })?;
        let tail = unmarshal_varuint64s_into(&mut item_lens_delta, tail)
            .map_err(|e| UnmarshalDataError::ZstdItemLens { source: e })?;
        if !tail.is_empty() {
            return Err(UnmarshalDataError::TrailingLensBytes(tail.len()));
        }

        // Reconstruct cumulative arrays.
        let mut prefix_lens = vec![0u64; n];
        for i in 0..n - 1 {
            prefix_lens[i + 1] = prefix_lens_delta[i] ^ prefix_lens[i];
        }
        let mut item_lens = vec![0u64; n];
        item_lens[0] = (first_item.len() as u64) - (self.common_prefix.len() as u64);
        for i in 0..n - 1 {
            item_lens[i + 1] = item_lens_delta[i] ^ item_lens[i];
        }

        // Decompress items chunk = concat(item[i][cp_len + prefix_lens[i]..]).
        let mut items_chunk = Vec::new();
        decompress_zstd(&mut items_chunk, &sb.items_data)?;

        let cp = self.common_prefix.clone();
        let cp_len = cp.len();
        self.data.reserve(first_item.len() + cp_len * (n - 1) + items_chunk.len());

        // Item 0
        self.push_item(first_item);
        let mut prev_item_stripped_start = cp_len;
        let mut items_cursor: &[u8] = &items_chunk;
        let mut tmp = Vec::new();

        for i in 1..n {
            let item_len = usize::try_from(item_lens[i])
                .map_err(|_| UnmarshalDataError::ItemLengthOverflow(item_lens[i]))?;
            let prefix_len = usize::try_from(prefix_lens[i])
                .map_err(|_| UnmarshalDataError::ItemLengthOverflow(prefix_lens[i]))?;
            if prefix_len > item_len {
                return Err(UnmarshalDataError::PrefixExceedsItem { prefix_len, item_len });
            }
            let suffix_len = item_len - prefix_len;
            if items_cursor.len() < suffix_len {
                return Err(UnmarshalDataError::ItemsTruncated {
                    need: suffix_len,
                    have: items_cursor.len(),
                });
            }

            // Prev item, stripped, lives in self.data after the last push.
            // SAFETY of indexing: we pushed at least `first_item` (item 0)
            // before entering this loop, so `items` is non-empty.
            let last_idx = self.items.len() - 1;
            let prev_item_full = range_slice(&self.data, &self.items[last_idx]);
            let prev_stripped = &prev_item_full[prev_item_stripped_start..];
            if prefix_len > prev_stripped.len() {
                return Err(UnmarshalDataError::PrefixExceedsPrev {
                    prefix_len,
                    prev_len: prev_stripped.len(),
                });
            }
            tmp.clear();
            tmp.extend_from_slice(&cp);
            tmp.extend_from_slice(&prev_stripped[..prefix_len]);
            tmp.extend_from_slice(&items_cursor[..suffix_len]);
            items_cursor = &items_cursor[suffix_len..];

            self.push_item(&tmp);
            // For the *next* iteration, the "prev item stripped" starts at
            // the same offset cp_len within the just-pushed item.
            prev_item_stripped_start = cp_len;
        }
        if !items_cursor.is_empty() {
            return Err(UnmarshalDataError::TrailingItemBytes(items_cursor.len()));
        }

        Ok(())
    }

    fn push_item(&mut self, item: &[u8]) {
        // SAFETY of casts: identical reasoning to `add`. Items pushed during
        // unmarshal are bounded by the validated `items_block_size` upstream.
        #[allow(clippy::cast_possible_truncation)]
        let start = self.data.len() as u32;
        self.data.extend_from_slice(item);
        #[allow(clippy::cast_possible_truncation)]
        let end = self.data.len() as u32;
        self.items.push(start..end);
    }
}

fn range_slice<'a>(data: &'a [u8], r: &Range<u32>) -> &'a [u8] {
    &data[r.start as usize..r.end as usize]
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

#[derive(Debug, Error)]
pub enum MarshalError {
    #[error("cannot marshal an empty inmemory block")]
    Empty,
    #[error("too many items in inmemory block: {0} exceeds u32::MAX")]
    TooManyItems(usize),
    #[error(transparent)]
    Zstd(#[from] ZstdError),
}

#[derive(Debug, Error)]
pub enum UnmarshalDataError {
    #[error("items_count must be > 0")]
    ZeroItemsCount,
    #[error("plain lens: {source}")]
    PlainLens { source: esm_compress::int::DecodeError },
    #[error("zstd prefix_lens: {source}")]
    ZstdPrefixLens { source: esm_compress::int::DecodeError },
    #[error("zstd item_lens: {source}")]
    ZstdItemLens { source: esm_compress::int::DecodeError },
    #[error("item length {0} does not fit in usize")]
    ItemLengthOverflow(u64),
    #[error("items chunk truncated: need {need} bytes, have {have}")]
    ItemsTruncated { need: usize, have: usize },
    #[error("prefix_len {prefix_len} exceeds item_len {item_len}")]
    PrefixExceedsItem { prefix_len: usize, item_len: usize },
    #[error("prefix_len {prefix_len} exceeds previous item length {prev_len}")]
    PrefixExceedsPrev { prefix_len: usize, prev_len: usize },
    #[error("{0} trailing bytes after lens chunk")]
    TrailingLensBytes(usize),
    #[error("{0} trailing bytes after items chunk")]
    TrailingItemBytes(usize),
    #[error("decoded items are not sorted")]
    DecodedUnsorted,
    #[error(transparent)]
    Zstd(#[from] ZstdError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(items: &[&[u8]]) -> InmemoryBlock {
        let mut ib = InmemoryBlock::default();
        for it in items {
            assert!(ib.add(it));
        }
        ib
    }

    #[test]
    fn add_and_read_back() {
        let mut ib = InmemoryBlock::default();
        assert!(ib.add(b"alpha"));
        assert!(ib.add(b"beta"));
        assert_eq!(ib.len(), 2);
        assert_eq!(ib.item_bytes(0), b"alpha");
        assert_eq!(ib.item_bytes(1), b"beta");
    }

    #[test]
    fn rejects_oversized_payload() {
        let mut ib = InmemoryBlock::default();
        let big = vec![0u8; super::super::MAX_INMEMORY_BLOCK_SIZE - 10];
        assert!(ib.add(&big));
        let overflow = vec![0u8; 11];
        assert!(!ib.add(&overflow));
        assert_eq!(ib.len(), 1);
    }

    #[test]
    fn reset_preserves_capacity() {
        let mut ib = InmemoryBlock::default();
        ib.add(b"x");
        let cap_before = ib.data.capacity();
        ib.reset();
        assert!(ib.is_empty());
        assert_eq!(ib.data.capacity(), cap_before);
    }

    #[test]
    fn sort_items_lex() {
        let mut ib = make_block(&[b"gamma", b"alpha", b"beta"]);
        ib.sort_items();
        assert_eq!(ib.item_bytes(0), b"alpha");
        assert_eq!(ib.item_bytes(1), b"beta");
        assert_eq!(ib.item_bytes(2), b"gamma");
    }

    #[test]
    fn common_prefix_sorted_detection() {
        let mut ib = make_block(&[b"prefix-aaa", b"prefix-bbb", b"prefix-ccc"]);
        ib.sort_items();
        ib.update_common_prefix_sorted();
        assert_eq!(ib.common_prefix, b"prefix-");
    }

    #[test]
    fn common_prefix_single_item_is_empty() {
        let mut ib = make_block(&[b"sole"]);
        ib.update_common_prefix_sorted();
        assert!(ib.common_prefix.is_empty());
    }

    #[test]
    fn marshal_unmarshal_plain_roundtrip_small_block() {
        // Small block triggers plain encoding.
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let mut ib = make_block(&items);
        ib.sort_items();

        let mut sb = StorageBlock::default();
        let mut first_item = Vec::new();
        let mut common_prefix = Vec::new();
        let (n, mt) =
            ib.marshal_sorted_data(&mut sb, &mut first_item, &mut common_prefix, 5).unwrap();
        assert_eq!(n, 3);
        assert_eq!(mt, MarshalType::Plain);
        assert_eq!(first_item, b"a");

        let mut decoded = InmemoryBlock::default();
        decoded.unmarshal_data(&sb, &first_item, &common_prefix, n, mt).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.item_bytes(0), b"a");
        assert_eq!(decoded.item_bytes(1), b"b");
        assert_eq!(decoded.item_bytes(2), b"c");
    }

    #[test]
    fn marshal_unmarshal_zstd_roundtrip_large_block() {
        // Repeated long items -> zstd path.
        let make_item = |i: u32| -> Vec<u8> {
            let mut v = b"shared/prefix/series-".to_vec();
            v.extend_from_slice(format!("{i:08}").as_bytes());
            v.extend_from_slice(&[0xab; 64]);
            v
        };
        let items: Vec<Vec<u8>> = (0..200u32).map(make_item).collect();
        let mut ib = InmemoryBlock::default();
        for it in &items {
            assert!(ib.add(it));
        }
        ib.sort_items();

        let mut sb = StorageBlock::default();
        let mut first_item = Vec::new();
        let mut common_prefix = Vec::new();
        let (n, mt) =
            ib.marshal_sorted_data(&mut sb, &mut first_item, &mut common_prefix, 5).unwrap();
        assert_eq!(n, 200);
        assert_eq!(mt, MarshalType::Zstd, "long repetitive payload should pick zstd");
        assert!(common_prefix.starts_with(b"shared/prefix/series-"));

        let mut decoded = InmemoryBlock::default();
        decoded.unmarshal_data(&sb, &first_item, &common_prefix, n, mt).unwrap();
        assert_eq!(decoded.len(), 200);
        // Spot-check: items are sorted and equal to the originals.
        let mut originals = items.clone();
        originals.sort();
        for (i, original) in originals.iter().enumerate() {
            assert_eq!(decoded.item_bytes(i), original.as_slice(), "mismatch at index {i}");
        }
    }

    #[test]
    fn marshal_unsorted_data_sorts_first() {
        let mut ib = make_block(&[b"zeta", b"alpha", b"beta"]);
        let mut sb = StorageBlock::default();
        let mut first_item = Vec::new();
        let mut common_prefix = Vec::new();
        let (n, mt) =
            ib.marshal_unsorted_data(&mut sb, &mut first_item, &mut common_prefix, 5).unwrap();
        assert_eq!(n, 3);
        assert_eq!(mt, MarshalType::Plain);
        assert_eq!(first_item, b"alpha");
    }

    #[test]
    fn unmarshal_zero_items_rejected() {
        let sb = StorageBlock::default();
        let mut decoded = InmemoryBlock::default();
        assert!(matches!(
            decoded.unmarshal_data(&sb, b"x", b"", 0, MarshalType::Plain),
            Err(UnmarshalDataError::ZeroItemsCount)
        ));
    }
}
