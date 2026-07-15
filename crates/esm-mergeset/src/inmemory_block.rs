//! Port of `encoding.go`: `Item`, `inmemoryBlock`, `storageBlock` and the
//! plain/zstd block marshaling.

use esm_encoding::{
    compress_zstd_level, decompress_zstd, marshal_uint64, marshal_var_uint64s, unmarshal_uint64,
    unmarshal_var_uint64s,
};

/// The maximum `InmemoryBlock` data size.
///
/// It must fit CPU cache size, i.e. 64KB for the current CPUs.
pub const MAX_INMEMORY_BLOCK_SIZE: usize = 64 * 1024;

/// A single item stored in a mergeset, as offsets into a shared data buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Item {
    /// Start offset of the item in data.
    pub start: u32,
    /// End offset of the item in data.
    pub end: u32,
}

impl Item {
    /// Returns the bytes representation of the item obtained from `data`.
    #[inline]
    pub fn bytes<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        &data[self.start as usize..self.end as usize]
    }
}

/// A block of data in its on-storage representation.
#[derive(Default)]
pub(crate) struct StorageBlock {
    pub items_data: Vec<u8>,
    pub lens_data: Vec<u8>,
}

impl StorageBlock {
    pub fn reset(&mut self) {
        self.items_data.clear();
        self.lens_data.clear();
    }
}

/// Marshal type used for block compression.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum MarshalType {
    #[default]
    Plain = 0,
    Zstd = 1,
}

impl MarshalType {
    pub fn from_u8(v: u8) -> Result<MarshalType, String> {
        match v {
            0 => Ok(MarshalType::Plain),
            1 => Ok(MarshalType::Zstd),
            _ => Err(format!("marshalType must be in the range [0..1]; got {v}")),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Returns the length of the common prefix of `a` and `b`.
pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// An in-memory block of sorted items sharing a common prefix.
#[derive(Default)]
pub(crate) struct InmemoryBlock {
    /// Common prefix for all the items stored in the block.
    pub common_prefix: Vec<u8>,
    /// Source data for items. Every item includes `common_prefix`.
    pub data: Vec<u8>,
    /// Items stored in the block.
    pub items: Vec<Item>,
}

impl InmemoryBlock {
    pub fn copy_from(&mut self, src: &InmemoryBlock) {
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(&src.common_prefix);
        self.data.clear();
        self.data.extend_from_slice(&src.data);
        self.items.clear();
        self.items.extend_from_slice(&src.items);
    }

    pub fn reset(&mut self) {
        self.common_prefix.clear();
        self.data.clear();
        self.items.clear();
    }

    /// The approximate in-memory size of the block (Go `inmemoryBlock.SizeBytes`).
    pub fn size_bytes(&self) -> usize {
        std::mem::size_of::<InmemoryBlock>()
            + self.common_prefix.capacity()
            + self.data.capacity()
            + self.items.capacity() * std::mem::size_of::<Item>()
    }

    /// Adds `x` to the end of the block.
    ///
    /// Returns false if `x` isn't added due to block size constraints.
    pub fn add(&mut self, x: &[u8]) -> bool {
        if x.len() + self.data.len() > MAX_INMEMORY_BLOCK_SIZE {
            return false;
        }
        if self.data.capacity() == 0 {
            // Pre-allocate data and items in order to reduce memory allocations.
            self.data.reserve(MAX_INMEMORY_BLOCK_SIZE);
            self.items.reserve(512);
        }
        let start = self.data.len() as u32;
        self.data.extend_from_slice(x);
        self.items.push(Item {
            start,
            end: self.data.len() as u32,
        });
        true
    }

    pub fn is_sorted(&self) -> bool {
        let data = &self.data;
        self.items
            .windows(2)
            .all(|w| w[0].bytes(data) <= w[1].bytes(data))
    }

    pub fn sort_items(&mut self) {
        if !self.is_sorted() {
            self.update_common_prefix_unsorted();
            let cp_len = self.common_prefix.len() as u32;
            let data = &self.data;
            // All items share common_prefix, so comparing the suffixes is
            // equivalent to comparing the whole items.
            self.items.sort_unstable_by(|a, b| {
                let sa = &data[(a.start + cp_len) as usize..a.end as usize];
                let sb = &data[(b.start + cp_len) as usize..b.end as usize];
                sa.cmp(sb)
            });
        } else {
            self.update_common_prefix_sorted();
        }
    }

    pub fn update_common_prefix_sorted(&mut self) {
        if self.items.len() <= 1 {
            // There is no sense in duplicating a single item or zero items
            // into common_prefix, since this only can increase blockHeader
            // size without any benefits.
            self.common_prefix.clear();
            return;
        }
        let first = self.items[0].bytes(&self.data);
        let last = self.items[self.items.len() - 1].bytes(&self.data);
        let cp_len = common_prefix_len(first, last);
        let cp_start = self.items[0].start as usize;
        // Copy through a temporary to satisfy the borrow checker.
        let cp: Vec<u8> = self.data[cp_start..cp_start + cp_len].to_vec();
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(&cp);
    }

    fn update_common_prefix_unsorted(&mut self) {
        self.common_prefix.clear();
        if self.items.is_empty() {
            return;
        }
        let data = &self.data;
        let first = self.items[0];
        let mut cp = first.bytes(data);
        for it in &self.items[1..] {
            let item = it.bytes(data);
            if item.starts_with(cp) {
                continue;
            }
            let cp_len = common_prefix_len(cp, item);
            if cp_len == 0 {
                return;
            }
            cp = &cp[..cp_len];
        }
        let cp: Vec<u8> = cp.to_vec();
        self.common_prefix.extend_from_slice(&cp);
    }

    pub fn debug_items_string(&self) -> String {
        use std::fmt::Write;
        let mut sb = String::new();
        let mut prev_item: &[u8] = b"";
        for (i, it) in self.items.iter().enumerate() {
            let item = it.bytes(&self.data);
            if item < prev_item {
                sb.push_str("!!! the next item is smaller than the previous item !!!\n");
            }
            let _ = writeln!(sb, "{i:05} {}", crate::util::hex_encode(item));
            prev_item = item;
        }
        sb
    }

    /// Marshals unsorted items to `sb` after sorting them.
    ///
    /// Appends the first item to `first_item_dst` and the common prefix to
    /// `common_prefix_dst`. Returns the number of items encoded (including
    /// the first item) and the marshal type used.
    pub fn marshal_unsorted_data(
        &mut self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> (u32, MarshalType) {
        self.sort_items();
        self.marshal_data(sb, first_item_dst, common_prefix_dst, compress_level)
    }

    /// Marshals already sorted items to `sb`.
    ///
    /// See [`InmemoryBlock::marshal_unsorted_data`] for the output contract.
    pub fn marshal_sorted_data(
        &mut self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> (u32, MarshalType) {
        #[cfg(debug_assertions)]
        if !self.is_sorted() {
            panic!(
                "BUG: {} items must be sorted; items:\n{}",
                self.items.len(),
                self.debug_items_string()
            );
        }
        self.update_common_prefix_sorted();
        self.marshal_data(sb, first_item_dst, common_prefix_dst, compress_level)
    }

    /// Preconditions:
    /// - `self.items` must be sorted.
    /// - `update_common_prefix_*` must be called.
    fn marshal_data(
        &self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> (u32, MarshalType) {
        assert!(
            !self.items.is_empty(),
            "BUG: InmemoryBlock::marshal_data must be called on non-empty blocks only"
        );
        assert!(
            (self.items.len() as u64) < (1u64 << 32),
            "BUG: the number of items in the block must be smaller than 2^32; got {} items",
            self.items.len()
        );

        let data = &self.data;
        let first_item = self.items[0].bytes(data);
        first_item_dst.extend_from_slice(first_item);
        common_prefix_dst.extend_from_slice(&self.common_prefix);

        let cp_len = self.common_prefix.len();
        if data.len() - cp_len * self.items.len() < 64 || self.items.len() < 2 {
            // Use plain encoding for small blocks, since it is cheaper.
            self.marshal_data_plain(sb);
            return (self.items.len() as u32, MarshalType::Plain);
        }

        let mut b_items: Vec<u8> = Vec::new();
        let mut b_lens: Vec<u8> = Vec::new();
        let mut xs: Vec<u64> = Vec::with_capacity(self.items.len() - 1);

        // Marshal items data.
        let mut prev_item = &first_item[cp_len..];
        let mut prev_prefix_len = 0u64;
        for it in &self.items[1..] {
            let item = &data[it.start as usize + cp_len..it.end as usize];
            let prefix_len = common_prefix_len(prev_item, item) as u64;
            b_items.extend_from_slice(&item[prefix_len as usize..]);
            xs.push(prefix_len ^ prev_prefix_len);
            prev_item = item;
            prev_prefix_len = prefix_len;
        }
        marshal_var_uint64s(&mut b_lens, &xs);
        sb.items_data.clear();
        compress_zstd_level(&mut sb.items_data, &b_items, compress_level);

        // Marshal lens data.
        xs.clear();
        let mut prev_item_len = (first_item.len() - cp_len) as u64;
        for it in &self.items[1..] {
            let item_len = ((it.end - it.start) as usize - cp_len) as u64;
            xs.push(item_len ^ prev_item_len);
            prev_item_len = item_len;
        }
        marshal_var_uint64s(&mut b_lens, &xs);
        sb.lens_data.clear();
        compress_zstd_level(&mut sb.lens_data, &b_lens, compress_level);

        if sb.items_data.len() as f64 > 0.9 * (data.len() - cp_len * self.items.len()) as f64 {
            // Bad compression rate. It is cheaper to use plain encoding.
            self.marshal_data_plain(sb);
            return (self.items.len() as u32, MarshalType::Plain);
        }

        // Good compression rate.
        (self.items.len() as u32, MarshalType::Zstd)
    }

    fn marshal_data_plain(&self, sb: &mut StorageBlock) {
        let data = &self.data;
        let cp_len = self.common_prefix.len();

        // Marshal items data.
        // There is no need in marshaling the first item, since it is returned
        // to the caller in marshal_data.
        sb.items_data.clear();
        for it in &self.items[1..] {
            sb.items_data
                .extend_from_slice(&data[it.start as usize + cp_len..it.end as usize]);
        }

        // Marshal length data.
        sb.lens_data.clear();
        for it in &self.items[1..] {
            marshal_uint64(
                &mut sb.lens_data,
                ((it.end - it.start) as usize - cp_len) as u64,
            );
        }
    }

    /// Rebuilds a single-item block from the block header data alone.
    pub fn unmarshal_single_item(
        &mut self,
        common_prefix: &[u8],
        first_item: &[u8],
        mt: MarshalType,
    ) {
        assert!(
            mt == MarshalType::Plain,
            "BUG: single item block must be always encoded with MarshalType::Plain"
        );
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(common_prefix);
        self.data.clear();
        self.data.extend_from_slice(first_item);
        self.items.clear();
        self.items.push(Item {
            start: 0,
            end: self.data.len() as u32,
        });
    }

    /// Decodes `items_count` items from `sb` and `first_item` and stores them
    /// in `self`.
    pub fn unmarshal_data(
        &mut self,
        sb: &StorageBlock,
        first_item: &[u8],
        common_prefix: &[u8],
        items_count: u32,
        mt: MarshalType,
    ) -> Result<(), String> {
        self.reset();

        assert!(items_count > 0, "BUG: cannot unmarshal zero items");

        self.common_prefix.extend_from_slice(common_prefix);

        match mt {
            MarshalType::Plain => {
                self.unmarshal_data_plain(sb, first_item, items_count)
                    .map_err(|e| format!("cannot unmarshal plain data: {e}"))?;
                if !self.is_sorted() {
                    return Err(format!(
                        "plain data block contains unsorted items; items:\n{}",
                        self.debug_items_string()
                    ));
                }
                return Ok(());
            }
            MarshalType::Zstd => {
                // Handled below.
            }
        }

        // Unmarshal mt = MarshalType::Zstd

        // Unmarshal lens data.
        let mut bb: Vec<u8> = Vec::new();
        decompress_zstd(&mut bb, &sb.lens_data)
            .map_err(|e| format!("cannot decompress lensData: {e}"))?;

        let items_count = items_count as usize;
        let mut prefix_lens: Vec<u64> = vec![0; items_count];
        let mut lens: Vec<u64> = vec![0; items_count];
        let mut xs: Vec<u64> = vec![0; items_count - 1];

        // Unmarshal prefix lens.
        let tail = unmarshal_var_uint64s(&mut xs, &bb)
            .map_err(|e| format!("cannot unmarshal prefixLens from lensData: {e}"))?;
        prefix_lens[0] = 0;
        for i in 0..xs.len() {
            prefix_lens[i + 1] = xs[i] ^ prefix_lens[i];
        }

        // Unmarshal lens.
        let tail = unmarshal_var_uint64s(&mut xs, tail)
            .map_err(|e| format!("cannot unmarshal lens from lensData: {e}"))?;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected tail left unmarshaling {} lens; tail size={}",
                items_count,
                tail.len()
            ));
        }
        lens[0] = (first_item.len() - common_prefix.len()) as u64;
        let mut data_len = common_prefix.len() * items_count;
        data_len += lens[0] as usize;
        for i in 0..xs.len() {
            let item_len = xs[i] ^ lens[i];
            lens[i + 1] = item_len;
            data_len += item_len as usize;
        }

        // Unmarshal items data.
        bb.clear();
        decompress_zstd(&mut bb, &sb.items_data)
            .map_err(|e| format!("cannot decompress itemsData: {e}"))?;

        self.data.reserve(data_len);
        self.data.extend_from_slice(first_item);
        self.items.push(Item {
            start: 0,
            end: self.data.len() as u32,
        });
        let mut prev_item_start = common_prefix.len();
        let mut b: &[u8] = &bb;
        for i in 1..items_count {
            let item_len = lens[i];
            let prefix_len = prefix_lens[i];
            if prefix_len > item_len {
                return Err(format!("prefixLen={prefix_len} exceeds itemLen={item_len}"));
            }
            let suffix_len = (item_len - prefix_len) as usize;
            if b.len() < suffix_len {
                return Err(format!(
                    "not enough data for decoding item from itemsData; want {suffix_len} bytes; remained {} bytes",
                    b.len()
                ));
            }
            if prev_item_start + prefix_len as usize > self.data.len() {
                return Err(format!(
                    "prefixLen cannot exceed {}; got {prefix_len}",
                    self.data.len() - prev_item_start
                ));
            }
            let data_start = self.data.len();
            self.data.extend_from_slice(common_prefix);
            self.data
                .extend_from_within(prev_item_start..prev_item_start + prefix_len as usize);
            self.data.extend_from_slice(&b[..suffix_len]);
            self.items.push(Item {
                start: data_start as u32,
                end: self.data.len() as u32,
            });
            b = &b[suffix_len..];
            prev_item_start = self.data.len() - item_len as usize;
        }
        if !b.is_empty() {
            return Err(format!(
                "unexpected tail left after itemsData with len {}",
                b.len()
            ));
        }
        if self.data.len() != data_len {
            return Err(format!(
                "unexpected data len; got {}; want {data_len}",
                self.data.len()
            ));
        }
        if !self.is_sorted() {
            return Err(format!(
                "decoded data block contains unsorted items; items:\n{}",
                self.debug_items_string()
            ));
        }
        Ok(())
    }

    fn unmarshal_data_plain(
        &mut self,
        sb: &StorageBlock,
        first_item: &[u8],
        items_count: u32,
    ) -> Result<(), String> {
        let common_prefix_len = self.common_prefix.len();
        let items_count = items_count as usize;

        // Unmarshal lens data.
        let mut lens: Vec<u64> = vec![0; items_count];
        lens[0] = (first_item.len() - common_prefix_len) as u64;
        let mut b: &[u8] = &sb.lens_data;
        for len_slot in lens.iter_mut().skip(1) {
            if b.len() < 8 {
                return Err(format!(
                    "too short tail for decoding len from lensData; got {} bytes; want at least 8 bytes",
                    b.len()
                ));
            }
            *len_slot = unmarshal_uint64(b);
            b = &b[8..];
        }
        if !b.is_empty() {
            return Err(format!(
                "unexpected tail left after lensData with len {}",
                b.len()
            ));
        }

        // Unmarshal items data.
        let data_len =
            first_item.len() + sb.items_data.len() + common_prefix_len * (items_count - 1);
        self.data.reserve(data_len);
        self.data.extend_from_slice(first_item);
        self.items.push(Item {
            start: 0,
            end: self.data.len() as u32,
        });
        b = &sb.items_data;
        for &item_len in lens.iter().take(items_count).skip(1) {
            let item_len = item_len as usize;
            if b.len() < item_len {
                return Err(format!(
                    "not enough data for decoding item from itemsData; want {item_len} bytes; remained {} bytes",
                    b.len()
                ));
            }
            let data_start = self.data.len();
            self.data.extend_from_slice(&self.common_prefix);
            self.data.extend_from_slice(&b[..item_len]);
            self.items.push(Item {
                start: data_start as u32,
                end: self.data.len() as u32,
            });
            b = &b[item_len..];
        }
        if !b.is_empty() {
            return Err(format!(
                "unexpected tail left after itemsData with len {}",
                b.len()
            ));
        }
        if self.data.len() != data_len {
            return Err(format!(
                "unexpected data len; got {}; want {data_len}",
                self.data.len()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    /// Deterministic pseudo-random generator (SplitMix64).
    pub struct Rng(pub u64);

    impl Rng {
        pub fn new(seed: u64) -> Rng {
            Rng(seed)
        }

        pub fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^ (z >> 31)
        }

        pub fn intn(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }

        /// Returns random bytes with length in [0, 50), mimicking Go's
        /// `testing/quick` generated `[]byte` values.
        pub fn random_bytes(&mut self) -> Vec<u8> {
            let n = self.intn(50);
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(self.next_u64() as u8);
            }
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::Rng;
    use super::*;

    #[test]
    fn test_common_prefix_len() {
        let f = |a: &str, b: &str, expected: usize| {
            assert_eq!(
                common_prefix_len(a.as_bytes(), b.as_bytes()),
                expected,
                "a={a:?} b={b:?}"
            );
        };
        f("", "", 0);
        f("a", "", 0);
        f("", "a", 0);
        f("a", "a", 1);
        f("abc", "xy", 0);
        f("abc", "abd", 2);
        f("01234567", "01234567", 8);
        f("01234567", "012345678", 8);
        f("012345679", "012345678", 8);
        f("01234569", "012345678", 7);
        f("01234569", "01234568", 7);
    }

    #[test]
    fn test_inmemory_block_add() {
        let mut r = Rng::new(1);
        let mut ib = InmemoryBlock::default();

        for i in 0..30 {
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut total_len = 0;
            ib.reset();

            // Fill ib.
            for _ in 0..(i * 100 + 1) {
                let s = r.random_bytes();
                if !ib.add(&s) {
                    break;
                }
                total_len += s.len();
                items.push(s);
            }

            // Verify all the items are added.
            assert_eq!(ib.items.len(), items.len());
            assert_eq!(ib.data.len(), total_len);
            for (j, it) in ib.items.iter().enumerate() {
                assert_eq!(it.bytes(&ib.data), &items[j][..], "index {j} loop {i}");
            }
        }
    }

    #[test]
    fn test_inmemory_block_sort() {
        let mut r = Rng::new(1);
        let mut ib = InmemoryBlock::default();

        for i in 0..100 {
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut total_len = 0;
            ib.reset();

            for _ in 0..r.intn(1500) {
                let s = r.random_bytes();
                if !ib.add(&s) {
                    break;
                }
                total_len += s.len();
                items.push(s);
            }

            ib.sort_items();
            items.sort();

            assert_eq!(ib.items.len(), items.len());
            assert_eq!(ib.data.len(), total_len);
            for (j, it) in ib.items.iter().enumerate() {
                assert_eq!(it.bytes(&ib.data), &items[j][..], "index {j} loop {i}");
            }
        }
    }

    #[test]
    fn test_inmemory_block_marshal_unmarshal() {
        let mut r = Rng::new(1);
        let mut ib = InmemoryBlock::default();
        let mut ib2 = InmemoryBlock::default();
        let mut sb = StorageBlock::default();

        for i in (0..1000).step_by(10) {
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut total_len = 0;
            ib.reset();

            // Fill ib.
            let items_count = 2 * (r.intn(i + 1) + 1);
            for _ in 0..items_count / 2 {
                let mut s = b"prefix ".to_vec();
                s.extend_from_slice(&r.random_bytes());
                if !ib.add(&s) {
                    break;
                }
                total_len += s.len();
                items.push(s);

                let s = r.random_bytes();
                if !ib.add(&s) {
                    break;
                }
                total_len += s.len();
                items.push(s);
            }

            // Marshal ib.
            items.sort();
            let mut first_item = Vec::new();
            let mut common_prefix = Vec::new();
            let (items_len, mt) =
                ib.marshal_unsorted_data(&mut sb, &mut first_item, &mut common_prefix, 0);
            assert_eq!(items_len as usize, ib.items.len());
            assert_eq!(&first_item[..], ib.items[0].bytes(&ib.data));

            // Unmarshal ib.
            ib2.unmarshal_data(&sb, &first_item, &common_prefix, items_len, mt)
                .unwrap_or_else(|e| {
                    panic!("cannot unmarshal data (itemsLen={items_len}, mt={mt:?}): {e}")
                });

            // Verify all the items are sorted and unmarshaled.
            assert_eq!(ib2.items.len(), items.len());
            assert_eq!(ib2.data.len(), total_len);
            for (j, it) in ib2.items.iter().enumerate() {
                assert_eq!(
                    it.bytes(&ib2.data),
                    &items[j][..],
                    "index {j} out of {} loop {i}",
                    items.len()
                );
            }
        }
    }

    #[test]
    fn test_single_item_roundtrip() {
        let mut ib = InmemoryBlock::default();
        assert!(ib.add(b"foobar"));
        let mut sb = StorageBlock::default();
        let mut first_item = Vec::new();
        let mut common_prefix = Vec::new();
        let (count, mt) = ib.marshal_unsorted_data(&mut sb, &mut first_item, &mut common_prefix, 0);
        assert_eq!(count, 1);
        assert_eq!(mt, MarshalType::Plain);
        assert_eq!(&first_item[..], b"foobar");
        assert!(common_prefix.is_empty());

        let mut ib2 = InmemoryBlock::default();
        ib2.unmarshal_single_item(&common_prefix, &first_item, mt);
        assert_eq!(ib2.items.len(), 1);
        assert_eq!(ib2.items[0].bytes(&ib2.data), b"foobar");
    }

    #[test]
    fn test_marshal_type_check() {
        assert_eq!(MarshalType::from_u8(0).unwrap(), MarshalType::Plain);
        assert_eq!(MarshalType::from_u8(1).unwrap(), MarshalType::Zstd);
        assert!(MarshalType::from_u8(2).is_err());
    }
}
