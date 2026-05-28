//! `StorageBlock` — paired items + lens chunks ready for placement in
//! `items.bin` / `lens.bin`. VM source: `lib/mergeset/encoding.go:187-195`.
//!
//! Buffers are caller-owned and reusable across encodings to avoid allocation
//! churn in the merger hot path.

/// One block's worth of `items.bin` and `lens.bin` bytes, as produced by
/// [`crate::mergeset::InmemoryBlock::marshal_sorted_data`] and consumed by
/// the reader.
///
/// The `items_data` and `lens_data` buffers are owned by the block and may
/// be reused across encodings via [`StorageBlock::reset`].
#[derive(Debug, Default, Clone)]
pub struct StorageBlock {
    /// Bytes destined for `items.bin`.
    pub items_data: Vec<u8>,
    /// Bytes destined for `lens.bin`.
    pub lens_data: Vec<u8>,
}

impl StorageBlock {
    /// Clear both buffers without releasing capacity.
    pub fn reset(&mut self) {
        self.items_data.clear();
        self.lens_data.clear();
    }
}
