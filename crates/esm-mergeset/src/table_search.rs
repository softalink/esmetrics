//! Port of `table_search.go`.

use std::sync::Arc;

use crate::part_search::PartSearch;
use crate::part_wrapper::PartWrapper;
use crate::table::Table;
use crate::Error;

/// A search cursor over a [`Table`].
///
/// Duplicates across parts are NOT deduplicated - callers may see them.
pub struct TableSearch {
    pws: Vec<Arc<PartWrapper>>,

    ps_pool: Vec<PartSearch>,
    /// Min-heap of indices into `ps_pool`, ordered by the current item.
    ps_heap: Vec<usize>,

    err: Option<Error>,

    next_item_noop: bool,
}

fn sift_down(pool: &[PartSearch], heap: &mut [usize], mut i: usize) {
    let n = heap.len();
    loop {
        let left = 2 * i + 1;
        if left >= n {
            return;
        }
        let mut smallest = left;
        let right = left + 1;
        if right < n && pool[heap[right]].item() < pool[heap[left]].item() {
            smallest = right;
        }
        if pool[heap[smallest]].item() >= pool[heap[i]].item() {
            return;
        }
        heap.swap(i, smallest);
        i = smallest;
    }
}

fn heap_init(pool: &[PartSearch], heap: &mut [usize]) {
    let n = heap.len();
    for i in (0..n / 2).rev() {
        sift_down(pool, heap, i);
    }
}

impl TableSearch {
    /// Creates a search cursor over `tb`.
    ///
    /// `sparse` selects the sparse data-block cache for cache-unfriendly
    /// scans (currently a no-op since the block caches are deferred).
    ///
    /// The cursor holds references to the table parts until it is dropped.
    pub fn new(tb: &Table, sparse: bool) -> TableSearch {
        let pws = tb.inner.get_parts();
        let ps_pool = pws
            .iter()
            .map(|pw| PartSearch::new(Arc::clone(&pw.p), sparse))
            .collect();
        TableSearch {
            pws,
            ps_pool,
            ps_heap: Vec::new(),
            err: None,
            next_item_noop: false,
        }
    }

    /// The current item after a successful `next_item` or
    /// `first_item_with_prefix` call.
    ///
    /// The contents are valid until the next call to `next_item`.
    pub fn item(&self) -> &[u8] {
        self.ps_pool[self.ps_heap[0]].item()
    }

    /// Seeks for the first item greater or equal to `k`.
    pub fn seek(&mut self, k: &[u8]) {
        if self.error().is_some() {
            // Do nothing on unrecoverable error.
            return;
        }
        self.err = None;

        // Initialize the heap.
        self.ps_heap.clear();
        for i in 0..self.ps_pool.len() {
            let ps = &mut self.ps_pool[i];
            ps.seek(k);
            if !ps.next_item() {
                if let Some(err) = ps.error() {
                    // Return only the first error, since it has no sense in
                    // returning all the errors.
                    self.err = Some(Error::Other(format!("cannot seek {k:?}: {err}")));
                    return;
                }
                continue;
            }
            self.ps_heap.push(i);
        }
        if self.ps_heap.is_empty() {
            self.err = Some(Error::Eof);
            return;
        }
        heap_init(&self.ps_pool, &mut self.ps_heap);
        self.next_item_noop = true;
    }

    /// Seeks for the first item with the given prefix.
    ///
    /// Returns `Error::Eof` if such an item doesn't exist.
    pub fn first_item_with_prefix(&mut self, prefix: &[u8]) -> Result<(), Error> {
        self.seek(prefix);
        if !self.next_item() {
            if let Some(err) = self.error() {
                return Err(err);
            }
            return Err(Error::Eof);
        }
        if let Some(err) = self.error() {
            return Err(err);
        }
        if !self.item().starts_with(prefix) {
            return Err(Error::Eof);
        }
        Ok(())
    }

    /// Advances to the next item. Returns true on success.
    pub fn next_item(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        if self.next_item_noop {
            self.next_item_noop = false;
            return true;
        }

        if let Err(err) = self.next_block() {
            self.err = Some(match err {
                Error::Eof => Error::Eof,
                Error::Other(msg) => Error::Other(format!(
                    "cannot obtain the next block to search in the table: {msg}"
                )),
            });
            return false;
        }
        true
    }

    fn next_block(&mut self) -> Result<(), Error> {
        let top = self.ps_heap[0];
        if self.ps_pool[top].next_item() {
            sift_down(&self.ps_pool, &mut self.ps_heap, 0);
            return Ok(());
        }

        if let Some(err) = self.ps_pool[top].error() {
            return Err(Error::Other(err));
        }

        // Pop the exhausted part search from the heap.
        let n = self.ps_heap.len();
        self.ps_heap.swap(0, n - 1);
        self.ps_heap.pop();
        sift_down(&self.ps_pool, &mut self.ps_heap, 0);

        if self.ps_heap.is_empty() {
            return Err(Error::Eof);
        }
        Ok(())
    }

    /// Returns the last error, ignoring EOF.
    pub fn error(&self) -> Option<Error> {
        match &self.err {
            Some(Error::Eof) | None => None,
            Some(err) => Some(err.clone()),
        }
    }

    /// Closes the search, releasing the table part references.
    pub fn must_close(self) {
        // The part references (self.pws) are dropped here.
    }

    /// The number of parts the search operates on.
    pub fn parts_count(&self) -> usize {
        self.pws.len()
    }
}
