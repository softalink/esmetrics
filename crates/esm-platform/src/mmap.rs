//! Memory-mapped file access.
//!
//! Thin wrappers around `memmap2` that document the cross-platform contract
//! EsMetrics relies on.
//!
//! ## Windows quirks
//!
//! On Windows, a memory-mapped file cannot be deleted while a mapping exists.
//! `esm-storage` coordinates this with a defer-unlink mechanism: a part that
//! would normally be unlinked after compaction is parked on a deletion queue
//! and reclaimed once all live mappings against it are dropped. This module
//! intentionally does NOT try to solve that at the platform layer.
//!
//! ## Safety contract
//!
//! Memory-mapped files in Rust are `unsafe` to create because the kernel can
//! mutate the underlying bytes (another process writing to the file, the file
//! being truncated, etc.) and produce undefined behaviour in safe code that
//! holds the mapping.
//!
//! EsMetrics only ever mmaps files that meet at least one of:
//! * The file is an immutable part file produced by the merger, and the
//!   merger holds a defer-unlink reference for the lifetime of any mapping.
//! * The mapping is created exclusively by the caller, who also owns the
//!   only writer (e.g., the merger writing a new part).
//!
//! These invariants are upheld by `esm-storage`; the `unsafe` blocks below
//! document the precondition each callsite must satisfy.

use std::fs::{File, OpenOptions};
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use memmap2::{Mmap as M2Mmap, MmapMut as M2MmapMut, MmapOptions};

/// Read-only memory-mapped view of a file.
pub struct Mmap {
    inner: M2Mmap,
    /// File handle is kept alive for the lifetime of the mapping; Windows
    /// requires this and on Unix it's harmless.
    _file: File,
}

impl Mmap {
    /// Open `path` for reading and memory-map its full contents.
    ///
    /// # Safety contract for callers
    ///
    /// See module docs. Callers must guarantee no other process or thread
    /// will mutate or truncate the underlying file for the lifetime of the
    /// returned `Mmap`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: see module docs and the safety contract on this function;
        // EsMetrics call sites only invoke this on immutable part files held
        // alive by the merger's defer-unlink queue.
        let inner = unsafe { MmapOptions::new().map(&file)? };
        Ok(Self { inner, _file: file })
    }

    /// Length of the mapping in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when the mapping has zero length.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Deref for Mmap {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.inner
    }
}

impl std::fmt::Debug for Mmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mmap").field("len", &self.inner.len()).finish_non_exhaustive()
    }
}

/// Read-write memory-mapped view of a file.
pub struct MmapMut {
    inner: M2MmapMut,
    _file: File,
}

impl MmapMut {
    /// Create (or truncate to length zero, then extend to `len`) the file at
    /// `path` and return a writable mapping over its contents.
    ///
    /// The caller is the exclusive author of the file until the returned
    /// `MmapMut` is dropped.
    pub fn create<P: AsRef<Path>>(path: P, len: u64) -> io::Result<Self> {
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        file.set_len(len)?;
        // SAFETY: caller holds exclusive write access to the file; no
        // external mutation can occur for the lifetime of this mapping.
        let inner = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Self { inner, _file: file })
    }

    /// Flush the entire mapped range to disk synchronously.
    pub fn flush(&self) -> io::Result<()> {
        self.inner.flush()
    }

    /// Length of the mapping in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when the mapping has zero length.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Deref for MmapMut {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.inner
    }
}

impl DerefMut for MmapMut {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.inner
    }
}

impl std::fmt::Debug for MmapMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapMut").field("len", &self.inner.len()).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::write;

    #[test]
    fn map_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        write(&path, b"hello world").unwrap();

        let m = Mmap::open(&path).unwrap();
        assert_eq!(&*m, b"hello world");
        assert_eq!(m.len(), 11);
        assert!(!m.is_empty());
    }

    #[test]
    fn map_mut_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");

        {
            let mut m = MmapMut::create(&path, 5).unwrap();
            m.copy_from_slice(b"hello");
            m.flush().unwrap();
        }

        let m = Mmap::open(&path).unwrap();
        assert_eq!(&*m, b"hello");
    }

    #[test]
    fn map_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        let m = MmapMut::create(&path, 0).unwrap();
        assert!(m.is_empty());
    }
}
