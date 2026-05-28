//! Exclusive data-directory locking.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use fs2::FileExt;

/// Holds an exclusive advisory lock on a file. Released when dropped.
///
/// Unix: `flock(2)` with `LOCK_EX`. Windows: `LockFileEx` with
/// `LOCKFILE_EXCLUSIVE_LOCK`.
///
/// EsMetrics binaries acquire this on the data directory at startup to
/// prevent two instances of `esm-single` (or `esm-single` + `vmsingle`) from
/// scribbling on the same storage tree.
#[derive(Debug)]
pub struct FileLock {
    file: File,
}

impl FileLock {
    /// Acquire an exclusive lock on `path`, creating the file if it does not
    /// exist. Blocks until the lock is available.
    pub fn acquire_exclusive<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        FileExt::lock_exclusive(&file)?;
        Ok(Self { file })
    }

    /// Attempt to acquire an exclusive lock on `path` without blocking.
    ///
    /// Returns `Ok(None)` if the lock is currently held by another process or
    /// thread; `Ok(Some(lock))` on success; `Err` on any other I/O failure.
    pub fn try_acquire_exclusive<P: AsRef<Path>>(path: P) -> io::Result<Option<Self>> {
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(Self { file })),
            // Contention surfaces as `WouldBlock` on Unix but `ERROR_LOCK_VIOLATION`
            // (Uncategorized) on Windows; `lock_contended_error` is the portable match.
            Err(e) if e.kind() == fs2::lock_contended_error().kind() => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_and_release() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lockfile");

        let lock = FileLock::acquire_exclusive(&path).unwrap();
        drop(lock);

        let again = FileLock::try_acquire_exclusive(&path).unwrap();
        assert!(again.is_some(), "expected to re-acquire after drop");
    }

    #[test]
    fn second_try_acquire_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lockfile");

        let first = FileLock::acquire_exclusive(&path).unwrap();
        let second = FileLock::try_acquire_exclusive(&path).unwrap();
        assert!(second.is_none(), "expected None while first lock is held");
        drop(first);
    }
}
