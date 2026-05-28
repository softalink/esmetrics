//! Atomic rename with cross-platform replacement semantics.

use std::fs;
use std::io;
use std::path::Path;

/// Atomically rename `from` to `to`, replacing `to` if it exists.
///
/// Same-filesystem renames are atomic on both POSIX and NTFS. Cross-filesystem
/// renames are not atomic anywhere (Rust's `std::fs::rename` falls back to
/// copy+unlink); EsMetrics never relies on cross-fs renames for durability.
///
/// On Windows, Rust's standard library uses `MoveFileEx` with
/// `MOVEFILE_REPLACE_EXISTING`, providing the same atomic-with-replace
/// behaviour as POSIX `rename(2)`. This module exists so callers can express
/// intent uniformly and so we have a single place to land retry logic if
/// Windows sharing violations turn up under real workloads.
pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<()> {
    fs::rename(from, to)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::write;

    #[test]
    fn rename_replaces_existing_target() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("source");
        let to = dir.path().join("dest");
        write(&from, b"new").unwrap();
        write(&to, b"old").unwrap();

        rename(&from, &to).unwrap();

        assert!(!from.exists());
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
    }

    #[test]
    fn rename_creates_new_target() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("source");
        let to = dir.path().join("dest");
        write(&from, b"hello").unwrap();

        rename(&from, &to).unwrap();

        assert_eq!(std::fs::read(&to).unwrap(), b"hello");
    }
}
