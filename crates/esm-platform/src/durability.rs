//! fsync semantics for files and directories.

use std::fs::File;
use std::io;
use std::path::Path;

/// Flush all in-memory data and metadata for `file` to durable storage.
///
/// Unix: `fsync(2)`. Windows: `FlushFileBuffers`. Both routes are exercised by
/// `File::sync_all`; this is a thin wrapper to keep the abstraction consistent
/// with [`fsync_dir`].
pub fn fsync_file(file: &File) -> io::Result<()> {
    file.sync_all()
}

/// Flush directory metadata for `path` to durable storage.
///
/// Unix: opens the directory and `fsync(2)`s it (required after creating,
/// renaming, or unlinking files for the directory entry to be durable).
///
/// Windows: no-op. NTFS does not require an explicit directory-level flush;
/// file-level `FlushFileBuffers` covers durability of the directory entry as
/// well.
#[allow(unused_variables)]
pub fn fsync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let dir = File::open(path)?;
        dir.sync_all()
    }
    #[cfg(windows)]
    {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, write};

    #[test]
    fn fsync_file_succeeds_on_a_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("foo");
        write(&path, b"data").unwrap();
        let file = File::open(&path).unwrap();
        fsync_file(&file).unwrap();
    }

    #[test]
    fn fsync_dir_succeeds_on_a_real_dir() {
        let dir = tempfile::tempdir().unwrap();
        fsync_dir(dir.path()).unwrap();
    }
}
