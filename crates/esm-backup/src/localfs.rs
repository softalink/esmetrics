//! Local filesystem access for backup sources (snapshot dirs) and restore
//! destinations. Go: lib/backup/fslocal + fscommon.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::names;
use crate::part::{split_into_parts, Part};

pub struct LocalFs {
    pub dir: PathBuf,
}

impl LocalFs {
    pub fn new(dir: impl AsRef<Path>) -> LocalFs {
        LocalFs {
            dir: dir.as_ref().to_path_buf(),
        }
    }

    /// Recursively lists all files as parts, excluding the special files.
    /// Symlinks are skipped (esmetrics snapshots contain none; upstream
    /// resolves them — divergence documented in the plan).
    pub fn list_parts(&self) -> anyhow::Result<Vec<Part>> {
        let mut parts = Vec::new();
        for (path, size) in collect_files(&self.dir)? {
            let name = path
                .file_name()
                .expect("BUG: collect_files returned a path without a file name")
                .to_string_lossy();
            if is_special_file(&name) {
                continue;
            }
            let rel = canonical_rel_path(&self.dir, &path);
            parts.extend(split_into_parts(&rel, size));
        }
        Ok(parts)
    }

    fn local_path(&self, canonical: &str) -> PathBuf {
        let mut p = self.dir.clone();
        for comp in canonical.split('/') {
            p.push(comp);
        }
        p
    }

    /// Opens a reader over exactly `p.size` bytes at `p.offset`.
    pub fn open_part_reader(&self, p: &Part) -> anyhow::Result<impl Read> {
        let path = self.local_path(&p.path);
        let mut f = File::open(&path).with_context(|| format!("open {path:?}"))?;
        f.seek(SeekFrom::Start(p.offset))?;
        Ok(f.take(p.size))
    }

    /// Writes `p.size` bytes from `r` at `p.offset` of the (created if
    /// missing) destination file, growing it to `p.file_size`, then fsyncs.
    /// Direct-write mode: equivalent to upstream `-skipFilePreallocation`.
    pub fn write_part_at(&self, p: &Part, r: &mut dyn Read) -> anyhow::Result<()> {
        anyhow::ensure!(
            p.offset
                .checked_add(p.size)
                .is_some_and(|end| end <= p.file_size),
            "invalid part {}: offset={} size={} exceeds file_size={}",
            p.path,
            p.offset,
            p.size,
            p.file_size
        );
        let path = self.local_path(&p.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        // `!=` (not just `<`) so a destination file left over from a
        // previously-larger version of this path is truncated down to
        // `p.file_size` too — protects against a stale tail on files that
        // shrank past the >1GiB split threshold, whose offset-0 part key
        // otherwise matches and would never rewrite the tail on its own.
        if f.metadata()?.len() != p.file_size {
            f.set_len(p.file_size)?;
        }
        f.seek(SeekFrom::Start(p.offset))?;
        let copied = std::io::copy(&mut r.take(p.size), &mut f)?;
        anyhow::ensure!(
            copied == p.size,
            "unexpected size for part {}: got {copied}, want {}",
            p.path,
            p.size
        );
        f.sync_all()?;
        Ok(())
    }

    pub fn delete_path(&self, canonical: &str) -> anyhow::Result<()> {
        let path = self.local_path(canonical);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("remove {path:?}")),
        }
    }

    /// Removes directories left empty (special files don't keep a dir alive
    /// upstream; we only delete truly empty dirs — simpler and safe).
    pub fn remove_empty_dirs(&self) -> anyhow::Result<()> {
        remove_empty_dirs_below(&self.dir)?;
        Ok(())
    }

    pub fn cleanup_tmp_files(&self) -> anyhow::Result<()> {
        let mut tmp = Vec::new();
        collect_matching(&self.dir, &|name| name.ends_with(".tmp"), &mut tmp)?;
        for p in tmp {
            std::fs::remove_file(&p).with_context(|| format!("remove {p:?}"))?;
        }
        Ok(())
    }
}

fn is_special_file(name: &str) -> bool {
    name == names::FLOCK_FILENAME
        || name == names::RESTORE_IN_PROGRESS_FILENAME
        || name == names::RESTORE_MARK_FILENAME
        || name.ends_with(".tmp")
}

/// Recursively collects every file (symlinks skipped) under `dir`, returning
/// each file's absolute path and size. Shared by `LocalFs` and
/// `remote::LocalRemote`, which apply their own filtering and
/// path-canonicalization rules on top.
pub(crate) fn collect_files(dir: &Path) -> anyhow::Result<Vec<(PathBuf, u64)>> {
    let mut out = Vec::new();
    walk_files(dir, &mut out)?;
    Ok(out)
}

fn walk_files(dir: &Path, out: &mut Vec<(PathBuf, u64)>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {dir:?}"))? {
        let entry = entry.with_context(|| format!("read_dir entry in {dir:?}"))?;
        let path = entry.path();
        let ft = entry
            .file_type()
            .with_context(|| format!("file_type {path:?}"))?;
        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() {
            walk_files(&path, out)?;
            continue;
        }
        let size = entry
            .metadata()
            .with_context(|| format!("metadata {path:?}"))?
            .len();
        out.push((path, size));
    }
    Ok(())
}

/// `/`-joined path of `path` relative to `root`, canonicalized across
/// platforms (Windows uses `\` natively).
pub(crate) fn canonical_rel_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("BUG: path escaped its root during a directory walk")
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Depth-first removal of empty subdirectories; returns whether `dir`
/// itself ended up empty (the root is never removed by the caller).
pub(crate) fn remove_empty_dirs_below(dir: &Path) -> anyhow::Result<bool> {
    let mut empty = true;
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {dir:?}"))? {
        let entry = entry.with_context(|| format!("read_dir entry in {dir:?}"))?;
        let path = entry.path();
        if entry
            .file_type()
            .with_context(|| format!("file_type {path:?}"))?
            .is_dir()
        {
            if remove_empty_dirs_below(&path)? {
                std::fs::remove_dir(&path).with_context(|| format!("remove_dir {path:?}"))?;
            } else {
                empty = false;
            }
        } else {
            empty = false;
        }
    }
    Ok(empty)
}

fn collect_matching(
    dir: &Path,
    pred: &dyn Fn(&str) -> bool,
    out: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {dir:?}"))? {
        let entry = entry.with_context(|| format!("read_dir entry in {dir:?}"))?;
        let path = entry.path();
        if entry
            .file_type()
            .with_context(|| format!("file_type {path:?}"))?
            .is_dir()
        {
            collect_matching(&path, pred, out)?;
        } else if pred(&entry.file_name().to_string_lossy()) {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("esm-backup-localfs-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_file(dir: &std::path::Path, rel: &str, data: &[u8]) {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::File::create(p).unwrap().write_all(data).unwrap();
    }

    #[test]
    fn list_parts_walks_excludes_and_canonicalizes() {
        let dir = test_dir("list");
        write_file(
            &dir,
            "data/small/2026_07/0000000000000001/values.bin",
            b"hello",
        );
        write_file(&dir, "data/small/2026_07/parts.json", b"{}");
        write_file(&dir, "empty.bin", b"");
        write_file(&dir, "flock.lock", b"x");
        write_file(&dir, "restore-in-progress", b"x");
        write_file(&dir, "backup_restore.ignore", b"x");
        write_file(&dir, "leftover.tmp", b"x");

        let mut parts = LocalFs::new(&dir).list_parts().unwrap();
        crate::part::sort_parts(&mut parts);
        let paths: Vec<&str> = parts.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "data/small/2026_07/0000000000000001/values.bin",
                "data/small/2026_07/parts.json",
                "empty.bin",
            ]
        );
        assert_eq!(parts[0].size, 5);
        assert_eq!(parts[2].size, 0); // zero-length file preserved
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn part_reader_respects_offset_and_size() {
        let dir = test_dir("reader");
        write_file(&dir, "f.bin", b"0123456789");
        let fs = LocalFs::new(&dir);
        let p = Part {
            path: "f.bin".into(),
            file_size: 10,
            offset: 3,
            size: 4,
            actual_size: 4,
        };
        let mut out = Vec::new();
        fs.open_part_reader(&p)
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, b"3456");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_part_at_assembles_file_out_of_order() {
        let dir = test_dir("writer");
        let fs = LocalFs::new(&dir);
        let mk = |offset, size| Part {
            path: "out/f.bin".into(),
            file_size: 10,
            offset,
            size,
            actual_size: size,
        };
        fs.write_part_at(&mk(5, 5), &mut &b"56789"[..]).unwrap();
        fs.write_part_at(&mk(0, 5), &mut &b"01234"[..]).unwrap();
        assert_eq!(std::fs::read(dir.join("out/f.bin")).unwrap(), b"0123456789");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_part_at_truncates_stale_tail_when_file_shrank() {
        let dir = test_dir("writer-shrink");
        // Pre-create a file longer than the new part's file_size, simulating
        // a destination left over from a previously-larger version of this
        // path (e.g. a >1GiB file that shrank, whose offset-0 part key still
        // matches).
        write_file(&dir, "out/f.bin", b"01234567890123456789"); // 20 bytes
        let fs = LocalFs::new(&dir);
        let p = Part {
            path: "out/f.bin".into(),
            file_size: 10,
            offset: 0,
            size: 10,
            actual_size: 10,
        };
        fs.write_part_at(&p, &mut &b"abcdefghij"[..]).unwrap();
        let got = std::fs::read(dir.join("out/f.bin")).unwrap();
        assert_eq!(got.len(), 10);
        assert_eq!(got, b"abcdefghij");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_part_at_rejects_out_of_bounds_part() {
        let dir = test_dir("writer-oob");
        let fs = LocalFs::new(&dir);
        let p = Part {
            path: "out/f.bin".into(),
            file_size: 10,
            offset: 8,
            size: 5,
            actual_size: 5,
        };
        let result = fs.write_part_at(&p, &mut &b"01234"[..]);
        assert!(result.is_err());
        assert!(!dir.join("out/f.bin").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_path_and_remove_empty_dirs() {
        let dir = test_dir("delete");
        write_file(&dir, "a/b/c.bin", b"x");
        let fs = LocalFs::new(&dir);
        fs.delete_path("a/b/c.bin").unwrap();
        fs.remove_empty_dirs().unwrap();
        assert!(!dir.join("a").exists());
        assert!(dir.exists()); // root survives
        let _ = std::fs::remove_dir_all(&dir);
    }
}
