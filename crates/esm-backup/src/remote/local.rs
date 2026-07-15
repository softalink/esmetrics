//! fs:// backend: stores each part as a file under `dir`, keyed by
//! `Part::remote_path("")`. Go: lib/backup/fslocal (as a backup destination).

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::localfs;
use crate::part::Part;

use super::RemoteFs;

pub struct LocalRemote {
    dir: PathBuf,
}

impl LocalRemote {
    pub fn new(dir: impl AsRef<Path>) -> anyhow::Result<LocalRemote> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).with_context(|| format!("create_dir_all {dir:?}"))?;
        let dir = dir
            .canonicalize()
            .with_context(|| format!("canonicalize {dir:?}"))?;
        Ok(LocalRemote { dir })
    }

    fn local_path(&self, key: &str) -> PathBuf {
        let mut p = self.dir.clone();
        for comp in key.split('/') {
            p.push(comp);
        }
        p
    }
}

impl RemoteFs for LocalRemote {
    fn describe(&self) -> String {
        format!("fs://{}", self.dir.display())
    }

    fn list_parts(&self) -> anyhow::Result<Vec<Part>> {
        let mut parts = Vec::new();
        for (path, size) in localfs::collect_files(&self.dir)? {
            let rel = localfs::canonical_rel_path(&self.dir, &path);
            if rel.ends_with(".ignore") {
                continue;
            }
            match Part::parse_from_remote_path(&rel, size) {
                Some(p) => parts.push(p),
                None => log::warn!("skipping unrecognized remote object {rel:?}"),
            }
        }
        Ok(parts)
    }

    fn delete_part(&self, p: &Part) -> anyhow::Result<()> {
        let path = self.local_path(&p.remote_path(""));
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("remove {path:?}")),
        }
    }

    fn download_part(&self, p: &Part, w: &mut dyn Write) -> anyhow::Result<()> {
        let path = self.local_path(&p.remote_path(""));
        let mut f = File::open(&path).with_context(|| format!("open {path:?}"))?;
        let n = std::io::copy(&mut f, w).with_context(|| format!("copy from {path:?}"))?;
        anyhow::ensure!(
            n == p.actual_size,
            "unexpected size downloaded for {}: got {n}, want {}",
            p.path,
            p.actual_size
        );
        Ok(())
    }

    fn upload_part(&self, p: &Part, r: &mut dyn Read) -> anyhow::Result<()> {
        let path = self.local_path(&p.remote_path(""));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create_dir_all {parent:?}"))?;
        }
        let mut f = File::create(&path).with_context(|| format!("create {path:?}"))?;
        let n = std::io::copy(&mut r.take(p.size), &mut f)
            .with_context(|| format!("copy into {path:?}"))?;
        if n != p.size {
            // Close the file before removing it so the cleanup also works
            // on platforms (e.g. Windows) that refuse to delete open files.
            drop(f);
            let _ = std::fs::remove_file(&path);
            anyhow::bail!(
                "unexpected size uploaded for {}: got {n}, want {}",
                p.path,
                p.size
            );
        }
        f.sync_all()?;
        Ok(())
    }

    fn copy_part_from(&self, src: &dyn RemoteFs, p: &Part) -> anyhow::Result<bool> {
        let Some(other) = src.as_any().downcast_ref::<LocalRemote>() else {
            return Ok(false);
        };
        let src_path = other.local_path(&p.remote_path(""));
        let dst_path = self.local_path(&p.remote_path(""));
        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create_dir_all {parent:?}"))?;
        }
        if std::fs::hard_link(&src_path, &dst_path).is_err() {
            std::fs::copy(&src_path, &dst_path)
                .with_context(|| format!("copy {src_path:?} -> {dst_path:?}"))?;
            OpenOptions::new()
                .write(true)
                .open(&dst_path)
                .with_context(|| format!("open {dst_path:?} for sync"))?
                .sync_all()?;
        }
        Ok(true)
    }

    fn remove_empty_dirs(&self) -> anyhow::Result<()> {
        localfs::remove_empty_dirs_below(&self.dir)?;
        Ok(())
    }

    fn create_file(&self, file_path: &str, data: &[u8]) -> anyhow::Result<()> {
        let path = self.local_path(file_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create_dir_all {parent:?}"))?;
        }
        let mut f = File::create(&path).with_context(|| format!("create {path:?}"))?;
        f.write_all(data)?;
        f.sync_all()?;
        Ok(())
    }

    fn delete_file(&self, file_path: &str) -> anyhow::Result<()> {
        let path = self.local_path(file_path);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("remove {path:?}")),
        }
    }

    fn has_file(&self, file_path: &str) -> anyhow::Result<bool> {
        let path = self.local_path(file_path);
        match std::fs::metadata(&path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e).with_context(|| format!("metadata {path:?}")),
        }
    }

    fn read_file(&self, file_path: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.local_path(file_path);
        std::fs::read(&path).with_context(|| format!("read {path:?}"))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
