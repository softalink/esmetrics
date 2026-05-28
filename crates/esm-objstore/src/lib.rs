//! Object-storage abstraction for backups and (future) tiered storage.
//!
//! Defines [`ObjectStore`] — the minimal `put` / `get` / `list` /
//! `delete` surface esm-backup needs. The local filesystem backend
//! ([`LocalFsStore`]) is shipped today; S3, GCS, and Azure are deferred
//! to the post-MVP phase. The trait shape is taken from the
//! `object_store` crate so a future drop-in is straightforward.

#![allow(clippy::missing_errors_doc)]

use std::path::{Path, PathBuf};

use thiserror::Error;

/// A blocking object-storage handle. All methods are synchronous because
/// esm-backup is itself a CLI; an async variant lives behind a feature
/// gate when first needed.
pub trait ObjectStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), ObjectStoreError>;
    fn get(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError>;
    fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;
    fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
}

/// Filesystem backend rooted at a single directory.
#[derive(Debug, Clone)]
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, ObjectStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(ObjectStoreError::Io)?;
        Ok(Self { root })
    }

    fn key_path(&self, key: &str) -> PathBuf {
        // Keys can contain `/`; we mirror them as directory structure.
        self.root.join(key)
    }
}

impl ObjectStore for LocalFsStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), ObjectStoreError> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(ObjectStoreError::Io)?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, bytes).map_err(ObjectStoreError::Io)?;
        std::fs::rename(&tmp, &path).map_err(ObjectStoreError::Io)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        std::fs::read(self.key_path(key)).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => ObjectStoreError::NotFound(key.to_string()),
            _ => ObjectStoreError::Io(e),
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let mut out = Vec::new();
        walk(&self.root, &self.root, &mut out)?;
        Ok(out.into_iter().filter(|k| k.starts_with(prefix)).collect())
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match std::fs::remove_file(self.key_path(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ObjectStoreError::Io(e)),
        }
    }
}

fn walk(root: &Path, current: &Path, out: &mut Vec<String>) -> Result<(), ObjectStoreError> {
    for entry in std::fs::read_dir(current).map_err(ObjectStoreError::Io)? {
        let entry = entry.map_err(ObjectStoreError::Io)?;
        let path = entry.path();
        let ft = entry.file_type().map_err(ObjectStoreError::Io)?;
        if ft.is_file()
            && let Ok(rel) = path.strip_prefix(root)
            && let Some(s) = rel.to_str()
        {
            out.push(s.replace('\\', "/"));
        } else if ft.is_dir() {
            walk(root, &path, out)?;
        }
    }
    Ok(())
}

/// Parse a backup target URL into a backend handle. Supported schemes:
/// `file://path` (LocalFs). `s3://`, `gs://`, `azure://` return
/// [`ObjectStoreError::UnsupportedScheme`] until the cloud backends land.
pub fn open_target(url: &str) -> Result<Box<dyn ObjectStore>, ObjectStoreError> {
    if let Some(path) = url.strip_prefix("file://") {
        return Ok(Box::new(LocalFsStore::open(path)?));
    }
    if url.starts_with("s3://") || url.starts_with("gs://") || url.starts_with("azure://") {
        let scheme = url.split_once("://").map_or(url, |(s, _)| s).to_string();
        return Err(ObjectStoreError::UnsupportedScheme(scheme));
    }
    Ok(Box::new(LocalFsStore::open(url)?))
}

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("scheme {0:?} not yet supported (S3 / GCS / Azure backends land post-MVP)")]
    UnsupportedScheme(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_put_get_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFsStore::open(tmp.path().join("store")).unwrap();
        store.put("a/b/c", b"hello").unwrap();
        assert_eq!(store.get("a/b/c").unwrap(), b"hello");
        let mut keys = store.list("").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a/b/c".to_string()]);
        store.delete("a/b/c").unwrap();
        assert!(matches!(store.get("a/b/c"), Err(ObjectStoreError::NotFound(_))));
    }

    #[test]
    fn cloud_schemes_explicitly_unsupported() {
        let r = open_target("s3://bucket/prefix");
        assert!(matches!(r, Err(ObjectStoreError::UnsupportedScheme(_))));
    }
}
