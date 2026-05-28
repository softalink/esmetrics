//! `esm-backup` — Phase 8 MVP.
//!
//! Local-filesystem snapshot + restore. Object-storage backends (S3, GCS,
//! Azure) and incremental manifests land in subsequent sub-phases; the MVP
//! today is a `cp -r`-style copy with a manifest that records the source
//! mtime+size for every file. Bidirectional compatibility with VM's
//! `vmbackup` / `vmrestore` directory layout (ADR-001 #12) lands when we
//! reverse-engineer that format.

#![allow(clippy::print_stderr)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::items_after_statements)]

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(name = "esm-backup", about = "Snapshot + restore for an EsMetrics data dir.", version)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Copy `--src` into `--dst`, writing a manifest file at `<dst>/MANIFEST.json`.
    /// If `--prev` is provided, files already present (matching size) in the
    /// previous backup directory are hardlinked rather than re-copied — this
    /// gives an incremental backup chain at the cost of a stable file-tree.
    Backup {
        #[arg(long)]
        src: PathBuf,
        #[arg(long)]
        dst: PathBuf,
        /// Optional previous backup directory for incremental dedup.
        #[arg(long)]
        prev: Option<PathBuf>,
    },
    /// Restore from `--src` (a directory produced by `backup`) into `--dst`.
    Restore {
        #[arg(long)]
        src: PathBuf,
        #[arg(long)]
        dst: PathBuf,
    },
    /// Trigger `/snapshot/create` on a running esm-single, then back up the
    /// resulting snapshot directory. Requires the URL of the server and the
    /// data dir path the server is using.
    Snapshot {
        #[arg(long)]
        server_url: String,
        #[arg(long)]
        storage_data_path: PathBuf,
        #[arg(long)]
        dst: PathBuf,
        #[arg(long)]
        prev: Option<PathBuf>,
        /// If set, delete the snapshot on the server after backup completes.
        #[arg(long)]
        cleanup: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    /// Backup format version. Bumped on any incompatible change.
    schema_version: u32,
    /// Files in the backup, keyed by path relative to the backup root.
    files: BTreeMap<String, FileEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileEntry {
    size_bytes: u64,
    /// Hex-encoded SHA-256 of the file contents. Not yet computed in the MVP
    /// (we use byte-length as the integrity check); reserved for the
    /// upcoming object-storage backend where verification is essential.
    sha256: Option<String>,
}

const SCHEMA_VERSION: u32 = 1;
const MANIFEST_NAME: &str = "MANIFEST.json";

/// vmbackup-compatible marker file names. Names must match
/// `lib/backup/backupnames/backupnames.go` in upstream VictoriaMetrics so a
/// VM-side `vmrestore` recognises our backup directory.
const VMBACKUP_COMPLETE_NAME: &str = "backup_complete.ignore";
const VMBACKUP_METADATA_NAME: &str = "backup_metadata.ignore";
const VM_RESTORE_IN_PROGRESS_NAME: &str = "restore-in-progress";

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct VmBackupMetadata {
    created_at: String,
    completed_at: String,
}

fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Cmd::Backup { src, dst, prev } => backup(&src, &dst, prev.as_deref()),
        Cmd::Restore { src, dst } => restore(&src, &dst),
        Cmd::Snapshot { server_url, storage_data_path, dst, prev, cleanup } => {
            snapshot_and_backup(&server_url, &storage_data_path, &dst, prev.as_deref(), cleanup)
        }
    }
}

fn init_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .finish();
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("warning: tracing init failed: {e}");
    }
}

fn backup(src: &Path, dst: &Path, prev: Option<&Path>) -> Result<()> {
    if !src.exists() {
        bail!("source path does not exist: {}", src.display());
    }
    if dst.exists() {
        bail!("destination path already exists: {}", dst.display());
    }
    std::fs::create_dir_all(dst).with_context(|| format!("create {}", dst.display()))?;

    let prev_manifest = prev
        .map(|p| load_manifest(p).with_context(|| format!("load prev manifest {}", p.display())))
        .transpose()?;
    let mut manifest = Manifest { schema_version: SCHEMA_VERSION, files: BTreeMap::new() };
    copy_tree_with_dedup(src, dst, src, &mut manifest, prev, prev_manifest.as_ref())?;

    let manifest_path = dst.join(MANIFEST_NAME);
    let bytes = serde_json::to_vec_pretty(&manifest).context("serialize manifest")?;
    std::fs::write(&manifest_path, bytes)
        .with_context(|| format!("write {}", manifest_path.display()))?;

    write_vmbackup_markers(dst).context("write vmbackup markers")?;

    tracing::info!(files = manifest.files.len(), "backup complete");
    Ok(())
}

fn write_vmbackup_markers(dst: &Path) -> Result<()> {
    // Write `backup_metadata.ignore` first; `backup_complete.ignore` is
    // touched last so that an interrupted backup never advertises completion.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let now_rfc3339 = rfc3339_utc(now);
    let metadata = VmBackupMetadata { created_at: now_rfc3339.clone(), completed_at: now_rfc3339 };
    let metadata_path = dst.join(VMBACKUP_METADATA_NAME);
    std::fs::write(&metadata_path, serde_json::to_vec(&metadata)?)
        .with_context(|| format!("write {}", metadata_path.display()))?;
    let complete_path = dst.join(VMBACKUP_COMPLETE_NAME);
    std::fs::write(&complete_path, b"")
        .with_context(|| format!("write {}", complete_path.display()))?;
    Ok(())
}

/// Format `unix_secs` as RFC3339 UTC (e.g. `2026-05-28T17:32:01Z`) without
/// pulling in `chrono`.
fn rfc3339_utc(unix_secs: u64) -> String {
    // Days-since-epoch algorithm: Howard Hinnant.
    let secs_per_day: u64 = 86_400;
    let days = (unix_secs / secs_per_day) as i64;
    let time_of_day = unix_secs % secs_per_day;
    let h = time_of_day / 3600;
    let m = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}Z")
}

#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_sign_loss)]
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn copy_tree_with_dedup(
    src_root: &Path,
    dst_root: &Path,
    current: &Path,
    manifest: &mut Manifest,
    prev_root: Option<&Path>,
    prev_manifest: Option<&Manifest>,
) -> Result<()> {
    let entries =
        std::fs::read_dir(current).with_context(|| format!("read_dir {}", current.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        // Skip the data-directory lock file; it's process-state, not data.
        if path.file_name().and_then(|s| s.to_str()) == Some(".lock") {
            continue;
        }
        let relative = path
            .strip_prefix(src_root)
            .with_context(|| format!("strip {} from {}", src_root.display(), path.display()))?;
        let dst_path = dst_root.join(relative);
        if file_type.is_dir() {
            std::fs::create_dir_all(&dst_path)
                .with_context(|| format!("mkdir {}", dst_path.display()))?;
            copy_tree_with_dedup(src_root, dst_root, &path, manifest, prev_root, prev_manifest)?;
        } else if file_type.is_file() {
            let key = relative.to_string_lossy().into_owned();
            let src_meta = std::fs::metadata(&path)?;
            let mut linked = false;
            if let (Some(prev_root), Some(prev_manifest)) = (prev_root, prev_manifest)
                && let Some(prev_entry) = prev_manifest.files.get(&key)
                && prev_entry.size_bytes == src_meta.len()
            {
                let prev_file = prev_root.join(relative);
                if prev_file.exists() && std::fs::hard_link(&prev_file, &dst_path).is_ok() {
                    linked = true;
                }
            }
            if !linked {
                std::fs::copy(&path, &dst_path).with_context(|| {
                    format!("copy {} -> {}", path.display(), dst_path.display())
                })?;
            }
            let metadata = std::fs::metadata(&dst_path)?;
            let sha = sha256_hex_of(&dst_path)?;
            manifest.files.insert(key, FileEntry { size_bytes: metadata.len(), sha256: Some(sha) });
        }
        // Symlinks intentionally not followed (Windows quirks; VM's parts
        // never use them).
    }
    Ok(())
}

fn sha256_hex_of(path: &Path) -> Result<String> {
    use sha2::Digest as _;
    let bytes =
        std::fs::read(path).with_context(|| format!("read {} for sha256", path.display()))?;
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();
    Ok(hex_encode(&digest))
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn load_manifest(dir: &Path) -> Result<Manifest> {
    let manifest_path = dir.join(MANIFEST_NAME);
    let raw = std::fs::read(&manifest_path)
        .with_context(|| format!("read {}", manifest_path.display()))?;
    serde_json::from_slice(&raw).context("parse manifest")
}

fn snapshot_and_backup(
    server_url: &str,
    storage_data_path: &Path,
    dst: &Path,
    prev: Option<&Path>,
    cleanup: bool,
) -> Result<()> {
    let resp =
        ureq_blocking_post(&format!("{}/snapshot/create", server_url.trim_end_matches('/')))?;
    let parsed: serde_json::Value =
        serde_json::from_str(&resp).context("parse snapshot response")?;
    let name = parsed
        .get("snapshot")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("snapshot response missing `snapshot`: {resp}"))?;
    let snap_dir = storage_data_path.join("snapshots").join(name);
    tracing::info!(snapshot = %name, dir = %snap_dir.display(), "backing up snapshot");
    backup(&snap_dir, dst, prev)?;
    if cleanup {
        let _ = ureq_blocking_post(&format!(
            "{}/snapshot/delete/{}",
            server_url.trim_end_matches('/'),
            name
        ));
    }
    Ok(())
}

fn ureq_blocking_post(url: &str) -> Result<String> {
    // Minimal blocking HTTP POST using std::net + a tiny hand-rolled HTTP/1.1
    // request to avoid adding reqwest as a runtime dependency for esm-backup.
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let url = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("only http:// URLs supported in esm-backup (got {url})"))?;
    let (authority, path) = url.split_once('/').map_or((url, ""), |(a, p)| (a, p));
    let path = format!("/{path}");
    let mut stream =
        TcpStream::connect(authority).with_context(|| format!("connect {authority}"))?;
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).context("write request")?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).context("read response")?;
    let body_start =
        raw.find("\r\n\r\n").ok_or_else(|| anyhow::anyhow!("malformed HTTP response: {raw}"))? + 4;
    Ok(raw[body_start..].to_string())
}

fn restore(src: &Path, dst: &Path) -> Result<()> {
    if !src.exists() {
        bail!("source path does not exist: {}", src.display());
    }
    // Refuse to restore from a backup directory missing the vmbackup
    // completion marker — `vmbackup` writes this last, so a missing marker
    // means the backup was interrupted.
    let complete_marker = src.join(VMBACKUP_COMPLETE_NAME);
    let our_manifest = src.join(MANIFEST_NAME);
    if !complete_marker.exists() && !our_manifest.exists() {
        bail!(
            "backup at {} is missing both {} (vmbackup) and {} (esm); refusing to restore from incomplete backup",
            src.display(),
            VMBACKUP_COMPLETE_NAME,
            MANIFEST_NAME,
        );
    }
    if dst.exists() {
        bail!("destination path already exists: {}", dst.display());
    }
    std::fs::create_dir_all(dst).with_context(|| format!("create {}", dst.display()))?;

    // Drop a `restore-in-progress` marker so a concurrent reader knows the
    // tree is incomplete. We remove it last, after the manifest verifies.
    let in_progress_marker = dst.join(VM_RESTORE_IN_PROGRESS_NAME);
    std::fs::write(&in_progress_marker, b"")
        .with_context(|| format!("write {}", in_progress_marker.display()))?;

    let restored_files = if our_manifest.exists() {
        restore_from_manifest(src, dst, &our_manifest)?
    } else {
        // vmbackup-only directory — copy everything except marker files.
        restore_tree_excluding_markers(src, dst)?
    };

    std::fs::remove_file(&in_progress_marker)
        .with_context(|| format!("remove {}", in_progress_marker.display()))?;
    tracing::info!(files = restored_files, "restore complete");
    Ok(())
}

fn restore_from_manifest(src: &Path, dst: &Path, manifest_path: &Path) -> Result<u64> {
    let raw = std::fs::read(manifest_path)
        .with_context(|| format!("read {}", manifest_path.display()))?;
    let manifest: Manifest = serde_json::from_slice(&raw).context("parse manifest")?;
    if manifest.schema_version != SCHEMA_VERSION {
        bail!(
            "manifest schema version {} is not supported; need {}",
            manifest.schema_version,
            SCHEMA_VERSION
        );
    }
    let mut restored_files = 0u64;
    for (relative_str, entry) in &manifest.files {
        let src_file = src.join(relative_str);
        let dst_file = dst.join(relative_str);
        if let Some(parent) = dst_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&src_file, &dst_file)
            .with_context(|| format!("restore {} -> {}", src_file.display(), dst_file.display()))?;
        let actual = std::fs::metadata(&dst_file)?.len();
        if actual != entry.size_bytes {
            bail!(
                "size mismatch for {}: manifest says {}, got {}",
                relative_str,
                entry.size_bytes,
                actual
            );
        }
        if let Some(expected_sha) = &entry.sha256 {
            let actual_sha = sha256_hex_of(&dst_file)?;
            if &actual_sha != expected_sha {
                bail!(
                    "sha256 mismatch for {relative_str}: manifest={expected_sha}, restored={actual_sha}"
                );
            }
        }
        restored_files += 1;
    }
    Ok(restored_files)
}

fn restore_tree_excluding_markers(src: &Path, dst: &Path) -> Result<u64> {
    let mut count = 0u64;
    fn walk(src_root: &Path, dst_root: &Path, current: &Path, count: &mut u64) -> Result<()> {
        for entry in std::fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if matches!(
                name,
                VMBACKUP_COMPLETE_NAME | VMBACKUP_METADATA_NAME | "backup_locked.ignore"
            ) {
                continue;
            }
            let relative = path.strip_prefix(src_root)?;
            let dst_path = dst_root.join(relative);
            let ft = entry.file_type()?;
            if ft.is_dir() {
                std::fs::create_dir_all(&dst_path)?;
                walk(src_root, dst_root, &path, count)?;
            } else if ft.is_file() {
                if let Some(parent) = dst_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::copy(&path, &dst_path)?;
                *count += 1;
            }
        }
        Ok(())
    }
    walk(src, dst, src, &mut count)?;
    Ok(count)
}

#[allow(dead_code)]
fn ensure_dir_exists(path: &Path) -> io::Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}
