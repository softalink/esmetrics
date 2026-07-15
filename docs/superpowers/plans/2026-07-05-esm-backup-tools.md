# esm-backup + esbackup/esrestore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port VictoriaMetrics `lib/backup` + `app/vmbackup` + `app/vmrestore` to Rust as a new `esm-backup` crate with two binaries, `esbackup` and `esrestore`, supporting `fs://`, `s3://`, `gs://`, `azblob://` destinations.

**Architecture:** `esm-backup` is a **sync** library. A `RemoteFs` trait has two impls: `LocalRemote` (native `std::fs`, hard-link server-side copy — used for `fs://` and all tests) and `ObjectRemote` (wraps `object_store` 0.14; a private tokio runtime is confined inside this one module — the rest of the workspace stays async-free). Backup = three-way part diff (src/dst/origin) with delete → server-side copy → upload → completion marker, identical ordering to upstream. Restore = rsync-style diff with direct-at-offset writes (upstream's `-skipFilePreallocation` mode). **Depends on the snapshots plan** (`2026-07-05-storage-snapshots.md`) being implemented first.

**Tech Stack:** Rust 1.85, `object_store = "0.14"` (features `aws`,`gcp`,`azure`), `tokio` (rt-multi-thread, esm-backup only), `futures`, `bytes`, `anyhow`, `reqwest` (blocking, for `-snapshot.createURL`), existing `esm-common` fs helpers.

## Global Constraints

- Rust edition 2021, `rust-version = "1.85"`; workspace at `/home/test/esmetrics`.
- tokio/async may appear **only inside `crates/esm-backup`**; the public API of every esm-backup module is synchronous. No async in any other crate.
- Every crate must pass `cargo clippy -- -D warnings` and `cargo check` on `x86_64-unknown-linux-gnu` **and** `x86_64-pc-windows-gnu`. **Contingency:** if `object_store`'s default crypto (`aws-lc-rs`) fails to build on `x86_64-pc-windows-gnu`, gate `ObjectRemote` + the object_store/reqwest deps behind a default-on cargo feature `cloud` in esm-backup, keep `fs://` always available, and check `-p esm-backup --no-default-features` on the Windows target; document the limitation in the README section. Do not sink more than one attempt into fixing the crypto build itself.
- Remote object layout must match upstream byte-for-byte so semantics carry over: part objects at `<prefix>/<Path>/<FILE_SIZE>_<OFFSET>_<SIZE>` (each `%016X` uppercase hex), markers `backup_complete.ignore` / `backup_metadata.ignore` at `<prefix>/`.
- `MAX_PART_SIZE = 1 GiB` (1024×1024×1024); files split into ≤1 GiB parts; zero-length file = one `{offset:0, size:0}` part.
- Part identity key = `(path, offset, size, actual_size)`; any file whose path ends in `/parts.json` (or equals `parts.json`) gets a per-call unique key so it is always re-copied (upstream issue #5005 — parts.json is mutable).
- Local listing excludes: `flock.lock`, `restore-in-progress`, `backup_restore.ignore`, `*.tmp`. Remote listing excludes `*.ignore`.
- Binary names: `esbackup`, `esrestore` (repo naming convention, mirroring `victoria-metrics`→`esmetrics`).
- Commit after each task, conventional format, no attribution footer.

## Reference: upstream + repo facts (verified)

- Upstream `lib/backup/actions/backup.go:58` ordering: `dst.DeleteFile(backup_complete.ignore)` **first**; diff-delete; server-side copy of `intersect(origin, to_copy)`; upload `difference(to_copy, origin)`; write `backup_metadata.ignore` `{created_at, completed_at}` (RFC3339); `dst.CreateFile(backup_complete.ignore, nil)` **last**.
- Upstream `lib/backup/actions/restore.go:53` ordering: mkdir + flock + create `restore-in-progress`; check `backup_complete.ignore` (unless skipped); cleanup `*.tmp`; list src; **path-traversal guard**; **contiguity validation** (per path sorted by offset: no gaps/overlaps, `size == actual_size`, total == `file_size`); diff-delete local files whose **offset-0 part** is in the delete set; re-list; group by path and download missing parts; remove `restore-in-progress`.
- Upstream `lib/storage/storage.go:222`: `MustOpenStorage` panics if `restore-in-progress` exists in the data path.
- `crates/esm-common/src/fs.rs`: `must_create_flock_file(dir) -> File` (385), `must_hard_link_files` (221), `must_mkdir_if_not_exist` (152), `is_path_exist` (190), `must_remove_dir` (561).
- Workspace `Cargo.toml`: members list to extend; `[workspace.dependencies]` table to extend. `anyhow`, `serde`, `serde_json`, `log`, `env_logger` already present.
- `object_store` 0.14: import `ObjectStoreExt` for `put/get/head/delete/copy`; `list` returns a futures `BoxStream`; `WriteMultipart` for streaming uploads; builders `AmazonS3Builder::from_env()` / `GoogleCloudStorageBuilder::from_env()` / `MicrosoftAzureBuilder::from_env()` (env-driven credentials; `parse_url` does NOT read env — don't use it). A store instance is bucket-scoped: server-side `copy` only works within one bucket.
- esmetrics test harness: `crates/esmetrics/tests/server_test.rs` — `test_flags()`, `http_get(addr, target)`; `esmetrics::run(&flags)` returns a server handle with `.local_addr()` / `.stop()`.

## File Structure

```
crates/esm-backup/
├── Cargo.toml
├── src/
│   ├── lib.rs            # pub mods + run_parallel helper
│   ├── names.rs          # marker filename constants
│   ├── timeutil.rs       # unix→RFC3339 + snapshot-name→RFC3339 (no chrono)
│   ├── part.rs           # Part, key, remote path codec, diff/intersect
│   ├── localfs.rs        # LocalFs: snapshot-source listing/reading + restore writes
│   ├── remote/
│   │   ├── mod.rs        # RemoteFs trait + new_remote_fs(url) factory
│   │   ├── local.rs      # LocalRemote (fs://)
│   │   └── object.rs     # ObjectRemote (s3/gs/azblob via object_store)
│   ├── backup.rs         # Backup action
│   ├── restore.rs        # Restore action
│   ├── cliflags.rs       # minimal Go-style -flag parser shared by both bins
│   └── bin/
│       ├── esbackup.rs
│       └── esrestore.rs
└── tests/
    └── backup_restore_test.rs   # fs:// round-trip + incremental + guards
crates/esm-storage/src/storage/mod.rs   # restore-in-progress startup guard
crates/esmetrics/tests/backup_e2e_test.rs # server→snapshot→backup→restore→serve
```

---

### Task 1: Crate scaffold, names, timeutil, Part model

**Files:**
- Modify: `Cargo.toml` (workspace: add member + deps)
- Create: `crates/esm-backup/Cargo.toml`, `src/lib.rs`, `src/names.rs`, `src/timeutil.rs`, `src/part.rs`

**Interfaces (produced, consumed by every later task):**
- `names::{BACKUP_COMPLETE_FILENAME, BACKUP_METADATA_FILENAME, RESTORE_IN_PROGRESS_FILENAME, RESTORE_MARK_FILENAME, FLOCK_FILENAME}`
- `timeutil::{rfc3339_from_unix(secs: u64) -> String, rfc3339_from_snapshot_name(name: &str) -> Option<String>, now_rfc3339() -> String}`
- `part::Part { path: String, file_size: u64, offset: u64, size: u64, actual_size: u64 }` with `key(&self) -> String`, `remote_path(&self, prefix: &str) -> String`, `parse_from_remote_path(remote: &str, actual_size: u64) -> Option<Part>` (caller strips the prefix first), `sort_parts(&mut [Part])`, `parts_difference(a, b) -> Vec<Part>`, `parts_intersect(a, b) -> Vec<Part>`, `MAX_PART_SIZE: u64`, `split_into_parts(path: &str, file_size: u64) -> Vec<Part>`
- `lib.rs`: `pub fn run_parallel<T: Sync>(items: &[T], concurrency: usize, f: impl Fn(&T) -> anyhow::Result<()> + Sync) -> anyhow::Result<()>`

- [ ] **Step 1: Workspace wiring**

Root `Cargo.toml`: add `"crates/esm-backup"` to `[workspace.members]`. Add to `[workspace.dependencies]`:

```toml
object_store = { version = "0.14", features = ["aws", "gcp", "azure"] }
tokio = { version = "1", features = ["rt-multi-thread"] }
futures = "0.3"
bytes = "1"
reqwest = { version = "0.12", default-features = false, features = ["blocking", "rustls-tls"] }
```

Create `crates/esm-backup/Cargo.toml` (copy `[package]` boilerplate style from `crates/esm-storage/Cargo.toml` — workspace version/edition/rust-version):

```toml
[package]
name = "esm-backup"
# version/edition/rust-version: match how sibling crates inherit from workspace

[dependencies]
esm-common = { path = "../esm-common" }
anyhow.workspace = true
log.workspace = true
serde.workspace = true
serde_json.workspace = true
env_logger.workspace = true
object_store.workspace = true
tokio.workspace = true
futures.workspace = true
bytes.workspace = true
reqwest.workspace = true

[[bin]]
name = "esbackup"
path = "src/bin/esbackup.rs"

[[bin]]
name = "esrestore"
path = "src/bin/esrestore.rs"
```

- [ ] **Step 2: names.rs + timeutil.rs (with unit tests)**

`src/names.rs`:

```rust
//! Reserved file names. Go: lib/backup/backupnames.

/// Written to the destination as the LAST step of a backup; its presence
/// means the backup is complete and valid.
pub const BACKUP_COMPLETE_FILENAME: &str = "backup_complete.ignore";
/// JSON `{created_at, completed_at}` written just before the complete marker.
pub const BACKUP_METADATA_FILENAME: &str = "backup_metadata.ignore";
/// Created locally at restore start, removed on success. esm-storage
/// refuses to open a data dir containing it.
pub const RESTORE_IN_PROGRESS_FILENAME: &str = "restore-in-progress";
/// Reserved by upstream backupmanager; excluded from local listings.
pub const RESTORE_MARK_FILENAME: &str = "backup_restore.ignore";
/// esm-storage's exclusive-lock file; never backed up.
pub const FLOCK_FILENAME: &str = "flock.lock";
```

`src/timeutil.rs`: RFC3339 formatting without a date-time dependency. Reuse the days-from-civil algorithm from the snapshots plan (`crates/esm-storage/src/snapshot.rs` has `format_compact_timestamp`; esm-backup cannot see it — duplicate the ~20-line civil algorithm here, it's the sanctioned exception to DRY across crate boundaries for a pure function):

```rust
//! Minimal UTC time formatting (no chrono dependency).

/// Formats a unix timestamp as RFC3339, e.g. "2026-07-05T12:34:56Z".
pub fn rfc3339_from_unix(unix_secs: u64) -> String {
    let (y, mo, d, h, mi, s) = civil_from_unix(unix_secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

pub fn now_rfc3339() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    rfc3339_from_unix(secs)
}

/// Converts a snapshot name (`YYYYMMDDhhmmss-XXXXXXXX`, UTC) to RFC3339.
pub fn rfc3339_from_snapshot_name(name: &str) -> Option<String> {
    let (ts, _) = name.split_once('-')?;
    if ts.len() != 14 || !ts.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(format!(
        "{}-{}-{}T{}:{}:{}Z",
        &ts[0..4], &ts[4..6], &ts[6..8], &ts[8..10], &ts[10..12], &ts[12..14]
    ))
}

fn civil_from_unix(unix_secs: u64) -> (i64, u64, u64, u64, u64, u64) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, d, h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_known_values() {
        assert_eq!(rfc3339_from_unix(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_from_unix(1_783_082_096), "2026-07-05T12:34:56Z");
    }

    #[test]
    fn snapshot_name_to_rfc3339() {
        assert_eq!(
            rfc3339_from_snapshot_name("20260705123456-0000000A").as_deref(),
            Some("2026-07-05T12:34:56Z")
        );
        assert_eq!(rfc3339_from_snapshot_name("garbage"), None);
    }
}
```

- [ ] **Step 3: part.rs with failing-first unit tests**

`src/part.rs` (write the `#[cfg(test)]` module FIRST, run to see red, then the impl):

```rust
//! Backup part model. Go: lib/backup/common/part.go

use std::sync::atomic::{AtomicU64, Ordering};

/// Files bigger than this are split into multiple parts.
pub const MAX_PART_SIZE: u64 = 1024 * 1024 * 1024;

/// A contiguous piece of a file, the unit of backup transfer/diffing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    /// Canonical file path relative to the backup root, `/`-separated.
    pub path: String,
    /// Size of the whole file this part belongs to.
    pub file_size: u64,
    /// Offset of this part within the file.
    pub offset: u64,
    /// Expected part length.
    pub size: u64,
    /// Observed length (differs from `size` for broken/partial remote parts).
    pub actual_size: u64,
}

impl Part {
    /// Identity for set-diffing. `file_size` is deliberately excluded so
    /// partially-restored files resume correctly (upstream comment).
    /// Mutable files (`parts.json`) get a unique key so they always re-copy.
    pub fn key(&self) -> String {
        if self.path.ends_with("/parts.json") || self.path == "parts.json" {
            static UNIQUE: AtomicU64 = AtomicU64::new(0);
            return format!("unique-{:016X}", UNIQUE.fetch_add(1, Ordering::Relaxed));
        }
        format!(
            "{}#{:016X}#{:016X}#{:016X}",
            self.path, self.offset, self.size, self.actual_size
        )
    }

    /// Remote object key: `<prefix>/<path>/<FILE_SIZE>_<OFFSET>_<SIZE>`.
    pub fn remote_path(&self, prefix: &str) -> String {
        let prefix = prefix.trim_end_matches('/');
        let sep = if prefix.is_empty() { "" } else { "/" };
        format!(
            "{prefix}{sep}{}/{:016X}_{:016X}_{:016X}",
            self.path, self.file_size, self.offset, self.size
        )
    }

    /// Inverse of `remote_path`. `remote` must already be prefix-stripped.
    pub fn parse_from_remote_path(remote: &str, actual_size: u64) -> Option<Part> {
        let (path, name) = remote.rsplit_once('/')?;
        let fields: Vec<&str> = name.split('_').collect();
        if path.is_empty() || fields.len() != 3 || fields.iter().any(|f| f.len() != 16) {
            return None;
        }
        let parse = |s: &str| u64::from_str_radix(s, 16).ok();
        Some(Part {
            path: path.to_string(),
            file_size: parse(fields[0])?,
            offset: parse(fields[1])?,
            size: parse(fields[2])?,
            actual_size,
        })
    }
}

/// Splits a file into `<= MAX_PART_SIZE` parts. Zero-length files produce a
/// single empty part so they are preserved by backup/restore.
pub fn split_into_parts(path: &str, file_size: u64) -> Vec<Part> {
    let mut parts = Vec::new();
    let mut offset = 0u64;
    loop {
        let n = (file_size - offset).min(MAX_PART_SIZE);
        parts.push(Part {
            path: path.to_string(),
            file_size,
            offset,
            size: n,
            actual_size: n,
        });
        offset += n;
        if offset >= file_size {
            return parts;
        }
    }
}

pub fn sort_parts(parts: &mut [Part]) {
    parts.sort_by(|a, b| a.path.cmp(&b.path).then(a.offset.cmp(&b.offset)));
}

/// Returns parts present in `a` but missing from `b` (a \ b).
pub fn parts_difference(a: &[Part], b: &[Part]) -> Vec<Part> {
    let keys: std::collections::HashSet<String> = b.iter().map(Part::key).collect();
    a.iter().filter(|p| !keys.contains(&p.key())).cloned().collect()
}

/// Returns parts present in both `a` and `b`.
pub fn parts_intersect(a: &[Part], b: &[Part]) -> Vec<Part> {
    let keys: std::collections::HashSet<String> = a.iter().map(Part::key).collect();
    b.iter().filter(|p| keys.contains(&p.key())).cloned().collect()
}
```

**Bug to avoid (spotted in self-review, fix is in the code above but the tests must pin it):** `key()` returning a *fresh* unique value per call means a `parts.json` part is never equal to anything — including itself — so `parts_difference(dst, src)` marks the old `parts.json` for deletion and `parts_difference(src, dst)` re-uploads the new one. That's the intended upstream behavior; the tests below encode it.

Unit tests in the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn part(path: &str, offset: u64, size: u64) -> Part {
        Part { path: path.into(), file_size: 4096, offset, size, actual_size: size }
    }

    #[test]
    fn remote_path_roundtrip() {
        let p = part("data/small/2026_07/0000000000000001/values.bin", 0, 4096);
        let rp = p.remote_path("base/dir");
        assert_eq!(
            rp,
            "base/dir/data/small/2026_07/0000000000000001/values.bin/\
             0000000000001000_0000000000000000_0000000000001000"
        );
        let stripped = rp.strip_prefix("base/dir/").unwrap();
        assert_eq!(Part::parse_from_remote_path(stripped, 4096).unwrap(), p);
    }

    #[test]
    fn parse_rejects_malformed() {
        assert!(Part::parse_from_remote_path("no-slash", 0).is_none());
        assert!(Part::parse_from_remote_path("a/b/short_1_2", 0).is_none());
        assert!(Part::parse_from_remote_path(
            "a/0000000000000000_0000000000000000_000000000000ZZZZ", 0).is_none());
    }

    #[test]
    fn split_into_parts_covers_file() {
        assert_eq!(split_into_parts("f", 0).len(), 1); // zero-length file
        assert_eq!(split_into_parts("f", 0)[0].size, 0);
        assert_eq!(split_into_parts("f", MAX_PART_SIZE).len(), 1);
        let parts = split_into_parts("f", 2 * MAX_PART_SIZE + 5);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[2].offset, 2 * MAX_PART_SIZE);
        assert_eq!(parts[2].size, 5);
        assert_eq!(parts.iter().map(|p| p.size).sum::<u64>(), 2 * MAX_PART_SIZE + 5);
    }

    #[test]
    fn diff_and_intersect() {
        let a = vec![part("x", 0, 10), part("y", 0, 10)];
        let b = vec![part("y", 0, 10), part("z", 0, 10)];
        assert_eq!(parts_difference(&a, &b), vec![part("x", 0, 10)]);
        assert_eq!(parts_intersect(&a, &b), vec![part("y", 0, 10)]);
        // actual_size participates in identity:
        let mut broken = part("y", 0, 10);
        broken.actual_size = 3;
        assert_eq!(parts_difference(&[broken.clone()], &b), vec![broken]);
    }

    #[test]
    fn parts_json_is_never_equal_to_itself() {
        let pj = part("data/small/2026_07/parts.json", 0, 64);
        assert_eq!(parts_difference(&[pj.clone()], &[pj.clone()]).len(), 1);
        assert!(parts_intersect(&[pj.clone()], &[pj]).is_empty());
    }
}
```

- [ ] **Step 4: lib.rs with `run_parallel`**

```rust
//! Port of VictoriaMetrics lib/backup: incremental backup/restore of
//! esm-storage snapshots to fs/s3/gcs/azblob destinations.

pub mod names;
pub mod part;
pub mod timeutil;
pub mod localfs;
pub mod remote;
pub mod backup;
pub mod restore;
pub mod cliflags;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Runs `f` over `items` on up to `concurrency` threads; returns the first
/// error (remaining items are skipped once an error is seen).
pub fn run_parallel<T: Sync>(
    items: &[T],
    concurrency: usize,
    f: impl Fn(&T) -> anyhow::Result<()> + Sync,
) -> anyhow::Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let next = AtomicUsize::new(0);
    let failed = std::sync::atomic::AtomicBool::new(false);
    let first_err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let workers = concurrency.clamp(1, items.len());
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| loop {
                if failed.load(Ordering::Relaxed) {
                    return;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= items.len() {
                    return;
                }
                if let Err(e) = f(&items[i]) {
                    failed.store(true, Ordering::Relaxed);
                    first_err.lock().unwrap().get_or_insert(e);
                    return;
                }
            });
        }
    });
    match first_err.into_inner().unwrap() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
```

(Until Tasks 2–5 exist, comment out the not-yet-created `pub mod` lines so the crate compiles; each task uncomments its own.)

- [ ] **Step 5: Run tests + clippy**

Run: `cargo test -p esm-backup && cargo clippy -p esm-backup -- -D warnings`
Expected: 7 unit tests pass, clean clippy.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/esm-backup
git commit -m "feat: esm-backup crate scaffold with Part model and helpers"
```

---

### Task 2: LocalFs (snapshot source + restore destination)

**Files:**
- Create: `crates/esm-backup/src/localfs.rs` (uncomment `pub mod localfs;`)

**Interfaces:**
- Consumes: `part::{Part, split_into_parts, sort_parts}`, `names::*`.
- Produces (consumed by backup.rs/restore.rs):
  - `LocalFs { pub dir: PathBuf }` with `new(dir)`, `list_parts() -> Result<Vec<Part>>`, `open_part_reader(&self, p: &Part) -> Result<impl Read>`, `write_part_at(&self, p: &Part, r: &mut dyn Read) -> Result<()>` (creates parent dirs; opens/creates the final file, `set_len(file_size)` if smaller, seek to offset, copy exactly `size` bytes, `sync_all`), `delete_path(&self, path: &str) -> Result<()>`, `remove_empty_dirs(&self) -> Result<()>`, `cleanup_tmp_files(&self) -> Result<()>`.

- [ ] **Step 1: Write failing unit tests**

In `src/localfs.rs` `#[cfg(test)]` module (temp dirs via the repo convention: `std::env::temp_dir().join(format!("esm-backup-localfs-{name}-{}", std::process::id()))`, removed before/after):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("esm-backup-localfs-{name}-{}", std::process::id()));
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
        write_file(&dir, "data/small/2026_07/0000000000000001/values.bin", b"hello");
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
        let p = Part { path: "f.bin".into(), file_size: 10, offset: 3, size: 4, actual_size: 4 };
        let mut out = Vec::new();
        fs.open_part_reader(&p).unwrap().read_to_end(&mut out).unwrap();
        assert_eq!(out, b"3456");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_part_at_assembles_file_out_of_order() {
        let dir = test_dir("writer");
        let fs = LocalFs::new(&dir);
        let mk = |offset, size| Part {
            path: "out/f.bin".into(), file_size: 10, offset, size, actual_size: size,
        };
        fs.write_part_at(&mk(5, 5), &mut &b"56789"[..]).unwrap();
        fs.write_part_at(&mk(0, 5), &mut &b"01234"[..]).unwrap();
        assert_eq!(std::fs::read(dir.join("out/f.bin")).unwrap(), b"0123456789");
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
```

Run: `cargo test -p esm-backup localfs 2>&1 | head -5` → compile error (red).

- [ ] **Step 2: Implement LocalFs**

```rust
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
        LocalFs { dir: dir.as_ref().to_path_buf() }
    }

    /// Recursively lists all files as parts, excluding the special files.
    /// Symlinks are skipped (esmetrics snapshots contain none; upstream
    /// resolves them — divergence documented in the plan).
    pub fn list_parts(&self) -> anyhow::Result<Vec<Part>> {
        let mut parts = Vec::new();
        self.walk(&self.dir, &mut parts)?;
        Ok(parts)
    }

    fn walk(&self, dir: &Path, out: &mut Vec<Part>) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {dir:?}"))? {
            let entry = entry?;
            let ft = entry.file_type()?;
            let path = entry.path();
            if ft.is_symlink() {
                continue;
            }
            if ft.is_dir() {
                self.walk(&path, out)?;
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if is_special_file(&name) {
                continue;
            }
            let rel = path
                .strip_prefix(&self.dir)
                .expect("BUG: walked outside root")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let size = entry.metadata()?.len();
            out.extend(split_into_parts(&rel, size));
        }
        Ok(())
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
        let path = self.local_path(&p.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = OpenOptions::new().create(true).write(true).truncate(false).open(&path)?;
        if f.metadata()?.len() < p.file_size {
            f.set_len(p.file_size)?;
        }
        f.seek(SeekFrom::Start(p.offset))?;
        let copied = std::io::copy(&mut r.take(p.size), &mut f)?;
        anyhow::ensure!(
            copied == p.size,
            "unexpected size for part {}: got {copied}, want {}", p.path, p.size
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

/// Depth-first removal of empty subdirectories; returns whether `dir`
/// itself ended up empty (the root is never removed by the caller).
fn remove_empty_dirs_below(dir: &Path) -> anyhow::Result<bool> {
    let mut empty = true;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let sub = entry.path();
            if remove_empty_dirs_below(&sub)? {
                std::fs::remove_dir(&sub)?;
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
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_matching(&entry.path(), pred, out)?;
        } else if pred(&entry.file_name().to_string_lossy()) {
            out.push(entry.path());
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Run tests + clippy**

Run: `cargo test -p esm-backup localfs && cargo clippy -p esm-backup -- -D warnings`
Expected: 4 tests pass, clean.

- [ ] **Step 4: Commit**

```bash
git add crates/esm-backup
git commit -m "feat: esm-backup LocalFs listing/reading/writing"
```

---

### Task 3: RemoteFs trait, fs:// backend, object-store backend, URL factory

**Files:**
- Create: `crates/esm-backup/src/remote/mod.rs`, `remote/local.rs`, `remote/object.rs` (uncomment `pub mod remote;`)

**Interfaces:**
- Produces (consumed by backup.rs/restore.rs/bins):

```rust
pub trait RemoteFs: Send + Sync {
    fn describe(&self) -> String;
    fn list_parts(&self) -> anyhow::Result<Vec<Part>>;
    fn delete_part(&self, p: &Part) -> anyhow::Result<()>;
    fn download_part(&self, p: &Part, w: &mut dyn Write) -> anyhow::Result<()>;
    fn upload_part(&self, p: &Part, r: &mut dyn Read) -> anyhow::Result<()>;
    /// Server-side copy of `p` from `src` into self. Ok(false) = not
    /// possible (different backend/bucket) — caller must stream-copy.
    fn copy_part_from(&self, src: &dyn RemoteFs, p: &Part) -> anyhow::Result<bool>;
    fn remove_empty_dirs(&self) -> anyhow::Result<()>;
    fn create_file(&self, file_path: &str, data: &[u8]) -> anyhow::Result<()>;
    fn delete_file(&self, file_path: &str) -> anyhow::Result<()>;
    fn has_file(&self, file_path: &str) -> anyhow::Result<bool>;
    fn read_file(&self, file_path: &str) -> anyhow::Result<Vec<u8>>;
    fn as_any(&self) -> &dyn std::any::Any;
}
pub fn new_remote_fs(url: &str) -> anyhow::Result<Box<dyn RemoteFs>>;
```

- URL schemes: `fs://</abs/path>` → `LocalRemote`; `s3://bucket[/prefix]`, `gs://bucket[/prefix]` (accept `gcs://` alias), `azblob://container[/prefix]` → `ObjectRemote`. Anything else → error listing supported schemes.

- [ ] **Step 1: Write failing tests (fs:// only — cloud backends are compile-checked, not integration-tested)**

In `remote/mod.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("esm-backup-remote-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn factory_parses_schemes() {
        let dir = test_dir("factory");
        let url = format!("fs://{}", dir.display());
        assert!(new_remote_fs(&url).is_ok());
        assert!(new_remote_fs("fs://relative/path").is_err()); // must be absolute
        assert!(new_remote_fs("ftp://nope").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fs_remote_roundtrip() {
        let dir = test_dir("roundtrip");
        let fs = new_remote_fs(&format!("fs://{}", dir.display())).unwrap();
        let p = crate::part::Part {
            path: "data/f.bin".into(), file_size: 5, offset: 0, size: 5, actual_size: 5,
        };
        fs.upload_part(&p, &mut &b"hello"[..]).unwrap();
        let listed = fs.list_parts().unwrap();
        assert_eq!(listed, vec![p.clone()]);

        let mut out = Vec::new();
        fs.download_part(&p, &mut out).unwrap();
        assert_eq!(out, b"hello");

        // marker files are excluded from part listings
        fs.create_file("backup_complete.ignore", b"").unwrap();
        assert!(fs.has_file("backup_complete.ignore").unwrap());
        assert_eq!(fs.list_parts().unwrap().len(), 1);
        fs.delete_file("backup_complete.ignore").unwrap();
        assert!(!fs.has_file("backup_complete.ignore").unwrap());

        fs.delete_part(&p).unwrap();
        fs.remove_empty_dirs().unwrap();
        assert!(fs.list_parts().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fs_remote_server_side_copy() {
        let a_dir = test_dir("copy-a");
        let b_dir = test_dir("copy-b");
        let a = new_remote_fs(&format!("fs://{}", a_dir.display())).unwrap();
        let b = new_remote_fs(&format!("fs://{}", b_dir.display())).unwrap();
        let p = crate::part::Part {
            path: "f.bin".into(), file_size: 3, offset: 0, size: 3, actual_size: 3,
        };
        a.upload_part(&p, &mut &b"abc"[..]).unwrap();
        assert!(b.copy_part_from(a.as_ref(), &p).unwrap()); // local↔local: hardlink/copy
        let mut out = Vec::new();
        b.download_part(&p, &mut out).unwrap();
        assert_eq!(out, b"abc");
        let _ = std::fs::remove_dir_all(&a_dir);
        let _ = std::fs::remove_dir_all(&b_dir);
    }
}
```

Run to red: `cargo test -p esm-backup remote 2>&1 | head -5`.

- [ ] **Step 2: Implement `remote/mod.rs` (trait + factory + stream-copy helper)**

```rust
//! Remote backup destinations. Go: lib/backup/common.RemoteFS.

mod local;
mod object;

use std::io::{Read, Write};

pub use local::LocalRemote;
pub use object::ObjectRemote;

use crate::part::Part;

pub trait RemoteFs: Send + Sync {
    // ... exactly the trait from the Interfaces block above ...
}

/// Builds a RemoteFs from a `-dst`/`-src`/`-origin` URL.
/// Credentials for cloud schemes come from standard env vars
/// (AWS_*, GOOGLE_APPLICATION_CREDENTIALS, AZURE_STORAGE_*).
pub fn new_remote_fs(url: &str) -> anyhow::Result<Box<dyn RemoteFs>> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("missing scheme in {url:?}; expected <scheme>://<path>"))?;
    match scheme {
        "fs" => {
            anyhow::ensure!(
                std::path::Path::new(rest).is_absolute(),
                "dir must be absolute in fs:// url, got {rest:?}"
            );
            Ok(Box::new(LocalRemote::new(rest)?))
        }
        "s3" | "gs" | "gcs" | "azblob" => {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            anyhow::ensure!(!bucket.is_empty(), "missing bucket/container in {url:?}");
            Ok(Box::new(ObjectRemote::new(scheme, bucket, prefix)?))
        }
        other => anyhow::bail!(
            "unsupported scheme {other:?} in {url:?}; supported: fs, s3, gs, azblob"
        ),
    }
}

/// Streams a part between two RemoteFs that cannot server-side copy:
/// a reader thread downloads into a bounded channel while the caller's
/// thread uploads. Go: actions.crossTypeCopy.
pub fn cross_copy(src: &dyn RemoteFs, dst: &dyn RemoteFs, p: &Part) -> anyhow::Result<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(4);
    std::thread::scope(|s| -> anyhow::Result<()> {
        let download = s.spawn(move || -> anyhow::Result<()> {
            let mut w = ChannelWriter { tx };
            src.download_part(p, &mut w)
        });
        let mut r = ChannelReader { rx, buf: Vec::new(), pos: 0 };
        let upload_res = dst.upload_part(p, &mut r);
        let download_res = download.join().expect("download thread panicked");
        download_res?;
        upload_res
    })
}

struct ChannelWriter {
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tx
            .send(buf.to_vec())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "upload side gone"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ChannelReader {
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.buf.len() {
            match self.rx.recv() {
                Ok(chunk) => {
                    self.buf = chunk;
                    self.pos = 0;
                }
                Err(_) => return Ok(0), // sender closed = EOF
            }
        }
        let n = (self.buf.len() - self.pos).min(out.len());
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}
```

- [ ] **Step 3: Implement `remote/local.rs`**

`LocalRemote { dir: PathBuf }` — objects stored at `dir/<Part::remote_path("")>` (i.e. `dir/<path>/<HEX_HEX_HEX>`), markers at `dir/<file_path>`. Implementation notes (write real code, this is the shape):

- `new(dir)`: `std::fs::create_dir_all`, store canonicalized path.
- `list_parts`: reuse the recursive-walk approach from `localfs.rs` (private helper duplicated here or shared via a `pub(crate)` util in `localfs.rs` — prefer sharing: make `collect_files(dir) -> Vec<(PathBuf, u64)>` `pub(crate)` in localfs.rs). Skip names ending `.ignore`; for each file compute the canonical rel path, `Part::parse_from_remote_path(rel, file_len)`; files that don't parse are ignored with a `log::warn!`.
- `delete_part`/`upload_part`/`download_part`: path = `dir` + `p.remote_path("")` split on `/`. Upload: create parent dirs, write via `std::io::copy(&mut r.take(p.size), &mut f)`, verify count == `p.size`, `sync_all`. Download: open, `std::io::copy` into `w`, verify count == `p.actual_size`.
- `copy_part_from`: `src.as_any().downcast_ref::<LocalRemote>()` — if `None` return `Ok(false)`; else try `std::fs::hard_link(src_path, dst_path)` after creating parent dirs; on error fall back to `std::fs::copy` + `sync_all`. Return `Ok(true)`.
- `create_file`/`read_file`/`has_file`/`delete_file`: direct `std::fs` ops on `dir/<file_path>`; `create_file` syncs; `delete_file` treats NotFound as Ok.
- `remove_empty_dirs`: call the shared `remove_empty_dirs_below` (make it `pub(crate)` in localfs.rs).
- `describe()`: `format!("fs://{}", self.dir.display())`.

- [ ] **Step 4: Implement `remote/object.rs`**

```rust
//! Cloud object-store backend via the `object_store` crate. The tokio
//! runtime lives HERE and nowhere else in the workspace.

use std::io::{Read, Write};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};

use crate::part::Part;
use super::RemoteFs;

const UPLOAD_CHUNK_SIZE: usize = 32 * 1024 * 1024;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("cannot build tokio runtime")
    })
}

pub struct ObjectRemote {
    scheme: String,
    bucket: String,
    /// Key prefix inside the bucket, no trailing slash, may be empty.
    prefix: String,
    store: Arc<dyn ObjectStore>,
}

impl ObjectRemote {
    pub fn new(scheme: &str, bucket: &str, prefix: &str) -> anyhow::Result<ObjectRemote> {
        let store: Arc<dyn ObjectStore> = match scheme {
            "s3" => Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .context("cannot initialize S3 client (check AWS_* env vars)")?,
            ),
            "gs" | "gcs" => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .context("cannot initialize GCS client (check GOOGLE_* env vars)")?,
            ),
            "azblob" => Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(bucket)
                    .build()
                    .context("cannot initialize Azure client (check AZURE_STORAGE_* env vars)")?,
            ),
            other => anyhow::bail!("BUG: ObjectRemote does not handle scheme {other:?}"),
        };
        Ok(ObjectRemote {
            scheme: scheme.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.trim_matches('/').to_string(),
            store,
        })
    }

    fn obj_path(&self, key: &str) -> ObjPath {
        if self.prefix.is_empty() {
            ObjPath::from(key)
        } else {
            ObjPath::from(format!("{}/{}", self.prefix, key))
        }
    }

    fn part_path(&self, p: &Part) -> ObjPath {
        ObjPath::from(p.remote_path(&self.prefix))
    }
}

impl RemoteFs for ObjectRemote {
    fn describe(&self) -> String {
        format!("{}://{}/{}", self.scheme, self.bucket, self.prefix)
    }

    fn list_parts(&self) -> anyhow::Result<Vec<Part>> {
        runtime().block_on(async {
            let prefix = if self.prefix.is_empty() {
                None
            } else {
                Some(ObjPath::from(self.prefix.as_str()))
            };
            let mut stream = self.store.list(prefix.as_ref());
            let mut parts = Vec::new();
            while let Some(meta) = stream.next().await.transpose()? {
                let key = meta.location.as_ref();
                let rel = key.strip_prefix(self.prefix.as_str()).unwrap_or(key);
                let rel = rel.trim_start_matches('/');
                if rel.ends_with(".ignore") {
                    continue;
                }
                match Part::parse_from_remote_path(rel, meta.size) {
                    Some(p) => parts.push(p),
                    None => log::warn!("skipping unknown object {key:?}"),
                }
            }
            Ok(parts)
        })
    }

    fn delete_part(&self, p: &Part) -> anyhow::Result<()> {
        runtime().block_on(async { Ok(self.store.delete(&self.part_path(p)).await?) })
    }

    fn download_part(&self, p: &Part, w: &mut dyn Write) -> anyhow::Result<()> {
        runtime().block_on(async {
            let res = self.store.get(&self.part_path(p)).await?;
            let mut stream = res.into_stream();
            let mut n: u64 = 0;
            while let Some(chunk) = stream.next().await.transpose()? {
                w.write_all(&chunk)?;
                n += chunk.len() as u64;
            }
            anyhow::ensure!(
                n == p.actual_size,
                "unexpected size downloaded for {}: got {n}, want {}", p.path, p.actual_size
            );
            Ok(())
        })
    }

    fn upload_part(&self, p: &Part, r: &mut dyn Read) -> anyhow::Result<()> {
        runtime().block_on(async {
            let upload = self.store.put_multipart(&self.part_path(p)).await?;
            let mut w = WriteMultipart::new_with_chunk_size(upload, UPLOAD_CHUNK_SIZE);
            let mut buf = vec![0u8; 1024 * 1024];
            let mut total: u64 = 0;
            loop {
                let n = r.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                w.wait_for_capacity(8).await?;
                w.write(&buf[..n]);
                total += n as u64;
            }
            w.finish().await?;
            anyhow::ensure!(
                total == p.size,
                "unexpected size uploaded for {}: got {total}, want {}", p.path, p.size
            );
            Ok(())
        })
    }

    fn copy_part_from(&self, src: &dyn RemoteFs, p: &Part) -> anyhow::Result<bool> {
        let Some(other) = src.as_any().downcast_ref::<ObjectRemote>() else {
            return Ok(false);
        };
        // object_store instances are bucket-scoped: server-side copy only
        // within the same scheme+bucket.
        if other.scheme != self.scheme || other.bucket != self.bucket {
            return Ok(false);
        }
        runtime().block_on(async {
            self.store.copy(&other.part_path(p), &self.part_path(p)).await?;
            Ok(true)
        })
    }

    fn remove_empty_dirs(&self) -> anyhow::Result<()> {
        Ok(()) // object stores have no directories
    }

    fn create_file(&self, file_path: &str, data: &[u8]) -> anyhow::Result<()> {
        let payload = bytes::Bytes::copy_from_slice(data);
        runtime().block_on(async {
            self.store.put(&self.obj_path(file_path), payload.into()).await?;
            Ok(())
        })
    }

    fn delete_file(&self, file_path: &str) -> anyhow::Result<()> {
        runtime().block_on(async {
            match self.store.delete(&self.obj_path(file_path)).await {
                Ok(()) => Ok(()),
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
    }

    fn has_file(&self, file_path: &str) -> anyhow::Result<bool> {
        runtime().block_on(async {
            match self.store.head(&self.obj_path(file_path)).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(e.into()),
            }
        })
    }

    fn read_file(&self, file_path: &str) -> anyhow::Result<Vec<u8>> {
        runtime().block_on(async {
            let res = self.store.get(&self.obj_path(file_path)).await?;
            Ok(res.bytes().await?.to_vec())
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
```

Adjust to the actual 0.14 API if method names differ at compile time (`ObjectStoreExt` import is required for `put/get/head/delete/copy`; check `WriteMultipart::wait_for_capacity` signature in docs.rs if the build errors).

- [ ] **Step 5: Run tests + clippy + Windows check**

Run: `cargo test -p esm-backup remote && cargo clippy -p esm-backup -- -D warnings && cargo check -p esm-backup --target x86_64-pc-windows-gnu`
Expected: tests pass; if the Windows check fails inside `aws-lc-rs`/`ring`, apply the `cloud` feature-gate contingency from Global Constraints now (move object.rs + deps behind `#[cfg(feature = "cloud")]`, factory returns a clear error for cloud schemes when built without it), and re-run with `--no-default-features` on Windows.

- [ ] **Step 6: Commit**

```bash
git add crates/esm-backup Cargo.lock
git commit -m "feat: esm-backup RemoteFs with fs:// and object-store backends"
```

---

### Task 4: Backup action

**Files:**
- Create: `crates/esm-backup/src/backup.rs` (uncomment `pub mod backup;`)
- Test: `crates/esm-backup/tests/backup_restore_test.rs` (created here, extended in Task 5)

**Interfaces:**
- Consumes: everything from Tasks 1–3.
- Produces: `Backup<'a> { concurrency: usize, src: &'a LocalFs, dst: &'a dyn RemoteFs, origin: Option<&'a dyn RemoteFs>, created_at: Option<String> }` with `run(&self) -> anyhow::Result<()>`.

- [ ] **Step 1: Write the failing integration test**

`crates/esm-backup/tests/backup_restore_test.rs`:

```rust
//! fs:// round-trip tests for the Backup and Restore actions.

use std::path::{Path, PathBuf};

use esm_backup::backup::Backup;
use esm_backup::localfs::LocalFs;
use esm_backup::names;
use esm_backup::remote::new_remote_fs;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("esm-backup-actions-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_file(dir: &Path, rel: &str, data: &[u8]) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, data).unwrap();
}

/// Recursively reads all regular files as (canonical-rel-path, contents).
fn read_tree(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(root, &entry.path(), out);
            } else {
                let rel = entry
                    .path()
                    .strip_prefix(root)
                    .unwrap()
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push((rel, std::fs::read(entry.path()).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

fn make_src_tree(src: &Path) {
    write_file(src, "data/small/2026_07/0000000000000001/values.bin", &[7u8; 4096]);
    write_file(src, "data/small/2026_07/0000000000000001/index.bin", b"idx");
    write_file(src, "data/small/2026_07/parts.json", b"{\"Small\":[\"0000000000000001\"],\"Big\":[]}");
    write_file(src, "data/indexdb/2026_07/parts.json", b"[]");
    write_file(src, "empty.bin", b"");
    write_file(src, "flock.lock", b"never backed up");
}

#[test]
fn backup_writes_markers_and_all_parts() {
    let src_dir = test_dir("backup-src");
    let dst_dir = test_dir("backup-dst");
    make_src_tree(&src_dir);

    let src = LocalFs::new(&src_dir);
    let dst = new_remote_fs(&format!("fs://{}", dst_dir.display())).unwrap();
    Backup { concurrency: 2, src: &src, dst: dst.as_ref(), origin: None,
             created_at: Some("2026-07-05T00:00:00Z".into()) }
        .run()
        .unwrap();

    assert!(dst.has_file(names::BACKUP_COMPLETE_FILENAME).unwrap());
    let meta = dst.read_file(names::BACKUP_METADATA_FILENAME).unwrap();
    let meta: serde_json::Value = serde_json::from_slice(&meta).unwrap();
    assert_eq!(meta["created_at"], "2026-07-05T00:00:00Z");
    assert!(meta["completed_at"].is_string());

    // 5 parts uploaded (flock.lock excluded, zero-length file kept).
    assert_eq!(dst.list_parts().unwrap().len(), 5);

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&dst_dir);
}

#[test]
fn incremental_backup_uploads_only_changes() {
    let src_dir = test_dir("incr-src");
    let dst_dir = test_dir("incr-dst");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let dst = new_remote_fs(&format!("fs://{}", dst_dir.display())).unwrap();
    let backup = || {
        Backup { concurrency: 2, src: &src, dst: dst.as_ref(), origin: None, created_at: None }
            .run()
            .unwrap()
    };
    backup();
    let first = dst.list_parts().unwrap().len();

    // Simulate a merge: one part dir replaced by another, parts.json rewritten
    // with the SAME byte length (so only the unique-key rule re-uploads it).
    std::fs::remove_dir_all(src_dir.join("data/small/2026_07/0000000000000001")).unwrap();
    write_file(&src_dir, "data/small/2026_07/0000000000000002/values.bin", &[8u8; 2048]);
    write_file(&src_dir, "data/small/2026_07/parts.json", b"{\"Small\":[\"0000000000000002\"],\"Big\":[]}");
    backup();

    let parts = dst.list_parts().unwrap();
    let paths: Vec<&str> = parts.iter().map(|p| p.path.as_str()).collect();
    assert!(paths.iter().any(|p| p.contains("0000000000000002")));
    assert!(!paths.iter().any(|p| p.contains("0000000000000001")), "old part must be deleted from dst");
    assert_eq!(parts.len(), first, "same file count after replace");
    assert!(dst.has_file(names::BACKUP_COMPLETE_FILENAME).unwrap());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&dst_dir);
}

#[test]
fn backup_with_origin_server_side_copies() {
    let src_dir = test_dir("origin-src");
    let old_dir = test_dir("origin-old");
    let new_dir = test_dir("origin-new");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let old = new_remote_fs(&format!("fs://{}", old_dir.display())).unwrap();
    let new = new_remote_fs(&format!("fs://{}", new_dir.display())).unwrap();

    Backup { concurrency: 2, src: &src, dst: old.as_ref(), origin: None, created_at: None }
        .run().unwrap();
    Backup { concurrency: 2, src: &src, dst: new.as_ref(), origin: Some(old.as_ref()),
             created_at: None }
        .run().unwrap();

    assert_eq!(new.list_parts().unwrap().len(), old.list_parts().unwrap().len());
    assert!(new.has_file(names::BACKUP_COMPLETE_FILENAME).unwrap());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&old_dir);
    let _ = std::fs::remove_dir_all(&new_dir);
}
```

Run to red: `cargo test -p esm-backup --test backup_restore_test 2>&1 | head -5`.

- [ ] **Step 2: Implement backup.rs**

```rust
//! The Backup action. Go: lib/backup/actions/backup.go

use anyhow::Context;
use serde::Serialize;

use crate::localfs::LocalFs;
use crate::names;
use crate::part::{parts_difference, parts_intersect};
use crate::remote::{cross_copy, RemoteFs};
use crate::run_parallel;
use crate::timeutil;

#[derive(Serialize)]
struct BackupMetadata {
    created_at: String,
    completed_at: String,
}

pub struct Backup<'a> {
    pub concurrency: usize,
    pub src: &'a LocalFs,
    pub dst: &'a dyn RemoteFs,
    pub origin: Option<&'a dyn RemoteFs>,
    /// RFC3339 creation time (derived from the snapshot name by esbackup);
    /// defaults to now.
    pub created_at: Option<String>,
}

impl Backup<'_> {
    pub fn run(&self) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        let concurrency = self.concurrency.max(1);

        // Remove the completion marker FIRST so an interrupted backup is
        // detectably incomplete.
        self.dst
            .delete_file(names::BACKUP_COMPLETE_FILENAME)
            .context("cannot delete backup_complete marker")?;

        let src_parts = self.src.list_parts().context("cannot list src parts")?;
        let dst_parts = self.dst.list_parts().context("cannot list dst parts")?;
        let origin_parts = match self.origin {
            Some(o) => o.list_parts().context("cannot list origin parts")?,
            None => Vec::new(),
        };

        // 1. Delete parts that vanished from src (also stale/broken parts —
        //    their actual_size key mismatch lands them here).
        let to_delete = parts_difference(&dst_parts, &src_parts);
        log::info!("deleting {} obsolete parts from {}", to_delete.len(), self.dst.describe());
        run_parallel(&to_delete, concurrency, |p| self.dst.delete_part(p))?;
        if !to_delete.is_empty() {
            self.dst.remove_empty_dirs()?;
        }

        // 2. Server-side copy of parts available in origin.
        let to_copy = parts_difference(&src_parts, &dst_parts);
        let from_origin = parts_intersect(&origin_parts, &to_copy);
        if let Some(origin) = self.origin {
            log::info!("server-side copying {} parts from {}", from_origin.len(), origin.describe());
            run_parallel(&from_origin, concurrency, |p| {
                if !self.dst.copy_part_from(origin, p)? {
                    cross_copy(origin, self.dst, p)?;
                }
                Ok(())
            })?;
        }

        // 3. Upload everything else from the local snapshot.
        let to_upload = parts_difference(&to_copy, &origin_parts);
        let total: u64 = to_upload.iter().map(|p| p.size).sum();
        log::info!(
            "uploading {} parts ({} bytes) to {}", to_upload.len(), total, self.dst.describe()
        );
        run_parallel(&to_upload, concurrency, |p| {
            let mut r = self.src.open_part_reader(p)?;
            self.dst
                .upload_part(p, &mut r)
                .with_context(|| format!("cannot upload part {}", p.path))
        })?;

        // 4. Metadata, then the completion marker LAST.
        let meta = BackupMetadata {
            created_at: self.created_at.clone().unwrap_or_else(timeutil::now_rfc3339),
            completed_at: timeutil::now_rfc3339(),
        };
        self.dst
            .create_file(names::BACKUP_METADATA_FILENAME, &serde_json::to_vec(&meta)?)?;
        self.dst.create_file(names::BACKUP_COMPLETE_FILENAME, b"")?;

        log::info!(
            "backup to {} complete in {:.3}s", self.dst.describe(), started.elapsed().as_secs_f64()
        );
        Ok(())
    }
}
```

- [ ] **Step 3: Run tests + clippy**

Run: `cargo test -p esm-backup --test backup_restore_test && cargo clippy -p esm-backup -- -D warnings`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/esm-backup
git commit -m "feat: esm-backup Backup action with incremental and origin copy"
```

---

### Task 5: Restore action + esm-storage startup guard

**Files:**
- Create: `crates/esm-backup/src/restore.rs` (uncomment `pub mod restore;`)
- Modify: `crates/esm-storage/src/storage/mod.rs` (`must_open` guard)
- Test: extend `crates/esm-backup/tests/backup_restore_test.rs`; add guard test to `crates/esm-storage/tests/snapshot_test.rs`

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: `Restore<'a> { concurrency: usize, src: &'a dyn RemoteFs, dst_dir: PathBuf, skip_backup_complete_check: bool }` with `run(&self) -> anyhow::Result<()>`.

- [ ] **Step 1: Write failing tests**

Append to `backup_restore_test.rs`:

```rust
use esm_backup::restore::Restore;

#[test]
fn restore_roundtrips_byte_for_byte() {
    let src_dir = test_dir("restore-src");
    let bak_dir = test_dir("restore-bak");
    let out_dir = test_dir("restore-out");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let bak = new_remote_fs(&format!("fs://{}", bak_dir.display())).unwrap();
    Backup { concurrency: 2, src: &src, dst: bak.as_ref(), origin: None, created_at: None }
        .run().unwrap();

    Restore { concurrency: 2, src: bak.as_ref(), dst_dir: out_dir.clone(),
              skip_backup_complete_check: false }
        .run().unwrap();

    // Identical trees, minus local-only special files.
    let mut want = read_tree(&src_dir);
    want.retain(|(p, _)| p != "flock.lock");
    let mut got = read_tree(&out_dir);
    got.retain(|(p, _)| p != "flock.lock"); // restore creates its own flock
    assert_eq!(got, want);
    // The in-progress marker must be gone on success.
    assert!(!out_dir.join("restore-in-progress").exists());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&bak_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[test]
fn restore_refuses_incomplete_backup() {
    let bak_dir = test_dir("incomplete-bak");
    let out_dir = test_dir("incomplete-out");
    let bak = new_remote_fs(&format!("fs://{}", bak_dir.display())).unwrap();
    // A part but no completion marker.
    let p = esm_backup::part::Part {
        path: "f.bin".into(), file_size: 1, offset: 0, size: 1, actual_size: 1,
    };
    bak.upload_part(&p, &mut &b"x"[..]).unwrap();

    let err = Restore { concurrency: 1, src: bak.as_ref(), dst_dir: out_dir.clone(),
                        skip_backup_complete_check: false }
        .run().unwrap_err();
    assert!(err.to_string().contains("backup_complete.ignore"), "err: {err}");

    // With the skip flag it proceeds.
    Restore { concurrency: 1, src: bak.as_ref(), dst_dir: out_dir.clone(),
              skip_backup_complete_check: true }
        .run().unwrap();
    assert_eq!(std::fs::read(out_dir.join("f.bin")).unwrap(), b"x");

    let _ = std::fs::remove_dir_all(&bak_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[test]
fn restore_deletes_local_files_missing_from_backup() {
    let src_dir = test_dir("rsync-src");
    let bak_dir = test_dir("rsync-bak");
    let out_dir = test_dir("rsync-out");
    make_src_tree(&src_dir);
    let src = LocalFs::new(&src_dir);
    let bak = new_remote_fs(&format!("fs://{}", bak_dir.display())).unwrap();
    Backup { concurrency: 2, src: &src, dst: bak.as_ref(), origin: None, created_at: None }
        .run().unwrap();

    // Pre-populate the restore target with a file NOT in the backup.
    write_file(&out_dir, "data/small/2026_07/9999999999999999/values.bin", b"stale");
    Restore { concurrency: 2, src: bak.as_ref(), dst_dir: out_dir.clone(),
              skip_backup_complete_check: false }
        .run().unwrap();
    assert!(!out_dir.join("data/small/2026_07/9999999999999999").exists());

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&bak_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}
```

And in `crates/esm-storage/tests/snapshot_test.rs`:

```rust
#[test]
#[should_panic(expected = "restore-in-progress")]
fn must_open_panics_on_incomplete_restore() {
    let dir = test_dir("restore-guard");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("restore-in-progress"), b"").unwrap();
    let _ = open_storage(&dir, RETENTION); // must panic
}
```

Run both to red.

- [ ] **Step 2: Implement restore.rs**

```rust
//! The Restore action. Go: lib/backup/actions/restore.go

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::Context;

use crate::localfs::LocalFs;
use crate::names;
use crate::part::{parts_difference, sort_parts, Part};
use crate::remote::RemoteFs;
use crate::run_parallel;

pub struct Restore<'a> {
    pub concurrency: usize,
    pub src: &'a dyn RemoteFs,
    pub dst_dir: PathBuf,
    pub skip_backup_complete_check: bool,
}

impl Restore<'_> {
    pub fn run(&self) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        std::fs::create_dir_all(&self.dst_dir)?;
        // Fails if an esmetrics server currently owns the data dir.
        let _flock = esm_common::fs::must_create_flock_file(&self.dst_dir);

        if !self.skip_backup_complete_check {
            anyhow::ensure!(
                self.src.has_file(names::BACKUP_COMPLETE_FILENAME)?,
                "cannot find {} in {}: the backup is incomplete or the -src path is wrong",
                names::BACKUP_COMPLETE_FILENAME,
                self.src.describe(),
            );
        }

        // Mark the restore as in progress; esm-storage refuses to open the
        // dir until this file is removed (i.e. until we succeed).
        let lock_path = self.dst_dir.join(names::RESTORE_IN_PROGRESS_FILENAME);
        std::fs::write(&lock_path, b"")?;

        let local = LocalFs::new(&self.dst_dir);
        local.cleanup_tmp_files()?;

        let mut src_parts = self.src.list_parts().context("cannot list backup parts")?;
        validate_parts(&self.dst_dir, &mut src_parts)?;

        // rsync --delete semantics: drop local files whose offset-0 part is
        // not in the backup (per-file identity lives in the offset-0 key).
        let local_parts = local.list_parts()?;
        let to_delete = parts_difference(&local_parts, &src_parts);
        let paths_to_delete: HashSet<&str> = to_delete
            .iter()
            .filter(|p| p.offset == 0)
            .map(|p| p.path.as_str())
            .collect();
        log::info!("deleting {} local files missing from the backup", paths_to_delete.len());
        for path in &paths_to_delete {
            local.delete_path(path)?;
        }
        if !paths_to_delete.is_empty() {
            local.remove_empty_dirs()?;
        }

        // Download missing parts, grouped per file, files in parallel.
        let local_parts = local.list_parts()?;
        let to_copy = parts_difference(&src_parts, &local_parts);
        let mut per_path: HashMap<String, Vec<Part>> = HashMap::new();
        for p in to_copy {
            per_path.entry(p.path.clone()).or_default().push(p);
        }
        let mut groups: Vec<Vec<Part>> = per_path.into_values().collect();
        for g in &mut groups {
            sort_parts(g);
        }
        let total: u64 = groups.iter().flatten().map(|p| p.size).sum();
        log::info!(
            "downloading {} files ({total} bytes) from {}", groups.len(), self.src.describe()
        );
        run_parallel(&groups, self.concurrency.max(1), |parts| {
            let local = LocalFs::new(&self.dst_dir);
            for p in parts {
                // One in-flight part buffer per worker (≤ 1 GiB worst case;
                // esm-storage part files are typically MBs). If this proves
                // heavy, swap Vec for the ChannelWriter/Reader pipe in
                // remote/mod.rs — out of scope now.
                let mut buf = Vec::with_capacity(p.size.min(64 * 1024 * 1024) as usize);
                self.src.download_part(p, &mut buf)?;
                local.write_part_at(p, &mut buf.as_slice())?;
            }
            Ok(())
        })?;

        std::fs::remove_file(&lock_path)?;
        log::info!(
            "restore from {} complete in {:.3}s", self.src.describe(), started.elapsed().as_secs_f64()
        );
        Ok(())
    }
}
```

(`Vec<u8>` implements `std::io::Write`, so it can be passed as the `&mut dyn Write` sink for `download_part` directly.)

And the validation helper:

```rust
/// Path-traversal guard + whole-file contiguity check.
/// Go: restore.go:94-137.
fn validate_parts(dst_dir: &std::path::Path, parts: &mut [Part]) -> anyhow::Result<()> {
    for p in parts.iter() {
        let ok = !p.path.is_empty()
            && !p.path.starts_with('/')
            && p.path.split('/').all(|c| !c.is_empty() && c != "." && c != "..");
        anyhow::ensure!(ok, "part path {:?} escapes the restore dir {dst_dir:?}", p.path);
    }
    sort_parts(parts);
    let mut i = 0;
    while i < parts.len() {
        let path = &parts[i].path;
        let file_size = parts[i].file_size;
        let mut expected_offset = 0u64;
        let mut j = i;
        while j < parts.len() && parts[j].path == *path {
            let p = &parts[j];
            anyhow::ensure!(
                p.offset == expected_offset && p.size == p.actual_size && p.file_size == file_size,
                "corrupted backup: part {:?} offset={} size={} actual_size={} file_size={} \
                 (expected offset {expected_offset}, file_size {file_size})",
                p.path, p.offset, p.size, p.actual_size, p.file_size
            );
            expected_offset += p.size;
            j += 1;
        }
        anyhow::ensure!(
            expected_offset == file_size,
            "corrupted backup: file {:?} has {expected_offset} bytes of parts, want {file_size}",
            path
        );
        i = j;
    }
    Ok(())
}
```

Write the final `restore.rs` with the clean loop only — do not include the rejected draft.

- [ ] **Step 3: Add the esm-storage guard**

In `crates/esm-storage/src/storage/mod.rs` `must_open`, immediately after the data-path mkdir and **before** the flock is taken (mirror upstream storage.go:222 placement — flock later is fine, the check must precede table open):

```rust
        // Refuse to open a half-restored dataset. The esrestore tool creates
        // this file at restore start and removes it only on success.
        let restore_lock = path.join("restore-in-progress");
        if esm_common::fs::is_path_exist(&restore_lock) {
            panic!(
                "FATAL: incomplete restore run detected; run esrestore again \
                 or remove the lock file {restore_lock:?}"
            );
        }
```

(Adapt the `path` variable name to what `must_open` actually calls its `PathBuf`.)

- [ ] **Step 4: Run all tests + clippy**

Run: `cargo test -p esm-backup && cargo test -p esm-storage --test snapshot_test && cargo clippy --workspace -- -D warnings`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/esm-backup crates/esm-storage
git commit -m "feat: esm-backup Restore action and restore-in-progress guard"
```

---

### Task 6: esbackup and esrestore binaries

**Files:**
- Create: `crates/esm-backup/src/cliflags.rs` (uncomment `pub mod cliflags;`)
- Create: `crates/esm-backup/src/bin/esbackup.rs`, `crates/esm-backup/src/bin/esrestore.rs`

**Interfaces:**
- Consumes: everything above; `reqwest::blocking` for `-snapshot.createURL`.
- Produces two binaries:
  - `esbackup -storageDataPath=<dir> [-snapshotName=<n> | -snapshot.createURL=<url>] -dst=<url> [-origin=<url>] [-concurrency=10]`
  - `esrestore -src=<url> -storageDataPath=<dir> [-concurrency=10] [-skipBackupCompleteCheck]`

- [ ] **Step 1: cliflags.rs**

A minimal Go-style parser (accepts `-flag=value`, `--flag=value`, `-flag value`; bools accept bare `-flag`); prints `-help` from the defs table:

```rust
//! Minimal Go-flag-style CLI parsing shared by esbackup/esrestore.

use std::collections::HashMap;

pub struct FlagSet {
    program: &'static str,
    defs: Vec<(&'static str, &'static str, &'static str)>, // name, default, help
    values: HashMap<&'static str, String>,
}

impl FlagSet {
    pub fn new(
        program: &'static str,
        defs: &[(&'static str, &'static str, &'static str)],
    ) -> FlagSet {
        FlagSet {
            program,
            defs: defs.to_vec(),
            values: defs.iter().map(|(n, d, _)| (*n, d.to_string())).collect(),
        }
    }

    /// Parses std::env::args; exits(0) on -help, exits(2) on unknown flags.
    pub fn parse(&mut self) {
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let flag = arg.trim_start_matches('-');
            if flag == "help" || flag == "h" {
                self.print_usage();
                std::process::exit(0);
            }
            let (name, value) = match flag.split_once('=') {
                Some((n, v)) => (n.to_string(), v.to_string()),
                None => {
                    let is_bool = self
                        .defs
                        .iter()
                        .any(|(n, d, _)| *n == flag && (*d == "true" || *d == "false"));
                    if is_bool {
                        (flag.to_string(), "true".to_string())
                    } else {
                        match args.next() {
                            Some(v) => (flag.to_string(), v),
                            None => self.die(&format!("flag -{flag} needs a value")),
                        }
                    }
                }
            };
            match self.defs.iter().find(|(n, _, _)| *n == name) {
                Some((n, _, _)) => {
                    self.values.insert(n, value);
                }
                None => self.die(&format!("unknown flag -{name}")),
            }
        }
    }

    pub fn get(&self, name: &str) -> &str {
        self.values.get(name).map(String::as_str).unwrap_or_else(|| {
            panic!("BUG: flag {name:?} was not declared")
        })
    }

    pub fn get_bool(&self, name: &str) -> bool {
        self.get(name) == "true"
    }

    pub fn get_usize(&self, name: &str) -> usize {
        self.get(name)
            .parse()
            .unwrap_or_else(|_| self.die(&format!("flag -{name} must be an integer")))
    }

    fn print_usage(&self) {
        eprintln!("Usage of {}:", self.program);
        for (name, default, help) in &self.defs {
            eprintln!("  -{name} (default {default:?})\n        {help}");
        }
        eprintln!(
            "\nCloud credentials come from standard env vars:\n  \
             s3://     AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_ENDPOINT\n  \
             gs://     GOOGLE_APPLICATION_CREDENTIALS (service-account JSON path)\n  \
             azblob:// AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY"
        );
    }

    fn die(&self, msg: &str) -> ! {
        eprintln!("{msg}");
        self.print_usage();
        std::process::exit(2);
    }
}
```

- [ ] **Step 2: esbackup.rs**

```rust
//! esbackup — backs up an esmetrics snapshot to fs/s3/gs/azblob.
//! Go: app/vmbackup.

use esm_backup::backup::Backup;
use esm_backup::cliflags::FlagSet;
use esm_backup::localfs::LocalFs;
use esm_backup::remote::new_remote_fs;
use esm_backup::timeutil;

const FLAG_DEFS: &[(&str, &str, &str)] = &[
    ("storageDataPath", "esmetrics-data", "Path to esmetrics data. Must match the server's -storageDataPath"),
    ("snapshotName", "", "Name of an existing snapshot under <storageDataPath>/snapshots to back up. \
      Not needed if -snapshot.createURL is set"),
    ("snapshot.createURL", "", "esmetrics create-snapshot URL, e.g. http://localhost:8428/snapshot/create. \
      When set, a snapshot is created, backed up and deleted afterwards"),
    ("dst", "", "Destination URL: fs:///abs/dir, s3://bucket/dir, gs://bucket/dir or azblob://container/dir. \
      Pointing -dst to an existing backup makes it incremental"),
    ("origin", "", "Optional URL of an existing backup for server-side copy of unchanged parts"),
    ("concurrency", "10", "The number of concurrent workers"),
];

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut flags = FlagSet::new("esbackup", FLAG_DEFS);
    flags.parse();
    if let Err(e) = run(&flags) {
        log::error!("esbackup failed: {e:#}");
        std::process::exit(1);
    }
}

fn run(flags: &FlagSet) -> anyhow::Result<()> {
    let storage_data_path = std::path::PathBuf::from(flags.get("storageDataPath"));
    let create_url = flags.get("snapshot.createURL").to_string();
    let mut snapshot_name = flags.get("snapshotName").to_string();
    let dst_url = flags.get("dst").to_string();
    anyhow::ensure!(!dst_url.is_empty(), "-dst cannot be empty");

    let mut created_via_url = false;
    if !create_url.is_empty() {
        anyhow::ensure!(
            snapshot_name.is_empty(),
            "-snapshotName and -snapshot.createURL cannot be set simultaneously"
        );
        snapshot_name = create_snapshot(&create_url)?;
        created_via_url = true;
        log::info!("created snapshot {snapshot_name}");
    }
    anyhow::ensure!(
        !snapshot_name.is_empty(),
        "either -snapshotName or -snapshot.createURL must be set"
    );

    let src_dir = storage_data_path.join("snapshots").join(&snapshot_name);
    anyhow::ensure!(src_dir.is_dir(), "snapshot dir {src_dir:?} does not exist");

    // Refuse fs:// destinations inside the storage data path.
    if let Some(dst_path) = dst_url.strip_prefix("fs://") {
        let storage_abs = std::path::absolute(&storage_data_path)?;
        anyhow::ensure!(
            !std::path::Path::new(dst_path).starts_with(&storage_abs),
            "-dst must not point inside -storageDataPath"
        );
    }

    let result = (|| -> anyhow::Result<()> {
        let src = LocalFs::new(&src_dir);
        let dst = new_remote_fs(&dst_url)?;
        let origin = match flags.get("origin") {
            "" => None,
            url => Some(new_remote_fs(url)?),
        };
        Backup {
            concurrency: flags.get_usize("concurrency"),
            src: &src,
            dst: dst.as_ref(),
            origin: origin.as_deref(),
            created_at: timeutil::rfc3339_from_snapshot_name(&snapshot_name),
        }
        .run()
    })();

    // Always try to delete an auto-created snapshot, success or failure.
    if created_via_url {
        let delete_url = create_url.replace("/create", "/delete");
        if let Err(e) = delete_snapshot(&delete_url, &snapshot_name) {
            log::warn!("cannot delete snapshot {snapshot_name}: {e:#}");
        }
    }
    result
}

fn create_snapshot(create_url: &str) -> anyhow::Result<String> {
    let body = reqwest::blocking::get(create_url)?.error_for_status()?.text()?;
    let v: serde_json::Value = serde_json::from_str(&body)?;
    anyhow::ensure!(v["status"] == "ok", "unexpected response from {create_url}: {body}");
    v["snapshot"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("no snapshot name in response: {body}"))
}

fn delete_snapshot(delete_url: &str, name: &str) -> anyhow::Result<()> {
    let url = format!("{delete_url}?snapshot={name}");
    reqwest::blocking::get(&url)?.error_for_status()?;
    Ok(())
}
```

- [ ] **Step 3: esrestore.rs**

```rust
//! esrestore — restores esmetrics data from a backup made by esbackup.
//! The esmetrics server must be stopped. Go: app/vmrestore.

use esm_backup::cliflags::FlagSet;
use esm_backup::remote::new_remote_fs;
use esm_backup::restore::Restore;

const FLAG_DEFS: &[(&str, &str, &str)] = &[
    ("src", "", "Source backup URL: fs:///abs/dir, s3://bucket/dir, gs://bucket/dir or azblob://container/dir"),
    ("storageDataPath", "esmetrics-data", "Destination path. Data is synced with the backup \
      (extra local files are DELETED, like rsync --delete)"),
    ("concurrency", "10", "The number of concurrent workers"),
    ("skipBackupCompleteCheck", "false", "Whether to skip checking for the backup_complete.ignore marker"),
];

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut flags = FlagSet::new("esrestore", FLAG_DEFS);
    flags.parse();
    if let Err(e) = run(&flags) {
        log::error!("esrestore failed: {e:#}");
        std::process::exit(1);
    }
}

fn run(flags: &FlagSet) -> anyhow::Result<()> {
    let src_url = flags.get("src").to_string();
    anyhow::ensure!(!src_url.is_empty(), "-src cannot be empty");
    let src = new_remote_fs(&src_url)?;
    Restore {
        concurrency: flags.get_usize("concurrency"),
        src: src.as_ref(),
        dst_dir: std::path::PathBuf::from(flags.get("storageDataPath")),
        skip_backup_complete_check: flags.get_bool("skipBackupCompleteCheck"),
    }
    .run()
}
```

- [ ] **Step 4: Smoke-test the binaries end to end over fs://**

```bash
cd /home/test/esmetrics
SCRATCH=$(mktemp -d)
mkdir -p "$SCRATCH/data/snapshots/20260705000000-0000000A/data/small/2026_07"
echo hello > "$SCRATCH/data/snapshots/20260705000000-0000000A/data/small/2026_07/f.bin"
cargo run -p esm-backup --bin esbackup -- \
  -storageDataPath="$SCRATCH/data" -snapshotName=20260705000000-0000000A \
  -dst="fs://$SCRATCH/backup"
cargo run -p esm-backup --bin esrestore -- \
  -src="fs://$SCRATCH/backup" -storageDataPath="$SCRATCH/restored"
cmp "$SCRATCH/data/snapshots/20260705000000-0000000A/data/small/2026_07/f.bin" \
    "$SCRATCH/restored/data/small/2026_07/f.bin" && echo ROUNDTRIP-OK
rm -rf "$SCRATCH"
```

Expected final line: `ROUNDTRIP-OK`.

- [ ] **Step 5: clippy + both-target check**

Run: `cargo clippy --workspace -- -D warnings && cargo check --workspace --target x86_64-unknown-linux-gnu && cargo check --workspace --target x86_64-pc-windows-gnu`
Expected: clean (with the `cloud` feature contingency applied on Windows if Task 3 needed it).

- [ ] **Step 6: Commit**

```bash
git add crates/esm-backup
git commit -m "feat: esbackup and esrestore binaries"
```

---

### Task 7: End-to-end test through the server + docs

**Files:**
- Create: `crates/esmetrics/tests/backup_e2e_test.rs`
- Modify: `crates/esmetrics/Cargo.toml` (`[dev-dependencies] esm-backup = { path = "../esm-backup" }`)
- Modify: `docs/PORTING.md`, `README.md`

**Interfaces:** none new.

- [ ] **Step 1: Write the e2e test**

`crates/esmetrics/tests/backup_e2e_test.rs` — reuse `test_flags()` and `http_get` from `server_test.rs` by copying them (test binaries can't share code without a common module; copying is the repo's existing convention). Add an `http_post(addr, target, body)` helper mirroring `http_get` with a `POST` request line, `Content-Length` header, and the body appended after the blank line. Test flow:

```rust
#[test]
fn ingest_snapshot_backup_restore_serve() {
    // 1. Server A: ingest via Influx line protocol, flush, snapshot.
    let flags_a = test_flags();
    let server_a = esmetrics::run(&flags_a).expect("run failed");
    let addr_a = server_a.local_addr();
    let (status, _) = http_post(addr_a, "/write", "e2e_metric,tag=v value=42 1751700000000000000");
    assert_eq!(status, "HTTP/1.1 204 No Content");
    http_get(addr_a, "/internal/force_flush");
    let (_, body) = http_get(addr_a, "/snapshot/create");
    let name = body
        .strip_prefix("{\"status\":\"ok\",\"snapshot\":\"")
        .and_then(|s| s.strip_suffix("\"}"))
        .expect("create response")
        .to_string();

    // 2. Backup the snapshot with the esm-backup library (fs:// dst).
    let backup_dir = std::env::temp_dir()
        .join(format!("esmetrics-e2e-backup-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&backup_dir);
    let snap_dir = std::path::PathBuf::from(&flags_a.storage_data_path)
        .join("snapshots")
        .join(&name);
    let src = esm_backup::localfs::LocalFs::new(&snap_dir);
    let dst = esm_backup::remote::new_remote_fs(&format!("fs://{}", backup_dir.display())).unwrap();
    esm_backup::backup::Backup {
        concurrency: 4, src: &src, dst: dst.as_ref(), origin: None, created_at: None,
    }
    .run()
    .unwrap();
    server_a.stop();

    // 3. Restore into a fresh dir and serve it with server B.
    let mut flags_b = test_flags();
    esm_backup::restore::Restore {
        concurrency: 4,
        src: dst.as_ref(),
        dst_dir: std::path::PathBuf::from(&flags_b.storage_data_path),
        skip_backup_complete_check: false,
    }
    .run()
    .unwrap();
    flags_b.http_listen_addr = "127.0.0.1:0".to_string();
    let server_b = esmetrics::run(&flags_b).expect("run failed");
    let (status, body) = http_get(
        server_b.local_addr(),
        "/api/v1/series?match[]=e2e_metric&start=2025-07-01T00:00:00Z&end=2026-07-30T00:00:00Z",
    );
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(body.contains("e2e_metric"), "restored data must be queryable: {body}");
    server_b.stop();

    let _ = std::fs::remove_dir_all(&backup_dir);
}
```

Adjust the `/write` status assertion and the `/api/v1/series` query-param shapes to whatever the existing esm-insert/esm-select tests use (check `crates/esm-insert`/`crates/esm-select` test files for the exact expected status codes and time formats; use their conventions).

- [ ] **Step 2: Run it**

Run: `cargo test -p esmetrics --test backup_e2e_test`
Expected: pass.

- [ ] **Step 3: Update docs**

`docs/PORTING.md`: add rows to the status table:

```markdown
| lib/backup + app/vmbackup | esm-backup / esbackup | done | fs/s3/gcs/azblob via object_store; no -maxBytesPerSecond, no bandwidth metrics |
| app/vmrestore | esm-backup / esrestore | done | direct-write restore (upstream -skipFilePreallocation mode) |
```

Remove `vmbackup` from the out-of-scope list (leave vmagent, vmalert, etc.).

`README.md`: add a short "Backup and restore" section: one esbackup example (`-snapshot.createURL` + `fs://` dst), one esrestore example, a note that the server must be stopped for restore, and the env-var credential table for s3/gs/azblob. Mention incremental behavior (`-dst` pointing at a previous backup) and `-origin`.

- [ ] **Step 4: Final full-workspace verification**

Run: `cargo test --workspace && cargo clippy --workspace -- -D warnings && cargo check --workspace --target x86_64-pc-windows-gnu`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/esmetrics docs/PORTING.md README.md
git commit -m "feat: backup e2e test and documentation"
```

---

## Self-Review Notes

- **Spec coverage:** snapshot source listing/reading (T2), remote backends + URL factory (T3), backup with incremental + origin + marker ordering (T4), restore with traversal guard, contiguity validation, offset-0 delete rule, in-progress lock + storage guard (T5), binaries with snapshot.createURL lifecycle (T6), e2e + docs (T7).
- **Deliberate omissions vs upstream (all documented in PORTING.md rows/README):** `-maxBytesPerSecond` throttling, `/metrics` HTTP listener in the tools, `-deleteAllObjectVersions`, S3 tuning flags (`-customS3Endpoint` → use `AWS_ENDPOINT` env, `-s3ForcePathStyle` → `AmazonS3Builder` env config), `backup_locked.ignore` retention protection, vmbackupmanager. Restore always uses direct-write mode (no `.tmp`+preallocation): crash-consistency is preserved by the offset-0 delete rule re-downloading partial files, matching upstream's `-skipFilePreallocation` mode.
- **Cross-backend copy:** `copy_part_from` returns `Ok(false)` for scheme/bucket mismatch and `cross_copy` streams via a bounded channel — covers upstream's `crossTypeCopy` including S3→GCS.
- **Type consistency check:** `Part` fields snake_case everywhere; `RemoteFs` object-safe (`&dyn` used in `Backup`/`Restore`/`cross_copy`); `LocalFs::write_part_at(&self, &Part, &mut dyn Read)` consumed by restore matches its definition in T2.
- **Restore concurrency bound:** one in-flight part buffer per worker (≤1 GiB worst case × concurrency); acceptable; noted inline with the escape hatch.
