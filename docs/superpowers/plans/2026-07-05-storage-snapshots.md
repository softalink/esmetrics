# Storage Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add instant hard-link snapshots to esm-storage plus `/snapshot/*` HTTP endpoints, porting VictoriaMetrics' snapshot mechanism (upstream `lib/storage` + `app/vmstorage` handlers).

**Architecture:** A snapshot is a directory `<storageDataPath>/snapshots/<name>/data/{small,big,indexdb}/<partition>/<part>/` whose part files are hard links to the live parts. In-memory data is force-flushed first; part wrappers are ref-counted (`Arc` clones) during linking so concurrent merges can't delete a part dir mid-link. **Divergence from upstream (deliberate):** upstream hard-links into `data/{small,big,indexdb}/snapshots/<name>` and builds the user-facing dir out of *symlinks*; we hard-link directly into the final layout with no symlinks, because symlink creation on Windows requires elevated privileges and this repo must build/run on `x86_64-pc-windows-*`. The snapshot dir is itself a valid, openable storage tree.

**Tech Stack:** Rust 1.85, edition 2021, existing workspace deps only (`parking_lot`, `serde_json`). No async. New code follows the repo's panic-on-error `must_*` convention.

## Global Constraints

- Rust edition 2021, `rust-version = "1.85"`, workspace at `/home/test/esmetrics`.
- Fully synchronous: **no tokio/async anywhere in this plan's crates**.
- Every crate must pass `cargo check --target x86_64-unknown-linux-gnu` and `cargo check --target x86_64-pc-windows-gnu` warning-free, plus `cargo clippy -- -D warnings`.
- Match upstream algorithmic behavior; on-disk *format* fidelity to upstream is a non-goal (docs/PORTING.md rule 1).
- Port unit tests alongside each module (PORTING.md rule 2).
- Snapshot name format `YYYYMMDDhhmmss-XXXXXXXX` (UTC time + 8-hex atomic counter); validation regex `^[0-9]{14}-[0-9A-Fa-f]+$` (upstream `lib/snapshot/snapshotutil`).
- HTTP JSON shapes must match upstream `app/vmstorage/main.go` exactly (documented per-endpoint in Task 4).
- Commit after each task with `<type>: <description>` conventional format, no attribution footer.

## Reference: verified existing code this plan builds on

- `crates/esm-mergeset/src/table.rs:254` — `pub fn must_create_snapshot_at(&self, dst_dir: impl AsRef<Path>)` **already exists and works** (flush → `get_parts()` refs → `must_write_part_names` → `must_hard_link_files` per part → sync). The indexdb side of every partition snapshot just calls this.
- `crates/esm-storage/src/partition/mod.rs:679` — `pub(crate) fn flush_inmemory_rows_to_files(self: &Arc<Self>)` on `PtInner` (flushes pending raw rows + in-memory parts to files, `is_final=true`).
- `crates/esm-storage/src/partition/mod.rs:188` — `PartsState { inmemory_parts, small_parts, big_parts, stopped }` guarded by `PtInner.parts: Mutex<PartsState>`.
- `crates/esm-storage/src/partition/merge.rs:700` — `pub(crate) fn must_write_part_names(pws_small: &[Arc<PartWrapper>], pws_big: &[Arc<PartWrapper>], dst_dir: &Path)` writes `parts.json` (`{"Small":[...],"Big":[...]}`), skipping in-memory parts.
- `crates/esm-storage/src/partition/mod.rs:220-228` — `PtInner` public fields: `small_parts_path`, `big_parts_path`, `index_db_parts_path`, `name: String`; `idb: IndexDb` at line 241; `Partition { pub(crate) inner: Arc<PtInner> }` at line 251.
- `crates/esm-storage/src/index/mod.rs:458` — `pub(crate) fn tb(&self) -> &Table` (the esm-mergeset Table of a partition's IndexDb).
- `crates/esm-storage/src/table.rs:295` — `pub fn get_all_partitions(&self) -> Vec<Arc<PartitionWrapper>>`; `PartitionWrapper::pt(&self) -> &Partition` at line 52. Holding the `Arc<PartitionWrapper>` prevents partition drop.
- `crates/esm-storage/src/table.rs:22-25` — dir name consts `SMALL_DIRNAME="small"`, `BIG_DIRNAME="big"`, `INDEXDB_DIRNAME="indexdb"`, `SNAPSHOTS_DIRNAME="snapshots"`; partition-name scanners already skip a `snapshots` entry.
- `crates/esm-storage/src/storage/mod.rs:118` — `StorageInner.tb: Table`; `path: PathBuf` field; `Storage { inner: Arc<StorageInner> }` at line 159; `must_open` at 184.
- `crates/esm-common/src/fs.rs` — `must_hard_link_files` (221, non-recursive, skips dirs/symlinks, syncs dst), `must_mkdir_fail_if_exist` (164), `must_mkdir_if_not_exist` (152), `must_sync_path_and_parent_dir` (49), `must_read_dir` (200), `is_path_exist` (190), `must_remove_dir` (561, recursive).
- `crates/esmetrics/src/lib.rs:115` — `request_handler(req, w, insert, select, storage)` router; add `/snapshot/*` arms next to `/internal/force_flush` (line ~150).
- `crates/esmetrics/src/flags.rs` — flag = entry in `FLAG_DEFS` (line 24) + field in `Flags` (line 82) + `Default` + `parse` match arm (line ~202) + `set_flag` (line ~229).
- Test conventions: `crates/esm-storage/tests/storage_test.rs` (helpers `test_dir`, `open_storage`, `make_row`, `metric_group_filter`, `read_series` — reuse by copying into the new test file); `crates/esmetrics/tests/server_test.rs` (`test_flags()`, `http_get(addr, target)`).

---

### Task 1: Snapshot name generation and validation

**Files:**
- Create: `crates/esm-storage/src/snapshot.rs`
- Modify: `crates/esm-storage/src/lib.rs` (add `mod snapshot;`)

**Interfaces:**
- Produces: `pub(crate) fn new_name() -> String`, `pub(crate) fn validate_name(name: &str) -> Result<(), String>` in `esm-storage::snapshot`. Task 3 consumes both.

- [ ] **Step 1: Write the module with failing unit tests**

Create `crates/esm-storage/src/snapshot.rs`:

```rust
//! Snapshot name generation and validation.
//! Go: lib/snapshot/snapshotutil/snapshotutil.go

use std::sync::atomic::{AtomicU64, Ordering};

/// Generates a new snapshot name: UTC `YYYYMMDDhhmmss` + `-` + 8-hex counter.
/// Go: snapshotutil.NewName.
pub(crate) fn new_name() -> String {
    static NEXT_IDX: AtomicU64 = AtomicU64::new(0);
    // Seed once from wall-clock nanos so names stay unique across restarts.
    static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let seed = *SEED.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    });
    let idx = seed.wrapping_add(NEXT_IDX.fetch_add(1, Ordering::Relaxed));
    format!("{}-{:08X}", utc_compact_timestamp(), idx as u32)
}

/// Returns the current UTC time formatted as `YYYYMMDDhhmmss` without
/// pulling in a date-time dependency (days-from-civil algorithm).
fn utc_compact_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_compact_timestamp(secs)
}

fn format_compact_timestamp(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil_from_days.
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
    format!("{year:04}{month:02}{d:02}{h:02}{m:02}{s:02}")
}

/// Validates a snapshot name. Go: snapshotutil.Validate
/// (regex `^[0-9]{14}-[0-9A-Fa-f]+$`).
pub(crate) fn validate_name(name: &str) -> Result<(), String> {
    let err = || format!("invalid snapshot name {name:?}");
    let (ts, idx) = name.split_once('-').ok_or_else(err)?;
    if ts.len() != 14 || !ts.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err());
    }
    if idx.is_empty() || !idx.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(err());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_names_are_valid_and_unique() {
        let a = new_name();
        let b = new_name();
        assert_ne!(a, b);
        validate_name(&a).unwrap();
        validate_name(&b).unwrap();
    }

    #[test]
    fn format_compact_timestamp_known_values() {
        assert_eq!(format_compact_timestamp(0), "19700101000000");
        // 2026-07-05 12:34:56 UTC
        assert_eq!(format_compact_timestamp(1_783_082_096), "20260705123456");
    }

    #[test]
    fn validate_rejects_bad_names() {
        assert!(validate_name("").is_err());
        assert!(validate_name("20260705123456").is_err()); // no dash/idx
        assert!(validate_name("2026070512345-0A").is_err()); // 13 digits
        assert!(validate_name("20260705123456-").is_err()); // empty idx
        assert!(validate_name("20260705123456-XYZ").is_err()); // non-hex
        assert!(validate_name("20260705123456-0000000A").is_ok());
        assert!(validate_name("20260705123456-deadBEEF").is_ok());
    }
}
```

Add to `crates/esm-storage/src/lib.rs` next to the other `mod` declarations:

```rust
mod snapshot;
```

- [ ] **Step 2: Verify the known-value test is right before trusting it**

Run: `date -u -d @1783082096 +%Y%m%d%H%M%S`
Expected: `20260705123456`. If it differs, fix the constant in the test to the command's output.

- [ ] **Step 3: Run the tests**

Run: `cargo test -p esm-storage --lib snapshot`
Expected: 3 passed.

- [ ] **Step 4: Lint**

Run: `cargo clippy -p esm-storage -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/esm-storage/src/snapshot.rs crates/esm-storage/src/lib.rs
git commit -m "feat: add snapshot name generation/validation to esm-storage"
```

---

### Task 2: Partition- and table-level snapshot creation

**Files:**
- Modify: `crates/esm-storage/src/partition/mod.rs` (new method on `Partition`, near `debug_flush` at line 467)
- Modify: `crates/esm-storage/src/table.rs` (new method on `Table`, near `get_all_partitions` at line 295)
- Test: `crates/esm-storage/tests/snapshot_test.rs` (created here; asserts via Task 3's Storage API, so it only compiles fully at the end of Task 3 — write it now, keep it failing)

**Interfaces:**
- Consumes: `PtInner.flush_inmemory_rows_to_files`, `must_write_part_names`, `esm_common::fs::must_hard_link_files`, `IndexDb::tb()`, mergeset `Table::must_create_snapshot_at`.
- Produces:
  - `impl Partition { pub(crate) fn must_create_snapshot_at(&self, small_dst: &Path, big_dst: &Path, indexdb_dst: &Path) }`
  - `impl Table { pub(crate) fn must_create_snapshot_at(&self, dst_data_dir: &Path) }` — creates `dst_data_dir/{small,big,indexdb}/<partition-name>` for every live partition. Task 3 consumes this.

- [ ] **Step 1: Write the failing integration test**

Create `crates/esm-storage/tests/snapshot_test.rs`. Copy the helpers `test_dir`, `now_ms`, `open_storage`, `make_row`, `metric_group_filter`, `read_series` **verbatim** from `crates/esm-storage/tests/storage_test.rs` (lines 17–90), changing the `test_dir` prefix string to `"esm-storage-snapshot-test-{name}-{}"`. Then add:

```rust
const RETENTION: i64 = RETENTION_365D_MSECS;

/// Ingests `n` rows of a gauge, snapshots, and verifies the snapshot dir is
/// a complete, independently openable storage containing the same data.
#[test]
fn snapshot_is_openable_and_complete() {
    let dir = test_dir("openable");
    let storage = open_storage(&dir, RETENTION);
    let ts0 = now_ms() - 1000 * 60;
    let rows: Vec<_> = (0..1000)
        .map(|i| make_row("snap_gauge", &[("i", "x")], ts0 + i * 10, i as f64))
        .collect();
    storage.add_rows(&rows, 64);
    storage.force_flush();

    let name = storage.must_create_snapshot();
    // Names are valid per the snapshotutil format.
    assert_eq!(storage.must_list_snapshots(), vec![name.clone()]);

    // Rows added AFTER the snapshot must not leak into it.
    let late = make_row("snap_gauge", &[("i", "x")], ts0 + 100_000, 1.0);
    storage.add_rows(&[late], 64);
    storage.force_flush();

    // The snapshot is a valid storage tree of its own.
    let snap_path = dir.join("snapshots").join(&name);
    let snap_storage = open_storage(&snap_path, RETENTION);
    let tr = TimeRange {
        min_timestamp: ts0 - MSEC_PER_DAY,
        max_timestamp: ts0 + MSEC_PER_DAY,
    };
    let tfs = metric_group_filter("snap_gauge");
    let series = read_series(&snap_storage, &[tfs], tr);
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].1.len(), 1000, "snapshot must contain exactly the pre-snapshot rows");
    snap_storage.must_close();

    storage.must_close();
    let _ = std::fs::remove_dir_all(&dir);
}
```

(The other tests in this file are added in Task 3 Step 1.)

- [ ] **Step 2: Run it to confirm it fails to compile**

Run: `cargo test -p esm-storage --test snapshot_test 2>&1 | head -20`
Expected: compile error — `must_create_snapshot` not found on `Storage`.

- [ ] **Step 3: Implement `Partition::must_create_snapshot_at`**

In `crates/esm-storage/src/partition/mod.rs`, inside `impl Partition` (after `debug_flush`, line ~470):

```rust
    /// Creates a snapshot of the partition's file parts at the given
    /// destination dirs using hard links. Go: partition.MustCreateSnapshotAt.
    ///
    /// In-memory rows/parts are force-flushed to files first; parts that are
    /// still purely in-memory afterwards are skipped (matches upstream).
    pub(crate) fn must_create_snapshot_at(
        &self,
        small_dst: &Path,
        big_dst: &Path,
        indexdb_dst: &Path,
    ) {
        self.inner.flush_inmemory_rows_to_files();

        // Ref the current file parts under the parts lock so a concurrent
        // merge cannot drop their directories while we hard-link them
        // (PartWrapper::Drop removes dirs only when the last Arc goes away).
        let (pws_small, pws_big) = {
            let state = self.inner.parts.lock();
            (state.small_parts.clone(), state.big_parts.clone())
        };

        esm_common::fs::must_mkdir_fail_if_exist(small_dst);
        esm_common::fs::must_mkdir_fail_if_exist(big_dst);

        // parts.json lives in the small dir and lists both small and big
        // part names (matches must_create_partition / swap_src_with_dst_parts).
        crate::partition::merge::must_write_part_names(&pws_small, &pws_big, small_dst);

        Self::must_hard_link_parts(&pws_small, small_dst);
        Self::must_hard_link_parts(&pws_big, big_dst);

        esm_common::fs::must_sync_path_and_parent_dir(small_dst);
        esm_common::fs::must_sync_path_and_parent_dir(big_dst);

        // The per-partition inverted index is an esm-mergeset table, which
        // already knows how to snapshot itself.
        self.inner.idb.tb().must_create_snapshot_at(indexdb_dst);
    }

    fn must_hard_link_parts(pws: &[Arc<PartWrapper>], dst_dir: &Path) {
        for pw in pws {
            if pw.mp.is_some() {
                continue; // skip in-memory parts
            }
            let src_part_path = &pw.p.path;
            let part_name = src_part_path.file_name().unwrap_or_else(|| {
                panic!("BUG: part path {src_part_path:?} has no base name")
            });
            esm_common::fs::must_hard_link_files(src_part_path, dst_dir.join(part_name));
        }
    }
```

If `must_write_part_names` is not visible from `mod.rs` (it is `pub(crate)` in `merge.rs`, so `crate::partition::merge::must_write_part_names` works only if `merge` is a `pub(crate) mod`; check the `mod merge;` declaration at the top of `partition/mod.rs` and adjust the path — a plain `merge::must_write_part_names(...)` call is correct if `merge` is a child module of `partition`).

- [ ] **Step 4: Implement `Table::must_create_snapshot_at`**

In `crates/esm-storage/src/table.rs`, inside `impl Table` (after `get_all_partitions`):

```rust
    /// Creates a snapshot of all partitions under `dst_data_dir`, producing
    /// `dst_data_dir/{small,big,indexdb}/<partition>/...` hard-link trees.
    /// Go: table.MustCreateSnapshot (layout adapted: no symlink indirection).
    pub(crate) fn must_create_snapshot_at(&self, dst_data_dir: &Path) {
        let dst_small = dst_data_dir.join(SMALL_DIRNAME);
        let dst_big = dst_data_dir.join(BIG_DIRNAME);
        let dst_indexdb = dst_data_dir.join(INDEXDB_DIRNAME);
        esm_common::fs::must_mkdir_fail_if_exist(dst_data_dir);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_small);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_big);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_indexdb);

        // Holding the Arc<PartitionWrapper>s keeps every partition alive
        // (retention/drop is deferred until the refs are released).
        let ptws = self.get_all_partitions();
        for ptw in &ptws {
            let pt = ptw.pt();
            let name = &pt.inner.name;
            pt.must_create_snapshot_at(
                &dst_small.join(name),
                &dst_big.join(name),
                &dst_indexdb.join(name),
            );
        }

        esm_common::fs::must_sync_path_and_parent_dir(&dst_small);
        esm_common::fs::must_sync_path_and_parent_dir(&dst_big);
        esm_common::fs::must_sync_path_and_parent_dir(&dst_indexdb);
    }
```

Check the imports at the top of each file (`std::path::Path`, `std::sync::Arc`) and add what's missing.

- [ ] **Step 5: Verify it compiles (test still fails — Storage API missing)**

Run: `cargo check -p esm-storage && cargo clippy -p esm-storage -- -D warnings`
Expected: clean. (`cargo test --test snapshot_test` still fails compile on `must_create_snapshot` — that's Task 3.)

- [ ] **Step 6: Commit**

```bash
git add crates/esm-storage/src/partition/mod.rs crates/esm-storage/src/table.rs crates/esm-storage/tests/snapshot_test.rs
git commit -m "feat: partition/table hard-link snapshot creation"
```

---

### Task 3: Storage-level snapshot API (create/list/delete)

**Files:**
- Modify: `crates/esm-storage/src/storage/mod.rs`
- Test: `crates/esm-storage/tests/snapshot_test.rs` (extend)

**Interfaces:**
- Consumes: Task 1 `snapshot::{new_name, validate_name}`, Task 2 `Table::must_create_snapshot_at`.
- Produces (consumed by esmetrics HTTP layer in Task 4 and by esbackup later):
  - `pub fn must_create_snapshot(&self) -> String`
  - `pub fn must_list_snapshots(&self) -> Vec<String>`
  - `pub fn delete_snapshot(&self, name: &str) -> Result<(), String>`

- [ ] **Step 1: Extend the integration test**

Append to `crates/esm-storage/tests/snapshot_test.rs`:

```rust
#[test]
fn list_and_delete_snapshots() {
    let dir = test_dir("list-delete");
    let storage = open_storage(&dir, RETENTION);
    let row = make_row("del_gauge", &[("a", "b")], now_ms(), 42.0);
    storage.add_rows(&[row], 64);
    storage.force_flush();

    assert!(storage.must_list_snapshots().is_empty());
    let s1 = storage.must_create_snapshot();
    let s2 = storage.must_create_snapshot();
    let listed = storage.must_list_snapshots();
    assert_eq!(listed.len(), 2);
    assert!(listed.contains(&s1) && listed.contains(&s2));
    // Sorted ascending (names sort chronologically).
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(listed, sorted);

    // Deleting a snapshot removes its dir and must not disturb live data
    // (hard links: the live parts keep their inodes).
    storage.delete_snapshot(&s1).unwrap();
    assert_eq!(storage.must_list_snapshots(), vec![s2.clone()]);
    assert!(!dir.join("snapshots").join(&s1).exists());

    // Unknown / invalid names are errors, not panics.
    assert!(storage.delete_snapshot(&s1).is_err());
    assert!(storage.delete_snapshot("../../etc").is_err());
    assert!(storage.delete_snapshot("").is_err());

    storage.delete_snapshot(&s2).unwrap();
    storage.must_close();

    // Live data survived both deletions.
    let reopened = open_storage(&dir, RETENTION);
    let tr = TimeRange { min_timestamp: now_ms() - MSEC_PER_DAY, max_timestamp: now_ms() + MSEC_PER_DAY };
    let series = read_series(&reopened, &[metric_group_filter("del_gauge")], tr);
    assert_eq!(series.len(), 1);
    reopened.must_close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_under_concurrent_ingestion() {
    let dir = test_dir("concurrent");
    let storage = Arc::new(open_storage(&dir, RETENTION));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let ingest = {
        let storage = Arc::clone(&storage);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let ts0 = now_ms();
            let mut i = 0i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let row = make_row("conc_gauge", &[("w", "1")], ts0 + i, i as f64);
                storage.add_rows(&[row], 64);
                i += 1;
            }
        })
    };

    // Snapshots taken while ingesting must each be valid openable storages.
    let mut names = Vec::new();
    for _ in 0..3 {
        names.push(storage.must_create_snapshot());
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    ingest.join().unwrap();

    for name in &names {
        let snap = open_storage(&dir.join("snapshots").join(name), RETENTION);
        snap.must_close(); // opening validates parts.json vs part dirs
    }
    for name in &names {
        storage.delete_snapshot(name).unwrap();
    }
    match Arc::try_unwrap(storage) {
        Ok(s) => s.must_close(),
        Err(_) => panic!("storage still referenced"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: Run to confirm compile failure**

Run: `cargo test -p esm-storage --test snapshot_test 2>&1 | head -5`
Expected: `must_create_snapshot` not found.

- [ ] **Step 3: Implement the Storage API**

In `crates/esm-storage/src/storage/mod.rs`:

1. Add to `StorageInner` fields (near `path: PathBuf`):

```rust
    /// Serializes snapshot creation. Go: Storage.snapshotLock.
    snapshot_lock: Mutex<()>,
```

(`parking_lot::Mutex` is already imported in this module; if not: `use parking_lot::Mutex;`. Initialize `snapshot_lock: Mutex::new(())` where `StorageInner` is constructed in `must_open`.)

2. Add a dirname const next to the existing ones:

```rust
const SNAPSHOTS_DIRNAME: &str = "snapshots";
```

3. Add methods to `impl Storage` (near `force_flush`):

```rust
    /// Creates a new snapshot under `<path>/snapshots/<name>` and returns
    /// the name. Instant (hard links). Go: Storage.MustCreateSnapshot.
    pub fn must_create_snapshot(&self) -> String {
        let _guard = self.inner.snapshot_lock.lock();
        let started = std::time::Instant::now();
        let name = crate::snapshot::new_name();
        log::info!("creating storage snapshot {name:?}...");

        let snapshots_dir = self.inner.path.join(SNAPSHOTS_DIRNAME);
        esm_common::fs::must_mkdir_if_not_exist(&snapshots_dir);
        let dst_dir = snapshots_dir.join(&name);
        esm_common::fs::must_mkdir_fail_if_exist(&dst_dir);

        self.inner.tb.must_create_snapshot_at(&dst_dir.join("data"));

        esm_common::fs::must_sync_path_and_parent_dir(&dst_dir);
        log::info!(
            "created storage snapshot {name:?} in {:.3} seconds",
            started.elapsed().as_secs_f64()
        );
        name
    }

    /// Returns the sorted list of existing snapshot names.
    /// Go: Storage.MustListSnapshots.
    pub fn must_list_snapshots(&self) -> Vec<String> {
        let snapshots_dir = self.inner.path.join(SNAPSHOTS_DIRNAME);
        if !esm_common::fs::is_path_exist(&snapshots_dir) {
            return Vec::new();
        }
        let mut names: Vec<String> = esm_common::fs::must_read_dir(&snapshots_dir)
            .iter()
            .filter_map(|e| e.file_name().to_str().map(str::to_owned))
            .filter(|name| crate::snapshot::validate_name(name).is_ok())
            .collect();
        names.sort();
        names
    }

    /// Deletes the snapshot with the given name. Go: Storage.DeleteSnapshot.
    pub fn delete_snapshot(&self, name: &str) -> Result<(), String> {
        crate::snapshot::validate_name(name)
            .map_err(|e| format!("invalid snapshotName {name:?}: {e}"))?;
        // Only delete names we actually listed (defense in depth against
        // path tricks; validate_name already excludes separators).
        if !self.must_list_snapshots().iter().any(|s| s == name) {
            return Err(format!("cannot find snapshot {name:?}"));
        }
        let started = std::time::Instant::now();
        log::info!("deleting snapshot {name:?}...");
        let snapshot_path = self.inner.path.join(SNAPSHOTS_DIRNAME).join(name);
        esm_common::fs::must_remove_dir(&snapshot_path);
        log::info!(
            "deleted snapshot {name:?} in {:.3} seconds",
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p esm-storage --test snapshot_test`
Expected: 3 passed (`snapshot_is_openable_and_complete`, `list_and_delete_snapshots`, `snapshot_under_concurrent_ingestion`).

- [ ] **Step 5: Run the full esm-storage suite + clippy (regressions)**

Run: `cargo test -p esm-storage && cargo clippy -p esm-storage -- -D warnings`
Expected: all pass, clean.

- [ ] **Step 6: Commit**

```bash
git add crates/esm-storage/src/storage/mod.rs crates/esm-storage/tests/snapshot_test.rs
git commit -m "feat: Storage snapshot create/list/delete API"
```

---

### Task 4: `/snapshot/*` HTTP endpoints + `-snapshotAuthKey` flag

**Files:**
- Modify: `crates/esmetrics/src/lib.rs` (router)
- Modify: `crates/esmetrics/src/flags.rs` (new flag)
- Modify: `crates/esmetrics/Cargo.toml` (add workspace `serde_json` dep for response/escape helpers)
- Test: `crates/esmetrics/tests/server_test.rs` (extend)

**Interfaces:**
- Consumes: Task 3 Storage API.
- Produces HTTP endpoints (upstream `app/vmstorage/main.go` shapes):
  - `GET/POST /snapshot/create` → `200 {"status":"ok","snapshot":"<name>"}`
  - `GET /snapshot/list` → `200 {"status":"ok","snapshots":["<n>",...]}`
  - `GET/POST /snapshot/delete?snapshot=<name>` → `200 {"status":"ok"}` or `500 {"status":"error","msg":"..."}`
  - `GET/POST /snapshot/delete_all` → `200 {"status":"ok"}`
  - `GET/POST /api/v1/admin/tsdb/snapshot` (Prometheus alias) → `200 {"status":"success","data":{"name":"<name>"}}`
  - All `/snapshot*` paths require `authKey=<value>` query param iff `-snapshotAuthKey` is non-empty; mismatch → `401 text/plain`.

- [ ] **Step 1: Write the failing server tests**

Append to `crates/esmetrics/tests/server_test.rs`:

```rust
#[test]
fn snapshot_endpoints_roundtrip() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let addr = server.local_addr();

    let (status, body) = http_get(addr, "/snapshot/create");
    assert_eq!(status, "HTTP/1.1 200 OK");
    let name = body
        .strip_prefix("{\"status\":\"ok\",\"snapshot\":\"")
        .and_then(|s| s.strip_suffix("\"}"))
        .unwrap_or_else(|| panic!("unexpected create body: {body:?}"))
        .to_string();

    let (status, body) = http_get(addr, "/snapshot/list");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, format!("{{\"status\":\"ok\",\"snapshots\":[\"{name}\"]}}"));

    let (status, body) = http_get(addr, &format!("/snapshot/delete?snapshot={name}"));
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "{\"status\":\"ok\"}");

    // Deleting again → error JSON with 500.
    let (status, body) = http_get(addr, &format!("/snapshot/delete?snapshot={name}"));
    assert_eq!(status, "HTTP/1.1 500 Internal Server Error");
    assert!(body.starts_with("{\"status\":\"error\""), "body: {body:?}");

    // create + delete_all
    http_get(addr, "/snapshot/create");
    http_get(addr, "/snapshot/create");
    let (status, body) = http_get(addr, "/snapshot/delete_all");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(body, "{\"status\":\"ok\"}");
    let (_, body) = http_get(addr, "/snapshot/list");
    assert_eq!(body, "{\"status\":\"ok\",\"snapshots\":[]}");

    server.stop();
}

#[test]
fn prometheus_admin_snapshot_alias() {
    let server = esmetrics::run(&test_flags()).expect("run failed");
    let (status, body) = http_get(server.local_addr(), "/api/v1/admin/tsdb/snapshot");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        body.starts_with("{\"status\":\"success\",\"data\":{\"name\":\""),
        "body: {body:?}"
    );
    server.stop();
}

#[test]
fn snapshot_auth_key_enforced() {
    let mut flags = test_flags();
    flags.snapshot_auth_key = "sekret".to_string();
    let server = esmetrics::run(&flags).expect("run failed");
    let addr = server.local_addr();

    let (status, _) = http_get(addr, "/snapshot/list");
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");
    let (status, _) = http_get(addr, "/snapshot/list?authKey=wrong");
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");
    let (status, _) = http_get(addr, "/snapshot/list?authKey=sekret");
    assert_eq!(status, "HTTP/1.1 200 OK");

    server.stop();
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p esmetrics --test server_test 2>&1 | tail -5`
Expected: compile error (`snapshot_auth_key` field missing) — that's the red state.

- [ ] **Step 3: Add the flag**

In `crates/esmetrics/src/flags.rs`, following the existing five-place pattern exactly:
- `FLAG_DEFS` entry: `("snapshotAuthKey", "", "authKey, which must be passed in query string to /snapshot* pages")`
- `Flags` struct field: `pub snapshot_auth_key: String,`
- `Default` impl: `snapshot_auth_key: String::new(),`
- `parse` match arm and `set_flag` arm: `"snapshotAuthKey" => self.snapshot_auth_key = value.to_string(),` (mirror however the sibling string flags are written in each of the two match sites).

- [ ] **Step 4: Add serde_json + implement the routes**

In `crates/esmetrics/Cargo.toml` under `[dependencies]`: `serde_json.workspace = true`.

In `crates/esmetrics/src/lib.rs`:

1. Thread the auth key into the router. `request_handler` already receives everything via the closure in `run()`; capture `flags.snapshot_auth_key.clone()` there and pass it as a new `snapshot_auth_key: &str` parameter to `request_handler` (update the closure call site accordingly).

2. Add the handler arms. Snapshot names never need escaping (validated `[0-9A-Fa-f-]`), error messages do — use `serde_json::to_string` for message escaping. Insert **before** the `match path` (because `/snapshot/*` is a prefix family, handle it first):

```rust
    if path.starts_with("/snapshot") || path == "/api/v1/admin/tsdb/snapshot" {
        handle_snapshot_request(req, w, storage, snapshot_auth_key);
        return;
    }
```

3. Add the functions at module level:

```rust
/// Handles /snapshot/{create,list,delete,delete_all} and the Prometheus
/// /api/v1/admin/tsdb/snapshot alias. Go: app/vmstorage RequestHandler.
fn handle_snapshot_request(
    req: &mut Request<'_>,
    w: &mut ResponseWriter<'_>,
    storage: &Arc<Storage>,
    auth_key: &str,
) {
    if !auth_key.is_empty() {
        let supplied = query_param(req, "authKey");
        if supplied.as_deref() != Some(auth_key) {
            w.set_status(401);
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(b"The provided authKey doesn't match -snapshotAuthKey\n");
            return;
        }
    }

    let path = req.path();
    let prometheus_compatible = path == "/api/v1/admin/tsdb/snapshot";
    let action = if prometheus_compatible {
        "/create"
    } else {
        path.strip_prefix("/snapshot").unwrap_or("")
    };

    match action {
        "/create" => {
            let name = storage.must_create_snapshot();
            if prometheus_compatible {
                w.write_json(
                    200,
                    &format!("{{\"status\":\"success\",\"data\":{{\"name\":\"{name}\"}}}}"),
                );
            } else {
                w.write_json(200, &format!("{{\"status\":\"ok\",\"snapshot\":\"{name}\"}}"));
            }
        }
        "/list" => {
            let names = storage.must_list_snapshots();
            let quoted: Vec<String> = names.iter().map(|n| format!("\"{n}\"")).collect();
            w.write_json(
                200,
                &format!("{{\"status\":\"ok\",\"snapshots\":[{}]}}", quoted.join(",")),
            );
        }
        "/delete" => {
            let name = query_param(req, "snapshot").unwrap_or_default();
            match storage.delete_snapshot(&name) {
                Ok(()) => w.write_json(200, "{\"status\":\"ok\"}"),
                Err(e) => write_json_error(w, &e),
            }
        }
        "/delete_all" => {
            for name in storage.must_list_snapshots() {
                if let Err(e) = storage.delete_snapshot(&name) {
                    write_json_error(w, &e);
                    return;
                }
            }
            w.write_json(200, "{\"status\":\"ok\"}");
        }
        _ => {
            w.set_status(404);
            w.set_content_type("text/plain; charset=utf-8");
            w.write_body(format!("unsupported path requested: {path:?}\n").as_bytes());
        }
    }
}

fn write_json_error(w: &mut ResponseWriter<'_>, msg: &str) {
    let quoted = serde_json::to_string(msg).unwrap_or_else(|_| "\"internal error\"".to_string());
    w.write_json(500, &format!("{{\"status\":\"error\",\"msg\":{quoted}}}"));
}

fn query_param(req: &Request<'_>, key: &str) -> Option<String> {
    req.query_params()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}
```

Check `write_json`'s exact behavior in `crates/esm-http/src/response.rs:93` (it takes `(status, json)`), and whether `query_params()` needs `&mut Request` — adjust receiver mutability to match. Add missing `use` items.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p esmetrics --test server_test`
Expected: all pass, including the three new ones.

- [ ] **Step 6: Full workspace check on both targets + clippy**

Run: `cargo clippy --workspace -- -D warnings && cargo check --workspace --target x86_64-unknown-linux-gnu && cargo check --workspace --target x86_64-pc-windows-gnu`
Expected: clean on all three.

- [ ] **Step 7: Commit**

```bash
git add crates/esmetrics crates/esm-storage
git commit -m "feat: /snapshot/* HTTP endpoints with -snapshotAuthKey"
```

---

### Task 5: Documentation updates

**Files:**
- Modify: `docs/PORTING.md` (status table + out-of-scope list)
- Modify: `README.md` (feature mention, if the README lists supported endpoints)

**Interfaces:** none (docs only).

- [ ] **Step 1: Update PORTING.md**

In the module status table, change the snapshots note: remove "snapshots API" from the out-of-scope paragraph (lines ~49-53) and add a table row:

```markdown
| lib/storage snapshots + /snapshot/* API | esm-storage / esmetrics | done | hard-link snapshots; no symlink indirection (Windows) |
```

- [ ] **Step 2: Update README.md**

Read `README.md`; if it enumerates HTTP endpoints or features, add `/snapshot/create|list|delete|delete_all` with one sentence. If it doesn't enumerate endpoints, skip this step.

- [ ] **Step 3: Commit**

```bash
git add docs/PORTING.md README.md
git commit -m "docs: record snapshot support in PORTING.md and README"
```

---

## Self-Review Notes

- Windows: `must_hard_link_files` already exists and is used by esm-mergeset snapshots; hard links work on NTFS without privileges. No symlinks anywhere in this plan (deliberate divergence, documented in header and PORTING.md).
- Concurrency: `Arc<PartWrapper>` clones under the parts lock replicate upstream's `incRefForParts`; `Arc<PartitionWrapper>` from `get_all_partitions` replicates partition refs; global `snapshot_lock` replicates upstream's `snapshotLock`.
- Skipped upstream features (document, don't build — YAGNI for the backup pipeline): `-snapshotsMaxAge` stale remover, `-snapshotCreateTimeout` (deprecated upstream), legacy top-level indexdb snapshotting (Rust port has no legacy IDBs), `metadata/` dir copy (Rust port has no metadata dir).
- `delete_snapshot` validates the name *and* requires it to be in the listed set before removing — same guard as upstream `vmstorage.deleteSnapshot`.
