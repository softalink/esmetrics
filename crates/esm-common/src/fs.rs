// Ported from the upstream VictoriaMetrics lib/fs (v1.146.0).
//! Filesystem helpers with the upstream's `Must*` semantics: unrecoverable FS errors
//! panic with a clear message (mirroring `logger.Panicf` in Go).
//!
//! Deviations from Go:
//! - `must_remove_dir` keeps the `.delete-this-dir` marker protocol (so
//!   `is_partially_removed_dir` works across unclean shutdowns), but replaces
//!   Go's background NFS dir remover with a bounded synchronous retry loop.
//! - `ReaderAt` opens and mmaps the file eagerly (Go opens lazily) and does
//!   not port the `mincore()` page-residency tracking or the
//!   `-fs.disableMmap` flag; files are always mmapped when non-empty.
//! - `must_fadvise_sequential_read` issues `POSIX_FADV_SEQUENTIAL` and
//!   (optionally) `POSIX_FADV_WILLNEED` as two calls on Linux (Go ORs the
//!   constants, which collapses to `WILLNEED` alone); no-op elsewhere.
//! - Directory fsync is a no-op on Windows, like Go's `fs_windows.go`.
//! - File-close errors cannot be observed via `std::fs::File`, so they are
//!   ignored instead of panicking.
//! - Metrics counters and the disk-space cache metric registration
//!   (`RegisterPathFsMetrics`) are not ported.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::filestream;

/// Returns true if fsync must be disabled (upstream `fsutil.IsFsyncDisabled`).
///
/// Fsync is controlled by the `DISABLE_FSYNC_FOR_TESTING` environment
/// variable; when unset it defaults to disabled in `cfg(test)` builds
/// (mirroring Go's `testing.Testing()` default) and enabled otherwise.
pub fn is_fsync_disabled() -> bool {
    static DISABLED: OnceLock<bool> = OnceLock::new();
    *DISABLED.get_or_init(|| match std::env::var("DISABLE_FSYNC_FOR_TESTING") {
        Ok(s) if !s.is_empty() => s.parse::<bool>().unwrap_or(false),
        _ => cfg!(test),
    })
}

/// Fsyncs the path and its parent dir (upstream `MustSyncPathAndParentDir`).
///
/// This guarantees the path is visible and readable after unclean shutdown.
pub fn must_sync_path_and_parent_dir(path: impl AsRef<Path>) {
    let path = path.as_ref();
    must_sync_path(path);
    if let Some(parent) = path.parent() {
        must_sync_path(parent);
    }
}

/// Syncs the contents of the given path (upstream `MustSyncPath`).
///
/// On Windows this is a no-op (directories cannot be fsynced there),
/// mirroring Go's `fs_windows.go`.
pub fn must_sync_path(path: impl AsRef<Path>) {
    let path = path.as_ref();
    if is_fsync_disabled() {
        // Just check that the path exists.
        if !is_path_exist(path) {
            panic!("FATAL: cannot fsync missing {path:?}");
        }
        return;
    }
    must_sync_path_internal(path);
}

#[cfg(unix)]
fn must_sync_path_internal(path: &Path) {
    let f =
        File::open(path).unwrap_or_else(|e| panic!("FATAL: cannot open {path:?} for fsync: {e}"));
    if let Err(e) = f.sync_all() {
        panic!("FATAL: cannot flush {path:?} to storage: {e}");
    }
}

#[cfg(windows)]
fn must_sync_path_internal(_path: &Path) {
    // On Windows only files can be synced; sync for directories is not
    // supported, so this is a no-op like in Go's fs_windows.go.
}

/// Writes data to the file at path and fsyncs it (upstream `MustWriteSync`).
///
/// This may leave the file in an inconsistent state on app crash in the
/// middle of the write; use [`must_write_atomic`] for all-or-nothing writes.
pub fn must_write_sync(path: impl AsRef<Path>, data: &[u8]) {
    let path = path.as_ref();
    let mut f = filestream::Writer::must_create(path, false);
    if let Err(e) = f.write_all(data) {
        // Do not remove the file, so the user could inspect its contents
        // during investigation of the issue.
        panic!("FATAL: cannot write {} bytes to {path:?}: {e}", data.len());
    }
    f.must_close();
}

static TMP_FILE_NUM: AtomicU64 = AtomicU64::new(0);

/// Atomically writes data to the given file path (upstream `MustWriteAtomic`).
///
/// Returns only after the file is fully written and synced to storage.
/// If the file already exists, it is overwritten atomically when
/// `can_overwrite` is true; otherwise the function panics.
pub fn must_write_atomic(path: impl AsRef<Path>, data: &[u8], can_overwrite: bool) {
    let path = path.as_ref();
    // It is expected that this function cannot be called concurrently
    // with the same `path`.
    if is_path_exist(path) && !can_overwrite {
        panic!("FATAL: cannot create file {path:?}, since it already exists");
    }

    // Write data to a temporary file.
    let n = TMP_FILE_NUM.fetch_add(1, Ordering::Relaxed) + 1;
    let mut tmp_path = path.as_os_str().to_os_string();
    tmp_path.push(format!(".tmp.{n}"));
    let tmp_path = PathBuf::from(tmp_path);
    must_write_sync(&tmp_path, data);

    // Atomically move the temporary file to path.
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        // Do not remove tmp_path, so the user could inspect its contents
        // during investigation of the issue.
        panic!("FATAL: cannot move temporary file {tmp_path:?} to {path:?}: {e}");
    }

    // Sync the containing directory, so the file is guaranteed
    // to appear in the directory.
    let abs_path = std::path::absolute(path)
        .unwrap_or_else(|e| panic!("FATAL: cannot obtain absolute path to {path:?}: {e}"));
    if let Some(parent) = abs_path.parent() {
        must_sync_path(parent);
    }
}

/// Returns true if `file_name` matches the temporary file name pattern
/// from [`must_write_atomic`] (upstream `IsTemporaryFileName`).
pub fn is_temporary_file_name(file_name: &str) -> bool {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"\.tmp\.\d+$").unwrap())
        .is_match(file_name)
}

/// Creates the given dir if it doesn't exist (upstream `MustMkdirIfNotExist`).
///
/// The caller is responsible for `must_sync_path` on the parent directory.
pub fn must_mkdir_if_not_exist(path: impl AsRef<Path>) {
    let path = path.as_ref();
    if is_path_exist(path) {
        return;
    }
    must_mkdir(path);
}

/// Creates the given dir, panicking if it already exists
/// (upstream `MustMkdirFailIfExist`).
///
/// The caller is responsible for `must_sync_path` on the parent directory.
pub fn must_mkdir_fail_if_exist(path: impl AsRef<Path>) {
    let path = path.as_ref();
    if is_path_exist(path) {
        panic!("FATAL: the {path:?} already exists");
    }
    must_mkdir(path);
}

fn must_mkdir(path: &Path) {
    if let Err(e) = std::fs::create_dir_all(path) {
        panic!("FATAL: cannot create directory {path:?}: {e}");
    }
    // Do not sync the parent directory - this is the responsibility of the caller.
}

/// Returns the file size for the given path (upstream `MustFileSize`).
pub fn must_file_size(path: impl AsRef<Path>) -> u64 {
    let path = path.as_ref();
    let m = std::fs::metadata(path).unwrap_or_else(|e| panic!("FATAL: cannot stat {path:?}: {e}"));
    if m.is_dir() {
        panic!("FATAL: {path:?} must be a file, not a directory");
    }
    m.len()
}

/// Returns whether the given path exists (upstream `IsPathExist`).
pub fn is_path_exist(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => panic!("FATAL: cannot stat {path:?}: {e}"),
    }
}

/// Reads directory entries at the given dir (upstream `MustReadDir`).
pub fn must_read_dir(dir: impl AsRef<Path>) -> Vec<std::fs::DirEntry> {
    let dir = dir.as_ref();
    let rd = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("FATAL: cannot read directory contents at {dir:?}: {e}"));
    rd.map(|de| de.unwrap_or_else(|e| panic!("FATAL: cannot read directory entry at {dir:?}: {e}")))
        .collect()
}

/// Returns true if the dir entry is a directory or a symlink
/// (upstream `IsDirOrSymlink`).
pub fn is_dir_or_symlink(de: &std::fs::DirEntry) -> bool {
    match de.file_type() {
        Ok(ft) => ft.is_dir() || ft.is_symlink(),
        Err(e) => panic!("FATAL: cannot determine file type for {:?}: {e}", de.path()),
    }
}

/// Creates dst_dir and hard-links all the files from src_dir into it
/// (upstream `MustHardLinkFiles`). Directories and symlinks are skipped.
///
/// The caller is responsible for `must_sync_path` on the parent of dst_dir.
pub fn must_hard_link_files(src_dir: impl AsRef<Path>, dst_dir: impl AsRef<Path>) {
    let (src_dir, dst_dir) = (src_dir.as_ref(), dst_dir.as_ref());
    must_mkdir(dst_dir);

    for de in must_read_dir(src_dir) {
        if is_dir_or_symlink(&de) {
            // Skip directories.
            continue;
        }
        let fn_ = de.file_name();
        let src_path = src_dir.join(&fn_);
        let dst_path = dst_dir.join(&fn_);
        if let Err(e) = std::fs::hard_link(&src_path, &dst_path) {
            panic!("FATAL: cannot link {src_path:?} to {dst_path:?}: {e}");
        }
    }

    must_sync_path(dst_dir);
}

/// Creates a relative symlink for src_path in dst_path
/// (upstream `MustSymlinkRelative`).
///
/// The caller is responsible for `must_sync_path` on the parent of dst_path.
pub fn must_symlink_relative(src_path: impl AsRef<Path>, dst_path: impl AsRef<Path>) {
    let (src_path, dst_path) = (src_path.as_ref(), dst_path.as_ref());
    let base_dir = dst_path.parent().unwrap_or(Path::new("."));
    let src_path_rel = relative_path(base_dir, src_path).unwrap_or_else(|| {
        panic!("FATAL: cannot make relative path for srcPath={src_path:?} against {base_dir:?}")
    });
    if let Err(e) = symlink(&src_path_rel, dst_path, src_path) {
        panic!("FATAL: cannot make a symlink from {dst_path:?} to {src_path_rel:?}: {e}");
    }
}

#[cfg(unix)]
fn symlink(target: &Path, link: &Path, _resolved_target: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn symlink(target: &Path, link: &Path, resolved_target: &Path) -> std::io::Result<()> {
    if resolved_target.is_dir() {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        std::os::windows::fs::symlink_file(target, link)
    }
}

/// Computes a relative path from `base` to `target`
/// (both must be absolute or both relative), like Go's `filepath.Rel`.
fn relative_path(base: &Path, target: &Path) -> Option<PathBuf> {
    if base.is_absolute() != target.is_absolute() {
        return None;
    }
    let base_c: Vec<Component<'_>> = base.components().collect();
    let target_c: Vec<Component<'_>> = target.components().collect();
    let mut i = 0;
    while i < base_c.len() && i < target_c.len() && base_c[i] == target_c[i] {
        i += 1;
    }
    if base_c[i..].contains(&Component::ParentDir) {
        return None;
    }
    let mut out = PathBuf::new();
    for _ in i..base_c.len() {
        out.push("..");
    }
    for c in &target_c[i..] {
        out.push(c.as_os_str());
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    Some(out)
}

/// Creates dst_path and copies all regular files from src_path into it
/// (upstream `MustCopyDirectory`).
///
/// The caller is responsible for `must_sync_path` on the parent of dst_path.
pub fn must_copy_directory(src_path: impl AsRef<Path>, dst_path: impl AsRef<Path>) {
    let (src_path, dst_path) = (src_path.as_ref(), dst_path.as_ref());
    must_mkdir(dst_path);

    for de in must_read_dir(src_path) {
        let is_file = de.file_type().map(|ft| ft.is_file()).unwrap_or(false);
        if !is_file {
            // Skip non-files.
            continue;
        }
        let fn_ = de.file_name();
        must_copy_file(src_path.join(&fn_), dst_path.join(&fn_));
    }

    must_sync_path(dst_path);
}

/// Copies the file from src_path to dst_path (upstream `MustCopyFile`).
pub fn must_copy_file(src_path: impl AsRef<Path>, dst_path: impl AsRef<Path>) {
    let (src_path, dst_path) = (src_path.as_ref(), dst_path.as_ref());
    if let Err(e) = std::fs::copy(src_path, dst_path) {
        panic!("FATAL: cannot copy {src_path:?} to {dst_path:?}: {e}");
    }
    must_sync_path(dst_path);
}

/// Reads `data.len()` bytes from r (upstream `MustReadData`).
///
/// Mirrors Go's behavior: a clean EOF before any byte is read returns
/// silently; a partial read panics.
pub fn must_read_data(r: &mut filestream::Reader, data: &mut [u8]) {
    let mut n = 0;
    while n < data.len() {
        match r.read(&mut data[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!(
                "FATAL: cannot read {} bytes from {:?}; read only {n} bytes; error: {e}",
                data.len(),
                r.path()
            ),
        }
    }
    if n == 0 {
        // Clean EOF: mirror Go's `if err == io.EOF { return }`.
        return;
    }
    if n != data.len() {
        panic!(
            "FATAL: cannot read {} bytes from {:?}; read only {n} bytes",
            data.len(),
            r.path()
        );
    }
}

/// Writes data to w (upstream `MustWriteData`).
pub fn must_write_data(w: &mut filestream::Writer, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    if let Err(e) = w.write_all(data) {
        panic!(
            "FATAL: cannot write {} bytes to {:?}: {e}",
            data.len(),
            w.path()
        );
    }
}

/// Reads the whole file at path (upstream `os.ReadFile` with `Must` semantics).
pub fn read_full_file(path: impl AsRef<Path>) -> Vec<u8> {
    let path = path.as_ref();
    std::fs::read(path).unwrap_or_else(|e| panic!("FATAL: cannot read {path:?}: {e}"))
}

/// The filename of the lock file created by [`must_create_flock_file`].
pub const FLOCK_FILENAME: &str = "flock.lock";

/// Creates [`FLOCK_FILENAME`] in the directory `dir`, takes an exclusive
/// non-blocking lock on it and returns the open file handle
/// (upstream `MustCreateFlockFile`). The lock is held while the file stays open.
pub fn must_create_flock_file(dir: impl AsRef<Path>) -> File {
    let dir = dir.as_ref();
    let flock_filepath = dir.join(FLOCK_FILENAME);
    create_flock_file(&flock_filepath).unwrap_or_else(|e| {
        panic!(
            "FATAL: cannot create lock file: {e}; make sure a single process has \
             exclusive access to {dir:?}"
        )
    })
}

#[cfg(unix)]
fn create_flock_file(flock_file: &Path) -> std::io::Result<File> {
    use std::os::unix::io::AsRawFd;

    let f = File::create(flock_file)?;
    // SAFETY: the fd is a valid open file descriptor owned by `f`.
    if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(f)
}

#[cfg(windows)]
fn create_flock_file(flock_file: &Path) -> std::io::Result<File> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    let f = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(flock_file)?;
    // SAFETY: OVERLAPPED is a plain-old-data struct; a zeroed value (offset 0,
    // no event) is valid for locking a synchronously opened file.
    let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
    // SAFETY: the handle is valid for the lifetime of `f`, and `ov` is a valid
    // OVERLAPPED describing the lock range starting at offset 0.
    let ok = unsafe {
        LockFileEx(
            f.as_raw_handle() as HANDLE,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut ov,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(f)
}

// ---------------------------------------------------------------------------
// Disk space (upstream MustGetFreeSpace / MustGetTotalSpace)
// ---------------------------------------------------------------------------

struct DiskSpaceEntry {
    update_time: u64,
    free: u64,
    total: u64,
}

static DISK_SPACE_MAP: Mutex<Option<HashMap<PathBuf, DiskSpaceEntry>>> = Mutex::new(None);

/// Returns the free space for the given directory path
/// (upstream `MustGetFreeSpace`). Values are cached for up to 2 seconds.
pub fn must_get_free_space(path: impl AsRef<Path>) -> u64 {
    get_disk_space(path.as_ref()).1
}

/// Returns the total disk space for the given directory path
/// (upstream `MustGetTotalSpace`). Values are cached for up to 2 seconds.
pub fn must_get_total_space(path: impl AsRef<Path>) -> u64 {
    get_disk_space(path.as_ref()).0
}

fn get_disk_space(path: &Path) -> (u64, u64) {
    let now = unix_timestamp();
    let mut guard = DISK_SPACE_MAP.lock();
    let m = guard.get_or_insert_with(HashMap::new);
    if let Some(e) = m.get(path) {
        if now - e.update_time < 2 {
            // Fast path - the entry is fresh.
            return (e.total, e.free);
        }
    }
    // Slow path: determine the disk space at path.
    let (total, free) = must_get_disk_space(path);
    m.insert(
        path.to_path_buf(),
        DiskSpaceEntry {
            update_time: now,
            free,
            total,
        },
    );
    (total, free)
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)] // libc field widths differ across platforms
fn must_get_disk_space(path: &Path) -> (u64, u64) {
    use std::os::unix::ffi::OsStrExt;

    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())
        .unwrap_or_else(|e| panic!("FATAL: invalid path {path:?}: {e}"));
    // SAFETY: statvfs() only writes into the zero-initialized struct.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `cpath` is a valid NUL-terminated string and `st` is a valid,
    // writable statvfs struct.
    if unsafe { libc::statvfs(cpath.as_ptr(), &mut st) } != 0 {
        panic!(
            "FATAL: cannot determine free disk space on {path:?}: {}",
            std::io::Error::last_os_error()
        );
    }
    let total = (st.f_blocks as u64).saturating_mul(st.f_frsize as u64);
    let free = (st.f_bavail as u64).saturating_mul(st.f_frsize as u64);
    (total, free)
}

#[cfg(windows)]
fn must_get_disk_space(path: &Path) -> (u64, u64) {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut free: u64 = 0;
    let mut total: u64 = 0;
    // SAFETY: `wide` is a valid NUL-terminated UTF-16 string, and the out
    // pointers reference valid u64 locations for the duration of the call.
    let ok =
        unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), &mut free, &mut total, std::ptr::null_mut()) };
    if ok == 0 {
        panic!(
            "FATAL: cannot get free space for {path:?}: {}",
            std::io::Error::last_os_error()
        );
    }
    (total, free)
}

// ---------------------------------------------------------------------------
// Directory removal (upstream dir_remover.go, simplified)
// ---------------------------------------------------------------------------

/// Directories containing a file with this name are scheduled for removal
/// by [`must_remove_dir`].
pub const DELETE_DIR_FILENAME: &str = ".delete-this-dir";

/// Removes dir_path with all its contents (upstream `MustRemoveDir`).
///
/// The contents may be partially deleted on unclean shutdown during removal.
/// The caller must verify partially removed directories on startup via
/// [`is_partially_removed_dir`] and remove them again if needed.
///
/// Deviation from Go: instead of a background NFS-aware remover goroutine,
/// removal is retried synchronously a few times and then panics.
pub fn must_remove_dir(dir_path: impl AsRef<Path>) {
    let dir_path = dir_path.as_ref();
    if !is_path_exist(dir_path) {
        // Nothing to delete.
        return;
    }

    // Create the marker file indicating that dir_path must be removed,
    // so partially deleted directories can be detected after unclean
    // shutdown via is_partially_removed_dir().
    let delete_file_path = dir_path.join(DELETE_DIR_FILENAME);
    match File::create(&delete_file_path) {
        Ok(_) => {}
        Err(e) => {
            panic!("FATAL: cannot create {delete_file_path:?} while deleting {dir_path:?}: {e}")
        }
    }
    // Make sure the marker file is visible in dir_path.
    must_sync_path(dir_path);

    const MAX_ATTEMPTS: u32 = 5;
    let mut attempt = 0;
    loop {
        match std::fs::remove_dir_all(dir_path) {
            Ok(()) => return,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                attempt += 1;
                if attempt >= MAX_ATTEMPTS {
                    panic!("FATAL: cannot remove {dir_path:?} after {attempt} attempts: {e}");
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Background directory remover (upstream dir_remover.go)
// ---------------------------------------------------------------------------

type RemoveDirTask = (PathBuf, Box<dyn FnOnce() + Send>);

struct RemoverShared {
    /// Number of scheduled-but-not-finished removals.
    pending: Mutex<u64>,
    cv: parking_lot::Condvar,
}

struct DirRemover {
    tx: std::sync::mpsc::Sender<RemoveDirTask>,
    shared: std::sync::Arc<RemoverShared>,
}

/// Decrements the pending-removals counter even if the task panics, so
/// [`remove_dir_async_drain`] cannot hang on a failed removal.
struct PendingGuard(std::sync::Arc<RemoverShared>);

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let mut n = self.0.pending.lock();
        *n -= 1;
        if *n == 0 {
            self.0.cv.notify_all();
        }
    }
}

static DIR_REMOVER: OnceLock<DirRemover> = OnceLock::new();

fn dir_remover() -> &'static DirRemover {
    DIR_REMOVER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<RemoveDirTask>();
        let shared = std::sync::Arc::new(RemoverShared {
            pending: Mutex::new(0),
            cv: parking_lot::Condvar::new(),
        });
        let thread_shared = std::sync::Arc::clone(&shared);
        std::thread::Builder::new()
            .name("dir-remover".to_string())
            .spawn(move || {
                for (dir_path, cleanup) in rx {
                    let _guard = PendingGuard(std::sync::Arc::clone(&thread_shared));
                    cleanup();
                    must_remove_dir(&dir_path);
                }
            })
            .expect("FATAL: cannot spawn the dir-remover thread");
        DirRemover { tx, shared }
    })
}

/// Schedules background removal of `dir_path` on a process-wide remover
/// thread (upstream `dir_remover.go`). `cleanup` runs on the remover thread just
/// before the removal — use it to close file handles that must not stay open
/// while the directory is removed (mandatory on Windows) and to purge caches.
///
/// This keeps slow filesystem work (munmap/CloseHandle, recursive deletes,
/// AV scans) off latency-sensitive threads, e.g. query threads dropping the
/// last reference to a merged-away part.
///
/// Call [`remove_dir_async_drain`] before re-opening a storage directory
/// that may contain scheduled-for-removal subdirectories.
pub fn remove_dir_async(dir_path: PathBuf, cleanup: Box<dyn FnOnce() + Send>) {
    let r = dir_remover();
    *r.shared.pending.lock() += 1;
    if let Err(std::sync::mpsc::SendError((dir_path, cleanup))) = r.tx.send((dir_path, cleanup)) {
        // The remover thread is gone (it panics only on unrecoverable FS
        // errors); fall back to synchronous removal.
        let _guard = PendingGuard(std::sync::Arc::clone(&r.shared));
        cleanup();
        must_remove_dir(&dir_path);
    }
}

/// Blocks until all the removals scheduled via [`remove_dir_async`] finish.
pub fn remove_dir_async_drain() {
    // Nothing was ever scheduled if the remover is uninitialized.
    let Some(r) = DIR_REMOVER.get() else {
        return;
    };
    let mut n = r.shared.pending.lock();
    while *n > 0 {
        r.shared.cv.wait(&mut n);
    }
}

/// Returns true if dir_path is partially removed because of an unclean
/// shutdown during a [`must_remove_dir`] call (upstream `IsPartiallyRemovedDir`).
pub fn is_partially_removed_dir(dir_path: impl AsRef<Path>) -> bool {
    let des = must_read_dir(dir_path.as_ref());
    if des.is_empty() {
        // Delete empty dirs too, since they may appear when the unclean
        // shutdown happens after the marker file is deleted, but before
        // the directory itself is deleted.
        return true;
    }
    for de in &des {
        if de.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        if de.file_name() == DELETE_DIR_FILENAME {
            // The directory contains the marker file: it is partially deleted.
            return true;
        }
    }
    false
}

/// Removes the given path; it must be a file or an empty directory
/// (upstream `MustRemovePath`). Use [`must_remove_dir`] for non-empty directories.
pub fn must_remove_path(path: impl AsRef<Path>) {
    let path = path.as_ref();
    let result = if std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false) {
        std::fs::remove_dir(path)
    } else {
        std::fs::remove_file(path)
    };
    if let Err(e) = result {
        panic!("FATAL: cannot remove {path:?}: {e}");
    }
}

/// Removes all the contents of the given dir if it exists, without removing
/// the dir itself (upstream `MustRemoveDirContents`), so the dir may be mounted
/// to a separate partition.
pub fn must_remove_dir_contents(dir: impl AsRef<Path>) {
    let dir = dir.as_ref();
    if !is_path_exist(dir) {
        // The path doesn't exist, so nothing to remove.
        return;
    }
    for de in must_read_dir(dir) {
        let full_path = de.path();
        let is_dir = de.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
        let result = if is_dir {
            std::fs::remove_dir_all(&full_path)
        } else {
            std::fs::remove_file(&full_path)
        };
        if let Err(e) = result {
            panic!("FATAL: cannot remove {full_path:?}: {e}");
        }
    }
    must_sync_path(dir);
}

// ---------------------------------------------------------------------------
// ReaderAt (upstream reader_at.go)
// ---------------------------------------------------------------------------

/// Random-access reader over an mmapped file (upstream `fs.ReaderAt`).
///
/// The file is mmapped read-only when it is non-empty; reads fall back to
/// pread when the mmap is unavailable.
pub struct ReaderAt {
    path: PathBuf,
    f: File,
    mmap: Option<memmap2::Mmap>,
}

impl ReaderAt {
    /// Returns the path to the file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads `p.len()` bytes at offset `off` (upstream `MustReadAt`);
    /// panics on out-of-range reads or IO errors.
    pub fn must_read_at(&self, p: &mut [u8], off: u64) {
        if p.is_empty() {
            return;
        }
        match &self.mmap {
            Some(m) => {
                let off = usize::try_from(off)
                    .unwrap_or_else(|_| panic!("BUG: off={off} overflows usize"));
                if off + p.len() > m.len() {
                    panic!(
                        "BUG: off={off} is out of allowed range [0...{}] for len(p)={} in file {:?}",
                        m.len().saturating_sub(p.len()),
                        p.len(),
                        self.path
                    );
                }
                p.copy_from_slice(&m[off..off + p.len()]);
            }
            None => self.must_pread(p, off),
        }
    }

    #[cfg(unix)]
    fn must_pread(&self, p: &mut [u8], off: u64) {
        use std::os::unix::fs::FileExt;
        if let Err(e) = self.f.read_exact_at(p, off) {
            panic!(
                "FATAL: cannot read {} bytes at offset {off} of file {:?}: {e}",
                p.len(),
                self.path
            );
        }
    }

    #[cfg(windows)]
    fn must_pread(&self, p: &mut [u8], off: u64) {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < p.len() {
            match self.f.seek_read(&mut p[done..], off + done as u64) {
                Ok(0) => panic!(
                    "FATAL: unexpected EOF at offset {} of file {:?}",
                    off + done as u64,
                    self.path
                ),
                Ok(n) => done += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => panic!(
                    "FATAL: cannot read {} bytes at offset {off} of file {:?}: {e}",
                    p.len(),
                    self.path
                ),
            }
        }
    }

    /// Hints the OS that the file is going to be read mostly sequentially
    /// (upstream `MustFadviseSequentialRead`). If `prefetch` is set, the OS is
    /// hinted to prefetch the data. No-op outside Linux.
    pub fn must_fadvise_sequential_read(&self, prefetch: bool) {
        if let Err(e) = fadvise_sequential_read(&self.f, prefetch) {
            panic!(
                "FATAL: error in fadvise_sequential_read({:?}, {prefetch}): {e}",
                self.path
            );
        }
    }

    /// Closes the reader (upstream `MustClose`); the mmap and file are released.
    pub fn must_close(self) {
        // The mmap is unmapped and the file closed on drop.
    }
}

/// Opens a [`ReaderAt`] for reading from the file at path
/// (upstream `MustOpenReaderAt`).
///
/// Deviation from Go: the file is opened and mmapped eagerly instead of
/// lazily on the first read.
pub fn must_open_reader_at(path: impl AsRef<Path>) -> ReaderAt {
    let path = path.as_ref();
    let f = File::open(path).unwrap_or_else(|e| {
        panic!(
            "FATAL: cannot open file {path:?} for reading: {e}; \
             try increasing the limit on the number of open files via 'ulimit -n'"
        )
    });
    new_reader_at(f, path)
}

/// Returns a [`ReaderAt`] for reading from `f` (upstream `NewReaderAt`).
///
/// Takes ownership of `f`. `path` is used for error messages only.
pub fn new_reader_at(f: File, path: impl AsRef<Path>) -> ReaderAt {
    let path = path.as_ref();
    let size = f
        .metadata()
        .unwrap_or_else(|e| panic!("FATAL: error in fstat({path:?}): {e}"))
        .len();
    let mmap = if size == 0 {
        None
    } else {
        // SAFETY: the mapping is read-only (PROT_READ). The underlying part
        // files are immutable while opened by the storage engine, so the
        // mapped memory is never modified concurrently.
        let m = unsafe { memmap2::Mmap::map(&f) }
            .unwrap_or_else(|e| panic!("FATAL: cannot mmap {path:?}: {e}"));
        Some(m)
    };
    ReaderAt {
        path: path.to_path_buf(),
        f,
        mmap,
    }
}

#[cfg(target_os = "linux")]
fn fadvise_sequential_read(f: &File, prefetch: bool) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let fd = f.as_raw_fd();
    // SAFETY: fd is a valid open file descriptor owned by `f`.
    let ret = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_SEQUENTIAL) };
    if ret != 0 {
        return Err(std::io::Error::from_raw_os_error(ret));
    }
    if prefetch {
        // SAFETY: same as above.
        let ret = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_WILLNEED) };
        if ret != 0 {
            return Err(std::io::Error::from_raw_os_error(ret));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn fadvise_sequential_read(_f: &File, _prefetch: bool) -> std::io::Result<()> {
    // No-op outside Linux, like Go's stubs (incl. fs_windows.go).
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "esm-fs-test-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn temporary_file_names_are_detected() {
        // Port of Go TestIsTemporaryFileName.
        let f = |s: &str, expected: bool| {
            assert_eq!(
                is_temporary_file_name(s),
                expected,
                "unexpected is_temporary_file_name({s:?})"
            );
        };
        f("", false);
        f(".", false);
        f(".tmp", false);
        f("tmp.123", false);
        f(".tmp.123.xx", false);
        f(".tmp.1", true);
        f("asdf.dff.tmp.123", true);
        f("asdf.sdfds.tmp.dfd", false);
        f("dfd.sdfds.dfds.1232", false);
    }

    #[test]
    fn write_atomic_writes_and_overwrites() {
        let dir = temp_dir("atomic");
        let path = dir.join("data.bin");
        must_write_atomic(&path, b"first", false);
        assert_eq!(read_full_file(&path), b"first");
        must_write_atomic(&path, b"second", true);
        assert_eq!(read_full_file(&path), b"second");
        // No temporary files must remain.
        for de in must_read_dir(&dir) {
            let name = de.file_name().into_string().unwrap();
            assert!(!is_temporary_file_name(&name), "stale tmp file: {name}");
        }
        must_remove_dir(&dir);
        assert!(!is_path_exist(&dir));
    }

    #[test]
    #[should_panic(expected = "already exists")]
    fn write_atomic_refuses_overwrite_when_disallowed() {
        let dir = temp_dir("atomic-noover");
        let path = dir.join("data.bin");
        must_write_atomic(&path, b"first", false);
        must_write_atomic(&path, b"second", false);
    }

    #[test]
    fn write_sync_and_file_size() {
        let dir = temp_dir("write-sync");
        let path = dir.join("f.bin");
        must_write_sync(&path, b"hello world");
        assert_eq!(must_file_size(&path), 11);
        assert_eq!(read_full_file(&path), b"hello world");
        must_sync_path_and_parent_dir(&path);
        must_remove_dir(&dir);
    }

    #[test]
    fn mkdir_if_not_exist_is_idempotent() {
        let dir = temp_dir("mkdir");
        let sub = dir.join("a/b");
        must_mkdir_if_not_exist(&sub);
        must_mkdir_if_not_exist(&sub);
        assert!(is_path_exist(&sub));
        must_remove_dir(&dir);
    }

    #[test]
    #[should_panic(expected = "already exists")]
    fn mkdir_fail_if_exist_panics_on_existing() {
        let dir = temp_dir("mkdir-fail");
        let sub = dir.join("x");
        must_mkdir_fail_if_exist(&sub);
        must_mkdir_fail_if_exist(&sub);
    }

    #[test]
    fn hard_link_files_links_regular_files_only() {
        let dir = temp_dir("hardlink");
        let src = dir.join("src");
        must_mkdir_if_not_exist(&src);
        must_write_sync(src.join("a.bin"), b"aaa");
        must_write_sync(src.join("b.bin"), b"bbbb");
        must_mkdir_if_not_exist(src.join("subdir"));

        let dst = dir.join("dst");
        must_hard_link_files(&src, &dst);
        assert_eq!(read_full_file(dst.join("a.bin")), b"aaa");
        assert_eq!(read_full_file(dst.join("b.bin")), b"bbbb");
        assert!(!is_path_exist(dst.join("subdir")));
        must_remove_dir(&dir);
    }

    #[test]
    fn copy_directory_copies_regular_files() {
        let dir = temp_dir("copydir");
        let src = dir.join("src");
        must_mkdir_if_not_exist(&src);
        must_write_sync(src.join("a.bin"), b"data-a");
        must_mkdir_if_not_exist(src.join("nested"));
        let dst = dir.join("dst");
        must_copy_directory(&src, &dst);
        assert_eq!(read_full_file(dst.join("a.bin")), b"data-a");
        assert!(!is_path_exist(dst.join("nested")));
        must_remove_dir(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_relative_points_to_source() {
        let dir = temp_dir("symlink");
        let src = dir.join("target-dir");
        must_mkdir_if_not_exist(&src);
        must_write_sync(src.join("f.bin"), b"via-symlink");
        let dst = dir.join("link");
        must_symlink_relative(&src, &dst);
        assert_eq!(read_full_file(dst.join("f.bin")), b"via-symlink");
        let link_target = std::fs::read_link(&dst).unwrap();
        assert!(link_target.is_relative(), "symlink target must be relative");
        must_remove_dir(&dir);
    }

    #[test]
    fn relative_path_is_computed() {
        let f = |base: &str, target: &str, expected: &str| {
            let result = relative_path(Path::new(base), Path::new(target)).unwrap();
            assert_eq!(result, Path::new(expected), "rel({base}, {target})");
        };
        f("/a/b", "/a/b/c", "c");
        f("/a/b", "/a/x/y", "../x/y");
        f("/a/b", "/a/b", ".");
        f("a", "b", "../b");
        // Mixing an absolute base with a relative target must fail; what
        // counts as "absolute" is platform-specific ("/a" has no drive
        // prefix on Windows).
        #[cfg(unix)]
        assert_eq!(relative_path(Path::new("/a"), Path::new("b")), None);
        #[cfg(windows)]
        assert_eq!(relative_path(Path::new("C:\\a"), Path::new("b")), None);
    }

    #[test]
    fn read_write_data_roundtrip() {
        let dir = temp_dir("rwdata");
        let path = dir.join("stream.bin");
        let mut w = filestream::Writer::must_create(&path, false);
        must_write_data(&mut w, b"0123456789");
        must_write_data(&mut w, b""); // empty write is a no-op
        w.must_close();

        let mut r = filestream::Reader::must_open(&path, false);
        let mut buf = [0u8; 10];
        must_read_data(&mut r, &mut buf);
        assert_eq!(&buf, b"0123456789");
        // Clean EOF returns silently, leaving the buffer untouched.
        let mut buf2 = [0u8; 4];
        must_read_data(&mut r, &mut buf2);
        assert_eq!(&buf2, &[0u8; 4]);
        r.must_close();
        must_remove_dir(&dir);
    }

    #[test]
    fn reader_at_reads_at_offsets() {
        let dir = temp_dir("reader-at");
        let path = dir.join("part.bin");
        let data: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 256) as u8).collect();
        must_write_sync(&path, &data);

        let r = must_open_reader_at(&path);
        assert_eq!(r.path(), path.as_path());
        r.must_fadvise_sequential_read(true);
        let mut buf = vec![0u8; 1000];
        for &off in &[0usize, 1, 4095, 4096, 60_000] {
            r.must_read_at(&mut buf, off as u64);
            assert_eq!(buf, data[off..off + 1000], "mismatch at offset {off}");
        }
        // Empty read is a no-op even at the end of the file.
        r.must_read_at(&mut [], data.len() as u64);
        r.must_close();
        must_remove_dir(&dir);
    }

    #[test]
    fn reader_at_handles_empty_files() {
        let dir = temp_dir("reader-at-empty");
        let path = dir.join("empty.bin");
        must_write_sync(&path, b"");
        let r = must_open_reader_at(&path);
        r.must_read_at(&mut [], 0);
        r.must_close();
        must_remove_dir(&dir);
    }

    #[test]
    #[should_panic(expected = "out of allowed range")]
    fn reader_at_panics_on_out_of_range_read() {
        let dir = temp_dir("reader-at-oob");
        let path = dir.join("small.bin");
        must_write_sync(&path, b"abc");
        let r = must_open_reader_at(&path);
        let mut buf = [0u8; 4];
        r.must_read_at(&mut buf, 0);
    }

    #[test]
    fn free_and_total_space_are_sane() {
        let dir = temp_dir("space");
        let free = must_get_free_space(&dir);
        let total = must_get_total_space(&dir);
        assert!(total > 0, "total space must be positive");
        assert!(
            free <= total,
            "free ({free}) must not exceed total ({total})"
        );
        // Second call hits the cache.
        assert_eq!(must_get_free_space(&dir), free);
        must_remove_dir(&dir);
    }

    #[test]
    fn remove_dir_and_partial_detection() {
        let dir = temp_dir("remove");
        let victim = dir.join("victim");
        must_mkdir_if_not_exist(victim.join("nested"));
        must_write_sync(victim.join("f.bin"), b"x");
        must_remove_dir(&victim);
        assert!(!is_path_exist(&victim));
        // Removing a missing dir is a no-op.
        must_remove_dir(&victim);

        // A dir containing the delete marker is partially removed.
        let partial = dir.join("partial");
        must_mkdir_if_not_exist(&partial);
        must_write_sync(partial.join(DELETE_DIR_FILENAME), b"");
        must_write_sync(partial.join("leftover.bin"), b"y");
        assert!(is_partially_removed_dir(&partial));

        // An empty dir is partially removed too.
        let empty = dir.join("empty");
        must_mkdir_if_not_exist(&empty);
        assert!(is_partially_removed_dir(&empty));

        // A dir with regular contents and no marker is not.
        let healthy = dir.join("healthy");
        must_mkdir_if_not_exist(&healthy);
        must_write_sync(healthy.join("data.bin"), b"z");
        assert!(!is_partially_removed_dir(&healthy));

        must_remove_dir(&dir);
    }

    #[test]
    fn remove_dir_contents_keeps_the_dir() {
        let dir = temp_dir("remove-contents");
        must_write_sync(dir.join("a.bin"), b"a");
        must_mkdir_if_not_exist(dir.join("sub/deep"));
        must_remove_dir_contents(&dir);
        assert!(is_path_exist(&dir));
        assert!(must_read_dir(&dir).is_empty());
        // Missing dir is a no-op.
        must_remove_dir_contents(dir.join("missing"));
        must_remove_dir(&dir);
    }

    #[test]
    fn remove_path_removes_files_and_empty_dirs() {
        let dir = temp_dir("remove-path");
        must_write_sync(dir.join("f.bin"), b"f");
        must_remove_path(dir.join("f.bin"));
        assert!(!is_path_exist(dir.join("f.bin")));
        must_mkdir_if_not_exist(dir.join("empty"));
        must_remove_path(dir.join("empty"));
        assert!(!is_path_exist(dir.join("empty")));
        must_remove_dir(&dir);
    }

    #[test]
    fn flock_file_is_exclusive() {
        let dir = temp_dir("flock");
        let _lock = must_create_flock_file(&dir);
        // A second lock attempt on the same file must fail while the first
        // handle is alive.
        let err = create_flock_file(&dir.join(FLOCK_FILENAME));
        assert!(err.is_err(), "second flock must fail");
        drop(_lock);
        must_remove_dir(&dir);
    }
}
