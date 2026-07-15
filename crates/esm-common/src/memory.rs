// Ported from the upstream VictoriaMetrics lib/memory + lib/cgroup memory-limit detection (v1.146.0).
//! System memory accounting.
//!
//! [`allowed`] returns the amount of system memory the app caches may occupy
//! (60% of the detected system memory by default, mirroring
//! `-memory.allowedPercent=60`). [`remaining`] returns what is left to the OS.
//!
//! [`init`] mirrors the `-memory.allowedPercent` / `-memory.allowedBytes`
//! flags and must be called before the first `allowed()`/`remaining()` call
//! to take effect.
//!
//! On Linux the system memory limit is capped by the cgroup v1/v2 memory
//! limits (ported from `lib/cgroup/mem.go`); on Windows it is detected via
//! `GlobalMemoryStatusEx`; on other unixes via `sysconf`.

use std::sync::OnceLock;

use parking_lot::Mutex;

struct MemInfo {
    allowed: usize,
    remaining: usize,
}

/// (allowed_percent, allowed_bytes) — mirrors the upstream flag values.
static CONFIG: Mutex<(f64, i64)> = Mutex::new((60.0, 0));
static MEM: OnceLock<MemInfo> = OnceLock::new();

/// Sets the memory limits configuration, mirroring the
/// `-memory.allowedPercent` and `-memory.allowedBytes` flags.
///
/// If `allowed_bytes > 0`, it overrides `allowed_percent`.
///
/// Must be called before the first [`allowed`]/[`remaining`] call;
/// panics otherwise (mirrors Go's "must be called after flag.Parse" BUG check).
pub fn init(allowed_percent: f64, allowed_bytes: i64) {
    *CONFIG.lock() = (allowed_percent, allowed_bytes);
    assert!(
        MEM.get().is_none(),
        "BUG: memory::init must be called before the first memory::allowed()/remaining() call"
    );
}

/// Returns the amount of system memory allowed to use by the app.
pub fn allowed() -> usize {
    get().allowed
}

/// Returns the amount of memory remaining to the OS.
pub fn remaining() -> usize {
    get().remaining
}

fn get() -> &'static MemInfo {
    MEM.get_or_init(init_once)
}

fn init_once() -> MemInfo {
    let (allowed_percent, allowed_bytes) = *CONFIG.lock();
    let memory_limit = sys_total_memory();
    if allowed_bytes <= 0 {
        assert!(
            (1.0..=100.0).contains(&allowed_percent),
            "FATAL: memory.allowedPercent must be in the range [1...100]; got {allowed_percent}"
        );
        let allowed = (memory_limit as f64 * allowed_percent / 100.0) as usize;
        let remaining = memory_limit.saturating_sub(allowed);
        assert!(
            remaining > 0,
            "BUG: remaining memory cannot be <= 0; system memory limit: {memory_limit} bytes, \
             memory.allowedPercent={allowed_percent}"
        );
        log::info!(
            "limiting caches to {allowed} bytes, leaving {remaining} bytes to the OS according to \
             memory.allowedPercent={allowed_percent}, system memory limit {memory_limit} bytes"
        );
        MemInfo { allowed, remaining }
    } else {
        let allowed = allowed_bytes as usize;
        if allowed < 1024 * 1024 {
            log::warn!(
                "allowed memory {allowed} bytes set by memory.allowedBytes is low. \
                 The process may behave unexpectedly."
            );
        }
        let remaining = memory_limit.saturating_sub(allowed);
        assert!(
            remaining > 0,
            "FATAL: remaining memory cannot be <= 0; system memory limit: {memory_limit} bytes, \
             memory.allowedBytes={allowed_bytes}"
        );
        log::info!(
            "limiting caches to {allowed} bytes, leaving {remaining} bytes to the OS according to \
             memory.allowedBytes={allowed_bytes}, system memory limit {memory_limit} bytes"
        );
        MemInfo { allowed, remaining }
    }
}

/// Returns the total system memory in bytes, capped by cgroup limits on Linux.
#[cfg(target_os = "linux")]
#[allow(clippy::unnecessary_cast)] // libc field widths differ across platforms
fn sys_total_memory() -> usize {
    // SAFETY: sysinfo() only writes into the zero-initialized struct pointed
    // to by the valid pointer passed to it.
    let mut si: libc::sysinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `si` is a valid, writable sysinfo struct.
    if unsafe { libc::sysinfo(&mut si) } != 0 {
        panic!(
            "FATAL: error in sysinfo(): {}",
            std::io::Error::last_os_error()
        );
    }
    let total_mem = (si.totalram as u64)
        .saturating_mul(si.mem_unit as u64)
        .min(usize::MAX as u64) as usize;

    let mem = cgroup::get_memory_limit();
    if mem <= 0 || mem as u64 > total_mem as u64 {
        // Try reading the hierarchical memory limit.
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/699
        let mem = cgroup::get_hierarchical_memory_limit();
        if mem <= 0 || mem as u64 > total_mem as u64 {
            return total_mem;
        }
        return mem as usize;
    }
    mem as usize
}

#[cfg(all(unix, not(target_os = "linux")))]
fn sys_total_memory() -> usize {
    // SAFETY: sysconf with valid constants has no memory-safety requirements.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    // SAFETY: same as above.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
    if pages <= 0 || page_size <= 0 {
        panic!("FATAL: cannot determine system memory via sysconf()");
    }
    (pages as u64)
        .saturating_mul(page_size as u64)
        .min(usize::MAX as u64) as usize
}

#[cfg(windows)]
fn sys_total_memory() -> usize {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    // SAFETY: MEMORYSTATUSEX is a plain-old-data struct; zero-init is valid.
    let mut msx: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
    msx.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
    // SAFETY: `msx` is a valid MEMORYSTATUSEX with dwLength set as required
    // by the GlobalMemoryStatusEx API contract.
    if unsafe { GlobalMemoryStatusEx(&mut msx) } == 0 {
        panic!(
            "FATAL: error in GlobalMemoryStatusEx: {}",
            std::io::Error::last_os_error()
        );
    }
    usize::try_from(msx.ullTotalPhys)
        .unwrap_or_else(|_| panic!("FATAL: int overflow for ullTotalPhys={}", msx.ullTotalPhys))
}

/// Ported from upstream `lib/cgroup` (mem.go + util.go), memory parts only.
///
/// The parsing helpers are platform-independent (plain file reads) so they can
/// be unit-tested everywhere; only Linux actually consults them.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod cgroup {
    use std::path::{Path, PathBuf};

    /// Returns the cgroup memory limit, or 0 if it cannot be determined.
    pub fn get_memory_limit() -> i64 {
        // Try determining the amount of memory inside docker/lxc containers.
        if let Some(n) = get_stat_generic(
            "memory.limit_in_bytes",
            "/sys/fs/cgroup/memory",
            "/proc/self/cgroup",
            "memory",
        ) {
            return n;
        }
        match get_mem_limit_v2("/sys/fs/cgroup", "/proc/self/cgroup", "memory.max") {
            Some(n) if n > 0 => n,
            _ => 0,
        }
    }

    /// Returns the cgroup v1 hierarchical memory limit, or 0 if unavailable.
    ///
    /// See https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
    pub fn get_hierarchical_memory_limit() -> i64 {
        get_hierarchical_memory_limit_at("/sys/fs/cgroup/memory", "/proc/self/cgroup").unwrap_or(0)
    }

    pub(super) fn get_hierarchical_memory_limit_at(
        sysfs_prefix: &str,
        cgroup_path: &str,
    ) -> Option<i64> {
        let data = get_file_contents("memory.stat", sysfs_prefix, cgroup_path, "memory")?;
        let mem_stat = grep_first_match(&data, "hierarchical_memory_limit", 1, " ")?;
        mem_stat.parse::<i64>().ok()
    }

    pub(super) fn get_stat_generic(
        stat_name: &str,
        sysfs_prefix: &str,
        cgroup_path: &str,
        cgroup_grep_line: &str,
    ) -> Option<i64> {
        let data = get_file_contents(stat_name, sysfs_prefix, cgroup_path, cgroup_grep_line)?;
        data.trim().parse::<i64>().ok()
    }

    /// Walks the cgroup v2 hierarchy and returns the minimal limit found,
    /// or None on parse failure / -1 when no limit is set.
    pub(super) fn get_mem_limit_v2(
        sysfs_prefix: &str,
        cgroup_path: &str,
        stat_name: &str,
    ) -> Option<i64> {
        let mut sub_path = read_cgroup_v2_sub_path(cgroup_path).unwrap_or_else(|| "/".to_string());
        let mut min_limit: i64 = -1;
        loop {
            // Traverse the sub-path hierarchy and use the minimal value for the stat.
            if let Ok(data) =
                std::fs::read_to_string(join_path(sysfs_prefix, &sub_path).join(stat_name))
            {
                let s = data.trim();
                if s != "max" {
                    let n = s.parse::<i64>().ok()?;
                    if n > 0 && (min_limit < 0 || n < min_limit) {
                        min_limit = n;
                    }
                }
            }
            if sub_path == "/" || sub_path == "." {
                break;
            }
            sub_path = path_dir(&sub_path);
        }
        Some(min_limit)
    }

    fn get_file_contents(
        stat_name: &str,
        sysfs_prefix: &str,
        cgroup_path: &str,
        cgroup_grep_line: &str,
    ) -> Option<String> {
        if let Ok(data) = std::fs::read_to_string(Path::new(sysfs_prefix).join(stat_name)) {
            return Some(data);
        }
        let cgroup_data = std::fs::read_to_string(cgroup_path).ok()?;
        let sub_path = grep_first_match(&cgroup_data, cgroup_grep_line, 2, ":")?;
        std::fs::read_to_string(join_path(sysfs_prefix, &sub_path).join(stat_name)).ok()
    }

    /// Reads the cgroup v2 sub-path, e.g. from a line like
    /// `0::/user.slice/user-1000.slice/session-5.scope`.
    fn read_cgroup_v2_sub_path(cgroup_path: &str) -> Option<String> {
        let data = std::fs::read_to_string(cgroup_path).ok()?;
        grep_first_match(&data, "", 2, ":")
    }

    /// Searches for the first line containing `pattern` in `data` and returns
    /// the `index`-th item after splitting the line by `delimiter`.
    pub(super) fn grep_first_match(
        data: &str,
        pattern: &str,
        index: usize,
        delimiter: &str,
    ) -> Option<String> {
        for line in data.split('\n') {
            if !line.contains(pattern) {
                continue;
            }
            let parts: Vec<&str> = line.split(delimiter).collect();
            if index < parts.len() {
                return Some(parts[index].trim().to_string());
            }
        }
        None
    }

    /// Joins `prefix` and a possibly absolute `sub` path (Go `path.Join`
    /// semantics: absolute sub-paths do not replace the prefix).
    fn join_path(prefix: &str, sub: &str) -> PathBuf {
        Path::new(prefix).join(sub.trim_start_matches('/'))
    }

    /// Go `path.Dir` for slash-separated paths.
    fn path_dir(p: &str) -> String {
        match p.rfind('/') {
            None => ".".to_string(),
            Some(0) => "/".to_string(),
            Some(n) => p[..n].to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::cgroup;
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir(name: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "esm-memory-test-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn allowed_and_remaining_are_sane() {
        let allowed = allowed();
        let remaining = remaining();
        assert!(allowed > 0, "allowed() must be positive; got {allowed}");
        assert!(
            remaining > 0,
            "remaining() must be positive; got {remaining}"
        );
        // Default is 60% of the memory limit, so allowed must exceed remaining.
        assert!(
            allowed > remaining,
            "with the 60% default allowed ({allowed}) must exceed remaining ({remaining})"
        );
    }

    #[test]
    fn grep_first_match_extracts_fields() {
        let data = "12:memory:/user.slice\n11:cpu:/foo\n";
        assert_eq!(
            cgroup::grep_first_match(data, "memory", 2, ":").as_deref(),
            Some("/user.slice")
        );
        assert_eq!(
            cgroup::grep_first_match(data, "cpu", 2, ":").as_deref(),
            Some("/foo")
        );
        assert_eq!(cgroup::grep_first_match(data, "blkio", 2, ":"), None);
        assert_eq!(
            cgroup::grep_first_match(
                "hierarchical_memory_limit 456\n",
                "hierarchical_memory_limit",
                1,
                " "
            )
            .as_deref(),
            Some("456")
        );
    }

    #[test]
    fn cgroup_v1_stat_is_read_from_prefix() {
        let dir = temp_dir("cgv1");
        std::fs::write(dir.join("memory.limit_in_bytes"), "123456789\n").unwrap();
        let n = cgroup::get_stat_generic(
            "memory.limit_in_bytes",
            dir.to_str().unwrap(),
            "/nonexistent/cgroup",
            "memory",
        );
        assert_eq!(n, Some(123_456_789));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn cgroup_v2_limit_uses_minimum_in_hierarchy() {
        let dir = temp_dir("cgv2");
        // Fake /proc/self/cgroup contents pointing at /a/b.
        let cgroup_file = dir.join("proc-self-cgroup");
        std::fs::write(&cgroup_file, "0::/a/b\n").unwrap();
        let sysfs = dir.join("sysfs");
        std::fs::create_dir_all(sysfs.join("a/b")).unwrap();
        std::fs::write(sysfs.join("a/b/memory.max"), "1048576\n").unwrap();
        std::fs::write(sysfs.join("a/memory.max"), "524288\n").unwrap();
        std::fs::write(sysfs.join("memory.max"), "max\n").unwrap();

        let n = cgroup::get_mem_limit_v2(
            sysfs.to_str().unwrap(),
            cgroup_file.to_str().unwrap(),
            "memory.max",
        );
        assert_eq!(n, Some(524_288));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn cgroup_v2_limit_is_negative_when_unset() {
        let dir = temp_dir("cgv2-unset");
        let cgroup_file = dir.join("proc-self-cgroup");
        std::fs::write(&cgroup_file, "0::/\n").unwrap();
        let sysfs = dir.join("sysfs");
        std::fs::create_dir_all(&sysfs).unwrap();
        std::fs::write(sysfs.join("memory.max"), "max\n").unwrap();

        let n = cgroup::get_mem_limit_v2(
            sysfs.to_str().unwrap(),
            cgroup_file.to_str().unwrap(),
            "memory.max",
        );
        assert_eq!(n, Some(-1));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn hierarchical_memory_limit_is_parsed_from_memory_stat() {
        let dir = temp_dir("cgv1-hier");
        std::fs::write(
            dir.join("memory.stat"),
            "cache 42\nhierarchical_memory_limit 9663676416\nrss 7\n",
        )
        .unwrap();
        let n =
            cgroup::get_hierarchical_memory_limit_at(dir.to_str().unwrap(), "/nonexistent/cgroup");
        assert_eq!(n, Some(9_663_676_416));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
