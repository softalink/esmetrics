//! Process-level OS knobs.

use std::io;

/// Best-effort attempt to raise the process's open-file-descriptor soft limit
/// to at least `min`.
///
/// Returns the actual soft limit in effect after the call (which may be lower
/// than `min` if the hard limit is more restrictive).
///
/// **Unix**: calls `setrlimit(RLIMIT_NOFILE, ...)`, clamped to the current hard
/// limit.
///
/// **Windows**: no-op; modern Windows does not expose a tunable per-process
/// handle limit. Returns `u64::MAX` to indicate "effectively unlimited".
#[allow(unused_variables)]
pub fn set_open_file_limit(min: u64) -> io::Result<u64> {
    #[cfg(unix)]
    {
        use nix::sys::resource::{Resource, getrlimit, setrlimit};
        let (current_soft, hard) = getrlimit(Resource::RLIMIT_NOFILE).map_err(io::Error::from)?;
        let target = min.min(hard).max(current_soft);
        if target > current_soft {
            setrlimit(Resource::RLIMIT_NOFILE, target, hard).map_err(io::Error::from)?;
        }
        Ok(target)
    }
    #[cfg(windows)]
    {
        Ok(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_limit_returns_actual_value() {
        // Asking for 1 should be a no-op (current limit is far higher); we
        // expect the returned value to be at least 1 and not error.
        let actual = set_open_file_limit(1).unwrap();
        assert!(actual >= 1);
    }
}
