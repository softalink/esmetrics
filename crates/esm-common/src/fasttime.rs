//! Port of the upstream VictoriaMetrics `lib/fasttime` (v1.146.0).
//!
//! Provides a coarse-grained unix timestamp updated by a background thread
//! once per second, which is faster than querying the system clock on every
//! call (mirrors the Go implementation in `fasttime_normal.go`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SECONDS_PER_HOUR: u64 = 3600;
const SECONDS_PER_DAY: u64 = 24 * 3600;

fn system_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before the unix epoch")
        .as_secs()
}

/// Returns the shared timestamp cell, spawning the background updater thread
/// on first use (the Go version does this in `init()`; Rust has no `init`,
/// so it is done lazily here).
fn current_timestamp() -> &'static AtomicU64 {
    static CURRENT_TIMESTAMP: OnceLock<&'static AtomicU64> = OnceLock::new();
    CURRENT_TIMESTAMP.get_or_init(|| {
        let ts: &'static AtomicU64 = Box::leak(Box::new(AtomicU64::new(system_unix_timestamp())));
        std::thread::Builder::new()
            .name("fasttime-updater".to_string())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_secs(1));
                ts.store(system_unix_timestamp(), Ordering::Relaxed);
            })
            .expect("failed to spawn fasttime updater thread");
        ts
    })
}

/// Port of Go `fasttime.UnixTimestamp`.
///
/// Returns the current unix timestamp in seconds.
/// It is faster than querying the system clock directly.
pub fn unix_timestamp() -> u64 {
    current_timestamp().load(Ordering::Relaxed)
}

/// Port of Go `fasttime.UnixDate`.
///
/// Returns date from the current unix timestamp.
/// The date is calculated by dividing unix timestamp by (24*3600).
pub fn unix_date() -> u64 {
    unix_timestamp() / SECONDS_PER_DAY
}

/// Port of Go `fasttime.UnixHour`.
///
/// Returns hour from the current unix timestamp.
/// The hour is calculated by dividing unix timestamp by 3600.
pub fn unix_hour() -> u64 {
    unix_timestamp() / SECONDS_PER_HOUR
}

#[cfg(test)]
mod tests {
    use super::*;

    // The cached value legitimately LAGS the fresh clock (it is refreshed
    // once per second by a background thread), so the comparison must be
    // two-sided: with the upstream-style `cached.wrapping_sub(fresh) <= 1`
    // check, any lag across a second boundary underflows to ~u64::MAX and
    // fails. That only stayed green while this test happened to be the
    // first fasttime caller in the process; other esm-common tests also
    // touch fasttime, so on loaded CI runners the cell is initialized
    // earlier and the flake fires. Tolerance is 2 (not 1) to absorb
    // updater-thread scheduling delay on starved runners.
    const CLOCK_TOLERANCE: u64 = 2;

    #[test]
    fn test_unix_timestamp() {
        let ts_expected = system_unix_timestamp();
        let ts = unix_timestamp();
        assert!(
            ts.abs_diff(ts_expected) <= CLOCK_TOLERANCE,
            "unexpected unix_timestamp; got {ts}; want {ts_expected}"
        );
    }

    #[test]
    fn test_unix_date() {
        let date_expected = system_unix_timestamp() / SECONDS_PER_DAY;
        let date = unix_date();
        assert!(
            date.abs_diff(date_expected) <= 1,
            "unexpected unix_date; got {date}; want {date_expected}"
        );
    }

    #[test]
    fn test_unix_hour() {
        let hour_expected = system_unix_timestamp() / SECONDS_PER_HOUR;
        let hour = unix_hour();
        assert!(
            hour.abs_diff(hour_expected) <= 1,
            "unexpected unix_hour; got {hour}; want {hour_expected}"
        );
    }
}
