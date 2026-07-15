// Ported from the upstream VictoriaMetrics lib/logger (v1.146.0).
//! Thin leveled logger matching the upstream's default log format:
//!
//! `2006-01-02T15:04:05.000Z\tlevel\tlocation\tmsg`
//!
//! written to stderr. Timestamps are always UTC (the upstream's `-loggerTimezone`
//! defaults to UTC; other timezones are not ported).
//!
//! [`init`] installs a [`log::Log`] implementation, so the standard
//! `log::info!`-style macros work. The crate also exports the upstream-flavored
//! `infof!`/`warnf!`/`errorf!`/`panicf!` macros.
//!
//! Deviations from Go:
//! - JSON format, log rate limiting, per-level counters and the
//!   `-loggerOutput`/`-loggerTimezone` options are not ported (not needed
//!   by the storage engine port).
//! - `panicf!` works without `init()` being called: it writes the line
//!   directly to stderr and then panics.

use std::io::Write as _;
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{Level, LevelFilter, Log, Metadata, Record};

/// Logs an info message via the `log` crate (upstream `logger.Infof`).
#[macro_export]
macro_rules! infof {
    ($($arg:tt)*) => { ::log::info!($($arg)*) };
}

/// Logs a warn message via the `log` crate (upstream `logger.Warnf`).
#[macro_export]
macro_rules! warnf {
    ($($arg:tt)*) => { ::log::warn!($($arg)*) };
}

/// Logs an error message via the `log` crate (upstream `logger.Errorf`).
#[macro_export]
macro_rules! errorf {
    ($($arg:tt)*) => { ::log::error!($($arg)*) };
}

/// Logs a panic message to stderr and panics (upstream `logger.Panicf`).
#[macro_export]
macro_rules! panicf {
    ($($arg:tt)*) => {
        $crate::logger::log_panic(file!(), line!(), &format!($($arg)*))
    };
}

struct EsmLogger;

impl Log for EsmLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let location = match (record.file(), record.line()) {
            (Some(file), Some(line)) => format!("{}:{}", strip_location_prefix(file), line),
            _ => "???:0".to_string(),
        };
        write_log_line(
            level_str(record.level()),
            &location,
            &record.args().to_string(),
        );
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

static LOGGER: EsmLogger = EsmLogger;
static INIT: Once = Once::new();

/// Initializes the logger with the INFO level (upstream `logger.Init`).
///
/// Calling it multiple times is safe. There is no need to call it from tests.
pub fn init() {
    init_with_level(LevelFilter::Info);
}

/// Initializes the logger with the given maximum level.
pub fn init_with_level(level: LevelFilter) {
    INIT.call_once(|| {
        // Ignore the error: another logger may already be installed
        // by the embedding application; its formatting then wins.
        let _ = log::set_logger(&LOGGER);
    });
    log::set_max_level(level);
}

/// Writes a single formatted log line and panics with `msg`.
///
/// This is the backend for the `panicf!` macro and mirrors the upstream's
/// `logger.Panicf` semantics.
pub fn log_panic(file: &str, line: u32, msg: &str) -> ! {
    let location = format!("{}:{}", strip_location_prefix(file), line);
    write_log_line("panic", &location, msg);
    panic!("{msg}");
}

fn level_str(level: Level) -> &'static str {
    match level {
        Level::Error => "error",
        Level::Warn => "warn",
        Level::Info => "info",
        Level::Debug => "debug",
        Level::Trace => "trace",
    }
}

/// Strips the leading path components before `crates/` or `src/`, mirroring
/// Go's stripping of the `/VictoriaMetrics/` repo prefix from log locations.
fn strip_location_prefix(file: &str) -> &str {
    for marker in ["crates/", "crates\\", "src/", "src\\"] {
        if let Some(n) = file.find(marker) {
            return &file[n..];
        }
    }
    file
}

fn write_log_line(level: &str, location: &str, msg: &str) {
    let msg = msg.trim_end_matches('\n');
    let ts = format_timestamp(SystemTime::now());
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{ts}\t{level}\t{location}\t{msg}");
}

/// Formats the given time as `2006-01-02T15:04:05.000Z` (UTC, millisecond
/// precision), matching the upstream's default log timestamp format.
pub fn format_timestamp(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let millis = d.subsec_millis();
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

/// Converts days since 1970-01-01 to (year, month, day).
///
/// Uses Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    let year = yoe as i64 + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn formats_timestamps_in_esm_format() {
        let f = |secs: u64, millis: u64, expected: &str| {
            let t = UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_millis(millis);
            assert_eq!(
                format_timestamp(t),
                expected,
                "unexpected timestamp for {secs}s+{millis}ms"
            );
        };
        f(0, 0, "1970-01-01T00:00:00.000Z");
        f(1_700_000_000, 123, "2023-11-14T22:13:20.123Z");
        f(951_782_400, 0, "2000-02-29T00:00:00.000Z"); // leap day
        f(4_102_444_799, 999, "2099-12-31T23:59:59.999Z");
    }

    #[test]
    fn strips_location_prefix() {
        assert_eq!(
            strip_location_prefix("/home/user/proj/crates/esm-common/src/fs.rs"),
            "crates/esm-common/src/fs.rs"
        );
        assert_eq!(strip_location_prefix("src/main.rs"), "src/main.rs");
        assert_eq!(strip_location_prefix("weird.rs"), "weird.rs");
    }

    #[test]
    fn init_is_idempotent_and_macros_work() {
        init();
        init();
        infof!("info message: {}", 42);
        warnf!("warn message");
        errorf!("error message");
    }

    #[test]
    #[should_panic(expected = "boom 42")]
    fn panicf_logs_and_panics() {
        panicf!("boom {}", 42);
    }
}
