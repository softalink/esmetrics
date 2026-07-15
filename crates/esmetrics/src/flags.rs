//! Go-`flag`-style command-line parsing for the esmetrics binary.
//!
//! Mirrors the subset of the upstream VictoriaMetrics single-node flag surface needed
//! by the skeleton. Syntax follows Go's `flag` package: `-name=value`,
//! `-name value`, `--name=value`, boolean flags without a value; parsing
//! errors mirror Go's `flag provided but not defined: -name` message with
//! the usage text appended.
//!
//! `-retentionPeriod` parsing is ported from upstream `lib/flagutil.RetentionDuration`:
//! a value without a suffix is in months (1 month = 31 days), `M` is months,
//! lowercase `m` (minutes) is rejected, and `s`/`h`/`d`/`w`/`y` suffixes are
//! parsed as durations capped at 1200 months.

/// Printed by `-version` (the upstream prints its `buildinfo` version the same way).
pub const VERSION_STRING: &str =
    concat!("EsMetrics v", env!("CARGO_PKG_VERSION"), " (Softalink LLC)");

const MAX_MONTHS: f64 = 1200.0; // upstream: maxMonths = 12 * 100
const MSECS_PER_31_DAYS: f64 = 31.0 * 24.0 * 3600.0 * 1000.0;

const LOGGER_LEVELS: &[&str] = &["INFO", "WARN", "ERROR", "FATAL", "PANIC"];

/// (name, default, help) for every defined flag; drives the usage text.
const FLAG_DEFS: &[(&str, &str, &str)] = &[
    (
        "httpListenAddr",
        ":8428",
        "TCP address to listen for incoming http requests",
    ),
    (
        "storageDataPath",
        "esmetrics-data",
        "Path to storage data",
    ),
    (
        "retentionPeriod",
        "1",
        "Data with timestamps outside the retentionPeriod is automatically deleted. \
         The following optional suffixes are supported: s (second), h (hour), d (day), \
         w (week), M (month), y (year). If suffix isn't set, then the duration is counted in months",
    ),
    (
        "loggerLevel",
        "INFO",
        "Minimum level of errors to log. Possible values: INFO, WARN, ERROR, FATAL, PANIC",
    ),
    (
        "memory.allowedPercent",
        "60",
        "Allowed percent of system memory esmetrics caches may occupy",
    ),
    (
        "memory.allowedBytes",
        "0",
        "Allowed size of system memory esmetrics caches may occupy. \
         Non-zero value overrides -memory.allowedPercent",
    ),
    (
        "search.maxConcurrentRequests",
        "",
        "The maximum number of concurrent search requests. It shouldn't be high, \
         since a single request can saturate all the CPU cores, while many \
         concurrently executed requests may require high amounts of memory. \
         See also -search.maxWorkersPerQuery",
    ),
    (
        "search.maxWorkersPerQuery",
        "",
        "The maximum number of CPU cores a single query can use. The default value \
         should work good for most cases. The flag can be set to lower values for \
         improving performance of big number of concurrently executed queries. \
         The flag can be set to bigger values for improving performance of heavy \
         queries, which scan big number of time series (>10K) and/or big number \
         of samples (>100M). There is no sense in setting this flag to values \
         bigger than the number of CPU cores available on the system",
    ),
    (
        "snapshotAuthKey",
        "",
        "authKey, which must be passed in query string to /snapshot* pages",
    ),
    (
        "graphiteListenAddr",
        "",
        "TCP and UDP address to listen for Graphite plaintext data. Usually \
         :2003 must be set. Doesn't work if empty",
    ),
    (
        "opentsdbListenAddr",
        "",
        "TCP and UDP address to listen for OpenTSDB telnet put messages. \
         Usually :4242 must be set. Doesn't work if empty. HTTP /api/put \
         requests are not served on this address",
    ),
    (
        "opentsdbHTTPListenAddr",
        "",
        "TCP address to listen for OpenTSDB HTTP put requests. \
         Usually :4242 must be set. Doesn't work if empty",
    ),
    (
        "streamAggr.config",
        "",
        "Path to a stream-aggregation config YAML file applied to ingested \
         data before storage. Empty disables it",
    ),
    (
        "streamAggr.keepInput",
        "false",
        "Whether to keep all the input samples after aggregation",
    ),
    (
        "streamAggr.dedupInterval",
        "0s",
        "Global de-duplication interval for stream aggregation",
    ),
    (
        "streamAggr.dropInputLabels",
        "",
        "Comma-separated labels to drop before stream aggregation",
    ),
    (
        "streamAggr.ignoreOldSamples",
        "false",
        "Ignore samples older than the current aggregation interval",
    ),
    (
        "streamAggr.ignoreFirstIntervals",
        "0",
        "Number of initial aggregation intervals to skip",
    ),
    (
        "streamAggr.flushOnShutdown",
        "false",
        "Flush incomplete stream-aggregation state on shutdown",
    ),
    (
        "streamAggr.enableWindows",
        "false",
        "Enable the blue/green aggregation-window mode, which delays each flush \
         to catch late samples at interval boundaries at the cost of double the \
         state memory",
    ),
    ("version", "", "Show esmetrics version"),
];

/// Parsed command-line flags with upstream-compatible defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Flags {
    pub http_listen_addr: String,
    pub storage_data_path: String,
    /// Raw `-retentionPeriod` value as given on the command line.
    pub retention_period: String,
    /// `-retentionPeriod` parsed to milliseconds.
    pub retention_msecs: i64,
    pub logger_level: String,
    pub memory_allowed_percent: f64,
    pub memory_allowed_bytes: i64,
    /// `-search.maxConcurrentRequests`; 0 → auto `min(2 × cpus, 16)`.
    pub search_max_concurrent_requests: usize,
    /// `-search.maxWorkersPerQuery`; 0 → auto (env override or `min(cpus, 32)`).
    pub search_max_workers_per_query: usize,
    /// `-snapshotAuthKey`; empty means `/snapshot*` pages are open.
    pub snapshot_auth_key: String,
    /// `-graphiteListenAddr`; empty means the Graphite TCP/UDP listener is
    /// disabled.
    pub graphite_listen_addr: String,
    /// `-opentsdbListenAddr`; empty means the OpenTSDB telnet TCP/UDP
    /// listener is disabled.
    pub opentsdb_listen_addr: String,
    /// `-opentsdbHTTPListenAddr`; empty means the dedicated OpenTSDB HTTP
    /// `/api/put` listener is disabled.
    pub opentsdb_http_listen_addr: String,
    /// `-streamAggr.config`; path to a stream-aggregation config YAML file
    /// applied to ingested rows before storage. `None` disables it.
    pub stream_aggr_config: Option<String>,
    /// `-streamAggr.keepInput`; keep all input rows in addition to output.
    pub stream_aggr_keep_input: bool,
    /// `-streamAggr.dedupInterval` in milliseconds (`0` = off).
    pub stream_aggr_dedup_interval_ms: i64,
    /// `-streamAggr.dropInputLabels`; comma-separated labels to drop.
    pub stream_aggr_drop_input_labels: Vec<String>,
    /// `-streamAggr.ignoreOldSamples`.
    pub stream_aggr_ignore_old_samples: bool,
    /// `-streamAggr.ignoreFirstIntervals`.
    pub stream_aggr_ignore_first_intervals: usize,
    /// `-streamAggr.flushOnShutdown`.
    pub stream_aggr_flush_on_shutdown: bool,
    /// `-streamAggr.enableWindows`; enable the blue/green aggregation-window mode.
    pub stream_aggr_enable_windows: bool,
}

impl Default for Flags {
    fn default() -> Flags {
        Flags {
            http_listen_addr: ":8428".to_string(),
            storage_data_path: "esmetrics-data".to_string(),
            retention_period: "1".to_string(),
            retention_msecs: MSECS_PER_31_DAYS as i64,
            logger_level: "INFO".to_string(),
            memory_allowed_percent: 60.0,
            memory_allowed_bytes: 0,
            search_max_concurrent_requests: 0,
            search_max_workers_per_query: 0,
            snapshot_auth_key: String::new(),
            graphite_listen_addr: String::new(),
            opentsdb_listen_addr: String::new(),
            opentsdb_http_listen_addr: String::new(),
            stream_aggr_config: None,
            stream_aggr_keep_input: false,
            stream_aggr_dedup_interval_ms: 0,
            stream_aggr_drop_input_labels: Vec::new(),
            stream_aggr_ignore_old_samples: false,
            stream_aggr_ignore_first_intervals: 0,
            stream_aggr_flush_on_shutdown: false,
            stream_aggr_enable_windows: false,
        }
    }
}

impl Flags {
    /// Maps the validated `-loggerLevel` value to a `log` level filter.
    /// FATAL and PANIC have no `log` counterpart and map to Error.
    pub fn level_filter(&self) -> log::LevelFilter {
        match self.logger_level.as_str() {
            "WARN" => log::LevelFilter::Warn,
            "ERROR" | "FATAL" | "PANIC" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        }
    }
}

/// Result of parsing the command line.
#[derive(Debug, PartialEq)]
pub enum ParseOutcome {
    // Boxed: `Flags` grew past clippy::large_enum_variant's threshold once
    // the graphite/opentsdb listen-addr strings were added, while `Help`
    // and `Version` carry no data at all.
    Flags(Box<Flags>),
    /// `-help`/`--help`/`-h`: the caller prints [`usage`] and exits 0.
    Help,
    /// `-version`: the caller prints [`VERSION_STRING`] and exits 0.
    Version,
}

/// Returns the `-help` text listing every defined flag.
pub fn usage() -> String {
    let mut s = String::from("esmetrics - a Rust port of the upstream VictoriaMetrics single-node (v1.146.0).\n\nUsage of esmetrics:\n");
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    for (name, default, help) in FLAG_DEFS {
        s.push_str("  -");
        s.push_str(name);
        s.push_str("\n    \t");
        s.push_str(help);
        // The search.* int flags display their computed auto default,
        // unquoted, the way Go's flag package prints int defaults.
        match *name {
            "search.maxConcurrentRequests" => s.push_str(&format!(
                " (default {})",
                esm_select::default_max_concurrent_requests()
            )),
            "search.maxWorkersPerQuery" => s.push_str(&format!(
                " (default {})",
                esm_common::query_workers::auto_max_workers(cpus)
            )),
            _ if !default.is_empty() => s.push_str(&format!(" (default {default:?})")),
            _ => {}
        }
        s.push('\n');
    }
    s
}

/// Parses the command-line arguments (without the program name).
pub fn parse<I>(args: I) -> Result<ParseOutcome, String>
where
    I: IntoIterator<Item = String>,
{
    let mut flags = Flags::default();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        if arg == "--" {
            if let Some(extra) = it.next() {
                return Err(format!("unexpected argument after \"--\": {extra:?}"));
            }
            break;
        }
        if !arg.starts_with('-') || arg == "-" {
            return Err(format!("unexpected non-flag argument: {arg:?}"));
        }
        let body = arg.strip_prefix("--").unwrap_or(&arg[1..]);
        let (name, inline_value) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (body, None),
        };
        match name {
            "help" | "h" => return Ok(ParseOutcome::Help),
            // Boolean flag: `-version` or `-version=true`; a following
            // argument is never consumed as its value (Go semantics).
            "version" => {
                let on = match &inline_value {
                    None => true,
                    Some(v) => parse_bool(v)
                        .ok_or_else(|| format!("invalid boolean value {v:?} for -version"))?,
                };
                if on {
                    return Ok(ParseOutcome::Version);
                }
            }
            "httpListenAddr"
            | "storageDataPath"
            | "retentionPeriod"
            | "loggerLevel"
            | "memory.allowedPercent"
            | "memory.allowedBytes"
            | "search.maxConcurrentRequests"
            | "search.maxWorkersPerQuery"
            | "snapshotAuthKey"
            | "graphiteListenAddr"
            | "opentsdbListenAddr"
            | "opentsdbHTTPListenAddr"
            | "streamAggr.config"
            | "streamAggr.dedupInterval"
            | "streamAggr.dropInputLabels"
            | "streamAggr.ignoreFirstIntervals" => {
                let value = match inline_value {
                    Some(v) => v,
                    None => it
                        .next()
                        .ok_or_else(|| format!("missing value for flag -{name}"))?,
                };
                set_flag(&mut flags, name, &value)?;
            }
            "streamAggr.keepInput" => {
                flags.stream_aggr_keep_input = parse_bool_arg(&inline_value, name)?;
            }
            "streamAggr.ignoreOldSamples" => {
                flags.stream_aggr_ignore_old_samples = parse_bool_arg(&inline_value, name)?;
            }
            "streamAggr.flushOnShutdown" => {
                flags.stream_aggr_flush_on_shutdown = parse_bool_arg(&inline_value, name)?;
            }
            "streamAggr.enableWindows" => {
                flags.stream_aggr_enable_windows = parse_bool_arg(&inline_value, name)?;
            }
            _ => {
                return Err(format!(
                    "flag provided but not defined: -{name}\n{}",
                    usage()
                ));
            }
        }
    }
    Ok(ParseOutcome::Flags(Box::new(flags)))
}

fn set_flag(flags: &mut Flags, name: &str, value: &str) -> Result<(), String> {
    match name {
        "httpListenAddr" => flags.http_listen_addr = value.to_string(),
        "storageDataPath" => flags.storage_data_path = value.to_string(),
        "retentionPeriod" => {
            flags.retention_msecs = parse_retention_msecs(value).map_err(|err| {
                format!("invalid value {value:?} for flag -retentionPeriod: {err}")
            })?;
            flags.retention_period = value.to_string();
        }
        "loggerLevel" => {
            if !LOGGER_LEVELS.contains(&value) {
                return Err(format!(
                    "invalid value {value:?} for flag -loggerLevel; supported values: {}",
                    LOGGER_LEVELS.join(", ")
                ));
            }
            flags.logger_level = value.to_string();
        }
        "memory.allowedPercent" => {
            flags.memory_allowed_percent = value
                .parse()
                .map_err(|_| format!("invalid value {value:?} for flag -memory.allowedPercent"))?;
        }
        "memory.allowedBytes" => {
            flags.memory_allowed_bytes = parse_bytes(value)
                .map_err(|_| format!("invalid value {value:?} for flag -memory.allowedBytes"))?;
        }
        "search.maxConcurrentRequests" => {
            flags.search_max_concurrent_requests = value.parse().map_err(|_| {
                format!("invalid value {value:?} for flag -search.maxConcurrentRequests")
            })?;
        }
        "search.maxWorkersPerQuery" => {
            flags.search_max_workers_per_query = value.parse().map_err(|_| {
                format!("invalid value {value:?} for flag -search.maxWorkersPerQuery")
            })?;
        }
        "snapshotAuthKey" => flags.snapshot_auth_key = value.to_string(),
        "graphiteListenAddr" => flags.graphite_listen_addr = value.to_string(),
        "opentsdbListenAddr" => flags.opentsdb_listen_addr = value.to_string(),
        "opentsdbHTTPListenAddr" => flags.opentsdb_http_listen_addr = value.to_string(),
        "streamAggr.config" => flags.stream_aggr_config = Some(value.to_string()),
        "streamAggr.dedupInterval" => {
            let ms = esm_metricsql::duration_value(value, 0).map_err(|e| {
                format!("invalid value {value:?} for flag -streamAggr.dedupInterval: {e}")
            })?;
            if ms < 0 {
                return Err("streamAggr.dedupInterval must be non-negative".to_string());
            }
            flags.stream_aggr_dedup_interval_ms = ms;
        }
        "streamAggr.dropInputLabels" => {
            flags.stream_aggr_drop_input_labels = value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        "streamAggr.ignoreFirstIntervals" => {
            flags.stream_aggr_ignore_first_intervals = value.parse().map_err(|_| {
                format!("invalid value {value:?} for flag -streamAggr.ignoreFirstIntervals")
            })?;
        }
        _ => unreachable!("set_flag called with undefined flag -{name}"),
    }
    Ok(())
}

/// Parses an optional inline boolean flag value: `None` (bare `-flag`) → true,
/// otherwise `strconv.ParseBool` semantics.
fn parse_bool_arg(inline_value: &Option<String>, name: &str) -> Result<bool, String> {
    match inline_value {
        None => Ok(true),
        Some(v) => parse_bool(v).ok_or_else(|| format!("invalid boolean value {v:?} for -{name}")),
    }
}

/// Go `strconv.ParseBool` value set.
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// Parses a `-retentionPeriod` value to milliseconds
/// (port of upstream `lib/flagutil.RetentionDuration.Set`).
pub fn parse_retention_msecs(value: &str) -> Result<i64, String> {
    if value.is_empty() {
        return Ok(0);
    }
    // Months with an explicit `M` suffix.
    if let Some(cut) = value.strip_suffix('M') {
        let months: f64 = cut
            .parse()
            .map_err(|_| format!("cannot parse months from {value:?}"))?;
        return months_to_msecs(months);
    }
    // A bare number is treated as months (historical upstream behavior).
    if let Ok(months) = value.parse::<f64>() {
        return months_to_msecs(months);
    }
    let lower = value.to_ascii_lowercase();
    if lower.ends_with('m') {
        return Err(format!(
            "duration in months must be set with capital `M` suffix, \
             lower case `m` means minutes and not allowed; got {value}"
        ));
    }
    // Parse the (possibly compound, multi-unit) duration via metricsql, just
    // like upstream RetentionDuration.Set delegates to
    // metricsql.PositiveDurationValue once the months / lowercase-`m` handling
    // above is done. This is what makes values like `1y6d` or `1w3d12h` parse
    // instead of only single-unit durations.
    let msecs = esm_metricsql::positive_duration_value(&lower, 0).map_err(|e| e.to_string())?;
    if msecs / (MSECS_PER_31_DAYS as i64) > MAX_MONTHS as i64 {
        return Err(format!(
            "duration must be smaller than {MAX_MONTHS} months; got approx {} months",
            msecs / (MSECS_PER_31_DAYS as i64)
        ));
    }
    Ok(msecs)
}

fn months_to_msecs(months: f64) -> Result<i64, String> {
    if months > MAX_MONTHS {
        return Err(format!(
            "duration months must be smaller than {MAX_MONTHS}; got {months}"
        ));
    }
    if months < 0.0 {
        return Err(format!("duration months cannot be negative; got {months}"));
    }
    Ok((months * MSECS_PER_31_DAYS) as i64)
}

/// Parses a byte-size value with an optional unit suffix to a byte count
/// (port of upstream `lib/flagutil.parseBytes` + `normalizeBytesString`).
///
/// Accepts a plain integer or float, or a value with a decimal suffix
/// (`KB`/`MB`/`GB`/`TB`, base 1000) or a binary suffix
/// (`KiB`/`MiB`/`GiB`/`TiB`, base 1024). The number part may itself be a
/// float (e.g. `1.5GB`). Matches Go's `int64(f * factor)` truncation.
pub fn parse_bytes(value: &str) -> Result<i64, String> {
    // Upstream `Bytes.Set` maps an empty value to zero.
    if value.is_empty() {
        return Ok(0);
    }
    // normalizeBytesString: upper-case, then restore the lower-case `i`
    // used by the binary (KiB/MiB/GiB/TiB) suffixes.
    let normalized = value.to_uppercase().replace('I', "i");
    let parse_num = |num: &str| -> Result<f64, String> {
        num.parse::<f64>()
            .map_err(|_| format!("cannot parse byte size {value:?}"))
    };
    // Binary suffixes are checked first; `KiB` never matches the `KB` case
    // since it ends in `iB`, so the order only avoids redundant work.
    let (num, factor) = if let Some(n) = normalized.strip_suffix("KiB") {
        (n, 1024.0)
    } else if let Some(n) = normalized.strip_suffix("MiB") {
        (n, 1024.0 * 1024.0)
    } else if let Some(n) = normalized.strip_suffix("GiB") {
        (n, 1024.0 * 1024.0 * 1024.0)
    } else if let Some(n) = normalized.strip_suffix("TiB") {
        (n, 1024.0 * 1024.0 * 1024.0 * 1024.0)
    } else if let Some(n) = normalized.strip_suffix("KB") {
        (n, 1000.0)
    } else if let Some(n) = normalized.strip_suffix("MB") {
        (n, 1000.0 * 1000.0)
    } else if let Some(n) = normalized.strip_suffix("GB") {
        (n, 1000.0 * 1000.0 * 1000.0)
    } else if let Some(n) = normalized.strip_suffix("TB") {
        (n, 1000.0 * 1000.0 * 1000.0 * 1000.0)
    } else {
        (normalized.as_str(), 1.0)
    };
    Ok((parse_num(num)? * factor) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_flags(args: &[&str]) -> Result<ParseOutcome, String> {
        parse(args.iter().map(|s| s.to_string()))
    }

    fn parse_ok(args: &[&str]) -> Flags {
        match parse_flags(args) {
            Ok(ParseOutcome::Flags(f)) => *f,
            other => panic!("expected flags for {args:?}; got {other:?}"),
        }
    }

    #[test]
    fn defaults_when_no_args() {
        let flags = parse_ok(&[]);
        assert_eq!(flags, Flags::default());
        assert_eq!(flags.http_listen_addr, ":8428");
        assert_eq!(flags.storage_data_path, "esmetrics-data");
        assert_eq!(flags.retention_period, "1");
        assert_eq!(flags.retention_msecs, 2_678_400_000); // 31 days
        assert_eq!(flags.logger_level, "INFO");
        assert_eq!(flags.memory_allowed_percent, 60.0);
        assert_eq!(flags.memory_allowed_bytes, 0);
        assert_eq!(flags.search_max_concurrent_requests, 0);
        assert_eq!(flags.search_max_workers_per_query, 0);
        assert_eq!(flags.graphite_listen_addr, "");
        assert_eq!(flags.opentsdb_listen_addr, "");
        assert_eq!(flags.opentsdb_http_listen_addr, "");
    }

    #[test]
    fn graphite_and_opentsdb_listen_addrs_default_disabled_and_parse() {
        let flags = parse_ok(&[]);
        assert_eq!(flags.graphite_listen_addr, "");
        assert_eq!(flags.opentsdb_listen_addr, "");
        assert_eq!(flags.opentsdb_http_listen_addr, "");

        let flags = parse_ok(&[
            "-graphiteListenAddr=:2003",
            "-opentsdbListenAddr=:4242",
            "-opentsdbHTTPListenAddr=:4243",
        ]);
        assert_eq!(flags.graphite_listen_addr, ":2003");
        assert_eq!(flags.opentsdb_listen_addr, ":4242");
        assert_eq!(flags.opentsdb_http_listen_addr, ":4243");
    }

    #[test]
    fn accepts_all_flag_syntaxes() {
        for args in [
            &["-httpListenAddr=127.0.0.1:9999"][..],
            &["--httpListenAddr=127.0.0.1:9999"][..],
            &["-httpListenAddr", "127.0.0.1:9999"][..],
            &["--httpListenAddr", "127.0.0.1:9999"][..],
        ] {
            let flags = parse_ok(args);
            assert_eq!(flags.http_listen_addr, "127.0.0.1:9999", "args: {args:?}");
        }
    }

    #[test]
    fn parses_every_defined_flag() {
        let flags = parse_ok(&[
            "-httpListenAddr=:9090",
            "-storageDataPath",
            "/tmp/esm",
            "-retentionPeriod=30d",
            "-loggerLevel=WARN",
            "-memory.allowedPercent=75.5",
            "-memory.allowedBytes=1048576",
            "-search.maxConcurrentRequests=4",
            "-search.maxWorkersPerQuery",
            "2",
        ]);
        assert_eq!(flags.http_listen_addr, ":9090");
        assert_eq!(flags.storage_data_path, "/tmp/esm");
        assert_eq!(flags.retention_period, "30d");
        assert_eq!(flags.retention_msecs, 30 * 86_400_000);
        assert_eq!(flags.logger_level, "WARN");
        assert_eq!(flags.memory_allowed_percent, 75.5);
        assert_eq!(flags.memory_allowed_bytes, 1_048_576);
        assert_eq!(flags.search_max_concurrent_requests, 4);
        assert_eq!(flags.search_max_workers_per_query, 2);
    }

    #[test]
    fn version_flag_is_boolean() {
        assert_eq!(parse_flags(&["-version"]), Ok(ParseOutcome::Version));
        assert_eq!(parse_flags(&["--version"]), Ok(ParseOutcome::Version));
        assert_eq!(parse_flags(&["-version=true"]), Ok(ParseOutcome::Version));
        // `-version=false` is a no-op, not a request to print the version.
        assert!(matches!(
            parse_flags(&["-version=false"]),
            Ok(ParseOutcome::Flags(_))
        ));
        // A following argument is never consumed as the bool's value.
        assert_eq!(
            parse_flags(&["-version", "-loggerLevel=WARN"]),
            Ok(ParseOutcome::Version)
        );
        assert!(parse_flags(&["-version=maybe"]).is_err());
    }

    #[test]
    fn help_flag_variants() {
        for args in [&["-help"][..], &["--help"][..], &["-h"][..]] {
            assert_eq!(parse_flags(args), Ok(ParseOutcome::Help), "args: {args:?}");
        }
    }

    #[test]
    fn unknown_flag_lists_all_flags() {
        let err = parse_flags(&["-bogus"]).unwrap_err();
        assert!(
            err.contains("flag provided but not defined: -bogus"),
            "{err}"
        );
        for (name, _, _) in FLAG_DEFS {
            assert!(
                err.contains(&format!("-{name}")),
                "usage misses -{name}: {err}"
            );
        }
    }

    #[test]
    fn missing_value_is_an_error() {
        let err = parse_flags(&["-httpListenAddr"]).unwrap_err();
        assert!(
            err.contains("missing value for flag -httpListenAddr"),
            "{err}"
        );
    }

    #[test]
    fn positional_argument_is_an_error() {
        assert!(parse_flags(&["serve"]).is_err());
        assert!(parse_flags(&["--", "serve"]).is_err());
        assert!(matches!(parse_flags(&["--"]), Ok(ParseOutcome::Flags(_))));
    }

    #[test]
    fn invalid_numeric_and_level_values_are_errors() {
        assert!(parse_flags(&["-memory.allowedPercent=abc"]).is_err());
        // Note: `-memory.allowedBytes=1.5` is VALID (Go truncates it to 1);
        // see `memory_allowed_bytes_accepts_suffixes_and_floats`.
        assert!(parse_flags(&["-memory.allowedBytes=abc"]).is_err());
        assert!(parse_flags(&["-loggerLevel=verbose"]).is_err());
        assert!(parse_flags(&["-retentionPeriod=1m"]).is_err());
        assert!(parse_flags(&["-search.maxConcurrentRequests=abc"]).is_err());
        assert!(parse_flags(&["-search.maxConcurrentRequests=-1"]).is_err());
        assert!(parse_flags(&["-search.maxWorkersPerQuery=1.5"]).is_err());
        assert!(parse_flags(&["-search.maxWorkersPerQuery=-3"]).is_err());
    }

    #[test]
    fn memory_allowed_bytes_accepts_suffixes_and_floats() {
        // Expected byte counts computed from Go's `flagutil.parseBytes`:
        // decimal suffixes are base 1000, binary suffixes base 1024, and a
        // bare float is truncated via `int64(f)`.
        let cases: &[(&str, i64)] = &[
            ("1GiB", 1_073_741_824),
            ("512MB", 512_000_000),
            ("1.5GB", 1_500_000_000),
            ("1048576", 1_048_576),
            ("1.5", 1),
        ];
        for (input, expected) in cases {
            let arg = format!("-memory.allowedBytes={input}");
            let flags = parse_ok(&[arg.as_str()]);
            assert_eq!(
                flags.memory_allowed_bytes, *expected,
                "-memory.allowedBytes={input}"
            );
        }
    }

    #[test]
    fn retention_parsing_table() {
        const MONTH: i64 = 2_678_400_000;
        const HOUR: i64 = 3_600_000;
        const DAY: i64 = 24 * HOUR;
        let ok_cases: &[(&str, i64)] = &[
            ("", 0),
            ("0", 0),
            ("1", MONTH),
            ("12", 12 * MONTH),
            ("1.5", (1.5 * MONTH as f64) as i64),
            ("2M", 2 * MONTH),
            ("60s", 60_000),
            ("24h", DAY),
            ("30d", 30 * DAY),
            ("4w", 28 * DAY),
            ("1y", 365 * DAY),
            ("1.5d", 36 * HOUR),
            ("1200", 1200 * MONTH),
            // Compound, multi-unit durations parsed via metricsql, matching
            // upstream RetentionDuration.Set (which delegates to
            // metricsql.PositiveDurationValue). None may end in lowercase `m`.
            ("1y6d", 365 * DAY + 6 * DAY),
            ("1w3d", 10 * DAY),
            ("1d12h", DAY + 12 * HOUR),
            ("2h30s", 2 * HOUR + 30_000),
        ];
        for &(input, want) in ok_cases {
            assert_eq!(parse_retention_msecs(input), Ok(want), "input: {input:?}");
        }
        let err_cases = [
            "1m", "5m", "-1", "-24h", "1201", "1300M", "200y", "foo", "hM",
        ];
        for input in err_cases {
            assert!(
                parse_retention_msecs(input).is_err(),
                "expected error for {input:?}"
            );
        }
    }

    #[test]
    fn logger_level_maps_to_filter() {
        let level = |s: &str| {
            Flags {
                logger_level: s.to_string(),
                ..Flags::default()
            }
            .level_filter()
        };
        assert_eq!(level("INFO"), log::LevelFilter::Info);
        assert_eq!(level("WARN"), log::LevelFilter::Warn);
        assert_eq!(level("ERROR"), log::LevelFilter::Error);
        assert_eq!(level("FATAL"), log::LevelFilter::Error);
        assert_eq!(level("PANIC"), log::LevelFilter::Error);
    }

    #[test]
    fn usage_mentions_defaults() {
        let usage = usage();
        assert!(usage.contains("(default \":8428\")"), "{usage}");
        assert!(usage.contains("(default \"esmetrics-data\")"), "{usage}");
    }

    #[test]
    fn search_flags_usage_shows_computed_defaults() {
        let usage = usage();
        let cpus = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        // Each search flag shows its own computed, unquoted default —
        // Go flag-package style — attached to its own help text.
        let concurrent = format!(
            "See also -search.maxWorkersPerQuery (default {})",
            esm_select::default_max_concurrent_requests()
        );
        assert!(usage.contains(&concurrent), "{usage}");
        let workers = format!(
            "available on the system (default {})",
            esm_common::query_workers::auto_max_workers(cpus)
        );
        assert!(usage.contains(&workers), "{usage}");
    }
}
