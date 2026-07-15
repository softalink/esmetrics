//! Go-`flag`-style command-line parsing for the esmauth binary.
//!
//! Mirrors the esmetrics binary's parser structure (see
//! `crates/esmetrics/src/flags.rs`): syntax follows Go's `flag` package
//! (`-name=value`, `-name value`, `--name=value`, boolean flags without a
//! value); an unknown flag mirrors Go's `flag provided but not defined: -name`
//! message with the usage text appended. Flag defaults mirror the upstream
//! `app/vmauth` surface (`-httpListenAddr` default `:8427`, etc.).

use std::time::Duration;

/// Printed by `-version`.
pub const VERSION_STRING: &str = concat!(
    "EsMetrics esmauth v",
    env!("CARGO_PKG_VERSION"),
    " (Softalink LLC)"
);

/// (name, default, help) for every defined flag; drives the usage text.
const FLAG_DEFS: &[(&str, &str, &str)] = &[
    (
        "auth.config",
        "",
        "Path to auth config. It must contain the configuration of users and \
         backends to proxy requests to. This flag is required",
    ),
    (
        "httpListenAddr",
        ":8427",
        "TCP address to listen for incoming http requests",
    ),
    (
        "maxConcurrentRequests",
        "1000",
        "The maximum number of concurrent requests esmauth can process simultaneously",
    ),
    (
        "maxConcurrentPerUserRequests",
        "100",
        "The maximum number of concurrent requests esmauth can process per each configured user. \
         Set to 0 to disable the per-user concurrency limit",
    ),
    (
        "maxQueueDuration",
        "60s",
        "The maximum duration to wait before rejecting incoming requests if the concurrency limit \
         is reached",
    ),
    (
        "failTimeout",
        "3s",
        "Sets a delay period for load balancing to skip a malfunctioning backend",
    ),
    (
        "configCheckInterval",
        "0",
        "Interval for config file re-read. Zero value disables config re-reading. By default, \
         refreshing is disabled, and it is triggered via SIGHUP signal",
    ),
    (
        "logInvalidAuthTokens",
        "false",
        "Whether to log requests with invalid auth tokens. Such requests are always counted at \
         esmauth_http_request_errors_total{reason=\"invalid_auth_token\"} metric",
    ),
    (
        "reloadAuthKey",
        "",
        "authKey, which must be passed in query string to /-/reload. It overrides the empty \
         default, which leaves /-/reload open",
    ),
    (
        "metricsAuthKey",
        "",
        "authKey, which must be passed in query string to /metrics. It overrides the empty \
         default, which leaves /metrics open",
    ),
    (
        "readTimeout",
        "30s",
        "Per-read idle timeout for incoming connections. A connection that stops making progress \
         (e.g. a slow-loris that trickles headers) is dropped after this idle period; a steadily \
         progressing large upload is not cut. Zero disables the timeout",
    ),
    (
        "maxRequestBodySizeToRetry",
        "16384",
        "The maximum request body size, which can be cached and re-tried at other backends. \
         Bigger values require more memory",
    ),
    ("version", "", "Show esmauth version"),
];

/// Parsed command-line flags with upstream-compatible defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Flags {
    /// `-auth.config` (required; empty is rejected by [`parse`]).
    pub auth_config: String,
    pub http_listen_addr: String,
    pub max_concurrent_requests: usize,
    /// `-maxConcurrentPerUserRequests`; 0 disables the per-user limit.
    pub max_concurrent_per_user_requests: usize,
    pub max_queue_duration: Duration,
    pub fail_timeout: Duration,
    /// `-configCheckInterval`; 0 disables interval-based re-reading.
    pub config_check_interval: Duration,
    pub log_invalid_auth_tokens: bool,
    pub max_request_body_size_to_retry: usize,
    /// `-reloadAuthKey`; empty means `/-/reload` is open (current behavior).
    pub reload_auth_key: String,
    /// `-metricsAuthKey`; empty means `/metrics` is open (current behavior).
    pub metrics_auth_key: String,
    /// `-readTimeout`; per-read idle timeout for incoming connections. Zero
    /// disables the timeout.
    pub read_timeout: Duration,
}

impl Default for Flags {
    fn default() -> Flags {
        Flags {
            auth_config: String::new(),
            http_listen_addr: ":8427".to_string(),
            max_concurrent_requests: 1000,
            max_concurrent_per_user_requests: 100,
            max_queue_duration: Duration::from_secs(60),
            fail_timeout: Duration::from_secs(3),
            config_check_interval: Duration::ZERO,
            log_invalid_auth_tokens: false,
            max_request_body_size_to_retry: 16384,
            reload_auth_key: String::new(),
            metrics_auth_key: String::new(),
            read_timeout: Duration::from_secs(30),
        }
    }
}

/// Result of parsing the command line.
#[derive(Debug, PartialEq)]
pub enum ParseOutcome {
    Flags(Box<Flags>),
    /// `-help`/`--help`/`-h`: the caller prints [`usage`] and exits 0.
    Help,
    /// `-version`: the caller prints [`VERSION_STRING`] and exits 0.
    Version,
}

/// Returns the `-help` text listing every defined flag.
pub fn usage() -> String {
    let mut s = String::from(
        "esmauth - a Rust port of the upstream VictoriaMetrics vmauth (v1.146.0).\n\n\
         Usage of esmauth:\n",
    );
    for (name, default, help) in FLAG_DEFS {
        s.push_str("  -");
        s.push_str(name);
        s.push_str("\n    \t");
        s.push_str(help);
        if !default.is_empty() {
            s.push_str(&format!(" (default {default:?})"));
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
            // Boolean flags: value-less means true; a following argument is
            // never consumed as the value (Go semantics).
            "version" => {
                if parse_optional_bool(&inline_value, "version")? {
                    return Ok(ParseOutcome::Version);
                }
            }
            "logInvalidAuthTokens" => {
                flags.log_invalid_auth_tokens =
                    parse_optional_bool(&inline_value, "logInvalidAuthTokens")?;
            }
            "auth.config"
            | "httpListenAddr"
            | "maxConcurrentRequests"
            | "maxConcurrentPerUserRequests"
            | "maxQueueDuration"
            | "failTimeout"
            | "configCheckInterval"
            | "maxRequestBodySizeToRetry"
            | "reloadAuthKey"
            | "metricsAuthKey"
            | "readTimeout" => {
                let value = match inline_value {
                    Some(v) => v,
                    None => it
                        .next()
                        .ok_or_else(|| format!("missing value for flag -{name}"))?,
                };
                set_flag(&mut flags, name, &value)?;
            }
            _ => {
                return Err(format!(
                    "flag provided but not defined: -{name}\n{}",
                    usage()
                ));
            }
        }
    }

    if flags.auth_config.is_empty() {
        return Err("missing required flag -auth.config".to_string());
    }
    Ok(ParseOutcome::Flags(Box::new(flags)))
}

fn set_flag(flags: &mut Flags, name: &str, value: &str) -> Result<(), String> {
    match name {
        "auth.config" => flags.auth_config = value.to_string(),
        "httpListenAddr" => flags.http_listen_addr = value.to_string(),
        "maxConcurrentRequests" => {
            flags.max_concurrent_requests = value
                .parse()
                .map_err(|_| format!("invalid value {value:?} for flag -maxConcurrentRequests"))?;
        }
        "maxConcurrentPerUserRequests" => {
            flags.max_concurrent_per_user_requests = value.parse().map_err(|_| {
                format!("invalid value {value:?} for flag -maxConcurrentPerUserRequests")
            })?;
        }
        "maxQueueDuration" => {
            flags.max_queue_duration = parse_go_duration(value)
                .map_err(|e| format!("invalid value {value:?} for flag -maxQueueDuration: {e}"))?;
        }
        "failTimeout" => {
            flags.fail_timeout = parse_go_duration(value)
                .map_err(|e| format!("invalid value {value:?} for flag -failTimeout: {e}"))?;
        }
        "configCheckInterval" => {
            flags.config_check_interval = parse_go_duration(value).map_err(|e| {
                format!("invalid value {value:?} for flag -configCheckInterval: {e}")
            })?;
        }
        "maxRequestBodySizeToRetry" => {
            flags.max_request_body_size_to_retry = value.parse().map_err(|_| {
                format!("invalid value {value:?} for flag -maxRequestBodySizeToRetry")
            })?;
        }
        "reloadAuthKey" => flags.reload_auth_key = value.to_string(),
        "metricsAuthKey" => flags.metrics_auth_key = value.to_string(),
        "readTimeout" => {
            flags.read_timeout = parse_go_duration(value)
                .map_err(|e| format!("invalid value {value:?} for flag -readTimeout: {e}"))?;
        }
        _ => unreachable!("set_flag called with undefined flag -{name}"),
    }
    Ok(())
}

/// Parses a value-less/inline boolean flag: `None` (value-less) is `true`;
/// `Some(v)` parses `v` as a Go bool.
fn parse_optional_bool(inline_value: &Option<String>, name: &str) -> Result<bool, String> {
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

/// Parses a Go `time.Duration` string (a signless sequence of decimal numbers,
/// each with an optional fraction and a required unit suffix, e.g. `"300ms"`,
/// `"1.5h"`, `"2h45m"`). A bare `"0"` is accepted as the zero duration, like
/// Go. Supported units: `ns`, `us`/`µs`, `ms`, `s`, `m`, `h`.
fn parse_go_duration(input: &str) -> Result<Duration, String> {
    if input.is_empty() {
        return Err("empty duration".to_string());
    }
    if input == "0" {
        return Ok(Duration::ZERO);
    }
    let mut rest = input;
    let mut total = Duration::ZERO;
    let mut saw_segment = false;
    while !rest.is_empty() {
        // Number part: digits with an optional single '.'.
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(|| format!("missing unit in duration {input:?}"))?;
        if num_end == 0 {
            return Err(format!("invalid number in duration {input:?}"));
        }
        let value: f64 = rest[..num_end]
            .parse()
            .map_err(|_| format!("invalid number in duration {input:?}"))?;
        rest = &rest[num_end..];
        let (unit_secs, unit_len) = parse_unit(rest)
            .ok_or_else(|| format!("unknown or missing unit in duration {input:?}"))?;
        total += Duration::from_secs_f64(value * unit_secs);
        rest = &rest[unit_len..];
        saw_segment = true;
    }
    if !saw_segment {
        return Err(format!("invalid duration {input:?}"));
    }
    Ok(total)
}

/// Returns `(seconds_per_unit, byte_length_of_unit)` for the unit prefixing
/// `s`, or `None` if no known unit is present.
fn parse_unit(s: &str) -> Option<(f64, usize)> {
    // Order matters: check two-byte units before their single-byte prefixes.
    const NS: f64 = 1e-9;
    if let Some(rest) = s.strip_prefix("ns") {
        let _ = rest;
        return Some((NS, 2));
    }
    if s.starts_with("us") {
        return Some((1e-6, 2));
    }
    if s.starts_with("µs") {
        return Some((1e-6, "µs".len()));
    }
    if s.starts_with("ms") {
        return Some((1e-3, 2));
    }
    match s.as_bytes().first() {
        Some(b's') => Some((1.0, 1)),
        Some(b'm') => Some((60.0, 1)),
        Some(b'h') => Some((3600.0, 1)),
        _ => None,
    }
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
    fn defaults_when_only_required_flag_given() {
        let flags = parse_ok(&["-auth.config=/etc/auth.yml"]);
        assert_eq!(flags.auth_config, "/etc/auth.yml");
        assert_eq!(flags.http_listen_addr, ":8427");
        assert_eq!(flags.max_concurrent_requests, 1000);
        assert_eq!(flags.max_concurrent_per_user_requests, 100);
        assert_eq!(flags.max_queue_duration, Duration::from_secs(60));
        assert_eq!(flags.fail_timeout, Duration::from_secs(3));
        assert_eq!(flags.config_check_interval, Duration::ZERO);
        assert!(!flags.log_invalid_auth_tokens);
        assert_eq!(flags.max_request_body_size_to_retry, 16384);
        // Everything but auth.config matches Default.
        assert_eq!(
            flags,
            Flags {
                auth_config: "/etc/auth.yml".to_string(),
                ..Flags::default()
            }
        );
    }

    #[test]
    fn auth_config_is_required() {
        let err = parse_flags(&[]).unwrap_err();
        assert!(err.contains("-auth.config"), "{err}");
        let err = parse_flags(&["-httpListenAddr=:9000"]).unwrap_err();
        assert!(err.contains("missing required flag -auth.config"), "{err}");
    }

    #[test]
    fn default_listen_addr_is_8427() {
        let flags = parse_ok(&["-auth.config=x"]);
        assert_eq!(flags.http_listen_addr, ":8427");
    }

    #[test]
    fn accepts_all_flag_syntaxes() {
        for args in [
            &["-auth.config=x", "-httpListenAddr=127.0.0.1:9999"][..],
            &["-auth.config=x", "--httpListenAddr=127.0.0.1:9999"][..],
            &["-auth.config=x", "-httpListenAddr", "127.0.0.1:9999"][..],
            &["-auth.config=x", "--httpListenAddr", "127.0.0.1:9999"][..],
        ] {
            let flags = parse_ok(args);
            assert_eq!(flags.http_listen_addr, "127.0.0.1:9999", "args: {args:?}");
        }
    }

    #[test]
    fn parses_every_defined_flag() {
        let flags = parse_ok(&[
            "-auth.config",
            "/tmp/a.yml",
            "-httpListenAddr=:9090",
            "-maxConcurrentRequests=500",
            "-maxConcurrentPerUserRequests=0",
            "-maxQueueDuration=30s",
            "-failTimeout=5s",
            "-configCheckInterval=1m",
            "-logInvalidAuthTokens",
            "-maxRequestBodySizeToRetry=4096",
            "-reloadAuthKey=rsecret",
            "-metricsAuthKey=msecret",
            "-readTimeout=15s",
        ]);
        assert_eq!(flags.auth_config, "/tmp/a.yml");
        assert_eq!(flags.http_listen_addr, ":9090");
        assert_eq!(flags.max_concurrent_requests, 500);
        assert_eq!(flags.max_concurrent_per_user_requests, 0);
        assert_eq!(flags.max_queue_duration, Duration::from_secs(30));
        assert_eq!(flags.fail_timeout, Duration::from_secs(5));
        assert_eq!(flags.config_check_interval, Duration::from_secs(60));
        assert!(flags.log_invalid_auth_tokens);
        assert_eq!(flags.max_request_body_size_to_retry, 4096);
        assert_eq!(flags.reload_auth_key, "rsecret");
        assert_eq!(flags.metrics_auth_key, "msecret");
        assert_eq!(flags.read_timeout, Duration::from_secs(15));
    }

    #[test]
    fn auth_key_and_read_timeout_defaults() {
        // Auth keys default to empty (endpoints open, preserving prior
        // behavior); readTimeout defaults to 30s.
        let flags = parse_ok(&["-auth.config=x"]);
        assert_eq!(flags.reload_auth_key, "");
        assert_eq!(flags.metrics_auth_key, "");
        assert_eq!(flags.read_timeout, Duration::from_secs(30));
    }

    #[test]
    fn read_timeout_zero_disables_and_parses_into_flags() {
        let flags = parse_ok(&["-auth.config=x", "-readTimeout=0"]);
        assert_eq!(flags.read_timeout, Duration::ZERO);
        assert!(parse_flags(&["-auth.config=x", "-readTimeout=nope"]).is_err());
    }

    #[test]
    fn log_invalid_auth_tokens_boolean_forms() {
        assert!(parse_ok(&["-auth.config=x", "-logInvalidAuthTokens"]).log_invalid_auth_tokens);
        assert!(
            parse_ok(&["-auth.config=x", "-logInvalidAuthTokens=true"]).log_invalid_auth_tokens
        );
        assert!(
            !parse_ok(&["-auth.config=x", "-logInvalidAuthTokens=false"]).log_invalid_auth_tokens
        );
        // A following argument is never consumed as the bool's value.
        let flags = parse_ok(&[
            "-auth.config=x",
            "-logInvalidAuthTokens",
            "-httpListenAddr=:1",
        ]);
        assert!(flags.log_invalid_auth_tokens);
        assert_eq!(flags.http_listen_addr, ":1");
    }

    #[test]
    fn version_flag_is_boolean() {
        assert_eq!(parse_flags(&["-version"]), Ok(ParseOutcome::Version));
        assert_eq!(parse_flags(&["--version"]), Ok(ParseOutcome::Version));
        assert_eq!(parse_flags(&["-version=true"]), Ok(ParseOutcome::Version));
        // `-version=false` is a no-op, but auth.config is still required.
        assert!(parse_flags(&["-version=false"]).is_err());
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
    fn invalid_numeric_and_duration_values_are_errors() {
        assert!(parse_flags(&["-auth.config=x", "-maxConcurrentRequests=abc"]).is_err());
        assert!(parse_flags(&["-auth.config=x", "-maxConcurrentRequests=-1"]).is_err());
        assert!(parse_flags(&["-auth.config=x", "-maxQueueDuration=10"]).is_err());
        assert!(parse_flags(&["-auth.config=x", "-failTimeout=soon"]).is_err());
        assert!(parse_flags(&["-auth.config=x", "-maxRequestBodySizeToRetry=1.5"]).is_err());
    }

    #[test]
    fn go_duration_parsing_table() {
        let cases: &[(&str, Duration)] = &[
            ("0", Duration::ZERO),
            ("60s", Duration::from_secs(60)),
            ("3s", Duration::from_secs(3)),
            ("500ms", Duration::from_millis(500)),
            ("1m", Duration::from_secs(60)),
            ("2h", Duration::from_secs(7200)),
            ("1.5s", Duration::from_millis(1500)),
            ("2h45m", Duration::from_secs(2 * 3600 + 45 * 60)),
            ("1m30s", Duration::from_secs(90)),
        ];
        for &(input, want) in cases {
            assert_eq!(parse_go_duration(input).unwrap(), want, "input {input:?}");
        }
        for bad in ["", "10", "abc", "5x", "s", "1.2.3s"] {
            assert!(
                parse_go_duration(bad).is_err(),
                "expected error for {bad:?}"
            );
        }
    }

    #[test]
    fn usage_mentions_defaults() {
        let usage = usage();
        assert!(usage.contains("(default \":8427\")"), "{usage}");
        assert!(usage.contains("(default \"1000\")"), "{usage}");
        assert!(usage.contains("(default \"60s\")"), "{usage}");
    }

    #[test]
    fn positional_argument_is_an_error() {
        assert!(parse_flags(&["serve"]).is_err());
    }
}
