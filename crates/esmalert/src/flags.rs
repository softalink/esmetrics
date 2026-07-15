//! Go-`flag`-style command-line parsing for the esmalert binary.
//!
//! Mirrors `esmauth::flags` (`crates/esmauth/src/flags.rs`): syntax follows
//! Go's `flag` package (`-name=value`, `-name value`, `--name=value`,
//! boolean flags without a value); an unknown flag mirrors Go's `flag
//! provided but not defined: -name` message with the usage text appended.
//! Durations parse via [`esm_metricsql::duration_value`], the same grammar
//! `crate::config`'s YAML duration fields use, for consistency between
//! flag-supplied and config-supplied durations.
//!
//! Auth/TLS flags are grouped per upstream-vmalert component (`-datasource.*`,
//! `-remoteWrite.*`, `-remoteRead.*`, `-notifier.*`) into one [`AuthFlagSet`]
//! shape, since all four components share the same
//! `basicAuth.{username,password[,File]}` / `bearerToken[,File]` / `tls*`
//! surface. `main.rs`'s `app` module resolves each `AuthFlagSet` into a
//! `datasource::AuthConfig`/`datasource::TlsConfig` pair at startup (reading
//! any `*File` secret from disk there, not here — this module never touches
//! the filesystem).

use std::time::Duration;

/// Printed by `-version`.
pub const VERSION_STRING: &str = concat!(
    "EsMetrics esmalert v",
    env!("CARGO_PKG_VERSION"),
    " (Softalink LLC)"
);

/// Raw auth/TLS flag values for one component (datasource, remote-write,
/// remote-read, or notifier). Field names mirror the CLI flag suffix with
/// `.`s replaced by `_` (e.g. `-datasource.basicAuth.passwordFile` ->
/// `password_file`). An empty string means "unset", matching Go's
/// `flag.String` zero value convention — [`crate::datasource::AuthConfig::from_flags`]
/// already treats an empty string the same as an absent flag.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuthFlagSet {
    pub username: String,
    pub password: String,
    pub password_file: String,
    pub bearer_token: String,
    pub bearer_token_file: String,
    pub tls_ca_file: String,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    pub tls_server_name: String,
    pub tls_insecure_skip_verify: bool,
}

/// Parsed command-line flags with upstream-vmalert-compatible defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Flags {
    /// `-rule`; repeatable. A glob pattern (or plain path) per occurrence,
    /// passed as-is to `config::load_config`.
    pub rule_globs: Vec<String>,
    pub datasource_url: String,
    pub datasource_auth: AuthFlagSet,
    /// `-remoteWrite.url`; `None` disables remote-write (recording rules
    /// then have nowhere to push results — see `app::check_rw_notifier_precondition`).
    pub remote_write_url: Option<String>,
    pub remote_write_auth: AuthFlagSet,
    pub remote_write_flush_interval: Duration,
    pub remote_write_max_batch_size: usize,
    pub remote_write_max_queue_size: usize,
    pub remote_write_concurrency: usize,
    /// `-remoteRead.url`; `None` disables startup alert-state restore.
    pub remote_read_url: Option<String>,
    pub remote_read_auth: AuthFlagSet,
    pub remote_read_lookback: Duration,
    /// `-notifier.url`; repeatable. Empty means no notifier is configured
    /// (alerting rules then have nowhere to send — see
    /// `app::check_rw_notifier_precondition`).
    pub notifier_urls: Vec<String>,
    pub notifier_auth: AuthFlagSet,
    pub evaluation_interval: Duration,
    pub external_url: String,
    /// `-external.alert.source`; accepted but not yet wired into the
    /// notifier's `generatorURL` (see `notifier::alertmanager::alert_json`'s
    /// doc comment) — see the task report for this gap.
    pub external_alert_source: String,
    pub config_check_interval: Duration,
    pub group_max_start_delay: Duration,
    pub http_listen_addr: String,
    pub dry_run: bool,
    /// `-disableAlertgroupLabel`; accepted but not yet wired into
    /// `rule::alert::build_alert_labels`'s `alertgroup` label (see the task
    /// report for this gap).
    pub disable_alertgroup_label: bool,
    pub reload_auth_key: String,
    pub metrics_auth_key: String,
    pub http_read_timeout: Duration,
}

impl Default for Flags {
    fn default() -> Flags {
        Flags {
            rule_globs: Vec::new(),
            datasource_url: String::new(),
            datasource_auth: AuthFlagSet::default(),
            remote_write_url: None,
            remote_write_auth: AuthFlagSet::default(),
            remote_write_flush_interval: Duration::from_secs(5),
            remote_write_max_batch_size: 1000,
            remote_write_max_queue_size: 100_000,
            remote_write_concurrency: 1,
            remote_read_url: None,
            remote_read_auth: AuthFlagSet::default(),
            remote_read_lookback: Duration::from_secs(3600),
            notifier_urls: Vec::new(),
            notifier_auth: AuthFlagSet::default(),
            evaluation_interval: Duration::from_secs(60),
            external_url: String::new(),
            external_alert_source: String::new(),
            config_check_interval: Duration::ZERO,
            group_max_start_delay: Duration::ZERO,
            http_listen_addr: ":8880".to_string(),
            dry_run: false,
            disable_alertgroup_label: false,
            reload_auth_key: String::new(),
            metrics_auth_key: String::new(),
            http_read_timeout: Duration::from_secs(30),
        }
    }
}

/// Error returned by [`parse_flags`]. `Help`/`Version` are control-flow
/// signals (mirroring esmauth's `ParseOutcome::Help`/`Version`), not real
/// parse failures; the caller (`main`) matches them to print
/// [`usage`]/[`VERSION_STRING`] and exit 0.
#[derive(Debug, PartialEq)]
pub enum FlagError {
    Help,
    Version,
    Invalid(String),
}

impl std::fmt::Display for FlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlagError::Help => write!(f, "{}", usage()),
            FlagError::Version => write!(f, "{VERSION_STRING}"),
            FlagError::Invalid(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for FlagError {}

/// Returns the `-help` text. Kept intentionally compact (not a per-flag
/// generated table like esmauth's `FLAG_DEFS`) given the flag surface here
/// spans four auth/TLS components; see the field docs on [`Flags`] for the
/// authoritative per-flag description.
pub fn usage() -> String {
    "esmalert - a Rust port of the upstream VictoriaMetrics vmalert.\n\n\
     Usage of esmalert:\n\
     \x20 -rule=<path-or-glob>            Rule file/glob to load (repeatable, required)\n\
     \x20 -datasource.url=<url>           Datasource URL for rule evaluation queries (required)\n\
     \x20 -datasource.basicAuth.username/-datasource.basicAuth.password[File]\n\
     \x20 -datasource.bearerToken[File]\n\
     \x20 -datasource.tlsCAFile/-datasource.tlsCertFile/-datasource.tlsKeyFile\n\
     \x20 -datasource.tlsServerName/-datasource.tlsInsecureSkipVerify\n\
     \x20 -remoteWrite.url=<url>          Push recording-rule results + alert state here\n\
     \x20 -remoteWrite.flushInterval=<dur> (default \"5s\")\n\
     \x20 -remoteWrite.maxBatchSize=<n>   (default \"1000\")\n\
     \x20 -remoteWrite.maxQueueSize=<n>   (default \"100000\")\n\
     \x20 -remoteWrite.concurrency=<n>    (default \"1\")\n\
     \x20 -remoteWrite.{basicAuth,bearerToken,tls*} -- same shape as -datasource.*\n\
     \x20 -remoteRead.url=<url>           Startup alert-state restore source\n\
     \x20 -remoteRead.lookback=<dur>      (default \"1h\")\n\
     \x20 -remoteRead.{basicAuth,bearerToken,tls*} -- same shape as -datasource.*\n\
     \x20 -notifier.url=<url>             Alertmanager target (repeatable)\n\
     \x20 -notifier.{basicAuth,bearerToken,tls*} -- same shape as -datasource.*\n\
     \x20 -evaluationInterval=<dur>       (default \"1m\")\n\
     \x20 -external.url=<url>             Externally reachable URL of this esmalert\n\
     \x20 -external.alert.source=<tpl>    (accepted, not yet wired -- see task report)\n\
     \x20 -configCheckInterval=<dur>      Re-read -rule on an interval (default \"0\", disabled)\n\
     \x20 -group.maxStartDelay=<dur>      (default \"0\")\n\
     \x20 -httpListenAddr=<addr>          (default \":8880\")\n\
     \x20 -httpReadTimeout=<dur>          (default \"30s\")\n\
     \x20 -reload.authKey=<key>           Gates POST /-/reload\n\
     \x20 -metrics.authKey=<key>          Gates GET /metrics\n\
     \x20 -dryRun                         Validate -rule files and exit\n\
     \x20 -disableAlertgroupLabel         (accepted, not yet wired -- see task report)\n\
     \x20 -version                        Show esmalert version\n\
     \x20 -help                           Show this help\n"
        .to_string()
}

/// Parses the command-line arguments (without the program name).
pub fn parse_flags(argv: &[String]) -> Result<Flags, FlagError> {
    let mut flags = Flags::default();
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        if arg == "--" {
            if let Some(extra) = it.next() {
                return Err(FlagError::Invalid(format!(
                    "unexpected argument after \"--\": {extra:?}"
                )));
            }
            break;
        }
        if !arg.starts_with('-') || arg == "-" {
            return Err(FlagError::Invalid(format!(
                "unexpected non-flag argument: {arg:?}"
            )));
        }
        let body = arg.strip_prefix("--").unwrap_or(&arg[1..]);
        let (name, inline_value) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (body, None),
        };

        match name {
            "help" | "h" => return Err(FlagError::Help),
            "version" => {
                if parse_optional_bool(&inline_value, "version")? {
                    return Err(FlagError::Version);
                }
            }
            "dryRun" => flags.dry_run = parse_optional_bool(&inline_value, "dryRun")?,
            "disableAlertgroupLabel" => {
                flags.disable_alertgroup_label =
                    parse_optional_bool(&inline_value, "disableAlertgroupLabel")?
            }
            "datasource.tlsInsecureSkipVerify" => {
                flags.datasource_auth.tls_insecure_skip_verify =
                    parse_optional_bool(&inline_value, name)?
            }
            "remoteWrite.tlsInsecureSkipVerify" => {
                flags.remote_write_auth.tls_insecure_skip_verify =
                    parse_optional_bool(&inline_value, name)?
            }
            "remoteRead.tlsInsecureSkipVerify" => {
                flags.remote_read_auth.tls_insecure_skip_verify =
                    parse_optional_bool(&inline_value, name)?
            }
            "notifier.tlsInsecureSkipVerify" => {
                flags.notifier_auth.tls_insecure_skip_verify =
                    parse_optional_bool(&inline_value, name)?
            }
            "rule" => flags
                .rule_globs
                .push(take_value(inline_value, &mut it, name)?),
            "notifier.url" => flags
                .notifier_urls
                .push(take_value(inline_value, &mut it, name)?),
            _ => {
                // Checked before consuming a value so an unknown flag with
                // no following argument reports "not defined" rather than
                // a misleading "missing value".
                if !is_known_value_flag(name) {
                    return Err(unknown_flag_error(name));
                }
                let value = take_value(inline_value, &mut it, name)?;
                set_value_flag(&mut flags, name, &value)?;
            }
        }
    }
    Ok(flags)
}

/// Returns the flag's value: the inline `-name=value` value if present, else
/// the next argument (Go's `-name value` form).
fn take_value<'a, I>(inline: Option<String>, it: &mut I, name: &str) -> Result<String, FlagError>
where
    I: Iterator<Item = &'a String>,
{
    match inline {
        Some(v) => Ok(v),
        None => it
            .next()
            .cloned()
            .ok_or_else(|| FlagError::Invalid(format!("missing value for flag -{name}"))),
    }
}

/// Whether `name` is a recognized non-boolean, non-repeatable flag --
/// checked in [`parse_flags`] *before* consuming a following argument as
/// this flag's value, so an unknown flag given without a trailing value
/// (e.g. a lone `-bogus`) reports "not defined" rather than "missing
/// value". Must stay in sync with [`set_value_flag`]'s exact-name arms plus
/// [`is_known_auth_suffix`].
fn is_known_value_flag(name: &str) -> bool {
    matches!(
        name,
        "datasource.url"
            | "remoteWrite.url"
            | "remoteRead.url"
            | "evaluationInterval"
            | "remoteRead.lookback"
            | "remoteWrite.flushInterval"
            | "remoteWrite.maxBatchSize"
            | "remoteWrite.maxQueueSize"
            | "remoteWrite.concurrency"
            | "external.url"
            | "external.alert.source"
            | "configCheckInterval"
            | "group.maxStartDelay"
            | "httpListenAddr"
            | "reload.authKey"
            | "metrics.authKey"
            | "httpReadTimeout"
    ) || is_known_auth_suffix(name)
}

/// Whether `name` is one of the four components' (`datasource.`/
/// `remoteWrite.`/`remoteRead.`/`notifier.`) value-taking auth/TLS flags
/// (every suffix but `tlsInsecureSkipVerify`, which is boolean and handled
/// separately in [`parse_flags`]).
fn is_known_auth_suffix(name: &str) -> bool {
    for prefix in ["datasource.", "remoteWrite.", "remoteRead.", "notifier."] {
        if let Some(suffix) = name.strip_prefix(prefix) {
            if matches!(
                suffix,
                "basicAuth.username"
                    | "basicAuth.password"
                    | "basicAuth.passwordFile"
                    | "bearerToken"
                    | "bearerTokenFile"
                    | "tlsCAFile"
                    | "tlsCertFile"
                    | "tlsKeyFile"
                    | "tlsServerName"
            ) {
                return true;
            }
        }
    }
    false
}

/// Dispatches every non-boolean, non-repeatable flag by exact name, falling
/// back to [`set_auth_suffix_flag`] for the four `<component>.*` auth/TLS
/// prefixes. The caller ([`parse_flags`]) has already verified `name` is
/// known via [`is_known_value_flag`], so the auth-suffix fallback here
/// never hits its own "unknown" case in practice; it stays only as a
/// defensive backstop against the two functions drifting out of sync.
fn set_value_flag(flags: &mut Flags, name: &str, value: &str) -> Result<(), FlagError> {
    match name {
        "datasource.url" => flags.datasource_url = value.to_string(),
        "remoteWrite.url" => flags.remote_write_url = non_empty_owned(value),
        "remoteRead.url" => flags.remote_read_url = non_empty_owned(value),
        "evaluationInterval" => flags.evaluation_interval = parse_duration_flag(value, name)?,
        "remoteRead.lookback" => flags.remote_read_lookback = parse_duration_flag(value, name)?,
        "remoteWrite.flushInterval" => {
            flags.remote_write_flush_interval = parse_duration_flag(value, name)?
        }
        "remoteWrite.maxBatchSize" => {
            flags.remote_write_max_batch_size = parse_usize_flag(value, name)?
        }
        "remoteWrite.maxQueueSize" => {
            flags.remote_write_max_queue_size = parse_usize_flag(value, name)?
        }
        "remoteWrite.concurrency" => {
            flags.remote_write_concurrency = parse_usize_flag(value, name)?
        }
        "external.url" => flags.external_url = value.to_string(),
        "external.alert.source" => flags.external_alert_source = value.to_string(),
        "configCheckInterval" => flags.config_check_interval = parse_duration_flag(value, name)?,
        "group.maxStartDelay" => flags.group_max_start_delay = parse_duration_flag(value, name)?,
        "httpListenAddr" => flags.http_listen_addr = value.to_string(),
        "reload.authKey" => flags.reload_auth_key = value.to_string(),
        "metrics.authKey" => flags.metrics_auth_key = value.to_string(),
        "httpReadTimeout" => flags.http_read_timeout = parse_duration_flag(value, name)?,
        _ => return set_auth_suffix_flag(flags, name, value),
    }
    Ok(())
}

/// Handles `-datasource.*` / `-remoteWrite.*` / `-remoteRead.*` /
/// `-notifier.*` auth/TLS flags (every suffix but the boolean
/// `tlsInsecureSkipVerify`, handled separately in [`parse_flags`] since it
/// takes no value).
fn set_auth_suffix_flag(flags: &mut Flags, name: &str, value: &str) -> Result<(), FlagError> {
    let (set, suffix): (&mut AuthFlagSet, &str) = if let Some(s) = name.strip_prefix("datasource.")
    {
        (&mut flags.datasource_auth, s)
    } else if let Some(s) = name.strip_prefix("remoteWrite.") {
        (&mut flags.remote_write_auth, s)
    } else if let Some(s) = name.strip_prefix("remoteRead.") {
        (&mut flags.remote_read_auth, s)
    } else if let Some(s) = name.strip_prefix("notifier.") {
        (&mut flags.notifier_auth, s)
    } else {
        return Err(unknown_flag_error(name));
    };

    match suffix {
        "basicAuth.username" => set.username = value.to_string(),
        "basicAuth.password" => set.password = value.to_string(),
        "basicAuth.passwordFile" => set.password_file = value.to_string(),
        "bearerToken" => set.bearer_token = value.to_string(),
        "bearerTokenFile" => set.bearer_token_file = value.to_string(),
        "tlsCAFile" => set.tls_ca_file = value.to_string(),
        "tlsCertFile" => set.tls_cert_file = value.to_string(),
        "tlsKeyFile" => set.tls_key_file = value.to_string(),
        "tlsServerName" => set.tls_server_name = value.to_string(),
        _ => return Err(unknown_flag_error(name)),
    }
    Ok(())
}

fn unknown_flag_error(name: &str) -> FlagError {
    FlagError::Invalid(format!(
        "flag provided but not defined: -{name}\n{}",
        usage()
    ))
}

/// Parses a value-less/inline boolean flag: `None` (value-less) is `true`;
/// `Some(v)` parses `v` as a Go bool.
fn parse_optional_bool(inline_value: &Option<String>, name: &str) -> Result<bool, FlagError> {
    match inline_value {
        None => Ok(true),
        Some(v) => parse_bool(v)
            .ok_or_else(|| FlagError::Invalid(format!("invalid boolean value {v:?} for -{name}"))),
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

/// Parses a duration flag via [`esm_metricsql::duration_value`] (the same
/// grammar `crate::config::types::deserialize_opt_duration` uses for YAML
/// duration fields), rejecting negatives -- a negative duration is
/// meaningless for every duration flag defined here.
fn parse_duration_flag(value: &str, name: &str) -> Result<Duration, FlagError> {
    let ms = esm_metricsql::duration_value(value, 0).map_err(|e| {
        FlagError::Invalid(format!("invalid value {value:?} for flag -{name}: {e}"))
    })?;
    if ms < 0 {
        return Err(FlagError::Invalid(format!(
            "invalid value {value:?} for flag -{name}: duration must be non-negative"
        )));
    }
    Ok(Duration::from_millis(ms as u64))
}

fn parse_usize_flag(value: &str, name: &str) -> Result<usize, FlagError> {
    value
        .parse()
        .map_err(|_| FlagError::Invalid(format!("invalid value {value:?} for flag -{name}")))
}

/// Empty string means "unset" (Go's `flag.String` zero value convention).
fn non_empty_owned(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_core_flags() {
        let f = parse_flags(
            &[
                "-datasource.url=http://vm:8428",
                "-rule=/etc/alerts/*.yml",
                "-evaluationInterval=30s",
                "-httpListenAddr=:8880",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(f.datasource_url, "http://vm:8428");
        assert_eq!(f.rule_globs, vec!["/etc/alerts/*.yml".to_string()]);
        assert_eq!(f.evaluation_interval, Duration::from_secs(30));
    }

    #[test]
    fn defaults_are_upstream_compatible() {
        let f = parse_flags(&args(&["-rule=x", "-datasource.url=http://vm"])).unwrap();
        assert_eq!(f.http_listen_addr, ":8880");
        assert_eq!(f.evaluation_interval, Duration::from_secs(60));
        assert_eq!(f.remote_read_lookback, Duration::from_secs(3600));
        assert_eq!(f.remote_write_flush_interval, Duration::from_secs(5));
        assert_eq!(f.remote_write_max_batch_size, 1000);
        assert_eq!(f.remote_write_max_queue_size, 100_000);
        assert_eq!(f.remote_write_concurrency, 1);
        assert_eq!(f.config_check_interval, Duration::ZERO);
        assert_eq!(f.group_max_start_delay, Duration::ZERO);
        assert_eq!(f.http_read_timeout, Duration::from_secs(30));
        assert!(!f.dry_run);
        assert!(!f.disable_alertgroup_label);
        assert_eq!(f.remote_write_url, None);
        assert_eq!(f.remote_read_url, None);
        assert!(f.notifier_urls.is_empty());
    }

    #[test]
    fn rule_and_notifier_url_are_repeatable() {
        let f = parse_flags(&args(&[
            "-rule=/a/*.yml",
            "-rule",
            "/b/*.yml",
            "-datasource.url=http://vm",
            "-notifier.url=http://am1:9093",
            "-notifier.url=http://am2:9093",
        ]))
        .unwrap();
        assert_eq!(f.rule_globs, vec!["/a/*.yml", "/b/*.yml"]);
        assert_eq!(f.notifier_urls, vec!["http://am1:9093", "http://am2:9093"]);
    }

    #[test]
    fn accepts_all_flag_syntaxes() {
        for a in [
            &["-rule=x", "-httpListenAddr=127.0.0.1:9999"][..],
            &["-rule=x", "--httpListenAddr=127.0.0.1:9999"][..],
            &["-rule=x", "-httpListenAddr", "127.0.0.1:9999"][..],
            &["-rule=x", "--httpListenAddr", "127.0.0.1:9999"][..],
        ] {
            let f = parse_flags(&args(a)).unwrap();
            assert_eq!(f.http_listen_addr, "127.0.0.1:9999", "args: {a:?}");
        }
    }

    #[test]
    fn parses_datasource_auth_and_tls_flags() {
        let f = parse_flags(&args(&[
            "-rule=x",
            "-datasource.url=http://vm",
            "-datasource.basicAuth.username=alice",
            "-datasource.basicAuth.password=s3cr3t",
            "-datasource.bearerToken=tok",
            "-datasource.tlsCAFile=/ca.pem",
            "-datasource.tlsCertFile=/cert.pem",
            "-datasource.tlsKeyFile=/key.pem",
            "-datasource.tlsServerName=vm.internal",
            "-datasource.tlsInsecureSkipVerify",
        ]))
        .unwrap();
        assert_eq!(f.datasource_auth.username, "alice");
        assert_eq!(f.datasource_auth.password, "s3cr3t");
        assert_eq!(f.datasource_auth.bearer_token, "tok");
        assert_eq!(f.datasource_auth.tls_ca_file, "/ca.pem");
        assert_eq!(f.datasource_auth.tls_cert_file, "/cert.pem");
        assert_eq!(f.datasource_auth.tls_key_file, "/key.pem");
        assert_eq!(f.datasource_auth.tls_server_name, "vm.internal");
        assert!(f.datasource_auth.tls_insecure_skip_verify);
    }

    #[test]
    fn parses_remote_write_read_and_notifier_component_flags() {
        let f = parse_flags(&args(&[
            "-rule=x",
            "-datasource.url=http://vm",
            "-remoteWrite.url=http://vm:8428",
            "-remoteWrite.flushInterval=2s",
            "-remoteWrite.maxBatchSize=500",
            "-remoteWrite.maxQueueSize=2000",
            "-remoteWrite.concurrency=4",
            "-remoteWrite.basicAuth.username=rw-user",
            "-remoteRead.url=http://vm:8428",
            "-remoteRead.lookback=2h",
            "-remoteRead.bearerToken=rr-tok",
            "-notifier.url=http://am:9093",
            "-notifier.tlsInsecureSkipVerify=true",
        ]))
        .unwrap();
        assert_eq!(f.remote_write_url.as_deref(), Some("http://vm:8428"));
        assert_eq!(f.remote_write_flush_interval, Duration::from_secs(2));
        assert_eq!(f.remote_write_max_batch_size, 500);
        assert_eq!(f.remote_write_max_queue_size, 2000);
        assert_eq!(f.remote_write_concurrency, 4);
        assert_eq!(f.remote_write_auth.username, "rw-user");
        assert_eq!(f.remote_read_url.as_deref(), Some("http://vm:8428"));
        assert_eq!(f.remote_read_lookback, Duration::from_secs(7200));
        assert_eq!(f.remote_read_auth.bearer_token, "rr-tok");
        assert!(f.notifier_auth.tls_insecure_skip_verify);
    }

    #[test]
    fn parses_web_gating_and_dry_run_flags() {
        let f = parse_flags(&args(&[
            "-rule=x",
            "-datasource.url=http://vm",
            "-reload.authKey=rsecret",
            "-metrics.authKey=msecret",
            "-httpReadTimeout=15s",
            "-dryRun",
            "-disableAlertgroupLabel=true",
        ]))
        .unwrap();
        assert_eq!(f.reload_auth_key, "rsecret");
        assert_eq!(f.metrics_auth_key, "msecret");
        assert_eq!(f.http_read_timeout, Duration::from_secs(15));
        assert!(f.dry_run);
        assert!(f.disable_alertgroup_label);
    }

    #[test]
    fn version_flag_is_boolean() {
        assert_eq!(parse_flags(&args(&["-version"])), Err(FlagError::Version));
        assert_eq!(parse_flags(&args(&["--version"])), Err(FlagError::Version));
        assert_eq!(
            parse_flags(&args(&["-version=true"])),
            Err(FlagError::Version)
        );
        // `-version=false` is a no-op; parsing otherwise succeeds.
        assert!(parse_flags(&args(&["-version=false"])).is_ok());
        assert!(matches!(
            parse_flags(&args(&["-version=maybe"])),
            Err(FlagError::Invalid(_))
        ));
    }

    #[test]
    fn help_flag_variants() {
        for a in [&["-help"][..], &["--help"][..], &["-h"][..]] {
            assert_eq!(parse_flags(&args(a)), Err(FlagError::Help), "args: {a:?}");
        }
    }

    #[test]
    fn unknown_flag_is_an_error_with_usage() {
        let err = parse_flags(&args(&["-bogus"])).unwrap_err();
        match err {
            FlagError::Invalid(msg) => {
                assert!(
                    msg.contains("flag provided but not defined: -bogus"),
                    "{msg}"
                );
                assert!(msg.contains("Usage of esmalert"), "{msg}");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }

        let err = parse_flags(&args(&["-datasource.bogusSuffix=x"])).unwrap_err();
        assert!(matches!(err, FlagError::Invalid(_)));
    }

    #[test]
    fn missing_value_is_an_error() {
        let err = parse_flags(&args(&["-httpListenAddr"])).unwrap_err();
        match err {
            FlagError::Invalid(msg) => {
                assert!(
                    msg.contains("missing value for flag -httpListenAddr"),
                    "{msg}"
                )
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn invalid_numeric_and_duration_values_are_errors() {
        assert!(matches!(
            parse_flags(&args(&["-remoteWrite.maxBatchSize=abc"])),
            Err(FlagError::Invalid(_))
        ));
        assert!(matches!(
            parse_flags(&args(&["-evaluationInterval=notaduration"])),
            Err(FlagError::Invalid(_))
        ));
        assert!(matches!(
            parse_flags(&args(&["-configCheckInterval=-5s"])),
            Err(FlagError::Invalid(_))
        ));
    }

    #[test]
    fn positional_argument_is_an_error() {
        assert!(parse_flags(&args(&["serve"])).is_err());
    }
}
