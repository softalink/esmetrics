//! `esmctl` — the EsMetrics command-line migration tool. A Rust port of
//! `app/vmctl` covering the **vm-native** (VM↔EsMetrics native streaming),
//! **opentsdb** (OpenTSDB → import), **remote-read** (Prometheus remote-read →
//! import), and **influx** (InfluxDB → import) migration modes,
//! plus **verify-block** (validate a native export block file).
//!
//! Out of scope in this port (documented in the crate README): the
//! `prometheus`/`thanos`/`mimir` modes only — they read Prometheus TSDB
//! **blocks** off disk (index symbol table / postings / series records +
//! chunk segment files, plus object storage and downsampled chunks for
//! thanos/mimir), which is a large storage-format reader orthogonal to the
//! HTTP-based paths here. Prometheus/Thanos/Mimir data migrates via
//! `remote-read` instead.

mod auth;
mod backoff;
mod chunkenc;
mod civil;
mod flags;
mod importer;
mod influx;
mod matchfilter;
mod native;
mod opentsdb;
mod proto;
mod remoteread;
mod signal;
mod stepper;
mod timeparse;
mod transport;
mod verify;
mod vmnative;

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use auth::AuthConfig;
use backoff::Backoff;
use flags::Flags;
use importer::{Importer, ImporterConfig};
use native::NativeClient;
use opentsdb::{Config as OtsdbConfig, OtsdbClient};
use vmnative::VmNativeConfig;

/// Prompts on stdin for confirmation, returning true when the user answers
/// `y`/`yes` (or when `assume_yes` is set). Ports `prompt`.
pub(crate) fn prompt(assume_yes: bool, question: &str) -> bool {
    if assume_yes {
        return true;
    }
    print!("{question} [y/N] ");
    let _ = std::io::stdout().flush();
    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        return false;
    }
    matches!(line.trim(), "y" | "Y" | "yes")
}

fn main() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .try_init();

    // Install the Ctrl-C handler so long migrations can be cancelled cleanly.
    signal::install();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("");
    let rest = if args.is_empty() { &[][..] } else { &args[1..] };

    let result = match cmd {
        "vm-native" => run_vm_native(rest),
        "opentsdb" => run_opentsdb(rest),
        "remote-read" => run_remote_read(rest),
        "influx" => run_influx(rest),
        "verify-block" => run_verify_block(rest),
        "" | "-h" | "--help" | "help" => {
            print_usage();
            return;
        }
        "prometheus" | "mimir" | "thanos" => Err(format!(
            "the {cmd:?} mode reads Prometheus TSDB blocks off disk (index + chunk \
             segment files) and is out of scope for esmctl; migrate that data via \
             `remote-read` instead. supported: vm-native, opentsdb, remote-read, \
             influx, verify-block"
        )),
        other => Err(format!(
            "unknown command {other:?}; supported: vm-native, opentsdb, remote-read, influx, verify-block"
        )),
    };

    if signal::is_cancelled() {
        eprintln!("- Execution cancelled");
    }
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    println!(
        "esmctl — EsMetrics command-line migration tool\n\n\
         USAGE:\n    esmctl vm-native [flags]     migrate between VM/EsMetrics installations\n\
         \x20   esmctl opentsdb [flags]      migrate from OpenTSDB\n\
         \x20   esmctl remote-read [flags]   migrate via Prometheus remote-read\n\
         \x20   esmctl influx [flags]        migrate from InfluxDB\n\
         \x20   esmctl verify-block <path>   validate a native export block file\n\n\
         vm-native key flags:\n\
         \x20 --vm-native-src-addr <url>            source endpoint (required)\n\
         \x20 --vm-native-dst-addr <url>            destination endpoint (required)\n\
         \x20 --vm-native-filter-match <selector>  series selector (default {{__name__!=\"\"}})\n\
         \x20 --vm-native-filter-time-start <ts>   start time (required; RFC3339/unix/relative)\n\
         \x20 --vm-native-filter-time-end <ts>     end time (default: now)\n\
         \x20 --vm-native-step-interval <step>     month|week|day|hour|minute (default month)\n\
         \x20 --vm-concurrency <n>                 import workers (default 2)\n\
         \x20 --vm-native-src-user/-src-password/-src-bearer-token/-src-headers\n\
         \x20 --vm-native-dst-user/-dst-password/-dst-bearer-token/-dst-headers\n\
         \x20 --vm-extra-label name=value          (repeatable) labels added on import\n\
         \x20 -s                                   silent: assume `yes` to prompts\n\n\
         opentsdb key flags:\n\
         \x20 --otsdb-addr <url>                   OpenTSDB endpoint (required)\n\
         \x20 --otsdb-retentions <spec>            (repeatable, required) e.g. sum-1m-avg:1h:30d\n\
         \x20 --otsdb-filters <prefix>             (repeatable) metric-name prefixes (default a..z)\n\
         \x20 --otsdb-query-limit/-offset-days/-hard-ts-start/-normalize/-msecstime\n\
         \x20 --vm-addr <url>                      destination import endpoint (required)\n\
         \x20 --vm-user/-password/-bearer-token/-headers, --vm-concurrency/-batch-size\n\
         \x20 --vm-significant-figures/-round-digits, --vm-extra-label, -s\n"
    );
}

fn run_vm_native(args: &[String]) -> Result<(), String> {
    let f = Flags::parse(args)?;

    let src_addr = f
        .require("vm-native-src-addr")?
        .trim_end_matches('/')
        .to_string();
    let dst_addr = f
        .require("vm-native-dst-addr")?
        .trim_end_matches('/')
        .to_string();

    let src_auth = AuthConfig::new(
        f.get("vm-native-src-user"),
        f.get("vm-native-src-password"),
        f.get("vm-native-src-bearer-token"),
        f.get("vm-native-src-headers"),
    )?;
    let dst_auth = AuthConfig::new(
        f.get("vm-native-dst-user"),
        f.get("vm-native-dst-password"),
        f.get("vm-native-dst-bearer-token"),
        f.get("vm-native-dst-headers"),
    )?;

    // Native exports can stream for a long time, so no request timeout.
    let src = Arc::new(NativeClient {
        addr: src_addr,
        auth: src_auth,
        extra_labels: Vec::new(),
        http: transport::build_client(&tls_files(&f, "vm-native-src", false), None)?,
    });
    let dst = Arc::new(NativeClient {
        addr: dst_addr,
        auth: dst_auth,
        extra_labels: f.get_all("vm-extra-label"),
        http: transport::build_client(&tls_files(&f, "vm-native-dst", false), None)?,
    });

    let bf = Backoff::new(
        f.int("vm-native-backoff-retries", 10)?,
        f.float("vm-native-backoff-factor", 1.8)?,
        f.duration("vm-native-backoff-min-duration", Duration::from_secs(2))?,
    )?;

    let cfg = VmNativeConfig {
        src,
        dst,
        filter_match: f
            .get_or("vm-native-filter-match", "{__name__!=\"\"}")
            .to_string(),
        time_start: f.get("vm-native-filter-time-start").to_string(),
        time_end: f.get("vm-native-filter-time-end").to_string(),
        chunk: f.get_or("vm-native-step-interval", "month").to_string(),
        time_reverse: f.bool("vm-native-filter-time-reverse"),
        concurrency: f.int("vm-concurrency", 2)?.max(1) as usize,
        is_native: !f.bool("vm-native-disable-binary-protocol"),
        disable_per_metric: f.bool("vm-native-disable-per-metric-migration"),
        inter_cluster: f.bool("vm-intercluster"),
        backoff: Arc::new(bf),
        rate_limit: f.int("vm-rate-limit", 0)?,
        assume_yes: f.bool("s") || f.bool("disable-progress-bar"),
    };

    // The cancel flag (flipped by the Ctrl-C handler) aborts in-flight backoff
    // waits and stops the native workers between requests.
    vmnative::run(&cfg, signal::cancel_flag())
}

fn run_opentsdb(args: &[String]) -> Result<(), String> {
    let f = Flags::parse(args)?;

    let http = transport::build_client(&tls_files(&f, "otsdb", true), None)?;
    let otsdb_cfg = OtsdbConfig {
        addr: f.require("otsdb-addr")?.to_string(),
        limit: f.int("otsdb-query-limit", 100_000_000)?,
        offset_days: f.int("otsdb-offset-days", 0)?,
        hard_ts: f.int("otsdb-hard-ts-start", 0)?,
        retentions: {
            let r = f.get_all("otsdb-retentions");
            if r.is_empty() {
                return Err("required flag --otsdb-retentions is missing".to_string());
            }
            r
        },
        filters: {
            let filters = f.get_all("otsdb-filters");
            if filters.is_empty() {
                // Upstream default: single-letter prefixes a..z.
                ('a'..='z').map(|c| c.to_string()).collect()
            } else {
                filters
            }
        },
        normalize: f.bool("otsdb-normalize"),
        msecs_time: f.bool("otsdb-msecstime"),
    };
    let client = OtsdbClient::new(otsdb_cfg, http)?;

    let importer = build_importer(&f)?;
    let concurrency = f.int("otsdb-concurrency", 1)?.max(1) as usize;
    opentsdb::run(&client, importer, concurrency, f.bool("s"))
}

fn run_remote_read(args: &[String]) -> Result<(), String> {
    let f = Flags::parse(args)?;

    let addr = f
        .require("remote-read-src-addr")?
        .trim_end_matches('/')
        .to_string();

    // Matchers: default to __name__=~.* when none are provided.
    let mut names = f.get_all("remote-read-filter-label");
    let mut values = f.get_all("remote-read-filter-label-value");
    if names.is_empty() && values.is_empty() {
        names = vec!["__name__".to_string()];
        values = vec![".*".to_string()];
    }
    if names.len() != values.len() {
        return Err("the number of --remote-read-filter-label and --remote-read-filter-label-value must be equal".to_string());
    }
    let matchers = names
        .into_iter()
        .zip(values)
        .map(|(name, value)| remoteread::Matcher { name, value })
        .collect();

    let timeout = f.duration("remote-read-http-timeout", Duration::from_secs(300))?;
    let http = transport::build_client(&tls_files(&f, "remote-read", true), Some(timeout))?;

    let client = Arc::new(remoteread::RemoteReadClient {
        addr,
        disable_path_append: f.bool("remote-read-disable-path-append"),
        user: f.get("remote-read-user").to_string(),
        password: f.get("remote-read-password").to_string(),
        headers: parse_headers(f.get("remote-read-headers"))?,
        matchers,
        use_stream: f.bool("remote-read-use-stream"),
        http,
    });

    let importer = build_importer(&f)?;
    let cfg = remoteread::RemoteReadConfig {
        client,
        time_start: f.require("remote-read-filter-time-start")?.to_string(),
        time_end: f.get("remote-read-filter-time-end").to_string(),
        chunk: f.require("remote-read-step-interval")?.to_string(),
        time_reverse: f.bool("remote-read-filter-time-reverse"),
        concurrency: f.int("remote-read-concurrency", 1)?.max(1) as usize,
        assume_yes: f.bool("s"),
    };
    remoteread::run(&cfg, importer)
}

fn run_influx(args: &[String]) -> Result<(), String> {
    let f = Flags::parse(args)?;

    let http = transport::build_client(&tls_files(&f, "influx", true), None)?;

    let client = influx::InfluxClient::new(influx::Config {
        addr: f.get_or("influx-addr", "http://localhost:8086").to_string(),
        user: f.get("influx-user").to_string(),
        password: f.get("influx-password").to_string(),
        database: f.require("influx-database")?.to_string(),
        retention: f.get_or("influx-retention-policy", "autogen").to_string(),
        filter_series: f.get("influx-filter-series").to_string(),
        filter_time_start: f.get("influx-filter-time-start").to_string(),
        filter_time_end: f.get("influx-filter-time-end").to_string(),
        http,
    })?;

    let importer = build_importer(&f)?;
    let cfg = influx::InfluxProcessorConfig {
        client: Arc::new(client),
        concurrency: f.int("influx-concurrency", 1)?.max(1) as usize,
        separator: f
            .get_or("influx-measurement-field-separator", "_")
            .to_string(),
        skip_db_label: f.bool("influx-skip-database-label"),
        prom_mode: f.bool("influx-prometheus-mode"),
        assume_yes: f.bool("s"),
    };
    influx::run(&cfg, importer)
}

fn run_verify_block(args: &[String]) -> Result<(), String> {
    // verify-block takes a positional block path plus an optional --gunzip.
    let mut gunzip = false;
    let mut path: Option<String> = None;
    for a in args {
        match a.as_str() {
            "--gunzip" | "-gunzip" | "--gunzip=true" => gunzip = true,
            "--gunzip=false" => gunzip = false,
            s if s.starts_with('-') => return Err(format!("unknown flag {s:?}")),
            s => {
                if path.is_some() {
                    return Err("multiple block paths provided".to_string());
                }
                path = Some(s.to_string());
            }
        }
    }
    let path = path.ok_or("you must provide path for exported data block")?;
    verify::run(&path, gunzip)
}

/// Parses a `^^`-separated `Key: Value` header list.
fn parse_headers(headers: &str) -> Result<Vec<(String, String)>, String> {
    if headers.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for h in headers.split("^^") {
        let (k, v) = h
            .split_once(':')
            .ok_or_else(|| format!("missing ':' in header {h:?}; expecting \"key: value\""))?;
        out.push((k.trim().to_string(), v.trim().to_string()));
    }
    Ok(out)
}

/// Builds the shared destination importer from the `vm-*` flags. Ports
/// `initConfigVM` + `vm.NewImporter`.
fn build_importer(f: &Flags) -> Result<Importer, String> {
    let addr = f.require("vm-addr")?.to_string();
    let auth = AuthConfig::new(
        f.get("vm-user"),
        f.get("vm-password"),
        f.get("vm-bearer-token"),
        f.get("vm-headers"),
    )?;
    let bf = Backoff::new(
        f.int("vm-backoff-retries", 10)?,
        f.float("vm-backoff-factor", 1.8)?,
        f.duration("vm-backoff-min-duration", Duration::from_secs(2))?,
    )?;
    Importer::new(ImporterConfig {
        addr,
        auth,
        http: transport::build_client(&tls_files(f, "vm", true), None)?,
        concurrency: f.int("vm-concurrency", 2)?.max(1) as usize,
        batch_size: f.int("vm-batch-size", 200_000)?.max(1) as usize,
        significant_figures: f.int("vm-significant-figures", 0)? as i32,
        round_digits: f.int("vm-round-digits", 100)? as i32,
        extra_labels: f.get_all("vm-extra-label"),
        backoff: Arc::new(bf),
        compress: f.bool("vm-compress"),
        rate_limit: f.int("vm-rate-limit", 0)?,
    })
}

/// Reads the TLS flags for a mode prefix. `ca_upper` selects the upstream's
/// `-CA-file` (uppercase) vs `-ca-file` casing, which differs by mode.
fn tls_files(f: &Flags, prefix: &str, ca_upper: bool) -> transport::TlsFiles {
    let ca_name = if ca_upper {
        format!("{prefix}-CA-file")
    } else {
        format!("{prefix}-ca-file")
    };
    transport::TlsFiles {
        cert_file: f.get(&format!("{prefix}-cert-file")).to_string(),
        key_file: f.get(&format!("{prefix}-key-file")).to_string(),
        ca_file: f.get(&ca_name).to_string(),
        server_name: f.get(&format!("{prefix}-server-name")).to_string(),
        insecure_skip_verify: f.bool(&format!("{prefix}-insecure-skip-verify")),
    }
}
