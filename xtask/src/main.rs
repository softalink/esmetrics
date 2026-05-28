//! `xtask` — workspace tooling for EsMetrics.
//!
//! Run via `cargo xtask <subcommand>`. The `[alias]` in `.cargo/config.toml`
//! routes the call through this binary.

// xtask is a CLI tool whose entire purpose is to talk to the developer over
// stdio; the workspace's anti-`print_*` lints don't apply here.
#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]
// `not_yet` returns Result so it can sit in the same match arms as the real
// commands; clippy wants us to drop the unused wrapper, but uniform return
// types are more valuable.
#![allow(clippy::unnecessary_wraps)]

use std::process::{Command, ExitCode, Stdio};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "xtask", about = "Workspace tooling for EsMetrics", version)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Format all workspace code.
    Fmt {
        /// Check formatting without making changes.
        #[arg(long)]
        check: bool,
    },
    /// Lint the workspace with clippy, denying warnings.
    Lint,
    /// Run the workspace test suite.
    Test {
        /// Test only this package.
        #[arg(short, long)]
        package: Option<String>,
    },
    /// Run benchmarks (full implementation lands in Phase 0.5).
    Bench,
    /// Performance tooling.
    Perf {
        #[command(subcommand)]
        cmd: PerfCmd,
    },
    /// Conformance harness fixture management.
    Fixtures {
        #[command(subcommand)]
        cmd: FixturesCmd,
    },
    /// Drive a sustained ingest + query workload against a running esm-single.
    /// Used for the H11 30-day soak test; smaller durations also serve as a
    /// crash-frequency check.
    Soak {
        /// Target esm-single HTTP URL.
        #[arg(long, default_value = "http://127.0.0.1:8428")]
        url: String,
        /// Duration in seconds (default 60 = quick smoke; production soak
        /// targets 30 × 86400 = 2_592_000).
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
        /// Series cardinality.
        #[arg(long, default_value_t = 1_000)]
        series: u64,
        /// Sample-write rate per second.
        #[arg(long, default_value_t = 10_000)]
        writes_per_sec: u64,
        /// Query rate per second.
        #[arg(long, default_value_t = 10)]
        queries_per_sec: u64,
    },
}

#[derive(Subcommand, Debug)]
enum PerfCmd {
    /// Record a perf profile of a workload.
    Record {
        /// Workload name to record.
        workload: String,
    },
    /// Compare benchmark results against the rolling baseline or against upstream VM.
    Compare {
        /// What to compare against: `baseline` or `vm`.
        #[arg(long, default_value = "baseline")]
        against: String,
    },
}

#[derive(Subcommand, Debug)]
enum FixturesCmd {
    /// Regenerate fixtures from upstream VictoriaMetrics at the pinned tag.
    Regenerate {
        /// Regenerate every fixture, not just stale ones.
        #[arg(long)]
        all: bool,
        /// Only regenerate the named scenario.
        #[arg(long)]
        scenario: Option<String>,
    },
    /// Push the local fixture cache to a shared object store.
    Push {
        /// Object-store target URI (e.g. `s3://bucket/path`).
        target: String,
    },
    /// Pull fixtures from a shared object store into the local cache.
    Pull {
        /// Object-store source URI.
        source: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("xtask: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Cmd::Fmt { check } => fmt(check),
        Cmd::Lint => lint(),
        Cmd::Test { package } => test(package.as_deref()),
        Cmd::Bench => bench(),
        Cmd::Perf { cmd } => match cmd {
            PerfCmd::Record { workload } => perf_record(&workload),
            PerfCmd::Compare { against } => perf_compare(&against),
        },
        Cmd::Soak { url, duration_secs, series, writes_per_sec, queries_per_sec } => {
            soak(&url, duration_secs, series, writes_per_sec, queries_per_sec)
        }
        Cmd::Fixtures { cmd } => match cmd {
            FixturesCmd::Regenerate { all, scenario } => {
                not_yet(&format!("fixtures regenerate (all={all}, scenario={scenario:?})"))
            }
            FixturesCmd::Push { target } => not_yet(&format!("fixtures push {target}")),
            FixturesCmd::Pull { source } => not_yet(&format!("fixtures pull {source}")),
        },
    }
}

fn fmt(check: bool) -> Result<()> {
    let mut cmd = cargo();
    cmd.args(["fmt", "--all"]);
    if check {
        cmd.arg("--check");
    }
    run(cmd)
}

fn lint() -> Result<()> {
    let mut cmd = cargo();
    cmd.args(["clippy", "--workspace", "--all-targets", "--", "-D", "warnings"]);
    run(cmd)
}

fn perf_record(workload: &str) -> Result<()> {
    // Save the current bench results under the named baseline using
    // criterion's --save-baseline. Subsequent runs of perf_compare can then
    // diff against this snapshot.
    let mut cmd = cargo();
    cmd.args(["bench", "--workspace", "--", "--save-baseline", workload]);
    run(cmd)
}

fn perf_compare(against: &str) -> Result<()> {
    if against == "vm" {
        // Head-to-head against upstream VictoriaMetrics. Wired through the
        // conformance harness, which knows how to spin up both sides.
        eprintln!("xtask perf compare --against vm: this delegates to the conformance harness.");
        eprintln!("Run:  cargo run --release --bin conformance-harness -- run");
        return Ok(());
    }
    let mut cmd = cargo();
    cmd.args(["bench", "--workspace", "--", "--baseline", against]);
    run(cmd)
}

fn bench() -> Result<()> {
    // Run the workspace's criterion benches. Today this is
    // `esm-storage::ingest_query` and `esm-promql::promql_eval`; new bench
    // crates pick up automatically because cargo bench discovers them.
    let mut cmd = cargo();
    cmd.args(["bench", "--workspace"]);
    run(cmd)
}

fn test(package: Option<&str>) -> Result<()> {
    let mut cmd = cargo();
    cmd.args(["test", "--workspace"]);
    if let Some(p) = package {
        cmd.args(["--package", p]);
    }
    run(cmd)
}

fn not_yet(what: &str) -> Result<()> {
    eprintln!("xtask: {what}: not yet implemented (stub)");
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_precision_loss)]
fn soak(
    url: &str,
    duration_secs: u64,
    series: u64,
    writes_per_sec: u64,
    queries_per_sec: u64,
) -> Result<()> {
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    let authority = url
        .trim_start_matches("http://")
        .split('/')
        .next()
        .ok_or_else(|| anyhow::anyhow!("bad URL {url:?}"))?;

    // Pre-build per-series metric names so we don't allocate on the hot path.
    let names: Vec<String> =
        (0..series).map(|i| format!("esm_soak_total{{inst=\"{i}\"}}")).collect();

    let started = Instant::now();
    let deadline = started + Duration::from_secs(duration_secs);
    let writes_per_tick = writes_per_sec.div_ceil(10).max(1); // 10 ticks/sec
    let tick = Duration::from_millis(100);

    let mut writes_sent = 0_u64;
    let mut queries_sent = 0_u64;
    let mut write_errors = 0_u64;
    let mut query_errors = 0_u64;
    let mut value = 0_i64;
    let mut series_cursor = 0_u64;

    eprintln!(
        "soak: target={url} duration={duration_secs}s series={series} writes/s={writes_per_sec} queries/s={queries_per_sec}"
    );

    let queries_each_sec_left = std::cell::Cell::new(queries_per_sec);
    let last_query_second = std::cell::Cell::new(started);

    while Instant::now() < deadline {
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;

        // Build a text-exposition body with writes_per_tick samples.
        let mut body = String::with_capacity(writes_per_tick as usize * 40);
        for _ in 0..writes_per_tick {
            value = value.wrapping_add(1);
            let idx = (series_cursor % series.max(1)) as usize;
            series_cursor = series_cursor.wrapping_add(1);
            body.push_str(&names[idx]);
            body.push(' ');
            body.push_str(&value.to_string());
            body.push(' ');
            body.push_str(&now_ms.to_string());
            body.push('\n');
        }

        if http_post_form(authority, "/api/v1/import/prometheus", &body).is_ok() {
            writes_sent += writes_per_tick;
        } else {
            write_errors += 1;
        }

        // Queries paced at queries_per_sec across the tick boundary.
        if last_query_second.get().elapsed() >= Duration::from_secs(1) {
            queries_each_sec_left.set(queries_per_sec);
            last_query_second.set(Instant::now());
        }
        let qs_to_send = std::cmp::min(queries_each_sec_left.get(), queries_per_sec.div_ceil(10));
        for _ in 0..qs_to_send {
            let q = format!("/api/v1/query?query=esm_soak_total&time={}", now_ms / 1000);
            if http_get(authority, &q).is_ok() {
                queries_sent += 1;
            } else {
                query_errors += 1;
            }
        }
        queries_each_sec_left.set(queries_each_sec_left.get().saturating_sub(qs_to_send));

        std::thread::sleep(tick);
    }

    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "soak complete in {:.1}s: writes={} ({:.0}/s, {} err) queries={} ({:.0}/s, {} err)",
        elapsed,
        writes_sent,
        writes_sent as f64 / elapsed.max(0.001),
        write_errors,
        queries_sent,
        queries_sent as f64 / elapsed.max(0.001),
        query_errors,
    );

    if write_errors > 0 || query_errors > 0 {
        bail!("{write_errors} write errors, {query_errors} query errors");
    }
    Ok(())
}

fn http_post_form(authority: &str, path: &str, body: &str) -> Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;
    let mut stream =
        TcpStream::connect(authority).with_context(|| format!("connect {authority}"))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let s = String::from_utf8_lossy(&buf);
    let line = s.lines().next().unwrap_or("");
    if !(line.contains(" 200 ") || line.contains(" 204 ")) {
        bail!("non-2xx: {line}");
    }
    Ok(())
}

fn http_get(authority: &str, path: &str) -> Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;
    let mut stream =
        TcpStream::connect(authority).with_context(|| format!("connect {authority}"))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let s = String::from_utf8_lossy(&buf);
    let line = s.lines().next().unwrap_or("");
    if !line.contains(" 200 ") {
        bail!("non-200: {line}");
    }
    Ok(())
}

fn cargo() -> Command {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    Command::new(cargo)
}

fn run(mut cmd: Command) -> Result<()> {
    let pretty = format!("{cmd:?}");
    let status = cmd
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("failed to spawn {pretty}"))?;
    if !status.success() {
        bail!("{pretty} exited with {status}");
    }
    Ok(())
}
