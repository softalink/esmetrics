//! Conformance harness.
//!
//! Drives upstream VictoriaMetrics v1.144.0 and EsMetrics side-by-side
//! against a scenario YAML and diffs the outputs. See PLAN.md §8.
//!
//! Phase 0.6 ships the **skeleton**: scenario YAML parsing, scenario
//! enumeration, and a dry-run mode. The actual Docker orchestration + HTTP
//! drivers + diff engine land alongside Phase 1 (storage engine) when the
//! `esm-single` binary first becomes capable of round-tripping data.

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

/// CLI entry point.
#[derive(Parser, Debug)]
#[command(
    name = "conformance-harness",
    about = "Drives upstream VictoriaMetrics + EsMetrics and diffs the outputs.",
    version
)]
struct Cli {
    /// Path to the `conformance/scenarios/` directory.
    #[arg(long, default_value = "conformance/scenarios")]
    scenarios_dir: PathBuf,

    /// Pinned upstream VictoriaMetrics tag (override only for local debugging).
    #[arg(long, default_value = "v1.144.0")]
    vm_tag: String,

    /// Path to the esm-single binary to use as the EsMetrics side. Defaults
    /// to `target/release/esm-single` under the current working directory.
    #[arg(long)]
    esm_bin: Option<PathBuf>,

    /// Path to the docker CLI. Defaults to `docker` on PATH.
    #[arg(long)]
    docker_bin: Option<String>,

    /// Scratch dir for scenario state (data dirs, logs). Defaults to a
    /// fresh subdirectory under the system temp dir.
    #[arg(long)]
    work_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// List all known scenarios.
    List,
    /// Validate scenario YAML structure without running anything.
    Check,
    /// Dry-run: enumerate what the harness *would* do for the named scenario.
    DryRun {
        /// Scenario name (file name without `.yaml`).
        scenario: String,
    },
    /// Run scenarios end-to-end (Phase 1+; currently surfaces a friendly error).
    Run {
        /// Scenario name (file name without `.yaml`). Omit to run all.
        scenario: Option<String>,
    },
}

/// On-disk scenario definition.
#[derive(Debug, Deserialize, Serialize)]
struct Scenario {
    /// Scenario name; should match the filename.
    name: String,
    /// One-line description.
    #[serde(default)]
    description: String,
    /// Ingest steps applied to both VM and EsMetrics before queries run.
    #[serde(default)]
    ingest: Vec<IngestStep>,
    /// Queries to run against both endpoints.
    #[serde(default)]
    queries: Vec<QueryStep>,
    /// Optional on-disk comparison block.
    #[serde(default)]
    on_disk: Option<OnDiskCompare>,
}

#[derive(Debug, Deserialize, Serialize)]
struct IngestStep {
    protocol: String,
    #[serde(default)]
    data: Option<PathBuf>,
    #[serde(default)]
    inline: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct QueryStep {
    path: String,
    #[serde(default)]
    params: serde_yaml_ng::Value,
    #[serde(default = "default_compare_kind")]
    compare: String,
    #[serde(default)]
    tolerance: serde_yaml_ng::Value,
}

fn default_compare_kind() -> String {
    "semantic_set".to_string()
}

#[derive(Debug, Deserialize, Serialize)]
struct OnDiskCompare {
    #[serde(default = "default_compare_after")]
    compare_after: String,
    #[serde(default)]
    paths: Vec<PathBuf>,
}

fn default_compare_after() -> String {
    "compact".to_string()
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("conformance-harness: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Cmd::List => list_scenarios(&cli.scenarios_dir),
        Cmd::Check => check_scenarios(&cli.scenarios_dir),
        Cmd::DryRun { scenario } => dry_run(&cli.scenarios_dir, &scenario, &cli.vm_tag),
        Cmd::Run { scenario } => run_scenarios(
            &cli.scenarios_dir,
            scenario.as_deref(),
            &cli.vm_tag,
            cli.esm_bin.as_deref(),
            cli.docker_bin.as_deref().unwrap_or("docker"),
            cli.work_dir.as_deref(),
        ),
    }
}

fn run_scenarios(
    dir: &Path,
    name: Option<&str>,
    vm_tag: &str,
    esm_bin: Option<&Path>,
    docker_bin: &str,
    work_dir: Option<&Path>,
) -> Result<()> {
    let scenarios = if let Some(n) = name { vec![load_one(dir, n)?] } else { load_all(dir)? };
    let esm_bin = esm_bin
        .map(std::path::Path::to_path_buf)
        .or_else(|| std::env::current_dir().ok().map(|d| d.join("target/release/esm-single")))
        .context("locate esm-single binary; pass --esm-bin")?;
    if !esm_bin.exists() {
        bail!(
            "esm-single binary not found at {} — build it with `cargo build --release --package esm-single`",
            esm_bin.display()
        );
    }
    let work_dir = work_dir.map_or_else(
        || std::env::temp_dir().join(format!("esm-conformance-{}", std::process::id())),
        std::path::Path::to_path_buf,
    );
    std::fs::create_dir_all(&work_dir).context("create work dir")?;
    let mut failures = 0_usize;
    for sc in &scenarios {
        println!("== scenario: {} ==", sc.name);
        match run_one(sc, vm_tag, &esm_bin, docker_bin, &work_dir) {
            Ok(()) => println!("  PASS"),
            Err(e) => {
                println!("  FAIL: {e:#}");
                failures += 1;
            }
        }
    }
    println!("== {} passed / {} failed ==", scenarios.len() - failures, failures);
    if failures > 0 {
        bail!("{failures} scenario(s) failed");
    }
    Ok(())
}

fn run_one(
    sc: &Scenario,
    vm_tag: &str,
    esm_bin: &Path,
    docker_bin: &str,
    work_dir: &Path,
) -> Result<()> {
    let scenario_workdir = work_dir.join(&sc.name);
    let _ = std::fs::remove_dir_all(&scenario_workdir);
    std::fs::create_dir_all(&scenario_workdir)?;
    let vm_port = free_port()?;
    let esm_port = free_port()?;

    let vm_container_name = format!("esm-conformance-vm-{}-{}", sc.name, std::process::id());
    let mut vm = VmContainer::start(docker_bin, vm_tag, &vm_container_name, vm_port)?;
    let mut esm = EsmProcess::start(esm_bin, &scenario_workdir.join("esm-data"), esm_port)?;
    wait_for_http(&format!("127.0.0.1:{vm_port}"), "/health", 30)?;
    wait_for_http(&format!("127.0.0.1:{esm_port}"), "/health", 30)?;

    // Replay ingest steps against both.
    for step in &sc.ingest {
        let body = if let Some(inline) = &step.inline {
            inline.clone()
        } else if let Some(path) = &step.data {
            std::fs::read_to_string(path)
                .with_context(|| format!("read ingest data {}", path.display()))?
        } else {
            bail!("ingest step has neither inline nor data");
        };
        let route = ingest_route_for(&step.protocol)?;
        post_or_warn(&format!("127.0.0.1:{vm_port}"), route, &body, "vm")?;
        post_or_warn(&format!("127.0.0.1:{esm_port}"), route, &body, "esm")?;
    }

    // Compare query responses.
    for q in &sc.queries {
        let qs = querystring(&q.params);
        let path_with_q = if qs.is_empty() { q.path.clone() } else { format!("{}?{}", q.path, qs) };
        let vm_body = http_get(&format!("127.0.0.1:{vm_port}"), &path_with_q)
            .with_context(|| format!("vm GET {path_with_q}"))?;
        let esm_body = http_get(&format!("127.0.0.1:{esm_port}"), &path_with_q)
            .with_context(|| format!("esm GET {path_with_q}"))?;
        compare_response(&q.path, &q.compare, &vm_body, &esm_body)?;
    }

    esm.stop();
    vm.stop();
    Ok(())
}

fn ingest_route_for(protocol: &str) -> Result<&'static str> {
    Ok(match protocol {
        "prom-text" | "prometheus" => "/api/v1/import/prometheus",
        "prom-remote-write" => "/api/v1/write",
        "influx-v1" => "/write",
        "influx-v2" => "/api/v2/write",
        "graphite" => "/api/v1/import/graphite",
        "opentsdb-telnet" => "/api/v1/import/opentsdb",
        "opentsdb-http" => "/api/put",
        "datadog" => "/api/v1/datadog/series",
        "json-line" => "/api/v1/import",
        "csv" => "/api/v1/import/csv",
        other => bail!("unknown ingest protocol {other:?}"),
    })
}

fn querystring(v: &serde_yaml_ng::Value) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    if let serde_yaml_ng::Value::Mapping(m) = v {
        for (i, (k, v)) in m.iter().enumerate() {
            let k = k.as_str().unwrap_or("");
            let v = match v {
                serde_yaml_ng::Value::String(s) => s.clone(),
                other => serde_yaml_ng::to_string(other).unwrap_or_default().trim().to_string(),
            };
            if i > 0 {
                out.push('&');
            }
            let _ = write!(out, "{}={}", url_encode(k), url_encode(&v));
        }
    }
    out
}

fn url_encode(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

fn compare_response(path: &str, compare: &str, vm_body: &str, esm_body: &str) -> Result<()> {
    match compare {
        "semantic_set" => semantic_set_eq(vm_body, esm_body)
            .with_context(|| format!("semantic_set comparison failed for {path}")),
        "exact_text" => {
            if vm_body == esm_body {
                Ok(())
            } else {
                bail!("exact_text mismatch for {path}\nvm:  {vm_body}\nesm: {esm_body}")
            }
        }
        other => bail!("unknown compare kind {other:?}"),
    }
}

fn semantic_set_eq(vm: &str, esm: &str) -> Result<()> {
    let vm_v: serde_json::Value = serde_json::from_str(vm).context("parse vm json")?;
    let esm_v: serde_json::Value = serde_json::from_str(esm).context("parse esm json")?;
    let vm_set = extract_result_set(&vm_v);
    let esm_set = extract_result_set(&esm_v);
    if vm_set == esm_set {
        Ok(())
    } else {
        bail!("result sets differ:\nvm:  {vm_set:?}\nesm: {esm_set:?}")
    }
}

fn extract_result_set(v: &serde_json::Value) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    if let Some(arr) = v.pointer("/data/result").and_then(|r| r.as_array()) {
        for elem in arr {
            // Use metric + last value (vector) or metric + values (matrix).
            let key = serde_json::to_string(elem).unwrap_or_default();
            out.insert(key);
        }
    }
    out
}

fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn wait_for_http(authority: &str, path: &str, timeout_secs: u64) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let mut last_err = None;
    while std::time::Instant::now() < deadline {
        match http_get(authority, path) {
            Ok(_) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    bail!("{authority}{path} did not become healthy within {timeout_secs}s: {last_err:?}")
}

fn http_get(authority: &str, path: &str) -> Result<String> {
    use std::io::{Read as _, Write as _};
    let mut stream =
        std::net::TcpStream::connect(authority).with_context(|| format!("connect {authority}"))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream.write_all(req.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let s = String::from_utf8_lossy(&raw).to_string();
    let body_start = s.find("\r\n\r\n").map_or(0, |i| i + 4);
    let header_block = &s[..body_start.saturating_sub(4)];
    let status_line = header_block.lines().next().unwrap_or("");
    if !status_line.contains(" 200 ") && !status_line.contains(" 204 ") {
        bail!("non-2xx response: {status_line}");
    }
    Ok(s[body_start..].to_string())
}

fn post_or_warn(authority: &str, path: &str, body: &str, who: &str) -> Result<()> {
    use std::io::{Read as _, Write as _};
    let mut stream =
        std::net::TcpStream::connect(authority).with_context(|| format!("connect {authority}"))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let s = String::from_utf8_lossy(&raw).to_string();
    let status_line = s.lines().next().unwrap_or("");
    if !status_line.contains(" 200 ") && !status_line.contains(" 204 ") {
        eprintln!("warning: {who} POST {path} -> {status_line}");
    }
    Ok(())
}

struct VmContainer {
    docker_bin: String,
    name: String,
}

impl VmContainer {
    fn start(docker_bin: &str, tag: &str, name: &str, port: u16) -> Result<Self> {
        let image = format!("victoriametrics/victoria-metrics:{tag}");
        let status = std::process::Command::new(docker_bin)
            .args(["run", "-d", "--rm", "--name", name, "-p", &format!("{port}:8428"), &image])
            .status()
            .with_context(|| format!("docker run {image}"))?;
        if !status.success() {
            bail!("docker run failed (exit={status:?})");
        }
        Ok(Self { docker_bin: docker_bin.to_string(), name: name.to_string() })
    }
    fn stop(&mut self) {
        let _ = std::process::Command::new(&self.docker_bin)
            .args(["stop", &self.name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

impl Drop for VmContainer {
    fn drop(&mut self) {
        self.stop();
    }
}

struct EsmProcess {
    child: std::process::Child,
}

impl EsmProcess {
    fn start(esm_bin: &Path, data_dir: &Path, port: u16) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let child = std::process::Command::new(esm_bin)
            .args([
                "--storage-data-path",
                data_dir.to_str().context("non-utf8 data dir")?,
                "--http-listen-addr",
                &format!("127.0.0.1:{port}"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("spawn esm-single")?;
        Ok(Self { child })
    }
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for EsmProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

fn list_scenarios(dir: &Path) -> Result<()> {
    for scenario in load_all(dir)? {
        println!("{name:24}  {desc}", name = scenario.name, desc = scenario.description);
    }
    Ok(())
}

fn check_scenarios(dir: &Path) -> Result<()> {
    let scenarios = load_all(dir)?;
    println!("{} scenarios parsed successfully", scenarios.len());
    Ok(())
}

fn dry_run(dir: &Path, name: &str, vm_tag: &str) -> Result<()> {
    let scenario = load_one(dir, name)?;
    println!("scenario: {} ({})", scenario.name, scenario.description);
    println!("upstream VM tag: {vm_tag}");
    println!("ingest steps: {}", scenario.ingest.len());
    for step in &scenario.ingest {
        let source = step.data.as_ref().map_or("<inline>", |p| p.to_str().unwrap_or("<non-utf8>"));
        println!("  - {protocol:20} <- {source}", protocol = step.protocol);
    }
    println!("queries: {}", scenario.queries.len());
    for q in &scenario.queries {
        println!("  - {path:32} (compare = {kind})", path = q.path, kind = q.compare);
    }
    if let Some(od) = &scenario.on_disk {
        println!(
            "on-disk: compare_after={after}, {n} paths",
            after = od.compare_after,
            n = od.paths.len()
        );
    }
    Ok(())
}

fn load_all(dir: &Path) -> Result<Vec<Scenario>> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let body =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let scenario: Scenario =
            serde_yaml_ng::from_str(&body).with_context(|| format!("parse {}", path.display()))?;
        out.push(scenario);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn load_one(dir: &Path, name: &str) -> Result<Scenario> {
    let path = dir.join(format!("{name}.yaml"));
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    serde_yaml_ng::from_str(&body).with_context(|| format!("parse {}", path.display()))
}
