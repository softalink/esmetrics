//! `esm-ctl` — migration / inspection / data-movement CLI.
//!
//! Drop-in candidate for VictoriaMetrics `vmctl` v1.144.0. The MVP today
//! supports `inspect` (data-dir summary) and `export` (JSON-line dump of
//! every sample). The native VM → EsMetrics migrator and the reverse
//! direction are scheduled for a later sub-phase per ADR-001.

#![allow(clippy::print_stdout)]
#![allow(clippy::cast_possible_truncation)]

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use esm_storage::{Storage, TimeRange};

#[derive(Parser, Debug)]
#[command(name = "esm-ctl", about = "EsMetrics data-movement CLI.", version)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Print a high-level summary of a data directory.
    Inspect {
        #[arg(long)]
        storage_data_path: PathBuf,
    },
    /// Dump every sample as JSON lines (one record per sample).
    Export {
        #[arg(long)]
        storage_data_path: PathBuf,
        /// Optional inclusive lower bound on sample timestamps (ms).
        #[arg(long)]
        from_ms: Option<i64>,
        /// Optional inclusive upper bound on sample timestamps (ms).
        #[arg(long)]
        to_ms: Option<i64>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Cmd::Inspect { storage_data_path } => inspect(&storage_data_path),
        Cmd::Export { storage_data_path, from_ms, to_ms } => {
            export(&storage_data_path, from_ms, to_ms)
        }
    }
}

fn inspect(path: &std::path::Path) -> Result<()> {
    let s = Storage::open(path).with_context(|| format!("open {}", path.display()))?;
    let names = s.iter_metric_names();
    println!("data_dir: {}", path.display());
    println!("metrics:  {}", names.len());
    if !names.is_empty() {
        let preview: Vec<_> = names.iter().take(5).collect();
        for (n, tsid) in preview {
            println!("  - {} (tsid={})", String::from_utf8_lossy(n), tsid.metric_id);
        }
        if names.len() > 5 {
            println!("  ... and {} more", names.len() - 5);
        }
    }
    Ok(())
}

fn export(path: &std::path::Path, from_ms: Option<i64>, to_ms: Option<i64>) -> Result<()> {
    let s = Storage::open(path).with_context(|| format!("open {}", path.display()))?;
    let range = TimeRange {
        min_timestamp_ms: from_ms.unwrap_or(i64::MIN),
        max_timestamp_ms: to_ms.unwrap_or(i64::MAX),
    };
    for (name, _tsid) in s.iter_metric_names() {
        let hits = s.search_by_metric_name(&name, range).context("search")?;
        for sample in hits {
            let record = serde_json::json!({
                "metric": String::from_utf8_lossy(&name),
                "timestamp_ms": sample.timestamp_ms,
                "value": sample.value,
            });
            println!("{record}");
        }
    }
    Ok(())
}
