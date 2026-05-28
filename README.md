# EsMetrics

A cross-platform (Linux, macOS, Windows) Rust reimplementation of the
single-node [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics)
suite, with wire-format compatibility, bidirectional native-binary
round-trip, and a hard performance-parity-or-better commitment against
upstream tag **v1.144.0** (released 2026-05-22).

## Status

**Pre-release.** Functionally compatible with VM v1.144.0 across the
ingest, query, backup, agent, alert, and auth surfaces. Empirically
validated against a live VM container — see
[`conformance/HOW-TO-RUN.md`](./conformance/HOW-TO-RUN.md).

Tracking docs:
- [`docs/state/phase-status.md`](./docs/state/phase-status.md) — phase-by-phase narrative
- [`docs/state/backlog.md`](./docs/state/backlog.md) — remaining work
- [`docs/state/progress-log.md`](./docs/state/progress-log.md) — append-only daily log
- [`docs/state/decisions.md`](./docs/state/decisions.md) — architectural decisions
- [`docs/migration-from-vm.md`](./docs/migration-from-vm.md) — VM → EsMetrics swap-in guide

## Binaries

| Binary | Replaces (VM v1.144.0) | Notes |
|---|---|---|
| `esm-single` | `vmsingle` | All major ingest protocols, full Prometheus/VM HTTP read API, snapshots, retention |
| `esm-agent`  | `vmagent`   | promscrape YAML, file_sd, full relabel engine, persistent disk queue |
| `esm-alert`  | `vmalert`   | for/keep_firing state machine + disk persistence + recording rules |
| `esm-auth`   | `vmauth`    | Multi-user YAML, regex URL routing, CIDR IP filters (v4 + v6) |
| `esm-backup` | `vmbackup`/`vmrestore` | sha256-verified incremental backups, vmbackup marker compat |
| `esm-ctl`    | `vmctl`     | inspect + JSON-line export |

`vmui` (the React frontend) is reused from upstream unchanged.

## Wire-format compatibility

Bidirectional native binary round-trip is **validated against a live
upstream VM v1.144.0 container**:

- `crates/esm-protocols/tests/vm_fixtures.rs` — esm decodes real
  VM-produced `/api/v1/export/native` bytes.
- `crates/esm-protocols/tests/vm_writeback.rs` — VM ingests
  esm-encoded `/api/v1/import/native` payloads and exports them
  byte-perfectly.
- `crates/esm-protocols/tests/vm_differential_codec.rs` — randomized
  samples encoded by esm and decoded by VM are byte-equal.

Conformance scenarios that diff esm and VM query responses live under
[`conformance/scenarios/`](./conformance/scenarios). All
five present scenarios (`smoke`, `sum_aggregation`, `series_metadata`,
`labels_endpoint`, `binary_arithmetic`) pass against upstream v1.144.0.

## Build & run

```sh
cargo build --release --workspace

# Local single-node:
./target/release/esm-single --storage-data-path ./esm-data

# Scrape + forward:
./target/release/esm-agent --scrape-url http://localhost:9100/metrics \
    --remote-write-url http://localhost:8428
```

For systemd / launchd / Windows-service templates, see
[`packaging/`](./packaging/).

## Performance

- PromQL evaluator bench: 1.5–1.8 ms / instant query on 50 series × 200
  samples after the TSID-keyed scan optimization. See
  [`docs/build-pgo.md`](./docs/build-pgo.md) for the PGO runbook.

## License

Apache License 2.0 — see [`LICENSE`](./LICENSE) and
[`NOTICE`](./NOTICE).

EsMetrics is an independent Rust reimplementation. It shares no source
code with VictoriaMetrics; compatibility is achieved through clean
reimplementation as permitted by Apache 2.0. See
[`CREDITS.md`](./CREDITS.md) for full attribution.

"VictoriaMetrics" is a trademark of VictoriaMetrics Inc. EsMetrics is
not affiliated with or endorsed by VictoriaMetrics Inc.
