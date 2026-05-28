# PGO build runbook

Profile-guided optimization (PGO) makes the compiler reorder hot paths
based on a real workload. Expect single-digit-to-low-teens-percent
throughput win on the PromQL evaluator and the ingest decoders.

This runbook assumes a Linux host. macOS works with the same commands;
Windows needs `llvm-profdata.exe` on PATH (ships with rustc on Windows
via `rustup component add llvm-tools-preview`).

## Prerequisites

```sh
rustup component add llvm-tools-preview
```

## 1. Build instrumented

```sh
RUSTFLAGS="-Cprofile-generate=$PWD/target/pgo-data" \
  cargo build --profile release-pgo-generate -p esm-single
```

## 2. Exercise the workload

Run the binary against representative traffic. Anything that touches
the hot paths counts — the criterion bench is a fine starting point:

```sh
RUSTFLAGS="-Cprofile-generate=$PWD/target/pgo-data" \
  cargo bench --profile release-pgo-generate -p esm-promql
```

Or run esm-single against a real dataset:

```sh
RUSTFLAGS="-Cprofile-generate=$PWD/target/pgo-data" \
  ./target/release-pgo-generate/esm-single \
    --storage-data-path ./pgo-data-dir &
# ... shovel scrapes and queries at it for a few minutes ...
kill %1
```

## 3. Merge the profiles

```sh
LLVM_PROFDATA=$(find ~/.rustup -name llvm-profdata | head -1)
$LLVM_PROFDATA merge -o target/pgo-data/merged.profdata target/pgo-data/
```

## 4. Build optimized

```sh
RUSTFLAGS="-Cprofile-use=$PWD/target/pgo-data/merged.profdata" \
  cargo build --profile release-pgo-use -p esm-single
```

The binary at `target/release-pgo-use/esm-single` is the optimized
build.

## CI

PGO does not run in `ci.yml` because the profiling pass needs a
representative workload, which CI doesn't have a clean way to model.
A nightly run on the self-hosted bench rig (per ADR-001 §5–6) is the
right home for this.
