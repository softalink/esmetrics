#!/bin/bash
# Flamegraph / profiling harness for EsMetrics ingest & query.
#
# Two profilers:
#   1. In-process PHASE profiler — works EVERYWHERE (no perf needed). Attributes
#      ingest cost to parse / buffer / flush. Run:
#        cargo test -p esm-single --release --test profile_ingest -- --ignored --nocapture
#
#   2. SAMPLING flamegraph — needs a real machine with PMU/perf access (fails in
#      sandboxes/VMs where perf_event_open is blocked or there's no vPMU). This
#      script drives that path. Requires: cargo install flamegraph; a kernel
#      allowing perf (sysctl kernel.perf_event_paranoid<=1, or run as root).
#
# Usage: docs/perf/flamegraph.sh {ingest|query} [data_file]
set -euo pipefail
MODE="${1:-ingest}"
DATA="${2:-../tsbs-bench/data/cpu-only.lp}"
ESM="target/release/esm-single"
BIN="${TSBS_BIN:-$HOME/.local/go-tsbs-bin}"
OUT="docs/perf/flamegraph-$MODE.svg"
PORT=18599
DD="$(mktemp -d)"

cargo build --release -p esm-single

# Prefer software event (cpu-clock) so it works without a hardware PMU.
export PERF="record -e cpu-clock -F 997 --call-graph dwarf -g"

"$ESM" --storage-data-path "$DD" --http-listen-addr "127.0.0.1:$PORT" &
ESM_PID=$!
trap 'kill "$ESM_PID" 2>/dev/null || true; rm -rf "$DD"' EXIT
sleep 2

if [ "$MODE" = "ingest" ]; then
  # Profile the server while a load runs against it.
  ( "$BIN/tsbs_load_victoriametrics" --file="$DATA" --urls="http://127.0.0.1:$PORT/write" --workers=8 >/dev/null 2>&1 ) &
  perf record -e cpu-clock -F 997 --call-graph dwarf -g -p "$ESM_PID" -o /tmp/esm.perf -- sleep 25
else
  "$BIN/tsbs_load_victoriametrics" --file="$DATA" --urls="http://127.0.0.1:$PORT/write" --workers=8 >/dev/null 2>&1
  curl -s "http://127.0.0.1:$PORT/api/v1/query_range?query=cpu_usage_user&start=1704067200&end=1704067260&step=60&flush=true" >/dev/null
  ( for _ in $(seq 1 100000); do
      "$BIN/tsbs_run_queries_victoriametrics" --file=../tsbs-bench/queries/double-groupby-all.dat --urls="http://127.0.0.1:$PORT" --workers=8 --max-queries=200 >/dev/null 2>&1
    done ) &
  LOADER=$!
  perf record -e cpu-clock -F 997 --call-graph dwarf -g -p "$ESM_PID" -o /tmp/esm.perf -- sleep 25
  kill "$LOADER" 2>/dev/null || true
fi

# Render. Needs inferno (cargo install inferno) or FlameGraph scripts on PATH.
perf script -i /tmp/esm.perf | inferno-collapse-perf | inferno-flamegraph > "$OUT"
echo "wrote $OUT"
