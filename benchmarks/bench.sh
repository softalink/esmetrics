#!/usr/bin/env bash
# TSBS benchmark harness: runs load + query benchmarks against a
# EsMetrics/upstream-compatible server binary and records results.
#
# Usage:
#   ./bench.sh <label> <server-binary> [extra server args...]
# Example:
#   ./bench.sh go-vm /home/test/refsrc/bin/victoria-metrics-prod
#   ./bench.sh rust  ../target/release/esmetrics
#
# Env overrides:
#   TSBS_BIN   dir with tsbs binaries        (default /home/test/refsrc/bin)
#   DATA_DIR   dir with generated data       (default /home/test/refsrc/tsbs-data)
#   DATA_FILE  line-protocol file            (default cpu-only-100h-1d.lp)
#   WORKERS    load/query workers            (default 4)
#   QUERY_TYPES space-separated list         (default: all *.dat in DATA_DIR)
set -euo pipefail

LABEL=${1:?label}; shift
SERVER_BIN=${1:?server binary}; shift
SERVER_ARGS=("$@")

TSBS_BIN=${TSBS_BIN:-/home/test/refsrc/bin}
DATA_DIR=${DATA_DIR:-/home/test/refsrc/tsbs-data}
DATA_FILE=${DATA_FILE:-cpu-only-100h-1d.lp}
WORKERS=${WORKERS:-4}
HERE=$(cd "$(dirname "$0")" && pwd)
RESULTS="$HERE/results/$LABEL"
STORAGE=$(mktemp -d /tmp/bench-storage-XXXX)
PORT=8428

mkdir -p "$RESULTS"

echo "=== [$LABEL] starting server ==="
"$SERVER_BIN" -storageDataPath="$STORAGE" -retentionPeriod=100y \
    -httpListenAddr=":$PORT" "${SERVER_ARGS[@]}" \
    >"$RESULTS/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true; wait $SERVER_PID 2>/dev/null || true; rm -rf "$STORAGE"' EXIT

for i in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    [ "$i" = 100 ] && { echo "server failed to start"; exit 1; }
    sleep 0.2
done

echo "=== [$LABEL] load benchmark ==="
"$TSBS_BIN/tsbs_load_victoriametrics" \
    --file="$DATA_DIR/$DATA_FILE" \
    --urls="http://127.0.0.1:$PORT/write" \
    --workers="$WORKERS" --batch-size=10000 \
    --results-file="$RESULTS/load.json" \
    | tee "$RESULTS/load.txt"

# Let background merges/flushes settle before querying.
sleep 5
curl -fsS "http://127.0.0.1:$PORT/internal/force_flush" >/dev/null 2>&1 || true
sleep 2

if [ -z "${QUERY_TYPES:-}" ]; then
    QUERY_TYPES=$(cd "$DATA_DIR" && ls queries-*.dat | sed 's/queries-//; s/\.dat//' | tr '\n' ' ')
fi

for qt in $QUERY_TYPES; do
    f="$DATA_DIR/queries-$qt.dat"
    [ -s "$f" ] || continue
    echo "=== [$LABEL] query benchmark: $qt ==="
    "$TSBS_BIN/tsbs_run_queries_victoriametrics" \
        --file="$f" --workers="$WORKERS" \
        --urls="http://127.0.0.1:$PORT" \
        --results-file="$RESULTS/query-$qt.json" \
        | tee "$RESULTS/query-$qt.txt"
done

echo "=== [$LABEL] done; results in $RESULTS ==="
