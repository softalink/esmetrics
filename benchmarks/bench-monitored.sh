#!/usr/bin/env bash
# TSBS benchmark with resource monitoring: peak RSS, CPU usage, storage size.
# Mirrors bench.sh regimen exactly (load -> settle+flush -> queries), adding a
# /proc sampler on the server PID and du measurements of the storage dir.
# Summarize the samples with analyze-resources.py.
#
# Usage: ./bench-monitored.sh <label> <server-binary> [extra args...]
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

rm -rf "$RESULTS"; mkdir -p "$RESULTS"

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

# --- resource sampler: epoch, cpu ticks (utime+stime, all threads), VmRSS kB, VmHWM kB
(
  while [ -r "/proc/$SERVER_PID/stat" ]; do
    now=$(date +%s.%N)
    stat=$(cat "/proc/$SERVER_PID/stat" 2>/dev/null) || break
    ticks=$(echo "$stat" | awk '{print $14+$15}')
    rss=$(awk '/^VmRSS:/{print $2}' "/proc/$SERVER_PID/status" 2>/dev/null)
    hwm=$(awk '/^VmHWM:/{print $2}' "/proc/$SERVER_PID/status" 2>/dev/null)
    echo "$now $ticks ${rss:-0} ${hwm:-0}"
    sleep 0.2
  done
) >"$RESULTS/samples.txt" &
SAMPLER_PID=$!

mark() { echo "$1 $(date +%s.%N)" >>"$RESULTS/phases.txt"; }

mark idle_end
echo "=== [$LABEL] load benchmark ==="
mark load_start
"$TSBS_BIN/tsbs_load_victoriametrics" \
    --file="$DATA_DIR/$DATA_FILE" \
    --urls="http://127.0.0.1:$PORT/write" \
    --workers="$WORKERS" --batch-size=10000 \
    --results-file="$RESULTS/load.json" \
    | tee "$RESULTS/load.txt"
mark load_end

sleep 5
curl -fsS "http://127.0.0.1:$PORT/internal/force_flush" >/dev/null 2>&1 || true
sleep 2
mark flush_end
du -sb "$STORAGE" | awk '{print "post_flush", $1}' >>"$RESULTS/storage.txt"

QUERY_TYPES=${QUERY_TYPES:-"single-groupby-1-1-1 single-groupby-1-1-12 single-groupby-1-8-1 single-groupby-5-1-1 single-groupby-5-8-1 cpu-max-all-1 cpu-max-all-8 double-groupby-1 double-groupby-5 double-groupby-all"}

mark query_start
for qt in $QUERY_TYPES; do
    f="$DATA_DIR/queries-$qt.dat"
    [ -s "$f" ] || continue
    echo "=== [$LABEL] query benchmark: $qt ==="
    "$TSBS_BIN/tsbs_run_queries_victoriametrics" \
        --file="$f" --workers="$WORKERS" \
        --urls="http://127.0.0.1:$PORT" \
        --results-file="$RESULTS/query-$qt.json" \
        | tee "$RESULTS/query-$qt.txt" >/dev/null
done
mark query_end

# Final storage size after a forced merge, polling du until stable.
curl -fsS "http://127.0.0.1:$PORT/internal/force_merge" >/dev/null 2>&1 || true
prev=-1
for i in $(seq 1 30); do
    sleep 2
    cur=$(du -sb "$STORAGE" | awk '{print $1}')
    [ "$cur" = "$prev" ] && break
    prev=$cur
done
du -sb "$STORAGE" | awk '{print "post_merge", $1}' >>"$RESULTS/storage.txt"
mark merge_end

kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
echo "=== [$LABEL] done; results in $RESULTS ==="
