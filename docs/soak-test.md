# Soak test runbook

The 30-day soak (backlog item H11) is driven by `cargo xtask soak`. The
test rig works against any running esm-single; in production it should
target a self-hosted runner with persistent storage and observability.

## Quick smoke (30 seconds)

```sh
./target/release/esm-single --storage-data-path /tmp/soak --http-listen-addr 127.0.0.1:18429 &
cargo xtask soak --url http://127.0.0.1:18429 --duration-secs 30
```

Verified baseline (local laptop, 100 series × 5 000 writes/s + 5
queries/s, 30 s): 147 500 writes, 150 queries, zero errors.

## 30-day production soak

```sh
nohup ./target/release/esm-single \
    --storage-data-path /var/lib/esm-soak \
    --http-listen-addr 127.0.0.1:18429 \
    --retention-period-secs $((35 * 86400)) \
    > /var/log/esm-single-soak.log 2>&1 &

# Drive it.
nohup cargo xtask soak \
    --url http://127.0.0.1:18429 \
    --duration-secs $((30 * 86400)) \
    --series 10000 \
    --writes-per-sec 50000 \
    --queries-per-sec 100 \
    > /var/log/esm-soak.log 2>&1 &
```

The soak rig is single-process and process-supervised; if it crashes
mid-run the test must be restarted from scratch (a 30-day run that
restarts at hour 20 is not credible). Use `systemd-run` or a similar
supervisor for the production run.

## What to look for

- **Steady-state RSS**: should plateau, not grow linearly.
- **Disk usage**: should grow at the rate
  (`writes_per_sec × bytes_per_sample`) minus the retention sweeper.
- **Query latency**: p99 should remain stable over 30 days.
- **Error count**: any non-zero count is a defect.

## Operator action required

The 30-day wall-clock run cannot be executed inside an autonomous
agent session. Schedule it on a self-hosted bench runner (per ADR-001
§5–6) and link the resulting log to this document.
