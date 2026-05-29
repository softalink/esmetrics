# Ingest profiling results

Goal: locate the ingest bottleneck (EsMetrics ~1.9× behind VM on TSBS cpu-only
load) after three allocation-reduction attempts (Cow tags, FNV hashing,
arena-keyed parse) all left throughput flat. Allocation was ruled out; this
pins where the time actually goes.

## Method

`perf`/`cargo flamegraph` capture **nothing usable in this VM** —
`perf_event_open` is blocked (0-byte records even with the `cpu-clock` software
event), so a sampling flamegraph is impossible here. `docs/perf/flamegraph.sh`
is committed for running that path on real PMU-capable hardware.

In its place: an **in-process phase profiler**
(`apps/esm-single/tests/profile_ingest.rs`) that times the three ingest phases
separately against a realistic batch (200k lines, 10 fields each = 2M samples,
1000-host bounded cardinality matching TSBS cpu-only, 16 shards):

```
cargo test -p esm-single --release --test profile_ingest -- --ignored --nocapture
```

## Findings

**Phase attribution (single-threaded, one batch):**

| phase                  | share | throughput        |
|------------------------|-------|-------------------|
| `parse_into` (arena)   | ~28%  | ~4.4M samples/s   |
| **buffer-ingest**      | ~67%  | ~1.83M samples/s  |
| `flush` (to disk)      | ~5%   | —                 |

Buffer-ingest dominates. Its cost is per-sample: `get_or_create_tsid` (FNV
lookup / interning by arena slice) + insert into the pending
`BTreeMap<Tsid, Vec<(i64,i64)>>` (O(log n) per sample). Parse is already cheap;
flush is negligible at this batch size.

**Worker scaling (end-to-end TSBS load against the running server):**

| workers | rows/s  |
|---------|---------|
| 4       | 244,675 |
| 8       | 329,623 |
| 16      | 433,828 |

Ingest **scales with cores but sub-linearly** (4× workers → 1.77× throughput).
It is CPU/contention-bound, not a hard serialization wall — more workers help,
and the gap to VM (648K rows/s) narrows to ~1.5× at 16 workers. The sub-linear
curve points at contention on the per-shard `Mutex` (writers > shards under
flush) on top of the per-sample CPU cost.

## Indicated next optimization

The dominant, addressable cost is the **per-sample buffer path**. The pending
`BTreeMap<Tsid, Vec<(i64,i64)>>` pays O(log n) on every sample purely to keep
keys ordered for flush — but flush can sort once. Replacing it with an FNV
`HashMap<Tsid, Vec<(i64,i64)>>` (O(1) amortized insert) + sort-on-flush removes
that per-sample log factor from 67% of the ingest cost. This is the first
ingest change with a profiled cost model behind it, vs. the prior three
allocation experiments that were flat.
