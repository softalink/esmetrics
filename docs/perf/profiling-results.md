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

## Optimization applied + result

The dominant, addressable cost was the **per-sample buffer path**. The pending
`BTreeMap<Tsid, Vec<(i64,i64)>>` paid O(log n) on every sample purely to keep
keys ordered for flush — but flush can sort once. Replaced it with an FNV
`HashMap<Tsid, Vec<(i64,i64)>>` (O(1) amortized insert) + a single
`sort_unstable_by_key` over the tsids at flush (the on-disk metaindex is
binary-searched by tsid, so blocks must still be written sorted — just once,
not maintained per sample).

Measured, validated against TSBS cpu-only (scale-1000, 25.92M rows):

| metric                       | before   | after    | gain |
|------------------------------|----------|----------|------|
| buffer phase (in-process)    | 1.83M/s  | 2.06M/s  | +12% |
| end-to-end ingest, 8 workers | 329,623  | 395,728  | +20% |
| end-to-end ingest, 16 workers| 433,828  | 541,669  | +25% |

At 16 workers the gap to VM (648K rows/s) closed from ~1.5× to **~1.20×**. This
was the first ingest change with a profiled cost model behind it — vs. the three
prior allocation experiments, which were all flat.

## Lever 2 — shard-lock contention (applied, the win that passed VM)

The sub-linear worker scaling pointed at per-shard `Mutex` contention. Default
shard count was `min(cores, 16)`. A shard-count sweep (`ESM_SHARDS` env knob,
22-core box) at fixed workers:

| shards | 8w     | 16w    | 24w    |
|--------|--------|--------|--------|
| 16     | —      | 570K   | —      |
| 22     | 410K   | 605K   | —      |
| **32** | **427K** | **646K** | **700K** |
| 48     | —      | 587K   | —      |

32 (~2× cores at this worker range) dominates everywhere; 48 over-shards —
flush/merge overhead and buffer RAM outweigh contention relief. Default changed
to `(2 × cores).min(32)`. Shipped default now ingests **652K rows/s at 16
workers and 700K at 24 — surpassing VM's 648K.** Ingest is no longer a deficit.

## Lever 1 — per-sample intern hash (evaluated, deferred)

A warm-vs-cold buffer measurement (second ingest of the same keys = all-hits,
the real sustained-load path) gives **2.12M samples/s warm vs 2.06M cold** —
near-identical. So first-seen interning/indexing is *not* the cost; the buffer
phase is dominated by per-sample work done on every sample: an FNV hash of the
~250-byte series key (the `name_to_tsid` probe) **plus** a second FNV hash of
the 32-byte `Tsid` (the pending probe) plus the push.

Cutting it requires one of:
- **two-level interning** (hash the shared ~200-byte tag suffix once per line,
  short per-field key per sample) — invasive: changes the key scheme, parser,
  index, and every query-side lookup;
- **hash-keyed intern** (precompute the key hash in the parser, key by `u64`) —
  trades a 64-bit collision margin for speed (VM uses 128-bit for this reason);
- **merge the two maps** (`name → (tsid, pending_slot)`) to drop the second
  per-sample hash — contained, but complicates flush/search-overlay.

All are single-threaded wins of bounded size (~15–30% of the buffer phase) that
sharding already masks — **ingest already beats VM**. Per the simplicity
mandate and the history of neutral/regressive deep ingest changes, lever 1 is
**deferred**: documented, understood, and not worth the architectural risk
unless a future goal makes single-thread ingest the binding constraint again.
