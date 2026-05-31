# Plan: beat VictoriaMetrics on *every* TSBS benchmark

Status 2026-05-30, HEAD `a427548` (Lever 1 per-part scan + both PromQL correctness
fixes). Based on a fresh, reproduced full benchmark + a four-front source study of
ESM and VM v1.144.0. Supersedes `surpass-all-plan.md` (whose "ingest already beats
VM" premise did **not** reproduce — see below).

## Honest current scoreboard (my clean run, 8 query workers, scale-1000)

| metric | VM | ESM | ESM/VM | verdict |
|---|--:|--:|--:|---|
| ingest @8w (rows/s) | 639,644 | 330,709 | **1.93× behind** | **VM wins (biggest gap)** |
| ingest @16w | ~647K | ~470K | 1.38× behind | VM wins |
| ingest @32w | ~643K | ~549K | 1.17× behind | VM wins |
| double-groupby-{1,5,all} | 58/326/700 ms | 75/384/820 ms | 1.17–1.28× behind | close |
| single-groupby-*-8-1 | 0.98/1.95 ms | 1.59/4.44 ms | 1.6–2.3× behind | behind |
| cpu-max-all-8 | 5.5 ms | 11.0 ms | 2.0× behind | behind |
| single-groupby (4 of 6) | — | — | 0.98–1.2× | parity/ahead |
| correctness | 11/11 | 11/11 | = | tie |
| peak RAM | 2.16 GB | 1.86 GB | **0.86×** | **ESM wins** |
| disk | 121 MB | 89 MB | **0.74×** | **ESM wins** |

**To win everything we must close: (1) ingest, (2) 8-host/cpu-max-all selectors,
(3) the double-groupby tail.** RAM and disk are already won — protect them.

Key worker-count fact: **VM ingest is flat ~640K from 8→32 workers** (it hits a
downstream single-pipeline limit); **ESM scales with workers** (331K→549K @8→32w)
but its per-row CPU cost is ~2× VM's, so it never catches VM at equal workers.
Beating VM ingest at the harness's 8 workers therefore means **~halving ESM's
per-sample ingest cost** — not more parallelism (VM parses per-connection too).

---

## Theme 1 — INGEST (the binding constraint; hardest)

**I3 DIAGNOSTIC RE-RUN (2026-05-30, current binary `a427548`, `profile_ingest`):**
parse 29% (4.50M samples/s), **buffer-ingest 65% (2.02M samples/s)**, flush 6%;
warm/all-hits 2.10M ≈ cold 2.02M. This confirms (a) buffer-ingest is the bottleneck,
and (b) warm≈cold ⇒ the cost is the **per-sample work done on every sample** (the
double FNV hash), NOT first-seen interning/indexing.

**CORRECTION to I1 below:** I initially scoped I1 as a parser tweak (build the tag
prefix once per line). On reading the code, **the parser already does this** —
`parse_line_into` builds the label suffix once per line into a reused `scratch` and
concatenates per field (influx_line.rs:225-260). There is no redundant parse work to
remove; parse is already 4.5M samples/s. So **I1 is purely the buffer-path
double-hash**, which is a hot-path change across sharded.rs + storage.rs, not a
localized parser edit. Prior work (profiling-results.md "Lever 1 deferred") measured
this as a **bounded ~15–30%-of-buffer win** and deferred it as risky. Implication:
**I1 alone moves ~330K→~370–400K @8w — it does NOT reach VM's ~640K @8w.** Beating
VM ingest at 8 workers essentially requires **I2 (two-level interning)**, the
invasive/risky lever. The ingest sweep is therefore gated on I2 succeeding without
regressing RAM/correctness — a real risk, called out explicitly.

**Measured cost structure** (profiling-results.md + ingest-path study): buffer-ingest
≈65%, parse ≈29%, flush ≈6%. Within buffer-ingest the per-sample hot path is:
- `shard_idx(name)` — FNV over the **full ~250-byte canonical key** (`sharded.rs:157`)
- `name_to_tsid.get(name)` — FNV over the **same ~250-byte key again** (`storage.rs:723`)
- `pending.entry(tsid).push(..)` — FNV over the 24-byte Tsid + Vec push (`storage.rs:684`)

So the canonical key is hashed **twice per sample**, and TSBS puts **10 samples per
line all sharing one ~200-byte tag suffix** (`cpu,hostname=h0,... usage_user=..,usage_system=..,(10 fields)`).
VM avoids this: it unmarshals the tag set once per line and its tsidCache/prevTSID
hot path resolves the TSID in ~2–5 cycles on repeats (`storage.go:1996`, per-CPU
rawRows sharding `partition.go:484`).

### Lever I1 — hash the canonical key once, reuse for routing + TSID lookup
Compute the key's hash in the parser/arena stage; thread `(slice, u64 hash)` into
both `shard_idx` and `name_to_tsid`. Look up via hashbrown `RawEntryMut` / a
pass-through `BuildHasher` so neither site re-hashes. Removes one full-key hash per
sample. **Est. +10–15% ingest.** Contained, low risk. **Do first, measure with
`profile_ingest`.**

### Lever I2 — two-level interning: intern the shared tag-set once per line
The 10 fields on a TSBS line share the tag suffix `{hostname=h0,...}`. Intern that
suffix once per line → a `tagset_id`; the per-sample key becomes
`(field_name_id, tagset_id)` — a tiny fixed-size key, hashed cheaply. Collapses the
per-sample 250-byte hash into one 200-byte hash per *line* + 10 short hashes.
**This is the lever most likely to push ingest past VM** (attacks the 67% directly),
but **invasive**: touches the parser, the name↔tsid key scheme, the inverted index,
and every query-side name lookup. **Gate behind I1's measured result;** prototype on
a branch and validate against `profile_ingest` + full correctness (11/11) before
committing. Highest risk, highest reward.

### Lever I3 — confirm it's per-sample cost, not contention (cheap diagnostic)
Re-run the worker-scaling curve (4/8/16/32w) on the current binary first. If scaling
is near-linear, contention is not the issue and I1/I2 are the whole story. If
sub-linear persists, revisit shard write-lock hold time / per-CPU accumulation.
**Do this diagnostic before I2** so we don't invest in the wrong lever.

> Reality check: I1 alone won't beat VM@8w (needs ~2×). I2 is likely **required**,
> and it's the riskiest change in this plan. If I2 doesn't land cleanly, ingest-at-8w
> may stay behind even as 16w+ closes — call that out rather than over-claim.

---

## Theme 2 — double-groupby tail (~1.2×; close to parity)

Wide path already uses the per-part scan, but `scan_tsids` **materializes
`Vec<Vec<StoredSample>>`** for all candidates (`storage.rs:1286`), then the `roll`
closure re-scans each series with `partition_point` per step
(`evaluator.rs:364-376`), with per-sample `i64→f64` in `reduce_over_time_samples`.

### Lever Q1 — fuse decode→rollup (no full materialization)
Push the per-series reducer into the scan: reduce each series' window as its blocks
finish decoding, instead of building the big per-series Vec then re-scanning. Removes
~100k+ `StoredSample` allocations and the second partition_point pass per query.
`scan_series_map` already passes a per-series closure; extend the contract so a
series is reduced as soon as its samples are complete across parts (the merge-join
already yields blocks grouped by series). **Est. 10–20% on double-groupby → parity
or slight lead.** Medium effort; guard with `fast_path_matches_generic` +
`scan_series_map_matches_per_series`.

### Lever Q2 — trim aggregation allocations (minor)
The group-reduce allocates a `Vec<f64>` per (step, group) (`evaluator.rs:431`). Use a
reused scratch buffer. Small; do only if Q1 leaves a measurable gap.

(SIMD decode and mmap remain **rejected** — measured ~1–2% and ~0.5% end-to-end.)

---

## Theme 3 — selective 8-host & cpu-max-all-8 (~2×)

These are sub-15 ms, candidate count 5–80, so they take the **selective per-series
path**: each series opens its shard's overlapping parts (≈40 series × ~4 parts =
~160 part-opens) and rayon fans over 5–40 items (work-imbalanced). Fixed per-query
overhead dominates.

### Lever Q3 — route multi-candidate selective queries through the per-part scan
`scan_tsids` opens each part **once** and seeks/merge-joins to candidate tsids only
(skips non-candidate blocks), so for 8–80 candidates it still reads only their
blocks but without the per-series re-opens. Replace the fixed `WIDE_SCAN_THRESHOLD=256`
with a cost-based chooser: use the scan when `candidates × parts_per_shard > parts`
(i.e. when per-series would re-open parts). **Re-measure** with a new
`selective_scan_compare` microbench at candidate counts 5/40/80 — at very low counts
the per-part fixed cost (open every overlapping part) can regress, so the threshold
must be empirical.

### Lever Q4 — cut `candidate_series` fan-out for tiny queries
Label/name posting lookups fan across all 32 shards (`candidate_series`,
`evaluator.rs:2562+`). For sub-5 ms queries this fixed cost is significant. Resolve
postings only on shards that can hold matches, or short-circuit when one anchor
posting is already tiny. Small but needed for a clean sweep.

---

## Sequencing, gates, risk

1. **I3 diagnostic** (re-measure worker scaling + `profile_ingest` on current binary)
   — cheap, sets the ingest baseline and confirms the lever. *(½ day)*
2. **I1** (hash-once) — contained ingest win, measure. *(1 day)*
3. **Q1** (fuse decode→rollup) — double-groupby to parity; independent of ingest. *(1–2 days)*
4. **Q3 + Q4** (selective scan + fan-out) — 8-host/cpu-max-all. *(1–2 days)*
5. **I2** (two-level interning) — the make-or-break ingest lever; only after I1's
   number is known. Prototype on a branch, full re-verify before landing. *(2–4 days, risky)*

Every step: `cargo build` + `clippy -D warnings` + `fmt` clean; the equivalence
tests (`fast_path_matches_generic`, `scan_series_map_matches_per_series`) green;
re-run the relevant microbench AND a fresh end-to-end value-parity (all 11 MATCH)
before committing. Protect the RAM/disk wins — reject any lever that regresses them.

**Feasibility, stated plainly:** Themes 2 and 3 are likely achievable (parity→lead).
Theme 1 (ingest at 8 workers) is the hard one and hinges on I2 landing without
regressing RAM/correctness; if it doesn't, ESM wins ingest at ≥16 workers but may
trail at 8. I'd rather flag that now than promise a clean sweep and miss.

## Measurement toolkit (no PMU in this VM; all in-process)
- `cargo test -p esm-single --release --test profile_ingest -- --ignored --nocapture` (phase attribution)
- `cargo test -p esm-single --release --test profile_query -- --ignored --nocapture`
- `cargo test -p esm-storage --release --test scan_compare -- --ignored --nocapture`
- `cargo test -p esm-storage --release --test read_path_split -- --ignored --nocapture`
- end-to-end: `../tsbs-bench/run-e2e-lever1.sh` + `run-correctness5.sh` + `compare_json5.py`
</content>

---

## I1 (hash-once) — MEASURED, design verified, ready to implement (2026-05-30)

**Decisive measurement** (`crates/esm-storage/tests/ingest_hash_split.rs`, commit `ee6876e`,
TSBS key shape: 10k keys, avg 183 B, steady state):

| step | ns/sample |
|---|--:|
| hash full key ×2 (today: shard_idx + name_to_tsid) | 239 |
| hash full key ×1 (hash-once ceiling) | 122 |
| hash 24-byte tsid (pending probe) | 4 |
| 2 map probes + push | 183 |
| FULL current buffer path (E) | 289 |

**Removing the duplicate full-key hash saves ~117 ns = ~41% of the buffer path** —
NOT the ~15% the old profiling-results.md estimated when it deferred this. Buffer is
~65% of ingest ⇒ **~26% end-to-end: ~330K → ~420K rows/s @8w.** Real, biggest safe
lever. Still short of VM ~640K@8w (that needs I2). **This supersedes the "Lever 1
deferred" note in profiling-results.md.**

### Verified implementation design (no blockers found)
- `hashbrown` 0.15.5 is already in Cargo.lock (transitive) + cached → builds offline.
  Its `raw-entry` feature is on by default; `raw_entry_mut().from_key_hashed_nocheck(hash, &key)`
  (src/raw_entry.rs:558) lets us supply a precomputed hash AND custom equality,
  bypassing `Hash for Vec<u8>`.
- Make `shard_idx` return the FNV `u64` it already computes (currently discarded after
  `% nshards`). Thread it: `ingest`/`ingest_keyed` route loop → `*_subset`/`*_selected`
  → `buffer_one(name, hash, ts, v)` → `get_or_create_tsid(name, hash)`.
- Switch `name_to_tsid` to `hashbrown::HashMap<Vec<u8>, Tsid>` (API-superset; the ~10
  cold callers — keys/iter/insert/get/entry/len — compile unchanged). Hot path uses
  `from_key_hashed_nocheck`. CRITICAL: the supplied hash (raw-byte FNV from shard_idx)
  must equal what the map would compute, so give the map a `BuildHasher` that FNV-hashes
  raw bytes only (matches shard_idx) — i.e. reuse the existing FnvHasher but ensure the
  probe path uses the precomputed value; equality closure is `|k| k == name`.
- Leave the pending (tsid-keyed) map alone (4 ns).
- Non-keyed paths (`ingest`, `ingest_selected`, used by prom-remote-write etc.) compute
  the hash inline before buffer_one — same one-hash total.

### Risk + guards
Multi-file hot-path change (sharded.rs + storage.rs). Guard: all esm-storage tests
(esp. `scan_series_map_matches_per_series`, ingest/query roundtrip), `profile_ingest`
before/after (expect buffer ~289→~200 ns/sample, ~2.0→~3.0M/s warm), full e2e
ingest@8w + value-parity (all 11 MATCH) + RAM must not regress. Apply via one atomic
Python script with per-anchor assertions (lag-hardened).

### Then I2 (two-level interning) — still the only path to actually beat VM ingest @8w
Intern the shared per-line tag suffix once → `(field_id, tagset_id)` small key. Removes
the remaining full-key hash entirely. Invasive (parser + key scheme + index + query
lookups). Do only after I1 lands and is measured.

---

## I1 — ✅ LANDED & VERIFIED (commit `4e8fb1c`, pushed)

Implemented exactly as designed. Measured end-to-end (scale-1000, 8 workers, fresh load):
- **ingest 330K → 455K rows/s (+38%)**; VM 637K ⇒ **0.52× → 0.71× VM**.
- in-process buffer phase 2.0M → 3.07M samples/s (+53%).
- **peak RSS 1.9 GB (< VM 2.10) and disk 89 MB (< 122) leads preserved.**
- query latencies unchanged; **ALL 11 TSBS query types still byte-identical to VM**
  (compare_json5); 73 esm-storage + 36 esm-promql tests pass; clippy/fmt clean.

The hashbrown `raw_entry` hash-match held across the full dataset — the correctness
risk (precomputed hash ≠ map's rehash → duplicate tsids) did not materialize because
`key_hash` uses the map's own `FnvBuild`.

**Ingest now 0.71× VM @8w — still behind. Only I2 (two-level tag-suffix interning)
removes the remaining full-key hash to close it.** I2 stays the invasive parser+index
rewrite; do it as its own scoped, measured effort.
