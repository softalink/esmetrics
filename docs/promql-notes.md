# PromQL Query Engine Porting Spec — esm-promql + esm-select

Source: VictoriaMetrics **v1.146.0**, `app/vmselect/{promql,netstorage,prometheus,searchutil}` +
`app/vmselect/main.go` (single-node). Companion to `docs/PORTING.md`. Line numbers refer to the
Go sources at `/home/test/refsrc/VictoriaMetrics`.

PRIORITY: the TSBS query path must be complete and fast — `/api/v1/query_range` evaluating
`max(max_over_time(cpu_usage_user{hostname=~'h1|h2'}[1m])) by (__name__)` and
`avg(avg_over_time({__name__=~'cpu_(a|b)'}[1h])) by (__name__, hostname)` — plus general PromQL
correctness on the standard endpoints. Both TSBS shapes hit the same hot path:
**aggr(rollup(metricExpr[d])) → incremental aggregation over parallel per-series rollups**.

Time is int64 milliseconds everywhere; values are f64; `NaN` = "no sample".

---

## 1. Query flow end-to-end

```
HTTP /api/v1/query_range (prometheus.go QueryRangeHandler, L925)
  parse query/start/end/step (httputil.GetTime/GetDuration), extra_label/extra_filters
  validate maxPointsPerTimeseries; AdjustStartEnd (cache alignment, §7.1)
  build promql.EvalConfig{Start,End,Step,MaxPointsPerSeries,Deadline,MayCache,LookbackDelta,RoundDigits}
promql.Exec (exec.go L36)
  parsePromQLWithCache → metricsql.Parse + Optimize + adjustCmpOps  [parse cache]
  evalExpr → evalExprInternal (eval.go L275) — dispatch on AST node type
  timeseriesToResult: removeEmptySeries, sort by MetricName, dup check, RoundDigits rounding
handler post-processing: adjustLastPoints (latency offset), removeEmptyValuesAndTimeseries
WriteQueryRangeResponse (qtpl) → JSON
```

`evalExprInternal` dispatch (eval.go L275-342), the whole evaluator in one switch:
- `MetricExpr` / `RollupExpr` → wrap into `default_rollup` and `evalRollupFunc(...)`.
- `FuncExpr`: if name is a rollup func → `evalRollupFuncArgs` (evaluates non-rollup args, keeps
  the rollup arg as `RollupExpr`) then `evalRollupFunc`; else transform func (args evaluated
  sequentially, except `union` in parallel).
- `AggrFuncExpr` → `evalAggrFunc` (eval.go L376): **first** tries
  `getIncrementalAggrFuncCallbacks(ae.Name)` + `tryGetArgRollupFuncWithMetricExpr(ae)` (L623) —
  matches `aggr(metricExpr)`, `aggr(metricExpr[d])`, `aggr(rollupFunc(metricExpr))`,
  `aggr(rollupFunc(metricExpr[d]))` with plain MetricExpr, no subquery. On match it builds an
  `incrementalAggrFuncContext` and calls `evalRollupFunc(..., iafc)` — this is the TSBS path,
  which never materializes all input series (§3). Otherwise: evaluate args in parallel, group,
  apply general aggr func (aggr.go).
- `BinaryOpExpr` → `evalBinaryOp`: with common-label-filter pushdown (evaluate first side, extract
  labels common to all returned series, push them as extra filters into the second side —
  `getCommonLabelFilters`/`PushdownBinaryOpFilters`, eval.go L469-545); `and`/`if` evaluate the
  right side first. For `or`/`default`/ungroupled aggregates, both sides run in parallel instead.
- `NumberExpr`/`StringExpr`/`DurationExpr` → constant series over the shared timestamp grid.

`evalRollupFunc` (L780) handles `@` modifier (collapse to instant eval at `int64(atValue*1000)`,
then broadcast the single point over the shared grid). `evalRollupFuncWithoutAt` (L831) applies
`offset` by shifting `ec.Start/End` down, evaluates, then shifts result timestamps back up.
`evalRollupFuncWithMetricExpr` (L1594) then:

1. Parses window: `window = re.Window.NonNegativeDuration(ec.Step)` (0 if absent).
2. If `Start == End` → `evalInstantRollup` (instant-query optimization family; port later, §9).
3. If `ec.mayCache()` → rollup result cache lookup (§4); evaluate only the uncovered suffix,
   `mergeSeries` cached + fresh, store back.
4. Cache miss core = `evalRollupFuncNoCache` (L1680):

```go
sharedTimestamps := getTimestamps(ec.Start, ec.End, ec.Step, ec.MaxPointsPerSeries)  // start,start+step,...,end
preFunc, rcs, err := getRollupConfigs(funcName, rf, expr, ec.Start, ec.End, ec.Step,
    ec.MaxPointsPerSeries, window, ec.LookbackDelta, sharedTimestamps)
// data fetch range: [minTimestamp .. ec.End]
minTimestamp := ec.Start
if needSilenceIntervalForRollupFunc[funcName] { minTimestamp -= maxSilenceInterval() }  // rollup.go L114 table
if window > ec.Step { minTimestamp -= window } else { minTimestamp -= ec.Step }
sq := storage.NewSearchQuery(minTimestamp, ec.End, tfss, ec.MaxSeries)
rss, err := netstorage.ProcessSearchQuery(qt, sq, ec.Deadline)
```

`maxSilenceInterval()` = `-search.minStalenessInterval` or **5m default** (eval.go L1796) — this
is the extra lookbehind fetched for functions that need the sample preceding the window
(`default_rollup`, `rate`, `increase`, `delta`, ... — full table rollup.go L114-144).
`max_over_time`/`avg_over_time` are NOT in that table, so TSBS fetches only `[start-window, end]`.

**Memory limiter** (eval.go L1742-1772, memory_limiter.go): estimated
`rollupMemorySize = timeseriesLen*1000 + rollupPoints*16` where
`rollupPoints = pointsPerSeries * timeseriesLen * len(rcs)`; with incremental aggregation
`timeseriesLen` is only `AvailableCPUs()` (×1000 if grouped `by(...)`, capped at series count).
Reserved against a global `memoryLimiter{MaxSize: memory.Allowed()/4}` (simple mutex counter,
`Get(n)`/`Put(n)`); rejection error tells the user to reduce series/increase step. Also honors
`-search.maxMemoryPerQuery`. Port as-is: an `AtomicU64`-or-mutex byte budget in esm-promql.

Then rollup evaluation fans out per series (§2/§3), and results flow back up through the
aggregation/binop/transform layers as `Vec<Timeseries>`.

**Parse cache** (parse_cache.go): 128 buckets keyed by `xxhash(query) % 128`, each a
`RwLock<HashMap<String, Arc<ParsedExpr|Error>>>`, max 10k entries total; on bucket overflow evict
a random ~10%. Caches the *optimized* AST (and parse errors). Rust: same shape, or a single
`DashMap`+random eviction; must cache post-`Optimize`+`adjustCmpOps` results.

## 2. evalRollupFunc mechanics (rollup.go)

### 2.1 rollupConfig and the window loop

One `rollupConfig` is built per output series-kind (usually one; `rollup_candlestick` → 4,
`aggr_over_time` → N) and **shared across all input series** — `Do` is called per series by
worker threads, each appending into its own dst:

```go
type rollupConfig struct {
    TagValue string        // extra "rollup" tag value (rollup_candlestick etc.)
    Func   rollupFunc      // fn(&rollupFuncArg) -> f64
    Start, End, Step, Window int64
    MaxPointsPerSeries int
    MayAdjustWindow bool   // true for rate/deriv/default_rollup/... (rollupFuncsCanAdjustWindow, L199)
    Timestamps []int64     // shared output grid
    LookbackDelta int64
    isDefaultRollup bool
    samplesScannedPerCall int  // cost model only
}
```

Core loop `doInternal` (rollup.go L701, exact semantics to replicate):

```go
maxPrevInterval := rc.Step
if rc.Start < rc.End {                       // range query: infer scrape interval
    scrapeInterval := getScrapeInterval(timestamps, rc.Step)   // 0.6-quantile of last ≤20 sample deltas
    maxPrevInterval = getMaxPrevInterval(scrapeInterval)
}
if rc.LookbackDelta > 0 && maxPrevInterval > rc.LookbackDelta { maxPrevInterval = rc.LookbackDelta }
if msi := minStalenessInterval; msi > 0 && maxPrevInterval < msi { maxPrevInterval = msi }
window := rc.Window
if window <= 0 {                             // only when no explicit [d] in the query
    window = rc.Step
    if rc.MayAdjustWindow && window < maxPrevInterval { window = maxPrevInterval }
    if rc.isDefaultRollup && rc.LookbackDelta > 0 && window > rc.LookbackDelta { window = rc.LookbackDelta }
}
i, j := 0, 0
for _, tEnd := range rc.Timestamps {
    tStart := tEnd - window
    i += seekFirstTimestampIdxAfter(timestamps[i:], tStart, niHint)   // first idx with ts > tStart
    if j < i { j = i }
    j += seekFirstTimestampIdxAfter(timestamps[j:], tEnd, njHint)     // first idx with ts > tEnd
    rfa.prevValue = nan; rfa.prevTimestamp = tStart - maxPrevInterval
    if i < len(timestamps) && i > 0 && timestamps[i-1] > rfa.prevTimestamp {
        rfa.prevValue = values[i-1]; rfa.prevTimestamp = timestamps[i-1]
    }
    rfa.values = values[i:j]; rfa.timestamps = timestamps[i:j]
    rfa.realPrevValue = nan
    if i > 0 {   // gated by LookbackDelta: gap from values[i-1] to first-in-window < LookbackDelta
        if rc.LookbackDelta == 0 || (currTimestamp-timestamps[i-1]) < rc.LookbackDelta { rfa.realPrevValue = values[i-1] }
    }
    rfa.realNextValue = if j < len(values) { values[j] } else { nan }
    rfa.currTimestamp = tEnd
    dstValues = append(dstValues, rc.Func(rfa)); rfa.idx++
}
```

Key invariants:
- Window is **(tEnd-window, tEnd]** — exclusive left, inclusive right.
- `i`/`j` advance monotonically; `seekFirstTimestampIdxAfter` first probes a ±2 neighborhood of
  the previous hit, scans linearly for <16 candidates, else binary-searches. In Rust: two cursors
  + `partition_point`, same hint trick.
- `getMaxPrevInterval` inflation table (L899): interval ≤2s→×5, ≤4s→×3, ≤8s→×2, ≤16s→×1.5,
  ≤32s→×1.25, else ×1.125. `getScrapeInterval` = 0.6-quantile of the last ≤20 inter-sample deltas
  (fallback `Step`).
- `rollupFuncArg` fields: `prevValue/prevTimestamp` (sample just before window, if within
  maxPrevInterval), `values/timestamps` (window slice, NaN-free by contract), `realPrevValue`
  / `realNextValue` (unconditional neighbors, LookbackDelta-gated), `currTimestamp`, `idx`, `window`.

### 2.2 preFunc

`getRollupConfigs` (L374) returns `(preFunc, Vec<rollupConfig>)`. `preFunc(values, timestamps)`
runs **once per raw series before windowing**. It is the identity for almost everything; only:
- `rollupFuncsRemoveCounterResets` = {rate, irate, increase, increase_pure, increase_prometheus,
  rate_prometheus, rollup_rate, rollup_increase} → `removeCounterResets(values, ts, stalenessInterval)`
  where `stalenessInterval = lookbackDelta + window` if lookbackDelta≠0. removeCounterResets
  (L921): running-correction algorithm with a partial-reset heuristic (`d < 0 && (-d*8) < prev`
  → treat as partial reset, correction -= d), gap-reset when `ts gap > maxStalenessInterval`,
  and a float-precision monotonic clamp.
- `rollup_rate/rollup_deriv` chain `derivValues`; `rollup_increase/rollup_delta` chain
  `deltaValues`; `rollup_scrape_interval` converts to inter-sample intervals.

Before preFunc, the driver (eval.go L1812/1856) calls `dropStaleNaNs` (§5) and — for subqueries —
`removeNanValues`. Rollup funcs may therefore assume NaN-free windows.

### 2.3 Rollup functions table and port priority

Full registry (`rollupFuncs`, rollup.go L24-108), 74 names:
absent_over_time, aggr_over_time, ascent_over_time, avg_over_time, changes, changes_prometheus,
count_{eq,gt,le,ne}_over_time, count_over_time, count_values_over_time, decreases_over_time,
default_rollup, delta, delta_prometheus, deriv, deriv_fast, descent_over_time,
distinct_over_time, duration_over_time, first_over_time, geomean_over_time, histogram_over_time,
hoeffding_bound_{lower,upper}, holt_winters, idelta, ideriv, increase, increase_prometheus,
increase_pure, increases_over_time, integrate, irate, lag, last_over_time, lifetime,
mad_over_time, max_over_time, median_over_time, min_over_time, mode_over_time,
outlier_iqr_over_time, predict_linear, present_over_time, quantile_over_time,
quantiles_over_time, range_over_time, rate, rate_prometheus, rate_over_sum, resets, rollup,
rollup_candlestick, rollup_delta, rollup_deriv, rollup_increase, rollup_rate,
rollup_scrape_interval, scrape_interval, share_{eq,gt,le}_over_time, stale_samples_over_time,
stddev_over_time, stdvar_over_time, sum_{eq,gt,le}_over_time, sum_over_time, sum2_over_time,
tfirst_over_time, timestamp, timestamp_with_name, tlast_change_over_time, tlast_over_time,
tmax_over_time, tmin_over_time, zscore_over_time.

**Stage 1 (TSBS + Prometheus basics):** max_over_time, min_over_time, avg_over_time,
sum_over_time, count_over_time, last_over_time, first_over_time, **default_rollup**, rate,
increase, delta, irate, deriv_fast, timestamp, present_over_time, stddev/stdvar_over_time,
quantile_over_time, changes, resets. Everything else is Stage 2 (mechanical: each is a small
`fn(&RollupFuncArg) -> f64`).

The Stage-1 implementations are trivial reductions over `rfa.values` with **`nan` on empty
window** (do NOT fall back to prevValue — deliberate, see comments in rollupAvg L1541):
`avg = sum/len`, `max/min` = fold, `count = len as f64`, `sum`, `last = values[last]`
(= `rollupDefault`, which intentionally keeps a trailing staleness mark), `first = values[0]`.

`rate` = removeCounterResets preFunc + `rollupDerivFast` (L1954): uses `(prevValue,prevTimestamp)`
as left endpoint; if prevValue is NaN it needs ≥2 in-window samples and uses `values[0]` as left
endpoint (single sample → NaN); if window empty but prevValue exists → 0.
`increase`/`delta` = `rollupDelta` (L1859): if prevValue NaN — use realPrevValue if present
(`last - realPrevValue`); else treat prev as 0 **only if** `|values[0]| < 10*(|d|+1)` where `d`
is the first in-window delta (guards against huge counters appearing mid-range), otherwise use
`values[0]` as prev and drop it; empty window → 0. These heuristics differ from Prometheus by
design; port verbatim.

Tag tables to port as sets: `rollupFuncsKeepMetricName` (L265: avg/min/max/median/mode/first/
last/quantile(s)_over_time, default_rollup, rollup*, timestamp_with_name, ... — these do not drop
`__name__`), `rollupFuncsCanAdjustWindow` (L199), `needSilenceIntervalForRollupFunc` (L114),
`rollupFuncsSamplesScannedPerCall` (L234, cost model only).

Per-series driver `doRollupForTimeseries` (eval.go L1886): copy MetricName, add `rollup` tag if
`rc.TagValue != ""`, `ResetMetricGroup()` unless keepMetricNames or table above, `rc.Do(...)`,
attach `sharedTimestamps` (no copy). A few funcs (`quantiles_over_time`, `count_values_over_time`,
`histogram_over_time`, `aggr_over_time`) emit multiple series via `timeseriesMap` — Stage 2.

## 3. Incremental aggregation (aggr_incremental.go) — CRITICAL for TSBS

Purpose: `max(max_over_time(m[1m])) by (__name__)` over 10k series must hold only
O(workers × groups) series in memory, not 10k rolled-up series.

Supported set (`incrementalAggrFuncCallbacksMap` L18): **sum, min, max, avg, count, sum2,
geomean, any, group**. Each provides `{update(iac, values), merge(dst, src), finalize(iac),
keep_original: bool}` (only `any` keeps original MetricName).

Structure:

```go
type incrementalAggrFuncContext struct {
    ae *metricsql.AggrFuncExpr
    byWorkerID []incrementalAggrContextMap   // one HashMap<String, incrementalAggrContext> per worker, cache-line padded
    callbacks  *incrementalAggrFuncCallbacks
}
type incrementalAggrContext struct {
    ts     *timeseries   // accumulator values, len == len(grid)
    values []float64     // per-point counts / aux state
}
```

`updateTimeseries(ts, workerID)` (L98) — called from netstorage RunParallel workers, **no locks**
(worker-private maps): apply `removeGroupTags(&ts.MetricName, &ae.Modifier)` (by/without, §6),
key = `marshalMetricNameSorted`, get-or-create accumulator (respecting `ae.Limit` on group
count), then `updateAggrFunc(iac, ts.Values)`. Update kernels are pure element-wise loops, e.g.:

```go
func updateAggrMax(iac, values) {
    for i, v := range values {
        if math.IsNaN(v) { continue }
        if dstCounts[i] == 0 { dstValues[i] = v; dstCounts[i] = 1; continue }
        if v > dstValues[i] { dstValues[i] = v }
    }
}
func updateAggrAvg(iac, values) {   // sum + count; finalize divides
    ... if dstCounts[i] == 0 { dstValues[i]=v; dstCounts[i]=1 } else { dstValues[i]+=v; dstCounts[i]++ }
}
```

`finalizeTimeseries()` (L141): merge all per-worker maps into one global map with
`mergeAggrFunc` (e.g. mergeAggrAvg adds both sums and counts; merge* skips src points with
count==0), then `finalizeAggrFunc` per group (avg divides; common: count==0 → NaN; count →
0→NaN; group → non-zero→1). Result = one series per group.

Rust design: `Vec<CachePadded<HashMap<Vec<u8>, IncrAggrContext>>>` indexed by worker id
(rayon `current_thread_index()` or explicit worker index from our own pool); counts as `Vec<f64>`
(counts can exceed u32 in avg-merge; f64 matches Go exactly). The `evalRollupWithIncrementalAggregate`
driver (eval.go L1804) per series: `dropStaleNaNs` → `preFunc` → for each rc: rollup into a
**scratch** timeseries (reused per worker) → `iafc.updateTimeseries(ts, workerID)`. The scratch's
Timestamps points at sharedTimestamps and is nulled before reuse.

The non-incremental path `evalRollupNoIncrementalAggregate` (L1845) is identical minus iafc:
each worker appends finished series to its own padded `Vec` (`timeseriesByWorkerID`), concatenated
at the end.

## 4. Rollup result cache (rollup_result_cache.go)

Caches **final rollup results per (expr, window, step)** so repeated range queries only compute
the new tail. Backed by `workingsetcache` (fastcache-style, size = `memory.Allowed()/16`,
persisted to disk at shutdown under `<cacheDataPath>/tmp/rollupResult`).

Two-level keying:
- **metainfo key** = `marshalRollupResultCacheKeyForSeries` (L559):
  `version(=11) ++ u64 keyPrefix ++ type(0=series,1=instant) ++ i64 window ++ i64 step ++
  marshaled etfs (CacheTagFilters) ++ expr.AppendString()`. Value = `rollupResultCacheMetainfo`:
  up to 10 `{start, end, key{prefix,suffix}}` entries (on overflow drop the 5 oldest).
- **data key** = `{version, prefix, suffix}` → zstd(level1)-compressed `marshalTimeseriesFast`
  blob (§5), capped at cacheSize/4 per entry.

`GetSeries` (L283): pick `GetBestKey(start,end)` = entry with `e.start <= start` maximizing
covered span; unpack; find `i` = first ts ≥ ec.Start — **require `timestamps[i] == ec.Start`**
(exact grid match, hence the mayCache alignment requirement); trim to `[i..j]` where
`timestamps[j] <= ec.End`; return `(tss, newStart = lastTs + step)`. Caller evaluates
`[newStart..end]` and `mergeSeries` (L618) concatenates per-key (marshalMetricNameSorted),
filling NaN runs for series present on only one side; bails (→ full re-eval) on duplicate keys.

`PutSeries` (L364): refuse duplicates-by-name; **truncate points newer than
`now - step - cacheTimestampOffset`** (default `-search.cacheTimestampOffset=5m`) so
possibly-still-arriving data is never cached; skip if metainfo already covers the range; store
under fresh `{prefix, suffix=atomic counter seeded with UnixNano}`.

Invalidation: `ResetRollupResultCache()` bumps the random-at-startup `keyPrefix` (O(1) global
invalidation); the insert path calls `ResetRollupResultCacheIfNeeded(mrs)` — if any ingested
sample is older than `now-5m`, a background loop resets the cache (backfill protection).
`ec.mayCache()` requires `!disableCache && MayCache && (instant || start%step==0 && end%step==0)`.

Rust: same two-level scheme over our workingset cache; zstd via `zstd` crate; the
marshalTimeseriesFast format is internal so we can keep the Go layout (u64 counts, raw
little-endian ts/values arrays 8-byte aligned, then metric names) for mmap-friendly zero-copy
unmarshal. Stage 1 can ship with the cache behind a flag but TSBS repeat-query latency benefits;
implement early, it is not large.

## 5. Timeseries model, staleness, result marshaling

```go
type timeseries struct {         // promql-internal
    MetricName storage.MetricName  // MetricGroup []byte + Tags []{Key,Value []byte}
    Values     []float64
    Timestamps []int64             // usually the SHARED grid slice — do not mutate
    denyReuse  bool
}
type netstorage.Result { MetricName; Values []f64; Timestamps []i64 }  // API-facing
```

- All series produced by one rollup share one `Timestamps` allocation
  (`assertIdenticalTimestamps` enforces). Rust: `Arc<Vec<i64>>` (or an enum
  `SharedOrOwned`) for Timestamps; Values always owned.
- Grid: `timestamps[k] = start + k*step`, count `= (end-start)/step + 1`, validated against
  `-search.maxPointsPerTimeseries` (default 30e3).
- **Staleness NaN**: Prometheus stale markers are a special NaN bit pattern
  (`decimal.StaleNaN`, payload-distinguished from arithmetic NaN — in Rust use the same fixed
  bit pattern `0x7ff0000000000002`-style constant ported from lib/decimal, compare with
  `to_bits()`). `dropStaleNaNs` (eval.go L1985) strips them before every rollup **except**
  `default_rollup` and `stale_samples_over_time` (default_rollup uses them for staleness
  detection: a stale marker as last window sample yields NaN → gap, matching Prometheus).
- Grouping/matching key primitive used *everywhere* (aggr grouping, incremental aggr, binop
  matching, mergeSeries, dedup checks): sort tags by key, then
  `marshalMetricNameSorted = len16-prefixed MetricGroup ++ (len16 key ++ len16 value)*`.
  Implement once in esm-promql and reuse identically.
- `Exec` epilogue: `removeEmptySeries` (all-NaN dropped), sort by (MetricGroup, tags) unless the
  top-level expr is sort*/topk*/bottomk*/`or`, duplicate-name check (error), round to
  `RoundDigits` if < 100.

**JSON shapes** (qtpl templates; no whitespace; port as handwritten serializers — do NOT use
serde_json trees on the hot path, write straight into the output buffer):

```
/api/v1/query_range:
{"status":"success","data":{"resultType":"matrix","result":[
  {"metric":{"__name__":"...","k":"v",...},"values":[[<ts>,"<val>"],...]},...]},
 "stats":{"seriesFetched": "<n>","executionTimeMsec": <ms>}}
/api/v1/query:   resultType is ALWAYS "vector" in the single-node Go upstream (scalar exprs → one series, "metric":{})
  {"metric":{...},"value":[<ts>,"<val>"]}
/api/v1/series:  {"status":"success","data":[{"__name__":"...","k":"v"},...]}
/api/v1/labels | /api/v1/label/X/values: {"status":"success","data":["a","b",...]}
/api/v1/export (JSON-lines, ts in ms integers, NaN→null, ±Inf→"Infinity"/"-Infinity"):
  {"metric":{...},"values":[v,...],"timestamps":[ms,...]}\n
```

Number formatting (util.qtpl `{%f=}`): if `value == value.trunc()` and fits → print as integer,
else `strconv.AppendFloat(f,'f',-1,64)` — shortest fixed-notation (never exponent). Timestamps
are `f64(ms)/1e3` seconds (≤3 decimals); sample values are that formatting **inside quotes**.
Rust: `ryu`-style fixed formatting must match (write a small formatter: integer fast path, else
shortest-roundtrip then reject exponent form like Go's 'f'); TSBS validates values textually.
`"seriesFetched"` is a quoted string (vmalert compat). `trace=1` appends `,"trace":{...}`.
Errors use the Prometheus envelope `{"status":"error","errorType":"...","error":"..."}` (422).
No `isPartial` in single-node.

## 6. Aggregate, binary, transform functions

### 6.1 aggr.go — 37 functions

sum, min, max, avg, count, group, any, mode, median, geomean, sum2, stddev, stdvar, distinct,
histogram, count_values, topk, bottomk, topk_/bottomk_{min,max,avg,median,last}, quantile,
quantiles, limitk, mad, outliers_iqr, outliers_mad, outliersk, share, zscore.

Generic path: concat args → `removeEmptySeries` → group by `removeGroupTags`+sorted-marshal key
(`aggrPrepareSeries` L121; `ae.Limit` caps group count) → reducer mutates `tss[0].Values`
in place and returns `tss[:1]`. `removeGroupTags`: `by (...)` = `RemoveTagsOn(args)` (keeps only
listed; drops `__name__` unless listed); `without (...)` = `RemoveTagsIgnoring(args)` +
`ResetMetricGroup()`. NaN skipped per point; all-NaN point → NaN.

**Port order:** Stage 1 = the incremental set (sum,min,max,avg,count,group — the general
versions too, they're needed when the incremental fast-path doesn't match, e.g.
`max(foo or bar)`) + topk/bottomk + quantile/median + count_values + stddev/stdvar.
Stage 2 = the rest. Note topk/bottomk are per-point (k from scalar arg per index; losers get NaN
at that index); topk_* rank whole series. Quantile: `rank = phi*(n-1)` linear interpolation.

### 6.2 binary_op.go

Ops: `+ - * / % ^ atan2`, `== != > < >= <=` (with/without `bool`), `and or unless`, MetricsQL
`if ifnot default`. Evaluation: `adjustBinaryOpTags` produces three parallel vectors
(left[i], right[i], dst[i]) then applies the scalar fn pointwise.

Matching: build per-side `HashMap<key, Vec<ts>>` where key = MetricName with `__name__` reset
(unless keep_metric_names) then `on(...)`→RemoveTagsOn / `ignoring(...)`→RemoveTagsIgnoring
(default = `ignoring ()`), sorted-marshaled. No modifier → `ensureSingleTimeseries` per side per
key (merges non-overlapping duplicates, ≤2-point overlap tolerated; else error
"duplicate time series"); output labels = left with group tags removed. `group_left/right` →
`groupJoin`: copy join-modifier tags from the "one" side onto each "many"-side series
(`MetricName.SetTags`), dedup by resulting name with non-overlapping merge. Scalar fast path when
one side is a single no-name no-tag series.

Comparison without `bool` = filter (keep left value or NaN; metric names preserved); with
`bool` = 1/0, NaN left → NaN. `and/or/unless/if/ifnot/default` operate on the keyed groups with
per-point NaN presence logic (see binary_op.go L416-638); `or` has Prometheus-compatible
ordering (left sorted, appended right sorted separately). Comparison ops skip
`removeEmptySeries` so `default` still sees all-NaN series.

**Stage 1:** arithmetic + comparison (scalar-vector and vector-vector one-to-one) + `and/or/
unless/default`. **Stage 2:** group_left/right, if/ifnot, filter pushdown optimization
(getCommonLabelFilters — worth doing for perf, not correctness).

### 6.3 transform.go — 108 functions

Full list at transform.go L23-137. Categories:
- One-arg math via `newTransformFuncOneArg` (elementwise, drops `__name__` unless
  keep_metric_names/whitelist): abs, ceil, floor, exp, ln, log2, log10, sqrt, round(2-arg),
  trig/hyperbolic, deg/rad, sgn. **Stage 1.**
- Label manipulation: label_replace (anchored regex `^(?:re)$`; non-matching series untouched;
  empty result removes dst tag; dst may be `__name__`), label_join, label_set, label_del,
  label_keep, label_copy/label_move, label_map, label_lowercase/uppercase, label_transform
  (unanchored), label_value, label_match/mismatch, labels_equal, drop_common_labels. **Stage 1:
  label_replace, label_join, label_set, label_del.**
- Time: time(), now, start/end/step, hour/minute/month/year/day_of_* (UTC on unix-seconds
  values). **Stage 1: time().**
- Complex: scalar, vector, union, absent, clamp/clamp_min/clamp_max, histogram_quantile (+
  vmrange→le conversion, bucket fixing), sort family, running_*/range_*, interpolate,
  keep_last_value, prometheus_buckets, etc. **Stage 1: scalar, vector, clamp*, absent, sort,
  sort_desc, histogram_quantile.** Rest Stage 2.

Gotcha: `transformFuncsKeepMetricName` contains a typo `"range_sddev"` — replicate the table
literally if bug-compat matters, otherwise document divergence.

## 7. HTTP layer (esm-select)

### 7.1 Endpoints

| Path | Handler notes |
|---|---|
| `/api/v1/query_range` | §1. `start` default `now-5m`, `end` default now, `step` default 5m (`defaultStep=300000`). `start>end` → `end=start+5m`. If `mayCache` (no `nocache=1`): `AdjustStartEnd` aligns start down / end up to step multiples when ≥50 points, then trims end to preserve point count. Post: if `step < -search.maxStepForPointsAdjustment(1m)` and `ct-latencyOffset < end` → `adjustLastPoints(result, ct-latencyOffset, ct+step)`; then strip NaN points + empty series. |
| `/api/v1/query` | `time` default now; `step` default = lookbackDelta or 5m. Special rewrites: bare `selector[d]` → raw-sample export in promapi matrix format over `(time-offset-d, time-offset]` (left-exclusive: `start++`); rollup-expr `expr[w:step]` → delegate to range handler. If `|ct-time| < latencyOffset` and no nocache: evaluate at `ct-latencyOffset`, then shift result timestamps back. `Exec(..., isFirstPointOnly=true)` truncates every series to 1 point. |
| `/api/v1/series` | requires `match[]`; start default `end-5m` (NOT min-time); `NewSearchQuery(..., -search.maxSeries=30e3)` → `SearchMetricNames`; `limit` arg. |
| `/api/v1/labels`, `/api/v1/label/X/values` | optional match[]/start/end (start default 0/end now → index-wide fast path in storage); cap `-search.maxLabelsAPISeries=1e6`; 5s default deadline; sorted output; `U__` label-name unescaping. |
| `/api/v1/export` | requires match[]; streams JSON lines via `ExportBlocks` (block-level parallel callback); formats: default JSON-lines / `prometheus` / `promapi`. |
| `/health` | in lib/httpserver: returns 200 `OK` (also `/ping`). |
| `/internal/force_flush`, `/internal/force_merge` | app/victoria-metrics level: force flush of in-memory parts (TSBS harness calls force_flush before querying) — implement in esmetrics bin against esm-storage. |
| `/internal/resetRollupResultCache` | authKey-gated `ResetRollupResultCache()`. |
| Stubs | `/api/v1/status/buildinfo` → `{"status":"success","data":{"version":"2.24.0"}}`; `/api/v1/rules`, `/api/v1/alerts`, `/api/v1/query_exemplars` → empty success stubs (Grafana compat). |

### 7.2 main.go plumbing

- Path normalization: collapse `//`, strip `/prometheus` & `/graphite` prefixes.
- **Concurrency limiter**: `Semaphore(-search.maxConcurrentRequests, default = 2*CPUs)`;
  non-blocking acquire, else wait `min(queryTimeout, -search.maxQueueDuration=10s)`; timeout →
  HTTP 429 + `Retry-After: 10`; client disconnect → abort. Static/UI/status paths bypass it.
- **Deadline**: `-search.maxQueryDuration=30s`, per-request `timeout` arg (seconds, clamps down
  only). `Deadline{deadline: unix_secs(start+timeout)}`; `Exceeded()` polls a coarse 1s cached
  clock (`fasttime`) — cheap enough to check per block / per series. Rust: `AtomicU64` global
  seconds tick updated by a timer thread.
- `-search.logSlowQueryDuration=5s` logging; `extra_label`/`extra_filters` args → enforced tag
  filters ANDed/cross-producted into every selector (`JoinTagFilterss`).
- Time parsing (lib/httputil + lib/timeutil): unix s/ms/µs/ns auto-detected by magnitude,
  RFC3339 + calendar prefixes, `now-1h` relative forms; defaults rounded to whole seconds.
  Durations: numeric = float **seconds**, else Prometheus duration string; range [1ms, 100y].

## 8. Netstorage: fetch, unpack, merge (esm-select ↔ esm-storage boundary)

`ProcessSearchQuery` (netstorage.go L1052):
1. `storage.Search::Init` over tag filters + time range (returns max series estimate).
2. Loop `NextMetricBlock()`: blocks arrive **grouped by series, in order**. Per block: enforce
   `-search.maxSamplesPerQuery` (default 1e9, counts full block rows), then
   `br.Marshal(buf)` (block header + still-compressed data) → `tbf.WriteBlockRefData(buf)` →
   record `blockRef{partRef, addr{offset,size}}` in the per-series list; unique metric names kept
   in first-seen order (`orderedMetricNames`) so later reads are near-sequential.
3. Result: `Results{tr, deadline, packedTimeseries[{metricName, brs}], search, tbf}`.

**tmpBlocksFile**: per-query buffer of capacity `clamp(memory.Allowed()/1024, 64KiB, 4MiB)`;
`WriteBlockRefData` returns logical `(offset,size)` before writing; when the buffer would
overflow it is flushed to an anonymous temp file under `<tmp>/searchResults` (unlinked
immediately on close). `Finalize()` flushes the tail and opens an mmap-backed `ReaderAt`
(fadvise-sequential). `MustReadBlockRefAt` = slice of buf (in-memory case) or pooled-buffer
`pread/mmap-copy`. Rationale: copies compressed blocks out of live storage parts so the storage
search (and part lifetimes) can be released before CPU-heavy unpacking; bounds query RAM. Rust:
same design — `enum TmpBlocks { Mem(Vec<u8>), File(File+Mmap) }`; unlink-after-create on Linux,
delete-on-close flag on Windows.

`Results::RunParallel(f)` (L217): `MaxWorkers() = min(cpus, 32)` (flag
`-search.maxWorkersPerQuery`). Fast path if 1 worker or 1 series. Otherwise: N worker channels,
round-robin series distribution, close all, each worker drains its own queue then **steals** from
other queues (non-blocking `len>0` check, rotating offset). Per series (`timeseriesWork.do`):
check mustStop + deadline → `Unpack`:
- decode each block: `MustReadBlockRefAt` → `MustReadBlock` → `UnmarshalData` →
  `AppendRowsWithTimeRangeFilter(tr)` (drops rows outside `[tr.min, tr.max]`, converts decimal
  to f64) → one sorted `sortBlock` per storage block. If a series has **>1000 blocks**, unpacking
  itself fans out over CPUs with the same steal pattern; enforce `-search.maxSamplesPerSeries=30e6`.
- `mergeSortBlocks` (L562): k-way merge via min-heap on `Timestamps[NextIdx]`. Per iteration:
  if dedup enabled and top shares an identical (ts, bitwise-value) prefix with the runner-up,
  skip that prefix from top; else bulk-copy from top all samples with `ts <= next_top_ts`
  (binary search; ties go to current top). Finish with `storage.DeduplicateSamples(ts, vals,
  dedupInterval)` (keeps last sample per aligned interval; equal timestamps → max value,
  preferring non-stale-NaN).
- then `f(result, workerID)` (the rollup+incremental-aggregation closure from §3).

Rust concurrency mapping: use a dedicated worker pool (or scoped rayon) with **explicit worker
ids 0..MaxWorkers** — worker id indexes the padded per-worker state (incremental aggr maps,
result vecs, scratch buffers: block decode buffer, sortBlock pool, Result scratch). A
`crossbeam_deque` (or simply `Vec<Mutex<VecDeque>>` + steal loop, faithful to Go) gives the same
locality: own queue first, then steal. Cancellation = `AtomicBool` + coarse deadline checks.
Don't share one global pool across queries for per-worker state; per-query worker arrays are the
Go design and avoid cross-query interference.

## 9. Adjustments & gotchas checklist

- **Step alignment**: cache requires `start%step==0 && end%step==0`; `AdjustStartEnd` only when
  ≥50 points and cache enabled; subqueries always `alignStartEnd`.
- **Lookback delta**: `LookbackDelta` in EvalConfig comes from `-search.maxLookback` /
  `max_lookback` arg / `-search.maxStalenessInterval` (all default 0 = auto). When 0, gap-filling
  for `default_rollup` is governed by the inferred `maxPrevInterval` (scrape-interval based), NOT
  a fixed 5m — this is the big divergence from Prometheus. The 5m constant appears only as
  `maxSilenceInterval()` extra fetch lookbehind.
- **maxPointsPerSeries**: `-search.maxPointsPerTimeseries=30e3` validated *before* eval;
  subqueries use `-search.maxPointsSubqueryPerTimeseries=100e3`.
- **adjustLastPoints / latencyOffset**: the trailing `(ct-latencyOffset, ct+step]` points of a
  range query are overwritten with the previous complete value (§7.1); runs only when
  `step < 1m` and `end` is near now — TSBS queries are historical, so it rarely fires, but port
  it (prometheus.go L1067) for live-query correctness.
- **Instant-rollup optimizations** (eval.go L1052-1412: avg→sum/count rewrite, rate→increase/d,
  incremental max/min/sum cache deltas): Stage 2+; correctness never depends on them.
- **jsonp**: skip (not implemented in these handlers).
- **Duplicate output series** → hard error ("duplicate output timeseries").
- **`nocache=1`** disables both cache read/write and latency-offset start adjustment.
- Empty selector `{}` is rejected at parse; empty result of `me.IsEmpty()` → NaN constant series.
- Rounding: `round_digits` arg, applied at Exec exit via decimal round-to-digits.

## 10. Rust module breakdown & staging

```
esm-promql/src/
  lib.rs            — Exec(), EvalConfig, public Result type
  eval.rs           — evalExprInternal dispatch, evalRollupFunc*, memory estimate, doParallel
  rollup/mod.rs     — RollupConfig, do_internal window loop, seek/scrape-interval helpers
  rollup/funcs.rs   — rollup fn registry + implementations (stage tables from §2.3)
  aggr.rs           — general aggregation (grouping + reducers)
  aggr_incremental.rs — callbacks map, per-worker maps, update/merge/finalize kernels
  binary_op.rs      — matching + op kernels
  transform.rs      — transform registry (stage 1 subset first)
  timeseries.rs     — Timeseries, MetricName sorted-marshal key, marshal_fast (cache format)
  rollup_result_cache.rs — metainfo + data entries over workingset cache, mergeSeries
  parse_cache.rs    — sharded parsed-AST cache
  memory_limiter.rs
esm-select/src/
  lib.rs / router.rs — path normalization, routing, concurrency limiter, slow-query log
  handlers/query.rs, query_range.rs, series.rs, labels.rs, label_values.rs, export.rs, stubs.rs
  netstorage/mod.rs — ProcessSearchQuery, Results/RunParallel, work-steal pool
  netstorage/tmp_blocks.rs — spill buffer/file
  netstorage/merge.rs — sortBlock, mergeSortBlocks, dedup glue
  json.rs           — hand-rolled Prometheus JSON writers (util.qtpl formatting rules)
  searchutil.rs     — Deadline, time/duration parsing, extra filters
```

**Stage 1 (TSBS-sufficient, in order):**
1. `esm-select::searchutil` + time parsing + Deadline. Verify: unit tests from
   lib/timeutil/timeutil_test.go + searchutil_test.go.
2. netstorage fetch/unpack/merge over esm-storage Search. Verify: netstorage_test.go
   (mergeSortBlocks cases) ported; integration against ingested sample data.
3. timeseries model + sorted-marshal key + shared grid.
4. rollup do_internal + {max,avg,min,sum,count,last,first,default,sum2}_over_time + rate/increase/delta.
   Verify: rollup_test.go table cases for these funcs (window edges, prevValue).
5. aggr_incremental (sum/min/max/avg/count/group) + evalRollupWithIncrementalAggregate.
6. eval dispatch for MetricExpr/RollupExpr/FuncExpr/AggrFuncExpr + general aggr sum/min/max/avg/count.
7. query_range + query handlers + JSON writers + concurrency limiter + /health + force_flush.
   Verify: end-to-end TSBS queries vs Go upstream output byte-compare (after `status/stats` field
   normalization).
8. rollup result cache + parse cache + AdjustStartEnd.
**Stage 2:** remaining rollup/transform/aggr funcs, binary ops beyond arithmetic/comparison,
group_left/right, subqueries (`evalRollupFuncWithSubquery`), `@` modifier, instant-rollup
optimizations, export formats, /federate, top_queries/active_queries.

**Tests worth porting** — `exec_test.go` (309KB) is gold: ~thousands of `f(q, resultExpected)`
cases running full `Exec` with `start=1000, end=2000, step=200` over synthetic data (`time()`
based series, no storage needed). Adapt: build a Rust harness `run_expr(q) -> Vec<Result>` with
the same EvalConfig and assert on values with `1e-13` relative tolerance (Go tests use
`testResultsEqual` with float comparison); port cases per function as each lands (grep the
function name). Also: rollup_test.go (rollupConfig.Do golden values — port wholesale for stage-1
funcs), aggr_incremental_test.go (update/merge equivalence vs the non-incremental reference —
property-testable in Rust: incremental result == general aggr result for random worker splits),
rollup_result_cache_test.go (Get/Put/merge windows), prometheus_test.go (adjustLastPoints).

**Concurrency design summary:** one query = one worker set of `n = min(cpus,32)` threads
(scoped threads or a fixed pool with per-query work queues + stealing); worker id → padded
per-worker state; incremental aggregation is lock-free until the single-threaded finalize merge;
deadline via coarse atomic clock; cancellation via AtomicBool. Rayon is acceptable for the
per-series map step (`for_each_init` with thread-local state keyed by
`current_thread_index()`), but the explicit steal-pool matches Go's behavior (bounded workers,
own-queue-first locality) more predictably under concurrent queries — prefer it.
