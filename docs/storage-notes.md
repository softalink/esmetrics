# esm-storage: Porting Spec for VictoriaMetrics lib/storage (v1.146.0)

Source analyzed: `/home/test/refsrc/VictoriaMetrics/lib/storage` (~17k lines, v1.146.0).
Target: Rust crate `esm-storage`, on top of `esm-mergeset` (lib/mergeset),
`esm-encoding` (lib/encoding), `esm-common` (decimal, uint64set, fs, bytesutil,
fasttime, workingsetcache/lrucache equivalents).

All timestamps are **int64 milliseconds** since Unix epoch (`rawRow.Timestamp`,
`TimeRange`, `partHeader.{Min,Max}Timestamp`, dedup intervals). `date` values are
`uint64(timestamp_ms / msecPerDay)`, `msecPerDay = 86_400_000`. Max allowed sample
timestamp: `maxUnixMilli = 9222422399999` (2262-03-31, time.go).

---

## 1. Big-picture dataflow

Write path:

```
Storage.AddRows([]MetricRow, precisionBits)
  └─ Storage.add(): per-row
       ├─ TSID lookup: tsidCache hit → (generation check) → per-partition indexDB
       │  (date,MetricName)->TSID → else create new TSID + index entries
       ├─ fill rawRow{TSID, Timestamp(ms), Value(f64), PrecisionBits}
       └─ table.MustAddRows → route rows to monthly partition(s)
            └─ partition.AddRows → rawRowsShards (per-CPU sharded buffers)
                 └─ flush (size- or deadline-triggered) →
                    rawRowsMarshaler.marshalToInmemoryPart:
                      sort rows by (TSID, Timestamp); group ≤8192 rows/TSID/block;
                      float64→decimal(int64 values, i16 scale);
                      encode timestamps/values via esm-encoding → inmemoryPart
                 └─ inmemory parts → merged → small file parts → big file parts
                    (background merge workers; dedup applied during merges)
```

Read path:

```
Search.Init(storage, tfss, tr, maxMetrics, deadline)
  └─ Storage.SearchTSIDs: per overlapping partition indexDB (+ legacy idbs):
       tag filters → metricIDs (uint64set) → TSID per metricID (cache→index) →
       merge + sort TSIDs
  └─ tableSearch.Init(tb, tsids, tr): heap over partitionSearch
       └─ partitionSearch: heap over partSearch (one per part; skips deleted
          metricIDs via idb.deletedMetricIDs)
            └─ partSearch: metaindex (in RAM) → indexBlock (zstd, cached) →
               blockHeaders → binary-search TSIDs → BlockRef
Search.NextMetricBlock: yields (MetricName, BlockRef) sorted by (TSID, MinTimestamp);
caller does BlockRef.MustReadBlock (reads timestampsData/valuesData at offsets)
then Block.UnmarshalData → ([]int64 timestamps, []int64 values, scale) →
decimal.AppendDecimalToFloat → []f64.
```

---

## 2. Core types and byte-exact layouts

Everything below uses `esm-encoding` big-endian fixed-width marshaling
(`MarshalUint64/32/16`; `MarshalInt64` stores `uint64(v)` big-endian — two's
complement bytes, no zig-zag; port exactly).

### 2.1 TSID (tsid.go)

```go
type TSID struct {
    MetricGroupID uint64 // xxhash64(mn.MetricGroup)
    JobID         uint32 // u32(xxhash64(mn.Tags[0].Value)) if any tag
    InstanceID    uint32 // u32(xxhash64(mn.Tags[1].Value)) if ≥2 tags
    MetricID      uint64 // globally unique counter
}
```

Marshaled: `MetricGroupID u64be | JobID u32be | InstanceID u32be | MetricID u64be`
= **24 bytes** (`marshaledTSIDSize`). Ordering (`TSID.Less`): lexicographic on
(MetricGroupID, JobID, InstanceID, MetricID) — this **is the physical sort key**
of blocks in parts; derive `Ord` on a field-ordered struct. `generateTSID`
(index_db.go:412) relies on canonical tag order (§2.2: job-like tag at Tags[0],
instance-like at Tags[1]). `generateUniqueMetricID()` = atomic counter seeded
with `UnixNano()` at startup (must not go backwards across restarts).

### 2.2 MetricName, tags, canonical form (metric_name.go)

```go
type Tag struct{ Key, Value []byte }
type MetricName struct {
    MetricGroup []byte
    Tags        []Tag // sorted canonically via sortTags()
}
```

Special chars in the index encoding:
`escapeChar = 0x00`, `tagSeparatorChar = 0x01`, `kvSeparatorChar = 0x02`.

`marshalTagValue(dst, src)`: append src with escapes
(`0x00→0x00 '0'`, `0x01→0x00 '1'`, `0x02→0x00 '2'`), then append `0x01`
terminator. `unmarshalTagValue` reverses (scan to first unescaped `0x01`).

`MetricName.Marshal` (canonical index form; **sortTags must be called first**):
`marshalTagValue(MetricGroup)` then for each tag
`marshalTagValue(Key) marshalTagValue(Value)`. `__name__` is not stored as a tag;
it *is* MetricGroup (empty key in tag->metricIDs rows denotes MetricGroup).

`sortTags()` canonicalization (metric_name.go:578): stable sort by
`normalizeTagKey(Key)`; duplicate keys deduped, **last value wins**.
`commonTagKeys` maps job-like keys to `\x00\x00..` sentinels
(`namespace`→`\x00\x00\x00`, `ns`→`..01`, `datacenter`→`..08`, `dc`→`..09`,
`environment`→`..0c`, `env`→`..0d`, `cluster`→`..10`, `service`→`..18`,
`job`→`..20`, `model`→`..28`, `type`→`..30`, `sensor_type`/`SensorType`→`..38`,
`db`→`..40`) and instance-like keys to `\x00\x01..` (`instance`→`\x00\x01\x00`,
`host`→`..08`, `server`→`..10`, `pod`→`..18`, `node`→`..20`, `device`→`..28`,
`tenant`→`..30`, `client`→`..38`, `name`→`..40`, `measurement`→`..48`);
Capitalized variants map identically. **Do not change sentinel bytes** — they
control disk locality and Tags[0]/Tags[1] → JobID/InstanceID. (TSBS's
`hostname` is NOT in the map.)

`MetricNameRaw` wire form (`MarshalMetricNameRaw`, used in `MetricRow` and as
tsidCache key): sequence of `(u16be len | bytes)` pairs `key,value`; `__name__`
key encoded as empty; labels with empty value are skipped. Test-only
`marshalRaw` prepends an empty `(0len,0len)` pair — ignore.

### 2.3 rawRow (raw_row.go)

```go
type rawRow struct {
    TSID          TSID
    Timestamp     int64   // ms
    Value         float64
    PrecisionBits uint8   // [1..64]; 64 = lossless
}
```

Sort comparator (rawRowsSort): (TSID fields..., Timestamp). PrecisionBits comes
from `Storage.AddRows(mrs, precisionBits)` — TSBS/vminsert default is 64.

### 2.4 Block, blockHeader (block.go, block_header.go)

Constants: `maxRowsPerBlock = 8*1024 = 8192`; `maxBlockSize = 8*maxRowsPerBlock =
64 KiB` (max size of a values byte-block; validation allows 2x).

```go
type blockHeader struct {
    TSID                  TSID
    MinTimestamp          int64  // first ts (rows sorted by ts)
    MaxTimestamp          int64
    FirstValue            int64  // first decimal value (for delta decoding)
    TimestampsBlockOffset uint64 // offset in timestamps.bin
    ValuesBlockOffset     uint64 // offset in values.bin
    TimestampsBlockSize   uint32
    ValuesBlockSize       uint32
    RowsCount             uint32 // ≥1
    Scale                 int16  // decimal 10^Scale multiplier
    TimestampsMarshalType encoding.MarshalType // 1 byte
    ValuesMarshalType     encoding.MarshalType // 1 byte
    PrecisionBits         uint8
}
```

Marshal order = struct order above: `TSID(24) | i64 | i64 | i64 | u64 | u64 |
u32 | u32 | u32 | i16 | 3 single bytes` = **85 bytes** fixed. Validation:
RowsCount ∈ (0, 2*8192]; block sizes ≤ 2*maxBlockSize; marshal types ∈ [1..6];
precisionBits ∈ [1..64].

`Block` holds `bh` + either unpacked `timestamps/values []int64` or packed
`timestampsData/valuesData []byte`.
- `MarshalData(tsOff, vOff)` (block.go:189): `encoding.MarshalValues(values,
  precisionBits)` → (valuesData, ValuesMarshalType, FirstValue);
  `encoding.MarshalTimestamps(...)` → (timestampsData, TimestampsMarshalType,
  MinTimestamp); MaxTimestamp = last ts; RowsCount = len. Returns
  (headerData(85B), timestampsData, valuesData).
- `UnmarshalData` (block.go:250): `UnmarshalTimestamps`; if PrecisionBits < 64
  → `EnsureNonDecreasingSequence(timestamps, MinTs, MaxTs)` (repair after lossy
  compression); else if marshal type NeedsValidation (5/6) →
  `checkTimestampsBounds` (ordered, within [MinTs,MaxTs]). Then
  `UnmarshalValues`.
- Merge-time dedup hook: `deduplicateSamplesDuringMerge` unpacks then runs the
  int64 dedup (§7) with the global interval.

encoding MarshalType values (esm-encoding must match): `1=ZSTDNearestDelta2`
(counters), `2=DeltaConst`, `3=Const`, `4=ZSTDNearestDelta` (gauges),
`5=NearestDelta2`, `6=NearestDelta` (5/6 = non-zstd fallbacks when compression
doesn't help).

Decimal rules (esm-common): `decimal.AppendFloatToDecimal([]f64) -> ([]i64,
i16 scale)` converts a whole block to one scale; sentinels: `vInfPos=2^63−1`,
`vInfNeg=−2^63`, `vStaleNaN=2^63−2`, `vMax=2^63−3`, `vMin=−2^63+2` (NaN →
StaleNaN, ±Inf → vInf*). Reverse: `AppendDecimalToFloat(values, scale)`.
`PrecisionBits` additionally rounds mantissas inside `MarshalValues`
(NearestDelta encodings); 64 = exact.

### 2.5 Part files & metaindexRow (metaindex_row.go, part_header.go, filenames.go)

Part directory files: `timestamps.bin`, `values.bin`, `index.bin`,
`metaindex.bin`, `metadata.json` (+ partition-level `parts.json` listing live
parts, and legacy per-part `min_dedup_interval` file).

`index.bin` = sequence of **index blocks**: each is ZSTD-compressed
concatenation of consecutive 85-byte blockHeaders (uncompressed group ≤
`maxBlockSize` bytes; headers sorted by TSID). `indexBlock` type in memory is
just `{ bhs: Vec<blockHeader> }`.

`metaindex.bin` = ZSTD-compressed sequence of metaindexRows (whole file
decompressed and kept in RAM per part):

```go
type metaindexRow struct {
    TSID              TSID   // first TSID in the index block
    MinTimestamp      int64
    MaxTimestamp      int64
    IndexBlockOffset  uint64
    BlockHeadersCount uint32 // >0
    IndexBlockSize    uint32 // compressed, ≤ 2*maxBlockSize
}
```

**Marshal order differs from struct order**: `TSID(24) | BlockHeadersCount u32 |
MinTimestamp i64 | MaxTimestamp i64 | IndexBlockOffset u64 | IndexBlockSize u32`
= 56 bytes. Rows sorted by TSID.

`metadata.json` = JSON of partHeader:

```go
type partHeader struct {
    RowsCount, BlocksCount        uint64
    MinTimestamp, MaxTimestamp    int64
    MinDedupInterval              int64 // ms
}
```

(serde_json, exact Go field names `RowsCount` etc.) Pre-v1.90 fallback parsing
of the part dir name — PORT-SKIP (we always write metadata.json).

### 2.6 blockStreamWriter / blockStreamReader (identical-timestamps trick)

Writer (block_stream_writer.go): sequentially receives blocks in TSID order
(`WriteExternalBlock`): applies merge dedup, `MarshalData`, then —
**timestamps block sharing**: if the marshaled `timestampsData` is byte-equal
to the previous block's, re-marshal header pointing at
`prevTimestampsBlockOffset` and skip writing timestamps (big disk win when many
series share scrape timestamps; TSBS benefits directly). Header bytes are
appended to `indexData`; when `len(indexData)+85 > maxBlockSize` → flush:
zstd-compress indexData → index.bin, emit metaindexRow (mr accumulates
min/max ts + count via `RegisterBlockHeader`). On close: flush + write
zstd-compressed metaindex. Compress level: -5 for in-memory parts (raw_row.go),
otherwise from part size (agent: `getCompressLevel`, partition.go).

Reader (block_stream_reader.go): mirror image; validates monotonic TSID order,
offsets continuity, per-part rows/blocks counts; reconstructs shared timestamp
blocks via `TimestampsBlockOffset == prevTimestampsBlockOffset`.

---

## 3. IndexDB (index_db.go — the heart)

### 3.0 Architecture in v1.146: per-partition indexDBs + legacy read-only idbs

**Important (differs from older upstream docs):** in v1.146 each monthly partition
owns its own `indexDB` (`partition.idb`; mergeset table at
`data/indexdb/<YYYY_MM>`; `indexDB.tr` = partition month; `id` used in cache
keys). There is **no retention-based prev/curr/next rotation anymore** —
a new month ⇒ a new partition+indexDB; dropping an expired partition drops its
indexDB. `legacyIndexDBs` (prev/curr, refcounted, read-only
`noRegisterNewSeries=true`, old `<root>/indexdb/<16-hex-generation>` layout)
are still searched for pre-migration data and rotated on the old retention
schedule (storage_legacy.go). Green-field port: **skip legacy idbs**, but keep
search paths iterating `Vec<Arc<IndexDb>>`. `legacyContainsTimeRange`
(index_db_legacy.go:90) caches per-key min-missing-timestamp to skip seeks in
read-only idbs whose data ends before `tr.MinTimestamp`.

indexDB fields to port: `id u64`, `tr`, `name`, `tb *mergeset.Table`,
`tagFiltersToMetricIDsCache` (lrucache), `loopsPerDateTagFilterCache`
(workingsetcache), `metricIDCache`, `dateMetricIDCache`, `deletedMetricIDs`
(atomic Arc<uint64set>), `noRegisterNewSeries`, stats counters. The mergeset
table is opened with `prepareBlock = mergeTagToMetricIDsRows` (§3.3) and with
`tagFiltersToMetricIDsCache.Reset` as the flush callback (cache invalidated
whenever newly ingested items become searchable).

### 3.1 Key namespaces (all keys start with 1 prefix byte)

`commonPrefixLen = 1`. Values are part of the mergeset **item** (mergeset
stores whole items; "key/value" split is by convention).

| ns | const | item layout |
|----|-------|-------------|
| 0 | `nsPrefixMetricNameToTSID` | `0x00 | mn.Marshal() | 0x02(kvSep) | TSID(24)` — **only when -disablePerDayIndex** (skip in port or gate behind option) |
| 1 | `nsPrefixTagToMetricIDs` | `0x01 | marshalTagValue(key) | marshalTagValue(value) | metricID u64be × N (N ≤ 64)` — global tag index. MetricGroup row uses `key=nil` (i.e. `0x01` sep only) and `value=MetricGroup`. |
| 2 | `nsPrefixMetricIDToTSID` | `0x02 | metricID u64be | TSID(24)` |
| 3 | `nsPrefixMetricIDToMetricName` | `0x03 | metricID u64be | mn.Marshal()` |
| 4 | `nsPrefixDeletedMetricID` | `0x04 | metricID u64be` |
| 5 | `nsPrefixDateToMetricID` | `0x05 | date u64be | metricID u64be` |
| 6 | `nsPrefixDateTagToMetricIDs` | `0x06 | date u64be | marshalTagValue(key) | marshalTagValue(value) | metricID u64be × N` |
| 7 | `nsPrefixDateMetricNameToTSID` | `0x07 | date u64be | mn.Marshal() | 0x02 | TSID(24)` |

Tag rows (1 and 6) also get **artificial keys**:
- reverse-Graphite: key = `graphiteReverseTagKey = 0xff`, value = reversed
  MetricGroup (only when MetricGroup contains `.`) — optional for TSBS,
  but note TSBS-influx metric names contain no dots → never emitted anyway.
- composite: key = `0xfe(compositeTagKeyPrefix) | varuint(len(name)) | name |
  tagKey`, value = tag value. Emitted for **every tag** alongside plain rows
  (`registerTagIndexes`, index_db.go:2763). These make `{__name__="m",k="v"}`
  lookups a single seek. **Required** — the query path converts filters to
  composite form whenever `tr.MinTimestamp >= s.minTimestampForCompositeIndex`.

### 3.2 Index entry creation

On new series (`createGlobalIndexes`, per-idb): metricIDCache.Set;
items: ns3 metricID→metricName, ns2 metricID→TSID, ns1 tag rows (metric group
row, per-tag rows, composite rows[, reverse graphite]).
Per (date, series) (`createPerDayIndexes`): dateMetricIDCache.Set(date,mid);
items: ns5 (date,metricID), ns7 (date,mn)→TSID, ns6 date-tag rows (same
registerTagIndexes with date-carrying prefix). Ingestion consults
`hasDateMetricID`/`hasMetricID` (cache → `FirstItemWithPrefix` seek) to decide
whether to (re)create entries.

### 3.3 mergeset item-merge callback: `mergeTagToMetricIDsRows`

Rows in ns1/ns6 sharing an identical `(prefix, tag)` are merged during mergeset
part merges: concatenate + sort + dedupe metricIDs, re-emit rows capped at
`maxMetricIDsPerRow = 64` ids each. First and last items of a block are never
merged (block boundary invariants). If the merged output would become unsorted
(possible with duplicate ids across items) — revert to originals. Port this as
the `prepare_block` hook of esm-mergeset. Counters:
`indexBlocksWithMetricIDsProcessed/IncorrectOrder`.

`tagToMetricIDsRowParser`: parses ns1/ns6 items (ns6 carries `Date`), exposes
`MetricIDsLen() = len(tail)/8`, lazy `ParseMetricIDs`, and
`GetMatchingSeriesCount(filter, dmis)`.

### 3.4 TagFilters → metricIDs search

`TagFilters { tfs: Vec<tagFilter>, commonPrefix = [0x01] }`. `tagFilter`:

```go
type tagFilter struct {
    key, value []byte
    isNegative, isRegexp bool
    matchCost uint64            // fullMatch=1,prefix=2,literal=3,suffix=4,middle=6,re=100
    regexpPrefix string
    prefix []byte               // {ns[,date]} + marshalTagValue(key) + escaped(prefix-of-value), no trailing 0x01
    orSuffixes []string         // regexp expanded to alternation of literals ("" for exact match)
    reSuffixMatch func([]byte) bool
    isEmptyMatch bool           // filter matches empty value
    graphiteReverseSuffix []byte
}
```

`TagFilters.Add` normalizations: empty value ⇒ negate + regexp `.+`;
`=~".*"` dropped; `!~".*"` ⇒ `!~".+"`; negative-with-empty-match (`key!~"|foo"`)
adds companion `key=~".+"` filter. Regexp handling: `simplifyRegexp` →
(literal prefix, remaining expr); if expr empty ⇒ plain-value filter;
`GetOrValuesPromRegex` extracts finite alternations into `orSuffixes` (fast
seek path); otherwise optimized matchers for `literal.*`/`.*literal.*` etc.
(getOptimizedReMatchFunc); regexp/prefix results cached in two lrucaches
(5% of mem each). Escaping regexps for index chars via `tagCharsRegexpEscaper`.

Composite conversion (`convertToCompositeTagFilterss`, tag_filters.go:48):
if a filter set has a plain `__name__="m"` (or `__name__=~"a|b"` with
orSuffixes) plus ≥1 positive non-empty filter, rewrite each other filter
`{k op v}` into `{composite(m,k) op v}` (key = marshalCompositeTagKey) and drop
the name filter. One TagFilters per alternated name.

Top-level per-idb search (`searchMetricIDs`):
1. Cache: key = `marshalTagFiltersKey` = `startDate u64be | endDate u64be |
   (0x00 | tf.Marshal()* )*` where tf.Marshal = tagValue(key)+tagValue(value)+
   isNegative byte+isRegexp byte; lookup in `tagFiltersToMetricIDsCache`
   (lrucache of `Arc<uint64set>`, size = mem/32 by default). Hit ⇒ done
   (check `len > maxMetrics`).
2. Miss ⇒ `searchMetricIDsInternal`: for each TagFilters group (empty group ⇒
   `{__name__!=""}`), `updateMetricIDsForTagFilters` unions into the result;
   error `errTooManyTimeseries` if > maxMetrics.
3. Subtract `deletedMetricIDs`. Store to cache.

`updateMetricIDsForTagFilters`: if tr != global ⇒ per-day path: for each date
in `tr.DateRange()` (parallel goroutines when >1 day) run
`getMetricIDsForDateAndFilters(date, tfs, maxMetrics)`; else global path uses
`globalIndexDate=0` (ns1 prefix instead of ns6).

`getMetricIDsForDateAndFilters` (index_db.go:2513) — **the core planner**:
- For each tf load `(loopsCount, filterLoopsCount, timestamp)` from
  `loopsPerDateTagFilterCache`; key = `idb.name | date u64be | tf.Marshal()`,
  value = `i64 loopsCount | i64 filterLoopsCount | u64 unixSec` (24 bytes).
  Stats older than 1h are zeroed if ≤ 10e6 (retry cheap filters hourly).
- Sort tfs ascending by loopsCount (ties: `tagFilter.Less` — composite first,
  lower matchCost first, non-regexp first, fewer orSuffixes, positive first,
  prefix bytes).
- Phase 1: pick the first positive, non-empty-match filter; run
  `getMetricIDsForDateTagFilter` with `maxLoopsCount` = next filter's stats and
  `maxDateMetrics = min(intMax, maxMetrics*50)`. `errTooManyLoops` ⇒ store
  2×loops, postpone filter. Result ≥ maxDateMetrics ⇒ postpone (store
  int64Max-1). If nothing suitable ⇒ `getMetricIDsForDate` (scan
  `(date,__name__=…)` rows via prefix `ns6|date|tagValue(nil)`); if that also
  hits maxDateMetrics ⇒ errTooManyTimeseries.
- Phase 2: re-sort remaining by filterLoopsCount; for each: if
  `filterLoopsCount > len(metricIDs)*loopsCountPerMetricNameMatch(=150)` ⇒
  postpone the rest to metricName matching. Else run the filter with
  maxLoopsCount = next stats (or len*150); negative/empty-match ⇒
  `metricIDs.Subtract(m)`, positive ⇒ `Intersect(m)`.
- Postponed filters: `updateMetricIDsByMetricNameMatch` — for each remaining
  metricID (sorted) fetch metricName (cache→ns3), `matchTagFilters` directly
  (composite filters are first unfolded via `removeCompositeTagFilters`).
  Failed tf is swapped to tfs[0] as a cheap MRU heuristic.

`getMetricIDsForDateTagFilter`: clone tf with prefix rebased onto
`marshalCommonPrefixForDate` (ns1 for date==0, else ns6|date), isNegative
forced false; then `getMetricIDsForTagFilter`:
- orSuffixes fast path (`updateMetricIDsForOrSuffixes`): for each suffix seek
  `prefix+suffix+0x01` and consume all rows with that exact prefix
  (`AddMulti` metricIDs); loopsCount += ids per row; abort on maxLoopsCount /
  maxMetrics.
- slow path (`getMetricIDsForTagFilterSlow`): scan all rows under `tf.prefix`;
  per row parse suffix (up to and incl. 0x01) and tail metricIDs;
  `loopsCount += MetricIDsLen`, `matchSuffix` memoized against previous suffix,
  regexp match cost `+= 10*matchCost`; on non-match with full row
  (≥ maxMetricIDsPerRow/2 ids) **seek-skip** to next tag value by incrementing
  the trailing 0x01 byte (loopsCount += 1000 per seek). errTooManyLoops when
  loopsCount > maxLoopsCount.
- empty-match positive filters (`{foo=~"bar|"}`) get special handling: result =
  metricIDs(`key=~".+"`) − metricIDs(tf), computed with tfGross.

Pace limiting: every 2^16 (fast) / 2^14 (medium) / 2^12 (slow) iterations check
`deadline` (unix seconds; `noDeadline = u64::MAX`). Port as simple counter
masks + `fasttime`-style cached clock.

### 3.5 TSID resolution & self-healing

`SearchTSIDs(tfss, tr, maxMetrics, deadline)`: metricIDs → for each:
`Storage.getTSIDByMetricIDFromCache` (Storage.metricIDCache, shared across
idbs; key = 8-byte metricID, value = raw TSID — metricIDs are globally unique
so no generation tag is needed here; the *tsidCache* value keeps an 8-byte
ignored padding where the generation used to live, §6.2) → miss ⇒ ns2 seek in
the partition idb. If ns2 misses and
`Storage.wasMetricIDMissingBefore(metricID)` (missing for >60s) ⇒ metricID is
added to a delete-set and persisted as ns4 (self-healing of corrupted index
after unclean shutdown). Result sorted by TSID. Same pattern in
`SearchMetricNames` for ns3 misses.

Deleted series: `DeleteSeries` = global-index search → union into in-memory
`deletedMetricIDs` (copy-on-write Arc<uint64set> under update mutex), reset
tagFilters cache, then persist ns4 rows. Readers: `getDeletedMetricIDs()`
filters in GetMatchingSeriesCount / searchMetricIDs / partitionSearch.Init.

### 3.6 Per-idb caches

- `tagFiltersToMetricIDsCache`: lrucache (esm-common: LRU with per-entry
  SizeBytes), default cap `mem/32`, key = marshalTagFiltersKey, value =
  Arc<uint64set>. Reset on mergeset flush + on DeleteSeries.
- `loopsPerDateTagFilterCache`: workingsetcache (fastcache with prev/curr
  rotation), cap `mem/128`; see §3.4 for entry layout.
- `metricIDCache` (metric_id_cache.go): membership set of metricIDs known to
  exist in **this** idb; ingestion-only. 16 shards (shard =
  `(metricID/65536)%16`, cache-line padded); each shard holds
  `curr: ArcSwap<Uint64Set>` (lock-free reads), `next` (mutable, mutex),
  `prev`; `Has` slow path migrates prev→next and syncs next→curr when
  `slowHits > (curr.len+next.len)/2`; a rotator thread rotates one shard per
  ~1min tick (prev=curr; curr=next; next=∅).
- `dateMetricIDCache` (date_metric_id_cache.go): same 16-shard
  curr/next/prev scheme but over `byDateMetricIDMap { hotEntry (last date),
  m: HashMap<date, Uint64Set> }`; rotation ~1h/shard; sync keeps only the two
  newest dates plus dates written recently. Ingestion-only.
Both are in-memory only. Exact eviction fidelity is a memory concern, not a
correctness one — port the two-generation shape, tune later.

---

## 4. Table / partitions / parts / merges (table.go, partition.go)

### 4.1 Directory layout & structs

```
<root>/data/
  small/<YYYY_MM>/<%016X mergeIdx>/   ← small file parts
  big/<YYYY_MM>/<%016X>/              ← big file parts
  indexdb/<YYYY_MM>/                  ← per-partition indexDB (mergeset table)
  {small,big,indexdb}/snapshots/      ← snapshot hardlink dirs (skip)
```

`table { path, smallPartitionsPath, bigPartitionsPath, indexDBPath, s *Storage,
ptws []*partitionWrapper + ptwsLock, stopCh, retentionWatcherWG, forceMergeWG,
historicalMergeWatcherWG }`. `partitionWrapper { refCount atomic.Int32,
mustDrop atomic.Bool, pt *partition }` — decRef at 0 closes the partition and,
if mustDrop, deletes its three dirs.

`partition` key fields (partition.go:75): per-type atomic merge counters;
`isDedupScheduled`; `mergeIdx atomic.Uint64` (seeded UnixNano, `%016X` part dir
names); `smallPartsPath, bigPartsPath, indexDBPartsPath`; `name "2006_01"`,
`tr TimeRange` (whole month, ms-inclusive: `[m0, m1-1]`); `rawRows
rawRowsShards`; `partsLock sync.Mutex` guarding `inmemoryParts/smallParts/
bigParts []*partWrapper`; `idb *indexDB`; `stopCh`, `wg`.
`partWrapper { refCount, mustDrop, p *part, mp *inmemoryPart (nil for file
parts), isInMerge bool (guarded by partsLock), flushToDiskDeadline time }`;
decRef at 0: return mp to pool / close files / delete dir if mustDrop.
`part { ph partHeader, path, size u64, timestampsFile/valuesFile/indexFile
(pread handles), metaindex []metaindexRow (in RAM) }`. Index-block cache
`ibCache` = blockcache sized `0.1*memory.Allowed()`, entries evicted per part
on part close.

### 4.2 Row routing (table.MustAddRows, table.go:300)

Fast path: all rows fit one existing partition (MRU: move it to ptws[0]).
Slower: bucket rows per partition by `pt.HasTimestamp` (= tr.contains).
Slowest: rows with no partition — clamp to `getMinMaxTimestamps()`
(`min = now − retentionMsecs`, `max = min(maxUnixMilli, now +
futureRetentionMsecs)`; rows outside silently dropped with counters at
Storage.add level) and `mustCreatePartition` under ptwsLock (re-check first).

### 4.3 rawRowsShards (ingestion buffering)

- `rawRowsShardsPerPartition = cgroup.AvailableCPUs()` shards, cache-line
  padded; round-robin via atomic counter.
- `maxRawRowsPerShard = (8<<20)/sizeof(rawRow)` (≈8 MiB per shard buffer;
  sizeof(rawRow)=40 ⇒ ~209k rows).
- `pendingRowsFlushInterval = 2s` (shard flush deadline; flusher ticker),
  `dataFlushInterval = 5s` (inmemory-part → disk deadline; settable, ≥2s).
- Full shard buffer ⇒ moved to `rowssToFlush`; when
  `len(rowssToFlush) >= defaultPartsToMerge (15)` ⇒ immediate
  `flushRowssToInmemoryParts`. A 2s-tick `pendingRowsFlusher` flushes on
  deadline; `flushPendingRows(isFinal=true)` on close.
- `flushRowssToInmemoryParts`: each rows-slice → `createInmemoryPart`
  (concurrency-gated), then repeatedly merge the resulting inmemory parts
  (`getPartsForOptimalMerge`, no size cap) until one remains or parts exceed
  `getMaxInmemoryPartSize()`; register in `pt.inmemoryParts`.

### 4.4 Part types & size thresholds

`partType: inmemory=0, small=1, big=2`. `getDstPartType(pws, isFinal)`:
size > `getMaxSmallPartSize()` ⇒ big; `isFinal ||` size >
`getMaxInmemoryPartSize()` ⇒ small; any file-part input ⇒ small; else inmemory.
- `maxBigPartSize = 1e12`; `maxInmemoryParts = 60`.
- `getMaxInmemoryPartSize() = max(0.1*memory.Allowed()/60, 1e6)`.
- `getMaxSmallPartSize() = min(max(memory.Remaining()/15, 10e6),
  getMaxOutBytes(smallPath, smallConcurrency))`.
- `getMaxBigPartSize() = getMaxOutBytes(bigPath, 4)`;
  `getMaxOutBytes = min(freeSpace/workers, maxBigPartSize)`.

### 4.5 Merge scheduling & concurrency

Global (process-wide) semaphore channels: inmemory = `AvailableCPUs()`,
small = `max(AvailableCPUs(),4)`, big = `max(AvailableCPUs(),4)`. Per
partition, `cap(ch)` merger goroutines per type run a loop: skip if readOnly;
under partsLock `getPartsToMerge(list, maxOutBytes=getMaxBigPartSize())`
(filters isInMerge, marks selected); acquire slot; `mergeParts`.

`appendPartsToMerge(dst, src, maxPartsToMerge=15, maxOutBytes)`: drop parts
larger than `maxOutBytes/1.7 (minMergeMultiplier)`; sort by (size asc,
MinTimestamp desc); exhaustive O(N²) window scan sizes
`[max((15+1)/2,2)..15]`, skipping unbalanced windows
(`a[0].size*len < a[last].size`) and windows exceeding maxOutBytes; maximize
`m = outSize/lastSize`; require `m ≥ max(15/2, 1.7) = 7.5` else no merge.
Same shape as esm-mergeset's picker — share the implementation if generic.

`inmemoryPartsFlusher` (5s tick): parts whose `flushToDiskDeadline` passed →
`mergePartsToFiles(..., isFinal=true)` (writes small parts). `stalePartsRemover`
(~7min jittered tick): drop parts with `ph.MaxTimestamp < now−retention`.
`ForceMergeAllParts`: all parts (only when no merges active), checked against
free disk, used by final dedup & the API.

### 4.6 mergeParts flow

1. `dstPartType`, `mergeIdx`, `dstPartPath`. Fast path: single inmemory part,
   final, dedup off ⇒ `mp.MustStoreToDisk(dstPartPath)` directly.
2. Open a blockStreamReader per source part; `compressLevel =
   getCompressLevel(srcRows/srcBlocks)`: ≤10→-5, ≤50→-2, ≤200→-1, ≤500→1,
   ≤1000→2, else 3. Writer nocache=(dst is big part).
3. `mergeBlockStreams(ph, bsw, bsrs, stopCh, dmis, retentionDeadline, …)`:
   heap of readers ordered by (TSID.MetricID fast path, MinTimestamp | TSID) —
   per output block: skip if `dmis.Has(MetricID)` (rowsDeleted += count); skip
   if `bh.MaxTimestamp < retentionDeadline`; accumulate into `pendingBlock`:
   distinct MetricID ⇒ flush pending; same TSID & pending `tooBig()` &
   non-overlapping ⇒ flush; else **slow path**: unmarshal both,
   `decimal.CalibrateScale` to common scale, `PrecisionBits = min(a,b)`,
   `mergeBlocks` interleaves by timestamp (equal timestamps NOT deduped here —
   dedup happens in WriteExternalBlock), `skipSamplesOutsideRetention` per
   sample; if merged > 8192 rows emit first 8192 and keep tail pending.
4. `ph.MinDedupInterval = GetDedupInterval()` stamped into metadata.json;
   `swapSrcWithDstParts` under partsLock (+ rewrite `parts.json` =
   `{"Small":[names],"Big":[names]}`, atomic write), mark old parts mustDrop,
   decRef, kick a merger for the dst type.
5. Merge errors other than `errForciblyStopped` (shutdown) are FATAL panics.

parts.json is authoritative on open: unknown part dirs are removed (unclean
shutdown debris); missing listed parts ⇒ panic.

### 4.7 Retention, final dedup, snapshots

- `table.retentionWatcher` (jittered 1min): drop whole partitions with
  `tr.MaxTimestamp < now−retention` (or beyond future retention) —
  scheduleToDrop + decRef (deferred until searches release refs).
- Per-part `stalePartsRemover` + per-merge block/sample retention filtering
  (§4.6) enforce retention inside live partitions.
- Final dedup: `table.historicalMergeWatcher` (jittered ~1h,
  `finalDedupScheduleInterval`), only when dedup enabled: for every partition
  except the current month, if `GetDedupInterval() >
  min(parts.MinDedupInterval)` ⇒ `isDedupScheduled=true` +
  `ForceMergeAllParts`.
- Snapshots: hardlink-based (`MustCreateSnapshotAt` after
  `flushInmemoryRowsToFiles`) — **PORT-SKIP** for TSBS; nothing else depends
  on them.
- Close order (partition.MustClose): close stopCh under partsLock → wg.Wait →
  flush pending rows (final) → flush inmemory parts to files (final) → assert
  empty → decRef small/big parts (must hit 0) → close idb.
- Read-only mode: mergers exit; `NotifyReadWriteMode` restarts them when disk
  space returns.

---

## 5. Search path details (search.go, *_search.go)

- `Search.Init`: `retentionDeadline = now_ms − retentionMsecs`; get
  `metricNameSearch` (snapshot of partition + legacy idbs for tr); then
  `SearchTSIDs`; then `tableSearch.Init(tb, tsids, tr)` (clamps tr.MinTimestamp
  to retention). Returns #tsids.
- `tableSearch`: `GetAllPartitions` snapshot (refcounted wrappers, released in
  MustClose); one `partitionSearch` each; k-way merge heap ordered by
  `blockHeader.Less` (TSID, then MinTimestamp). `nextBlockNoop` pattern: Init
  positions on the first block; first NextBlock is a no-op.
- `partitionSearch.Init`: skip if `!pt.tr.overlapsWith(tr)`; filter tsids
  against `pt.idb.deletedMetricIDs`; snapshot parts; heap over `partSearch`.
- `partSearch`: holds sorted `tsids`, cursor `tsidIdx`; part-level pruning by
  `ph.MinTimestamp/MaxTimestamp` vs tr. Iterates metaindex rows: skip rows
  whose [MinTs,MaxTs] misses tr; `skipTSIDsSmallerThan` advances the tsid
  cursor with binary search; `skipSmallMetaindexRows` binary-searches metaindex
  and **backs up one row** (a tsid may live in the previous row's block range).
  Index block fetch: `ibCache` (lib/blockcache keyed by (part ptr, offset) —
  port as sharded LRU keyed by (part_id, offset)) else read+zstd-decompress+
  `unmarshalBlockHeaders`. Within `bhs`: binary search to current tsid; linear
  scan for time-range overlap (blocks of one TSID sorted by MinTimestamp but
  may overlap); emit BlockRef{part, bh}.
- `Search.NextMetricBlock`: dedupes per-metricID work (`prevMetricID`), applies
  `retentionDeadline` (skip blocks with `MaxTimestamp < deadline`), resolves
  MetricName via metricNameSearch (partition idbs then legacy; sparse-cache
  mode for exports skips the shared metricNameCache), deadline check every 2^12
  blocks. Caller (vmselect equivalent) then `MustReadBlock` (2 pread calls) and
  `UnmarshalData`, `AppendRowsWithTimeRangeFilter(dstTs, dstVals, tr)` which
  linear-trims to [tr.Min, tr.Max] and converts decimal→f64.
- `MetricBlock`/`BlockRef.Marshal` wire helpers are for the cluster protocol —
  optional.

---

## 6. Storage struct & lifecycle (storage.go)

### 6.1 Struct essentials (storage.go:42)

`Storage { path, cachePath, retentionMsecs, futureRetentionMsecs,
denyQueriesOutsideRetention, flockF, legacyIndexDBs
atomic.Pointer<legacyIndexDBs>, disablePerDayIndex, tb *table,
hourly/dailySeriesLimiter (bloomfilter — PORT-SKIP), tsidCache, metricIDCache,
metricNameCache (workingsetcache), currHourMetricIDs/prevHourMetricIDs
atomic.Pointer<hourMetricIDs>, nextDayMetricIDs atomic.Pointer,
pendingHourEntries + lock, pendingNextDayMetricIDs + lock, stopCh, 4 worker
WaitGroups, minTimestampForCompositeIndex, missingMetricIDs map + reset
deadline, isReadOnly atomic.Bool, metricsTracker, idbPrefillStartSeconds,
logNewSeries, metadataStorage }` + ingest counters (tooSmall/tooBigTimestamp,
slowRowInserts, slowPerDayIndexInserts, newTimeseriesCreated,
timeseriesRepopulated…).

`MetricRow { MetricNameRaw []byte, Timestamp int64, Value float64 }`.
`OpenOptions { Retention, FutureRetention, DenyQueriesOutsideRetention,
MaxHourlySeries, MaxDailySeries, DisablePerDayIndex, TrackMetricNamesStats,
IDBPrefillStart, LogNewSeries }`. Retention clamped to (0, 100y]; future
retention default `2*24h`.

### 6.2 Open sequence (MustOpenStorage, exact order)

mkdir root → honor `cache/reset_cache_on_startup` (wipe cache contents) →
`flock.lock` exclusive → mkdir `snapshots` → load caches from `cache/`
(`workingsetcache.Load(file, size)`): tsidCache `metricName_tsid`
(**0.37×mem**), metricIDCache `metricID_tsid` (mem/16), metricNameCache
`metricID_metricName` (mem/10) → `metadata/minTimestampForCompositeIndex`
(8-byte i64; fresh DB ⇒ 0, upgrade ⇒ (today+2)*msecPerDay) → open legacy idbs
from `<root>/indexdb/<16-hex>` (keep last 2, drop "next"; PORT-SKIP) →
free-disk check sets isReadOnly → `mustOpenTable(path/data)` (opens all
partitions + their idbs; reads parts.json) → propagate legacy deleted
metricIDs into all partition idbs → load hour caches
(`curr/prev_hour_metric_ids_v2`: `u64 hour | uint64set.Marshal`; discarded if
hour stale) and next-day cache (`next_day_metric_ids_v3`: `u64 date | set`)
→ start workers.

Cache value encodings (unsafe struct-bytes in Go; in Rust use explicit LE/
native fixed layouts — these caches are node-local, format is ours to choose,
but keep sizes):
- tsidCache: key = MetricNameRaw, value = `legacyTSID { TSID, _pad u64 }`
  (trailing 8 bytes = former indexdb generation, now ignored padding).
- metricIDCache: key = 8 bytes metricID, value = TSID struct bytes.
- metricNameCache: key = 8 bytes metricID, value = `mn.Marshal()`.

`MustClose`: close stopCh → wait 4 worker WGs → `tb.MustClose()` (asserts each
ptw refCount==1) → close legacy idbs → save+stop the 3 workingset caches →
save hour/nextday caches → close flock. Cache saves go through a global
`saveCacheLock`.

### 6.3 Background workers (storage-level)

| worker | interval | job |
|---|---|---|
| currHourMetricIDsUpdater | ~10s jittered | swap pendingHourEntries into currHourMetricIDs (clone+union if same hour; new hour ⇒ rotate curr→prev; hour%24==0 ⇒ don't carry pending) |
| nextDayMetricIDsUpdater | ~11s jittered | fold pendingNextDayMetricIDs into nextDayMetricIDs{idbID, date, set} |
| legacyRetentionWatcher | at legacy rotation deadline | rotate legacy idbs (PORT-SKIP) |
| freeDiskSpaceWatcher | ~1s jittered | isReadOnly = freeSpace < SetFreeDiskSpaceLimit(); on RW transition `NotifyReadWriteMode` restarts mergers |

Plus per-idb cache rotators (metricIDCache ~1min/shard, dateMetricIDCache
~1h/shard) and per-partition flushers/mergers (§4).

### 6.4 add() ingestion path (storage.go:1874)

`AddRows` splits into ≤8000-row blocks (`maxMetricRowsPerBlock`, pooled ctx).
Per row in `add()`:
1. Value: reject NaN unless `decimal.IsStaleNaN`.
2. Timestamp: reject outside `[now−retention, now+futureRetention]`
   (tooSmall/tooBigTimestampRows counters, warn-throttled 5s).
3. Partition switch when `!ptw.pt.HasTimestamp(ts)` ⇒ get partition + its idb
   + indexSearch + deletedMetricIDs snapshot.
4. TSID resolution, in order:
   a. same MetricNameRaw as previous row ⇒ reuse prevTSID
      (+`is.hasMetricID` check: create global indexes in this partition idb if
      the series isn't registered here yet — series hopping months);
   b. cardinality limiters (PORT-SKIP);
   c. tsidCache hit (& not deleted) ⇒ use; if unknown to this partition idb ⇒
      `createGlobalIndexes` here (timeseriesRepopulated++);
   d. miss ⇒ unmarshal MetricNameRaw → `mn.sortTags()` → `mn.Marshal` →
      `is.getTSIDByMetricName(date)` (ns7 seek; skips deleted); hit ⇒ cache;
      miss ⇒ `generateTSID` + create global+per-day indexes + cache
      (newTimeseriesCreated++, optional new-series log).
   Each resolved metricID goes to `pendingHourEntries` if in current hour and
   not yet in currHourMetricIDs.
5. `prefillNextIndexDB(rows)`: during the last `IDBPrefillStart` (default 1h)
   before month end, probabilistically (ramp 0→1, hash-gated per metricID)
   pre-create global+per-day indexes in next month's partition idb, so the
   month rollover doesn't cause an ingestion latency spike.
6. `updatePerDateData(rows)`: per (date, metricID), skip if covered by
   currHour/prevHour sets (hot path — most rows land here), else
   `idb.dateMetricIDCache.Has`, else queue; sort queued by (date, metricID);
   `is.hasDateMetricID` (§3.2) else `createPerDayIndexes`. Also ramps next-day
   pre-registration during the last hour of the day
   (`pMin = ts%86400/3600 − 23`), feeding `pendingNextDayMetricIDs`.
7. `tb.MustAddRows(rows)` (§4.2).

Slow-path counters: slowRowInserts, slowPerDayIndexInserts — TSBS's insert
benchmark is essentially a stress test of steps 4c/6-fast-path; keep the hour
caches or slow-path index probes dominate.

### 6.5 Search entry points & deadline

`SearchTSIDs / SearchMetricNames / SearchLabelNames / SearchLabelValues /
SearchTagValueSuffixes / GetSeriesCount / GetTSDBStatus` all: optional
`checkTimeRange` (only if denyQueriesOutsideRetention), then `searchAndMerge`:
collect idbs = partition idbs overlapping tr (+ legacy prev/curr), each
searched with `adjustTimeRange(tr, idb.tr)` — global-index search when the
clamped range covers the whole idb or spans > `maxDaysForPerDaySearch = 40`
days; parallel across idbs; results merged (`mergeSortedTSIDs` /
map-dedup). `deadline` = unix seconds, checked inside index loops
(§3.4 pace limiting) and per-block in Search; exceeded ⇒
`ErrDeadlineExceeded`.

`missingMetricIDs` self-heal map: metricID → first-seen+60s deadline, map
reset every 120s; `wasMetricIDMissingBefore` gates ns4 tombstoning (§3.5).

### 6.6 hourMetricIDs / nextDayMetricIDs types

```go
type hourMetricIDs struct { m *uint64set.Set; hour uint64; idbID uint64 }
type nextDayMetricIDs struct { idbID uint64; date uint64; metricIDs uint64set.Set }
```

idbID ties the cached set to the partition idb that was current for that
hour/day; consulted in updatePerDateData to avoid cross-idb false skips.

---

## 7. dedup.go — exact algorithm

Global `dedupInterval` (ms) set once at startup via `SetDedupInterval`;
`isDedupEnabled() = interval > 0`. Two byte-identical implementations:
`DeduplicateSamples([]i64 ts, []f64 vals)` (query-time, vmselect) and
`deduplicateSamplesDuringMerge([]i64 ts, []i64 vals)` (merge-time, decimal
values). Port once, generic over the value type with an `is_stale_nan`
predicate (`f64` StaleNaN bits vs `i64 == vStaleNaN = 1<<63-2`).

Algorithm (`needsDedup` fast-path scan first):

```
tsNext = ts[0] + interval - 1; tsNext -= tsNext % interval
for i, ts in src[1..]:
    if ts <= tsNext: continue        // same dedup bucket → drop predecessor
    // keep src[i] (the LAST sample in the bucket), but among samples with
    // timestamp == src[i]: walk j backwards while ts equal, prefer
    // non-StaleNaN, then maximum value (issues #3333, #10196)
    emit (tsPrev, vPrev)
    tsNext += interval
    if tsNext < ts: tsNext = ts + interval - 1; tsNext -= tsNext % interval
always emit the last sample (same equal-timestamp max/non-stale rule)
```

Semantics: buckets are aligned to interval boundaries
(`tsNext = ceil_to_multiple(ts0, interval) - ?` — exactly the two-line formula
above; keep it verbatim, incl. the `-1` and modulo). The **last** sample in
each bucket wins; ties on identical timestamps prefer non-StaleNaN then max
value. Applied: (a) in every merge via `Block::deduplicateSamplesDuringMerge`
(so data converges as parts merge), (b) partition "final dedup" background pass
when `MinDedupInterval` of parts < current interval (§4), (c) query-side by the
caller. `needsDedup` returns false for <2 samples or interval ≤ 0.

---

## 8. What TSBS exercises — and what to defer

TSBS (load + queries via Influx/Prometheus-style path) exercises: AddRows with
precisionBits=64, high-cardinality series creation (per-day index + caches),
rawRows sharding/flush, inmemory→small→big merges, SearchTSIDs with plain and
composite tag filters (`hostname`, `region`, …), Search block iteration,
decimal encode/decode, dedup off by default.

Defer / skip (mark in code with `// PORT-SKIP`):
- **Graphite**: SearchGraphitePaths, SearchTagValueSuffixes, `__graphite__`
  filters, graphiteReverseTagKey rows (never emitted for dot-free names).
- **Downsampling**: not in lib/storage proper (vmalert-level) — skip entirely.
- **Snapshots** (`MustCreateSnapshotAt`, snapshots/ dir, hardlink parts): skip;
  note partition dirs must stay compatible if added later.
- **Legacy indexDB reading** (index_db_legacy.go, storage_legacy.go): green
  field ⇒ skip; keep search loops shaped as "iterate idbs".
- `-disablePerDayIndex` mode (ns0 writes, global-only lookups): skip; always
  use per-day index.
- **metricnamestats / metricsmetadata** subpackages, TSDBStatus, SeriesCount,
  cardinality limiter, RegisterMetricNames API: skip (stats/UI features).
- DeleteSeries + deletedMetricIDs: implement the read-side filtering and the
  in-memory set (cheap), defer the API.
- Cluster wire types (SearchQuery/TagFilter/TenantToken Marshal, MetricBlock
  Marshal, marshalPortable): skip.
- Retention watcher & free-disk watcher: implement minimal versions (drop old
  partitions; read-only flag) — needed for long runs, trivial.
- Query tracer (`querytracer`): replace with `tracing` crate spans or no-ops.

---

## 9. Rust crate design

### 9.1 Module map (Go file → Rust module)

```
esm-storage/src/
  lib.rs               // pub API: Storage, Search, MetricRow, TSID, TimeRange…
  time.rs              // time.go: TimeRange, date math, partition names
  tsid.rs              // tsid.go (+ Ord impl, merge of sorted TSID vecs)
  metric_name.rs       // metric_name.go: Tag, MetricName, escaping, sortTags,
                       //   commonTagKeys table, raw (u16-len) encoding
  block/
    mod.rs             // block.go: Block, constants
    header.rs          // block_header.go (85-byte layout + validate)
    metaindex.rs       // metaindex_row.go
    stream_writer.rs   // block_stream_writer.go (timestamp-block sharing)
    stream_reader.rs   // block_stream_reader.go
    stream_merger.rs   // block_stream_merger.go + merge.go (mergeBlockStreams)
  part/
    mod.rs             // part.go: Part (mmap/pread files), ibCache
    header.rs          // part_header.go (serde metadata.json)
    inmemory.rs        // inmemory_part.go + raw_row.go (rawRowsMarshaler)
    search.rs          // part_search.go
  partition/
    mod.rs             // partition.go: partition, partWrapper, rawRowsShards,
                       //   flushers, merge scheduling, retention, final dedup
    search.rs          // partition_search.go
  table.rs             // table.go: monthly partition set, ptw refcounts
  table_search.rs      // table_search.go
  index/
    mod.rs             // indexDB struct, ns constants, item builders
    create.rs          // createGlobalIndexes / createPerDayIndexes
    row_merge.rs       // mergeTagToMetricIDsRows (mergeset prepare_block hook)
    tag_filters.rs     // tag_filters.go (+ regex via `regex` crate; see 9.3)
    search.rs          // indexSearch: planner, per-day search, TSID/name lookup
    caches.rs          // metric_id_cache.go, date_metric_id_cache.go,
                       //   loops cache, tagFilters cache
  dedup.rs             // dedup.go (generic over i64/f64)
  storage.rs           // storage.go: Storage, AddRows/add, caches, bg threads
  search.rs            // search.go: Search, BlockRef, MetricBlockRef
  metric_name_search.rs
```

Test mapping: port `*_test.go` table tests alongside each module
(`dedup_test.go` → `dedup.rs` unit tests; `index_db_test.go` →
`index/search.rs` integration tests under `tests/`; `part_search_test.go`,
`merge_test.go`, `storage_test.go` similarly). Golden-value tests: marshal a
known blockHeader/metaindexRow/TSID/MetricName and byte-compare against
constants captured from Go (write a tiny Go harness once to dump vectors).

### 9.2 Porting order (5 stages, each testable)

1. **Codecs & core types** (no I/O): time.rs, tsid.rs, metric_name.rs,
   block/header.rs, block/metaindex.rs, dedup.rs, block/mod.rs
   (Marshal/UnmarshalData on top of esm-encoding + esm-common decimal).
   Test: golden vectors vs Go, round-trips, dedup_test.go tables,
   metric_name_test.go (sortTags canonicalization).
2. **Part layer**: part/inmemory.rs + raw_row.rs sorting/grouping,
   stream_writer/reader, part/header.rs, part/mod.rs, stream_merger + merge.rs.
   Test: rows → inmemoryPart → read back (inmemory_part_test.go,
   block_stream_reader_test.go, merge_test.go: N sorted streams → merged part
   equals expected rows; dedup during merge).
3. **IndexDB**: index/* on top of esm-mergeset (needs prepare_block hook and
   TableSearch seek API). Test: index_db_test.go ports — create series,
   search by tag filters incl. composite/negative/regexp/empty-match, per-day
   vs global, deleted metricIDs, row-merge callback property tests.
4. **Partition/table + Storage write path**: partition/*, table.rs, storage.rs
   (AddRows/add, per-day cache logic, bg flushers, retention). Test:
   storage_test.go subset — AddRows→flush→search counts, partition routing at
   month boundaries, restart/reopen (parts.json), concurrent add.
5. **Search path + end-to-end**: search.rs, table_search, partition/part
   search, metric_name_search. Test: search_test.go, part_search_test.go
   (block iteration order (TSID,MinTs), time-range trimming, retention-deadline
   skipping), then a TSBS smoke benchmark.

### 9.3 Go-specific constructs → Rust

- `sync.Pool` (blocks, indexSearch, kbPool byte buffers, rawRowsMarshaler):
  use `thread_local!` scratch + small object pools only where profiling says
  so; start with plain reuse-by-&mut and Vec::clear — Rust has no GC pressure
  to amortize. Keep `Vec` capacity-reuse patterns (`buf.clear()` not realloc).
- `[]byte` ↔ string unsafe casts (`bytesutil.ToUnsafeBytes/String`): irrelevant
  — use `&[u8]` everywhere; keys are `&[u8]`, HashMap<Box<[u8]>, …> or hashbrown
  raw-entry to avoid alloc-per-lookup.
- `atomic.Pointer[uint64set.Set]` (deletedMetricIDs): `arc_swap::ArcSwap<Uint64Set>`
  + a `Mutex` for copy-on-write updates.
- refcounted wrappers (partWrapper/partitionWrapper `incRef/decRef`, legacy idb):
  `Arc<Part>` / `Arc<Partition>`; "mustDrop after last ref" ⇒ `Drop` impl on a
  wrapper that deletes the directory when flagged (`AtomicBool must_drop`).
- `logger.Panicf("BUG: …")` invariants: `debug_assert!`/`panic!` with same
  messages; FATAL data-corruption paths → `Result` where recoverable at part
  granularity, panic where the upstream panics.
- Go regexp → `regex` crate: port the metricsql-compatible simplification
  (prefix extraction, or-values) — behavior must match `regexutil.SimplifyPromRegex`
  and `GetOrValuesPromRegex`; anchor as `^(?:expr)$`. Match-cost constants stay.
- fastcache/workingsetcache/lrucache: from esm-common; if absent, implement
  lrucache (size-bounded LRU of Arc entries) and a two-generation
  "working-set" wrapper (prev/curr maps swapped on size/interval).
- `container/heap` k-way merges: `std::collections::BinaryHeap` with `Reverse`
  or hand-rolled sift (partSearchHeap holds &mut cursors — in Rust store
  indices into a `Vec<PartSearch>` to satisfy the borrow checker).
- xxhash: `twox-hash`/`xxhash-rust` (xxh64, seed 0 — must match cespare/xxhash).

### 9.4 Concurrency design

- Storage-wide: background threads (std::thread + `stop: Arc<AtomicBool>` +
  Condvar, or crossbeam channels): pending-rows flusher (2s), inmemory-parts
  flusher (5s), merge workers per part type (counts per §4.5, gated by three
  global semaphores), retention watcher, historical-dedup watcher, hour/day
  metricID updaters, free-disk watcher. All join on `MustClose`.
- Locks: partition parts list = `Mutex<PartsState>` (Go: partsLock) —
  snapshot-copy `Vec<Arc<PartWrapper>>` under lock, search outside lock.
  rawRowsShards: per-shard `Mutex<Vec<RawRow>>` (shard chosen by
  atomic round-robin counter); flush swaps the Vec out under the lock.
  table partitions list = `RwLock<Vec<Arc<PartitionWrapper>>>`.
- Caches: sharded/lock-free where hot — tsidCache & metricNameCache are
  fastcache-style byte caches (esm-common); uint64set is single-writer/
  multi-reader via ArcSwap clones.
- Search parallelism: per-day index searches spawn scoped threads
  (`std::thread::scope` or rayon) mirroring Go's WaitGroup fan-out; results
  merged under a Mutex, first error wins.
- Deadlines: pass `deadline_unix_secs: u64` + pace-limiter masks; use a cached
  coarse clock (fasttime equivalent: AtomicU64 refreshed by a ticker thread).

---

## Appendix A. Constants & sizing cheat-sheet

| constant | value | where |
|---|---|---|
| maxRowsPerBlock | 8192 | block.go |
| maxBlockSize | 65536 | block.go |
| marshaled blockHeader / metaindexRow / TSID | 85 / 56 / 24 bytes | §2 |
| maxMetricIDsPerRow (tag rows) | 64 | index_db.go |
| maxMetricRowsPerBlock (AddRows chunk) | 8000 | storage.go |
| defaultPartsToMerge / minMergeMultiplier | 15 / 1.7 | partition.go |
| maxInmemoryParts / maxBigPartSize | 60 / 1e12 | partition.go |
| pendingRowsFlushInterval / dataFlushInterval | 2s / 5s | partition.go |
| finalDedupScheduleInterval | 1h | table.go |
| retentionWatcher / stalePartsRemover ticks | ~1min / ~7min | table/partition |
| maxRawRowsPerShard | 8MiB/40B ≈ 209k rows | partition.go |
| maxDaysForPerDaySearch | 40 | storage.go |
| loopsCountPerMetricNameMatch | 150 | index_db.go |
| maxMetricIDsForDirectLabelsLookup | 100e3 | index_db.go |
| pace masks fast/medium/slow | 2^16−1 / 2^14−1 / 2^12−1 | search.go |
| tsidCache / metricIDCache / metricNameCache | 0.37 / 1/16 / 1/10 ×mem | storage.go |
| tagFiltersToMetricIDsCache / loopsCache | mem/32 / mem/128 | index_db.go |
| ibCache (index blocks) | 0.1×mem | part.go |
| regexpCache / prefixesCache | 0.05×mem each | tag_filters.go |
| compress levels (rows/block) | ≤10:-5 ≤50:-2 ≤200:-1 ≤500:1 ≤1000:2 else 3 | partition.go |
| merge concurrency (inmem/small/big) | CPUs / max(CPUs,4) / max(CPUs,4), global | partition.go |
| missing-metricID tombstone delay | 60s (map reset 120s) | storage.go |
| decimal sentinels | vInfPos=2^63−1, vStaleNaN=2^63−2, vMax=2^63−3 | decimal |
| maxUnixMilli | 9222422399999 | time.go |
