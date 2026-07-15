# Porting spec: VictoriaMetrics `lib/mergeset` (v1.146.0) → Rust crate `esm-mergeset`

Source analyzed: `/home/test/refsrc/VictoriaMetrics/lib/mergeset` (read-only reference).
All byte layouts and constants below are quoted from that exact revision.

## 1. Purpose and role

`lib/mergeset` is a general-purpose LSM-tree over **sorted byte-string items** (no
key/value split — the whole item is the key). VictoriaMetrics uses it as the storage
engine for the **inverted index** (`indexdb`): `lib/storage/index_db.go:175` opens it as

```go
tb := mergeset.MustOpenTable(path, dataFlushInterval, tfssCache.Reset, 0, mergeTagToMetricIDsRows, isReadOnly)
```

- Items are namespaced rows: `nsPrefix + tag → MetricIDs`, `metricName → TSID`, etc.
- `flushCallback` = `tfssCache.Reset` (invalidates tagFilters cache when new data becomes searchable).
- `prepareBlock` = `mergeTagToMetricIDsRows` (index_db.go:3250) — merges rows sharing a
  `tag` prefix into one row with a combined, deduplicated MetricIDs list during merges.
- Searches go through `indexSearch.ts mergeset.TableSearch` (`ts.Init(db.tb, sparse)`,
  `ts.Seek(prefix)`, `ts.NextItem()`).
- VictoriaLogs' `logstorage/indexdb.go` uses it identically for stream indexes.

Key design points: partition-free single `Table` per directory; two part tiers
(in-memory and file); items ≤ 64KiB; everything immutable once written; deletion only
via whole-table drop (indexdb rotates generations instead of deleting items).

## 2. Data model and on-disk formats

### Primitive encodings (`lib/encoding/int.go`)
- `MarshalUint32/MarshalUint64`: **big-endian** fixed width.
- `MarshalVarUint64`: LEB128-style varint, 7 bits per byte, `0x80` continuation, little-end first.
- `MarshalBytes(dst, b)`: `varuint64(len(b)) ++ b`.
- `MarshalVarUint64s`: the varints concatenated.
- Compression: zstd (`encoding.CompressZSTDLevel(dst, src, level)` appends to dst). Rust: `zstd` crate; levels −5..3 map directly (zstd supports negative levels).

### Item and inmemoryBlock (`encoding.go`)
```go
type Item struct { Start uint32; End uint32 }          // offsets into a shared data buf

type inmemoryBlock struct {
    commonPrefix []byte  // common prefix of ALL items in the block
    data         []byte  // concatenated item bytes (each item INCLUDES the prefix)
    items        []Item
}
const maxInmemoryBlockSize = 64 * 1024   // max data size; also max single-item size
```
`Add(x)` rejects when `len(x)+len(data) > 64KiB` (caller starts a new block). On first
Add: `data` preallocated to 64KiB, `items` to 512. `SortItems()` sorts by the item
bytes *after* commonPrefix (equivalent to full-item order since prefix is shared).
`updateCommonPrefixSorted` uses `commonPrefixLen(first, last)`; a block with ≤1 item
gets an empty commonPrefix.

Rust: `struct InmemoryBlock { common_prefix: Vec<u8>, data: Vec<u8>, items: Vec<Item> }`,
`Item { start: u32, end: u32 }` with `fn bytes<'a>(&self, data: &'a [u8]) -> &'a [u8]`.
No unsafe needed; the Go `unsafe.Slice/String` are only alloc-avoidance.

### storageBlock and item/lens encoding (`marshalData`, encoding.go:265)
```go
type storageBlock struct { itemsData []byte; lensData []byte }
type marshalType uint8  // 0 = plain, 1 = zstd; values >1 invalid
```
`marshalData(sb, firstItemDst, commonPrefixDst, compressLevel)` returns
`(firstItem, commonPrefix, itemsCount u32, marshalType)`. firstItem/commonPrefix are
*not* stored in the storageBlock — they live in the blockHeader.

**Plain** (`marshalTypePlain=0`) — chosen when
`len(data) - len(commonPrefix)*len(items) < 64 || len(items) < 2`, or when zstd
compresses to > 0.9× of that size:
- `itemsData` = concatenation of items **[1..]** with commonPrefix stripped (first item omitted — it is in the header).
- `lensData` = for items [1..]: `MarshalUint64(itemLen - cpLen)` — fixed 8-byte BE each.

**ZSTD** (`marshalTypeZSTD=1`):
- For items [1..] compute `prefixLen[i] = commonPrefixLen(prevItem, item)` where both
  items have the block commonPrefix already stripped; `prevItem` starts as
  `firstItem[cpLen:]`.
- `itemsData` = zstd( concat of `item[prefixLen:]` suffixes for items [1..] ).
- `lensData` = zstd( varints of `prefixLen[i] XOR prefixLen[i-1]` for items[1..]
  (prev starts at 0), followed by varints of `itemLen[i] XOR itemLen[i-1]`
  (prev starts at `len(firstItem)-cpLen`) ). Both arrays have `itemsCount-1` entries.
  The XOR-delta makes runs of equal lens encode as zeros.

`UnmarshalData` reverses this; decodes lens first (must consume lensData exactly),
computes total dataLen, reconstructs each item as
`commonPrefix ++ prevItem[:prefixLen] ++ suffix`, validates sortedness. Single-item
blocks are rebuilt from header alone (`unmarshalSingleItem`; must be plain).

### blockHeader (`block_header.go`) — one per data block, lives in index.bin
```go
type blockHeader struct {
    commonPrefix []byte; firstItem []byte
    noCopy bool                    // in-memory only flag
    marshalType marshalType
    itemsCount uint32              // NOTE: comment says "excluding first" but it INCLUDES it (== len(ib.items))
    itemsBlockOffset uint64; lensBlockOffset uint64
    itemsBlockSize uint32; lensBlockSize uint32
}
func (bh *blockHeader) Marshal(dst []byte) []byte {
    dst = encoding.MarshalBytes(dst, bh.commonPrefix)
    dst = encoding.MarshalBytes(dst, bh.firstItem)
    dst = append(dst, byte(bh.marshalType))
    dst = encoding.MarshalUint32(dst, bh.itemsCount)
    dst = encoding.MarshalUint64(dst, bh.itemsBlockOffset)
    dst = encoding.MarshalUint64(dst, bh.lensBlockOffset)
    dst = encoding.MarshalUint32(dst, bh.itemsBlockSize)
    dst = encoding.MarshalUint32(dst, bh.lensBlockSize)
    return dst
}
```
Byte layout: `varbytes(commonPrefix) varbytes(firstItem) u8 u32be u64be u64be u32be u32be`.
Unmarshal validation: `itemsCount > 0`, `itemsBlockSize ≤ 2*64KiB`,
`lensBlockSize ≤ 2*8*64KiB`. `UnmarshalNoCopy` borrows slices from the index block buf
(Rust: either copy, or hold `Range<usize>` offsets into the owned `IndexBlock.buf`).
An **index block** is a sequence of blockHeaders sorted by firstItem, marshaled
back-to-back, zstd-compressed as a unit; uncompressed size capped at
`maxIndexBlockSize = 64 * 1024` (a single header may exceed it; hard cap 3× when built
from one header in `inmemoryPart.Init`).

### metaindexRow (`metaindex_row.go`) — one per index block, lives in metaindex.bin
```go
type metaindexRow struct {
    firstItem []byte            // first item of the first block in this index block
    blockHeadersCount uint32
    indexBlockOffset uint64
    indexBlockSize uint32
}
// Marshal: varbytes(firstItem) u32be u64be u32be
```
metaindex.bin = zstd( concat of all rows, sorted by firstItem ). Read fully into
memory at part open (`unmarshalMetaindexRows`). Validation: count > 0,
`indexBlockSize ≤ 4*maxIndexBlockSize`.

### partHeader (`part_header.go`) — metadata.json, JSON with hex strings
```go
type partHeader struct { itemsCount, blocksCount uint64; firstItem, lastItem []byte }
// JSON: {"ItemsCount":N,"BlocksCount":N,"FirstItem":"<hex>","LastItem":"<hex>"}
```
Validation on read: itemsCount > 0, blocksCount > 0, blocksCount ≤ itemsCount.

### part (`part.go`) and inmemoryPart (`inmemory_part.go`)
```go
type part struct {
    ph partHeader; path string; size uint64
    mrs []metaindexRow
    indexFile, itemsFile, lensFile fs.MustReadAtCloser   // random-access readers
}
type inmemoryPart struct {
    ph partHeader; bh blockHeader; mr metaindexRow
    metaindexData, indexData, itemsData, lensData chunkedbuffer.Buffer
}
```
`inmemoryPart.Init(ib)` builds a complete 1-block part in memory at zstd level **−5**
(comment: "will be merged into file part soon"). `inmemoryPart` mirrors the four file
streams as in-memory buffers, so one `part` open path (`newPart`) serves both: an
in-memory part is just a part whose "files" are byte buffers (Rust: trait
`ReadAt { fn read_at(&self, buf, off) }` implemented by `File` and by `Vec<u8>`/chunked buffer).
`mustOpenFilePart` reads metadata.json + metaindex.bin, opens the other three files.

### Hierarchy summary
```
Table dir/           parts.json  = JSON array of part dir names (sorted)
  <%016X mergeIdx>/  metadata.json (partHeader)
                     metaindex.bin = zstd(metaindexRow*)       — fully in RAM
                     index.bin     = zstd(blockHeader*)*       — per index block, cached
                     items.bin     = itemsData blocks           — per data block, cached
                     lens.bin      = lensData blocks
```
Lookup chain: partHeader → mrs (binary search firstItem) → index block → blockHeaders
(binary search firstItem) → items/lens block → inmemoryBlock → binary search item.

## 3. Write path

`Table.AddItems(items [][]byte)` → `rawItemsShards.addItems`:

- **Sharding**: `rawItemsShardsPerTable = cpus * min(cpus,16)` shards; round-robin via
  atomic `shardIdx`. Each `rawItemsShard` is `{flushDeadlineMs atomic.Int64; mu Mutex; ibs []*inmemoryBlock}`
  padded to a cache line (Rust: `#[repr(align(64))]` or `crossbeam_utils::CachePadded`).
- `rawItemsShard.addItems`: append into last `inmemoryBlock`; when full, start a new
  one; when `len(ibs) >= maxBlocksPerShard (256)` hand the whole batch out as
  `ibsToFlush` and return remaining items as `tailItems` (caller re-loops picking
  another shard). Items longer than 64KiB are **dropped with a throttled error log**
  (counter `tooLongItemsTotal`). First block insertion arms the shard's flush deadline
  (`now + pendingItemsFlushInterval`, = 1s).
- `rawItemsShards.addIbsToFlush`: accumulates into `ibsToFlush`; when
  `≥ maxBlocksPerShard * cpus` blocks, flushes them immediately via
  `flushBlocksToInmemoryParts`.
- **pendingItems semantics**: raw blocks are *not searchable*. A background
  `pendingItemsFlusher` ticks every `pendingItemsFlushInterval = 1s` and moves due
  shards' blocks to in-memory parts (`appendBlocksToFlush` honors per-shard deadline;
  `isFinal` forces). `DebugFlush()` = flush(final) + wait on `flushPendingItemsWG`
  (a wait group usable concurrently from many goroutines).

`flushBlocksToInmemoryParts(ibs, isFinal)`:
1. Chunk ibs into groups of `defaultPartsToMerge = 15`; for each chunk (bounded by
   `inmemoryPartsConcurrencyCh`, capacity = CPUs) run `createInmemoryPart`:
   single non-empty block → `inmemoryPart.Init`; else k-way merge of the sorted blocks
   into one inmemoryPart via blockStreamReaders (`MustInitFromInmemoryBlock` sorts the block).
2. Repeatedly `mustMergeInmemoryParts` (optimal-merge groups, parallel) until one part
   remains or parts exceed `getMaxInmemoryPartSize()` = `max(5% of mem / 30, 1e6)` bytes;
   oversized ones are registered immediately.
3. `addToInmemoryParts`: acquire a slot in `inmemoryPartsLimitCh`
   (**capacity `maxInmemoryParts = 30`; blocks ingestion — this is the backpressure**;
   counted in `inmemoryPartsLimitReachedCount`), append under `partsLock`, kick an
   in-memory merger goroutine, then trigger `flushCallback` (immediately if final,
   else set `needFlushCallbackCall` for the callback worker which ticks at
   `flushCallbackInterval`, default 10s + jitter).

**Durability flush**: `inmemoryPartsFlusher` ticks every `flushInterval` (min 1s;
storage passes `dataFlushInterval`, default 5s) and merges every in-memory part whose
`flushToDiskDeadline` passed into a **file** part (`flushInmemoryPartsToFiles`).
`flushToDiskDeadline = now + flushInterval` at part creation; merges of in-memory
parts inherit the **earliest** source deadline (`getFlushToDiskDeadline`).

There are no assisted merges in this version; backpressure is solely the
30-in-memory-parts semaphore (plus merger concurrency semaphores).

## 4. Merge machinery

### Concurrency semaphores
```go
inmemoryPartsConcurrencyCh = make(chan struct{}, cpus)
filePartsConcurrencyCh     = make(chan struct{}, max(cpus, 4))  // ≥4 so small merges can run beside big ones
```

### Background mergers
`MustOpenTable` starts `cap(filePartsConcurrencyCh)` `filePartsMerger` goroutines;
in-memory mergers start lazily when in-memory parts appear (`startInmemoryPartsMergerLocked`
is called on each add, `wg.Go` guarded by `stopCh` under `partsLock`). Each merger loops:
```
if isReadOnly → return
maxOutBytes = getMaxFilePartSize()      // min(freeDiskSpace / cap(filePartsConcurrencyCh), maxPartSize=400e9)
pws = getPartsToMerge(parts, maxOutBytes)   // under partsLock; marks isInMerge=true
if none → return (goroutine exits; restarted on next part add)
acquire semaphore; mergeParts(pws, tb.stopCh, false); release
```

### Part selection: `appendPartsToMerge(dst, src, maxPartsToMerge=15, maxOutBytes)`
- Skip if < 2 candidates. Filter parts with `size > maxOutBytes/minMergeMultiplier`
  (`minMergeMultiplier = 1.7`). Sort ascending by size.
- Exhaustive scan of contiguous windows of length i ∈ [max((15+1)/2,2)=8 … min(15,len)]:
  reject window if `a[0].size*len(a) < a[len-1].size` (too unbalanced); reject if
  `sum > maxOutBytes`; score `m = sum / a[last].size` (write-amplification proxy);
  keep the window with max m.
- Require `maxM ≥ max(maxPartsToMerge/2, 1.7) = 7.5`; else merge nothing.
  (`getPartsForOptimalMerge`, used by the flush path, calls this with
  `maxOutBytes = MaxUint64` so the 7.5 threshold still gates.)

### mergeParts (`table.go:1182`)
- Fast path: `isFinal && len(pws)==1 && in-memory` → `mp.MustStoreToDisk(dstPartPath)`.
- Destination type (`getDstPartType`): file if `isFinal`, or total size >
  maxInmemoryPartSize, or any source is a file part; else in-memory.
- Destination file path: `tb.path/%016X` of `tb.nextMergeIdx()` (seeded with UnixNano).
- Compression level from destination items count (`getCompressLevel`):
  `≤2^16→-5, ≤2^17→-4, ≤2^18→-3, ≤2^19→-2, ≤2^20→-1, ≤2^22→1, ≤2^25→2, else 3`.
- `nocache = srcItemsCount > maxItemsPerCachedPart()` (= `max(mem/(4*15), 1e6)`) —
  written with O_DIRECT-ish no-page-cache streams (Rust: fadvise DONTNEED or just skip).
- After the stream merge: write metadata.json, sync dir, `openCreatedPart`,
  `swapSrcWithDstParts` (below). Merges > 30s are logged.

### Stream pipeline
`blockStreamReader` (bsr): iterates a part's blocks in order — walks `mrs`, reads +
unzstds each index block into `bhs`, then for each bh reads itemsBlockSize/lensBlockSize
bytes from the sequential items/lens readers and `UnmarshalData`s into `bsr.Block`.
Verifies first/last item against partHeader and item/block counts. Can also wrap a
single sorted `inmemoryBlock` (`MustInitFromInmemoryBlock`, used when merging raw blocks).

`blockStreamWriter` (bsw): `WriteBlock(ib)` marshals the sorted block
(`MarshalSortedData`), appends itemsData/lensData to items.bin/lens.bin tracking
offsets, marshals the blockHeader into `unpackedIndexBlockBuf`; when that exceeds
64KiB, `flushIndexData()` zstd-compresses it into index.bin and emits a metaindexRow.
`MustClose` flushes the tail index block and writes zstd(metaindex).

`mergeBlockStreams(ph, bsw, bsrs, prepareBlock, stopCh, itemsMerged)` /
`blockStreamMerger`:
- Min-heap of bsrs ordered by `CurrItem()` (current item of current block).
- Loop: take heap top; `compareEveryItem` optimization — if the top block's *last*
  item ≤ next reader's current item (`getNextReader` peeks heap children 1,2), copy the
  whole rest of the block without per-item comparisons. Append items into scratch
  `ib inmemoryBlock`; when full, `flushIB`.
- `flushIB`: calls `prepareBlock(data, items)` if set — the callback may rewrite the
  block in place (dedup/merge tag→MetricIDs rows) but **must keep items sorted, first
  item ≥ original first, last ≤ original last** (checked; sortedness only in tests).
  Then updates `ph.{itemsCount,blocksCount,firstItem,lastItem}` and `bsw.WriteBlock`.
- On block exhaustion: `bsr.Next()` then `heap.Fix(0)`, else `heap.Pop`. `stopCh`
  closed → `errForciblyStopped`. Stats flushed to the shared atomic ~1/second.

Rust: `BinaryHeap<Reverse<BsrEntry>>` or hand-rolled sift-down heap (needed for
`heap.Fix(0)` = `sift_down(0)`; a hand-rolled 4-line heap over `Vec<Box<BlockStreamReader>>`
is simplest and matches Go semantics).

### swapSrcWithDstParts
Under `partsLock`: remove sources from `inmemoryParts`/`fileParts`, append the new
part, kick the relevant merger, and if any file part was removed or created, rewrite
`parts.json` **atomically inside the lock**. Outside the lock: release
`inmemoryPartsLimitCh` slots for removed in-memory parts (re-acquire one if dst is
in-memory); then for each source `pw.mustDrop.Store(true); pw.decRef()` — the last
reference-dropper deletes the part directory.

## 5. Read path

`TableSearch` (`table_search.go`) — reusable cursor:
- `Init(tb, sparse)`: `pws = tb.getParts()` (snapshot with incRef under partsLock over
  inmemory+file parts), one `partSearch` per part.
- `Seek(k)`: seek every partSearch, push non-exhausted ones onto `partSearchHeap`
  (min-heap by current `Item`); `Item = heap[0].Item`; `nextItemNoop=true` so the first
  `NextItem()` just returns the seeked item.
- `NextItem()`: advance heap top; `heap.Fix(0)` or `Pop`; EOF when empty. **Duplicates
  across parts are NOT deduplicated** — callers see them (indexdb tolerates this).
- `FirstItemWithPrefix(prefix)`: Seek + NextItem + `bytes.HasPrefix` check → io.EOF if absent.
- `MustClose`: `tb.putParts(pws)` (decRef).

`partSearch` (`part_search.go`):
- `Seek(k)`: if `k > ph.lastItem` → EOF. `tryFastSeek` — if the current cached block
  may contain k (k ≤ last item of block, ≥ item at idx-1), binary search within it and
  avoid re-descending; handles "k in earlier block" bail-outs. Otherwise: if
  `k ≤ ph.firstItem` just load the first block; else `sort.Search` over `mrs` by
  firstItem then step back one (`n--`), load that index block's bhs, `sort.Search` over
  bhs then `n--`, load the data block, then `binarySearchKey(data, items, k, cpLen)`
  where `cpLen = commonPrefixLen(ib.commonPrefix, k)` and comparisons use the k-suffix
  vs prefix-stripped items (valid because all items share the block prefix). If k is
  past the block's last item, advance to the next block (k is then its first item).
- `NextItem()`: bump `ibItemIdx`; on block exhaustion `nextBlock()` → next bh, next
  index block, next metaindex row, else EOF.
- **Caches** (`part.go`, `lib/blockcache`): three process-global caches shared by all
  tables, keyed by `Key{Part: *part (pointer identity), Offset: u64}`:
  - `idxbCache` — decompressed index blocks (`*indexBlock`), 10% of allowed mem.
  - `ibCache` — decoded data blocks (`*inmemoryBlock`), 25% of allowed mem.
  - `ibSparseCache` — same but used when `Init(..., sparse=true)` (storage uses sparse
    for cache-unfriendly scans), 5% of allowed mem.
  blockcache internals: sharded (cpus*min(cpus,16)) LRU-ish maps with per-entry
  lastAccessTime heap eviction, and `missesBeforeCaching = 2` (a block enters the cache
  only on the 2nd miss for its key — keeps one-shot scans from thrashing).
  `part.MustClose` calls `RemoveBlocksForPart(p)` on all three.
  partSearch keeps `tmpIB`/`tmpIdB` scratch blocks; when `TryPutBlock` accepts one,
  ownership transfers to the cache and a fresh scratch is allocated.
  Single-item blocks bypass the cache (`unmarshalSingleItem` from the header alone).

Rust cache proposal: `esm-blockcache` module — sharded `Mutex<HashMap<Key, Arc<CacheEntry>>>`
where Key = `(part_id: u64, offset: u64)` (give each Part a unique u64 id from a global
counter instead of pointer identity), values `Arc<IndexBlock>` / `Arc<InmemoryBlock>`;
keep the misses-map + size-based eviction; return `Arc` clones to searchers.

`Table.MustCreateSnapshotAt(dstDir)` **is used** (via `LegacyMustCreateSnapshotAt`,
table_legacy.go, for legacy indexdb snapshots): flushes everything to files, writes
parts.json into dstDir, hard-links every part file. Port it (it's ~40 lines) but it can
be deferred to last.

## 6. Concurrency & lifecycle → Rust mapping

Go structure:
- `partsLock sync.Mutex` guards `inmemoryParts`, `fileParts`, merger spawning, and
  `close(stopCh)`; `wg.Add` only under the lock after checking stopCh (prevents
  add-after-Wait races).
- `partWrapper{refCount atomic.Int32, mustDrop atomic.Bool, p *part, mp *inmemoryPart, isInMerge bool, flushToDiskDeadline}`;
  `isInMerge` is guarded by partsLock, not atomic. decRef→0 closes the part and deletes
  its dir if mustDrop.
- Background goroutines: N file mergers (persistent), lazy in-memory mergers,
  pendingItemsFlusher (1s), inmemoryPartsFlusher (flushInterval), flushCallback worker.
- `MustClose`: close stopCh under lock → wg.Wait → final flush to files → assert no
  raw items / in-memory parts → decRef all file parts (must hit 0).

Rust proposal (std threads; **no rayon/tokio** — the workloads are long-lived loops and
blocking I/O, and we want deterministic shutdown):
- `struct PartWrapper { p: Part, mp: Option<InmemoryPart>, must_drop: AtomicBool, flush_deadline: Instant }`
  held as `Arc<PartWrapper>`. **Drop the manual refcount**: `Arc` strong count is the
  refcount; implement `Drop for PartWrapper` doing "if must_drop { remove_dir_all(path) }"
  and file close. Snapshot-for-search = clone the `Vec<Arc<PartWrapper>>`.
  `is_in_merge` moves into the locked state (see below) as a `HashSet<PartId>` or a
  `Cell`-like bool inside the mutex-protected struct — do not put it on the Arc'd part.
- `struct TableState { inmemory_parts: Vec<Arc<PartWrapper>>, file_parts: Vec<Arc<PartWrapper>>, in_merge: HashSet<PartId>, stopped: bool }`
  inside `Mutex<TableState>` (Mutex, not RwLock: all accesses are short and mutating;
  matches Go). `Table` itself is `Arc<TableInner>` so worker threads hold it.
- stopCh → `Arc<AtomicBool>` + `Condvar`/`crossbeam::channel::Receiver<()>`
  (crossbeam `select!` replaces Go `select` on stopCh vs ticker; a plain
  `Condvar::wait_timeout` loop also suffices and avoids the dependency).
- Semaphores (`inmemoryPartsConcurrencyCh`, `filePartsConcurrencyCh`,
  `inmemoryPartsLimitCh`) → `std::sync::Semaphore` is unstable; use a small
  `struct Sema(Mutex<usize>, Condvar)` or crossbeam bounded channel of `()` — the
  bounded channel is the literal translation and also gives the "abort on stop" select.
- WaitGroup → `std::thread::JoinHandle`s collected in the Table + a
  `Mutex<Vec<JoinHandle>>`; lazy merger spawn checks `stopped` under the state lock
  before pushing a handle (same race-prevention discipline as Go).
- `flushPendingItemsWG` (concurrent Add/Wait) → `Mutex<usize>+Condvar` counter.
- rawItemsShards → `Vec<CachePadded<Mutex<RawShard>>>` + `AtomicU32` round-robin +
  `AtomicI64` deadlines. Per-shard Mutex only; no global lock on the hot insert path.
- Panics (`logger.Panicf` on corruption/BUG) → `panic!` for BUG-class invariants;
  return `Result` for data corruption found on read paths that Go returns errors for
  (UnmarshalData etc. already return errors — keep that split exactly).

## 7. Durability

- Part dir: `metaindex.bin`, `index.bin`, `items.bin`, `lens.bin`, `metadata.json`;
  created via write-to-final-name then `fsync` files, fsync part dir, fsync parent
  (`fs.MustSyncPathAndParentDir`). metadata.json is plain write+sync (created once).
- `parts.json` in the table dir is the **source of truth** for live parts; written
  atomically (`fs.MustWriteAtomic`: write `parts.json.tmp`-style, fsync, rename, fsync
  dir) under partsLock whenever the file-part set changes.
- Recovery (`mustOpenParts`): delete legacy `txn`/`tmp` dirs; read parts.json (if
  missing — pre-v1.90 upgrade — list dirs, skipping `tmp/txn/snapshots/cache`);
  **panic if a listed part dir is missing**; delete any dir *not* listed (leftover from
  unclean shutdown mid-merge — the old parts are still listed, so this is safe); write
  parts.json if absent.
- Crash-consistency invariant: a merge becomes durable only at the parts.json rewrite;
  until then the new part dir is garbage that recovery removes. In-memory parts and raw
  items (≤ ~flushInterval + 1s of writes) are lost on crash — accepted for an inverted
  index that can be rebuilt from raw data.

Rust: `esm-fs` helpers — `write_atomic(path, data)` (tempfile+rename+dir fsync),
`sync_path_and_parent`, `hard_link_files` (for snapshot), `must_read_at` via
`std::os::unix::fs::FileExt::read_at_exact`-style loop.

## 8. Rust module breakdown & porting order

Crate `esm-mergeset` (files under `src/`), in porting order — each step compiles and
has its Go tests ported before the next:

| # | Rust module | Go source | Go tests to port |
|---|-------------|-----------|------------------|
| 1 | `encoding.rs` (varint/bytes/u32/u64 helpers, zstd wrappers) | `lib/encoding/int.go` (subset), zstd | encoding_test.go round-trip cases |
| 2 | `inmemory_block.rs` (Item, InmemoryBlock, StorageBlock, MarshalType, marshal/unmarshal plain+zstd) | `encoding.go` | `encoding_test.go` (TestCommonPrefixLen, TestInmemoryBlockAdd/Sort, TestInmemoryBlockMarshalUnmarshal — the critical corpus) |
| 3 | `block_header.rs` | `block_header.go` | (covered via 5/6) |
| 4 | `metaindex.rs` + `part_header.rs` + `filenames.rs` | `metaindex_row.go`, `part_header.go`, `filenames.go` | metadata.json round-trip (new small test) |
| 5 | `inmemory_part.rs`, `part.rs` | `inmemory_part.go`, `part.go` | — |
| 6 | `block_stream_reader.rs`, `block_stream_writer.rs` | `block_stream_{reader,writer}.go` | `block_stream_reader_test.go` (empty/single/multi-block streams) |
| 7 | `merge.rs` (BlockStreamMerger, PrepareBlockCallback = `fn(&mut Vec<u8>, &mut Vec<Item>)` or boxed closure) | `merge.go` | `merge_test.go` (TestMergeBlockStreams*, TestMergeForciblyStop) |
| 8 | `blockcache.rs` (or sibling crate) | `lib/blockcache/blockcache.go` | blockcache tests (basic get/put/evict) |
| 9 | `part_search.rs` | `part_search.go` | `part_search_test.go` (exhaustive seek/next over random parts) |
| 10 | `table.rs` (Table, rawItemsShards, partWrapper, mergers, flushers, parts.json, snapshot) | `table.go`, `table_legacy.go` | `table_test.go` (open/close, concurrent AddItems, reopen persistence), appendPartsToMerge unit tests if present in storage history — else write property tests for the 1.7/7.5 heuristics |
| 11 | `table_search.rs` | `table_search.go` | `table_search_test.go`, `table_search_timing_test.go` → criterion benches |
| 12 | `fs.rs` util (atomic write, sync, readat traits) | `lib/fs` subset | — |

Public API surface: `Table::open`, `Table::add_items`, `TableSearch::{init,seek,next_item,first_item_with_prefix}`,
`Table::{debug_flush, close, update_metrics, create_snapshot_at, notify_read_write_mode}`,
`PrepareBlockCallback`, `TableMetrics`, cache-size setters.

## 9. Go-GC-specific constructs to redesign

| Go construct | Where | Rust replacement |
|---|---|---|
| `sync.Pool` of bsr/bsw/blockStreamMerger/WaitGroup/lensBuffer/ByteBuffer | everywhere | Mostly **delete**: rely on reusing owned buffers inside long-lived structs (`Vec::clear()` keeps capacity). Where pooling measurably matters (per-merge readers), a `thread_local!` scratch or a tiny `Mutex<Vec<T>>` pool. Start without pools; add after benchmarks. |
| `unsafe.String`/`unsafe.Slice` in `Item.Bytes/String` | encoding.go | Plain `&data[start..end]`; comparisons on `&[u8]` (Ord on byte slices == Go string compare). Zero unsafe. |
| `append(dst[:0], ...)` caller-supplied dst buffers | all marshal fns | Take `&mut Vec<u8>` and append (`fn marshal(&self, dst: &mut Vec<u8>)`); return counts via tuple. |
| `blockHeader.UnmarshalNoCopy` aliasing the index-block buf | block_header.go | `IndexBlock { buf: Vec<u8>, bhs: Vec<BlockHeaderRef> }` where the ref stores `Range<u32>` offsets into `buf` for commonPrefix/firstItem — self-referential struct avoided by using offsets, preserving the no-copy win. |
| Pointer-identity cache keys (`Key{Part: any}`) | blockcache | Monotonic `PartId(u64)` assigned at Part construction. |
| partWrapper manual refcount + `mustDrop` | table.go | `Arc<PartWrapper>` + `Drop` impl + `AtomicBool must_drop` (set before dropping the table's Arc clones). |
| Goroutine-per-merger + channels-as-semaphores | table.go | std threads + bounded crossbeam channels (or Mutex+Condvar semaphore); `select` on stop via crossbeam `select!`. |
| `chunkedbuffer.Buffer` (chunked to avoid large-alloc GC pressure) | inmemory_part.go | Plain `Vec<u8>` per stream is fine in Rust (no GC); keep a `ReadAt` impl for it. If 64KiB-chunk allocation behavior is desired for fragmentation, add later. |
| `atomicutil.CacheLineSize` padding | rawItemsShard | `crossbeam_utils::CachePadded<Mutex<RawShard>>`. |
| `fasttime.UnixTimestamp()` coarse clock | merge.go | `Instant` snapshots; the 1s stats-flush granularity just needs `Instant::elapsed`. |
| `logger.Panicf(FATAL/BUG)` | everywhere | `panic!` for BUG invariants; `Result<_, MergesetError>` where Go returns errors. FATAL-on-corruption at open time may stay `panic!` (process cannot serve without its index) — decide once at crate boundary. |

Dependencies: `zstd`, `crossbeam-utils` (+ optionally `crossbeam-channel`), `serde`/`serde_json` (metadata.json, parts.json), `xxhash-rust` (blockcache key hash). Everything else std.
