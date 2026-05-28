# Mergeset part — on-disk format

This document is the canonical EsMetrics specification for the byte layout of
a **mergeset part** as produced by VictoriaMetrics v1.144.0. It is the source
of truth for the Rust reader and writer in `esm-storage::mergeset`.

Every claim below is anchored to a VictoriaMetrics source file and line range
so the spec can be re-verified when we bump the upstream pin. References use
the shorthand `lib/<file>.go:<line>` and resolve against
`github.com/VictoriaMetrics/VictoriaMetrics` at tag **v1.144.0**.

## 0. Conventions

- **Byte order:** all fixed-width integers are **big-endian** unless explicitly
  noted as variable-length (varuint). See `lib/encoding/int.go:25,38` for
  `MarshalUint32` / `MarshalUint64`.
- **Variable-length unsigned integer (varuint):** little-endian
  base-128 with a high-bit continuation flag. Matches Protocol Buffers' varint
  for unsigned values. Implementation: `lib/encoding/int.go:287`
  (`MarshalVarUint64`). The reader is at line 368 (`UnmarshalVarUint64`).
- **Length-prefixed byte string ("Bytes"):** a varuint length followed by that
  many bytes. `lib/encoding/int.go:506` (`MarshalBytes`) and 515
  (`UnmarshalBytes`).
- **zstd-compressed payload:** opaque to the format layer. EsMetrics pins to the
  same zstd version VictoriaMetrics vendors so the compressed bytes are
  reproducible per-input per-level (ADR-001 #14).
- **Sort order:** lexicographic on raw bytes.

## 1. Part directory layout

A part is a directory containing **five files** (filenames are constants in
`lib/mergeset/filenames.go:3`):

| File             | Purpose                                                             |
| ---------------- | ------------------------------------------------------------------- |
| `metadata.json`  | Whole-part summary (item count, block count, first/last item).     |
| `metaindex.bin`  | Zstd-compressed sequence of `metaindexRow` entries.                |
| `index.bin`      | Concatenated, zstd-compressed *index blocks*; each holds a batch of `blockHeader` rows. |
| `items.bin`      | Concatenated *item payloads*; one variable-size payload per block. |
| `lens.bin`       | Concatenated *length payloads*; one per block, paired with `items.bin`. |

The top-level table directory additionally contains `parts.json` listing the
parts currently visible to readers; that file is **out of scope** for this
spec — it's a table-level concern, not a part-level one.

## 2. `metadata.json`

JSON document, UTF-8, encoded with Go's default `encoding/json` settings.
See `lib/mergeset/part_header.go:28-44,114-130`.

```json
{
  "ItemsCount": <uint64>,
  "BlocksCount": <uint64>,
  "FirstItem": "<hex>",
  "LastItem":  "<hex>"
}
```

| Field         | Type        | Notes                                                   |
| ------------- | ----------- | ------------------------------------------------------- |
| `ItemsCount`  | uint64      | Must be > 0. Total number of items in the part.        |
| `BlocksCount` | uint64      | Must be > 0 and ≤ `ItemsCount`. Total number of data blocks (each block aggregates ≥1 sorted items). |
| `FirstItem`   | hex string  | Lower-case hex of the smallest item bytes (lex-sorted). |
| `LastItem`    | hex string  | Lower-case hex of the largest item bytes.              |

VM writes the file with `fs.MustWriteSync` (no atomic-rename trick) because
the part directory is `fsync`'d after creation. EsMetrics matches this
sequence to keep crash-recovery semantics identical.

## 3. `metaindex.bin`

The entire file is a **single zstd frame**. When decompressed, it yields a
flat byte stream containing one or more `metaindexRow` entries, packed back-
to-back, **sorted by `firstItem`** (lexicographic).

Reader: `lib/mergeset/metaindex_row.go:85`.

### metaindexRow byte layout

| Offset within row | Field               | Encoding              |
| ----------------- | ------------------- | --------------------- |
| 0                 | `firstItem`         | Bytes (varuint len + raw bytes) |
| varies            | `blockHeadersCount` | BE uint32             |
| +4                | `indexBlockOffset`  | BE uint64             |
| +8                | `indexBlockSize`    | BE uint32             |

Source: `lib/mergeset/metaindex_row.go:34-40`.

Validation rules enforced by the reader (lines 72-80):
- `blockHeadersCount > 0`
- `indexBlockSize <= 4 * maxIndexBlockSize` (= 4 × 64 KiB = 256 KiB). The
  upper bound exceeds the per-block target because `commonPrefix` and
  `firstItem` inside a `blockHeader` can each grow to roughly `maxIndexBlockSize`
  before the writer flushes.

Each `metaindexRow` points to one **index block** inside `index.bin`,
identified by `(indexBlockOffset, indexBlockSize)`.

## 4. `index.bin`

`index.bin` is a flat concatenation of **index blocks**, in the same order as
`metaindex.bin`. Each index block is an independently zstd-compressed payload.

Decompressing an index block (at the offset/size from a `metaindexRow`)
yields a flat byte stream of `blockHeadersCount` `blockHeader` rows, packed
back-to-back, **sorted by `firstItem`**.

Writer flushes an index block when its accumulated unpacked size would exceed
`maxIndexBlockSize` (= **65,536 bytes**, `lib/mergeset/block_stream_writer.go:166`).

### blockHeader byte layout

Source: `lib/mergeset/block_header.go:62-72`.

| Offset    | Field              | Encoding              | Notes                                      |
| --------- | ------------------ | --------------------- | ------------------------------------------ |
| 0         | `commonPrefix`     | Bytes (varuint + raw) | Common byte prefix of every item in the block. May be empty. |
| varies    | `firstItem`        | Bytes (varuint + raw) | First item in the block (full bytes — `commonPrefix` is NOT stripped from this field). |
| varies    | `marshalType`      | uint8                 | 0 = plain, 1 = zstd. See §5.                 |
| +1        | `itemsCount`       | BE uint32             | Total items in the block, **including** `firstItem`. Must be > 0. |
| +4        | `itemsBlockOffset` | BE uint64             | Byte offset into `items.bin`.              |
| +8        | `lensBlockOffset`  | BE uint64             | Byte offset into `lens.bin`.               |
| +8        | `itemsBlockSize`   | BE uint32             | Byte length within `items.bin`. Max `2 * maxInmemoryBlockSize` = 128 KiB. |
| +4        | `lensBlockSize`    | BE uint32             | Byte length within `lens.bin`. Max `2 * 8 * maxInmemoryBlockSize` = 1 MiB. |

Validation rules: `lib/mergeset/block_header.go:140-148`.

## 5. `items.bin` + `lens.bin` — block payloads

Each `blockHeader` references one **block payload**, split across `items.bin`
(`itemsBlockOffset .. itemsBlockOffset + itemsBlockSize`) and `lens.bin`
(`lensBlockOffset .. lensBlockOffset + lensBlockSize`).

The interpretation depends on `marshalType`.

### 5.1 `marshalType = 0` (plain)

Used for small blocks where compression overhead would exceed the gain (logic
at `lib/mergeset/encoding.go:278-282`).

- **`items.bin` chunk:** concatenation of items 2..N, **with `commonPrefix`
  stripped from each item**. The bytes for the first item are NOT in
  `items.bin` — they live in `blockHeader.firstItem` and are reassembled by
  the reader.
- **`lens.bin` chunk:** sequence of `(itemsCount - 1)` **BE uint64** values,
  each giving the byte length of the corresponding item **after** stripping
  `commonPrefix`. So decoding item *i* (1-indexed against the items-2..N
  sequence) reads `lens[i]` bytes from the current cursor in the items chunk,
  prepends `commonPrefix`, and stores the result.

Sort invariant: items must remain in lex-sorted order after decoding
(`lib/mergeset/encoding.go:368`).

Reader: `lib/mergeset/encoding.go:505` (`unmarshalDataPlain`).

### 5.2 `marshalType = 1` (zstd)

Used when the compressed items chunk is materially smaller than the plain
form (the writer rejects this branch and reverts to plain if the compression
ratio is worse than 90%, `lib/mergeset/encoding.go:329`).

The **`items.bin` chunk** is a zstd-compressed payload. Decompressed, it is
the concatenation of items 2..N, but with **delta-prefix compression** layered
on top:

- Item *i* (1-indexed in 2..N) shares a prefix of length `prefixLens[i]` with
  item *i-1* (in its `commonPrefix`-stripped form). The shared prefix is
  omitted from the payload — only the differing suffix is encoded.

The **`lens.bin` chunk** is a zstd-compressed payload. Decompressed, it
contains **two arrays of varuint64s**, packed in order
(`lib/mergeset/encoding.go:399-413`):

1. **`prefixLensDelta[0..itemsCount]`** — XOR-delta of prefix lengths between
   adjacent items. `prefixLens[0] = 0`, then
   `prefixLens[i+1] = prefixLensDelta[i] ^ prefixLens[i]` for i in 0..N-2.
   First slot is always 0.
2. **`lensDelta[0..itemsCount]`** — XOR-delta of full item lengths (after
   `commonPrefix` strip). `lens[0] = len(firstItem) - len(commonPrefix)`,
   then `lens[i+1] = lensDelta[i] ^ lens[i]` for i in 0..N-2.

Both arrays have `itemsCount` slots; the first slot of each is a synthesised
constant (0 for prefix lengths, `len(firstItem) - len(commonPrefix)` for full
lengths), and the remaining `itemsCount - 1` slots come from `is.A[]` written
by the encoder. The pool function `encoding.GetUint64s(itemsCount - 1)`
(line 396) sizes the per-array allocation accordingly.

Reading procedure (`lib/mergeset/encoding.go:353-478`):

1. Decompress `lens.bin` chunk; deserialize `prefixLensDelta` then `lensDelta`.
2. Reconstruct `prefixLens[]` and `lens[]` via running XOR.
3. Decompress `items.bin` chunk into a single byte stream.
4. Walk items 1..N-1:
   - `prefixLen = prefixLens[i]`
   - `suffixLen = lens[i] - prefixLen`
   - item bytes = `commonPrefix || prevItem[..prefixLen] || items_chunk[..suffixLen]`
   - advance the items-chunk cursor by `suffixLen`.
5. Verify sort order; if not sorted, reject as corrupt (line 475).

## 6. Important constants

| Constant               | Value     | Source                                     |
| ---------------------- | --------- | ------------------------------------------ |
| `maxInmemoryBlockSize` | 64 KiB    | `lib/mergeset/encoding.go:184`             |
| `maxIndexBlockSize`    | 64 KiB    | `lib/mergeset/block_stream_writer.go:166`  |
| max `itemsBlockSize`   | 128 KiB   | `lib/mergeset/block_header.go:143-144`     |
| max `lensBlockSize`    | 1 MiB     | `lib/mergeset/block_header.go:146-147`     |
| max `indexBlockSize`   | 256 KiB   | `lib/mergeset/metaindex_row.go:75-79`      |

## 7. Sort invariants

Every level of sorting is validated by the reader. EsMetrics must preserve
all of them on the writer side:

1. Items within an `inmemoryBlock` are lex-sorted before encoding
   (`lib/mergeset/encoding.go:223-225,232-245`).
2. `blockHeader` rows within an index block are sorted by `firstItem`
   (`lib/mergeset/block_header.go:178-180`).
3. `metaindexRow` rows within `metaindex.bin` are sorted by `firstItem`
   (`lib/mergeset/metaindex_row.go:117-121`).
4. After decode, items in a single block must remain sorted; the reader
   verifies and rejects otherwise (`lib/mergeset/encoding.go:367-368, 475-477`).

## 8. Deliberate divergences

None. The mergeset format is byte-compatible across VM and EsMetrics by
design (ADR-001 #2). Any future divergence must land in
`docs/format/compat-deltas.md` with explicit rationale and be cross-linked
from here.

## 9. Test surface (Phase 1A.4)

The conformance harness must validate the following round-trip pairs:

1. **VM-writes → esm-reads** on a synthetic 10K-row workload; assert
   `Iterator` yields identical items.
2. **esm-writes → VM-reads** on the same workload; assert VM's
   `block_stream_reader` accepts the produced part without errors and
   produces identical items.
3. **Byte-equality** of resulting part files after a deterministic build
   (same input order, same compression level). This is the strongest
   compatibility guarantee.

## 10. References

| File                                                       | Subsystem                |
| ---------------------------------------------------------- | ------------------------ |
| `lib/mergeset/filenames.go`                                | Part directory contents  |
| `lib/mergeset/part_header.go`                              | `metadata.json` schema   |
| `lib/mergeset/metaindex_row.go`                            | `metaindex.bin` rows     |
| `lib/mergeset/block_header.go`                             | `index.bin` block headers|
| `lib/mergeset/encoding.go`                                 | `items.bin` + `lens.bin` block payloads |
| `lib/mergeset/block_stream_writer.go`                      | Writer state machine     |
| `lib/mergeset/block_stream_reader.go`                      | Reader state machine     |
| `lib/encoding/int.go:25,38,287,506`                        | Numeric + bytes encoding |
