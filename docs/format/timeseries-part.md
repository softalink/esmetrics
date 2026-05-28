# Time-series part — on-disk format

Canonical EsMetrics specification for the byte layout of a **time-series part**
as produced by VictoriaMetrics v1.144.0 (`lib/storage/`). Source of truth for
the Rust reader and writer in `esm-storage::timeseries`.

References use the shorthand `lib/storage/<file>.go:<line>` against the
upstream pin v1.144.0.

## 1. Conventions

Same as [`mergeset-part.md`](mergeset-part.md) §0:
- Fixed-width ints are **big-endian**.
- Signed ints are big-endian two's-complement (Go's `MarshalInt16` / `MarshalInt64`).
- Varuints + signed-varints (zig-zag) match VM's helpers in `lib/encoding/int.go`.

## 2. Part directory layout

A time-series part is a directory with **four** binary files plus
`metadata.json`. The mergeset's `lens.bin` does **not** exist here; instead
there are paired `timestamps.bin` + `values.bin` whose blocks are independently
encoded under the [`esm_compress::timeseries`] codec.

| File              | Purpose                                                             |
| ----------------- | ------------------------------------------------------------------- |
| `metadata.json`   | Whole-part summary (rows, blocks, MinTimestamp / MaxTimestamp, etc).|
| `metaindex.bin`   | Zstd-compressed sequence of `metaindexRow` entries (groups index blocks). |
| `index.bin`       | Concatenated, zstd-compressed *index blocks*. Each holds N fixed-size `blockHeader`s. |
| `timestamps.bin`  | Per-block compressed timestamps payload (encoded via [`esm_compress::timeseries`]). |
| `values.bin`      | Per-block compressed values payload (same codec, different precision possibly). |

## 3. `metadata.json`

VM marshals via `lib/storage/part_header.go`. Field list:

```json
{
  "RowsCount": <uint64>,
  "BlocksCount": <uint64>,
  "MinTimestamp": <int64>,
  "MaxTimestamp": <int64>
}
```

All four fields are required and validated by the loader (rows > 0, blocks
> 0, blocks ≤ rows, min ≤ max).

## 4. `metaindex.bin`

Identical layout to the mergeset's `metaindex.bin` (§3 of `mergeset-part.md`)
**but with three additional fields per row**: the metaindex carries the TSID
range of every index block.

Per-row layout (single zstd frame over the whole file decomposes into rows):

| Field                  | Encoding              |
| ---------------------- | --------------------- |
| `firstTSID` (24 bytes) | TSID (raw, BE, see §6)|
| `blockHeadersCount`    | BE uint32             |
| `minTimestamp`         | BE int64              |
| `maxTimestamp`         | BE int64              |
| `indexBlockOffset`     | BE uint64             |
| `indexBlockSize`       | BE uint32             |

(Layout reconstructed from VM `lib/storage/metaindex_row.go`. Verify against
that source when implementing 1C.)

## 5. `index.bin`

Concatenation of independently zstd-compressed **index blocks**. Each
decompressed index block holds `blockHeadersCount` fixed-size `blockHeader`
rows packed back-to-back, **sorted by TSID** (ties broken by MinTimestamp).

### blockHeader byte layout (fixed 81 bytes)

Source: `lib/storage/block_header.go:103-115`.

| Offset | Field                    | Encoding   |
| ------ | ------------------------ | ---------- |
| 0      | `TSID` (24 bytes)        | See §6     |
| 24     | `MinTimestamp`           | BE int64   |
| 32     | `MaxTimestamp`           | BE int64   |
| 40     | `FirstValue`             | BE int64   |
| 48     | `TimestampsBlockOffset`  | BE uint64  |
| 56     | `ValuesBlockOffset`      | BE uint64  |
| 64     | `TimestampsBlockSize`    | BE uint32  |
| 68     | `ValuesBlockSize`        | BE uint32  |
| 72     | `RowsCount`              | BE uint32  |
| 76     | `Scale`                  | BE int16   |
| 78     | `TimestampsMarshalType`  | uint8 (1..6) |
| 79     | `ValuesMarshalType`      | uint8 (1..6) |
| 80     | `PrecisionBits`          | uint8 (1..64) |

VM caches `marshaledBlockHeaderSize` at startup; EsMetrics will use the
compile-time constant `81`.

### Validation rules (`lib/storage/block_header.go:232-255`)

- `RowsCount > 0`.
- `RowsCount <= 2 * maxRowsPerBlock` (VM constant; mirrors EsMetrics
  `MAX_ROWS_PER_BLOCK = 8192` once we lift it from `lib/storage/`).
- `TimestampsMarshalType`, `ValuesMarshalType` in `[1, 6]`.
- `PrecisionBits` in `[1, 64]`.
- `TimestampsBlockSize`, `ValuesBlockSize <= 2 * maxBlockSize` (VM's
  `lib/storage/encoding.go` per-block size guard, ~256 KiB).

## 6. `TSID` byte layout (24 bytes)

Source: `lib/storage/tsid.go:62-86`.

| Offset | Field            | Encoding  |
| ------ | ---------------- | --------- |
| 0      | `MetricGroupID`  | BE uint64 |
| 8      | `JobID`          | BE uint32 |
| 12     | `InstanceID`     | BE uint32 |
| 16     | `MetricID`       | BE uint64 |

`MetricID` is the unique-per-time-series identifier; the other fields are
optional grouping hints. Ordering is lexicographic over the 24 bytes.

## 7. `timestamps.bin` + `values.bin` — block payloads

Each `blockHeader` references a paired payload across the two files.
Both files are concatenations of variable-size compressed blocks; sizes and
offsets live in the `blockHeader`.

Each block is encoded by [`esm_compress::timeseries::marshal_int64_array`]
(VM `lib/encoding/encoding.go`):

- Timestamps and values are independently encoded.
- The first timestamp / first value live in the `blockHeader`
  (`MinTimestamp` / `FirstValue` respectively) and **are not** stored in the
  payload — they seed the decode.
- `RowsCount` items are encoded; the decoder produces `RowsCount`
  consecutive int64s.
- Scaling: values are integer-scaled by `10^Scale` to recover floats; the
  decimal layer (`lib/decimal`) handles this above the codec.

## 8. Sort invariants

1. Items within a block are sorted by timestamp (`lib/storage/block.go`).
2. Block headers within an index block are sorted by `TSID` (ties broken by
   `MinTimestamp`); `unmarshalBlockHeaders` validates and rejects otherwise
   (`block_header.go:284-287`).
3. Metaindex rows within `metaindex.bin` are sorted by `firstTSID`.

## 9. Test surface (Phase 1C)

1. **VM-writes → esm-reads** on a synthetic single-series block (random
   timestamps + values), then on multi-series multi-block.
2. **esm-writes → VM-reads** reverse direction.
3. **Byte-equality** of part files for a deterministic input order +
   compression level.

## 10. References

| File                                            | Subsystem           |
| ----------------------------------------------- | ------------------- |
| `lib/storage/part.go`                           | Part open/close     |
| `lib/storage/part_header.go`                    | metadata.json       |
| `lib/storage/metaindex_row.go`                  | metaindex layout    |
| `lib/storage/block_header.go`                   | per-block header    |
| `lib/storage/block.go`                          | Block container     |
| `lib/storage/tsid.go`                           | TSID                |
| `lib/storage/block_stream_writer.go`            | Writer state machine|
| `lib/storage/block_stream_reader.go`            | Reader state machine|
| `lib/encoding/encoding.go`                      | Timestamps + values codec |
