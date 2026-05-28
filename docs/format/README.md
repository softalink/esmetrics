# On-disk format documentation

Reverse-engineered specifications for the byte layouts EsMetrics must read and
write to maintain on-disk compatibility with VictoriaMetrics v1.144.0. Each
file in this directory is the canonical EsMetrics format spec for one
subsystem; conformance fixtures verify reality matches the spec.

Planned files (populated during Phase 1):
- `mergeset-part.md` — inverted-index part layout.
- `timeseries-part.md` — timestamps.bin / values.bin / index.bin / metaindex.bin.
- `timeseries-codecs.md` — Gorilla XOR, delta-of-delta, block-zstd.
- `indexdb.md` — TSID assignment + label-set lookup tables.
- `compat-deltas.md` — any *deliberate* deviations from VM, with rationale.
