# Conformance harness

Drives upstream **VictoriaMetrics v1.144.0** and **EsMetrics** side-by-side
against scenario YAML files, then diffs the outputs. The single most
important piece of infrastructure for the "drop-in compatibility" claim
(see [`PLAN.md` §8](../PLAN.md)).

## Layout

```
conformance/
├── harness/              # the `conformance-harness` binary
├── scenarios/            # scenario YAML files (small, text, in Git)
└── fixtures.lock.json    # manifest of (scenario, vm_tag) -> expected sha256
```

Fixtures themselves are **not** stored in Git (see PLAN.md §8.3). They are
regenerated on demand by running upstream VM at the pinned tag against the
scenario script, and cached locally under `target/conformance-cache/`.

## Usage

The harness is built as part of the workspace; invoke it via Cargo:

```sh
cargo run --release --bin conformance-harness -- list
cargo run --release --bin conformance-harness -- check
cargo run --release --bin conformance-harness -- dry-run smoke
cargo run --release --bin conformance-harness -- run smoke      # Phase 1+
```

Or use the wrapper alias (lands when xtask gains a `conformance` subcommand
during Phase 1).

## Phase status

Phase 0.6 ships the skeleton:
- Scenario YAML parser
- `list`, `check`, `dry-run` subcommands
- Empty `fixtures.lock.json`
- One trivial smoke scenario

End-to-end execution (Docker orchestration, HTTP drivers, output diffing,
on-disk part diffing) lands alongside Phase 1, when `esm-single` first becomes
capable of round-tripping real data.
