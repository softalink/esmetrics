# esm-relabel

A from-scratch Rust port of upstream VictoriaMetrics'
[`lib/promrelabel`](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/lib/promrelabel)
relabel engine: parses `relabel_configs`-style YAML and applies it to a
series' label set. Used by [`esmagent`](../esmagent/README.md) for its
global and per-destination relabel configs.

## Usage

```rust
use esm_relabel::{Label, ParsedConfigs};

let cfgs = ParsedConfigs::parse(
    "- source_labels: [__name__]\n  regex: \"debug_.*\"\n  action: drop\n",
)?;

let mut labels = vec![Label { name: "__name__".into(), value: "debug_cpu".into() }];
let kept = cfgs.apply(&mut labels); // false: this series is dropped
```

`ParsedConfigs::apply` mutates `labels` in place and returns `false` as soon
as a `keep`/`drop`-family action drops the series (later configs in the same
list are not applied, matching upstream's early-return behavior).

## Config format

```yaml
- source_labels: [__name__, job]
  separator: ";"
  regex: "(cpu_.*);(prod)"
  target_label: dc
  replacement: "us-east"
  action: replace
  if: 'up{job="node"}'
```

### Supported actions

`replace` (default) · `replace_all` · `keep` · `drop` · `keepequal` ·
`dropequal` · `keep_if_equal` · `drop_if_equal` · `keep_if_contains` ·
`drop_if_contains` · `keep_metrics` · `drop_metrics` · `labelmap` ·
`labelmap_all` · `labeldrop` · `labelkeep` · `hashmod` · `lowercase` ·
`uppercase` · `graphite`. Action names are matched case-insensitively,
matching upstream's `strings.ToLower(rc.Action)`.

`if:` gates a config on a label selector before it's applied — a non-match
skips the config, except for `action: keep`, where a non-match drops the
series (vmagent's idiom for "keep only series matching this selector, drop
everything else").

## Limitations

- **Single-series API.** This crate applies one series' labels at a time
  (`ParsedConfigs::apply(&mut Vec<Label>)`); it does not port the
  whole-scrape-target multi-series batching upstream's `promrelabel` does
  for a scrape job's full label set at once. `esmagent` calls `apply` once
  per series, which is the only mode this port's forwarding-only scope
  needs — there's no scrape target to batch across.
- Every relabel action described above is implemented; there is no known
  gap versus upstream's action set as of this port. If a config parses
  successfully here, it behaves the same as upstream for the actions above.
