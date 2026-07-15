# esmalert-tool

A Rust port of upstream VictoriaMetrics
[`vmalert-tool`](https://docs.victoriametrics.com/victoriametrics/vmalert-tool/):
an offline unit-test runner for `esmalert`/`vmalert` rule files (the
`promtool test rules` analog). It reads one or more YAML "unit test" files,
each of which declares synthetic `input_series`, the rule files to evaluate
against them, and assertions about the resulting alerts and/or MetricsQL
expression results — no live datasource, remote-write target, or
Alertmanager required.

For every test file, this tool starts a **real, in-process `esmetrics`
storage + query engine** (fresh per file, torn down after), ingests the
file's `input_series` into it, drives `esmalert`'s real rule-evaluation loop
(`Group::eval_once`) against it tick by tick, and checks each assertion
against the actual evaluation state. This is not a mock or a simplified
re-implementation of rule semantics — it exercises the same rule-group
building, alert state machine, and MetricsQL query path `esmalert` itself
uses.

## Build

```bash
cargo build --release -p esmalert-tool
```

## Usage

```bash
esmalert-tool unittest mytest.yml [more.yml ...]
```

Exit codes: `0` if every test in every file passed, `1` if any assertion
failed (or a file-level error occurred, e.g. bad YAML or a missing rule
file), `2` on a usage error (no subcommand, unknown subcommand, or
`unittest` given no files).

## Test file format

```yaml
# mytest.yml
rule_files:
  - alerts.yml              # glob patterns, resolved relative to the CWD
evaluation_interval: 1m      # default eval-loop tick spacing; defaults to 1m if omitted
group_eval_order:            # optional: evaluate named rule groups in this order
  - example
tests:
  - name: high load fires after for
    interval: 1m              # input_series sample cadence for this group (defaults to evaluation_interval)
    external_labels:          # merged into every rule's labels for this test group
      cluster: prod
    input_series:
      - series: 'node_load1{instance="h1"}'
        values: '5 5 5 5 5'    # space-separated; supports `a+bxN`/`axN` expansion, `_` gaps, `stale`
    alert_rule_test:
      - eval_time: 2m
        groupname: example
        alertname: HighLoad
        exp_alerts:
          - exp_labels: { severity: warning, instance: h1 }
            exp_annotations: { summary: "h1 load is 5" }
    metricsql_expr_test:
      - expr: job:up:avg
        eval_time: 1m
        exp_samples:
          - labels: 'job:up:avg{job="x"}'
            value: 1
```

- `rule_files` must resolve to at least one rule group, even for a test
  file that only uses `metricsql_expr_test` — matching upstream.
- `input_series` value notation: a bare number, `_` (an omitted point — no
  sample is ingested for that timestamp, but it still advances the
  timestamp), `stale` (a stale-marker sample), `axN`/`a+bxN` repeat/ramp
  expansion, and `inf`/`nan` (case-insensitive).
- `alert_rule_test.exp_alerts` matches only `Firing`-state alerts, by a
  set comparison (order doesn't matter) of labels + annotations;
  `alertgroup`/`alertname` are added to the expected labels automatically.
- `metricsql_expr_test` queries `expr` as an instant query at the exact
  `eval_time` offset (not floored to the nearest eval tick) after the whole
  eval loop has finished, and set-matches the results against `exp_samples`
  (label-set equality, small-epsilon value comparison).
- A test group's own `external_labels` (deprecated upstream, still
  supported here) are merged into every rule's labels for that group,
  alongside `-external.label`-equivalent labels — see Limitations below.

## Limitations

**Not implemented:**
- Only the `unittest` subcommand exists — matching upstream, which has no
  other subcommands either.
- The top-level `-external.label` CLI flag (file-level external labels
  applied across every test group) isn't wired up yet; a test group's own
  `external_labels` field works as documented above.
- `group_eval_order` duplicate-name validation: upstream errors if the same
  group name appears twice in `group_eval_order`; this port doesn't reject
  that — a duplicate's sort key silently resolves to its first occurrence's
  index.
- The `query()` template function inside a rule's `labels`/`annotations`
  (`{{ query "..." }}`) is evaluated against **no data** during a unit test:
  the tool renders annotations with an empty query result, so a rule whose
  annotations call `query()` will produce a spurious assertion diff. (A
  faithful fix needs an eval-time-aware datasource query in the template
  context; deferred. Static annotations and `{{ $value }}`/`{{ $labels.x }}`
  work normally.)

**Inherited from `esmalert`:** the Go-template subset used to render
`labels`/`annotations` doesn't support time/duration **method-call** syntax
(`.Add`/`.Sub`/`.UnixMilli`) — a rule using that pattern fails rule
validation. See
[crates/esmalert/README.md](../esmalert/README.md#limitations) for details.

**Multi-file batches:** `esmalert-tool unittest a.yml b.yml` runs each file
in order. A *hard* error in one file — unreadable file, malformed YAML, an
invalid/missing `rule_files` entry, or a harness that fails to start —
aborts the whole run immediately (no later file runs), matching upstream's
`logger.Fatalf` behavior for the equivalent failures. Assertion failures
(a file that runs fine but has a wrong expectation) are not fatal: they're
collected and reported per file, and later files in the batch still run.
