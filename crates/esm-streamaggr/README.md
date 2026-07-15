# esm-streamaggr

A Rust port of `github.com/VictoriaMetrics/lib/streamaggr` (VictoriaMetrics
v1.146.0) — the streaming-aggregation engine that powers vmagent's
`-remoteWrite.streamAggr.config` and the single-node `-streamAggr.config`.

Incoming samples are matched, relabeled, grouped by an output label set, and
aggregated in memory; once per configured `interval` the aggregated series are
flushed to a push callback.

## What is ported

- **Config surface** (`Config`/`Options`): `interval`, `by`/`without`,
  `dedup_interval`, `staleness_interval`, `ignore_first_sample_interval`,
  `keep_metric_names`, `ignore_old_samples`, `ignore_first_intervals`,
  `flush_on_shutdown`, `no_align_flush_to_interval`, `drop_input_labels`,
  `input_relabel_configs`, `output_relabel_configs`, `enable_windows`, and
  `match`. Config parsing is strict (`yaml.UnmarshalStrict` →
  `#[serde(deny_unknown_fields)]`) and reproduces every validation error in
  upstream's `newAggregator`.
- **All 18 outputs**: `avg`, `count_samples`, `count_series`,
  `histogram_bucket`, `increase`, `increase_prometheus`, `last`, `max`, `min`,
  `quantiles(phi…)`, `rate_avg`, `rate_sum`, `stddev`, `stdvar`, `sum_samples`,
  `sum_samples_total`, `total`, `total_prometheus`, `unique_samples`.
  `histogram_bucket` ports VictoriaMetrics' `vmrange` bucketing, `quantiles`
  ports the `valyala/histogram.Fast` estimator (including its deterministic
  reservoir sampling above 1000 samples).
- **De-duplication**: the last-sample-per-series `dedup_interval` path (with
  blue/green window buffers), and the standalone [`Deduplicator`] for HA
  de-duplication.
- **Aggregation windows** (`enable_windows`): the blue/green double-buffering
  that routes samples by their timestamp relative to the window boundary,
  delays each flush by the observed sample lag to catch late samples, and
  seeds the counter outputs' (`total`/`increase`/`rate`) cross-window state so
  their deltas stay continuous across window flips.
- **Self-monitoring counters** (`esm_streamaggr_*`): `matched_samples_total`,
  `ignored_samples_total` (`reason="nan"`/`"too_old"`), `output_samples_total`,
  `flush_timeouts_total`, `counter_resets_total`, and
  `dedup_dropped_samples_total`, labeled by the aggregator's `name`/`position`.
- **Output name suffixing**, `by`/`without` grouping, input/output relabeling,
  aligned/unaligned flushing, and `flush_on_shutdown`.

## Deliberate deviations / deferrals

- **Self-monitoring histograms and gauges** — only the *counter* subset of
  upstream's `vm_streamaggr_*` metrics is exposed (see "Ported"), because
  `esm_common::metrics` is counters-only. The histograms
  (`flush_duration_seconds`, `samples_lag_seconds`) and gauges (dedup/labels-
  compressor state sizes) have no counterpart here. Aggregation behaviour is
  unaffected.
- **Label dictionary compressor** — upstream's process-global
  `promutil.LabelsCompressor` is replaced by a plain round-tripping
  in-memory label-key encoding (`src/key.rs`). The key never leaves the
  process, so byte-format compatibility is a non-goal (per the porting rules);
  algorithmic behaviour (grouping, input/output key split) is identical.
- **Concurrency shape** — upstream fans `Push` across aggregators with a
  goroutine pool and shards the dedup map 128 ways behind cache-line padding.
  This port pushes to aggregators sequentially and uses a single mutex-guarded
  map per aggregator plus one dedup map per window buffer. Sharding/fan-out
  changes throughput, not results; stream aggregation is not on the TSBS hot
  path.
- **`Duration` parsing** uses a faithful port of Go's `time.ParseDuration`
  (`src/godur.rs`) rather than MetricsQL's duration grammar, so `1d`/`1w` and
  bare-number durations are rejected exactly as upstream rejects them.

The background flusher's exact sub-interval timing (the `synctest`
virtual-clock harness in `streamaggr_synctest_test.go`) is not ported; the
flush *logic* is ported faithfully and exercised via synchronous
push→flush tests.
