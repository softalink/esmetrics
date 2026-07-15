//! Config parsing, validation, and lowering into the runtime aggregator
//! parameters. Ports the `Config`/`Options` structs and `newAggregator`'s
//! validation from `streamaggr.go`.

use std::collections::HashSet;

use esm_relabel::{parse_relabel_configs, IfExpression, Label, ParsedConfigs};
use serde::{Deserialize, Serialize};

use crate::godur::{parse_duration_millis, parse_duration_nanos};
use crate::outputs::OutputKind;
use crate::Error;

const SECOND_MS: i64 = 1000;

/// Optional global settings applied to every aggregator, overridable
/// per-config. Ports `streamaggr.Options`.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Default de-duplication interval in milliseconds (0 = disabled).
    pub dedup_interval_ms: i64,
    /// Labels to drop from every sample before dedup/aggregation.
    pub drop_input_labels: Vec<String>,
    /// Disable aligning flushes to the aggregation interval.
    pub no_align_flush_to_interval: bool,
    /// Flush incomplete state on startup and shutdown.
    pub flush_on_shutdown: bool,
    /// Leave metric names unchanged (no aggregation suffix).
    pub keep_metric_names: bool,
    /// Ignore samples older than the current aggregation interval.
    pub ignore_old_samples: bool,
    /// Number of initial aggregation intervals to ignore.
    pub ignore_first_intervals: usize,
    /// Keep all input samples in addition to the aggregated output.
    pub keep_input: bool,
    /// Enable the experimental blue/green aggregation-window mode.
    pub enable_windows: bool,
}

/// A `match` selector: a series-selector string or list. Empty matches all.
/// Ports `promrelabel.IfExpression` in `Config.Match`.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(untagged)]
enum MatchRaw {
    #[default]
    Empty,
    Single(String),
    Multi(Vec<String>),
}

/// Compiled `match`.
pub(crate) struct MatchExpr {
    exprs: Vec<IfExpression>,
}

impl MatchExpr {
    /// Returns true if `labels` matches the selector (or the selector is
    /// empty). Ports `IfExpression.Match` with an empty-selector shortcut.
    pub(crate) fn matches(&self, labels: &[Label]) -> bool {
        self.exprs.is_empty() || self.exprs.iter().any(|e| e.matches(labels))
    }
}

/// One raw stream-aggregation config entry. Ports `streamaggr.Config`.
///
/// `#[serde(deny_unknown_fields)]` reproduces upstream's
/// `yaml.UnmarshalStrict`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, rename = "match", skip_serializing_if = "is_match_empty")]
    match_expr: MatchRaw,
    interval: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    no_align_flush_to_interval: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    flush_on_shutdown: Option<bool>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    dedup_interval: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    staleness_interval: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    ignore_first_sample_interval: String,
    outputs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    keep_metric_names: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ignore_old_samples: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ignore_first_intervals: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    by: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    without: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    drop_input_labels: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    input_relabel_configs: Vec<serde_yaml_ng::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    output_relabel_configs: Vec<serde_yaml_ng::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enable_windows: Option<bool>,
}

fn is_match_empty(m: &MatchRaw) -> bool {
    matches!(m, MatchRaw::Empty)
}

/// Runtime parameters for a single aggregator, produced by lowering a
/// validated [`Config`].
pub(crate) struct AggregatorParams {
    pub(crate) matcher: MatchExpr,
    pub(crate) drop_input_labels: Vec<String>,
    pub(crate) input_relabeling: Option<ParsedConfigs>,
    pub(crate) output_relabeling: Option<ParsedConfigs>,
    pub(crate) keep_metric_names: bool,
    pub(crate) ignore_old_samples: bool,
    /// Whether the experimental blue/green window mode is on for this aggregator.
    pub(crate) enable_windows: bool,
    /// `enable_windows && dedup_interval_ms <= 0`: the aggr-output map keeps a
    /// separate green buffer sharing the counter outputs' cross-window state.
    /// Ports `useSharedState`.
    pub(crate) use_shared_state: bool,
    pub(crate) by: Vec<String>,
    pub(crate) without: Vec<String>,
    pub(crate) aggregate_only_by_time: bool,
    pub(crate) interval_ms: i64,
    pub(crate) dedup_interval_ms: i64,
    pub(crate) staleness_interval_ms: i64,
    pub(crate) name_suffix: String,
    pub(crate) outputs: Vec<OutputKind>,
    pub(crate) ignore_first_intervals: usize,
    pub(crate) align_flush_to_interval: bool,
    pub(crate) flush_on_shutdown: bool,
    pub(crate) metrics: crate::metrics::AggrMetrics,
}

/// Parses the top-level YAML config document into raw configs, plus a
/// canonical JSON signature used by `Aggregators::equal`. Ports the
/// `yaml.UnmarshalStrict` + `json.Marshal(cfgs)` steps of `loadFromData`.
pub(crate) fn parse_configs(data: &str) -> Result<(Vec<Config>, String), Error> {
    let cfgs: Vec<Config> = if data.trim().is_empty() {
        Vec::new()
    } else {
        serde_yaml_ng::from_str(data)
            .map_err(|e| Error::new(format!("cannot parse stream aggregation config: {e}")))?
    };
    let signature = serde_json::to_string(&cfgs)
        .map_err(|e| Error::new(format!("cannot marshal configs: {e}")))?;
    Ok((cfgs, signature))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Lowers a validated config into runtime params. Ports `newAggregator`'s
/// validation and setup. `aggr_id` is the 1-based config position, used in
/// the self-monitoring metric labels.
pub(crate) fn build_params(
    cfg: &Config,
    opts: &Options,
    aggr_id: usize,
) -> Result<AggregatorParams, Error> {
    // interval
    if cfg.interval.is_empty() {
        return Err(Error::new("missing `interval` option"));
    }
    let interval_ns = parse_duration_nanos(&cfg.interval)
        .map_err(|e| Error::new(format!("cannot parse `interval: {:?}`: {e}", cfg.interval)))?;
    if interval_ns < 1_000_000_000 {
        return Err(Error::new(format!(
            "aggregation interval cannot be smaller than 1s; got {}",
            cfg.interval
        )));
    }
    let interval_ms = interval_ns / 1_000_000;

    // dedup_interval
    let mut dedup_interval_ms = opts.dedup_interval_ms;
    if !cfg.dedup_interval.is_empty() {
        dedup_interval_ms = parse_duration_millis(&cfg.dedup_interval).map_err(|e| {
            Error::new(format!(
                "cannot parse `dedup_interval: {:?}`: {e}",
                cfg.dedup_interval
            ))
        })?;
    }
    if dedup_interval_ms > interval_ms {
        return Err(Error::new(format!(
            "dedup_interval={} cannot exceed interval={}",
            cfg.dedup_interval, cfg.interval
        )));
    }
    if dedup_interval_ms > 0 && interval_ms % dedup_interval_ms != 0 {
        return Err(Error::new(format!(
            "interval={} must be a multiple of dedup_interval={}",
            cfg.interval, cfg.dedup_interval
        )));
    }

    // staleness_interval (default = interval)
    let mut staleness_interval_ms = interval_ms;
    if !cfg.staleness_interval.is_empty() {
        staleness_interval_ms = parse_duration_millis(&cfg.staleness_interval).map_err(|e| {
            Error::new(format!(
                "cannot parse `staleness_interval: {:?}`: {e}",
                cfg.staleness_interval
            ))
        })?;
        if staleness_interval_ms < interval_ms {
            return Err(Error::new(format!(
                "staleness_interval={} cannot be smaller than interval={}",
                cfg.staleness_interval, cfg.interval
            )));
        }
    }

    // ignore_first_sample_interval (default = staleness_interval)
    let mut ignore_first_sample_ms = staleness_interval_ms;
    if !cfg.ignore_first_sample_interval.is_empty() {
        ignore_first_sample_ms =
            parse_duration_millis(&cfg.ignore_first_sample_interval).map_err(|e| {
                Error::new(format!(
                    "cannot parse `ignore_first_sample_interval: {:?}`: {e}",
                    cfg.ignore_first_sample_interval
                ))
            })?;
    }
    let ignore_first_sample_secs = (ignore_first_sample_ms / SECOND_MS).max(0) as u64;
    let ignore_first_sample_deadline = now_secs() + ignore_first_sample_secs;

    // drop_input_labels
    let drop_input_labels = match &cfg.drop_input_labels {
        Some(v) => v.clone(),
        None => opts.drop_input_labels.clone(),
    };

    // relabel configs
    let input_relabeling = parse_relabel(&cfg.input_relabel_configs, "input_relabel_configs")?;
    let output_relabeling = parse_relabel(&cfg.output_relabel_configs, "output_relabel_configs")?;

    // by / without
    let by = sort_and_dedup(&cfg.by);
    let without = sort_and_dedup(&cfg.without);
    if !by.is_empty() && !without.is_empty() {
        return Err(Error::new(
            "`by` and `without` lists cannot be set simultaneously",
        ));
    }
    let aggregate_only_by_time = by.is_empty() && without.is_empty();
    let by = if !aggregate_only_by_time && without.is_empty() {
        add_missing_underscore_name(&by)
    } else {
        by
    };

    // keep_metric_names
    let keep_metric_names = cfg.keep_metric_names.unwrap_or(opts.keep_metric_names);
    if keep_metric_names {
        if opts.keep_input {
            return Err(Error::new(
                "`-streamAggr.keepInput` and `keep_metric_names` options can't be enabled at the same time",
            ));
        }
        if cfg.outputs.len() != 1 {
            return Err(Error::new(format!(
                "`outputs` list must contain only a single entry if `keep_metric_names` is set; got {:?}",
                cfg.outputs
            )));
        }
        let o = &cfg.outputs[0];
        if o == "histogram_bucket" || (o.starts_with("quantiles(") && o.contains(',')) {
            return Err(Error::new(format!(
                "`keep_metric_names` cannot be applied to `outputs: {:?}`, since they can generate multiple time series",
                cfg.outputs
            )));
        }
    }

    let ignore_old_samples = cfg.ignore_old_samples.unwrap_or(opts.ignore_old_samples);
    let ignore_first_intervals = cfg
        .ignore_first_intervals
        .unwrap_or(opts.ignore_first_intervals);
    let enable_windows = cfg.enable_windows.unwrap_or(opts.enable_windows);
    // Shared blue/green output state is only needed when de-duplication is
    // disabled; otherwise the dedup layer owns the blue/green split. Ports
    // `useSharedState := enableWindows && useInputKey`.
    let use_shared_state = enable_windows && dedup_interval_ms <= 0;

    // outputs
    if cfg.outputs.is_empty() {
        return Err(Error::new(
            "`outputs` list must contain at least a single entry",
        ));
    }
    // Self-monitoring counters, labeled by the aggregator's name + position.
    let metric_name = match &cfg.name {
        Some(n) if !n.is_empty() => n.as_str(),
        _ => "none",
    };
    let metric_labels = format!("name={metric_name:?},position=\"{aggr_id}\"");
    let metrics = crate::metrics::AggrMetrics::new(&metric_labels);

    let mut seen: HashSet<String> = HashSet::new();
    let mut outputs = Vec::with_capacity(cfg.outputs.len());
    for output in &cfg.outputs {
        outputs.push(parse_output(
            output,
            &mut seen,
            ignore_first_sample_deadline,
            metrics.counter_resets,
        )?);
    }

    // name suffix
    let mut name_suffix = format!(":{}", cfg.interval);
    let by_labels = remove_underscore_name(&by);
    if !by_labels.is_empty() {
        name_suffix.push_str(&format!("_by_{}", by_labels.join("_")));
    }
    let without_labels = remove_underscore_name(&without);
    if !without_labels.is_empty() {
        name_suffix.push_str(&format!("_without_{}", without_labels.join("_")));
    }
    name_suffix.push('_');

    let align_flush_to_interval = match cfg.no_align_flush_to_interval {
        Some(v) => !v,
        None => !opts.no_align_flush_to_interval,
    };
    let flush_on_shutdown = match cfg.flush_on_shutdown {
        Some(v) => v,
        None => opts.flush_on_shutdown,
    };

    let matcher = compile_match(&cfg.match_expr)?;

    Ok(AggregatorParams {
        matcher,
        drop_input_labels,
        input_relabeling,
        output_relabeling,
        keep_metric_names,
        ignore_old_samples,
        enable_windows,
        use_shared_state,
        by,
        without,
        aggregate_only_by_time,
        interval_ms,
        dedup_interval_ms,
        staleness_interval_ms,
        name_suffix,
        outputs,
        ignore_first_intervals,
        align_flush_to_interval,
        flush_on_shutdown,
        metrics,
    })
}

/// Parses one output name into an [`OutputKind`], enforcing the
/// duplicate/quantiles rules. Ports `newOutputConfig`.
fn parse_output(
    output: &str,
    seen: &mut HashSet<String>,
    ignore_first_sample_deadline: u64,
    counter_resets: &'static esm_common::metrics::Counter,
) -> Result<OutputKind, Error> {
    if seen.contains(output) {
        return Err(Error::new(format!(
            "`outputs` list contains duplicate aggregation function: {output}"
        )));
    }
    seen.insert(output.to_string());

    if let Some(inner) = output.strip_prefix("quantiles(") {
        let Some(args_str) = inner.strip_suffix(')') else {
            return Err(Error::new("missing closing brace for `quantiles()` output"));
        };
        if args_str.is_empty() {
            return Err(Error::new("`quantiles()` must contain at least one phi"));
        }
        let mut phis = Vec::new();
        for arg in args_str.split(',') {
            let arg = arg.trim();
            let phi: f64 = arg.parse().map_err(|_| {
                Error::new(format!(
                    "cannot parse phi={arg:?} for quantiles({args_str})"
                ))
            })?;
            if !(0.0..=1.0).contains(&phi) {
                return Err(Error::new(format!(
                    "phi inside quantiles({args_str}) must be in the range [0..1]; got {phi}"
                )));
            }
            phis.push(phi);
        }
        if seen.contains("quantiles") {
            return Err(Error::new(
                "`outputs` list contains duplicated `quantiles()` function, please combine multiple phi* like `quantiles(0.5, 0.9)`",
            ));
        }
        seen.insert("quantiles".to_string());
        return Ok(OutputKind::Quantiles(phis));
    }

    let d = ignore_first_sample_deadline;
    Ok(match output {
        "avg" => OutputKind::Avg,
        "count_samples" => OutputKind::CountSamples,
        "count_series" => OutputKind::CountSeries,
        "histogram_bucket" => OutputKind::HistogramBucket,
        "increase" => OutputKind::Increase {
            keep_first_sample: true,
            ignore_first_sample_deadline: d,
            counter_resets,
        },
        "increase_prometheus" => OutputKind::Increase {
            keep_first_sample: false,
            ignore_first_sample_deadline: d,
            counter_resets,
        },
        "last" => OutputKind::Last,
        "max" => OutputKind::Max,
        "min" => OutputKind::Min,
        "rate_avg" => OutputKind::Rate {
            is_avg: true,
            counter_resets,
        },
        "rate_sum" => OutputKind::Rate {
            is_avg: false,
            counter_resets,
        },
        "stddev" => OutputKind::Stddev,
        "stdvar" => OutputKind::Stdvar,
        "sum_samples" => OutputKind::SumSamples {
            reset_on_flush: true,
        },
        "sum_samples_total" => OutputKind::SumSamples {
            reset_on_flush: false,
        },
        "total" => OutputKind::Total {
            keep_first_sample: true,
            ignore_first_sample_deadline: d,
            counter_resets,
        },
        "total_prometheus" => OutputKind::Total {
            keep_first_sample: false,
            ignore_first_sample_deadline: d,
            counter_resets,
        },
        "unique_samples" => OutputKind::UniqueSamples,
        other => {
            return Err(Error::new(format!("unsupported output={other:?}")));
        }
    })
}

fn parse_relabel(
    values: &[serde_yaml_ng::Value],
    field: &str,
) -> Result<Option<ParsedConfigs>, Error> {
    if values.is_empty() {
        return Ok(None);
    }
    let yaml = serde_yaml_ng::to_string(values)
        .map_err(|e| Error::new(format!("cannot re-encode {field}: {e}")))?;
    let raw = parse_relabel_configs(&yaml)
        .map_err(|e| Error::new(format!("cannot parse {field}: {}", e.msg)))?;
    let parsed = ParsedConfigs::from_raw_configs(raw)
        .map_err(|e| Error::new(format!("cannot parse {field}: {}", e.msg)))?;
    Ok(Some(parsed))
}

fn compile_match(m: &MatchRaw) -> Result<MatchExpr, Error> {
    let selectors: Vec<&str> = match m {
        MatchRaw::Empty => Vec::new(),
        MatchRaw::Single(s) => vec![s.as_str()],
        MatchRaw::Multi(v) => v.iter().map(|s| s.as_str()).collect(),
    };
    let mut exprs = Vec::with_capacity(selectors.len());
    for s in selectors {
        let e = IfExpression::parse(s)
            .map_err(|e| Error::new(format!("cannot parse `match: {s:?}`: {}", e.msg)))?;
        exprs.push(e);
    }
    Ok(MatchExpr { exprs })
}

fn sort_and_dedup(a: &[String]) -> Vec<String> {
    if a.is_empty() {
        return Vec::new();
    }
    let mut v = a.to_vec();
    v.sort();
    v.dedup();
    v
}

fn add_missing_underscore_name(labels: &[String]) -> Vec<String> {
    let mut result = vec!["__name__".to_string()];
    for s in labels {
        if s != "__name__" {
            result.push(s.clone());
        }
    }
    result
}

fn remove_underscore_name(labels: &[String]) -> Vec<String> {
    labels
        .iter()
        .filter(|s| *s != "__name__")
        .cloned()
        .collect()
}
