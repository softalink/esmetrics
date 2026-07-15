//! The 18 output aggregation functions and the flush context that turns
//! aggregated state into output series. Ports `output.go` plus the per-output
//! files (`avg.go`, `total.go`, `rate.go`, …).
//!
//! With the experimental windowing mode deferred, upstream's blue/green
//! shared-state split collapses to a single per-output-key state that
//! persists in the aggregator map across flushes, so each [`Output`] carries
//! both its immutable config and its mutable accumulators inline.

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::histogram::{Fast, VmHistogram};
use crate::key::decode_labels;
use crate::{Label, Sample, TimeSeries};

/// One matched sample handed to an output. The series identity travels
/// separately as `input_key`/`output_key` (see [`crate::key`]).
pub(crate) struct PushSample {
    pub(crate) value: f64,
    pub(crate) timestamp: i64,
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A per-output-key state cell that is either owned outright (the common
/// single-buffer case) or shared between the blue and green window buffers
/// via an `Arc<Mutex<_>>`. The mutex is always locked uncontended — access is
/// serialized by the aggregator's outer `outputs` lock — so the shared arm
/// only exists to let the two buffers reach the same allocation. Ports the
/// role of the `state()`/`getValue(state)` seeding in upstream's `output.go`.
pub(crate) enum SharedCell<T> {
    Owned(T),
    Shared(std::sync::Arc<std::sync::Mutex<T>>),
}

impl<T: Default> SharedCell<T> {
    fn new() -> SharedCell<T> {
        SharedCell::Owned(T::default())
    }

    /// Upgrades an owned cell to a shared one in place and returns a second
    /// handle onto the same allocation, for seeding the green buffer. Ports
    /// `getValue(nv.blue[idx].state())`.
    fn share(&mut self) -> SharedCell<T> {
        let arc = match std::mem::replace(self, SharedCell::Owned(T::default())) {
            SharedCell::Owned(t) => std::sync::Arc::new(std::sync::Mutex::new(t)),
            SharedCell::Shared(a) => a,
        };
        *self = SharedCell::Shared(std::sync::Arc::clone(&arc));
        SharedCell::Shared(arc)
    }
}

impl<T> SharedCell<T> {
    #[inline]
    fn with_mut<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> R {
        match self {
            SharedCell::Owned(t) => f(t),
            SharedCell::Shared(a) => f(&mut a.lock().unwrap()),
        }
    }
}

/// Per-input-series last-seen value for the counter-style outputs
/// (`total`/`increase`). Ports `totalLastValue`/`increaseLastValue`.
#[derive(Clone, Copy)]
pub(crate) struct LastValue {
    value: f64,
    timestamp: i64,
    delete_deadline: i64,
}

/// The cross-window shared state for `total`: the carried-over total plus the
/// per-input-series last values. Ports `totalAggrValueShared`.
#[derive(Default)]
pub(crate) struct TotalShared {
    shared_total: f64,
    last: HashMap<Vec<u8>, LastValue>,
}

/// The per-window accumulator half of a `rate` series (`increase` observed in
/// the current interval). Ports `rateAggrStateValue`.
#[derive(Clone, Copy, Default)]
pub(crate) struct RateState {
    increase: f64,
    timestamp: i64,
}

/// Per-input-series rate state. The cross-window fields (`value`,
/// `prev_timestamp`, `delete_deadline`) are shared, while `blue`/`green` hold
/// each window's in-progress increase. Ports `rateAggrSharedValue`.
#[derive(Clone, Copy)]
pub(crate) struct RateShared {
    value: f64,
    delete_deadline: i64,
    prev_timestamp: i64,
    blue: RateState,
    green: RateState,
}

impl RateShared {
    #[inline]
    fn state(&mut self, is_green: bool) -> &mut RateState {
        if is_green {
            &mut self.green
        } else {
            &mut self.blue
        }
    }
}

/// A single output's config + persistent state for one output key.
pub(crate) enum Output {
    Avg {
        sum: f64,
        count: f64,
    },
    CountSamples {
        count: u64,
    },
    CountSeries {
        hashes: HashSet<u64>,
    },
    HistogramBucket {
        shared: VmHistogram,
        h: VmHistogram,
    },
    Increase {
        keep_first_sample: bool,
        ignore_first_sample_deadline: u64,
        counter_resets: &'static esm_common::metrics::Counter,
        /// Per-window increase accumulator.
        total: Option<f64>,
        /// Cross-window last-value map (shared between blue/green).
        last: SharedCell<HashMap<Vec<u8>, LastValue>>,
    },
    Last {
        last: f64,
        timestamp: i64,
    },
    Max {
        max: f64,
        defined: bool,
    },
    Min {
        min: f64,
        defined: bool,
    },
    Quantiles {
        phis: Vec<f64>,
        h: Option<Fast>,
    },
    Rate {
        is_avg: bool,
        /// Which window buffer this instance reads/writes. Ports
        /// `rateAggrValue.isGreen`.
        is_green: bool,
        counter_resets: &'static esm_common::metrics::Counter,
        /// Cross-window per-series state (shared between blue/green).
        shared: SharedCell<HashMap<Vec<u8>, RateShared>>,
    },
    Std {
        is_deviation: bool,
        count: f64,
        avg: f64,
        q: f64,
    },
    SumSamples {
        reset_on_flush: bool,
        sum: f64,
    },
    Total {
        keep_first_sample: bool,
        ignore_first_sample_deadline: u64,
        counter_resets: &'static esm_common::metrics::Counter,
        /// Per-window delta accumulator.
        total: f64,
        /// Cross-window carried total + last-value map (shared blue/green).
        shared: SharedCell<TotalShared>,
    },
    UniqueSamples {
        values: HashSet<u64>,
    },
}

/// The immutable per-output config template. One `OutputKind` is built per
/// entry in the config's `outputs` list; [`OutputKind::new_state`] then
/// instantiates a fresh mutable [`Output`] for each output key. Ports
/// `newOutputConfig` / the `aggrConfig`s.
#[derive(Clone)]
pub(crate) enum OutputKind {
    Avg,
    CountSamples,
    CountSeries,
    HistogramBucket,
    Increase {
        keep_first_sample: bool,
        ignore_first_sample_deadline: u64,
        counter_resets: &'static esm_common::metrics::Counter,
    },
    Last,
    Max,
    Min,
    Quantiles(Vec<f64>),
    Rate {
        is_avg: bool,
        counter_resets: &'static esm_common::metrics::Counter,
    },
    Stddev,
    Stdvar,
    SumSamples {
        reset_on_flush: bool,
    },
    Total {
        keep_first_sample: bool,
        ignore_first_sample_deadline: u64,
        counter_resets: &'static esm_common::metrics::Counter,
    },
    UniqueSamples,
}

impl OutputKind {
    /// Creates a fresh mutable state for a new output key. Ports
    /// `aggrConfig.getValue(nil)`.
    pub(crate) fn new_state(&self) -> Output {
        match self {
            OutputKind::Avg => Output::Avg {
                sum: 0.0,
                count: 0.0,
            },
            OutputKind::CountSamples => Output::CountSamples { count: 0 },
            OutputKind::CountSeries => Output::CountSeries {
                hashes: HashSet::new(),
            },
            OutputKind::HistogramBucket => Output::HistogramBucket {
                shared: VmHistogram::new(),
                h: VmHistogram::new(),
            },
            OutputKind::Increase {
                keep_first_sample,
                ignore_first_sample_deadline,
                counter_resets,
            } => Output::Increase {
                keep_first_sample: *keep_first_sample,
                ignore_first_sample_deadline: *ignore_first_sample_deadline,
                counter_resets,
                total: None,
                last: SharedCell::new(),
            },
            OutputKind::Last => Output::Last {
                last: 0.0,
                timestamp: 0,
            },
            OutputKind::Max => Output::Max {
                max: 0.0,
                defined: false,
            },
            OutputKind::Min => Output::Min {
                min: 0.0,
                defined: false,
            },
            OutputKind::Quantiles(phis) => Output::Quantiles {
                phis: phis.clone(),
                h: None,
            },
            OutputKind::Rate {
                is_avg,
                counter_resets,
            } => Output::Rate {
                is_avg: *is_avg,
                is_green: false,
                counter_resets,
                shared: SharedCell::new(),
            },
            OutputKind::Stddev => Output::Std {
                is_deviation: true,
                count: 0.0,
                avg: 0.0,
                q: 0.0,
            },
            OutputKind::Stdvar => Output::Std {
                is_deviation: false,
                count: 0.0,
                avg: 0.0,
                q: 0.0,
            },
            OutputKind::SumSamples { reset_on_flush } => Output::SumSamples {
                reset_on_flush: *reset_on_flush,
                sum: 0.0,
            },
            OutputKind::Total {
                keep_first_sample,
                ignore_first_sample_deadline,
                counter_resets,
            } => Output::Total {
                keep_first_sample: *keep_first_sample,
                ignore_first_sample_deadline: *ignore_first_sample_deadline,
                counter_resets,
                total: 0.0,
                shared: SharedCell::new(),
            },
            OutputKind::UniqueSamples => Output::UniqueSamples {
                values: HashSet::new(),
            },
        }
    }

    /// Builds the green window buffer that pairs with a freshly-created blue
    /// state `blue`. For the counter outputs (`total`/`increase`/`rate`) the
    /// cross-window state is shared with blue (blue is upgraded to a shared
    /// cell in place); every other output gets an independent green
    /// accumulator. Ports `nv.green[idx] = ac.getValue(nv.blue[idx].state())`.
    pub(crate) fn new_green(&self, blue: &mut Output) -> Output {
        match blue {
            Output::Total {
                keep_first_sample,
                ignore_first_sample_deadline,
                counter_resets,
                shared,
                ..
            } => Output::Total {
                keep_first_sample: *keep_first_sample,
                ignore_first_sample_deadline: *ignore_first_sample_deadline,
                counter_resets,
                total: 0.0,
                shared: shared.share(),
            },
            Output::Increase {
                keep_first_sample,
                ignore_first_sample_deadline,
                counter_resets,
                last,
                ..
            } => Output::Increase {
                keep_first_sample: *keep_first_sample,
                ignore_first_sample_deadline: *ignore_first_sample_deadline,
                counter_resets,
                total: None,
                last: last.share(),
            },
            Output::Rate {
                is_avg,
                counter_resets,
                shared,
                ..
            } => Output::Rate {
                is_avg: *is_avg,
                is_green: true,
                counter_resets,
                shared: shared.share(),
            },
            // Stateless outputs: green is an independent accumulator.
            _ => self.new_state(),
        }
    }
}

impl Output {
    pub(crate) fn push_sample(&mut self, s: &PushSample, input_key: &[u8], delete_deadline: i64) {
        match self {
            Output::Avg { sum, count } => {
                *sum += s.value;
                *count += 1.0;
            }
            Output::CountSamples { count } => {
                *count += 1;
            }
            Output::CountSeries { hashes } => {
                // Count unique key hashes, matching upstream's memory tradeoff.
                hashes.insert(xxhash_rust::xxh64::xxh64(input_key, 0));
            }
            Output::HistogramBucket { h, .. } => {
                h.update(s.value);
            }
            Output::Last { last, timestamp } => {
                if s.timestamp >= *timestamp {
                    *last = s.value;
                    *timestamp = s.timestamp;
                }
            }
            Output::Max { max, defined } => {
                if s.value > *max || !*defined {
                    *max = s.value;
                }
                *defined = true;
            }
            Output::Min { min, defined } => {
                if s.value < *min || !*defined {
                    *min = s.value;
                }
                *defined = true;
            }
            Output::Quantiles { h, .. } => {
                h.get_or_insert_with(Fast::new).update(s.value);
            }
            Output::Std { count, avg, q, .. } => {
                *count += 1.0;
                let new_avg = *avg + (s.value - *avg) / *count;
                *q += (s.value - *avg) * (s.value - new_avg);
                *avg = new_avg;
            }
            Output::SumSamples { sum, .. } => {
                if sum.abs() >= (1u64 << 53) as f64 {
                    // Reset before float64 precision is lost.
                    *sum = 0.0;
                }
                *sum += s.value;
            }
            Output::UniqueSamples { values } => {
                values.insert(s.value.to_bits());
            }
            Output::Total {
                keep_first_sample,
                ignore_first_sample_deadline,
                counter_resets,
                total,
                shared,
            } => {
                let keep_first_sample = *keep_first_sample;
                let ignore_first_sample_deadline = *ignore_first_sample_deadline;
                shared.with_mut(|sh| {
                    push_counter(
                        total,
                        &mut sh.last,
                        s,
                        input_key,
                        delete_deadline,
                        keep_first_sample,
                        ignore_first_sample_deadline,
                        counter_resets,
                    );
                });
            }
            Output::Increase {
                keep_first_sample,
                ignore_first_sample_deadline,
                counter_resets,
                total,
                last,
            } => {
                let keep_first_sample = *keep_first_sample;
                let ignore_first_sample_deadline = *ignore_first_sample_deadline;
                let acc = total.get_or_insert(0.0);
                last.with_mut(|l| {
                    push_counter(
                        acc,
                        l,
                        s,
                        input_key,
                        delete_deadline,
                        keep_first_sample,
                        ignore_first_sample_deadline,
                        counter_resets,
                    );
                });
            }
            Output::Rate {
                is_green,
                counter_resets,
                shared,
                ..
            } => {
                let is_green = *is_green;
                shared.with_mut(|sh| {
                    push_rate(sh, is_green, s, input_key, delete_deadline, counter_resets);
                });
            }
        }
    }

    pub(crate) fn flush(&mut self, ctx: &mut FlushCtx, output_key: &[u8], is_last: bool) {
        match self {
            Output::Avg { sum, count } => {
                if *count > 0.0 {
                    ctx.append_series(output_key, "avg", *sum / *count);
                    *sum = 0.0;
                    *count = 0.0;
                }
            }
            Output::CountSamples { count } => {
                if *count > 0 {
                    ctx.append_series(output_key, "count_samples", *count as f64);
                    *count = 0;
                }
            }
            Output::CountSeries { hashes } => {
                if !hashes.is_empty() {
                    ctx.append_series(output_key, "count_series", hashes.len() as f64);
                    hashes.clear();
                }
            }
            Output::HistogramBucket { shared, h } => {
                shared.merge(h);
                h.reset();
                let mut ranges: Vec<(String, u64)> = Vec::new();
                shared.visit_non_zero_buckets(|r, c| ranges.push((r.to_string(), c)));
                for (vmrange, count) in ranges {
                    ctx.append_series_with_extra_label(
                        output_key,
                        "histogram_bucket",
                        count as f64,
                        "vmrange",
                        &vmrange,
                    );
                }
            }
            Output::Last { last, timestamp } => {
                if *timestamp > 0 {
                    ctx.append_series(output_key, "last", *last);
                    *timestamp = 0;
                }
            }
            Output::Max { max, defined } => {
                if *defined {
                    ctx.append_series(output_key, "max", *max);
                    *max = 0.0;
                    *defined = false;
                }
            }
            Output::Min { min, defined } => {
                if *defined {
                    ctx.append_series(output_key, "min", *min);
                    *min = 0.0;
                    *defined = false;
                }
            }
            Output::Quantiles { phis, h } => {
                let Some(hist) = h.take() else { return };
                let mut quantiles = Vec::new();
                hist.quantiles(&mut quantiles, phis);
                for (i, q) in quantiles.iter().enumerate() {
                    let phi_str = format_go_float(phis[i]);
                    ctx.append_series_with_extra_label(
                        output_key,
                        "quantiles",
                        *q,
                        "quantile",
                        &phi_str,
                    );
                }
            }
            Output::Std {
                is_deviation,
                count,
                avg,
                q,
            } => {
                if *count > 0.0 {
                    let mut output = *q / *count;
                    if *is_deviation {
                        output = output.sqrt();
                    }
                    let suffix = if *is_deviation { "stddev" } else { "stdvar" };
                    ctx.append_series(output_key, suffix, output);
                    *count = 0.0;
                    *avg = 0.0;
                    *q = 0.0;
                }
            }
            Output::SumSamples {
                reset_on_flush,
                sum,
            } => {
                if *reset_on_flush {
                    ctx.append_series(output_key, "sum_samples", *sum);
                    *sum = 0.0;
                } else {
                    ctx.append_series(output_key, "sum_samples_total", *sum);
                }
            }
            Output::UniqueSamples { values } => {
                if !values.is_empty() {
                    ctx.append_series(output_key, "unique_samples", values.len() as f64);
                    values.clear();
                }
            }
            Output::Total {
                keep_first_sample,
                total,
                shared,
                ..
            } => {
                let flush_ts = ctx.flush_timestamp;
                let t = shared.with_mut(|sh| {
                    let t = sh.shared_total + *total;
                    *total = 0.0;
                    sh.last
                        .retain(|_, lv| !(flush_ts > lv.delete_deadline || is_last));
                    if t.abs() >= (1u64 << 53) as f64 {
                        sh.shared_total = 0.0;
                    } else {
                        sh.shared_total = t;
                    }
                    t
                });
                let suffix = if *keep_first_sample {
                    "total"
                } else {
                    "total_prometheus"
                };
                ctx.append_series(output_key, suffix, t);
            }
            Output::Increase {
                keep_first_sample,
                total,
                last,
                ..
            } => {
                let flush_ts = ctx.flush_timestamp;
                last.with_mut(|l| l.retain(|_, lv| !(flush_ts > lv.delete_deadline || is_last)));
                let Some(t) = total.take() else { return };
                let suffix = if *keep_first_sample {
                    "increase"
                } else {
                    "increase_prometheus"
                };
                ctx.append_series(output_key, suffix, t);
            }
            Output::Rate {
                is_avg,
                is_green,
                shared,
                ..
            } => {
                let is_green = *is_green;
                let flush_ts = ctx.flush_timestamp;
                let mut rate = 0.0;
                let mut count_series = 0u64;
                shared.with_mut(|map| {
                    map.retain(|_, sv| {
                        if flush_ts > sv.delete_deadline {
                            return false;
                        }
                        let st_ts = sv.state(is_green).timestamp;
                        if st_ts > 0 {
                            let d = (st_ts - sv.prev_timestamp) as f64 / 1000.0;
                            if d > 0.0 {
                                rate += sv.state(is_green).increase / d;
                                count_series += 1;
                            }
                            // Only advance prev_timestamp when this window saw
                            // samples, so an empty window doesn't zero out the
                            // next flush's delta.
                            sv.prev_timestamp = st_ts;
                            let st = sv.state(is_green);
                            st.timestamp = 0;
                            st.increase = 0.0;
                        }
                        !is_last
                    });
                });
                if count_series == 0 {
                    return;
                }
                if *is_avg {
                    rate /= count_series as f64;
                }
                let suffix = if *is_avg { "rate_avg" } else { "rate_sum" };
                ctx.append_series(output_key, suffix, rate);
            }
        }
    }
}

/// Shared counter-delta accumulation for `total`/`increase`. Ports the body
/// of `totalAggrValue.pushSample` / `increaseAggrValue.pushSample`.
#[allow(clippy::too_many_arguments)]
fn push_counter(
    total: &mut f64,
    last: &mut HashMap<Vec<u8>, LastValue>,
    s: &PushSample,
    input_key: &[u8],
    delete_deadline: i64,
    keep_first_sample: bool,
    ignore_first_sample_deadline: u64,
    counter_resets: &'static esm_common::metrics::Counter,
) {
    let current_time = unix_secs();
    let keep_first_sample = keep_first_sample && current_time >= ignore_first_sample_deadline;
    let mut lv = last.get(input_key).copied();
    if let Some(v) = lv {
        if v.delete_deadline < current_time as i64 * 1000 {
            lv = None; // stale, reset
        }
    }
    if let Some(v) = lv {
        if s.timestamp < v.timestamp {
            return; // skip out-of-order sample
        }
        if s.value >= v.value {
            *total += s.value - v.value;
        } else {
            *total += s.value; // counter reset
            counter_resets.inc();
        }
    } else if keep_first_sample {
        *total += s.value;
    }
    last.insert(
        input_key.to_vec(),
        LastValue {
            value: s.value,
            timestamp: s.timestamp,
            delete_deadline,
        },
    );
}

/// Ports `rateAggrValue.pushSample`. `is_green` selects which window buffer's
/// `increase` accumulator the sample lands in; the counter-tracking fields
/// (`value`, `prev_timestamp`) are shared across both windows.
fn push_rate(
    shared: &mut HashMap<Vec<u8>, RateShared>,
    is_green: bool,
    s: &PushSample,
    input_key: &[u8],
    delete_deadline: i64,
    counter_resets: &'static esm_common::metrics::Counter,
) {
    let now_ms = unix_secs() as i64 * 1000;
    let stale = shared
        .get(input_key)
        .is_some_and(|sv| sv.delete_deadline < now_ms);
    if stale {
        shared.remove(input_key);
    }
    match shared.get_mut(input_key) {
        Some(sv) => {
            let prev_timestamp = sv.prev_timestamp;
            let sv_value = sv.value;
            {
                let st = sv.state(is_green);
                if s.timestamp < st.timestamp || s.timestamp < prev_timestamp {
                    return; // out of order
                }
                if s.value >= sv_value {
                    st.increase += s.value - sv_value;
                } else {
                    st.increase += s.value; // counter reset
                    counter_resets.inc();
                }
            }
            sv.value = s.value;
            sv.delete_deadline = delete_deadline;
            sv.state(is_green).timestamp = s.timestamp;
        }
        None => {
            let mut sv = RateShared {
                value: s.value,
                delete_deadline,
                prev_timestamp: s.timestamp,
                blue: RateState::default(),
                green: RateState::default(),
            };
            sv.state(is_green).timestamp = s.timestamp;
            shared.insert(input_key.to_vec(), sv);
        }
    }
}

/// Context for one flush pass: accumulates output series which the caller
/// then output-relabels and pushes.
pub(crate) struct FlushCtx<'a> {
    /// The name suffix built from interval + by/without (`:1m_by_job_`).
    pub(crate) name_suffix: &'a str,
    pub(crate) keep_metric_names: bool,
    pub(crate) flush_timestamp: i64,
    pub(crate) out: Vec<TimeSeries>,
}

impl FlushCtx<'_> {
    fn append_series(&mut self, output_key: &[u8], output_suffix: &str, value: f64) {
        let mut labels = decode_labels(output_key);
        if !self.keep_metric_names {
            add_metric_suffix(&mut labels, self.name_suffix, output_suffix);
        }
        self.out.push(TimeSeries {
            labels,
            samples: vec![Sample {
                timestamp: self.flush_timestamp,
                value,
            }],
        });
    }

    fn append_series_with_extra_label(
        &mut self,
        output_key: &[u8],
        output_suffix: &str,
        value: f64,
        extra_name: &str,
        extra_value: &str,
    ) {
        let mut labels = decode_labels(output_key);
        if !self.keep_metric_names {
            add_metric_suffix(&mut labels, self.name_suffix, output_suffix);
        }
        labels.push(Label {
            name: extra_name.to_string(),
            value: extra_value.to_string(),
        });
        self.out.push(TimeSeries {
            labels,
            samples: vec![Sample {
                timestamp: self.flush_timestamp,
                value,
            }],
        });
    }
}

/// Ports `addMetricSuffix`: appends `first_suffix + last_suffix` to the
/// `__name__` value, creating `__name__` if absent.
fn add_metric_suffix(labels: &mut Vec<Label>, first_suffix: &str, last_suffix: &str) {
    if let Some(l) = labels.iter_mut().find(|l| l.name == "__name__") {
        l.value.push_str(first_suffix);
        l.value.push_str(last_suffix);
        return;
    }
    labels.push(Label {
        name: "__name__".to_string(),
        value: format!("{first_suffix}{last_suffix}"),
    });
}

/// Go `strconv.AppendFloat(_, phi, 'g', -1, 64)` — shortest round-tripping
/// representation. Used for the `quantile="…"` label.
///
/// Rust's f64 `Display` is also shortest round-tripping, but it never emits
/// exponent form, so it diverges from Go's `'g'` for phi < 1e-4 (e.g.
/// "0.00001" vs "1e-05"). This ports Go's `ftoa` shortest-`'g'` rule: use
/// `%e` when the decimal exponent is < -4 or >= 6, otherwise `%f`. Mirrors
/// `esm_metricsql::strutil::format_float_go` (not reused to avoid pulling the
/// whole metricsql parser crate into this one).
fn format_go_float(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    // Shortest round-trip digits via Rust's exponential formatting,
    // e.g. "2.565e2", "-1.23e9", "0e0".
    let s = format!("{v:e}");
    let epos = s.rfind('e').expect("exponent in {:e} output");
    let exp: i32 = s[epos + 1..].parse().expect("valid exponent");
    let mantissa = &s[..epos];
    let neg = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };

    let mut out = String::new();
    if neg {
        out.push('-');
    }
    // Go ftoa shortest 'g': %e when the decimal exponent is < -4 or >= 6.
    if !(-4..6).contains(&exp) {
        // %e form: d.ddde±XX
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        if exp >= 0 {
            out.push('+');
        } else {
            out.push('-');
        }
        let ae = exp.unsigned_abs();
        if ae < 10 {
            out.push('0');
        }
        out.push_str(&ae.to_string());
    } else if exp >= 0 {
        // %f form with the decimal point inside or right after the digits.
        let ip = exp as usize + 1;
        if digits.len() > ip {
            out.push_str(&digits[..ip]);
            out.push('.');
            out.push_str(&digits[ip..]);
        } else {
            out.push_str(digits);
            for _ in 0..(ip - digits.len()) {
                out.push('0');
            }
        }
    } else {
        // %f form: 0.000ddd
        out.push_str("0.");
        for _ in 0..(-exp - 1) {
            out.push('0');
        }
        out.push_str(digits);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_go_float_matches_strconv_g() {
        // Go `strconv.AppendFloat(_, phi, 'g', -1, 64)` switches to exponent
        // form when the decimal exponent is < -4; Rust's f64 Display never
        // does, so phi < 1e-4 used to diverge (e.g. "0.00001" vs "1e-05").
        assert_eq!(format_go_float(0.5), "0.5");
        assert_eq!(format_go_float(0.9), "0.9");
        assert_eq!(format_go_float(0.99), "0.99");
        assert_eq!(format_go_float(0.999), "0.999");
        assert_eq!(format_go_float(0.0), "0");
        assert_eq!(format_go_float(1.0), "1");
        assert_eq!(format_go_float(0.0001), "0.0001");
        // The divergent cases: phi < 1e-4 must use Go 'g' exponent form.
        assert_eq!(format_go_float(0.00001), "1e-05");
        assert_eq!(format_go_float(0.00002), "2e-05");
        assert_eq!(format_go_float(1e-10), "1e-10");
        assert_eq!(format_go_float(0.000015), "1.5e-05");
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn test_counter(name: &str) -> &'static esm_common::metrics::Counter {
        esm_common::metrics::get_or_create_counter(name)
    }

    fn push(o: &mut Output, ts: i64, v: f64) {
        o.push_sample(
            &PushSample {
                value: v,
                timestamp: ts,
            },
            b"series",
            ts + 3_600_000,
        );
    }

    fn flush_value(o: &mut Output, flush_ts: i64) -> Option<f64> {
        let mut ctx = FlushCtx {
            name_suffix: ":1s_",
            keep_metric_names: false,
            flush_timestamp: flush_ts,
            out: Vec::new(),
        };
        o.flush(&mut ctx, b"", false);
        ctx.out.first().map(|t| t.samples[0].value)
    }

    // The blue/green window buffers must share the counter last-value state so
    // a delta computed in the green window still sees the value the blue
    // window recorded. Ports the `useSharedState` seeding of `total`.
    #[test]
    fn total_green_shares_last_value_with_blue() {
        let base = now_ms();
        let kind = OutputKind::Total {
            keep_first_sample: false,
            ignore_first_sample_deadline: 0,
            counter_resets: test_counter("test_streamaggr_cr_total_share"),
        };
        let mut blue = kind.new_state();
        let mut green = kind.new_green(&mut blue);

        // Blue records the first sample (no delta emitted for a fresh series).
        push(&mut blue, base, 10.0);
        // Green sees the shared last value (10) and computes a 15 delta.
        push(&mut green, base + 1000, 25.0);

        assert_eq!(flush_value(&mut blue, base + 500), Some(0.0));
        assert_eq!(flush_value(&mut green, base + 1500), Some(15.0));
    }

    // Stateless outputs get an independent green accumulator.
    #[test]
    fn sum_samples_green_is_independent() {
        let base = now_ms();
        let kind = OutputKind::SumSamples {
            reset_on_flush: true,
        };
        let mut blue = kind.new_state();
        let mut green = kind.new_green(&mut blue);

        push(&mut blue, base, 3.0);
        push(&mut green, base, 5.0);

        assert_eq!(flush_value(&mut blue, base + 500), Some(3.0));
        assert_eq!(flush_value(&mut green, base + 500), Some(5.0));
    }

    // Rate keeps a per-window increase but shares the counter-tracking value,
    // so blue and green each report their own window's per-second rate.
    #[test]
    fn rate_green_shares_counter_value() {
        let base = now_ms();
        let kind = OutputKind::Rate {
            is_avg: false,
            counter_resets: test_counter("test_streamaggr_cr_rate_share"),
        };
        let mut blue = kind.new_state();
        let mut green = kind.new_green(&mut blue);

        // Establish the series in blue, then two green samples 1s apart.
        push(&mut blue, base, 10.0);
        push(&mut green, base + 1000, 20.0);
        push(&mut green, base + 2000, 35.0);

        // Blue window saw no usable interval (single sample) → no output.
        assert_eq!(flush_value(&mut blue, base + 500), None);
        // Green shares blue's recorded value (10): increase = (20-10)+(35-20)
        // = 25 over base..base+2000 = 2s → rate 12.5. Without sharing the
        // first green sample would reset the baseline and yield 15.
        assert_eq!(flush_value(&mut green, base + 2500), Some(12.5));
    }

    #[test]
    fn add_suffix_appends_to_name() {
        let mut labels = vec![Label {
            name: "__name__".into(),
            value: "foo".into(),
        }];
        add_metric_suffix(&mut labels, ":1m_by_job_", "avg");
        assert_eq!(labels[0].value, "foo:1m_by_job_avg");
    }

    #[test]
    fn add_suffix_creates_name_when_absent() {
        let mut labels = vec![Label {
            name: "job".into(),
            value: "x".into(),
        }];
        add_metric_suffix(&mut labels, ":1m_", "sum_samples");
        assert!(labels
            .iter()
            .any(|l| l.name == "__name__" && l.value == ":1m_sum_samples"));
    }
}
