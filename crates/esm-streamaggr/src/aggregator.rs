//! The aggregator engine: the input push path, per-interval flush, and the
//! background flusher thread. Ports the `Aggregators`/`aggregator` types and
//! their `Push`/`flush`/`runFlusher` methods from `streamaggr.go` (with the
//! experimental windowing removed — see the crate docs).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use esm_relabel::{Label, ParsedConfigs};

use crate::config::{build_params, parse_configs, AggregatorParams, Options};
use crate::dedup::{DedupAggr, KeyedSample};
use crate::key::{compress_labels, split_key};
use crate::outputs::{FlushCtx, Output, PushSample};
use crate::{Error, PushFunc, TimeSeries};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Persistent per-output-key state. Ports `aggrValues`. `green` is populated
/// only when `use_shared_state` (windowing without dedup); otherwise all
/// samples flow through `blue`.
struct AggrValues {
    blue: Vec<Output>,
    green: Option<Vec<Output>>,
    delete_deadline: i64,
}

/// The window-flip state, snapshotted and advanced by the flusher. Ports
/// `currentState`.
#[derive(Clone, Copy)]
struct CurrentState {
    /// When `enable_windows`, samples with `timestamp <= max_deadline` are
    /// routed to whichever buffer matches `is_green`.
    is_green: bool,
    /// The boundary timestamp separating the two windows.
    max_deadline: i64,
}

/// A single running aggregator.
struct Aggregator {
    params: AggregatorParams,
    /// True when de-duplication is disabled (`getInputOutputKey` returns the
    /// input-label half as the per-series key). Ports `useInputKey`.
    use_input_key: bool,
    outputs: Mutex<HashMap<Vec<u8>, AggrValues>>,
    dedup: Option<DedupAggr>,
    /// Lower bound for `ignore_old_samples`, advanced on every flush.
    min_deadline: AtomicI64,
    /// Experimental blue/green aggregation-window mode.
    enable_windows: bool,
    /// Whether the output map keeps a separate green buffer (windows on and
    /// dedup off). Ports `aggrOutputs.useSharedState`.
    use_shared_state: bool,
    /// The window-flip state, advanced by the flusher and read by `push`.
    /// Ports `aggregator.cs` (an atomic pointer upstream).
    cs: Mutex<CurrentState>,
    /// Max observed sample lag (ms) in the current interval, used to delay the
    /// next windowed flush so late samples land. Ports `flushAfterMsec`.
    flush_after_ms: AtomicI64,
    push_func: PushFunc,
}

impl Aggregator {
    fn new(params: AggregatorParams, push_func: PushFunc) -> Aggregator {
        let use_input_key = params.dedup_interval_ms <= 0;
        let dedup = if params.dedup_interval_ms > 0 {
            Some(DedupAggr::new(params.metrics.dedup_dropped_samples))
        } else {
            None
        };
        // Aligned start deadline (ports the minTime computation).
        let start = now_ms();
        let min_time = if params.align_flush_to_interval && params.interval_ms > 0 {
            let truncated = start - start.rem_euclid(params.interval_ms);
            if truncated != start {
                truncated + params.interval_ms
            } else {
                truncated
            }
        } else {
            start
        };
        let max_deadline = if params.dedup_interval_ms > 0 {
            min_time + params.dedup_interval_ms
        } else {
            min_time + params.interval_ms
        };
        let enable_windows = params.enable_windows;
        let use_shared_state = params.use_shared_state;
        Aggregator {
            params,
            use_input_key,
            outputs: Mutex::new(HashMap::new()),
            dedup,
            min_deadline: AtomicI64::new(min_time),
            enable_windows,
            use_shared_state,
            cs: Mutex::new(CurrentState {
                is_green: false,
                max_deadline,
            }),
            flush_after_ms: AtomicI64::new(0),
            push_func,
        }
    }

    /// Ports `aggregator.Push` — matches, relabels, groups and buffers the
    /// samples, then routes them to dedup or straight to the outputs. When
    /// `enable_windows` is set, each sample is routed to the blue or green
    /// window buffer by comparing its timestamp against the current window
    /// boundary.
    fn push(&self, tss: &[TimeSeries], match_idxs: &mut [u32]) {
        let now = now_ms();
        let delete_deadline = now + self.params.staleness_interval_ms;
        let min_deadline = self.min_deadline.load(Ordering::Relaxed);
        let enable_windows = self.enable_windows;
        let cs = *self.cs.lock().unwrap();

        let mut blue: Vec<KeyedSample> = Vec::new();
        let mut green: Vec<KeyedSample> = Vec::new();
        let mut max_lag_ms: i64 = 0;
        for (idx, ts) in tss.iter().enumerate() {
            if !self.params.matcher.matches(&ts.labels) {
                continue;
            }
            match_idxs[idx] = 1;

            let mut labels: Vec<Label> = if !self.params.drop_input_labels.is_empty() {
                drop_series_labels(&ts.labels, &self.params.drop_input_labels)
            } else {
                ts.labels.clone()
            };
            if let Some(ir) = &self.params.input_relabeling {
                if !ir.apply(&mut labels) {
                    continue;
                }
            }
            if labels.is_empty() {
                continue;
            }
            labels.sort_by(|a, b| a.name.cmp(&b.name));

            let (input_labels, output_labels) = if !self.params.aggregate_only_by_time {
                get_input_output_labels(&labels, &self.params.by, &self.params.without)
            } else {
                (Vec::new(), labels)
            };
            let key = compress_labels(&input_labels, &output_labels);

            for s in &ts.samples {
                if s.value.is_nan() {
                    self.params.metrics.ignored_nan_samples.inc();
                    continue;
                }
                if (self.params.ignore_old_samples || enable_windows) && s.timestamp < min_deadline {
                    self.params.metrics.ignored_old_samples.inc();
                    continue;
                }
                let lag = now - s.timestamp;
                if lag > max_lag_ms {
                    max_lag_ms = lag;
                }
                let sample = KeyedSample {
                    key: key.clone(),
                    value: s.value,
                    timestamp: s.timestamp,
                };
                // Ports `s.Timestamp <= cs.maxDeadline == cs.isGreen`.
                if enable_windows && (s.timestamp <= cs.max_deadline) == cs.is_green {
                    green.push(sample);
                } else {
                    blue.push(sample);
                }
            }
        }

        if enable_windows && max_lag_ms > 0 {
            self.flush_after_ms.fetch_max(max_lag_ms, Ordering::Relaxed);
        }

        let total = (blue.len() + green.len()) as u64;
        if total == 0 {
            return;
        }
        self.params.metrics.matched_samples.inc_by(total);

        for (samples, is_green) in [(blue, false), (green, true)] {
            if samples.is_empty() {
                continue;
            }
            match &self.dedup {
                Some(dedup) => dedup.push_samples(&samples, is_green),
                None => self.push_samples(&samples, delete_deadline, is_green),
            }
        }
    }

    /// Ports `aggrOutputs.pushSamples`. `is_green` selects the window buffer.
    fn push_samples(&self, samples: &[KeyedSample], delete_deadline: i64, is_green: bool) {
        let use_shared_state = self.use_shared_state;
        let mut map = self.outputs.lock().unwrap();
        for s in samples {
            let (input_key, output_key) = split_key(&s.key, self.use_input_key);
            let entry = map.entry(output_key.to_vec()).or_insert_with(|| {
                let mut blue: Vec<Output> =
                    self.params.outputs.iter().map(|o| o.new_state()).collect();
                let green = if use_shared_state {
                    Some(
                        self.params
                            .outputs
                            .iter()
                            .zip(blue.iter_mut())
                            .map(|(kind, b)| kind.new_green(b))
                            .collect(),
                    )
                } else {
                    None
                };
                AggrValues {
                    blue,
                    green,
                    delete_deadline,
                }
            });
            let ps = PushSample {
                value: s.value,
                timestamp: s.timestamp,
            };
            let states = match (is_green, entry.green.as_mut()) {
                (true, Some(g)) => g,
                _ => &mut entry.blue,
            };
            for st in states.iter_mut() {
                st.push_sample(&ps, input_key, delete_deadline);
            }
            entry.delete_deadline = delete_deadline;
        }
    }

    /// De-duplicates the interval's samples into the outputs and advances
    /// `cs.max_deadline`. Ports `aggregator.dedupFlush`. A no-op when
    /// de-duplication is disabled.
    fn dedup_flush(&self, dedup_time_ms: i64, cs: &mut CurrentState) {
        let dedup_ms = self.params.dedup_interval_ms;
        if dedup_ms <= 0 {
            return;
        }
        self.min_deadline.store(cs.max_deadline, Ordering::Relaxed);
        let delete_deadline = dedup_time_ms + self.params.staleness_interval_ms;
        if let Some(dedup) = &self.dedup {
            // Deduplicated samples always land in the blue output buffer
            // (dedup owns the blue/green split at the dedup layer).
            dedup.flush(cs.is_green, |samples| {
                self.push_samples(samples, delete_deadline, false)
            });
        }
        let mut t = dedup_time_ms;
        while now_ms() > t {
            t += dedup_ms;
        }
        cs.max_deadline = t;
    }

    /// Ports `aggregator.flush` + `aggrOutputs.flushState` + `flushSeries`.
    fn flush(&self, push: bool, flush_ts_ms: i64, cs: CurrentState, is_last: bool) {
        let start = std::time::Instant::now();
        // With dedup disabled the flushed buffer is chosen by the window
        // state; with dedup enabled the output map only has the blue buffer.
        let is_green = if self.params.dedup_interval_ms <= 0 {
            self.min_deadline.store(cs.max_deadline, Ordering::Relaxed);
            cs.is_green
        } else {
            false
        };
        let mut ctx = FlushCtx {
            name_suffix: &self.params.name_suffix,
            keep_metric_names: self.params.keep_metric_names,
            flush_timestamp: flush_ts_ms,
            out: Vec::new(),
        };
        {
            let mut map = self.outputs.lock().unwrap();
            map.retain(|output_key, av| {
                if flush_ts_ms > av.delete_deadline {
                    // Stale entry: drop without flushing.
                    return false;
                }
                let states = match (is_green, av.green.as_mut()) {
                    (true, Some(g)) => g,
                    _ => &mut av.blue,
                };
                for st in states.iter_mut() {
                    st.flush(&mut ctx, output_key, is_last);
                }
                !is_last
            });
        }
        if push {
            self.push_output(ctx.out);
        }
        // A flush slower than the interval risks falling behind (ports the
        // `flushTimeouts` counter; the duration histogram is not ported).
        if self.params.interval_ms > 0
            && start.elapsed().as_millis() as i64 > self.params.interval_ms
        {
            self.params.metrics.flush_timeouts.inc();
        }
    }

    fn push_output(&self, mut out: Vec<TimeSeries>) {
        if out.is_empty() {
            return;
        }
        if let Some(relabel) = &self.params.output_relabeling {
            apply_output_relabel(&mut out, relabel);
        }
        if !out.is_empty() {
            self.params.metrics.output_samples.inc_by(out.len() as u64);
            (self.push_func)(&out);
        }
    }

    /// The background flusher loop. Ports `runFlusher`.
    fn run_flusher(&self, stop_rx: &Receiver<()>) {
        let interval_ms = self.params.interval_ms;
        let dedup_ms = self.params.dedup_interval_ms;
        let align = self.params.align_flush_to_interval;
        let loop_ms = if dedup_ms > 0 { dedup_ms } else { interval_ms };

        let min_time = self.min_deadline.load(Ordering::Relaxed);
        let mut flush_time = min_time + interval_ms;
        let mut ignore_first = self.params.ignore_first_intervals;

        // Aligned sleep until the first boundary.
        if wait_until(stop_rx, min_time) == Wake::Stop {
            self.shutdown_flush(align, flush_time, ignore_first);
            return;
        }

        loop {
            if wait_ms(stop_rx, loop_ms) == Wake::Stop {
                break;
            }
            // Snapshot and advance the window state. Ports `cs.Load().newState()`.
            let mut cs = *self.cs.lock().unwrap();
            if self.enable_windows {
                // Delay the flush by the max sample lag seen last interval so
                // late samples for the window being flushed can still arrive.
                let delay = self.flush_after_ms.swap(0, Ordering::Relaxed);
                if delay > 0 {
                    std::thread::sleep(Duration::from_millis(delay as u64));
                }
            }
            let deadline_time = cs.max_deadline;
            self.dedup_flush(deadline_time, &mut cs);
            if flush_time <= deadline_time {
                let push = ignore_first == 0;
                self.flush(push, flush_time, cs, false);
                ignore_first = ignore_first.saturating_sub(1);
                flush_time += interval_ms;
                while now_ms() > flush_time {
                    flush_time += interval_ms;
                }
                if dedup_ms <= 0 {
                    cs.max_deadline = flush_time;
                }
            }
            if self.enable_windows {
                cs.is_green = !cs.is_green;
            }
            *self.cs.lock().unwrap() = cs;
        }

        self.shutdown_flush(align, flush_time, ignore_first);
    }

    fn shutdown_flush(&self, align: bool, flush_time: i64, ignore_first: usize) {
        let mut cs = *self.cs.lock().unwrap();
        let dedup_time = if align { cs.max_deadline } else { now_ms() };
        let flush_ts = if align { flush_time } else { now_ms() };
        self.dedup_flush(dedup_time, &mut cs);
        let push = self.params.flush_on_shutdown && ignore_first == 0;
        self.flush(push, flush_ts, cs, true);
        *self.cs.lock().unwrap() = cs;
    }
}

/// Ports `getInputOutputLabels`: splits `labels` into the `(input, output)`
/// halves by the `by`/`without` lists.
fn get_input_output_labels(
    labels: &[Label],
    by: &[String],
    without: &[String],
) -> (Vec<Label>, Vec<Label>) {
    let mut input = Vec::new();
    let mut output = Vec::new();
    if !without.is_empty() {
        for l in labels {
            if without.iter().any(|w| w == &l.name) {
                input.push(l.clone());
            } else {
                output.push(l.clone());
            }
        }
    } else {
        for l in labels {
            if by.iter().any(|b| b == &l.name) {
                output.push(l.clone());
            } else {
                input.push(l.clone());
            }
        }
    }
    (input, output)
}

/// Ports `dropSeriesLabels`.
fn drop_series_labels(src: &[Label], drop: &[String]) -> Vec<Label> {
    src.iter()
        .filter(|l| !drop.iter().any(|d| d == &l.name))
        .cloned()
        .collect()
}

/// Applies output relabeling in place, dropping series that relabeling
/// removes. Ports the slow path of `flushSeries`.
fn apply_output_relabel(out: &mut Vec<TimeSeries>, relabel: &ParsedConfigs) {
    out.retain_mut(|ts| relabel.apply(&mut ts.labels) && !ts.labels.is_empty());
}

#[derive(PartialEq)]
enum Wake {
    Tick,
    Stop,
}

fn wait_ms(rx: &Receiver<()>, ms: i64) -> Wake {
    if ms <= 0 {
        return match rx.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => Wake::Stop,
            Err(TryRecvError::Empty) => Wake::Tick,
        };
    }
    match rx.recv_timeout(Duration::from_millis(ms as u64)) {
        Ok(()) | Err(RecvTimeoutError::Disconnected) => Wake::Stop,
        Err(RecvTimeoutError::Timeout) => Wake::Tick,
    }
}

fn wait_until(rx: &Receiver<()>, deadline_ms: i64) -> Wake {
    wait_ms(rx, deadline_ms - now_ms())
}

/// A set of running aggregators sharing a config document. Ports
/// `streamaggr.Aggregators`.
pub struct Aggregators {
    aggregators: Vec<Arc<Aggregator>>,
    stops: Vec<Sender<()>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    stopped: AtomicBool,
    signature: String,
}

impl Aggregators {
    /// Loads aggregators from a YAML config document and starts their
    /// background flushers. Ports `LoadFromData`.
    pub fn load_from_data(
        data: &str,
        push_func: PushFunc,
        opts: &Options,
    ) -> Result<Aggregators, Error> {
        let (cfgs, signature) = parse_configs(data)?;
        let mut aggregators = Vec::with_capacity(cfgs.len());
        let mut stops = Vec::with_capacity(cfgs.len());
        let mut handles = Vec::with_capacity(cfgs.len());
        for (i, cfg) in cfgs.iter().enumerate() {
            let params = build_params(cfg, opts, i + 1)
                .map_err(|e| Error::new(format!("cannot initialize aggregator #{i}: {}", e.msg)))?;
            let agg = Arc::new(Aggregator::new(params, push_func.clone()));
            let (tx, rx) = std::sync::mpsc::channel();
            let agg_thread = Arc::clone(&agg);
            let handle = std::thread::Builder::new()
                .name("esm-streamaggr".to_string())
                .spawn(move || agg_thread.run_flusher(&rx))
                .expect("spawn streamaggr flusher");
            aggregators.push(agg);
            stops.push(tx);
            handles.push(handle);
        }
        Ok(Aggregators {
            aggregators,
            stops,
            handles: Mutex::new(handles),
            stopped: AtomicBool::new(false),
            signature,
        })
    }

    /// Returns true if at least one aggregator is configured. Ports
    /// `IsEnabled`.
    pub fn is_enabled(&self) -> bool {
        !self.aggregators.is_empty()
    }

    /// Pushes `tss` to every aggregator, setting `match_idxs[i] = 1` for each
    /// input series consumed by at least one aggregator. Ports
    /// `Aggregators.Push`.
    pub fn push(&self, tss: &[TimeSeries], match_idxs: &mut Vec<u32>) {
        match_idxs.clear();
        match_idxs.resize(tss.len(), 0);
        for agg in &self.aggregators {
            agg.push(tss, match_idxs);
        }
    }

    /// Returns true if `self` and `other` were built from identical configs.
    /// Ports `Equal`.
    pub fn equal(&self, other: &Aggregators) -> bool {
        self.signature == other.signature
    }

    /// Stops all flushers, running the shutdown flush. Idempotent. Ports
    /// `MustStop`. Takes `&self` (interior mutability) so the `Aggregators`
    /// can be shared behind an `Arc` — its `push` path is `&self` too — and
    /// stopped explicitly at shutdown without exclusive ownership.
    pub fn must_stop(&self) {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        for tx in &self.stops {
            let _ = tx.send(());
        }
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
    }

    // ---- test helpers (deterministic, thread-free driving) ----

    #[cfg(test)]
    pub(crate) fn load_without_flusher(data: &str, opts: &Options) -> Result<Aggregators, Error> {
        let (cfgs, signature) = parse_configs(data)?;
        let noop: PushFunc = Arc::new(|_: &[TimeSeries]| {});
        let mut aggregators = Vec::with_capacity(cfgs.len());
        for (i, cfg) in cfgs.iter().enumerate() {
            let params = build_params(cfg, opts, i + 1)
                .map_err(|e| Error::new(format!("cannot initialize aggregator #{i}: {}", e.msg)))?;
            aggregators.push(Arc::new(Aggregator::new(params, noop.clone())));
        }
        Ok(Aggregators {
            aggregators,
            stops: Vec::new(),
            handles: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
            signature,
        })
    }

    /// Drives a synchronous push then flush and returns the flushed series,
    /// sorted by their `LabelsToString`. Test-only.
    #[cfg(test)]
    pub(crate) fn push_and_flush(&self, tss: &[TimeSeries]) -> Vec<TimeSeries> {
        let collected = Arc::new(Mutex::new(Vec::new()));
        let mut idxs = Vec::new();
        self.push(tss, &mut idxs);
        let ts = now_ms();
        for agg in &self.aggregators {
            let c = Arc::clone(&collected);
            let cf: PushFunc = Arc::new(move |series: &[TimeSeries]| {
                c.lock().unwrap().extend_from_slice(series);
            });
            // Temporarily flush with a capturing push func.
            agg.flush_with(&cf, ts, false);
        }
        let mut out = Arc::try_unwrap(collected).unwrap().into_inner().unwrap();
        out.sort_by_key(|a| labels_to_string(&a.labels));
        out
    }
}

impl Aggregator {
    /// Like [`Aggregator::flush`] but pushes to a caller-supplied func.
    /// Test-only.
    #[cfg(test)]
    fn flush_with(&self, push_func: &PushFunc, flush_ts_ms: i64, is_last: bool) {
        if self.params.dedup_interval_ms <= 0 {
            self.min_deadline.store(flush_ts_ms, Ordering::Relaxed);
        }
        let mut ctx = FlushCtx {
            name_suffix: &self.params.name_suffix,
            keep_metric_names: self.params.keep_metric_names,
            flush_timestamp: flush_ts_ms,
            out: Vec::new(),
        };
        {
            let mut map = self.outputs.lock().unwrap();
            map.retain(|output_key, av| {
                if flush_ts_ms > av.delete_deadline {
                    return false;
                }
                for st in av.blue.iter_mut() {
                    st.flush(&mut ctx, output_key, is_last);
                }
                !is_last
            });
        }
        let mut out = ctx.out;
        if let Some(relabel) = &self.params.output_relabeling {
            apply_output_relabel(&mut out, relabel);
        }
        if !out.is_empty() {
            push_func(&out);
        }
    }
}

impl Drop for Aggregators {
    fn drop(&mut self) {
        self.must_stop();
    }
}

/// Ports `promrelabel.LabelsToString` for the deterministic test ordering.
#[cfg(test)]
fn labels_to_string(labels: &[Label]) -> String {
    let mut sorted: Vec<&Label> = labels.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let mut s = String::from("{");
    for (i, l) in sorted.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{}={:?}", l.name, l.value));
    }
    s.push('}');
    s
}
