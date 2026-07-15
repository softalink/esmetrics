//! Scrape-once core: fetch a target's `/metrics` response, parse it, apply
//! `metric_relabel`, merge target/external labels per `honor_labels`,
//! build the resulting [`OwnedSeries`], and append the per-scrape
//! auto-metrics (`up`, `scrape_duration_seconds`, ...) via [`Scraper`].
//!
//! Port of the fetch + per-row assembly slice of
//! `lib/promscrape/scrapework.go`'s `scrapeInternal`/`addRowToTimeseries`/
//! `addRow`/`appendLabels`/`appendExtraLabels`, and the scrape HTTP client
//! in `lib/promscrape/client.go`. Auto-metric generation itself lives in
//! [`super::autometrics`] (`addAutoMetrics`). [`Scraper`] also tracks
//! cross-scrape staleness (vanished-series stale-NaN markers,
//! [`Scraper::mark_stale_all`] for target removal) — see the doc comments
//! on [`Scraper`] and [`Scraper::scrape`]. [`Scraper::scrape`] enforces
//! `sample_limit`/`label_limit` per `scrapework.go:548-608`/`1159-1163` —
//! see its doc comment. The scrape *loop* (interval ticking, target
//! management, `/targets` reporting) is a later task.
//!
//! ## Per-row label pipeline (upstream-faithful)
//!
//! Matches upstream `addRow` (`scrapework.go:1144-1158`) +
//! `promrelabel.FinalizeLabels` (`relabel.go:149-158`) exactly:
//! `__name__`+tags -> merge `target_labels` -> `metric_relabel` ->
//! finalize (strip `__*` except `__name__`) -> merge `external_labels` ->
//! sample. `target_labels` is merged *before* `metric_relabel` so relabel
//! rules can match on `job`/`instance`/target labels; `external_labels` is
//! merged *after* relabel+finalize, per
//! <https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3137>. See
//! [`build_series`] for the step-by-step and [`merge_extra_labels`] for the
//! honor_labels rename-on-conflict direction.
//!
//! ## `max_scrape_size` / gzip
//!
//! Mirrors `client.go`'s `ReadData`: the raw (possibly gzip-compressed)
//! response body is read with a hard cap of `max_scrape_size + 1` bytes
//! (`max_scrape_size == 0` means unlimited, matching this codebase's
//! `sample_limit`/`label_limit` "0 = unlimited" convention elsewhere), and
//! if the read hits that cap the scrape fails with an error naming the
//! limit rather than silently truncating. Decompression (`gzip`, based on
//! the response's `Content-Encoding` header) happens after that cap check,
//! via [`esm_protoparser::prometheus::parse_stream`]'s `encoding`
//! parameter — the cap therefore bounds the wire size, same as upstream's
//! `dst.Len()` check (measured before `isGzipped` decompression).
//! Requesting compression is separately gated by
//! [`ScrapeConfigResolved::enable_compression`]: `Accept-Encoding: gzip` is
//! only sent when it's `true` (the default). Decompression itself always
//! goes by the response's actual `Content-Encoding`, so a target that
//! ignores the (possibly omitted) `Accept-Encoding` request header and
//! responds compressed — or uncompressed — anyway still parses correctly
//! either way.

use std::collections::HashMap;
use std::io::Read;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use esm_protoparser::prometheus::{parse_stream, Row};
use esm_protoparser::prompb::Sample;
use esm_relabel::{Label, ParsedConfigs};

use super::autometrics::{append_auto_metrics, series_identity_hash, AUTO_METRIC_COUNT};
use crate::client::{AuthConfig, TlsConfig};
use crate::series::OwnedSeries;

/// Prometheus-workaround `Accept` header value, per the task brief (a
/// simplified form of upstream's full
/// `"text/plain;version=0.0.4;q=1,*/*;q=0.1"` — see `client.go:135`).
const ACCEPT_HEADER: &str = "text/plain;version=0.0.4";

/// The per-target resolved config [`Scraper::scrape`] needs. Built by a
/// later task's manager (Task 8) from a [`crate::scrape::target::Target`]
/// plus its owning `ScrapeConfig`; this task only consumes it.
pub struct ScrapeConfigResolved {
    pub metric_relabel: ParsedConfigs,
    pub honor_labels: bool,
    pub honor_timestamps: bool,
    pub external_labels: Vec<Label>,
    pub target_labels: Vec<Label>,
    /// `0` means unlimited. Enforced in [`Scraper::scrape`]: if the
    /// scrape's post-relabel series count exceeds this, the whole scrape
    /// fails and all scraped samples are dropped
    /// (`scrapework.go:555-559`).
    pub sample_limit: usize,
    /// `0` means unlimited. Enforced per series in [`fetch_and_parse`],
    /// right after [`build_series`] returns each one: the first series
    /// whose final label count exceeds this fails the whole scrape and all
    /// scraped samples are dropped (`scrapework.go:1159-1164`).
    pub label_limit: usize,
    pub scrape_timeout: Duration,
    /// Byte cap on the raw (pre-decompression) response body. `0` means
    /// unlimited.
    pub max_scrape_size: u64,
    /// Port of `ScrapeConfig.enable_compression` (default `true`): when
    /// `true`, [`fetch_and_parse`] sends `Accept-Encoding: gzip` so the
    /// target may respond compressed. When `false`, the header is omitted
    /// entirely — matching upstream's `enable_compression`/
    /// `disable_compression` gating of the scrape request's compression
    /// advertisement. Either way, a response that does arrive gzip-encoded
    /// (or not) is still decompressed correctly based on its actual
    /// `Content-Encoding` header — this field only controls whether gzip is
    /// *requested*, not how a response is *handled*.
    pub enable_compression: bool,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
}

/// Result of one [`Scraper::scrape`] call.
pub struct ScrapeResult {
    /// Scraped series (post `metric_relabel` + label merge), followed by
    /// the seven auto-metric series appended by
    /// [`append_auto_metrics`](super::autometrics::append_auto_metrics) —
    /// see [`Scraper::scrape`].
    pub series: Vec<OwnedSeries>,
    pub up: bool,
    pub samples_scraped: usize,
    pub samples_post_relabel: usize,
    pub duration: Duration,
    pub error: Option<String>,
    pub response_size: usize,
}

/// Owns one scrape target's cross-scrape state: the previous *successful*
/// scrape's series (identity hash -> labels, used for both
/// `scrape_series_added` and staleness markers) plus whether that previous
/// scrape succeeded, plus the `sample_limit`/`label_limit` skip counters —
/// see the doc comments in [`Scraper::scrape`].
pub struct Scraper {
    cfg: ScrapeConfigResolved,
    /// Series from the previous *successful* scrape's post-relabel,
    /// pre-auto-metric series, keyed by [`series_identity_hash`] with the
    /// series' full label set as the value (needed to emit a staleness
    /// marker for a series once it vanishes — the hash alone isn't enough
    /// to reconstruct the labels for the stale sample). Empty before the
    /// first scrape, so the first scrape's `scrape_series_added` equals its
    /// full series count — matching upstream's `seriesAdded` for an empty
    /// `lastScrapeStr`.
    ///
    /// Updated ONLY on a successful scrape (`storeLastScrape` in upstream
    /// runs only `if !areIdenticalSeries && err == nil`,
    /// `scrapework.go:617-620`/`729-732`) — a failed scrape must not
    /// clobber this with an empty set, or the next successful scrape would
    /// spuriously see every series as "added" and staleness markers for a
    /// still-failing target would never be (re-)emitted correctly. See
    /// [`Scraper::scrape`] for the exactly-once-per-failure-run logic this
    /// enables.
    last_series: HashMap<u64, Vec<Label>>,
    /// Whether the *previous* call to [`Scraper::scrape`] succeeded.
    /// Mirrors upstream's `sw.lastScrapeSuccess`
    /// (`scrapework.go:579-582`/`699-701`). Used to gate staleness-marker
    /// emission: `scrapework.go:611`/`723` sends stale markers when
    /// `lastScrapeSuccess || err == nil`, so a run of consecutive failures
    /// emits stale markers exactly once (right after the first failure),
    /// not on every failure.
    last_scrape_ok: bool,
    /// Internal counter mirroring upstream's `vm_promscrape_stale_samples_created_total`
    /// (`scrapework.go:951`). Deliberately NOT pushed as a per-target
    /// series/auto-metric: upstream exposes it on vmagent's own `/metrics`
    /// self-scrape endpoint, not the remote-write stream — treating it as
    /// an auto-metric here would push a non-standard series real
    /// VM/Prometheus targets never emit. See [`Scraper::stale_samples_created`].
    stale_samples_created: u64,
    /// Internal counter mirroring upstream's
    /// `vm_promscrape_scrapes_skipped_by_sample_limit_total`
    /// (`scrapework.go:556`). Bumped once per scrape whose post-relabel
    /// series count exceeds `cfg.sample_limit`. Deliberately NOT pushed as
    /// a per-target series — same rationale as `stale_samples_created`.
    /// See [`Scraper::scrapes_skipped_by_sample_limit`].
    scrapes_skipped_by_sample_limit: u64,
    /// Internal counter mirroring upstream's
    /// `vm_promscrape_scrapes_skipped_by_label_limit_total`
    /// (`scrapework.go:563`/`667`). Bumped once per scrape aborted because
    /// some series' final label count exceeded `cfg.label_limit`.
    /// Deliberately NOT pushed as a per-target series — same rationale as
    /// `stale_samples_created`. See
    /// [`Scraper::scrapes_skipped_by_label_limit`].
    scrapes_skipped_by_label_limit: u64,
}

impl Scraper {
    pub fn new(cfg: ScrapeConfigResolved) -> Self {
        Self {
            cfg,
            last_series: HashMap::new(),
            last_scrape_ok: false,
            stale_samples_created: 0,
            scrapes_skipped_by_sample_limit: 0,
            scrapes_skipped_by_label_limit: 0,
        }
    }

    /// Total number of stale-NaN samples this `Scraper` has produced so
    /// far (per-scrape vanished-series markers + [`Scraper::mark_stale_all`]
    /// markers). Mirrors upstream's process-wide
    /// `vm_promscrape_stale_samples_created_total` counter
    /// (`scrapework.go:951`) — see the field doc on `stale_samples_created`
    /// for why this is a plain counter and not a pushed series. Hook for a
    /// future self-metrics `/metrics` endpoint.
    pub fn stale_samples_created(&self) -> u64 {
        self.stale_samples_created
    }

    /// Total number of scrapes this `Scraper` has failed because the
    /// post-relabel series count exceeded `cfg.sample_limit`. Mirrors
    /// upstream's process-wide
    /// `vm_promscrape_scrapes_skipped_by_sample_limit_total` counter
    /// (`scrapework.go:556`) — see the field doc on
    /// `scrapes_skipped_by_sample_limit` for why this is a plain counter
    /// and not a pushed series. Hook for a future self-metrics `/metrics`
    /// endpoint.
    pub fn scrapes_skipped_by_sample_limit(&self) -> u64 {
        self.scrapes_skipped_by_sample_limit
    }

    /// Total number of scrapes this `Scraper` has failed because some
    /// series' final label count exceeded `cfg.label_limit`. Mirrors
    /// upstream's process-wide
    /// `vm_promscrape_scrapes_skipped_by_label_limit_total` counter
    /// (`scrapework.go:563`/`667`) — see the field doc on
    /// `scrapes_skipped_by_label_limit` for why this is a plain counter and
    /// not a pushed series. Hook for a future self-metrics `/metrics`
    /// endpoint.
    pub fn scrapes_skipped_by_label_limit(&self) -> u64 {
        self.scrapes_skipped_by_label_limit
    }

    /// Scrapes `scrape_url` once: fetch -> parse exposition text ->
    /// `metric_relabel` -> honor_labels merge of `target_labels` +
    /// `external_labels` -> build [`OwnedSeries`] -> enforce
    /// `sample_limit`/`label_limit` -> append auto-metrics. Never panics:
    /// any fetch, status, size-cap, parse, or limit failure is reported as
    /// `up: false` with `error` set, not propagated as a `Result`/panic.
    ///
    /// ## `sample_limit`/`label_limit` enforcement
    ///
    /// Port of `scrapework.go:548-608` (`processDataOneShot`) +
    /// `scrapework.go:1159-1164` (`addRow`'s `label_limit` check).
    /// Auto-metrics are exempt from both limits upstream (`addRows(...,
    /// false)` bypasses `label_limit`, and auto-metrics are appended after
    /// the `sample_limit` check) — [`append_auto_metrics`] runs after this
    /// enforcement, unconditionally.
    ///
    /// - **`label_limit`** (checked per series, in [`fetch_and_parse`]'s row
    ///   loop right after [`build_series`] returns): the first series whose
    ///   final label count (after target+relabel+finalize+external merge)
    ///   exceeds `cfg.label_limit` fails the *whole* scrape. Upstream aborts
    ///   `addRows` here (returning `errLabelsLimitExceeded`), but only
    ///   *after* the entire body was already unmarshaled up front — so
    ///   [`fetch_and_parse`] keeps reading/counting the rest of the body
    ///   (just stops building series) rather than halting the read loop, to
    ///   keep `samples_scraped` (`scrape_samples_scraped`) accurate for
    ///   bodies larger than one read block. `samples_post_relabel` is left
    ///   at `0` (upstream leaves `samplesPostRelabeling = 0` whenever
    ///   `scrapeErr != nil` — the `= len(...)` assignment only runs on the
    ///   `scrapeErr == nil` branch, `scrapework.go:548-554`).
    /// - **`sample_limit`** (checked once, after the full response is
    ///   parsed and relabeled): if the resulting series count exceeds
    ///   `cfg.sample_limit`, the scrape fails with the full
    ///   `samples_post_relabel` count preserved (it was computed before the
    ///   limit check ran, matching `scrapework.go:553-559`).
    ///
    /// Either failure: `up = false`, every scraped series is dropped (none
    /// forwarded — upstream's `wc.writeRequest.Reset()`), `response_size`
    /// is `0` (upstream zeroes `bodyString` whenever `up == 0`, *before*
    /// `responseSize := len(bodyString)` runs), and the corresponding
    /// internal skip counter is bumped exactly once. Because `up` flips to
    /// `0`, the staleness path below treats this exactly like any other
    /// failed scrape — see its comment.
    pub fn scrape(&mut self, client: &reqwest::blocking::Client, scrape_url: &str) -> ScrapeResult {
        let start = Instant::now();
        let scrape_timestamp_ms = now_millis();

        let mut result = match fetch_and_parse(client, scrape_url, &self.cfg, scrape_timestamp_ms) {
            Ok(outcome)
                if self.cfg.sample_limit > 0
                    && outcome.samples_post_relabel > self.cfg.sample_limit =>
            {
                self.scrapes_skipped_by_sample_limit += 1;
                ScrapeResult {
                    series: Vec::new(),
                    up: false,
                    samples_scraped: outcome.samples_scraped,
                    samples_post_relabel: outcome.samples_post_relabel,
                    duration: start.elapsed(),
                    error: Some(format!(
                        "the response from {scrape_url:?} exceeds sample_limit={}; either \
                         reduce the sample count for the target or increase sample_limit",
                        self.cfg.sample_limit
                    )),
                    response_size: 0,
                }
            }
            Ok(outcome) => ScrapeResult {
                series: outcome.series,
                up: true,
                samples_scraped: outcome.samples_scraped,
                samples_post_relabel: outcome.samples_post_relabel,
                duration: start.elapsed(),
                error: None,
                response_size: outcome.response_size,
            },
            Err(ScrapeFailure::LabelLimitExceeded { samples_scraped }) => {
                self.scrapes_skipped_by_label_limit += 1;
                ScrapeResult {
                    series: Vec::new(),
                    up: false,
                    samples_scraped,
                    samples_post_relabel: 0,
                    duration: start.elapsed(),
                    error: Some(format!(
                        "the response from {scrape_url:?} contains samples with a number of \
                         labels exceeding label_limit={}; either reduce the labels count for \
                         the target or increase label_limit",
                        self.cfg.label_limit
                    )),
                    response_size: 0,
                }
            }
            Err(ScrapeFailure::Generic(err)) => ScrapeResult {
                series: Vec::new(),
                up: false,
                samples_scraped: 0,
                samples_post_relabel: 0,
                duration: start.elapsed(),
                error: Some(err),
                response_size: 0,
            },
        };

        let scrape_ok = result.up;
        let current_series: HashMap<u64, Vec<Label>> = result
            .series
            .iter()
            .map(|s| (series_identity_hash(s), s.labels.clone()))
            .collect();
        let series_added = current_series
            .keys()
            .filter(|k| !self.last_series.contains_key(*k))
            .count();

        // Staleness: series present in `self.last_series` (previous
        // *successful* scrape) but absent from `current_series` — matches
        // upstream `sendStaleSeries(lastScrapeStr, bodyString, ...)`
        // (`scrapework.go:611-615`/`723-727`), gated on
        // `lastScrapeSuccess || err == nil` so a run of consecutive
        // failures emits stale markers exactly once. On a failed scrape
        // `current_series` is empty, so this naturally marks ALL
        // previously-tracked series stale — but only the first time,
        // because `last_scrape_ok` flips to `false` after that failure and
        // stays `false` while `self.last_series` (still holding the
        // now-stale set) is left untouched by the "update on success only"
        // rule below.
        let vanished: Vec<Vec<Label>> = self
            .last_series
            .iter()
            .filter(|(hash, _)| !current_series.contains_key(*hash))
            .map(|(_, labels)| labels.clone())
            .collect();
        if !vanished.is_empty() && (self.last_scrape_ok || scrape_ok) {
            self.stale_samples_created += vanished.len() as u64;
            result.series.extend(
                vanished
                    .into_iter()
                    .map(|labels| make_stale_series(labels, scrape_timestamp_ms)),
            );
        }

        // Only update `last_series` when this scrape succeeded — see the
        // field doc on `last_series` for why (upstream
        // `scrapework.go:617-620`/`729-732`).
        if scrape_ok {
            self.last_series = current_series;
        }
        self.last_scrape_ok = scrape_ok;

        append_auto_metrics(&mut result, &self.cfg, series_added, scrape_timestamp_ms);
        result
    }

    /// Emits stale-NaN markers for every currently-tracked series (the
    /// last successful scrape's series) plus a stale-NaN auto-metric set
    /// (`up`, `scrape_*`), then clears the tracked set — the target is
    /// gone. Port of upstream's target-removal path, which calls
    /// `sw.sendStaleSeries(lastScrape, "", t, addAutoSeries=true)`
    /// (`scrapework.go:887-939`); the `addAutoSeries=true` there is what
    /// also stale-marks the auto-metrics, not just the scraped series.
    /// Called by the manager (Task 8) on target removal / shutdown.
    pub fn mark_stale_all(&mut self, ts: i64) -> Vec<OwnedSeries> {
        let mut stale = Vec::with_capacity(self.last_series.len() + AUTO_METRIC_COUNT);
        for labels in self.last_series.values() {
            stale.push(make_stale_series(labels.clone(), ts));
        }
        self.stale_samples_created += self.last_series.len() as u64;

        // Auto-metric set, stale-marked. Values fed in here (up=false,
        // zero durations/sizes) are irrelevant — every sample is
        // overwritten to STALE_NAN below, matching upstream's zero-value
        // `var am autoMetrics` passed to `addAutoMetrics` before
        // `setStaleMarkersForRows` overwrites every value.
        let mut auto = ScrapeResult {
            series: Vec::new(),
            up: false,
            samples_scraped: 0,
            samples_post_relabel: 0,
            duration: Duration::ZERO,
            error: None,
            response_size: 0,
        };
        append_auto_metrics(&mut auto, &self.cfg, 0, ts);
        self.stale_samples_created += auto.series.len() as u64;
        for mut series in auto.series {
            for sample in &mut series.samples {
                sample.value = esm_common::decimal::STALE_NAN;
            }
            stale.push(series);
        }

        self.last_series.clear();
        stale
    }
}

/// Builds one stale-NaN [`OwnedSeries`]: `labels` unchanged, a single
/// sample carrying `esm_common::decimal::STALE_NAN` at `timestamp`. Port of
/// `setStaleMarkersForRows` (`scrapework.go:941-949`).
fn make_stale_series(labels: Vec<Label>, timestamp: i64) -> OwnedSeries {
    OwnedSeries {
        labels,
        samples: vec![Sample {
            value: esm_common::decimal::STALE_NAN,
            timestamp,
        }],
    }
}

/// Successful outcome of [`fetch_and_parse`], bundled to avoid a long tuple.
/// "Successful" here only means the fetch/parse/`label_limit` pipeline
/// completed — [`Scraper::scrape`] still separately checks
/// `samples_post_relabel` against `cfg.sample_limit` on this outcome.
struct FetchOutcome {
    series: Vec<OwnedSeries>,
    samples_scraped: usize,
    samples_post_relabel: usize,
    response_size: usize,
}

/// Failure outcome of [`fetch_and_parse`]. [`Scraper::scrape`] turns either
/// variant into a `ScrapeResult { up: false, .. }` with all scraped samples
/// dropped.
enum ScrapeFailure {
    /// A fetch, status, size-cap, or parse failure — nothing beyond it was
    /// processed, so [`Scraper::scrape`] reports zeroed counts for this
    /// variant (matching the pre-Task-7 behavior).
    Generic(String),
    /// Port of `errLabelsLimitExceeded` (`scrapework.go:1098`): some series'
    /// final label count exceeded `cfg.label_limit`
    /// (`scrapework.go:1159-1164`), which fails the whole scrape.
    /// `samples_scraped` is the count of ALL rows parsed across the whole
    /// body (the read is not cut short at the offending row — see
    /// [`fetch_and_parse`]), so `scrape_samples_scraped` stays accurate for
    /// multi-block bodies.
    LabelLimitExceeded { samples_scraped: usize },
}

/// Fetches `scrape_url`, reads its body (capped at `cfg.max_scrape_size`),
/// and parses + relabels + merges it into [`OwnedSeries`], enforcing
/// `cfg.label_limit` per series as each row is built (see
/// [`ScrapeFailure::LabelLimitExceeded`]). `cfg.sample_limit` is NOT
/// enforced here — it needs the full post-relabel count, which the caller
/// ([`Scraper::scrape`]) checks against this function's `Ok` outcome.
fn fetch_and_parse(
    client: &reqwest::blocking::Client,
    scrape_url: &str,
    cfg: &ScrapeConfigResolved,
    scrape_timestamp_ms: i64,
) -> Result<FetchOutcome, ScrapeFailure> {
    let mut req = client
        .get(scrape_url)
        .header(reqwest::header::ACCEPT, ACCEPT_HEADER)
        .timeout(cfg.scrape_timeout);
    if cfg.enable_compression {
        req = req.header(reqwest::header::ACCEPT_ENCODING, "gzip");
    }
    if let Some((user, pass)) = &cfg.auth.basic {
        req = req.basic_auth(user, Some(pass));
    } else if let Some(token) = &cfg.auth.bearer {
        req = req.bearer_auth(token);
    }

    let resp = req
        .send()
        .map_err(|e| ScrapeFailure::Generic(format!("cannot scrape {scrape_url:?}: {e}")))?;
    if resp.status() != reqwest::StatusCode::OK {
        return Err(ScrapeFailure::Generic(format!(
            "unexpected status code returned when scraping {scrape_url:?}: {}; expecting 200",
            resp.status()
        )));
    }

    let content_encoding = resp
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let body =
        read_capped_body(resp, scrape_url, cfg.max_scrape_size).map_err(ScrapeFailure::Generic)?;
    let response_size = body.len();
    let encoding = if content_encoding == "gzip" {
        "gzip"
    } else {
        ""
    };

    let mut series = Vec::new();
    let mut samples_scraped = 0usize;
    let mut samples_post_relabel = 0usize;
    let mut label_limit_exceeded = false;
    parse_stream(
        body.as_slice(),
        encoding,
        scrape_timestamp_ms,
        |msg| log::warn!("esmagent scrape {scrape_url}: {msg}"),
        |rows| {
            for row in rows {
                // Count EVERY parsed row across the WHOLE body, even after a
                // `label_limit` breach. Upstream unmarshals the entire body
                // up front (`wc.rows.UnmarshalWithErrLogger`, then
                // `samplesScraped = len(wc.rows.Rows)`,
                // `scrapework.go:545-549`) before `addRows` runs, so
                // `scrape_samples_scraped` reflects all parsed rows
                // regardless of where the `label_limit` abort later happens.
                samples_scraped += 1;
                if label_limit_exceeded {
                    // A prior row already breached `label_limit`: the whole
                    // scrape is doomed and all built series are discarded on
                    // failure, so there is no point building or keeping any
                    // more of them. But we MUST keep reading the rest of the
                    // body so `samples_scraped` stays accurate for bodies
                    // spanning more than one read block.
                    continue;
                }
                if let Some(owned) = build_series(row, cfg, scrape_timestamp_ms) {
                    if cfg.label_limit > 0 && owned.labels.len() > cfg.label_limit {
                        // Port of `errLabelsLimitExceeded`
                        // (`scrapework.go:1159-1164`): the first over-limit
                        // series fails the whole scrape. Upstream aborts
                        // `addRows` here, but that is AFTER the full body was
                        // already parsed — so we set a flag and let the parse
                        // finish reading rather than returning `Err` (which
                        // would halt the read loop and undercount
                        // `samples_scraped`). All series built so far are
                        // dropped by the caller anyway.
                        label_limit_exceeded = true;
                        series.clear();
                        samples_post_relabel = 0;
                        continue;
                    }
                    series.push(owned);
                    samples_post_relabel += 1;
                }
            }
            Ok(())
        },
    )
    .map_err(|e| {
        ScrapeFailure::Generic(format!("cannot parse response from {scrape_url:?}: {e}"))
    })?;

    if label_limit_exceeded {
        // `samples_post_relabel` stays 0: upstream leaves
        // `samplesPostRelabeling = 0` whenever `scrapeErr != nil`
        // (`scrapework.go:548-554`; the `= len(...)` assignment only runs on
        // the `scrapeErr == nil` branch).
        return Err(ScrapeFailure::LabelLimitExceeded { samples_scraped });
    }

    Ok(FetchOutcome {
        series,
        samples_scraped,
        samples_post_relabel,
        response_size,
    })
}

/// Reads `resp`'s body, capped at `max_scrape_size + 1` bytes
/// (`max_scrape_size == 0` means unlimited) so a hostile/misbehaving target
/// can't force an unbounded read. Returns an error naming the limit if the
/// cap was hit — mirrors `client.go`'s `dst.Len() > c.maxScrapeSize` check.
fn read_capped_body(
    resp: reqwest::blocking::Response,
    scrape_url: &str,
    max_scrape_size: u64,
) -> Result<Vec<u8>, String> {
    let read_limit = if max_scrape_size == 0 {
        u64::MAX
    } else {
        max_scrape_size + 1
    };
    let mut body = Vec::new();
    resp.take(read_limit)
        .read_to_end(&mut body)
        .map_err(|e| format!("cannot read response from {scrape_url:?}: {e}"))?;
    if max_scrape_size != 0 && body.len() as u64 > max_scrape_size {
        return Err(format!(
            "the response from {scrape_url:?} exceeds max_scrape_size ({max_scrape_size} bytes). \
             Possible solutions are: reduce the response size for the target, or increase \
             max_scrape_size in the scrape config"
        ));
    }
    Ok(body)
}

/// Builds one [`OwnedSeries`] from a parsed [`Row`], matching upstream
/// `addRow`'s pipeline (`scrapework.go:1144-1158`) exactly:
///
/// 1. raw labels: `__name__` = metric + tags;
/// 2. merge `target_labels` (honor_labels-aware) — *before* relabel, so a
///    `metric_relabel` rule can match on `job`/`instance`/target labels;
/// 3. apply `metric_relabel` (drop-capable);
/// 4. [`finalize_labels`]: strip every `__`-prefixed label except
///    `__name__` (relabel may introduce `__tmp_*` labels that must not be
///    emitted); skip the row if nothing but internal labels survived;
/// 5. merge `external_labels` (honor_labels-aware) — *after* relabel, per
///    <https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3137>;
/// 6. one [`Sample`], with honor_timestamps handling.
///
/// Returns `None` if `metric_relabel` dropped the row or nothing but
/// internal labels remained after finalize.
///
/// Known gap (not in Task 5's scope): the `isAutoMetric` metric-*name*
/// `exported_` clash rename (`scrapework.go:1137-1143`, run before this
/// pipeline when `!honor_labels && len(tags) == 0`) — which renames a
/// *scraped* row literally named e.g. `up` to `exported_up` so it can't
/// collide with the auto-metric of the same name — is still not
/// implemented. Task 5 only builds the auto-metrics themselves (see
/// [`super::autometrics`]); it does not touch this scraped-row rename.
fn build_series(
    row: &Row<'_>,
    cfg: &ScrapeConfigResolved,
    scrape_timestamp_ms: i64,
) -> Option<OwnedSeries> {
    let mut labels = row_labels(row);
    merge_extra_labels(&mut labels, &cfg.target_labels, cfg.honor_labels);
    if !cfg.metric_relabel.apply(&mut labels) {
        return None;
    }
    finalize_labels(&mut labels);
    if labels.is_empty() {
        // Only internal (`__*`) labels remained — skip the row (upstream's
        // `len(wc.labels) == labelsLen` check).
        return None;
    }
    merge_extra_labels(&mut labels, &cfg.external_labels, cfg.honor_labels);

    let sample_timestamp = if cfg.honor_timestamps && row.timestamp != 0 {
        row.timestamp
    } else {
        scrape_timestamp_ms
    };
    Some(OwnedSeries {
        labels,
        samples: vec![Sample {
            value: row.value,
            timestamp: sample_timestamp,
        }],
    })
}

/// Removes every label whose name starts with `__` except `__name__`. Port
/// of `promrelabel.FinalizeLabels` (`relabel.go:149-158`): `metric_relabel`
/// can introduce internal `__tmp_*` labels for intermediate work, which
/// must not be emitted as part of the final series.
pub(crate) fn finalize_labels(labels: &mut Vec<Label>) {
    labels.retain(|l| !l.name.starts_with("__") || l.name == "__name__");
}

/// Builds the raw pre-relabel label set for one row: `__name__` = the
/// metric name, then one [`Label`] per tag. Port of `appendLabels`'s
/// name+tags half (`scrapework.go:1183`); the target-labels merge that
/// `appendLabels` folds in is done as a separate [`merge_extra_labels`]
/// call in [`build_series`], immediately after this.
fn row_labels(row: &Row<'_>) -> Vec<Label> {
    let mut labels = Vec::with_capacity(row.tags.len() + 1);
    labels.push(Label {
        name: "__name__".to_string(),
        value: row.metric.to_string(),
    });
    for tag in &row.tags {
        labels.push(Label {
            name: tag.key.to_string(),
            value: tag.value.to_string(),
        });
    }
    labels
}

/// Merges `extra` into `labels` per Prometheus `honor_labels` semantics.
/// Port of `appendExtraLabels` (`scrapework.go:1199-1240`):
///
/// - `honor_labels == false` (default): on a name conflict, the *existing*
///   (scraped) label is renamed to `exported_<name>` (chaining another
///   `exported_` prefix if that name is already taken), and `extra`'s label
///   is added under the original name — i.e. the target/external label
///   wins.
/// - `honor_labels == true`: on a name conflict, `extra`'s label is
///   dropped and the existing (scraped) label is left untouched — i.e. the
///   scraped label wins.
///
/// Conflict-checking (and the window `exported_` renames search) is scoped
/// to `labels` as it stood *before* this call — labels appended during this
/// call are never revisited by a later entry in `extra`, matching
/// upstream's fixed `offset:offsetEnd` window.
pub(crate) fn merge_extra_labels(labels: &mut Vec<Label>, extra: &[Label], honor_labels: bool) {
    let window_end = labels.len();
    for extra_label in extra {
        let existing_pos = labels[..window_end]
            .iter()
            .position(|l| l.name == extra_label.name);
        match existing_pos {
            None => labels.push(extra_label.clone()),
            Some(pos) => {
                if honor_labels {
                    continue;
                }
                let exported_name = format!("exported_{}", extra_label.name);
                if let Some(dup_pos) = labels[..window_end]
                    .iter()
                    .position(|l| l.name == exported_name)
                {
                    labels[dup_pos].name = format!("exported_{exported_name}");
                }
                labels[pos].name = exported_name;
                labels.push(extra_label.clone());
            }
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "scrapework_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "scrapework_limits_tests.rs"]
mod limits_tests;

#[cfg(test)]
#[path = "scrapework_compression_tests.rs"]
mod compression_tests;
