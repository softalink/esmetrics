//! Pure timestamp/duration math used by [`super::group::Group`]'s
//! evaluation loop: the evaluation-timestamp adjustment, the alert
//! auto-resolve deadline, the deterministic start-delay spread, and the
//! current-time helper the loop reads its tick timestamp from.
//!
//! Split out of `group.rs` purely to keep that file under this crate's
//! 800-line file cap — these functions have no dependency on [`Group`]
//! itself (they're free functions of plain `Duration`/`i64`/`&str`
//! arguments), which is what makes them straightforward to test in
//! isolation (see this module's `tests`) and to relocate without touching
//! `Group`'s own logic.
//!
//! Port of `app/vmalert/rule/group.go`'s `getResolveDuration` (`:690-700`),
//! `adjustReqTimestamp`/`getEvalDelay` (`:702-723`), and `delayBeforeStart`
//! (`:506-533`).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Default `-rule.evalDelay` (`group.go:35-37`): compensates for
/// intentional data delay from the datasource when a group has no explicit
/// `eval_delay`.
pub const DEFAULT_EVAL_DELAY: Duration = Duration::from_secs(30);

/// Returns the duration after which a still-firing alert should be
/// considered auto-resolved by the notifier (i.e. the deadline written as
/// `endsAt`), so Alertmanager expires it on its own if this esmalert
/// instance stops sending updates.
///
/// Port of `getResolveDuration` (`group.go:690-700`): `delta =
/// max(group_interval, resend_delay)`; `resolve_duration = delta * 4`;
/// clamped to `max_resolve_duration` if that's set and smaller.
pub fn get_resolve_duration(
    group_interval: Duration,
    resend_delay: Duration,
    max_resolve_duration: Option<Duration>,
) -> Duration {
    let delta = group_interval.max(resend_delay);
    let resolve_duration = delta.saturating_mul(4);
    match max_resolve_duration {
        Some(max) if !max.is_zero() && resolve_duration > max => max,
        _ => resolve_duration,
    }
}

/// Adjusts a raw evaluation timestamp the way upstream's `eval` closure
/// does before querying the datasource. Port of `adjustReqTimestamp`
/// (`group.go:702-716`) plus `getEvalDelay` (`:718-723`), collapsed into one
/// pure function of the pieces `Group` carries:
/// - if `eval_offset` is set, upstream leaves the timestamp untouched here
///   (offset alignment happens once, in `delayBeforeStart`/[`delay_before_start`]
///   picking the tick's phase — not re-applied per tick);
/// - otherwise `eval_delay` (or [`DEFAULT_EVAL_DELAY`] if unset) is
///   subtracted;
/// - then, if `eval_alignment`, the result is truncated down to the nearest
///   `interval` boundary (`time.Truncate`), via Euclidean remainder so it's
///   correct even if the subtraction above made the timestamp negative.
pub fn adjust_req_timestamp(
    ts: i64,
    interval: Duration,
    eval_offset: Option<Duration>,
    eval_delay: Option<Duration>,
    eval_alignment: bool,
) -> i64 {
    if eval_offset.is_some() {
        return ts;
    }
    let delay_ms =
        i64::try_from(eval_delay.unwrap_or(DEFAULT_EVAL_DELAY).as_millis()).unwrap_or(i64::MAX);
    let adjusted = ts.saturating_sub(delay_ms);
    if !eval_alignment {
        return adjusted;
    }
    let interval_ms = i64::try_from(interval.as_millis()).unwrap_or(0);
    if interval_ms <= 0 {
        return adjusted;
    }
    adjusted - adjusted.rem_euclid(interval_ms)
}

/// Deterministic (never `rand`) replacement for upstream's ID-hash-based
/// `delayBeforeStart` (`group.go:506-533`) for the common no-`eval_offset`
/// case: a hash of the group's name maps into `[0, min(interval,
/// max_delay)]`, spreading multiple groups' (and, across esmalert
/// instances loading the same file, multiple instances') first evaluation
/// instead of firing in lockstep. Doesn't port the `eval_offset`-aligned
/// branch (see `Group::start`'s doc comment).
pub(super) fn delay_before_start(name: &str, interval: Duration, max_delay: Duration) -> Duration {
    let bound = interval.min(max_delay);
    if bound.is_zero() {
        return Duration::ZERO;
    }
    let frac = (fnv1a64(name.as_bytes()) as f64) / (u64::MAX as f64);
    Duration::from_secs_f64(bound.as_secs_f64() * frac)
}

/// A small, dependency-free FNV-1a hash, used only for
/// [`delay_before_start`]'s deterministic spread (not a cryptographic or
/// collision-resistant hash). Duplicated (not shared) from the equivalent
/// helper in `remotewrite::client` / `rule::alert` — this repo's
/// established convention (see those modules' doc comments) is to
/// duplicate small already-verified helpers per module.
fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Current unix-millis timestamp, saturating to `i64::MAX`/`0` instead of
/// panicking on an implausible system clock (pre-epoch or overflowing
/// `i64` millis).
pub(super) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjust_req_timestamp_passes_through_when_eval_offset_set() {
        let ts = adjust_req_timestamp(
            100_000,
            Duration::from_secs(60),
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(30)),
            true,
        );
        assert_eq!(ts, 100_000);
    }

    #[test]
    fn adjust_req_timestamp_subtracts_eval_delay_without_alignment() {
        let ts = adjust_req_timestamp(
            100_000,
            Duration::from_secs(60),
            None,
            Some(Duration::from_secs(30)),
            false,
        );
        assert_eq!(ts, 70_000);
    }

    #[test]
    fn adjust_req_timestamp_uses_default_delay_when_unset() {
        let ts = adjust_req_timestamp(100_000, Duration::from_secs(60), None, None, false);
        // DEFAULT_EVAL_DELAY is 30s -> 30_000ms.
        assert_eq!(ts, 70_000);
    }

    #[test]
    fn adjust_req_timestamp_truncates_to_interval_when_aligned() {
        let ts = adjust_req_timestamp(
            125_000,
            Duration::from_secs(60),
            None,
            Some(Duration::ZERO),
            true,
        );
        // adjusted = 125_000 (no delay); truncated down to nearest 60_000.
        assert_eq!(ts, 120_000);
    }

    #[test]
    fn get_resolve_duration_is_four_times_the_larger_of_interval_and_resend_delay() {
        let d = get_resolve_duration(Duration::from_secs(30), Duration::from_secs(10), None);
        assert_eq!(d, Duration::from_secs(120));

        let d2 = get_resolve_duration(Duration::from_secs(10), Duration::from_secs(40), None);
        assert_eq!(d2, Duration::from_secs(160));
    }

    #[test]
    fn get_resolve_duration_clamps_to_max_resolve_duration() {
        let d = get_resolve_duration(
            Duration::from_secs(60),
            Duration::ZERO,
            Some(Duration::from_secs(60)),
        );
        assert_eq!(d, Duration::from_secs(60));
    }

    #[test]
    fn get_resolve_duration_ignores_zero_max_resolve_duration() {
        let d = get_resolve_duration(
            Duration::from_secs(30),
            Duration::ZERO,
            Some(Duration::ZERO),
        );
        assert_eq!(d, Duration::from_secs(120));
    }

    #[test]
    fn delay_before_start_is_zero_when_bound_is_zero() {
        assert_eq!(
            delay_before_start("g", Duration::from_secs(60), Duration::ZERO),
            Duration::ZERO
        );
        assert_eq!(
            delay_before_start("g", Duration::ZERO, Duration::from_secs(60)),
            Duration::ZERO
        );
    }

    #[test]
    fn delay_before_start_is_bounded_and_deterministic() {
        let bound = Duration::from_secs(60);
        let a = delay_before_start("group-a", bound, bound);
        let b = delay_before_start("group-a", bound, bound);
        assert_eq!(a, b, "same name must yield the same delay");
        assert!(a <= bound);

        let c = delay_before_start("group-b", bound, bound);
        assert_ne!(
            a, c,
            "different names should (overwhelmingly likely) differ"
        );
    }
}
