//! `/api/v1/targets` JSON envelope. Port of the JSON (not HTML/qtpl) shape
//! from `lib/promscrape/targetstatus.go`'s `WriteTargetsResponse`. This
//! module only produces the serialized JSON string from a
//! [`super::manager::TargetsSnapshot`] — the HTTP route + CLI wiring that
//! calls `ScrapeManager::targets_snapshot()` and serves this at
//! `GET /api/v1/targets` is a later task (see `manager`'s module doc).

use std::collections::HashMap;

use serde::Serialize;

use super::manager::{ActiveTarget, DroppedTargetView, Health, TargetsSnapshot};
use esm_relabel::Label;

/// Builds the Prometheus-compatible `/api/v1/targets` JSON envelope from a
/// snapshot, applying `state`'s optional `active`/`dropped` filter.
///
/// `state`: `None` includes both `activeTargets` and `droppedTargets`.
/// `Some("active")` populates only `activeTargets` (`droppedTargets: []`).
/// `Some("dropped")` populates only `droppedTargets` (`activeTargets: []`).
/// Any other value is treated leniently, like an unrecognized filter is
/// simply ignored upstream — both lists are included. Both envelope keys
/// are always present, even when the corresponding list is empty because
/// of a filter.
pub fn targets_json(snapshot: &TargetsSnapshot, state: Option<&str>) -> String {
    let include_active = state != Some("dropped");
    let include_dropped = state != Some("active");

    let active_targets: Vec<ActiveTargetView> = if include_active {
        snapshot.active.iter().map(ActiveTargetView::from).collect()
    } else {
        Vec::new()
    };
    let dropped_targets: Vec<DroppedTargetViewJson> = if include_dropped {
        snapshot
            .dropped
            .iter()
            .map(DroppedTargetViewJson::from)
            .collect()
    } else {
        Vec::new()
    };

    let envelope = TargetsResponse {
        status: "success",
        data: TargetsData {
            active_targets,
            dropped_targets,
        },
    };

    // `TargetsResponse` is a fixed, always-serializable shape (no
    // `Result`/interior state that can fail), so this can't panic in
    // practice — `unwrap_or_default` still avoids a panic path outside
    // tests per this crate's no-panic convention.
    serde_json::to_string(&envelope).unwrap_or_default()
}

#[derive(Serialize)]
struct TargetsResponse {
    status: &'static str,
    data: TargetsData,
}

#[derive(Serialize)]
struct TargetsData {
    #[serde(rename = "activeTargets")]
    active_targets: Vec<ActiveTargetView>,
    #[serde(rename = "droppedTargets")]
    dropped_targets: Vec<DroppedTargetViewJson>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActiveTargetView {
    scrape_pool: String,
    scrape_url: String,
    labels: HashMap<String, String>,
    discovered_labels: HashMap<String, String>,
    health: Health,
    last_error: String,
    last_scrape: String,
    last_scrape_duration: f64,
}

impl From<&ActiveTarget> for ActiveTargetView {
    fn from(t: &ActiveTarget) -> Self {
        ActiveTargetView {
            scrape_pool: t.scrape_pool.clone(),
            scrape_url: t.scrape_url.clone(),
            labels: labels_to_map(&t.labels),
            discovered_labels: labels_to_map(&t.discovered_labels),
            health: t.health,
            last_error: t.last_error.clone().unwrap_or_default(),
            last_scrape: rfc3339_from_unix_ms(t.last_scrape_ms),
            last_scrape_duration: t.last_scrape_duration_ms as f64 / 1000.0,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DroppedTargetViewJson {
    discovered_labels: HashMap<String, String>,
}

impl From<&DroppedTargetView> for DroppedTargetViewJson {
    fn from(d: &DroppedTargetView) -> Self {
        DroppedTargetViewJson {
            discovered_labels: labels_to_map(&d.discovered_labels),
        }
    }
}

/// Converts a `Vec<Label>` into a JSON object map. If two labels share a
/// name (shouldn't happen post-relabel) the last one wins, matching a plain
/// object-build (later insert overwrites earlier).
fn labels_to_map(labels: &[Label]) -> HashMap<String, String> {
    labels
        .iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

/// Formats a unix-millis timestamp as RFC3339 seconds-precision UTC.
/// `0` (never scraped) formats as `"1970-01-01T00:00:00Z"`, which is
/// acceptable per this task's brief. Duplicated (not shared) from the
/// equivalent civil-date algorithm in `esmalert::rule::alert::format_active_at`
/// / `esm-backup::timeutil::rfc3339_from_unix` — this repo's established
/// convention (see `esm-gotemplate::value`'s `format_float_go_g` doc
/// comment) is to duplicate small already-verified helpers per module
/// rather than add cross-crate plumbing for one function. There is no
/// chrono/time crate dependency in this workspace.
fn rfc3339_from_unix_ms(ms: i64) -> String {
    let unix_secs = ms.div_euclid(1000).max(0) as u64;
    let (y, mo, d, h, mi, s) = civil_from_unix(unix_secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn civil_from_unix(unix_secs: u64) -> (i64, u64, u64, u64, u64, u64) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, d, h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn sample_snapshot() -> TargetsSnapshot {
        TargetsSnapshot {
            active: vec![ActiveTarget {
                scrape_pool: "job1".to_string(),
                scrape_url: "http://localhost:9100/metrics".to_string(),
                labels: vec![label("instance", "localhost:9100"), label("job", "job1")],
                discovered_labels: vec![label("__address__", "localhost:9100")],
                health: Health::Up,
                last_error: None,
                last_scrape_ms: 1_783_082_096_000,
                last_scrape_duration_ms: 250,
            }],
            dropped: vec![DroppedTargetView {
                discovered_labels: vec![label("__address__", "localhost:9200")],
            }],
        }
    }

    #[test]
    fn targets_json_shape_and_filter() {
        let snap = sample_snapshot();

        let j: serde_json::Value = serde_json::from_str(&targets_json(&snap, None)).unwrap();
        assert_eq!(j["status"], "success");
        assert_eq!(j["data"]["activeTargets"][0]["health"], "up");
        assert_eq!(j["data"]["activeTargets"][0]["scrapePool"], "job1");
        assert_eq!(
            j["data"]["activeTargets"][0]["scrapeUrl"],
            "http://localhost:9100/metrics"
        );
        assert_eq!(j["data"]["activeTargets"][0]["lastError"], "");
        assert_eq!(
            j["data"]["activeTargets"][0]["lastScrape"],
            "2026-07-03T12:34:56Z"
        );
        assert_eq!(j["data"]["activeTargets"][0]["lastScrapeDuration"], 0.25);
        assert!(j["data"]["activeTargets"][0]["labels"].is_object());
        assert_eq!(
            j["data"]["activeTargets"][0]["labels"]["instance"],
            "localhost:9100"
        );
        assert_eq!(j["data"]["activeTargets"][0]["labels"]["job"], "job1");
        assert!(j["data"]["activeTargets"][0]["discoveredLabels"].is_object());
        assert_eq!(
            j["data"]["activeTargets"][0]["discoveredLabels"]["__address__"],
            "localhost:9100"
        );
        assert!(j["data"]["droppedTargets"].as_array().unwrap().len() == 1);
        assert_eq!(
            j["data"]["droppedTargets"][0]["discoveredLabels"]["__address__"],
            "localhost:9200"
        );

        // state filter: active only
        let active_only: serde_json::Value =
            serde_json::from_str(&targets_json(&snap, Some("active"))).unwrap();
        assert_eq!(
            active_only["data"]["activeTargets"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            active_only["data"]["droppedTargets"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        // state filter: dropped only
        let dropped_only: serde_json::Value =
            serde_json::from_str(&targets_json(&snap, Some("dropped"))).unwrap();
        assert_eq!(
            dropped_only["data"]["activeTargets"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            dropped_only["data"]["droppedTargets"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // unrecognized filter value: lenient, include both
        let lenient: serde_json::Value =
            serde_json::from_str(&targets_json(&snap, Some("bogus"))).unwrap();
        assert_eq!(
            lenient["data"]["activeTargets"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            lenient["data"]["droppedTargets"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn never_scraped_target_uses_epoch() {
        let mut snap = sample_snapshot();
        snap.active[0].last_scrape_ms = 0;
        snap.active[0].health = Health::Unknown;
        snap.active[0].last_error = Some("connection refused".to_string());

        let j: serde_json::Value = serde_json::from_str(&targets_json(&snap, None)).unwrap();
        assert_eq!(
            j["data"]["activeTargets"][0]["lastScrape"],
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(j["data"]["activeTargets"][0]["health"], "unknown");
        assert_eq!(
            j["data"]["activeTargets"][0]["lastError"],
            "connection refused"
        );
    }

    #[test]
    fn empty_snapshot_produces_empty_arrays_not_omitted_keys() {
        let snap = TargetsSnapshot::default();
        let j: serde_json::Value = serde_json::from_str(&targets_json(&snap, None)).unwrap();
        assert!(j["data"]["activeTargets"].is_array());
        assert!(j["data"]["droppedTargets"].is_array());
        assert_eq!(j["data"]["activeTargets"].as_array().unwrap().len(), 0);
        assert_eq!(j["data"]["droppedTargets"].as_array().unwrap().len(), 0);
    }
}
