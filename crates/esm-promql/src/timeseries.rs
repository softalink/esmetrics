//! Timeseries model and metric-name helpers. Port of `timeseries.go` plus
//! the group-key marshaling used across aggregation, binary ops and result
//! deduplication.

use esm_storage::metric_name::{MetricName, Tag};
use std::sync::Arc;

use crate::{Error, Result};

/// A time series produced during query evaluation. Port of the Go
/// `timeseries` struct.
///
/// All series produced by a single rollup share the same `timestamps`
/// allocation (the shared grid) via `Arc`; `values` are always owned.
/// The Go `denyReuse` machinery is unnecessary: `Arc` handles sharing.
#[derive(Debug, Clone, Default)]
pub struct Timeseries {
    pub metric_name: MetricName,
    pub values: Vec<f64>,
    pub timestamps: Arc<Vec<i64>>,
}

impl Timeseries {
    /// Port of Go `timeseries.CopyFromShallowTimestamps`: deep-copies the
    /// metric name and values, shares timestamps.
    pub fn copy_from_shallow_timestamps(src: &Timeseries) -> Timeseries {
        Timeseries {
            metric_name: src.metric_name.clone(),
            values: src.values.clone(),
            timestamps: Arc::clone(&src.timestamps),
        }
    }
}

/// Generates the shared timestamps grid `start, start+step, ..., end`.
/// Port of Go `getTimestamps`.
pub fn get_timestamps(
    start: i64,
    end: i64,
    step: i64,
    max_points_per_series: usize,
) -> Result<Arc<Vec<i64>>> {
    assert!(step > 0, "BUG: Step must be bigger than 0; got {step}");
    assert!(
        start <= end,
        "BUG: Start cannot exceed End; got {start} vs {end}"
    );
    validate_max_points_per_series(start, end, step, max_points_per_series)?;
    let points = (1 + (end - start) / step) as usize;
    let mut timestamps = Vec::with_capacity(points);
    let mut ts = start;
    for _ in 0..points {
        timestamps.push(ts);
        ts += step;
    }
    Ok(Arc::new(timestamps))
}

/// Port of Go `ValidateMaxPointsPerSeries`.
pub fn validate_max_points_per_series(
    start: i64,
    end: i64,
    step: i64,
    max_points: usize,
) -> Result<()> {
    if step == 0 {
        return Err(Error::new("step can't be equal to zero"));
    }
    let points = (end - start) / step + 1;
    if points > max_points as i64 {
        return Err(Error::new(format!(
            "too many points for the given start={start}, end={end} and step={step}: {points}; \
             the maximum number of points is {max_points}"
        )));
    }
    Ok(())
}

/// Sorts `mn.tags` by raw key bytes (NOT the storage `sortTags` sentinel
/// order). Port of Go promql `sortMetricTags`.
pub fn sort_metric_tags(mn: &mut MetricName) {
    if !mn.tags.is_sorted_by(|a, b| a.key <= b.key) {
        mn.tags.sort_by(|a, b| a.key.cmp(&b.key));
    }
}

/// Appends the canonical group key for `mn` to `dst`: u16le-length-prefixed
/// metric group followed by u16le-length-prefixed (key, value) pairs of tags
/// sorted by key. Port of Go `marshalMetricNameSorted`.
///
/// This key is used everywhere grouping/matching happens: aggregation
/// grouping, incremental aggregation, binary-op matching and duplicate
/// output detection.
pub fn marshal_metric_name_sorted(dst: &mut Vec<u8>, mn: &mut MetricName) {
    marshal_bytes_fast(dst, &mn.metric_group);
    sort_metric_tags(mn);
    for tag in &mn.tags {
        marshal_bytes_fast(dst, &tag.key);
        marshal_bytes_fast(dst, &tag.value);
    }
}

/// Port of Go `marshalBytesFast` (little-endian u16 length prefix as used by
/// promql group keys; the concrete byte order is irrelevant as long as it is
/// consistent, since the keys never leave the process).
fn marshal_bytes_fast(dst: &mut Vec<u8>, s: &[u8]) {
    dst.extend_from_slice(&(s.len() as u16).to_le_bytes());
    dst.extend_from_slice(s);
}

/// Returns the group key for `mn` as a fresh Vec. See
/// [`marshal_metric_name_sorted`].
pub fn metric_name_group_key(mn: &mut MetricName) -> Vec<u8> {
    let mut dst = Vec::with_capacity(64);
    marshal_metric_name_sorted(&mut dst, mn);
    dst
}

/// Port of Go `metricNameLess`; tags must be sorted by the caller.
pub fn metric_name_less(a: &MetricName, b: &MetricName) -> bool {
    if a.metric_group != b.metric_group {
        return a.metric_group < b.metric_group;
    }
    let ats = &a.tags;
    let bts = &b.tags;
    for (i, at) in ats.iter().enumerate() {
        if i >= bts.len() {
            return false;
        }
        let bt = &bts[i];
        if at.key != bt.key {
            return at.key < bt.key;
        }
        if at.value != bt.value {
            return at.value < bt.value;
        }
    }
    ats.len() < bts.len()
}

/// Port of Go `sortSeriesByMetricName`. Sorts tags of every series first,
/// since `metric_name_less` requires sorted tags.
pub fn sort_series_by_metric_name(tss: &mut [Timeseries]) {
    for ts in tss.iter_mut() {
        sort_metric_tags(&mut ts.metric_name);
    }
    tss.sort_by(|a, b| {
        if metric_name_less(&a.metric_name, &b.metric_name) {
            std::cmp::Ordering::Less
        } else if metric_name_less(&b.metric_name, &a.metric_name) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}

/// Drops series with all-NaN values. Port of Go `removeEmptySeries`.
pub fn remove_empty_series(tss: Vec<Timeseries>) -> Vec<Timeseries> {
    tss.into_iter()
        .filter(|ts| ts.values.iter().any(|v| !v.is_nan()))
        .collect()
}

/// Port of Go `storage.MetricName.SetTags`: copies the values of `add_tags`
/// from `src` to `dst` with the optional `prefix`, skipping `skip_tags`.
/// `add_tags == ["*"]` copies all tags from `src` except `skip_tags`.
///
/// Used by `group_left`/`group_right` joins. Lives here because
/// esm-storage's `MetricName` doesn't ship `SetTags` in Stage 1.
pub fn set_tags(
    dst: &mut MetricName,
    add_tags: &[String],
    prefix: &str,
    skip_tags: &[String],
    src: &MetricName,
) {
    if add_tags.len() == 1 && add_tags[0] == "*" {
        // Special case for copying all the tags except of skip_tags from src.
        for tag in &src.tags {
            let key = std::str::from_utf8(&tag.key).unwrap_or_default();
            if skip_tags.iter().any(|s| s == key) {
                continue;
            }
            let mut new_key = Vec::with_capacity(prefix.len() + tag.key.len());
            new_key.extend_from_slice(prefix.as_bytes());
            new_key.extend_from_slice(&tag.key);
            set_tag_bytes(dst, &new_key, &tag.value);
        }
        return;
    }
    for tag_name in add_tags {
        if skip_tags.contains(tag_name) {
            continue;
        }
        if tag_name == "__name__" {
            dst.metric_group.clear();
            dst.metric_group.extend_from_slice(&src.metric_group);
            continue;
        }
        let src_tag = src.tags.iter().find(|t| t.key == tag_name.as_bytes());
        match src_tag {
            None => dst.remove_tag(tag_name),
            Some(t) => {
                let mut new_key = Vec::with_capacity(prefix.len() + tag_name.len());
                new_key.extend_from_slice(prefix.as_bytes());
                new_key.extend_from_slice(tag_name.as_bytes());
                set_tag_bytes(dst, &new_key, &t.value);
            }
        }
    }
}

/// Port of Go `storage.MetricName.SetTagBytes`: sets the tag value, adding
/// the tag if missing.
fn set_tag_bytes(mn: &mut MetricName, key: &[u8], value: &[u8]) {
    for tag in &mut mn.tags {
        if tag.key == key {
            tag.value.clear();
            tag.value.extend_from_slice(value);
            return;
        }
    }
    mn.tags.push(Tag {
        key: key.to_vec(),
        value: value.to_vec(),
    });
}

/// Human-readable `{tag="value", ...}` form. Port of Go `stringMetricTags`.
pub fn string_metric_tags(mn: &mut MetricName) -> String {
    sort_metric_tags(mn);
    let mut dst = String::from("{");
    for (i, tag) in mn.tags.iter().enumerate() {
        if i > 0 {
            dst.push_str(", ");
        }
        dst.push_str(&String::from_utf8_lossy(&tag.key));
        dst.push('=');
        dst.push_str(&format!("{:?}", String::from_utf8_lossy(&tag.value)));
    }
    dst.push('}');
    dst
}

/// Human-readable `name{tag="value", ...}` form. Port of Go
/// `stringMetricName`.
pub fn string_metric_name(mn: &mut MetricName) -> String {
    let mut dst = String::from_utf8_lossy(&mn.metric_group).into_owned();
    dst.push_str(&string_metric_tags(mn));
    dst
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_key_distinguishes_boundaries() {
        // ("ab", "c") must differ from ("a", "bc").
        let mut mn1 = MetricName::default();
        mn1.add_tag("x", "ab");
        mn1.add_tag("y", "c");
        let mut mn2 = MetricName::default();
        mn2.add_tag("x", "a");
        mn2.add_tag("y", "bc");
        assert_ne!(
            metric_name_group_key(&mut mn1),
            metric_name_group_key(&mut mn2)
        );
    }

    #[test]
    fn group_key_is_order_independent() {
        let mut mn1 = MetricName::default();
        mn1.add_tag("b", "2");
        mn1.add_tag("a", "1");
        let mut mn2 = MetricName::default();
        mn2.add_tag("a", "1");
        mn2.add_tag("b", "2");
        assert_eq!(
            metric_name_group_key(&mut mn1),
            metric_name_group_key(&mut mn2)
        );
    }

    #[test]
    fn metric_name_less_ordering() {
        let mut a = MetricName {
            metric_group: b"foo".to_vec(),
            ..Default::default()
        };
        let mut b = MetricName {
            metric_group: b"foo".to_vec(),
            ..Default::default()
        };
        b.add_tag("x", "1");
        assert!(metric_name_less(&a, &b));
        assert!(!metric_name_less(&b, &a));

        a.add_tag("x", "1");
        assert!(!metric_name_less(&a, &b));
        assert!(!metric_name_less(&b, &a));
    }

    #[test]
    fn set_tags_copies_and_removes() {
        let mut src = MetricName::default();
        src.add_tag("node", "n1");
        let mut dst = MetricName::default();
        dst.add_tag("pod", "p1");
        dst.add_tag("gone", "x");
        set_tags(
            &mut dst,
            &["node".to_string(), "gone".to_string()],
            "",
            &[],
            &src,
        );
        assert_eq!(dst.get_tag_value("node"), Some(b"n1".as_slice()));
        assert_eq!(dst.get_tag_value("gone"), None);
        assert_eq!(dst.get_tag_value("pod"), Some(b"p1".as_slice()));
    }
}
