//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/metric_name.go.
//!
//! Covers: tag-value escaping, `Tag`, `MetricName`, `sort_tags` with the
//! `commonTagKeys` sentinel table, the canonical (escaped) marshaling used in
//! the index, and the raw (u16-length-prefixed) `MetricNameRaw` encoding used
//! in `MetricRow` and as the tsidCache key.
//!
//! PORT-SKIP (later stages): sync.Pool helpers (GetMetricName/PutMetricName),
//! MetricName.SetTags/setAllTags (relabeling, stage 5), UnmarshalString.

use esm_encoding as encoding;
use std::borrow::Cow;
use std::fmt;

/// Go: escapeChar.
pub const ESCAPE_CHAR: u8 = 0;
/// Go: tagSeparatorChar.
pub const TAG_SEPARATOR_CHAR: u8 = 1;
/// Go: kvSeparatorChar.
pub const KV_SEPARATOR_CHAR: u8 = 2;

/// Go: metricGroupTagKey.
const METRIC_GROUP_TAG_KEY: &[u8] = b"__name__";

/// Appends escaped `src` to `dst`, terminated with [`TAG_SEPARATOR_CHAR`].
/// Escapes: `0x00 -> 0x00 '0'`, `0x01 -> 0x00 '1'`, `0x02 -> 0x00 '2'`.
/// Go: marshalTagValue.
pub fn marshal_tag_value(dst: &mut Vec<u8>, src: &[u8]) {
    if !src
        .iter()
        .any(|&ch| ch == ESCAPE_CHAR || ch == TAG_SEPARATOR_CHAR || ch == KV_SEPARATOR_CHAR)
    {
        // Fast path.
        dst.extend_from_slice(src);
        dst.push(TAG_SEPARATOR_CHAR);
        return;
    }

    // Slow path.
    for &ch in src {
        match ch {
            ESCAPE_CHAR => dst.extend_from_slice(&[ESCAPE_CHAR, b'0']),
            TAG_SEPARATOR_CHAR => dst.extend_from_slice(&[ESCAPE_CHAR, b'1']),
            KV_SEPARATOR_CHAR => dst.extend_from_slice(&[ESCAPE_CHAR, b'2']),
            _ => dst.push(ch),
        }
    }
    dst.push(TAG_SEPARATOR_CHAR);
}

/// Unescapes a tag value from `src` into `dst` (scanning up to the first
/// unescaped [`TAG_SEPARATOR_CHAR`]) and returns the rest of `src`.
/// Go: unmarshalTagValue.
pub fn unmarshal_tag_value<'a>(dst: &mut Vec<u8>, src: &'a [u8]) -> Result<&'a [u8], String> {
    let Some(n) = src.iter().position(|&c| c == TAG_SEPARATOR_CHAR) else {
        return Err("cannot find the end of tag value".to_string());
    };
    let mut b = &src[..n];
    let tail = &src[n + 1..];
    loop {
        let Some(n) = b.iter().position(|&c| c == ESCAPE_CHAR) else {
            dst.extend_from_slice(b);
            return Ok(tail);
        };
        dst.extend_from_slice(&b[..n]);
        b = &b[n + 1..];
        if b.is_empty() {
            return Err("missing escaped char".to_string());
        }
        match b[0] {
            b'0' => dst.push(ESCAPE_CHAR),
            b'1' => dst.push(TAG_SEPARATOR_CHAR),
            b'2' => dst.push(KV_SEPARATOR_CHAR),
            ch => return Err(format!("unsupported escaped char: {}", ch as char)),
        }
        b = &b[1..];
    }
}

/// Tag represents a (key, value) tag for a metric. Go: Tag.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tag {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Tag {
    /// Appends marshaled tag to `dst`. Go: Tag.Marshal.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_tag_value(dst, &self.key);
        marshal_tag_value(dst, &self.value);
    }

    /// Unmarshals tag from `src` and returns the remaining data.
    /// Go: Tag.Unmarshal.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        self.key.clear();
        let src = unmarshal_tag_value(&mut self.key, src)
            .map_err(|err| format!("cannot unmarshal key: {err}"))?;
        self.value.clear();
        let src = unmarshal_tag_value(&mut self.value, src)
            .map_err(|err| format!("cannot unmarshal value: {err}"))?;
        Ok(src)
    }
}

/// MetricName represents a metric name. Go: MetricName.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MetricName {
    pub metric_group: Vec<u8>,

    /// Tags are optional. They must be sorted by tag key for canonical view.
    /// Use the [`MetricName::sort_tags`] method.
    pub tags: Vec<Tag>,
}

impl MetricName {
    /// Resets the metric name. Go: MetricName.Reset.
    pub fn reset(&mut self) {
        self.metric_group.clear();
        self.tags.clear();
    }

    /// Copies `src` to `self`. Go: MetricName.CopyFrom.
    pub fn copy_from(&mut self, src: &MetricName) {
        self.metric_group.clear();
        self.metric_group.extend_from_slice(&src.metric_group);
        self.tags.clone_from(&src.tags);
    }

    /// Adds new tag to `self` with the given key and value.
    /// Go: MetricName.AddTag.
    pub fn add_tag(&mut self, key: &str, value: &str) {
        self.add_tag_bytes(key.as_bytes(), value.as_bytes());
    }

    /// Adds new tag to `self` with the given key and value.
    /// Go: MetricName.AddTagBytes.
    pub fn add_tag_bytes(&mut self, key: &[u8], value: &[u8]) {
        if key == METRIC_GROUP_TAG_KEY {
            self.metric_group.extend_from_slice(value);
            return;
        }
        self.tags.push(Tag {
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    /// Resets `metric_group`. Go: MetricName.ResetMetricGroup.
    pub fn reset_metric_group(&mut self) {
        self.metric_group.clear();
    }

    /// Removes all the tags not included in `on_tags`.
    /// Go: MetricName.RemoveTagsOn.
    pub fn remove_tags_on(&mut self, on_tags: &[&str]) {
        if !on_tags
            .iter()
            .any(|&t| t.as_bytes() == METRIC_GROUP_TAG_KEY)
        {
            self.reset_metric_group();
        }
        let tags = std::mem::take(&mut self.tags);
        if on_tags.is_empty() {
            return;
        }
        for tag in &tags {
            if on_tags.iter().any(|&t| t.as_bytes() == tag.key) {
                self.add_tag_bytes(&tag.key, &tag.value);
            }
        }
    }

    /// Removes a tag with the given `tag_key`. Go: MetricName.RemoveTag.
    pub fn remove_tag(&mut self, tag_key: &str) {
        if tag_key == "__name__" {
            self.reset_metric_group();
            return;
        }
        let tags = std::mem::take(&mut self.tags);
        for tag in &tags {
            if tag.key != tag_key.as_bytes() {
                self.add_tag_bytes(&tag.key, &tag.value);
            }
        }
    }

    /// Removes all the tags included in `ignoring_tags`.
    /// Go: MetricName.RemoveTagsIgnoring.
    pub fn remove_tags_ignoring(&mut self, ignoring_tags: &[&str]) {
        if ignoring_tags.is_empty() {
            return;
        }
        if ignoring_tags
            .iter()
            .any(|&t| t.as_bytes() == METRIC_GROUP_TAG_KEY)
        {
            self.reset_metric_group();
        }
        let tags = std::mem::take(&mut self.tags);
        for tag in &tags {
            if !ignoring_tags.iter().any(|&t| t.as_bytes() == tag.key) {
                self.add_tag_bytes(&tag.key, &tag.value);
            }
        }
    }

    /// Returns tag value for the given `tag_key` (`"__name__"` returns the
    /// metric group). Go: MetricName.GetTagValue.
    pub fn get_tag_value(&self, tag_key: &str) -> Option<&[u8]> {
        if tag_key == "__name__" {
            return Some(&self.metric_group);
        }
        self.tags
            .iter()
            .find(|tag| tag.key == tag_key.as_bytes())
            .map(|tag| tag.value.as_slice())
    }

    /// Appends the marshaled canonical index form of `self` to `dst`.
    ///
    /// [`MetricName::sort_tags`] must be called before calling this function
    /// in order to sort and de-duplicate tags. Go: MetricName.Marshal.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_tag_value(dst, &self.metric_group);
        for tag in &self.tags {
            tag.marshal(dst);
        }
    }

    /// Sorts tags and appends the canonical marshaled form to `dst`.
    ///
    /// This is the form used as the key in index entries and caches keyed by
    /// canonical metric names.
    pub fn marshal_canonical(&mut self, dst: &mut Vec<u8>) {
        self.sort_tags();
        self.marshal(dst);
    }

    /// Unmarshals `self` from `src` (canonical index form).
    /// Go: MetricName.Unmarshal.
    pub fn unmarshal(&mut self, src: &[u8]) -> Result<(), String> {
        self.metric_group.clear();
        let mut src = unmarshal_tag_value(&mut self.metric_group, src)
            .map_err(|err| format!("cannot unmarshal MetricGroup: {err}"))?;

        self.tags.clear();
        while !src.is_empty() {
            let mut tag = Tag::default();
            src = tag
                .unmarshal(src)
                .map_err(|err| format!("cannot unmarshal tag: {err}"))?;
            self.tags.push(tag);
        }

        // There is no need in verifying for identical tag keys, since they
        // must be handled by MetricName.sort_tags before MetricName.marshal.
        Ok(())
    }

    /// Marshals `self` to `dst` in the raw (u16-length-prefixed) form,
    /// prepending an empty (key, value) pair for the metric group slot.
    ///
    /// The result may be unmarshaled with [`MetricName::unmarshal_raw`].
    /// This function is for testing purposes; [`marshal_metric_name_raw`]
    /// must be used in prod instead. Go: MetricName.marshalRaw.
    pub fn marshal_raw(&mut self, dst: &mut Vec<u8>) {
        marshal_bytes_fast(dst, b"");
        marshal_bytes_fast(dst, &self.metric_group);

        self.sort_tags();
        for tag in &self.tags {
            marshal_bytes_fast(dst, &tag.key);
            marshal_bytes_fast(dst, &tag.value);
        }
    }

    /// Unmarshals `self` from data encoded with [`marshal_metric_name_raw`]
    /// or [`MetricName::marshal_raw`]. Go: MetricName.UnmarshalRaw.
    pub fn unmarshal_raw(&mut self, src: &[u8]) -> Result<(), String> {
        self.reset();
        let mut src = src;
        while !src.is_empty() {
            let (tail, key) =
                unmarshal_bytes_fast(src).map_err(|err| format!("cannot decode key: {err}"))?;
            src = tail;

            let (tail, value) =
                unmarshal_bytes_fast(src).map_err(|err| format!("cannot decode value: {err}"))?;
            src = tail;

            if key.is_empty() {
                self.metric_group.clear();
                self.metric_group.extend_from_slice(value);
            } else {
                // Copy out of `src` before mutating self.
                let (key, value) = (key.to_vec(), value.to_vec());
                self.add_tag_bytes(&key, &value);
            }
        }
        Ok(())
    }

    /// Sorts tags in `self` to canonical form needed for storing in the index.
    ///
    /// sort_tags tries moving job-like tags to `tags[0]` and instance-like
    /// tags to `tags[1]` — see the `common_tag_key` sentinel list. This
    /// guarantees that indexdb entries for the same (job, instance) are
    /// located close to each other on disk, reducing disk seeks and read IO.
    ///
    /// The function also de-duplicates tags with identical keys; the last tag
    /// value for duplicate tags wins. Go: MetricName.sortTags.
    pub fn sort_tags(&mut self) {
        if self.tags.is_empty() {
            return;
        }

        let tags = std::mem::take(&mut self.tags);
        let mut cts: Vec<(Cow<'static, [u8]>, Tag)> = tags
            .into_iter()
            .map(|tag| {
                let key = match common_tag_key(&tag.key) {
                    Some(sentinel) => Cow::Borrowed(sentinel),
                    None => Cow::Owned(tag.key.clone()),
                };
                (key, tag)
            })
            .collect();

        // Stable sort in order to preserve the order of tags with duplicate
        // keys: the last tag value wins.
        cts.sort_by(|a, b| a.0.cmp(&b.0));

        let mut result: Vec<Tag> = Vec::with_capacity(cts.len());
        for (_, tag) in cts {
            match result.last_mut() {
                Some(last) if last.key == tag.key => {
                    // Overwrite the previous tag with duplicate key.
                    *last = tag;
                }
                _ => result.push(tag),
            }
        }
        self.tags = result;
    }
}

/// User-readable representation of the metric name (tags sorted).
/// Go: MetricName.String.
impl fmt::Display for MetricName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut mn_copy = MetricName::default();
        mn_copy.copy_from(self);
        mn_copy.sort_tags();
        let tags: Vec<String> = mn_copy
            .tags
            .iter()
            .map(|t| {
                format!(
                    "{}={:?}",
                    String::from_utf8_lossy(&t.key),
                    String::from_utf8_lossy(&t.value)
                )
            })
            .collect();
        write!(
            f,
            "{}{{{}}}",
            String::from_utf8_lossy(&mn_copy.metric_group),
            tags.join(",")
        )
    }
}

/// Marshals `labels` (name, value) pairs to `dst` in the MetricNameRaw wire
/// form: a sequence of `(u16be len | bytes)` pairs for key then value.
/// The `__name__` key is encoded as an empty key; labels with empty values
/// are skipped, since they have no sense in Prometheus.
///
/// The result must be unmarshaled with [`MetricName::unmarshal_raw`].
/// Go: MarshalMetricNameRaw.
pub fn marshal_metric_name_raw(dst: &mut Vec<u8>, labels: &[(&[u8], &[u8])]) {
    for &(name, value) in labels {
        if value.is_empty() {
            // Skip labels without values, since they have no sense in
            // Prometheus.
            continue;
        }
        let name = if name == METRIC_GROUP_TAG_KEY {
            b"" as &[u8]
        } else {
            name
        };
        marshal_bytes_fast(dst, name);
        marshal_bytes_fast(dst, value);
    }
}

/// Go: marshalBytesFast.
fn marshal_bytes_fast(dst: &mut Vec<u8>, s: &[u8]) {
    encoding::marshal_uint16(dst, s.len() as u16);
    dst.extend_from_slice(s);
}

/// Go: unmarshalBytesFast.
fn unmarshal_bytes_fast(src: &[u8]) -> Result<(&[u8], &[u8]), String> {
    if src.len() < 2 {
        return Err(format!(
            "cannot decode size from src={src:02X?}; it must be at least 2 bytes"
        ));
    }
    let n = encoding::unmarshal_uint16(src) as usize;
    let src = &src[2..];
    if src.len() < n {
        return Err(format!(
            "too short src={src:02X?}; it must be at least {n} bytes"
        ));
    }
    Ok((&src[n..], &src[..n]))
}

/// Returns the `commonTagKeys` sentinel for well-known job-like and
/// instance-like tag keys, or `None` for other keys.
///
/// Job-like tags must go first in `MetricName.tags` (sentinels start with
/// `\x00\x00`), instance-like tags second (`\x00\x01`). This improves data
/// locality. Do not change the sentinel values! Go: commonTagKeys.
fn common_tag_key(key: &[u8]) -> Option<&'static [u8]> {
    let sentinel: &'static [u8] = match key {
        // job-like tags.
        b"namespace" | b"Namespace" => b"\x00\x00\x00",
        b"ns" | b"Ns" => b"\x00\x00\x01",
        b"datacenter" | b"Datacenter" => b"\x00\x00\x08",
        b"dc" | b"Dc" => b"\x00\x00\x09",
        b"environment" | b"Environment" => b"\x00\x00\x0c",
        b"env" | b"Env" => b"\x00\x00\x0d",
        b"cluster" | b"Cluster" => b"\x00\x00\x10",
        b"service" | b"Service" => b"\x00\x00\x18",
        b"job" | b"Job" => b"\x00\x00\x20",
        b"model" | b"Model" => b"\x00\x00\x28",
        b"type" | b"Type" => b"\x00\x00\x30",
        b"sensor_type" | b"Sensor_type" | b"SensorType" => b"\x00\x00\x38",
        b"db" | b"Db" => b"\x00\x00\x40",

        // instance-like tags.
        b"instance" | b"Instance" => b"\x00\x01\x00",
        b"host" | b"Host" => b"\x00\x01\x08",
        b"server" | b"Server" => b"\x00\x01\x10",
        b"pod" | b"Pod" => b"\x00\x01\x18",
        b"node" | b"Node" => b"\x00\x01\x20",
        b"device" | b"Device" => b"\x00\x01\x28",
        b"tenant" | b"Tenant" => b"\x00\x01\x30",
        b"client" | b"Client" => b"\x00\x01\x38",
        b"name" | b"Name" => b"\x00\x01\x40",
        b"measurement" | b"Measurement" => b"\x00\x01\x48",

        _ => return None,
    };
    Some(sentinel)
}

/// Go: normalizeTagKey.
#[allow(dead_code)] // used by index building in later stages
pub(crate) fn normalize_tag_key(key: &[u8]) -> &[u8] {
    common_tag_key(key).unwrap_or(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mn_with_group(group: &[u8]) -> MetricName {
        MetricName {
            metric_group: group.to_vec(),
            ..Default::default()
        }
    }

    // Port of TestMetricNameString.
    #[test]
    fn metric_name_string() {
        let mn = MetricName {
            metric_group: b"foobar".to_vec(),
            ..Default::default()
        };
        assert_eq!(mn.to_string(), "foobar{}");

        let mn = MetricName {
            metric_group: b"abc".to_vec(),
            tags: vec![
                Tag {
                    key: b"foo".to_vec(),
                    value: b"bar".to_vec(),
                },
                Tag {
                    key: b"baz".to_vec(),
                    value: b"123".to_vec(),
                },
            ],
        };
        assert_eq!(mn.to_string(), r#"abc{baz="123",foo="bar"}"#);
    }

    // Port of TestMetricNameSortTags.
    #[test]
    fn metric_name_sort_tags() {
        fn check(tags: &[&str], expected_tags: &[&str]) {
            let mut mn = MetricName::default();
            for t in tags {
                mn.add_tag(t, "");
            }
            mn.sort_tags();
            let result_tags: Vec<String> = mn
                .tags
                .iter()
                .map(|t| String::from_utf8_lossy(&t.key).into_owned())
                .collect();
            assert_eq!(result_tags, expected_tags, "input: {tags:?}");
        }

        check(&[], &[]);
        check(&["foo"], &["foo"]);
        check(&["job"], &["job"]);
        check(&["server"], &["server"]);
        check(
            &["host", "foo", "bar", "service"],
            &["service", "host", "bar", "foo"],
        );
        check(
            &["model", "foo", "job", "host", "server", "instance"],
            &["job", "model", "instance", "host", "server", "foo"],
        );
    }

    // Port of TestMetricNameMarshalDuplicateKeys.
    #[test]
    fn metric_name_marshal_duplicate_keys() {
        let mut mn = mn_with_group(b"xxx");
        mn.add_tag("foo", "bar");
        mn.add_tag("duplicate", "tag1");
        mn.add_tag("duplicate", "tag2");
        mn.add_tag("tt", "xx");
        mn.add_tag("foo", "abc");
        mn.add_tag("duplicate", "tag3");

        let mut mn_expected = mn_with_group(b"xxx");
        mn_expected.add_tag("duplicate", "tag3");
        mn_expected.add_tag("foo", "abc");
        mn_expected.add_tag("tt", "xx");

        mn.sort_tags();
        let mut data = Vec::new();
        mn.marshal(&mut data);
        let mut mn1 = MetricName::default();
        mn1.unmarshal(&data).expect("cannot unmarshal mn");
        assert_eq!(mn_expected, mn1);
    }

    // Port of TestMetricNameMarshalUnmarshal.
    #[test]
    fn metric_name_marshal_unmarshal() {
        for i in 0..10 {
            for tags_count in 0..10 {
                let mut mn = MetricName::default();
                for j in 0..tags_count {
                    let key = format!("key_{i}_{j}_\x00\x01\x02");
                    let value = format!("\x02\x00\x01value_{i}_{j}");
                    mn.add_tag(&key, &value);
                }
                mn.sort_tags();
                let mut data = Vec::new();
                mn.marshal(&mut data);
                let mut mn1 = MetricName::default();
                mn1.unmarshal(&data).expect("cannot unmarshal mn");
                assert_eq!(mn, mn1);

                // Try unmarshaling MetricName without tag value.
                let mut broken_data = data.clone();
                marshal_tag_value(&mut broken_data, b"foobar");
                assert!(
                    mn1.unmarshal(&broken_data).is_err(),
                    "expecting error when unmarshaling MetricName without tag value"
                );

                // Try unmarshaling MetricName with invalid tag key.
                let last = broken_data.len() - 1;
                broken_data[last] = 123;
                assert!(
                    mn1.unmarshal(&broken_data).is_err(),
                    "expecting error when unmarshaling MetricName with invalid tag key"
                );

                // Try unmarshaling MetricName with invalid tag value.
                let mut broken_data = data.clone();
                marshal_tag_value(&mut broken_data, b"foobar");
                marshal_tag_value(&mut broken_data, b"aaa");
                let last = broken_data.len() - 1;
                broken_data[last] = 123;
                assert!(
                    mn1.unmarshal(&broken_data).is_err(),
                    "expecting error when unmarshaling MetricName with invalid tag value"
                );
            }
        }
    }

    // Port of TestMetricNameMarshalUnmarshalRaw.
    #[test]
    fn metric_name_marshal_unmarshal_raw() {
        for i in 0..10 {
            for tags_count in 0..10 {
                let mut mn = MetricName::default();
                for j in 0..tags_count {
                    let key = format!("key_{i}_{j}_\x00\x01\x02");
                    let value = format!("\x02\x00\x01value_{i}_{j}");
                    mn.add_tag(&key, &value);
                }
                let mut data = Vec::new();
                mn.marshal_raw(&mut data);
                let mut mn1 = MetricName::default();
                mn1.unmarshal_raw(&data).expect("cannot unmarshal mn");
                assert_eq!(mn, mn1);

                // Try unmarshaling MetricName without tag value.
                let mut broken_data = data.clone();
                marshal_tag_value(&mut broken_data, b"foobar");
                assert!(
                    mn1.unmarshal_raw(&broken_data).is_err(),
                    "expecting error when unmarshaling MetricName without tag value"
                );

                // Try unmarshaling MetricName with invalid tag key.
                let last = broken_data.len() - 1;
                broken_data[last] = 123;
                assert!(
                    mn1.unmarshal_raw(&broken_data).is_err(),
                    "expecting error when unmarshaling MetricName with invalid tag key"
                );

                // Try unmarshaling MetricName with invalid tag value.
                let mut broken_data = data.clone();
                marshal_tag_value(&mut broken_data, b"foobar");
                marshal_tag_value(&mut broken_data, b"aaa");
                let last = broken_data.len() - 1;
                broken_data[last] = 123;
                assert!(
                    mn1.unmarshal_raw(&broken_data).is_err(),
                    "expecting error when unmarshaling MetricName with invalid tag value"
                );
            }
        }
    }

    // Port of TestMetricNameCopyFrom.
    #[test]
    fn metric_name_copy_from() {
        let mut from = mn_with_group(b"group");
        from.add_tag("key", "value");

        let mut to = MetricName::default();
        to.copy_from(&from);

        let mut expected = mn_with_group(b"group");
        expected.add_tag("key", "value");

        assert_eq!(expected, to);
    }

    // Port of TestMetricNameRemoveTagsOn.
    #[test]
    fn metric_name_remove_tags_on() {
        let mut empty_mn = mn_with_group(b"name");
        empty_mn.add_tag("key", "value");
        empty_mn.remove_tags_on(&[]);
        assert!(
            empty_mn.metric_group.is_empty() && empty_mn.tags.is_empty(),
            "expecting empty metric name got {empty_mn}"
        );

        let mut as_is_mn = mn_with_group(b"name");
        as_is_mn.add_tag("key", "value");
        as_is_mn.remove_tags_on(&["__name__", "key"]);
        let mut exp_as_is_mn = mn_with_group(b"name");
        exp_as_is_mn.add_tag("key", "value");
        assert_eq!(exp_as_is_mn, as_is_mn);

        let mut mn = mn_with_group(b"name");
        mn.add_tag("foo", "bar");
        mn.add_tag("baz", "qux");
        mn.remove_tags_on(&["baz"]);
        let mut exp_mn = MetricName::default();
        exp_mn.add_tag("baz", "qux");
        assert_eq!(exp_mn.tags, mn.tags);
        assert_eq!(exp_mn.metric_group.len(), mn.metric_group.len());
    }

    // Port of TestMetricNameRemoveTag.
    #[test]
    fn metric_name_remove_tag() {
        let mut mn = mn_with_group(b"name");
        mn.add_tag("foo", "bar");
        mn.add_tag("baz", "qux");
        mn.remove_tag("__name__");
        assert!(
            mn.metric_group.is_empty(),
            "expecting empty metric group got {mn}"
        );
        mn.remove_tag("foo");
        let mut exp_mn = MetricName::default();
        exp_mn.add_tag("baz", "qux");
        assert_eq!(exp_mn.tags, mn.tags);
        assert_eq!(exp_mn.metric_group.len(), mn.metric_group.len());
    }

    // Port of TestMetricNameRemoveTagsIgnoring.
    #[test]
    fn metric_name_remove_tags_ignoring() {
        let mut mn = mn_with_group(b"name");
        mn.add_tag("foo", "bar");
        mn.add_tag("baz", "qux");
        mn.remove_tags_ignoring(&["__name__", "foo"]);
        let mut exp_mn = MetricName::default();
        exp_mn.add_tag("baz", "qux");
        assert_eq!(exp_mn.tags, mn.tags);
        assert_eq!(exp_mn.metric_group.len(), mn.metric_group.len());
    }

    // Golden byte layout for the escaped canonical form.
    #[test]
    fn marshal_golden_bytes() {
        // Escaping: 0x00 -> 0x00 '0', 0x01 -> 0x00 '1', 0x02 -> 0x00 '2',
        // trailing tag separator 0x01.
        let mut dst = Vec::new();
        marshal_tag_value(&mut dst, b"a\x00\x01\x02");
        assert_eq!(dst, b"a\x000\x001\x002\x01");

        let mut roundtrip = Vec::new();
        let tail = unmarshal_tag_value(&mut roundtrip, &dst).unwrap();
        assert!(tail.is_empty());
        assert_eq!(roundtrip, b"a\x00\x01\x02");

        // Canonical form: group, then (key, value) per tag, all escaped,
        // each terminated with 0x01. Sentinels affect sort order only —
        // the original keys are what gets marshaled.
        let mut mn = mn_with_group(b"name");
        mn.add_tag("zzz", "z");
        mn.add_tag("instance", "i");
        mn.add_tag("job", "j");
        let mut data = Vec::new();
        mn.marshal_canonical(&mut data);
        assert_eq!(data, b"name\x01job\x01j\x01instance\x01i\x01zzz\x01z\x01");
    }

    // Golden byte layout for the raw (u16be length-prefixed) form.
    #[test]
    fn marshal_metric_name_raw_golden_bytes() {
        let mut dst = Vec::new();
        marshal_metric_name_raw(
            &mut dst,
            &[
                (b"__name__", b"m"),
                (b"skipped", b""), // empty value is skipped
                (b"k", b"v"),
            ],
        );
        assert_eq!(
            dst, b"\x00\x00\x00\x01m\x00\x01k\x00\x01v",
            "unexpected raw encoding: {dst:02X?}"
        );

        let mut mn = MetricName::default();
        mn.unmarshal_raw(&dst).unwrap();
        assert_eq!(mn.metric_group, b"m");
        assert_eq!(mn.tags.len(), 1);
        assert_eq!(mn.tags[0].key, b"k");
        assert_eq!(mn.tags[0].value, b"v");
    }
}
