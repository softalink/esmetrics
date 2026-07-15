//! Port of `tag_filters.go`: TagFilters/tagFilter, regexp simplification,
//! or-values fast path, match costs and composite-filter conversion.
//!
//! PORT-SKIP (per spec §8): Graphite query filters (`InitFromGraphiteQuery`,
//! `__graphite__`, reverse-suffix filters). The `graphiteReverseTagKey`
//! constant is kept because artificial-key checks depend on it.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use esm_common::regexutil;

use crate::metric_name::{marshal_tag_value, TAG_SEPARATOR_CHAR};

use super::caches::{EntrySize, WorkingSetCache};
use super::{
    marshal_common_prefix, marshal_composite_tag_key, unmarshal_composite_tag_key,
    COMPOSITE_TAG_KEY_PREFIX, GRAPHITE_REVERSE_TAG_KEY, NS_PREFIX_TAG_TO_METRIC_IDS,
};

// These cost values are used for sorting tag filters in ascending order of
// the required CPU time for execution. Go: tag_filters.go match costs.
pub const FULL_MATCH_COST: u64 = 1;
pub const PREFIX_MATCH_COST: u64 = 2;
pub const LITERAL_MATCH_COST: u64 = 3;
pub const SUFFIX_MATCH_COST: u64 = 4;
pub const MIDDLE_MATCH_COST: u64 = 6;
pub const RE_MATCH_COST: u64 = 100;

/// The number of successful conversions to composite filters.
pub static COMPOSITE_FILTER_SUCCESS_CONVERSIONS: AtomicU64 = AtomicU64::new(0);
/// The number of failed conversions to composite filters.
pub static COMPOSITE_FILTER_MISSING_CONVERSIONS: AtomicU64 = AtomicU64::new(0);

type ReMatchFn = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// A single tag filter. Go: tagFilter.
#[derive(Clone, Default)]
pub struct TagFilter {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) is_negative: bool,
    pub(crate) is_regexp: bool,

    /// The cost for matching the filter against a single string.
    pub(crate) match_cost: u64,

    /// The literal prefix for the regexp filter if is_regexp is true.
    pub(crate) regexp_prefix: String,

    /// Contains either {nsPrefixTagToMetricIDs, key} or
    /// {nsPrefixDateTagToMetricIDs, date, key}, plus the escaped literal
    /// value prefix without the trailing tagSeparatorChar.
    pub(crate) prefix: Vec<u8>,

    /// `or` values obtained from the regexp suffix if it equals
    /// "foo|bar|..."; contains a single empty string for non-regexp filters.
    pub(crate) or_suffixes: Vec<String>,

    /// Matches the regexp suffix (set only when is_regexp is true).
    pub(crate) re_suffix_match: Option<ReMatchFn>,

    /// Set to true for filters matching an empty value.
    pub(crate) is_empty_match: bool,
}

impl TagFilter {
    /// The filter key (empty for `__name__`).
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// The filter value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Whether the filter is negative (`!=` / `!~`).
    pub fn is_negative(&self) -> bool {
        self.is_negative
    }

    /// Whether the filter is a regexp filter (`=~` / `!~`).
    pub fn is_regexp(&self) -> bool {
        self.is_regexp
    }

    /// Whether the filter matches an empty value.
    pub fn is_empty_match(&self) -> bool {
        self.is_empty_match
    }

    /// The index seek prefix for the filter.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// The `or` suffixes for the fast seek path.
    pub fn or_suffixes(&self) -> &[String] {
        &self.or_suffixes
    }

    /// The estimated cost of matching this filter against a single string.
    pub fn match_cost(&self) -> u64 {
        self.match_cost
    }

    /// Returns true for composite filters. Go: tagFilter.isComposite.
    pub fn is_composite(&self) -> bool {
        !self.key.is_empty() && self.key[0] == COMPOSITE_TAG_KEY_PREFIX
    }

    /// Ordering used by the tag-filter planner. Go: tagFilter.Less.
    pub fn less(&self, other: &TagFilter) -> bool {
        // Move composite filters to the top, since they usually match a
        // lower number of time series. Move regexp filters to the bottom,
        // since they require scanning all the entries for the given label.
        let is_composite_a = self.is_composite();
        let is_composite_b = other.is_composite();
        if is_composite_a != is_composite_b {
            return is_composite_a;
        }
        if self.match_cost != other.match_cost {
            return self.match_cost < other.match_cost;
        }
        if self.is_regexp != other.is_regexp {
            return !self.is_regexp;
        }
        if self.or_suffixes.len() != other.or_suffixes.len() {
            return self.or_suffixes.len() < other.or_suffixes.len();
        }
        if self.is_negative != other.is_negative {
            return !self.is_negative;
        }
        self.prefix < other.prefix
    }

    fn get_op(&self) -> &'static str {
        match (self.is_negative, self.is_regexp) {
            (true, true) => "!~",
            (true, false) => "!=",
            (false, true) => "=~",
            (false, false) => "=",
        }
    }

    /// Appends the marshaled filter to `dst` (used in cache keys).
    /// Go: tagFilter.Marshal.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_tag_value(dst, &self.key);
        marshal_tag_value(dst, &self.value);
        dst.push(self.is_negative as u8);
        dst.push(self.is_regexp as u8);
    }

    /// Initializes the tag filter for the given common_prefix, key and value.
    ///
    /// `common_prefix` must contain either {nsPrefixTagToMetricIDs} or
    /// {nsPrefixDateTagToMetricIDs, date}.
    ///
    /// If `is_regexp` is true, the value is interpreted as an anchored
    /// regexp, i.e. `^(value)$`. MetricGroup must be encoded in the value
    /// with an empty key. Go: tagFilter.Init.
    pub fn init(
        &mut self,
        common_prefix: &[u8],
        key: &[u8],
        value: &[u8],
        is_negative: bool,
        is_regexp: bool,
    ) -> Result<(), String> {
        self.key.clear();
        self.key.extend_from_slice(key);
        self.value.clear();
        self.value.extend_from_slice(value);
        self.is_negative = is_negative;
        self.is_regexp = is_regexp;
        self.match_cost = 0;
        self.regexp_prefix.clear();
        self.prefix.clear();
        self.or_suffixes.clear();
        self.re_suffix_match = None;
        self.is_empty_match = false;

        self.prefix.extend_from_slice(common_prefix);
        marshal_tag_value(&mut self.prefix, key);

        let mut prefix: Vec<u8> = self.value.clone();
        let mut expr = String::new();
        if self.is_regexp {
            let value_str = std::str::from_utf8(&self.value)
                .map_err(|_| format!("invalid regexp {:?}: not valid UTF-8", self.value))?;
            let (p, e) = simplify_regexp(value_str);
            if e.is_empty() {
                self.value.clear();
                self.value.extend_from_slice(p.as_bytes());
                self.is_regexp = false;
            } else {
                self.regexp_prefix = p.clone();
                expr = e;
            }
            prefix = p.into_bytes();
        }
        marshal_tag_value_no_trailing_tag_separator(&mut self.prefix, &prefix);
        if !self.is_regexp {
            // The filter contains a plain value without a regexp.
            // Add an empty or_suffix in order to trigger the fast path for
            // or_suffixes during the search for matching metricIDs.
            self.or_suffixes.push(String::new());
            self.is_empty_match = prefix.is_empty();
            self.match_cost = FULL_MATCH_COST;
            return Ok(());
        }
        let rcv = get_regexp_from_cache(&expr)?;
        self.or_suffixes.extend_from_slice(&rcv.or_values);
        self.re_suffix_match = Some(Arc::clone(&rcv.re_match));
        self.match_cost = rcv.re_cost;
        self.is_empty_match = prefix.is_empty() && (rcv.re_match)(b"");
        // PORT-SKIP: graphiteReverseSuffix for dotted __name__ regexps.
        Ok(())
    }

    /// Matches the full marshaled `{prefix, value, tagSeparatorChar}` item
    /// against the filter, honoring is_negative. Go: tagFilter.match.
    pub fn matches(&self, b: &[u8]) -> Result<bool, String> {
        if !b.starts_with(&self.prefix) {
            return Ok(self.is_negative);
        }
        let ok = self.match_suffix(&b[self.prefix.len()..])?;
        if !ok {
            return Ok(self.is_negative);
        }
        Ok(!self.is_negative)
    }

    /// Matches the tag-value suffix (which must end with tagSeparatorChar)
    /// against the filter, ignoring is_negative. Go: tagFilter.matchSuffix.
    pub fn match_suffix(&self, b: &[u8]) -> Result<bool, String> {
        // Remove the trailing tagSeparatorChar.
        if b.is_empty() || b[b.len() - 1] != TAG_SEPARATOR_CHAR {
            return Err(format!(
                "unexpected end of b; want {TAG_SEPARATOR_CHAR}; b={b:?}"
            ));
        }
        let b = &b[..b.len() - 1];
        if !self.is_regexp {
            return Ok(b.is_empty());
        }
        let re_match = self
            .re_suffix_match
            .as_ref()
            .expect("BUG: re_suffix_match must be set for regexp filters");
        Ok(re_match(b))
    }
}

impl std::fmt::Display for TagFilter {
    /// Go: tagFilter.String.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = self.get_op();
        let value = String::from_utf8_lossy(&self.value);
        let value: String = value.chars().take(60).collect();
        if self.key == GRAPHITE_REVERSE_TAG_KEY {
            return write!(f, "__graphite_reverse__{op}{value:?}");
        }
        if self.is_composite() {
            let (name, key) = unmarshal_composite_tag_key(&self.key)
                .expect("BUG: cannot unmarshal composite tag key");
            return write!(
                f,
                "composite({},{}){op}{value:?}",
                String::from_utf8_lossy(name),
                String::from_utf8_lossy(key)
            );
        }
        if self.key.is_empty() {
            return write!(f, "__name__{op}{value:?}");
        }
        write!(f, "{}{op}{value:?}", String::from_utf8_lossy(&self.key))
    }
}

impl std::fmt::Debug for TagFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// A set of tag filters (ANDed together). Go: TagFilters.
#[derive(Clone, Default)]
pub struct TagFilters {
    pub(crate) tfs: Vec<TagFilter>,

    /// Common prefix for all the tag filters.
    /// Contains the encoded nsPrefixTagToMetricIDs.
    pub(crate) common_prefix: Vec<u8>,
}

impl TagFilters {
    /// Returns new TagFilters. Go: NewTagFilters.
    pub fn new() -> TagFilters {
        let mut common_prefix = Vec::with_capacity(1);
        marshal_common_prefix(&mut common_prefix, NS_PREFIX_TAG_TO_METRIC_IDS);
        TagFilters {
            tfs: Vec::new(),
            common_prefix,
        }
    }

    /// The filters in the set.
    pub fn filters(&self) -> &[TagFilter] {
        &self.tfs
    }

    /// Adds the given tag filter to the set.
    ///
    /// MetricGroup must be encoded with an empty key. Go: TagFilters.Add.
    pub fn add(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_negative: bool,
        is_regexp: bool,
    ) -> Result<(), String> {
        let mut is_negative = is_negative;
        let mut is_regexp = is_regexp;
        let mut value = value;

        // Verify whether the tag filter is empty.
        if value.is_empty() {
            // Substitute an empty tag value with the negative match of the
            // `.+` regexp in order to filter out all the values with the
            // given tag.
            is_negative = !is_negative;
            is_regexp = true;
            value = b".+";
        }
        if is_regexp && value == b".*" {
            if !is_negative {
                // Skip a tag filter matching anything, since it equals to no
                // filter.
                return Ok(());
            }
            // Substitute a negative tag filter matching anything with a
            // negative tag filter matching a non-empty value in order to
            // filter out all the time series with the given key.
            value = b".+";
        }

        let mut tf = TagFilter::default();
        tf.init(&self.common_prefix, key, value, is_negative, is_regexp)
            .map_err(|err| format!("cannot initialize tagFilter: {err}"))?;
        let needs_non_empty_companion = tf.is_negative && tf.is_empty_match;
        self.tfs.push(tf);
        if needs_non_empty_companion {
            // We have a `{key!~"|foo"}` tag filter, which matches non-empty
            // key values. So add a `{key=~".+"}` tag filter in order to
            // enforce this.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/546
            let mut tf_new = TagFilter::default();
            tf_new
                .init(&self.common_prefix, key, b".+", false, true)
                .map_err(|err| {
                    format!(
                        "cannot initialize {{{}=\".+\"}} tag filter: {err}",
                        String::from_utf8_lossy(key)
                    )
                })?;
            self.tfs.push(tf_new);
        }
        // PORT-SKIP: graphiteReverseSuffix companion filter.
        Ok(())
    }

    /// Resets the filter set. Go: TagFilters.Reset.
    pub fn reset(&mut self) {
        self.tfs.clear();
        self.common_prefix.clear();
        marshal_common_prefix(&mut self.common_prefix, NS_PREFIX_TAG_TO_METRIC_IDS);
    }
}

impl std::fmt::Display for TagFilters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a: Vec<String> = self.tfs.iter().map(|tf| tf.to_string()).collect();
        write!(f, "{{{}}}", a.join(","))
    }
}

impl std::fmt::Debug for TagFilters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// Appends `src` to `dst` in the marshaled tag-value form minus the trailing
/// tagSeparatorChar. Go: marshalTagValueNoTrailingTagSeparator.
pub(crate) fn marshal_tag_value_no_trailing_tag_separator(dst: &mut Vec<u8>, src: &[u8]) {
    marshal_tag_value(dst, src);
    dst.pop(); // Remove the trailing tagSeparatorChar.
}

// --- composite filters ---

/// Returns the common plain metric-name filter value across all the groups.
/// Used by the stage-5 label-name/label-value search paths.
/// Go: getCommonMetricNameForTagFilterss.
pub fn get_common_metric_name_for_tag_filterss(tfss: &[TagFilters]) -> Vec<u8> {
    let Some(first) = tfss.first() else {
        return Vec::new();
    };
    let prev_name = get_metric_name_filter(first);
    for tfs in &tfss[1..] {
        if get_metric_name_filter(tfs) != prev_name {
            return Vec::new();
        }
    }
    prev_name.map(<[u8]>::to_vec).unwrap_or_default()
}

fn get_metric_name_filter(tfs: &TagFilters) -> Option<&[u8]> {
    tfs.tfs
        .iter()
        .find(|tf| tf.key.is_empty() && !tf.is_negative && !tf.is_regexp)
        .map(|tf| tf.value.as_slice())
}

/// Converts `tfss` to composite filters.
///
/// This converts `foo{bar="baz",x=~"a.+"}` to
/// `{composite(foo,bar)="baz",composite(foo,x)=~"a.+"}`.
/// Go: convertToCompositeTagFilterss.
pub fn convert_to_composite_tag_filterss(tfss: &[TagFilters]) -> Vec<TagFilters> {
    let mut tfss_new = Vec::with_capacity(tfss.len());
    for tfs in tfss {
        tfss_new.extend(convert_to_composite_tag_filters(tfs));
    }
    tfss_new
}

fn convert_to_composite_tag_filters(tfs: &TagFilters) -> Vec<TagFilters> {
    use std::sync::atomic::Ordering;

    // Search for filters on the metric name, which will be used for creating
    // composite filters.
    let mut names: Vec<Vec<u8>> = Vec::new();
    let mut name_prefix = String::new();
    let mut has_positive_filter = false;
    for tf in &tfs.tfs {
        if tf.key.is_empty() {
            if !tf.is_negative && !tf.is_regexp {
                names = vec![tf.value.clone()];
            } else if !tf.is_negative && tf.is_regexp && !tf.or_suffixes.is_empty() {
                // Split the filter {__name__=~"name1|...|nameN", other}
                // into name1{other}, ..., nameN{other} and generate composite
                // filters for each of them.
                names.clear();
                for or_suffix in &tf.or_suffixes {
                    names.push(or_suffix.as_bytes().to_vec());
                }
                name_prefix = tf.regexp_prefix.clone();
            }
        } else if !tf.is_negative && !tf.is_empty_match {
            has_positive_filter = true;
        }
    }
    // If tfs have no filters on __name__ or no non-negative filters, then it
    // is impossible to construct a composite tag filter.
    if names.is_empty() || !has_positive_filter {
        COMPOSITE_FILTER_MISSING_CONVERSIONS.fetch_add(1, Ordering::Relaxed);
        return vec![tfs.clone()];
    }

    // Create composite filters for the found names.
    let mut tfss_compiled: Vec<TagFilters> = Vec::with_capacity(names.len());
    let mut composite_key: Vec<u8> = Vec::new();
    let mut name_with_prefix: Vec<u8> = Vec::new();
    for name in &names {
        let mut composite_filters = 0;
        let mut tfs_new: Vec<TagFilter> = Vec::with_capacity(tfs.tfs.len());
        for tf in &tfs.tfs {
            if tf.key.is_empty() {
                if tf.is_negative {
                    // Negative filters on the metric name cannot be used for
                    // building composite filters, so leave them as is.
                    tfs_new.push(tf.clone());
                    continue;
                }
                if tf.is_regexp {
                    if !tf.or_suffixes.iter().any(|s| s.as_bytes() == name) {
                        // Leave as is the regexp filter on the metric name if
                        // it doesn't match the current name.
                        tfs_new.push(tf.clone());
                    }
                    // Otherwise skip the tf, since its part (name) is used as
                    // a prefix in the composite filter.
                    continue;
                }
                if tf.value != *name {
                    // Leave as is the filter on another metric name.
                    tfs_new.push(tf.clone());
                }
                // Otherwise skip the tf, since it is used as a prefix in the
                // composite filter.
                continue;
            }
            if tf.key == b"__graphite__" || tf.key == GRAPHITE_REVERSE_TAG_KEY {
                // Leave as is __graphite__ filters, since they cannot be
                // used for building composite filters.
                tfs_new.push(tf.clone());
                continue;
            }
            // Create a composite filter on (name, tf).
            name_with_prefix.clear();
            name_with_prefix.extend_from_slice(name_prefix.as_bytes());
            name_with_prefix.extend_from_slice(name);
            composite_key.clear();
            marshal_composite_tag_key(&mut composite_key, &name_with_prefix, &tf.key);
            let mut tf_new = TagFilter::default();
            tf_new
                .init(
                    &tfs.common_prefix,
                    &composite_key,
                    &tf.value,
                    tf.is_negative,
                    tf.is_regexp,
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "BUG: unexpected error when creating composite tag filter for name={name:?} and key={:?}: {err}",
                        tf.key
                    )
                });
            tfs_new.push(tf_new);
            composite_filters += 1;
        }
        if composite_filters == 0 {
            // Cannot use tfs_new, since it doesn't contain composite filters,
            // e.g. it may match a broader set of series. Fall back to the
            // original tfs.
            COMPOSITE_FILTER_MISSING_CONVERSIONS.fetch_add(1, Ordering::Relaxed);
            return vec![tfs.clone()];
        }
        let mut tfs_compiled = TagFilters::new();
        tfs_compiled.tfs = tfs_new;
        tfss_compiled.push(tfs_compiled);
    }
    COMPOSITE_FILTER_SUCCESS_CONVERSIONS.fetch_add(1, Ordering::Relaxed);
    tfss_compiled
}

// --- regexp handling ---

struct RegexpCacheValue {
    or_values: Vec<String>,
    re_match: ReMatchFn,
    re_cost: u64,
    size_bytes: u64,
}

impl EntrySize for Arc<RegexpCacheValue> {
    fn entry_size(&self) -> u64 {
        self.size_bytes
    }
}

impl EntrySize for Arc<(String, String)> {
    fn entry_size(&self) -> u64 {
        (self.0.len() + self.1.len() + 16) as u64
    }
}

fn regexp_cache() -> &'static WorkingSetCache<Arc<RegexpCacheValue>> {
    static CACHE: OnceLock<WorkingSetCache<Arc<RegexpCacheValue>>> = OnceLock::new();
    CACHE.get_or_init(|| WorkingSetCache::new((0.05 * esm_common::memory::allowed() as f64) as u64))
}

fn prefixes_cache() -> &'static WorkingSetCache<Arc<(String, String)>> {
    static CACHE: OnceLock<WorkingSetCache<Arc<(String, String)>>> = OnceLock::new();
    CACHE.get_or_init(|| WorkingSetCache::new((0.05 * esm_common::memory::allowed() as f64) as u64))
}

/// Returns (literal prefix, remaining expression) for the given anchored
/// regexp, memoized in a process-wide cache. Go: simplifyRegexp.
pub(crate) fn simplify_regexp(expr: &str) -> (String, String) {
    if let Some(ps) = prefixes_cache().get(expr.as_bytes()) {
        // Fast path - the simplified expr is found in the cache.
        return (ps.0.clone(), ps.1.clone());
    }
    // Slow path - simplify the expr.
    let (prefix, suffix) = regexutil::simplify_prom_regex(expr);
    prefixes_cache().set(expr.as_bytes(), Arc::new((prefix.clone(), suffix.clone())));
    (prefix, suffix)
}

fn get_regexp_from_cache(expr: &str) -> Result<Arc<RegexpCacheValue>, String> {
    if let Some(rcv) = regexp_cache().get(expr.as_bytes()) {
        // Fast path - the regexp is found in the cache.
        return Ok(rcv);
    }
    // Slow path - build the regexp.
    let escaped = tag_chars_regexp_escape(expr);
    let expr_anchored = format!("^(?:{escaped})$");
    let re = regex::bytes::Regex::new(&expr_anchored)
        .map_err(|err| format!("invalid regexp {expr_anchored:?}: {err}"))?;

    let or_values = regexutil::get_or_values_prom_regex(&escaped);
    let (re_match, re_cost): (ReMatchFn, u64) = if !or_values.is_empty() {
        new_match_func_for_or_suffixes(or_values.clone())
    } else {
        get_optimized_re_match_func(re, &escaped)
    };

    // Put the compiled matcher in the cache.
    let size_bytes = 8 * expr.len() as u64;
    let rcv = Arc::new(RegexpCacheValue {
        or_values,
        re_match,
        re_cost,
        size_bytes,
    });
    regexp_cache().set(expr.as_bytes(), Arc::clone(&rcv));
    Ok(rcv)
}

fn new_match_func_for_or_suffixes(or_values: Vec<String>) -> (ReMatchFn, u64) {
    let re_cost = or_values.len() as u64 * LITERAL_MATCH_COST;
    let re_match: ReMatchFn = if or_values.len() == 1 {
        let v = or_values.into_iter().next().expect("one value");
        Arc::new(move |b: &[u8]| b == v.as_bytes())
    } else {
        Arc::new(move |b: &[u8]| or_values.iter().any(|v| b == v.as_bytes()))
    };
    (re_match, re_cost)
}

/// Escapes the special index chars (0x00/0x01/0x02 and their `\xNN` escape
/// sequences) in the regexp the same way `marshalTagValue` escapes values.
/// Go: tagCharsRegexpEscaper.
fn tag_chars_regexp_escape(expr: &str) -> String {
    let src = expr.as_bytes();
    let mut dst: Vec<u8> = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < src.len() {
        let rest = &src[i..];
        if rest.starts_with(br"\x00") {
            dst.extend_from_slice(br"\x000");
            i += 4;
        } else if rest.starts_with(br"\x01") {
            dst.extend_from_slice(br"\x001");
            i += 4;
        } else if rest.starts_with(br"\x02") {
            dst.extend_from_slice(br"\x002");
            i += 4;
        } else {
            match rest[0] {
                0x00 => dst.extend_from_slice(br"\x000"),
                0x01 => dst.extend_from_slice(br"\x001"),
                0x02 => dst.extend_from_slice(br"\x002"),
                b => dst.push(b),
            }
            i += 1;
        }
    }
    String::from_utf8(dst).expect("BUG: escaping must preserve UTF-8 validity")
}

/// Returns an optimized match function for the given expr along with its
/// match cost. Go: getOptimizedReMatchFunc.
///
/// Optimized cases (analyzed on the simplified suffix expression):
///
/// - `.*`, `.+`
/// - `literal.*`, `literal.+`
/// - `.*literal`, `.+literal`
/// - `.*literal.*`, `.*literal.+`, `.+literal.*`, `.+literal.+`
///
/// Deviation from Go: the analysis is string-shape based instead of walking
/// the `regexp/syntax` tree, so some shapes Go optimizes (e.g. `(a|b)?foo`)
/// fall back to the compiled regexp here. The fallback matches identically —
/// only the match-cost heuristic differs. `literalSuffix` (Graphite-only) is
/// not computed.
fn get_optimized_re_match_func(re: regex::bytes::Regex, expr: &str) -> (ReMatchFn, u64) {
    #[derive(Clone, Copy, PartialEq)]
    enum Dot {
        None,
        Star,
        Plus,
    }

    let (head, rest) = if let Some(rest) = expr.strip_prefix(".*") {
        (Dot::Star, rest)
    } else if let Some(rest) = expr.strip_prefix(".+") {
        (Dot::Plus, rest)
    } else {
        (Dot::None, expr)
    };
    let (tail, middle) = if let Some(middle) = rest.strip_suffix(".*") {
        (Dot::Star, middle)
    } else if let Some(middle) = rest.strip_suffix(".+") {
        (Dot::Plus, middle)
    } else {
        (Dot::None, rest)
    };

    if middle.is_empty() && tail == Dot::None {
        match head {
            // '.*'
            Dot::Star => return (Arc::new(|_| true), FULL_MATCH_COST),
            // '.+'
            Dot::Plus => return (Arc::new(|b: &[u8]| !b.is_empty()), FULL_MATCH_COST),
            Dot::None => {}
        }
    }

    if let Some(lit) = parse_regexp_literal(middle) {
        if !lit.is_empty() {
            match (head, tail) {
                (Dot::None, Dot::None) => {
                    // 'literal'
                    return (Arc::new(move |b: &[u8]| b == lit), LITERAL_MATCH_COST);
                }
                (Dot::None, Dot::Star) => {
                    // 'literal.*'
                    return (
                        Arc::new(move |b: &[u8]| b.starts_with(&lit)),
                        PREFIX_MATCH_COST,
                    );
                }
                (Dot::None, Dot::Plus) => {
                    // 'literal.+'
                    return (
                        Arc::new(move |b: &[u8]| b.len() > lit.len() && b.starts_with(&lit)),
                        PREFIX_MATCH_COST,
                    );
                }
                (Dot::Star, Dot::None) => {
                    // '.*literal'
                    return (
                        Arc::new(move |b: &[u8]| b.ends_with(&lit)),
                        SUFFIX_MATCH_COST,
                    );
                }
                (Dot::Plus, Dot::None) => {
                    // '.+literal'
                    return (
                        Arc::new(move |b: &[u8]| b.len() > lit.len() && b[1..].ends_with(&lit)),
                        SUFFIX_MATCH_COST,
                    );
                }
                (Dot::Star, Dot::Star) => {
                    // '.*literal.*'
                    return (
                        Arc::new(move |b: &[u8]| contains(b, &lit)),
                        MIDDLE_MATCH_COST,
                    );
                }
                (Dot::Star, Dot::Plus) => {
                    // '.*literal.+'
                    return (
                        Arc::new(move |b: &[u8]| {
                            b.len() > lit.len() && contains(&b[..b.len() - 1], &lit)
                        }),
                        MIDDLE_MATCH_COST,
                    );
                }
                (Dot::Plus, Dot::Star) => {
                    // '.+literal.*'
                    return (
                        Arc::new(move |b: &[u8]| b.len() > lit.len() && contains(&b[1..], &lit)),
                        MIDDLE_MATCH_COST,
                    );
                }
                (Dot::Plus, Dot::Plus) => {
                    // '.+literal.+'
                    return (
                        Arc::new(move |b: &[u8]| {
                            b.len() > lit.len() + 1 && contains(&b[1..b.len() - 1], &lit)
                        }),
                        MIDDLE_MATCH_COST,
                    );
                }
            }
        }
    }

    // Fall back to the compiled regexp.
    (Arc::new(move |b: &[u8]| re.is_match(b)), RE_MATCH_COST)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Parses `s` as a plain regexp literal (no metacharacters other than
/// escaped ones). Returns None if `s` is not a plain literal.
fn parse_regexp_literal(s: &str) -> Option<Vec<u8>> {
    let src = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < src.len() {
        let c = src[i];
        match c {
            b'\\' => {
                if i + 1 >= src.len() {
                    return None;
                }
                let e = src[i + 1];
                match e {
                    b'x' => {
                        // \xNN
                        if i + 3 >= src.len() {
                            return None;
                        }
                        let hex = std::str::from_utf8(&src[i + 2..i + 4]).ok()?;
                        let v = u8::from_str_radix(hex, 16).ok()?;
                        out.push(v);
                        i += 4;
                    }
                    b'.' | b'+' | b'*' | b'?' | b'(' | b')' | b'|' | b'[' | b']' | b'{' | b'}'
                    | b'^' | b'$' | b'\\' | b'-' | b'/' => {
                        out.push(e);
                        i += 2;
                    }
                    _ => return None,
                }
            }
            b'.' | b'+' | b'*' | b'?' | b'(' | b')' | b'|' | b'[' | b']' | b'{' | b'}' | b'^'
            | b'$' => return None,
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    Some(out)
}
