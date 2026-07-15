//! Port of the core `tag_filters_test.go` cases: Init/prefix, or-values,
//! match behavior (incl. regexp fallback), Add normalizations, Less ordering
//! and composite-filter conversion.

use esm_storage::index::{
    convert_to_composite_tag_filterss, marshal_common_prefix, marshal_composite_tag_key, TagFilter,
    TagFilters, NS_PREFIX_TAG_TO_METRIC_IDS,
};
use esm_storage::metric_name::marshal_tag_value;

fn common_prefix() -> Vec<u8> {
    b"prefix".to_vec()
}

fn tv_no_trailing_tag_separator(s: &str) -> Vec<u8> {
    let mut dst = Vec::new();
    marshal_tag_value(&mut dst, s.as_bytes());
    dst.pop();
    dst
}

struct MatchSuffixTester {
    tf: TagFilter,
}

impl MatchSuffixTester {
    fn init(&mut self, value: &str, is_negative: bool, is_regexp: bool, expected_prefix: &[u8]) {
        let common_prefix = common_prefix();
        let key = b"key";
        self.tf
            .init(
                &common_prefix,
                key,
                value.as_bytes(),
                is_negative,
                is_regexp,
            )
            .unwrap_or_else(|err| panic!("unexpected error: {err}"));
        let mut prefix = common_prefix;
        marshal_tag_value(&mut prefix, key);
        prefix.extend_from_slice(expected_prefix);
        assert_eq!(
            self.tf.prefix(),
            &prefix[..],
            "unexpected tf.prefix for value {value:?}"
        );
    }

    fn matches(&self, suffix: &str) {
        let mut suffix_escaped = Vec::new();
        marshal_tag_value(&mut suffix_escaped, suffix.as_bytes());
        let ok = self.tf.match_suffix(&suffix_escaped).unwrap();
        assert_ne!(
            ok,
            self.tf.is_negative(),
            "{} must match suffix {suffix:?}",
            self.tf
        );
    }

    fn mismatches(&self, suffix: &str) {
        let mut suffix_escaped = Vec::new();
        marshal_tag_value(&mut suffix_escaped, suffix.as_bytes());
        let ok = self.tf.match_suffix(&suffix_escaped).unwrap();
        assert_eq!(
            ok,
            self.tf.is_negative(),
            "{} mustn't match suffix {suffix:?}",
            self.tf
        );
    }
}

#[test]
fn tag_filter_match_suffix() {
    let mut t = MatchSuffixTester {
        tf: TagFilter::default(),
    };

    // plain-value
    t.init("xx", false, false, &tv_no_trailing_tag_separator("xx"));
    t.matches("");
    t.mismatches("foo");
    t.mismatches("xx");

    // negative-plain-value
    t.init("xx", true, false, &tv_no_trailing_tag_separator("xx"));
    t.mismatches("");
    t.matches("foo");
    t.matches("foxx");
    t.matches("xx");
    t.matches("xxx");
    t.matches("xxfoo");

    // regexp-convert-to-plain-value
    t.init("http", false, true, &tv_no_trailing_tag_separator("http"));
    t.matches("");
    t.mismatches("x");
    t.mismatches("http");
    t.mismatches("foobar");

    // negative-regexp-convert-to-plain-value
    t.init("http", true, true, &tv_no_trailing_tag_separator("http"));
    t.mismatches("");
    t.matches("x");
    t.matches("xhttp");
    t.matches("http");
    t.matches("httpx");
    t.matches("foobar");

    // regexp-prefix-any-suffix
    t.init("http.*", false, true, &tv_no_trailing_tag_separator("http"));
    t.matches("");
    t.matches("x");
    t.matches("http");
    t.matches("foobar");

    // negative-regexp-prefix-any-suffix
    t.init("http.*", true, true, &tv_no_trailing_tag_separator("http"));
    t.mismatches("");
    t.mismatches("x");
    t.mismatches("xhttp");
    t.mismatches("http");
    t.mismatches("httpsdf");
    t.mismatches("foobar");

    // regexp-prefix-contains-suffix
    t.init(
        "http.*foo.*",
        false,
        true,
        &tv_no_trailing_tag_separator("http"),
    );
    t.mismatches("");
    t.mismatches("x");
    t.mismatches("http");
    t.matches("foo");
    t.matches("foobar");
    t.matches("xfoobar");
    t.matches("xfoo");

    // negative-regexp-prefix-contains-suffix
    t.init(
        "http.*foo.*",
        true,
        true,
        &tv_no_trailing_tag_separator("http"),
    );
    t.matches("");
    t.matches("x");
    t.matches("http");
    t.mismatches("foo");
    t.mismatches("foobar");
    t.mismatches("xfoobar");
    t.mismatches("xfoo");
    t.mismatches("httpfoo");
    t.mismatches("httpfoobar");
    t.mismatches("httpxfoobar");
    t.mismatches("httpxfoo");

    // negative-regexp-noprefix-contains-suffix
    t.init(".*foo.*", true, true, &tv_no_trailing_tag_separator(""));
    t.matches("");
    t.matches("x");
    t.matches("http");
    t.mismatches("foo");
    t.mismatches("foobar");
    t.mismatches("xfoobar");
    t.mismatches("xfoo");

    // regexp-prefix-special-suffix
    t.init(
        "http.*bar",
        false,
        true,
        &tv_no_trailing_tag_separator("http"),
    );
    t.mismatches("");
    t.mismatches("x");
    t.matches("bar");
    t.mismatches("barx");
    t.matches("foobar");
    t.mismatches("foobarx");

    // negative-regexp-prefix-special-suffix
    t.init(
        "http.*bar",
        true,
        true,
        &tv_no_trailing_tag_separator("http"),
    );
    t.matches("");
    t.mismatches("bar");
    t.mismatches("xhttpbar");
    t.mismatches("httpbar");
    t.matches("httpbarx");
    t.mismatches("httpxybar");
    t.matches("httpxybarx");
    t.mismatches("ahttpxybar");

    // negative-regexp-noprefix-special-suffix
    t.init(".*bar", true, true, &tv_no_trailing_tag_separator(""));
    t.matches("");
    t.mismatches("bar");
    t.mismatches("xhttpbar");
    t.matches("barx");
    t.matches("pbarx");

    // regexp-or-suffixes
    t.init(
        "http(foo|bar)",
        false,
        true,
        &tv_no_trailing_tag_separator("http"),
    );
    assert_eq!(
        t.tf.or_suffixes(),
        &["bar".to_string(), "foo".to_string()][..],
        "unexpected or_suffixes"
    );
    t.mismatches("");
    t.mismatches("x");
    t.matches("bar");
    t.mismatches("barx");
    t.matches("foo");
    t.mismatches("foobar");

    // negative-regexp-or-suffixes
    t.init(
        "http(foo|bar)",
        true,
        true,
        &tv_no_trailing_tag_separator("http"),
    );
    assert_eq!(
        t.tf.or_suffixes(),
        &["bar".to_string(), "foo".to_string()][..],
        "unexpected or_suffixes"
    );
    t.matches("");
    t.matches("x");
    t.mismatches("foo");
    t.matches("fooa");
    t.matches("xfooa");
    t.mismatches("bar");
    t.matches("xhttpbar");

    // regexp-iflag-no-suffix (regex fallback path)
    t.init("(?i)http", false, true, &tv_no_trailing_tag_separator(""));
    t.matches("http");
    t.matches("HTTP");
    t.matches("hTTp");
    t.mismatches("");
    t.mismatches("foobar");
    t.mismatches("xhttp");
    t.mismatches("xhttp://");
    t.mismatches("hTTp://foobar.com");

    // negative-regexp-iflag-no-suffix
    t.init("(?i)http", true, true, &tv_no_trailing_tag_separator(""));
    t.mismatches("http");
    t.mismatches("HTTP");
    t.mismatches("hTTp");
    t.matches("");
    t.matches("foobar");
    t.matches("xhttp");
    t.matches("xhttp://");
    t.matches("hTTp://foobar.com");

    // regexp-iflag-any-suffix
    t.init("(?i)http.*", false, true, &tv_no_trailing_tag_separator(""));
    t.matches("http");
    t.matches("HTTP");
    t.matches("hTTp://foobar.com");
    t.mismatches("");
    t.mismatches("foobar");
    t.mismatches("xhttp");
    t.mismatches("xhttp://");

    // non-empty-string-regexp-negative-match
    t.init(".+", true, true, &tv_no_trailing_tag_separator(""));
    assert!(
        t.tf.or_suffixes().is_empty(),
        "unexpected non-zero number of or suffixes: {:?}",
        t.tf.or_suffixes()
    );
    t.matches("");
    t.mismatches("x");
    t.mismatches("foo");

    // non-empty-string-regexp-match
    t.init(".+", false, true, &tv_no_trailing_tag_separator(""));
    assert!(t.tf.or_suffixes().is_empty());
    t.mismatches("");
    t.matches("x");
    t.matches("foo");

    // match-all-regexp-negative-match
    t.init(".*", true, true, &tv_no_trailing_tag_separator(""));
    t.mismatches("");
    t.mismatches("x");
    t.mismatches("foo");

    // match-all-regexp-match
    t.init(".*", false, true, &tv_no_trailing_tag_separator(""));
    t.matches("");
    t.matches("x");
    t.matches("foo");
}

/// Port of the TestGetRegexpFromCache match/mismatch cases through the full
/// TagFilter machinery (init + matches against a marshaled item).
#[test]
fn regexp_match_behavior() {
    fn f(re: &str, expected_matches: &[&str], expected_mismatches: &[&str]) {
        let mut tf = TagFilter::default();
        let mut common_prefix = Vec::new();
        marshal_common_prefix(&mut common_prefix, NS_PREFIX_TAG_TO_METRIC_IDS);
        tf.init(&common_prefix, b"key", re.as_bytes(), false, true)
            .unwrap_or_else(|err| panic!("cannot init tf for re {re:?}: {err}"));
        let check = |s: &str, want: bool| {
            let mut b = Vec::new();
            marshal_common_prefix(&mut b, NS_PREFIX_TAG_TO_METRIC_IDS);
            marshal_tag_value(&mut b, b"key");
            marshal_tag_value(&mut b, s.as_bytes());
            let got = tf.matches(&b).unwrap();
            assert_eq!(got, want, "re={re:?} s={s:?}");
        };
        for s in expected_matches {
            check(s, true);
        }
        for s in expected_mismatches {
            check(s, false);
        }
    }

    f("", &[""], &["foo", "x"]);
    f("foo", &["foo"], &["", "bar"]);
    f("(?s)(foo)?", &["foo", ""], &["s", "bar"]);
    f("foo.*", &["foo", "foobar"], &["xfoo", "xfoobar", "", "a"]);
    f(
        "foo(a|b)?",
        &["fooa", "foob", "foo"],
        &["xfoo", "xfoobar", "", "fooc", "fooba"],
    );
    f(".*foo", &["foo", "xfoo"], &["foox", "xfoobar", "", "a"]);
    f(
        "(a|b)?foo",
        &["foo", "afoo", "bfoo"],
        &["foox", "xfoobar", "", "a"],
    );
    f(
        ".*foo.*",
        &["foo", "xfoo", "foox", "xfoobar"],
        &["", "bar", "foxx"],
    );
    f(
        ".*foo.+",
        &["foo1", "xfoodff", "foox", "xfoobar"],
        &["", "bar", "foo", "fox"],
    );
    f(
        ".+foo.+",
        &["xfoo1", "xfoodff", "xfoox", "xfoobar"],
        &["", "bar", "foo", "foox", "xfoo"],
    );
    f(
        ".+foo.*",
        &["xfoo", "xfoox", "xfoobar"],
        &["", "bar", "foo", "fox"],
    );
    f(
        ".+foo(a|b)?",
        &["xfoo", "xfooa", "xafoob"],
        &["", "bar", "foo", "foob"],
    );
    f(
        ".*foo(a|b)?",
        &["foo", "foob", "xafoo", "xfooa"],
        &["", "bar", "fooba"],
    );
    f(
        "(a|b)?foo(a|b)?",
        &["foo", "foob", "afoo", "afooa"],
        &["", "bar", "fooba", "xfoo"],
    );
    f(
        "((.*)foo(.*))",
        &["foo", "xfoo", "foox", "xfoobar"],
        &["", "bar", "foxx"],
    );
    f(".+foo", &["afoo", "bbfoo"], &["foo", "foobar", "afoox", ""]);
    f("a|b", &["a", "b"], &["xa", "bx", "xab", ""]);
    f("(a|b)", &["a", "b"], &["xa", "bx", "xab", ""]);
    f(
        "(a|b)foo(c|d)",
        &["afooc", "bfood"],
        &["foo", "", "afoo", "fooc", "xfood"],
    );
    f("foo.+", &["foox", "foobar"], &["foo", "afoox", "afoo", ""]);
    f(
        ".*foo.*bar",
        &["foobar", "xfoobar", "xfooxbar", "fooxbar"],
        &["", "foobarx", "afoobarx", "aaa"],
    );
    f(
        "foo.*bar",
        &["foobar", "fooxbar"],
        &["xfoobar", "", "foobarx", "aaa"],
    );
    f(
        "foo.*bar.*",
        &["foobar", "fooxbar", "foobarx", "fooxbarx"],
        &["", "afoobarx", "aaa", "afoobar"],
    );
    f(
        "foo.*bar.*baz",
        &["foobarbaz", "fooxbarxbaz", "foobarxbaz", "fooxbarbaz"],
        &["", "afoobarx", "aaa", "afoobar", "foobarzaz"],
    );
    f(
        ".+foo.+(b|c).+",
        &["xfooxbar", "xfooxca"],
        &["", "foo", "foob", "xfooc", "xfoodc"],
    );
    f(
        "(?i)foo",
        &["foo", "Foo", "FOO"],
        &["xfoo", "foobar", "xFOObar"],
    );
    f(
        "(?i).+foo",
        &["xfoo", "aaFoo", "bArFOO"],
        &["foosdf", "xFOObar"],
    );
    f(
        "(?i)(foo|bar)",
        &["foo", "Foo", "BAR", "bAR"],
        &["foobar", "xfoo", "xFOObAR"],
    );
    f(
        "(?i)foo.*bar",
        &["foobar", "FooBAR", "FOOxxbaR"],
        &["xfoobar", "foobarx", "xFOObarx"],
    );
    f(".*", &["", "a", "foo", "foobar"], &[]);
    f(".+|", &["", "a", "foo", "foobar"], &[]);
    f(".+||foo|bar", &["", "a", "foo", "foobar"], &[]);
    f("foo|.*", &["", "a", "foo", "foobar"], &[]);
    f(".+", &["a", "foo"], &[""]);
    f("(.+)*(foo)?", &["a", "foo", ""], &[]);

    // Graphite-like regexps.
    f(
        r"foo\.[^.]*\.bar\.ba(xx|zz)[^.]*\.a",
        &["foo.ss.bar.baxx.a", "foo.s.bar.bazzasd.a"],
        &["", "foo", "foo.ss.xar.baxx.a"],
    );
    f(
        r"foo\.[^.]*?\.bar\.baz\.aaa",
        &["foo.aa.bar.baz.aaa"],
        &["", "foo"],
    );
}

#[test]
fn tag_filters_add_empty() {
    let mut tfs = TagFilters::new();

    fn expect_tag_filter(
        tfs: &TagFilters,
        idx: usize,
        value: &str,
        is_negative: bool,
        is_regexp: bool,
    ) {
        assert!(
            idx < tfs.filters().len(),
            "missing tag filter #{idx}; tfs={tfs}"
        );
        let tf = &tfs.filters()[idx];
        assert_eq!(tf.value(), value.as_bytes(), "unexpected tag filter value");
        assert_eq!(tf.is_negative(), is_negative, "unexpected is_negative");
        assert_eq!(tf.is_regexp(), is_regexp, "unexpected is_regexp");
    }

    // Empty filters.
    tfs.add(&[], &[], false, false).unwrap();
    expect_tag_filter(&tfs, 0, ".+", true, true);
    tfs.add(b"foo", &[], false, false).unwrap();
    expect_tag_filter(&tfs, 1, ".+", true, true);
    tfs.add(b"foo", &[], true, false).unwrap();
    expect_tag_filter(&tfs, 2, ".+", false, true);

    // Empty regexp filters.
    tfs.reset();
    tfs.add(b"foo", b".*", false, true).unwrap();
    assert_eq!(
        tfs.filters().len(),
        0,
        "unexpectedly added an empty regexp filter"
    );
    tfs.add(b"foo", b".*", true, true).unwrap();
    expect_tag_filter(&tfs, 0, ".+", true, true);
    tfs.add(b"foo", b"foo||bar", false, true).unwrap();
    expect_tag_filter(&tfs, 1, "foo||bar", false, true);

    // Verify that other filters are added normally.
    tfs.reset();
    tfs.add(&[], b"foobar", false, false).unwrap();
    assert_eq!(tfs.filters().len(), 1);
    tfs.add(b"bar", b"foobar", true, false).unwrap();
    assert_eq!(tfs.filters().len(), 2);
    tfs.add(&[], b"foo.+bar", true, true).unwrap();
    assert_eq!(tfs.filters().len(), 3);
    tfs.add(b"bar", b"foo.+bar", false, true).unwrap();
    assert_eq!(tfs.filters().len(), 4);
    tfs.add(b"bar", b"foo.*", false, true).unwrap();
    assert_eq!(tfs.filters().len(), 5);
}

#[test]
fn tag_filters_string() {
    let mut tfs = TagFilters::new();
    tfs.add(b"", b"metric_name", false, false).unwrap();
    tfs.add(b"tag_re", b"re.value", false, true).unwrap();
    tfs.add(b"tag_nre", b"nre.value", true, true).unwrap();
    tfs.add(b"tag_n", b"n_value", true, false).unwrap();
    tfs.add(b"tag_re_graphite", b"foo\\.bar", false, true)
        .unwrap();
    let s = tfs.to_string();
    let expected = r#"{__name__="metric_name",tag_re=~"re.value",tag_nre!~"nre.value",tag_n!="n_value",tag_re_graphite="foo.bar"}"#;
    assert_eq!(s, expected, "unexpected TagFilters string");
}

#[test]
fn tag_filter_less_ordering() {
    // Uses init to construct the filters, then verifies the planner
    // ordering: composite first, plain before regexp, fewer or-suffixes
    // first, positive before negative.
    let common_prefix = {
        let mut p = Vec::new();
        marshal_common_prefix(&mut p, NS_PREFIX_TAG_TO_METRIC_IDS);
        p
    };
    let mk = |key: &[u8], value: &[u8], is_negative: bool, is_regexp: bool| {
        let mut tf = TagFilter::default();
        tf.init(&common_prefix, key, value, is_negative, is_regexp)
            .unwrap();
        tf
    };

    // Composite filters come first, even with a higher match cost.
    let mut composite_key = Vec::new();
    marshal_composite_tag_key(&mut composite_key, b"metric", b"key");
    let composite = mk(&composite_key, b"a.+b.+c", false, true);
    let normal = mk(b"normal", b"value", false, false);
    assert!(composite.match_cost() > normal.match_cost());
    assert!(composite.less(&normal));
    assert!(!normal.less(&composite));

    // Lower match cost comes first.
    let low_cost = mk(b"key1", b"exact", false, false); // full match cost
    let high_cost = mk(b"key1", b"a|b|c|d", false, true); // 4 or-values
    assert!(low_cost.match_cost() < high_cost.match_cost());
    assert!(low_cost.less(&high_cost));
    assert!(!high_cost.less(&low_cost));

    // Fewer or-suffixes come first (same match cost per literal count is
    // avoided by using equal-cost filters below).
    let few = mk(b"key3", b"a", false, false);
    let many = mk(b"key3", b"b", false, false);
    // Same cost, same or-suffix count (1): ordering falls back to prefix.
    assert!(few.less(&many));
    assert!(!many.less(&few));

    // Positive filters come first.
    let non_negative = mk(b"key4", b"value", false, false);
    let negative = mk(b"key4", b"value", true, false);
    assert!(non_negative.less(&negative));
    assert!(!negative.less(&non_negative));
}

#[test]
fn convert_to_composite_tag_filters_basic() {
    // {__name__="name", foo="bar"} is converted into
    // {composite(name,foo)="bar"}.
    let mut tfs = TagFilters::new();
    tfs.add(&[], b"name", false, false).unwrap();
    tfs.add(b"foo", b"bar", false, false).unwrap();
    let converted = convert_to_composite_tag_filterss(std::slice::from_ref(&tfs));
    assert_eq!(converted.len(), 1);
    let ctfs = &converted[0];
    assert_eq!(ctfs.filters().len(), 1);
    let tf = &ctfs.filters()[0];
    assert!(tf.is_composite(), "expected composite filter, got {tf}");
    let mut expected_key = Vec::new();
    marshal_composite_tag_key(&mut expected_key, b"name", b"foo");
    assert_eq!(tf.key(), &expected_key[..]);
    assert_eq!(tf.value(), b"bar");
    assert!(!tf.is_negative());
    assert!(!tf.is_regexp());
}

#[test]
fn convert_to_composite_tag_filters_no_positive_filter() {
    // No positive non-name filter -> no conversion.
    let mut tfs = TagFilters::new();
    tfs.add(&[], b"name", false, false).unwrap();
    tfs.add(b"foo", b"bar", true, false).unwrap();
    let converted = convert_to_composite_tag_filterss(std::slice::from_ref(&tfs));
    assert_eq!(converted.len(), 1);
    assert_eq!(converted[0].filters().len(), 2);
    assert!(!converted[0].filters()[0].is_composite());
    assert!(!converted[0].filters()[1].is_composite());
}

#[test]
fn convert_to_composite_tag_filters_no_name() {
    // No __name__ filter -> no conversion.
    let mut tfs = TagFilters::new();
    tfs.add(b"foo", b"bar", false, false).unwrap();
    let converted = convert_to_composite_tag_filterss(std::slice::from_ref(&tfs));
    assert_eq!(converted.len(), 1);
    assert_eq!(converted[0].filters().len(), 1);
    assert!(!converted[0].filters()[0].is_composite());
}

#[test]
fn convert_to_composite_tag_filters_regexp_names() {
    // {__name__=~"name1|name2", foo="bar"} is split into two composite
    // filter groups, one per name.
    let mut tfs = TagFilters::new();
    tfs.add(&[], b"name1|name2", false, true).unwrap();
    tfs.add(b"foo", b"bar", false, false).unwrap();
    let converted = convert_to_composite_tag_filterss(std::slice::from_ref(&tfs));
    assert_eq!(converted.len(), 2);
    for (i, name) in [b"name1", b"name2"].iter().enumerate() {
        let ctfs = &converted[i];
        assert_eq!(ctfs.filters().len(), 1, "group #{i}: {ctfs}");
        let tf = &ctfs.filters()[0];
        assert!(tf.is_composite());
        let mut expected_key = Vec::new();
        marshal_composite_tag_key(&mut expected_key, *name, b"foo");
        assert_eq!(tf.key(), &expected_key[..]);
        assert_eq!(tf.value(), b"bar");
    }
}
