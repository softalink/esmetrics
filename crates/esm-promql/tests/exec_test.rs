//! Port of the provider-independent subset of `exec_test.go`
//! (`TestExecSuccess`/`TestExecError`): queries over `time()`, scalars,
//! `label_set(...)`-built vectors and transform/binary-op/aggregation
//! combinations, evaluated at start=1000000, end=2000000, step=200000.

use esm_promql::provider::EmptyProvider;
use esm_promql::{exec, EvalConfig, QueryResult};

const NAN: f64 = f64::NAN;

fn eval_config() -> EvalConfig {
    let mut ec = EvalConfig::new(1_000_000, 2_000_000, 200_000);
    ec.max_points_per_series = 10_000;
    ec.max_series = 1000;
    ec.round_digits = 100;
    ec
}

fn run(q: &str) -> esm_promql::Result<Vec<QueryResult>> {
    exec(&EmptyProvider, &eval_config(), q)
}

/// Expected series: metric group, sorted tags, values over the shared grid.
struct R {
    group: &'static str,
    tags: &'static [(&'static str, &'static str)],
    values: [f64; 6],
}

fn r(values: [f64; 6]) -> R {
    R {
        group: "",
        tags: &[],
        values,
    }
}

fn rt(group: &'static str, tags: &'static [(&'static str, &'static str)], values: [f64; 6]) -> R {
    R {
        group,
        tags,
        values,
    }
}

const TIMESTAMPS_EXPECTED: [i64; 6] = [
    1_000_000, 1_200_000, 1_400_000, 1_600_000, 1_800_000, 2_000_000,
];

#[track_caller]
fn f(q: &str, expected: &[R]) {
    // Run the query multiple times like the Go test does, exercising the
    // parse cache.
    for _ in 0..3 {
        let result = run(q).unwrap_or_else(|err| panic!("unexpected error for {q:?}: {err}"));
        assert_eq!(
            result.len(),
            expected.len(),
            "unexpected series count for {q:?}: got {:?}",
            result
                .iter()
                .map(|r| format!("{} {:?}", r.metric_name, r.values))
                .collect::<Vec<_>>()
        );
        for (i, (got, want)) in result.iter().zip(expected).enumerate() {
            assert_eq!(
                got.metric_name.metric_group,
                want.group.as_bytes(),
                "unexpected metric group for series #{i} of {q:?}"
            );
            assert_eq!(
                got.metric_name.tags.len(),
                want.tags.len(),
                "unexpected tag count for series #{i} of {q:?}: got {}",
                got.metric_name
            );
            for &(k, v) in want.tags {
                assert_eq!(
                    got.metric_name.get_tag_value(k),
                    Some(v.as_bytes()),
                    "unexpected tag {k:?} for series #{i} of {q:?}"
                );
            }
            assert_eq!(got.timestamps.as_slice(), &TIMESTAMPS_EXPECTED);
            assert_eq!(got.values.len(), want.values.len());
            for (j, (&gv, &wv)) in got.values.iter().zip(&want.values).enumerate() {
                if wv.is_nan() {
                    assert!(
                        gv.is_nan(),
                        "series #{i} value #{j} of {q:?}: got {gv}; want NaN"
                    );
                } else {
                    assert!(
                        !gv.is_nan() && (gv - wv).abs() / wv.abs().max(f64::MIN_POSITIVE) <= 1e-13,
                        "series #{i} value #{j} of {q:?}: got {gv}; want {wv}"
                    );
                }
            }
        }
    }
}

#[track_caller]
fn f_err(q: &str) {
    for _ in 0..2 {
        let result = run(q);
        assert!(result.is_err(), "expecting non-nil error for {q:?}");
    }
}

#[test]
fn simple_number() {
    f("123", &[r([123.0; 6])]);
    f("123_456_789", &[r([123_456_789.0; 6])]);
    f("1_2.3_456_789", &[r([12.3456789; 6])]);
    f("-1.234", &[r([-1.234; 6])]);
}

#[test]
fn duration_constant() {
    f("1h23m5S", &[r([4985.0; 6])]);
    f("1h", &[r([3600.0; 6])]);
}

#[test]
fn num_with_suffix() {
    f("123M", &[r([123e6; 6])]);
    f("1.23TB", &[r([1.23e12; 6])]);
    f("1.23Mib", &[r([1.23 * (1 << 20) as f64; 6])]);
}

#[test]
fn simple_arithmetic() {
    f("-1+2 *3 ^ 4+5%6", &[r([166.0; 6])]);
    f("time()/100", &[r([10.0, 12.0, 14.0, 16.0, 18.0, 20.0])]);
    f(
        "1e3/time()*2*9*7",
        &[r([126.0, 105.0, 90.0, 78.75, 70.0, 63.0])],
    );
    f(
        "time() + time()",
        &[r([2000.0, 2400.0, 2800.0, 3200.0, 3600.0, 4000.0])],
    );
}

#[test]
fn scalar_vector_arithmetic() {
    f("scalar(-1)+2 *vector(3) ^ scalar(4)+5", &[r([166.0; 6])]);
}

#[test]
fn nan_pow_any() {
    // nan^any returns nan, even for nan^0.
    f(
        "(time() == 1600)^1",
        &[r([NAN, NAN, NAN, 1600.0, NAN, NAN])],
    );
    f("(time() < 0)^1", &[]);
    f("(time() < 0)^0", &[]);
}

#[test]
fn pow_precedence() {
    // (-4)^0.5 = NaN -> the all-NaN series is removed.
    f("time()*(-4)^0.5", &[]);
    // -4^0.5 parses as -(4^0.5).
    f(
        "time()*-4^0.5",
        &[r([-2000.0, -2400.0, -2800.0, -3200.0, -3600.0, -4000.0])],
    );
}

#[test]
fn time_func() {
    f(
        "time()",
        &[r([1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0])],
    );
}

#[test]
fn transform_math_funcs() {
    f(
        "abs(1500-time())",
        &[r([500.0, 300.0, 100.0, 100.0, 300.0, 500.0])],
    );
    f(
        "abs(-time()+1300)",
        &[r([300.0, 100.0, 100.0, 300.0, 500.0, 700.0])],
    );
    f("ceil(time()/500)", &[r([2.0, 3.0, 3.0, 4.0, 4.0, 4.0])]);
    f("floor(time()/500)", &[r([2.0, 2.0, 2.0, 3.0, 3.0, 4.0])]);
    f(
        "sqrt(time())",
        &[r([
            31.622776601683793,
            34.64101615137755,
            37.416573867739416,
            40.0,
            42.42640687119285,
            44.721359549995796,
        ])],
    );
    f(
        "ln(time())",
        &[r([
            6.907755278982137,
            7.090076835776092,
            7.24422751560335,
            7.3777589082278725,
            7.495541943884256,
            7.600902459542082,
        ])],
    );
    f(
        "log2(time())",
        &[r([
            9.965784284662087,
            10.228818690495881,
            10.451211111832329,
            10.643856189774725,
            10.813781191217037,
            10.965784284662087,
        ])],
    );
    f(
        "log10(time())",
        &[r([
            3.0,
            3.0791812460476247,
            3.1461280356782377,
            3.2041199826559246,
            3.255272505103306,
            3.3010299956639813,
        ])],
    );
    f(
        "exp(time()/1e3)",
        &[r([
            std::f64::consts::E,
            3.3201169227365472,
            4.0551999668446745,
            4.953032424395115,
            6.0496474644129465,
            7.38905609893065,
        ])],
    );
    f("sgn(time()-1400)", &[r([-1.0, -1.0, 0.0, 1.0, 1.0, 1.0])]);
}

#[test]
fn transform_clamp_funcs() {
    f(
        "clamp(time(), 1400, 1800)",
        &[r([1400.0, 1400.0, 1400.0, 1600.0, 1800.0, 1800.0])],
    );
    f(
        "clamp_max(time(), 1400)",
        &[r([1000.0, 1200.0, 1400.0, 1400.0, 1400.0, 1400.0])],
    );
    f(
        "clamp_min(time(), -time()+2500)",
        &[r([1500.0, 1300.0, 1400.0, 1600.0, 1800.0, 2000.0])],
    );
    f(
        "clamp_min(1500, time())",
        &[r([1500.0, 1500.0, 1500.0, 1600.0, 1800.0, 2000.0])],
    );
}

#[test]
fn transform_round() {
    f("round(time()/1e3)", &[r([1.0, 1.0, 1.0, 2.0, 2.0, 2.0])]);
    f(
        "round(time()/1e3, 0.5)",
        &[r([1.0, 1.0, 1.5, 1.5, 2.0, 2.0])],
    );
    f(
        "round(-time()/1e3, 0.5)",
        &[r([-1.0, -1.0, -1.5, -1.5, -2.0, -2.0])],
    );
}

#[test]
fn scalar_string() {
    // Verify that a string is converted to a number via scalar().
    f(r#"scalar("123")"#, &[r([123.0; 6])]);
    f(r#"scalar("fooobar")"#, &[]);
    // scalar over multiple series returns NaN (empty result).
    f(r#"scalar(1 or label_set(2, "xx", "foo"))"#, &[]);
}

#[test]
fn label_set_basic() {
    f(
        r#"label_set(time()/100, "tagname", "tagvalue")"#,
        &[rt(
            "",
            &[("tagname", "tagvalue")],
            [10.0, 12.0, 14.0, 16.0, 18.0, 20.0],
        )],
    );
    f(
        r#"label_set(time()/100, "__name__", "foobar")"#,
        &[rt("foobar", &[], [10.0, 12.0, 14.0, 16.0, 18.0, 20.0])],
    );
    // Empty value removes the tag.
    f(
        r#"label_set(label_set(time(), "a", "b"), "a", "")"#,
        &[r([1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0])],
    );
}

#[test]
fn sort_funcs() {
    f(
        r#"sort(2 or label_set(1, "xx", "foo"))"#,
        &[rt("", &[("xx", "foo")], [1.0; 6]), r([2.0; 6])],
    );
    f(
        r#"sort_desc(1 or label_set(2, "xx", "foo"))"#,
        &[rt("", &[("xx", "foo")], [2.0; 6]), r([1.0; 6])],
    );
    f(
        r#"sort_desc(time() or label_set(2, "xx", "foo"))"#,
        &[
            r([1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0]),
            rt("", &[("xx", "foo")], [2.0; 6]),
        ],
    );
}

#[test]
fn comparison_ops() {
    f(
        "123 < time()",
        &[r([1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0])],
    );
    f(
        "time() > 1234",
        &[r([NAN, NAN, 1400.0, 1600.0, 1800.0, 2000.0])],
    );
    f("time() >bool 1234", &[r([0.0, 0.0, 1.0, 1.0, 1.0, 1.0])]);
    f(
        "(time() > 1234) >bool 1450",
        &[r([NAN, NAN, 0.0, 1.0, 1.0, 1.0])],
    );
    f(
        "(time() > 1234) !=bool 1400",
        &[r([NAN, NAN, 0.0, 1.0, 1.0, 1.0])],
    );
    f(
        "1400 !=bool (time() > 1234)",
        &[r([NAN, NAN, 0.0, 1.0, 1.0, 1.0])],
    );
    f("123 > time()", &[]);
    f("time() < 123", &[]);
    f(
        "1300 < time() < 1700",
        &[r([NAN, NAN, 1400.0, 1600.0, NAN, NAN])],
    );
    f("1 > 2", &[]);
    f("-1 < 2", &[r([-1.0; 6])]);
    f("time() >= bool 2", &[r([1.0; 6])]);
    f("vector(1) == bool time()", &[r([0.0; 6])]);
    f("vector(1) == time()", &[]);
}

#[test]
fn compare_to_nan() {
    f("1 != nan", &[r([1.0; 6])]);
    f("nan != 1", &[]);
}

#[test]
fn default_for_nan_series() {
    f(
        r#"label_set(0, "foo", "bar")/0 default 7"#,
        &[rt("", &[("foo", "bar")], [7.0; 6])],
    );
}

#[test]
fn logical_and() {
    f("1 and (0 > 1)", &[]);
    f(
        "time() and 2",
        &[r([1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0])],
    );
    f(
        "time() and time() > 1300",
        &[r([NAN, NAN, 1400.0, 1600.0, 1800.0, 2000.0])],
    );
}

#[test]
fn logical_unless() {
    f("time() unless 2", &[]);
    f(
        "time() unless time() > 1500",
        &[r([1000.0, 1200.0, 1400.0, NAN, NAN, NAN])],
    );
    f(
        r#"label_set(time(), "foo", "bar") unless 2"#,
        &[rt(
            "",
            &[("foo", "bar")],
            [1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0],
        )],
    );
}

#[test]
fn logical_or() {
    f(
        r#"(
            label_set(time(), "x", "foo"),
            label_set(time()+1, "x", "bar"),
        ) or (
            label_set(time()+2, "x", "foo"),
            label_set(time()+3, "x", "baz"),
        )"#,
        &[
            rt(
                "",
                &[("x", "bar")],
                [1001.0, 1201.0, 1401.0, 1601.0, 1801.0, 2001.0],
            ),
            rt(
                "",
                &[("x", "foo")],
                [1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0],
            ),
            rt(
                "",
                &[("x", "baz")],
                [1003.0, 1203.0, 1403.0, 1603.0, 1803.0, 2003.0],
            ),
        ],
    );
    f(
        "time() > 1400 or 123",
        &[r([123.0, 123.0, 123.0, 1600.0, 1800.0, 2000.0])],
    );
}

#[test]
fn default_op() {
    f(
        "time() > 1400 default 123",
        &[r([123.0, 123.0, 123.0, 1600.0, 1800.0, 2000.0])],
    );
    f(
        r#"time() > 1400 default scalar(label_set(123, "foo", "bar"))"#,
        &[r([123.0, 123.0, 123.0, 1600.0, 1800.0, 2000.0])],
    );
    f(
        r#"time() > 1400 default label_set(123, "foo", "bar")"#,
        &[r([NAN, NAN, NAN, 1600.0, 1800.0, 2000.0])],
    );
    f(
        r#"time() > 1400 default (
            label_set(123, "foo", "bar"),
            label_set(456, "__name__", "xxx"),
        )"#,
        &[r([456.0, 456.0, 456.0, 1600.0, 1800.0, 2000.0])],
    );
    f(
        "time() > 1400 default (time() < -100)",
        &[r([NAN, NAN, NAN, 1600.0, 1800.0, 2000.0])],
    );
    f(
        r#"sort_desc(union(
            label_set(time() > 1400, "__name__", "x", "foo", "bar"),
            label_set(time() < 1700, "__name__", "y", "foo", "baz")) default 123)"#,
        &[
            rt(
                "x",
                &[("foo", "bar")],
                [123.0, 123.0, 123.0, 1600.0, 1800.0, 2000.0],
            ),
            rt(
                "y",
                &[("foo", "baz")],
                [1000.0, 1200.0, 1400.0, 1600.0, 123.0, 123.0],
            ),
        ],
    );
}

#[test]
fn vector_scalar_ops() {
    f(
        r#"sort_desc((label_set(time(), "foo", "bar") or label_set(10, "foo", "qwert")) / 2)"#,
        &[
            rt(
                "",
                &[("foo", "bar")],
                [500.0, 600.0, 700.0, 800.0, 900.0, 1000.0],
            ),
            rt("", &[("foo", "qwert")], [5.0; 6]),
        ],
    );
}

#[test]
fn vector_vector_matching() {
    f(
        r#"sort_desc(
            (label_set(time(), "t1", "v1") or label_set(10, "t2", "v2"))
            +
            (label_set(100, "t1", "v1") or label_set(time(), "t2", "v2"))
        )"#,
        &[
            rt(
                "",
                &[("t1", "v1")],
                [1100.0, 1300.0, 1500.0, 1700.0, 1900.0, 2100.0],
            ),
            rt(
                "",
                &[("t2", "v2")],
                [1010.0, 1210.0, 1410.0, 1610.0, 1810.0, 2010.0],
            ),
        ],
    );
    f(
        r#"sort_desc(
            (label_set(time(), "t1", "v1") or label_set(10, "t2", "v2"))
            +
            (label_set(100, "t1", "v1") or label_set(time(), "t2", "v3"))
        )"#,
        &[rt(
            "",
            &[("t1", "v1")],
            [1100.0, 1300.0, 1500.0, 1700.0, 1900.0, 2100.0],
        )],
    );
    f(
        r#"(
          (label_set(time(), "t1", "v1", "__name__", "q1") or label_set(10, "t2", "v2", "__name__", "q2"))
            +
          (label_set(100, "t1", "v1", "__name__", "q1") or label_set(time(), "t2", "v3"))
        ) keep_metric_names"#,
        &[rt(
            "q1",
            &[("t1", "v1")],
            [1100.0, 1300.0, 1500.0, 1700.0, 1900.0, 2100.0],
        )],
    );
    f(
        r#"sort_desc(
            (label_set(time(), "t2", "v1") or label_set(10, "t2", "v2"))
            +
            (label_set(100, "t1", "v1") or label_set(time(), "t2", "v3"))
        )"#,
        &[],
    );
    f(
        r#"sort_desc(
            (label_set(time(), "t1", "v123", "t2", "v3") or label_set(10, "t2", "v2"))
            + on (foo, t2)
            (label_set(100, "t1", "v1") or label_set(time(), "t2", "v3"))
        )"#,
        &[rt(
            "",
            &[("t2", "v3")],
            [2000.0, 2400.0, 2800.0, 3200.0, 3600.0, 4000.0],
        )],
    );
    f(
        r#"sort_desc(
            (label_set(time(), "t1", "v123", "t2", "v3") or label_set(10, "t2", "v2"))
            + ignoring (foo, t1, bar)
            (label_set(100, "t1", "v1") or label_set(time(), "t2", "v3"))
        )"#,
        &[rt(
            "",
            &[("t2", "v3")],
            [2000.0, 2400.0, 2800.0, 3200.0, 3600.0, 4000.0],
        )],
    );
}

#[test]
fn vector_vector_group_left() {
    f(
        r#"sort_desc(
            (label_set(time(), "t1", "v123", "t2", "v3"), label_set(10, "t2", "v3", "xxx", "yy"))
            + on (foo, t2) group_left (t1, noxxx)
            (label_set(100, "t1", "v1"), label_set(time(), "t2", "v3", "noxxx", "aa"))
        )"#,
        &[
            rt(
                "",
                &[("noxxx", "aa"), ("t2", "v3")],
                [2000.0, 2400.0, 2800.0, 3200.0, 3600.0, 4000.0],
            ),
            rt(
                "",
                &[("noxxx", "aa"), ("t2", "v3"), ("xxx", "yy")],
                [1010.0, 1210.0, 1410.0, 1610.0, 1810.0, 2010.0],
            ),
        ],
    );
}

#[test]
fn vector_vector_group_right() {
    f(
        r#"sort_desc(
            (label_set(time(), "t1", "v123", "t2", "v3") or label_set(10, "t2", "v321", "t1", "v123", "t32", "v32"))
            + ignoring (foo, t2) group_right ()
            (label_set(100, "t1", "v123") or label_set(time(), "t1", "v123", "t2", "v3"))
        )"#,
        &[
            rt(
                "",
                &[("t1", "v123"), ("t2", "v3")],
                [2000.0, 2400.0, 2800.0, 3200.0, 3600.0, 4000.0],
            ),
            rt(
                "",
                &[("t1", "v123")],
                [1100.0, 1300.0, 1500.0, 1700.0, 1900.0, 2100.0],
            ),
        ],
    );
}

#[test]
fn absent_transform() {
    f("absent(time())", &[]);
    f("absent(123)", &[]);
    f("absent(vector(scalar(123)))", &[]);
    f("absent(NaN)", &[r([1.0; 6])]);
    f(
        r#"absent(label_set(scalar(1 or label_set(2, "xx", "foo")), "yy", "foo"))"#,
        &[r([1.0; 6])],
    );
    f(
        "absent(time() > 1500)",
        &[r([1.0, 1.0, 1.0, NAN, NAN, NAN])],
    );
}

#[test]
fn union_func() {
    f("union(1)", &[r([1.0; 6])]);
    f(
        r#"union(
            label_set(1, "__name__", "one"),
            label_set(2, "__name__", "two"),
        )"#,
        &[rt("one", &[], [1.0; 6]), rt("two", &[], [2.0; 6])],
    );
    // Union dedups by metric name.
    f(
        r#"sum(union(
            label_set(1, "__name__", "foo"),
            label_set(2, "__name__", "foo"),
            label_set(3, "__name__", "foo"),
        ))"#,
        &[r([1.0; 6])],
    );
}

#[test]
fn aggr_sum() {
    f("sum(123)", &[r([123.0; 6])]);
    f("sum(1, 2, 3)", &[r([6.0; 6])]);
    f("sum((1, 2, 3))", &[r([6.0; 6])]);
    f("sum(123) by ()", &[r([123.0; 6])]);
    f("sum(123) without ()", &[r([123.0; 6])]);
    f(
        "sum(time()/100)",
        &[r([10.0, 12.0, 14.0, 16.0, 18.0, 20.0])],
    );
    f(
        "sum2(time()/100)",
        &[r([100.0, 144.0, 196.0, 256.0, 324.0, 400.0])],
    );
    f(
        r#"sum(label_set(10, "foo", "bar") or label_set(time()/100, "baz", "sss"))"#,
        &[r([20.0, 22.0, 24.0, 26.0, 28.0, 30.0])],
    );
    f(
        r#"sum2(label_set(10, "foo", "bar") or label_set(time()/100, "baz", "sss"))"#,
        &[r([200.0, 244.0, 296.0, 356.0, 424.0, 500.0])],
    );
    f(
        "geomean(time()/100)",
        &[r([10.0, 12.0, 14.0, 16.0, 18.0, 20.0])],
    );
}

#[test]
fn aggr_avg_count_stddev() {
    f(
        r#"avg(label_set(10, "foo", "bar") or label_set(time()/100, "baz", "sss"))"#,
        &[r([10.0, 11.0, 12.0, 13.0, 14.0, 15.0])],
    );
    f(
        r#"stddev(label_set(10, "foo", "bar") or label_set(time()/100, "baz", "sss"))"#,
        &[r([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])],
    );
    f(
        r#"count(label_set(time()<1500, "foo", "bar") or label_set(time()<1800, "baz", "sss"))"#,
        &[r([2.0, 2.0, 2.0, 1.0, NAN, NAN])],
    );
}

#[test]
fn aggr_by_modifiers() {
    f(
        r#"sort(sum(label_set(10, "foo", "bar") or label_set(time()/100, "baz", "sss")) by (foo))"#,
        &[
            rt("", &[("foo", "bar")], [10.0; 6]),
            r([10.0, 12.0, 14.0, 16.0, 18.0, 20.0]),
        ],
    );
    f(
        r#"sum(label_set(10, "foo", "bar", "baz", "sss", "x", "y") or label_set(time()/100, "baz", "sss", "foo", "bar")) by (foo, baz, foo)"#,
        &[rt(
            "",
            &[("baz", "sss"), ("foo", "bar")],
            [20.0, 22.0, 24.0, 26.0, 28.0, 30.0],
        )],
    );
    f(
        r#"sort(sum(label_set(10, "__name__", "bar", "baz", "sss", "x", "y") or label_set(time()/100, "baz", "sss", "__name__", "aaa")) by (__name__))"#,
        &[
            rt("bar", &[], [10.0; 6]),
            rt("aaa", &[], [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]),
        ],
    );
    f(
        r#"min(label_set(10, "foo", "bar") or label_set(time()/100/1.5, "baz", "sss")) by (unknowntag)"#,
        &[r([
            6.666666666666667,
            8.0,
            9.333333333333334,
            10.0,
            10.0,
            10.0,
        ])],
    );
    f(
        r#"max(label_set(10, "foo", "bar") or label_set(time()/100/1.5, "baz", "sss")) by (unknowntag)"#,
        &[r([
            10.0,
            10.0,
            10.0,
            10.666666666666666,
            12.0,
            13.333333333333334,
        ])],
    );
    f(
        r#"sum(label_set(10, "foo", "bar") or label_set(time()/100, "baz", "sss")) by (foo) limit 1"#,
        &[rt("", &[("foo", "bar")], [10.0; 6])],
    );
    f(r#"avg(123) wiTHout (xx, yy)"#, &[r([123.0; 6])]);
}

#[test]
fn aggr_group() {
    f(
        r#"group((
            label_set(time()<1500, "foo", "bar"),
            label_set(time()<1800, "baz", "sss"),
        ))"#,
        &[r([1.0, 1.0, 1.0, 1.0, NAN, NAN])],
    );
}

#[test]
fn rate_over_empty_selector() {
    // rate({}) — an empty metric expr yields NaN, which is removed from
    // the output.
    f("rate({}[5m])", &[]);
}

#[test]
fn metric_selector_no_data() {
    // The EmptyProvider returns no series.
    f("foobar", &[]);
    f("rate(foobar[5m])", &[]);
    f("sum(rate(foobar[5m]))", &[]);
    // absent over a missing metric returns 1 with the plain filters as tags.
    f(
        r#"absent(foobar{job="x"})"#,
        &[rt("", &[("job", "x")], [1.0; 6])],
    );
}

#[test]
fn keep_metric_names_modifiers() {
    f(
        r#"exp(label_set(time()/1e3, "__name__", "foobar")) keep_metric_names"#,
        &[rt(
            "foobar",
            &[],
            [
                std::f64::consts::E,
                3.3201169227365472,
                4.0551999668446745,
                4.953032424395115,
                6.0496474644129465,
                7.38905609893065,
            ],
        )],
    );
}

#[test]
fn cmp_keeps_metric_group() {
    // Comparison without `bool` keeps the metric group.
    f(
        r#"sort_desc((
            label_set(time(), "__name__", "foo", "a", "x"),
            label_set(time()+200, "__name__", "bar", "a", "x"),
        ) > 1300)"#,
        &[
            rt(
                "bar",
                &[("a", "x")],
                [NAN, 1400.0, 1600.0, 1800.0, 2000.0, 2200.0],
            ),
            rt(
                "foo",
                &[("a", "x")],
                [NAN, NAN, 1400.0, 1600.0, 1800.0, 2000.0],
            ),
        ],
    );
    // Comparison with `bool` drops the metric group.
    f(
        r#"sort_desc((
            label_set(time(), "__name__", "foo", "a", "x"),
            label_set(time()+200, "__name__", "bar", "a", "y"),
        ) >bool 1300)"#,
        &[
            rt("", &[("a", "y")], [0.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            rt("", &[("a", "x")], [0.0, 0.0, 1.0, 1.0, 1.0, 1.0]),
        ],
    );
}

#[test]
fn start_end_step_funcs() {
    f("start()", &[r([1000.0; 6])]);
    f("end()", &[r([2000.0; 6])]);
    f("step()", &[r([200.0; 6])]);
}

#[test]
fn exec_errors() {
    // Parse errors.
    f_err("fn selector");
    f_err("foo{");
    f_err("sum(");
    // Unknown functions.
    f_err("nonexisting_func(1)");
    // Invalid arg counts.
    f_err("abs()");
    f_err("abs(1, 2)");
    f_err("clamp_max(1)");
    f_err("round(1, 2, 3)");
    f_err("scalar()");
    f_err("vector()");
    f_err("time(123)");
    f_err("sum()");
    // Duplicate time series on a binary op side.
    f_err(r#"(label_set(1, "foo", "bar"), label_set(2, "foo", "baz")) + on() 1"#);
    f_err(r#"1 + on() (label_set(1, "foo", "bar"), label_set(2, "foo", "baz"))"#);
    // Subqueries aren't supported in Stage 1.
    f_err("rate(sum(foobar)[5m:1m])");
    // label_set with odd number of string args.
    f_err(r#"label_set(1, "foo")"#);
}
