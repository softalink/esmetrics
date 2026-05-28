//! Property-test fuzz harness for the PromQL parser.
//!
//! The contract under test is simple: the parser must never panic on any
//! input. It may return an `Err`; that's fine. Anything else (panic,
//! infinite loop, stack overflow) is a bug.
//!
//! For each shape we generate a structured input (so the parser exercises
//! interesting code paths) and a raw arbitrary string (so the lexer
//! exercises edge cases). A bounded `cases` count keeps `cargo test`
//! turnaround reasonable; CI can crank it via `PROPTEST_CASES`.

use esm_promql::parser::parse;
use proptest::prelude::*;

fn arb_metric_name() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,20}".prop_map(String::from)
}

fn arb_label_pair() -> impl Strategy<Value = (String, String)> {
    (arb_metric_name(), "[a-zA-Z0-9 _.-]{0,20}".prop_map(String::from))
}

fn arb_duration() -> impl Strategy<Value = String> {
    (1u64..=10_000, prop::sample::select(vec!["s", "m", "h", "d"]))
        .prop_map(|(n, u)| format!("{n}{u}"))
}

fn arb_well_formed_selector() -> impl Strategy<Value = String> {
    (arb_metric_name(), prop::collection::vec(arb_label_pair(), 0..4)).prop_map(|(name, labels)| {
        if labels.is_empty() {
            name
        } else {
            let inner =
                labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect::<Vec<_>>().join(",");
            format!("{name}{{{inner}}}")
        }
    })
}

fn arb_well_formed_query() -> impl Strategy<Value = String> {
    prop_oneof![
        arb_well_formed_selector(),
        (arb_well_formed_selector(), arb_duration()).prop_map(|(s, d)| format!("rate({s}[{d}])")),
        arb_well_formed_selector().prop_map(|s| format!("sum({s})")),
        (arb_well_formed_selector(), arb_metric_name())
            .prop_map(|(s, l)| format!("sum by ({l}) ({s})")),
        arb_well_formed_selector().prop_map(|s| format!("topk(5, {s})")),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// Well-formed queries should parse successfully.
    #[test]
    fn well_formed_queries_parse_ok(q in arb_well_formed_query()) {
        let _ = parse(&q).expect("well-formed query should parse");
    }

    /// Arbitrary byte sequences must not panic the parser.
    #[test]
    fn arbitrary_bytes_dont_panic(s in ".{0,200}") {
        let _ = parse(&s); // Err is fine; panic is not.
    }

    /// Deeply nested parens must not stack-overflow within a reasonable
    /// depth bound.
    #[test]
    fn deep_parens_dont_overflow(depth in 0usize..50) {
        let s = format!("{}up{}", "(".repeat(depth), ")".repeat(depth));
        let _ = parse(&s);
    }
}
