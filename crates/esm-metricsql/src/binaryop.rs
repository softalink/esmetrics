//! Binary operators: precedence tables and constant evaluation.
//!
//! Port of `binary_op.go` and `binaryop/funcs.go`.

/// All the supported binary operators (lowercase).
static BINARY_OPS: &[&str] = &[
    "+", "-", "*", "/", "%", "^",     // arithmetic
    "atan2", // See https://github.com/prometheus/prometheus/pull/9248
    "==", "!=", ">", "<", ">=", "<=", // cmp ops
    "and", "or", "unless", // logical set ops
    "if", "ifnot", "default", // MetricsQL extensions
];

/// Port of Go `isBinaryOp`.
pub(crate) fn is_binary_op(op: &str) -> bool {
    let op = op.to_ascii_lowercase();
    BINARY_OPS.contains(&op.as_str())
}

/// Port of Go `binaryOpPriority`.
///
/// See <https://prometheus.io/docs/prometheus/latest/querying/operators/#binary-operator-precedence>
pub(crate) fn binary_op_priority(op: &str) -> i32 {
    match op.to_ascii_lowercase().as_str() {
        "default" => -1,
        "if" | "ifnot" => 0,
        "or" => 1,
        "and" | "unless" => 2,
        "==" | "!=" | "<" | ">" | "<=" | ">=" => 3,
        "+" | "-" => 4,
        "*" | "/" | "%" | "atan2" => 5,
        "^" => 6,
        _ => i32::MIN,
    }
}

/// Port of Go `scanBinaryOpPrefix`: returns the length of the longest binary
/// operator that prefixes `s` (case-insensitive), or 0.
pub(crate) fn scan_binary_op_prefix(s: &str) -> usize {
    let b = s.as_bytes();
    let mut n = 0;
    for op in BINARY_OPS {
        if b.len() < op.len() {
            continue;
        }
        if b[..op.len()].eq_ignore_ascii_case(op.as_bytes()) && op.len() > n {
            n = op.len();
        }
    }
    n
}

/// Port of Go `isRightAssociativeBinaryOp`.
pub(crate) fn is_right_associative_binary_op(op: &str) -> bool {
    op == "^"
}

/// Port of Go `isBinaryOpGroupModifier` (`on` / `ignoring`).
pub(crate) fn is_binary_op_group_modifier(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "on" | "ignoring")
}

/// Port of Go `isBinaryOpJoinModifier` (`group_left` / `group_right`).
pub(crate) fn is_binary_op_join_modifier(s: &str) -> bool {
    matches!(
        s.to_ascii_lowercase().as_str(),
        "group_left" | "group_right"
    )
}

/// Port of Go `isBinaryOpBoolModifier`.
pub(crate) fn is_binary_op_bool_modifier(s: &str) -> bool {
    s.eq_ignore_ascii_case("bool")
}

/// Returns true if `op` is a comparison operator such as `==`, `!=`, etc.
///
/// Port of Go `IsBinaryOpCmp`.
pub fn is_binary_op_cmp(op: &str) -> bool {
    matches!(op, "==" | "!=" | ">" | "<" | ">=" | "<=")
}

/// Port of Go `isBinaryOpLogicalSet`.
pub(crate) fn is_binary_op_logical_set(op: &str) -> bool {
    matches!(op.to_ascii_lowercase().as_str(), "and" | "or" | "unless")
}

/// Port of Go `binaryOpEvalNumber` plus `binaryop/funcs.go`.
pub(crate) fn binary_op_eval_number(op: &str, left: f64, right: f64, is_bool: bool) -> f64 {
    let op = op.to_ascii_lowercase();
    if is_binary_op_cmp(&op) {
        let ok = match op.as_str() {
            // Special handling for NaN comparisons.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/150
            "==" => {
                if left.is_nan() {
                    right.is_nan()
                } else {
                    left == right
                }
            }
            "!=" => {
                if left.is_nan() {
                    !right.is_nan()
                } else if right.is_nan() {
                    true
                } else {
                    left != right
                }
            }
            ">" => left > right,
            "<" => left < right,
            ">=" => left >= right,
            "<=" => left <= right,
            _ => unreachable!("BUG: unexpected comparison binaryOp: {op:?}"),
        };
        if is_bool {
            if ok {
                1.0
            } else {
                0.0
            }
        } else if ok {
            left
        } else {
            f64::NAN
        }
    } else {
        match op.as_str() {
            "+" => left + right,
            "-" => left - right,
            "*" => left * right,
            "/" => left / right,
            "%" => left % right,
            "atan2" => left.atan2(right),
            "^" => {
                // Special case for NaN^any.
                // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/7359
                if left.is_nan() {
                    f64::NAN
                } else {
                    left.powf(right)
                }
            }
            "and" => {
                if left.is_nan() || right.is_nan() {
                    f64::NAN
                } else {
                    left
                }
            }
            "or" => {
                if !left.is_nan() {
                    left
                } else {
                    right
                }
            }
            "unless" => f64::NAN,
            "default" => {
                if left.is_nan() {
                    right
                } else {
                    left
                }
            }
            "if" => {
                if right.is_nan() {
                    f64::NAN
                } else {
                    left
                }
            }
            "ifnot" => {
                if right.is_nan() {
                    left
                } else {
                    f64::NAN
                }
            }
            _ => unreachable!("BUG: unexpected non-comparison binaryOp: {op:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of TestIsBinaryOpSuccess.
    #[test]
    fn is_binary_op_success() {
        for s in [
            "and", "AND", "unless", "unleSS", "==", "!=", ">=", "<=", "or", "Or", "+", "-", "*",
            "/", "%", "atan2", "^", ">", "<",
        ] {
            assert!(is_binary_op(s), "expecting valid binaryOp: {s:?}");
        }
    }

    // Port of TestIsBinaryOpError.
    #[test]
    fn is_binary_op_error() {
        for s in ["foobar", "=~", "!~", "=", "<==", "234"] {
            assert!(!is_binary_op(s), "unexpected valid binaryOp: {s:?}");
        }
    }

    // Port of TestIsBinaryOpGroupModifierSuccess/Error.
    #[test]
    fn is_binary_op_group_modifier_cases() {
        for s in ["on", "ON", "oN", "ignoring", "IGnoring"] {
            assert!(is_binary_op_group_modifier(s), "expecting valid: {s:?}");
        }
        for s in ["off", "by", "without", "123"] {
            assert!(!is_binary_op_group_modifier(s), "unexpected valid: {s:?}");
        }
    }

    // Port of TestIsBinaryOpJoinModifierSuccess/Error.
    #[test]
    fn is_binary_op_join_modifier_cases() {
        for s in ["group_left", "group_right", "group_LEft", "GRoup_RighT"] {
            assert!(is_binary_op_join_modifier(s), "expecting valid: {s:?}");
        }
        for s in ["on", "by", "without", "123"] {
            assert!(!is_binary_op_join_modifier(s), "unexpected valid: {s:?}");
        }
    }

    // Port of TestIsBinaryOpBoolModifierSuccess/Error.
    #[test]
    fn is_binary_op_bool_modifier_cases() {
        for s in ["bool", "bOOL", "BOOL"] {
            assert!(is_binary_op_bool_modifier(s), "expecting valid: {s:?}");
        }
        for s in ["on", "by", "without", "123"] {
            assert!(!is_binary_op_bool_modifier(s), "unexpected valid: {s:?}");
        }
    }
}
