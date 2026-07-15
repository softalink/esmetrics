//! Port of the upstream VictoriaMetrics `lib/regexutil` (v1.146.0).
//!
//! Provides regex simplification (`simplify_regex`, `simplify_prom_regex`),
//! "or"-values extraction (`get_or_values_regex`, `get_or_values_prom_regex`)
//! and the optimized matchers [`PromRegex`] and [`Regex`].
//!
//! The Go implementation relies on Go's `regexp/syntax` package (parse tree,
//! `Simplify`, `String()` serialization with flag hoisting). Since the exact
//! parse/factoring/serialization semantics are load-bearing for the
//! simplification results, the required subset of Go `regexp/syntax`
//! (Go 1.22+ behavior) is ported below in the private [`syntax`] module.
//! The general regex fallback matching uses the `regex` crate.
//!
//! Deviations from Go (documented in detail at the deviation sites):
//! - Unicode character classes (`\p{...}`, `\P{...}`) are not ported (they
//!   would require the full Unicode range tables). Such regexps are treated
//!   as "valid but not simplifiable", so matching stays correct via the
//!   `regex`-crate fallback; only the simplification optimization is lost.
//! - `unicode.SimpleFold` is approximated with `char::to_lowercase` /
//!   `char::to_uppercase` single-char orbits (exact for ASCII).
//! - Where Go checks `syntax.Compile`, this port checks that the serialized
//!   regexp compiles with the `regex` crate.

mod syntax {
    //! Minimal port of Go `regexp/syntax` (parser, `Simplify`, `String()`),
    //! restricted to the features reachable through `Flags::PERL | DOT_NL`
    //! minus Unicode character classes.

    use std::collections::HashMap;

    pub const MAX_RUNE: i32 = 0x10FFFF;

    // Flags (port of Go syntax.Flags).
    pub const FOLD_CASE: u16 = 1 << 0;
    pub const CLASS_NL: u16 = 1 << 2;
    pub const DOT_NL: u16 = 1 << 3;
    pub const ONE_LINE: u16 = 1 << 4;
    pub const NON_GREEDY: u16 = 1 << 5;
    pub const PERL_X: u16 = 1 << 6;
    pub const UNICODE_GROUPS: u16 = 1 << 7;
    pub const WAS_DOLLAR: u16 = 1 << 8;
    pub const PERL: u16 = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS;

    /// Port of Go `syntax.Op` (pseudo-ops included, like Go's parser).
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    pub enum Op {
        NoMatch = 1,
        EmptyMatch,
        Literal,
        CharClass,
        AnyCharNotNL,
        AnyChar,
        BeginLine,
        EndLine,
        BeginText,
        EndText,
        WordBoundary,
        NoWordBoundary,
        Capture,
        Star,
        Plus,
        Quest,
        Repeat,
        Concat,
        Alternate,
        // Pseudo-ops for the parsing stack (>= OP_PSEUDO in Go).
        PseudoLeftParen = 128,
        PseudoVerticalBar,
    }

    impl Op {
        fn is_pseudo(self) -> bool {
            self >= Op::PseudoLeftParen
        }
    }

    /// Port of Go `syntax.Regexp` (parse tree node).
    #[derive(Clone, Debug)]
    pub struct Regexp {
        pub op: Op,
        pub flags: u16,
        pub sub: Vec<Regexp>,
        pub rune: Vec<i32>,
        pub min: i32,
        pub max: i32,
        pub cap: i32,
        pub name: String,
    }

    impl Regexp {
        pub fn new(op: Op) -> Regexp {
            Regexp {
                op,
                flags: 0,
                sub: Vec::new(),
                rune: Vec::new(),
                min: 0,
                max: 0,
                cap: 0,
                name: String::new(),
            }
        }

        /// Port of Go `Regexp.Equal`.
        pub fn equal(&self, y: &Regexp) -> bool {
            if self.op != y.op {
                return false;
            }
            match self.op {
                Op::EndText => {
                    if self.flags & WAS_DOLLAR != y.flags & WAS_DOLLAR {
                        return false;
                    }
                }
                Op::Literal | Op::CharClass => {
                    return self.flags & FOLD_CASE == y.flags & FOLD_CASE && self.rune == y.rune;
                }
                Op::Alternate | Op::Concat => {
                    return self.sub.len() == y.sub.len()
                        && self.sub.iter().zip(y.sub.iter()).all(|(a, b)| a.equal(b));
                }
                Op::Star | Op::Plus | Op::Quest => {
                    if self.flags & NON_GREEDY != y.flags & NON_GREEDY
                        || !self.sub[0].equal(&y.sub[0])
                    {
                        return false;
                    }
                }
                Op::Repeat => {
                    if self.flags & NON_GREEDY != y.flags & NON_GREEDY
                        || self.min != y.min
                        || self.max != y.max
                        || !self.sub[0].equal(&y.sub[0])
                    {
                        return false;
                    }
                }
                Op::Capture => {
                    return self.cap == y.cap
                        && self.name == y.name
                        && self.sub[0].equal(&y.sub[0]);
                }
                _ => {}
            }
            true
        }
    }

    /// Parse error codes (port of Go `syntax.ErrorCode`), plus `Unsupported`
    /// for Go-valid constructs this port does not implement (`\p{...}`).
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum ParseError {
        InvalidCharRange,
        InvalidEscape,
        InvalidNamedCapture,
        InvalidPerlOp,
        InvalidRepeatOp,
        InvalidRepeatSize,
        MissingBracket,
        MissingParen,
        MissingRepeatArgument,
        TrailingBackslash,
        UnexpectedParen,
        NestingDepth,
        Large,
        /// Not a Go parse error: the construct is valid in Go but not
        /// supported by this port (Unicode character classes).
        Unsupported,
    }

    pub type Result<T> = std::result::Result<T, ParseError>;

    // --- Case folding -------------------------------------------------------

    // Minimum and maximum runes involved in folding (Go parse.go).
    const MIN_FOLD: i32 = 0x0041;
    const MAX_FOLD: i32 = 0x1e943;

    /// Approximation of Go `unicode.SimpleFold`: returns the next rune in the
    /// case-folding orbit of `r` (cyclic, ascending).
    ///
    /// Deviation from Go: the orbit is computed from `char::to_lowercase` /
    /// `char::to_uppercase` single-char mappings, which misses exotic
    /// multi-member orbits (e.g. 'k'/'K'/KELVIN SIGN). Exact for ASCII.
    /// Maximum meaningful length of a simple-fold orbit walk. Unicode
    /// simple-fold orbits have at most 4 members; walks guarded by this
    /// bound terminate even where [`simple_fold`]'s std-based approximation
    /// produces an orbit that is not a closed cycle (e.g. U+212A KELVIN SIGN
    /// folds to 'k', but neither 'k' nor 'K' folds back to U+212A).
    pub(crate) const MAX_FOLD_ORBIT: usize = 8;

    pub fn simple_fold(r: i32) -> i32 {
        let Some(c) = char::from_u32(r as u32) else {
            return r;
        };
        let mut orbit = [r, r, r];
        let mut n = 1;
        let mut lower = c.to_lowercase();
        if lower.len() == 1 {
            let l = lower.next().unwrap() as i32;
            if !orbit[..n].contains(&l) {
                orbit[n] = l;
                n += 1;
            }
        }
        let mut upper = c.to_uppercase();
        if upper.len() == 1 {
            let u = upper.next().unwrap() as i32;
            if !orbit[..n].contains(&u) {
                orbit[n] = u;
                n += 1;
            }
        }
        if n == 1 {
            return r;
        }
        let orbit = &mut orbit[..n];
        orbit.sort_unstable();
        // Return the smallest rune > r, wrapping around to the minimum.
        for &x in orbit.iter() {
            if x > r {
                return x;
            }
        }
        orbit[0]
    }

    /// Port of Go `minFoldRune`.
    fn min_fold_rune(r: i32) -> i32 {
        if !(MIN_FOLD..=MAX_FOLD).contains(&r) {
            return r;
        }
        let mut m = r;
        let r0 = r;
        let mut r = simple_fold(r);
        let mut steps = 0;
        while r != r0 && steps < MAX_FOLD_ORBIT {
            m = m.min(r);
            r = simple_fold(r);
            steps += 1;
        }
        m
    }

    // --- Character class helpers (ports of Go parse.go helpers) ------------

    /// Port of Go `cleanClass`: sorts ranges, merges abutting/overlapping.
    fn clean_class(r: &mut Vec<i32>) {
        // Sort by lo increasing, hi decreasing to break ties.
        let mut pairs: Vec<(i32, i32)> = r.chunks(2).map(|c| (c[0], c[1])).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        r.clear();
        for (lo, hi) in pairs {
            r.push(lo);
            r.push(hi);
        }
        if r.len() < 4 {
            return;
        }
        let mut w = 2;
        let mut i = 2;
        while i < r.len() {
            let (lo, hi) = (r[i], r[i + 1]);
            if lo <= r[w - 1] + 1 {
                if hi > r[w - 1] {
                    r[w - 1] = hi;
                }
                i += 2;
                continue;
            }
            r[w] = lo;
            r[w + 1] = hi;
            w += 2;
            i += 2;
        }
        r.truncate(w);
    }

    /// Port of Go `inCharClass` (class must be clean).
    fn in_char_class(r: i32, class: &[i32]) -> bool {
        class.chunks(2).any(|c| c[0] <= r && r <= c[1])
    }

    /// Port of Go `appendLiteral`.
    fn append_literal(r: &mut Vec<i32>, x: i32, flags: u16) {
        if flags & FOLD_CASE != 0 {
            append_folded_range(r, x, x);
        } else {
            append_range(r, x, x);
        }
    }

    /// Port of Go `appendRange`.
    fn append_range(r: &mut Vec<i32>, lo: i32, hi: i32) {
        // Expand last range or next-to-last range if it overlaps or abuts.
        let n = r.len();
        for &i in &[2usize, 4usize] {
            if n >= i {
                let (rlo, rhi) = (r[n - i], r[n - i + 1]);
                if lo <= rhi + 1 && rlo <= hi + 1 {
                    if lo < rlo {
                        r[n - i] = lo;
                    }
                    if hi > rhi {
                        r[n - i + 1] = hi;
                    }
                    return;
                }
            }
        }
        r.push(lo);
        r.push(hi);
    }

    /// Port of Go `appendFoldedRange`.
    fn append_folded_range(r: &mut Vec<i32>, mut lo: i32, mut hi: i32) {
        if lo <= MIN_FOLD && hi >= MAX_FOLD {
            append_range(r, lo, hi);
            return;
        }
        if hi < MIN_FOLD || lo > MAX_FOLD {
            append_range(r, lo, hi);
            return;
        }
        if lo < MIN_FOLD {
            append_range(r, lo, MIN_FOLD - 1);
            lo = MIN_FOLD;
        }
        if hi > MAX_FOLD {
            append_range(r, MAX_FOLD + 1, hi);
            hi = MAX_FOLD;
        }
        let mut c = lo;
        while c <= hi {
            append_range(r, c, c);
            let mut f = simple_fold(c);
            let mut steps = 0;
            while f != c && steps < MAX_FOLD_ORBIT {
                append_range(r, f, f);
                f = simple_fold(f);
                steps += 1;
            }
            c += 1;
        }
    }

    /// Port of Go `appendClass` (x must be clean).
    fn append_class(r: &mut Vec<i32>, x: &[i32]) {
        for c in x.chunks(2) {
            append_range(r, c[0], c[1]);
        }
    }

    /// Port of Go `appendFoldedClass`.
    fn append_folded_class(r: &mut Vec<i32>, x: &[i32]) {
        for c in x.chunks(2) {
            append_folded_range(r, c[0], c[1]);
        }
    }

    /// Port of Go `appendNegatedClass` (x must be clean).
    fn append_negated_class(r: &mut Vec<i32>, x: &[i32]) {
        let mut next_lo = 0i32;
        for c in x.chunks(2) {
            let (lo, hi) = (c[0], c[1]);
            if next_lo < lo {
                append_range(r, next_lo, lo - 1);
            }
            next_lo = hi + 1;
        }
        if next_lo <= MAX_RUNE {
            append_range(r, next_lo, MAX_RUNE);
        }
    }

    /// Port of Go `negateClass` (r must be clean).
    fn negate_class(r: &mut Vec<i32>) {
        let mut out: Vec<i32> = Vec::with_capacity(r.len() + 2);
        let mut next_lo = 0i32;
        for c in r.chunks(2) {
            let (lo, hi) = (c[0], c[1]);
            if next_lo < lo {
                out.push(next_lo);
                out.push(lo - 1);
            }
            next_lo = hi + 1;
        }
        if next_lo <= MAX_RUNE {
            out.push(next_lo);
            out.push(MAX_RUNE);
        }
        *r = out;
    }

    // Perl and POSIX character groups (port of Go perl_groups.go).
    struct CharGroup {
        sign: i32,
        class: &'static [i32],
    }

    const CODE_D: &[i32] = &[0x30, 0x39];
    const CODE_S: &[i32] = &[0x9, 0xa, 0xc, 0xd, 0x20, 0x20];
    const CODE_W: &[i32] = &[0x30, 0x39, 0x41, 0x5a, 0x5f, 0x5f, 0x61, 0x7a];

    fn perl_group(name: &str) -> Option<CharGroup> {
        let (sign, class) = match name {
            r"\d" => (1, CODE_D),
            r"\D" => (-1, CODE_D),
            r"\s" => (1, CODE_S),
            r"\S" => (-1, CODE_S),
            r"\w" => (1, CODE_W),
            r"\W" => (-1, CODE_W),
            _ => return None,
        };
        Some(CharGroup { sign, class })
    }

    const CODE_ALNUM: &[i32] = &[0x30, 0x39, 0x41, 0x5a, 0x61, 0x7a];
    const CODE_ALPHA: &[i32] = &[0x41, 0x5a, 0x61, 0x7a];
    const CODE_ASCII: &[i32] = &[0x0, 0x7f];
    const CODE_BLANK: &[i32] = &[0x9, 0x9, 0x20, 0x20];
    const CODE_CNTRL: &[i32] = &[0x0, 0x1f, 0x7f, 0x7f];
    const CODE_DIGIT: &[i32] = &[0x30, 0x39];
    const CODE_GRAPH: &[i32] = &[0x21, 0x7e];
    const CODE_LOWER: &[i32] = &[0x61, 0x7a];
    const CODE_PRINT: &[i32] = &[0x20, 0x7e];
    const CODE_PUNCT: &[i32] = &[0x21, 0x2f, 0x3a, 0x40, 0x5b, 0x60, 0x7b, 0x7e];
    const CODE_SPACE: &[i32] = &[0x9, 0xd, 0x20, 0x20];
    const CODE_UPPER: &[i32] = &[0x41, 0x5a];
    const CODE_WORD: &[i32] = &[0x30, 0x39, 0x41, 0x5a, 0x5f, 0x5f, 0x61, 0x7a];
    const CODE_XDIGIT: &[i32] = &[0x30, 0x39, 0x41, 0x46, 0x61, 0x66];

    fn posix_group(name: &str) -> Option<CharGroup> {
        let (sign, class) = match name {
            "[:alnum:]" => (1, CODE_ALNUM),
            "[:^alnum:]" => (-1, CODE_ALNUM),
            "[:alpha:]" => (1, CODE_ALPHA),
            "[:^alpha:]" => (-1, CODE_ALPHA),
            "[:ascii:]" => (1, CODE_ASCII),
            "[:^ascii:]" => (-1, CODE_ASCII),
            "[:blank:]" => (1, CODE_BLANK),
            "[:^blank:]" => (-1, CODE_BLANK),
            "[:cntrl:]" => (1, CODE_CNTRL),
            "[:^cntrl:]" => (-1, CODE_CNTRL),
            "[:digit:]" => (1, CODE_DIGIT),
            "[:^digit:]" => (-1, CODE_DIGIT),
            "[:graph:]" => (1, CODE_GRAPH),
            "[:^graph:]" => (-1, CODE_GRAPH),
            "[:lower:]" => (1, CODE_LOWER),
            "[:^lower:]" => (-1, CODE_LOWER),
            "[:print:]" => (1, CODE_PRINT),
            "[:^print:]" => (-1, CODE_PRINT),
            "[:punct:]" => (1, CODE_PUNCT),
            "[:^punct:]" => (-1, CODE_PUNCT),
            "[:space:]" => (1, CODE_SPACE),
            "[:^space:]" => (-1, CODE_SPACE),
            "[:upper:]" => (1, CODE_UPPER),
            "[:^upper:]" => (-1, CODE_UPPER),
            "[:word:]" => (1, CODE_WORD),
            "[:^word:]" => (-1, CODE_WORD),
            "[:xdigit:]" => (1, CODE_XDIGIT),
            "[:^xdigit:]" => (-1, CODE_XDIGIT),
            _ => return None,
        };
        Some(CharGroup { sign, class })
    }

    fn append_group(r: &mut Vec<i32>, g: &CharGroup, fold: bool) {
        if !fold {
            if g.sign < 0 {
                append_negated_class(r, g.class);
            } else {
                append_class(r, g.class);
            }
        } else {
            let mut tmp: Vec<i32> = Vec::new();
            append_folded_class(&mut tmp, g.class);
            clean_class(&mut tmp);
            if g.sign < 0 {
                append_negated_class(r, &tmp);
            } else {
                append_class(r, &tmp);
            }
        }
    }

    /// Port of Go `isCharClass`.
    fn is_char_class(re: &Regexp) -> bool {
        (re.op == Op::Literal && re.rune.len() == 1)
            || re.op == Op::CharClass
            || re.op == Op::AnyCharNotNL
            || re.op == Op::AnyChar
    }

    /// Port of Go `matchRune`.
    fn match_rune(re: &Regexp, r: i32) -> bool {
        match re.op {
            Op::Literal => re.rune.len() == 1 && re.rune[0] == r,
            Op::CharClass => in_char_class(r, &re.rune),
            Op::AnyCharNotNL => r != '\n' as i32,
            Op::AnyChar => true,
            _ => false,
        }
    }

    /// Port of Go `mergeCharClass` (caller must ensure `dst.op >= src.op`).
    fn merge_char_class(dst: &mut Regexp, src: &Regexp) {
        match dst.op {
            Op::AnyChar => {}
            Op::AnyCharNotNL => {
                if match_rune(src, '\n' as i32) {
                    dst.op = Op::AnyChar;
                }
            }
            Op::CharClass => {
                if src.op == Op::Literal {
                    append_literal(&mut dst.rune, src.rune[0], src.flags);
                } else {
                    append_class(&mut dst.rune, &src.rune);
                }
            }
            Op::Literal => {
                if src.rune[0] == dst.rune[0] && src.flags == dst.flags {
                    return;
                }
                dst.op = Op::CharClass;
                let (r0, f0) = (dst.rune[0], dst.flags);
                dst.rune.clear();
                append_literal(&mut dst.rune, r0, f0);
                append_literal(&mut dst.rune, src.rune[0], src.flags);
            }
            _ => {}
        }
    }

    /// Port of Go `cleanAlt`.
    fn clean_alt(re: &mut Regexp) {
        if re.op != Op::CharClass {
            return;
        }
        clean_class(&mut re.rune);
        if re.rune.len() == 2 && re.rune[0] == 0 && re.rune[1] == MAX_RUNE {
            re.rune.clear();
            re.op = Op::AnyChar;
            return;
        }
        if re.rune.len() == 4
            && re.rune[0] == 0
            && re.rune[1] == '\n' as i32 - 1
            && re.rune[2] == '\n' as i32 + 1
            && re.rune[3] == MAX_RUNE
        {
            re.rune.clear();
            re.op = Op::AnyCharNotNL;
        }
    }

    /// Port of Go `isalnum`.
    fn isalnum(c: char) -> bool {
        c.is_ascii_digit() || c.is_ascii_uppercase() || c.is_ascii_lowercase()
    }

    fn unhex(c: char) -> i32 {
        match c {
            '0'..='9' => c as i32 - '0' as i32,
            'a'..='f' => c as i32 - 'a' as i32 + 10,
            'A'..='F' => c as i32 - 'A' as i32 + 10,
            _ => -1,
        }
    }

    /// Port of Go `isValidCaptureName`.
    fn is_valid_capture_name(name: &str) -> bool {
        !name.is_empty() && name.chars().all(|c| c == '_' || isalnum(c))
    }

    /// Port of Go `parseEscape`. `s` begins with the backslash.
    fn parse_escape(s: &str) -> Result<(i32, &str)> {
        let t = &s[1..];
        if t.is_empty() {
            return Err(ParseError::TrailingBackslash);
        }
        let c = t.chars().next().unwrap();
        let mut t = &t[c.len_utf8()..];
        match c {
            // Octal escapes.
            '1'..='7' => {
                // Single non-zero digit is a backreference; not supported.
                if t.is_empty() || !(b'0'..=b'7').contains(&t.as_bytes()[0]) {
                    return Err(ParseError::InvalidEscape);
                }
                let mut r = c as i32 - '0' as i32;
                for _ in 1..3 {
                    if t.is_empty() || !(b'0'..=b'7').contains(&t.as_bytes()[0]) {
                        break;
                    }
                    r = r * 8 + (t.as_bytes()[0] - b'0') as i32;
                    t = &t[1..];
                }
                Ok((r, t))
            }
            '0' => {
                let mut r = 0i32;
                for _ in 1..3 {
                    if t.is_empty() || !(b'0'..=b'7').contains(&t.as_bytes()[0]) {
                        break;
                    }
                    r = r * 8 + (t.as_bytes()[0] - b'0') as i32;
                    t = &t[1..];
                }
                Ok((r, t))
            }
            // Hexadecimal escapes.
            'x' => {
                if t.is_empty() {
                    return Err(ParseError::InvalidEscape);
                }
                let c = t.chars().next().unwrap();
                t = &t[c.len_utf8()..];
                if c == '{' {
                    // Any number of hex digits in braces.
                    let mut nhex = 0;
                    let mut r = 0i32;
                    loop {
                        if t.is_empty() {
                            return Err(ParseError::InvalidEscape);
                        }
                        let c = t.chars().next().unwrap();
                        t = &t[c.len_utf8()..];
                        if c == '}' {
                            break;
                        }
                        let v = unhex(c);
                        if v < 0 {
                            return Err(ParseError::InvalidEscape);
                        }
                        r = r * 16 + v;
                        if r > MAX_RUNE {
                            return Err(ParseError::InvalidEscape);
                        }
                        nhex += 1;
                    }
                    if nhex == 0 {
                        return Err(ParseError::InvalidEscape);
                    }
                    return Ok((r, t));
                }
                // Easy case: two hex digits.
                let x = unhex(c);
                if t.is_empty() {
                    return Err(ParseError::InvalidEscape);
                }
                let c2 = t.chars().next().unwrap();
                t = &t[c2.len_utf8()..];
                let y = unhex(c2);
                if x < 0 || y < 0 {
                    return Err(ParseError::InvalidEscape);
                }
                Ok((x * 16 + y, t))
            }
            // C escapes.
            'a' => Ok((0x07, t)),
            'f' => Ok((0x0C, t)),
            'n' => Ok(('\n' as i32, t)),
            'r' => Ok(('\r' as i32, t)),
            't' => Ok(('\t' as i32, t)),
            'v' => Ok((0x0B, t)),
            _ => {
                if (c as u32) < 0x80 && !isalnum(c) {
                    // Escaped non-word characters are always themselves.
                    return Ok((c as i32, t));
                }
                Err(ParseError::InvalidEscape)
            }
        }
    }

    /// Port of Go `parseClassChar`.
    fn parse_class_char(s: &str) -> Result<(i32, &str)> {
        if s.is_empty() {
            return Err(ParseError::MissingBracket);
        }
        if s.as_bytes()[0] == b'\\' {
            return parse_escape(s);
        }
        let c = s.chars().next().unwrap();
        Ok((c as i32, &s[c.len_utf8()..]))
    }

    // Limits (port of Go parse.go constants).
    const MAX_HEIGHT: i64 = 1000;
    const INST_SIZE: i64 = 5 * 8;
    const MAX_SIZE: i64 = (128 << 20) / INST_SIZE;
    const RUNE_SIZE: i64 = 4;
    const MAX_RUNES: i64 = (128 << 20) / RUNE_SIZE;

    /// Port of Go `repeatIsValid`.
    fn repeat_is_valid(re: &Regexp, n: i32) -> bool {
        let mut n = n;
        if re.op == Op::Repeat {
            let mut m = re.max;
            if m == 0 {
                return true;
            }
            if m < 0 {
                m = re.min;
            }
            if m > n {
                return false;
            }
            if m > 0 {
                n /= m;
            }
        }
        re.sub.iter().all(|sub| repeat_is_valid(sub, n))
    }

    /// Port of Go `(*parser).calcSize` (non-memoized; see deviation note on
    /// `Parser` below).
    fn calc_size(re: &Regexp) -> i64 {
        let size: i64 = match re.op {
            Op::Literal => re.rune.len() as i64,
            Op::Capture | Op::Star => 2 + calc_size(&re.sub[0]),
            Op::Plus | Op::Quest => 1 + calc_size(&re.sub[0]),
            Op::Concat => re.sub.iter().map(calc_size).sum(),
            Op::Alternate => {
                let mut s: i64 = re.sub.iter().map(calc_size).sum();
                if re.sub.len() > 1 {
                    s += re.sub.len() as i64 - 1;
                }
                s
            }
            Op::Repeat => {
                let sub = calc_size(&re.sub[0]);
                if re.max == -1 {
                    if re.min == 0 {
                        2 + sub
                    } else {
                        1 + re.min as i64 * sub
                    }
                } else {
                    re.max as i64 * sub + (re.max - re.min) as i64
                }
            }
            _ => 0,
        };
        size.max(1)
    }

    fn calc_height(re: &Regexp) -> i64 {
        1 + re.sub.iter().map(calc_height).max().unwrap_or(0)
    }

    /// Port of Go `(*parser)`.
    ///
    /// Deviations from Go: no free-list node reuse (so `num_regexp` counts
    /// every allocation, which can only make the limit checks kick in
    /// earlier, never change their outcome), and the height/size checks are
    /// recomputed instead of memoized in maps keyed by node pointers
    /// (identical results, worse worst-case complexity).
    struct Parser {
        flags: u16,
        stack: Vec<Regexp>,
        num_cap: i32,
        num_regexp: i64,
        num_runes: i64,
        repeats: i64,
    }

    impl Parser {
        fn new_regexp(&mut self, op: Op) -> Regexp {
            self.num_regexp += 1;
            Regexp::new(op)
        }

        /// Port of Go `(*parser).checkLimits`.
        fn check_limits(&mut self, re: &Regexp) -> Result<()> {
            if self.num_runes > MAX_RUNES {
                return Err(ParseError::Large);
            }
            self.check_size(re)?;
            self.check_height(re)
        }

        /// Port of Go `(*parser).checkSize`.
        fn check_size(&mut self, re: &Regexp) -> Result<()> {
            if self.repeats == 0 {
                self.repeats = 1;
            }
            if re.op == Op::Repeat {
                let mut n = re.max;
                if n == -1 {
                    n = re.min;
                }
                if n <= 0 {
                    n = 1;
                }
                if n as i64 > MAX_SIZE / self.repeats {
                    self.repeats = MAX_SIZE;
                } else {
                    self.repeats *= n as i64;
                }
            }
            if self.num_regexp < MAX_SIZE / self.repeats {
                return Ok(());
            }
            if calc_size(re) > MAX_SIZE {
                return Err(ParseError::Large);
            }
            Ok(())
        }

        /// Port of Go `(*parser).checkHeight`.
        fn check_height(&mut self, re: &Regexp) -> Result<()> {
            if self.num_regexp < MAX_HEIGHT {
                return Ok(());
            }
            if calc_height(re) > MAX_HEIGHT {
                return Err(ParseError::NestingDepth);
            }
            Ok(())
        }

        /// Port of Go `(*parser).maybeConcat`.
        fn maybe_concat(&mut self, r: i32, flags: u16) -> bool {
            let n = self.stack.len();
            if n < 2 {
                return false;
            }
            {
                let re1 = &self.stack[n - 1];
                let re2 = &self.stack[n - 2];
                if re1.op != Op::Literal
                    || re2.op != Op::Literal
                    || re1.flags & FOLD_CASE != re2.flags & FOLD_CASE
                {
                    return false;
                }
            }
            // Push re1 into re2.
            let runes = std::mem::take(&mut self.stack[n - 1].rune);
            self.stack[n - 2].rune.extend(runes);
            if r >= 0 {
                let re1 = &mut self.stack[n - 1];
                re1.rune.push(r);
                re1.flags = flags;
                return true;
            }
            self.stack.pop();
            false
        }

        /// Port of Go `(*parser).literal`.
        fn literal(&mut self, mut r: i32) -> Result<()> {
            let mut re = self.new_regexp(Op::Literal);
            re.flags = self.flags;
            if self.flags & FOLD_CASE != 0 {
                r = min_fold_rune(r);
            }
            re.rune.push(r);
            self.push(re)
        }

        /// Port of Go `(*parser).push`.
        fn push(&mut self, mut re: Regexp) -> Result<()> {
            self.num_runes += re.rune.len() as i64;
            if re.op == Op::CharClass && re.rune.len() == 2 && re.rune[0] == re.rune[1] {
                // Single rune.
                if self.maybe_concat(re.rune[0], self.flags & !FOLD_CASE) {
                    return Ok(());
                }
                re.op = Op::Literal;
                re.rune.truncate(1);
                re.flags = self.flags & !FOLD_CASE;
            } else if (re.op == Op::CharClass
                && re.rune.len() == 4
                && re.rune[0] == re.rune[1]
                && re.rune[2] == re.rune[3]
                && simple_fold(re.rune[0]) == re.rune[2]
                && simple_fold(re.rune[2]) == re.rune[0])
                || (re.op == Op::CharClass
                    && re.rune.len() == 2
                    && re.rune[0] + 1 == re.rune[1]
                    && simple_fold(re.rune[0]) == re.rune[1]
                    && simple_fold(re.rune[1]) == re.rune[0])
            {
                // Case-insensitive rune like [Aa] or [Δδ].
                if self.maybe_concat(re.rune[0], self.flags | FOLD_CASE) {
                    return Ok(());
                }
                re.op = Op::Literal;
                re.rune.truncate(1);
                re.flags = self.flags | FOLD_CASE;
            } else {
                // Incremental concatenation.
                self.maybe_concat(-1, 0);
            }
            self.check_limits(&re)?;
            self.stack.push(re);
            Ok(())
        }

        /// Port of Go `(*parser).op`, with optional extra flags for the node.
        fn op_push(&mut self, op: Op, extra_flags: u16) -> Result<()> {
            let mut re = self.new_regexp(op);
            re.flags = self.flags | extra_flags;
            self.push(re)
        }

        /// Port of Go `(*parser).repeat`.
        fn repeat<'a>(
            &mut self,
            op: Op,
            min: i32,
            max: i32,
            mut after: &'a str,
            last_repeat: bool,
        ) -> Result<&'a str> {
            let mut flags = self.flags;
            if self.flags & PERL_X != 0 {
                if !after.is_empty() && after.as_bytes()[0] == b'?' {
                    after = &after[1..];
                    flags ^= NON_GREEDY;
                }
                if last_repeat {
                    // In Perl it is not allowed to stack repetition operators.
                    return Err(ParseError::InvalidRepeatOp);
                }
            }
            let n = self.stack.len();
            if n == 0 || self.stack[n - 1].op.is_pseudo() {
                return Err(ParseError::MissingRepeatArgument);
            }
            let sub = self.stack.pop().unwrap();
            let mut re = self.new_regexp(op);
            re.min = min;
            re.max = max;
            re.flags = flags;
            re.sub.push(sub);
            self.check_limits(&re)?;
            if op == Op::Repeat && (min >= 2 || max >= 2) && !repeat_is_valid(&re, 1000) {
                return Err(ParseError::InvalidRepeatSize);
            }
            self.stack.push(re);
            Ok(after)
        }

        /// Port of Go `(*parser).concat`.
        fn concat(&mut self) -> Result<()> {
            self.maybe_concat(-1, 0);
            // Scan down to find pseudo-operator | or (.
            let i = self
                .stack
                .iter()
                .rposition(|re| re.op.is_pseudo())
                .map_or(0, |p| p + 1);
            let subs = self.stack.split_off(i);
            if subs.is_empty() {
                let re = self.new_regexp(Op::EmptyMatch);
                return self.push(re);
            }
            let re = self.collapse(subs, Op::Concat)?;
            self.push(re)
        }

        /// Port of Go `(*parser).alternate`.
        fn alternate(&mut self) -> Result<()> {
            // Scan down to find pseudo-operator (. There are no | above (.
            let i = self
                .stack
                .iter()
                .rposition(|re| re.op.is_pseudo())
                .map_or(0, |p| p + 1);
            let mut subs = self.stack.split_off(i);
            // Make sure top class is clean.
            if let Some(last) = subs.last_mut() {
                clean_alt(last);
            }
            if subs.is_empty() {
                let re = self.new_regexp(Op::NoMatch);
                return self.push(re);
            }
            let re = self.collapse(subs, Op::Alternate)?;
            self.push(re)
        }

        /// Port of Go `(*parser).collapse`.
        fn collapse(&mut self, mut subs: Vec<Regexp>, op: Op) -> Result<Regexp> {
            if subs.len() == 1 {
                return Ok(subs.pop().unwrap());
            }
            let mut re = self.new_regexp(op);
            for sub in subs {
                if sub.op == op {
                    re.sub.extend(sub.sub);
                } else {
                    re.sub.push(sub);
                }
            }
            if op == Op::Alternate {
                let subs = std::mem::take(&mut re.sub);
                re.sub = self.factor(subs)?;
                if re.sub.len() == 1 {
                    return Ok(re.sub.pop().unwrap());
                }
            }
            Ok(re)
        }

        /// Port of Go `(*parser).factor` (all 4 rounds).
        fn factor(&mut self, subs: Vec<Regexp>) -> Result<Vec<Regexp>> {
            if subs.len() < 2 {
                return Ok(subs);
            }

            // Round 1: Factor out common literal prefixes.
            let mut sub: Vec<Option<Regexp>> = subs.into_iter().map(Some).collect();
            let mut out: Vec<Regexp> = Vec::new();
            let mut str_: Vec<i32> = Vec::new();
            let mut strflags: u16 = 0;
            let mut start = 0usize;
            for i in 0..=sub.len() {
                let mut istr: Vec<i32> = Vec::new();
                let mut iflags: u16 = 0;
                if i < sub.len() {
                    let (s, f) = leading_string(sub[i].as_ref().unwrap());
                    istr = s.to_vec();
                    iflags = f;
                    if iflags == strflags {
                        let mut same = 0;
                        while same < str_.len() && same < istr.len() && str_[same] == istr[same] {
                            same += 1;
                        }
                        if same > 0 {
                            // Matches at least one rune in current range.
                            str_.truncate(same);
                            continue;
                        }
                    }
                }
                if i == start {
                    // Nothing to do - run of length 0.
                } else if i == start + 1 {
                    // Just one: don't bother factoring.
                    out.push(sub[start].take().unwrap());
                } else {
                    // Construct factored form: prefix(suffix1|suffix2|...)
                    let mut prefix = self.new_regexp(Op::Literal);
                    prefix.flags = strflags;
                    prefix.rune = str_.clone();
                    let mut run: Vec<Regexp> = Vec::with_capacity(i - start);
                    for slot in sub[start..i].iter_mut() {
                        let s = remove_leading_string(slot.take().unwrap(), str_.len());
                        self.check_limits(&s)?;
                        run.push(s);
                    }
                    let suffix = self.collapse(run, Op::Alternate)?;
                    let mut re = self.new_regexp(Op::Concat);
                    re.sub.push(prefix);
                    re.sub.push(suffix);
                    out.push(re);
                }
                start = i;
                str_ = istr;
                strflags = iflags;
            }

            // Round 2: Factor out common simple prefixes.
            let mut sub: Vec<Option<Regexp>> = out.into_iter().map(Some).collect();
            let mut out: Vec<Regexp> = Vec::new();
            let mut start = 0usize;
            let mut first: Option<Regexp> = None;
            for i in 0..=sub.len() {
                let mut ifirst: Option<Regexp> = None;
                if i < sub.len() {
                    ifirst = leading_regexp(sub[i].as_ref().unwrap()).cloned();
                    if let (Some(f), Some(inf)) = (&first, &ifirst) {
                        // first must be a character class OR a fixed repeat
                        // of a character class.
                        if f.equal(inf)
                            && (is_char_class(f)
                                || (f.op == Op::Repeat
                                    && f.min == f.max
                                    && is_char_class(&f.sub[0])))
                        {
                            continue;
                        }
                    }
                }
                if i == start {
                    // Nothing to do - run of length 0.
                } else if i == start + 1 {
                    out.push(sub[start].take().unwrap());
                } else {
                    let mut prefix: Option<Regexp> = None;
                    let mut run: Vec<Regexp> = Vec::with_capacity(i - start);
                    for (j, slot) in sub[start..i].iter_mut().enumerate() {
                        let (lead, rest) = remove_leading_regexp(slot.take().unwrap());
                        if j == 0 {
                            prefix = Some(lead);
                        }
                        self.check_limits(&rest)?;
                        run.push(rest);
                    }
                    let suffix = self.collapse(run, Op::Alternate)?;
                    let mut re = self.new_regexp(Op::Concat);
                    re.sub
                        .push(prefix.expect("run leader must have a leading regexp"));
                    re.sub.push(suffix);
                    out.push(re);
                }
                start = i;
                first = ifirst;
            }

            // Round 3: Collapse runs of single literals into character classes.
            let mut sub: Vec<Option<Regexp>> = out.into_iter().map(Some).collect();
            let mut out: Vec<Regexp> = Vec::new();
            let mut start = 0usize;
            for i in 0..=sub.len() {
                if i < sub.len() && is_char_class(sub[i].as_ref().unwrap()) {
                    continue;
                }
                if i == start {
                    // Nothing to do - run of length 0.
                } else if i == start + 1 {
                    out.push(sub[start].take().unwrap());
                } else {
                    // Make new char class. Start with most complex regexp.
                    let mut maxj = start;
                    for j in start + 1..i {
                        let a = sub[maxj].as_ref().unwrap();
                        let b = sub[j].as_ref().unwrap();
                        if a.op < b.op || (a.op == b.op && a.rune.len() < b.rune.len()) {
                            maxj = j;
                        }
                    }
                    sub.swap(start, maxj);
                    let mut dst = sub[start].take().unwrap();
                    for slot in sub[start + 1..i].iter_mut() {
                        let src = slot.take().unwrap();
                        merge_char_class(&mut dst, &src);
                    }
                    clean_alt(&mut dst);
                    out.push(dst);
                }
                if i < sub.len() {
                    out.push(sub[i].take().unwrap());
                }
                start = i + 1;
            }

            // Round 4: Collapse runs of empty matches into a single empty match.
            let sub = out;
            let mut out: Vec<Regexp> = Vec::new();
            for (i, s) in sub.iter().enumerate() {
                if i + 1 < sub.len() && s.op == Op::EmptyMatch && sub[i + 1].op == Op::EmptyMatch {
                    continue;
                }
                out.push(s.clone());
            }
            Ok(out)
        }

        /// Port of Go `(*parser).parseVerticalBar`.
        fn parse_vertical_bar(&mut self) -> Result<()> {
            self.concat()?;
            if !self.swap_vertical_bar() {
                self.op_push(Op::PseudoVerticalBar, 0)?;
            }
            Ok(())
        }

        /// Port of Go `(*parser).swapVerticalBar`.
        fn swap_vertical_bar(&mut self) -> bool {
            // If above and below vertical bar are literal or char class,
            // can merge into a single char class.
            let n = self.stack.len();
            if n >= 3
                && self.stack[n - 2].op == Op::PseudoVerticalBar
                && is_char_class(&self.stack[n - 1])
                && is_char_class(&self.stack[n - 3])
            {
                let mut src = self.stack.pop().unwrap();
                // Make stack[n-3] the more complex of the two.
                if src.op > self.stack[n - 3].op {
                    std::mem::swap(&mut src, &mut self.stack[n - 3]);
                }
                merge_char_class(&mut self.stack[n - 3], &src);
                return true;
            }
            if n >= 2 && self.stack[n - 2].op == Op::PseudoVerticalBar {
                if n >= 3 {
                    // Now out of reach. Clean opportunistically.
                    clean_alt(&mut self.stack[n - 3]);
                }
                self.stack.swap(n - 1, n - 2);
                return true;
            }
            false
        }

        /// Port of Go `(*parser).parseRightParen`.
        fn parse_right_paren(&mut self) -> Result<()> {
            self.concat()?;
            if self.swap_vertical_bar() {
                // pop vertical bar
                self.stack.pop();
            }
            self.alternate()?;
            if self.stack.len() < 2 {
                return Err(ParseError::UnexpectedParen);
            }
            let re1 = self.stack.pop().unwrap();
            let mut re2 = self.stack.pop().unwrap();
            if re2.op != Op::PseudoLeftParen {
                return Err(ParseError::UnexpectedParen);
            }
            // Restore flags at time of paren.
            self.flags = re2.flags;
            if re2.cap == 0 {
                // Just for grouping.
                self.push(re1)
            } else {
                re2.op = Op::Capture;
                re2.sub = vec![re1];
                self.push(re2)
            }
        }

        /// Port of Go `(*parser).parsePerlFlags`.
        fn parse_perl_flags<'a>(&mut self, s: &'a str) -> Result<&'a str> {
            let t = s;
            let b = t.as_bytes();
            let starts_with_p = t.len() > 4 && b[2] == b'P' && b[3] == b'<';
            let starts_with_name = t.len() > 3 && b[2] == b'<';
            if starts_with_p || starts_with_name {
                let expr_start = if starts_with_name && !starts_with_p {
                    3
                } else {
                    4
                };
                // Pull out name.
                let Some(end) = t.find('>') else {
                    return Err(ParseError::InvalidNamedCapture);
                };
                let name = &t[expr_start..end];
                if !is_valid_capture_name(name) {
                    return Err(ParseError::InvalidNamedCapture);
                }
                // Like ordinary capture, but named.
                self.num_cap += 1;
                let mut re = self.new_regexp(Op::PseudoLeftParen);
                re.flags = self.flags;
                re.cap = self.num_cap;
                re.name = name.to_string();
                self.push(re)?;
                return Ok(&t[end + 1..]);
            }

            // Non-capturing group. Might also twiddle Perl flags.
            let mut t = &t[2..]; // skip (?
            let mut flags = self.flags;
            let mut sign = 1i32;
            let mut saw_flag = false;
            while !t.is_empty() {
                let c = t.chars().next().unwrap();
                t = &t[c.len_utf8()..];
                match c {
                    'i' => {
                        flags |= FOLD_CASE;
                        saw_flag = true;
                    }
                    'm' => {
                        flags &= !ONE_LINE;
                        saw_flag = true;
                    }
                    's' => {
                        flags |= DOT_NL;
                        saw_flag = true;
                    }
                    'U' => {
                        flags |= NON_GREEDY;
                        saw_flag = true;
                    }
                    '-' => {
                        if sign < 0 {
                            break;
                        }
                        sign = -1;
                        // Invert flags so that | above turn into &^ and vice
                        // versa. We'll invert flags again before using it.
                        flags = !flags;
                        saw_flag = false;
                    }
                    ':' | ')' => {
                        if sign < 0 {
                            if !saw_flag {
                                break;
                            }
                            flags = !flags;
                        }
                        if c == ':' {
                            // Open new group
                            self.op_push(Op::PseudoLeftParen, 0)?;
                        }
                        self.flags = flags;
                        return Ok(t);
                    }
                    _ => break,
                }
            }
            Err(ParseError::InvalidPerlOp)
        }

        /// Port of Go `(*parser).parseInt`.
        fn parse_int<'a>(&self, s: &'a str) -> Option<(i32, &'a str)> {
            if s.is_empty() || !s.as_bytes()[0].is_ascii_digit() {
                return None;
            }
            // Disallow leading zeros.
            if s.len() >= 2 && s.as_bytes()[0] == b'0' && s.as_bytes()[1].is_ascii_digit() {
                return None;
            }
            let mut rest = s;
            while !rest.is_empty() && rest.as_bytes()[0].is_ascii_digit() {
                rest = &rest[1..];
            }
            let digits = &s[..s.len() - rest.len()];
            let mut n = 0i32;
            for &d in digits.as_bytes() {
                // Avoid overflow.
                if n >= 100_000_000 {
                    n = -1;
                    break;
                }
                n = n * 10 + (d - b'0') as i32;
            }
            Some((n, rest))
        }

        /// Port of Go `(*parser).parseRepeat`.
        /// Returns `None` if s is not of the form `{min[,[max]]}` (the `{`
        /// is then a literal).
        fn parse_repeat<'a>(&self, s: &'a str) -> Option<(i32, i32, &'a str)> {
            if s.is_empty() || s.as_bytes()[0] != b'{' {
                return None;
            }
            let s = &s[1..];
            let (mut min, mut s) = self.parse_int(s)?;
            if s.is_empty() {
                return None;
            }
            let max;
            if s.as_bytes()[0] != b',' {
                max = min;
            } else {
                s = &s[1..];
                if s.is_empty() {
                    return None;
                }
                if s.as_bytes()[0] == b'}' {
                    max = -1;
                } else {
                    let (m, rest) = self.parse_int(s)?;
                    s = rest;
                    max = m;
                    if max < 0 {
                        // parse_int found too big a number
                        min = -1;
                    }
                }
            }
            if s.is_empty() || s.as_bytes()[0] != b'}' {
                return None;
            }
            Some((min, max, &s[1..]))
        }

        /// Port of Go `parsePerlClassEscape`.
        fn parse_perl_class_escape<'a>(&self, s: &'a str) -> Option<(CharGroup, &'a str)> {
            if self.flags & PERL_X == 0 || s.len() < 2 || s.as_bytes()[0] != b'\\' {
                return None;
            }
            let b1 = s.as_bytes()[1];
            if !b1.is_ascii() {
                return None;
            }
            let key = &s[..2];
            let g = perl_group(key)?;
            Some((g, &s[2..]))
        }

        /// Port of Go `parseNamedClass`. Appends to `class` on success and
        /// returns the remaining string; returns `Ok(None)` when `s` does not
        /// contain a named class (fall through to normal parsing).
        fn parse_named_class<'a>(
            &self,
            s: &'a str,
            class: &mut Vec<i32>,
        ) -> Result<Option<&'a str>> {
            if s.len() < 2 || s.as_bytes()[0] != b'[' || s.as_bytes()[1] != b':' {
                return Ok(None);
            }
            let Some(i) = s[2..].find(":]") else {
                return Ok(None);
            };
            let i = i + 2;
            let name = &s[..i + 2];
            let rest = &s[i + 2..];
            let Some(g) = posix_group(name) else {
                return Err(ParseError::InvalidCharRange);
            };
            append_group(class, &g, self.flags & FOLD_CASE != 0);
            Ok(Some(rest))
        }

        /// Port of the `\p{...}` detection. Go's `parseUnicodeClass` needs
        /// the full Unicode range tables; this port reports `Unsupported`
        /// instead (callers treat the regexp as valid but unsimplifiable).
        fn check_unicode_class(&self, s: &str) -> Result<()> {
            if self.flags & UNICODE_GROUPS != 0
                && s.len() >= 2
                && s.as_bytes()[0] == b'\\'
                && (s.as_bytes()[1] == b'p' || s.as_bytes()[1] == b'P')
            {
                return Err(ParseError::Unsupported);
            }
            Ok(())
        }

        /// Port of Go `(*parser).parseClass`.
        fn parse_class<'a>(&mut self, s: &'a str) -> Result<&'a str> {
            let mut t = &s[1..]; // chop [
            let mut re = self.new_regexp(Op::CharClass);
            re.flags = self.flags;

            let mut sign = 1i32;
            if !t.is_empty() && t.as_bytes()[0] == b'^' {
                sign = -1;
                t = &t[1..];
                // If character class does not match \n, add it here, so that
                // negation later will do the right thing.
                if self.flags & CLASS_NL == 0 {
                    re.rune.push('\n' as i32);
                    re.rune.push('\n' as i32);
                }
            }

            let mut class = std::mem::take(&mut re.rune);
            let mut first = true; // ] and - are okay as first char in class
            while t.is_empty() || t.as_bytes()[0] != b']' || first {
                // POSIX: - is only okay unescaped as first or last in class.
                // Perl: - is okay anywhere.
                if !t.is_empty()
                    && t.as_bytes()[0] == b'-'
                    && self.flags & PERL_X == 0
                    && !first
                    && (t.len() == 1 || t.as_bytes()[1] != b']')
                {
                    return Err(ParseError::InvalidCharRange);
                }
                first = false;

                // Look for POSIX [:alnum:] etc.
                if t.len() > 2 && t.as_bytes()[0] == b'[' && t.as_bytes()[1] == b':' {
                    if let Some(nt) = self.parse_named_class(t, &mut class)? {
                        t = nt;
                        continue;
                    }
                }

                // Look for Unicode character group like \p{Han}.
                self.check_unicode_class(t)?;

                // Look for Perl character class symbols (extension).
                if let Some((g, nt)) = self.parse_perl_class_escape(t) {
                    append_group(&mut class, &g, self.flags & FOLD_CASE != 0);
                    t = nt;
                    continue;
                }

                // Single character or simple range.
                let (lo, nt) = parse_class_char(t)?;
                t = nt;
                let mut hi = lo;
                // [a-] means (a|-) so check for final ].
                if t.len() >= 2 && t.as_bytes()[0] == b'-' && t.as_bytes()[1] != b']' {
                    t = &t[1..];
                    let (h, nt) = parse_class_char(t)?;
                    t = nt;
                    hi = h;
                    if hi < lo {
                        return Err(ParseError::InvalidCharRange);
                    }
                }
                if self.flags & FOLD_CASE == 0 {
                    append_range(&mut class, lo, hi);
                } else {
                    append_folded_range(&mut class, lo, hi);
                }
            }
            t = &t[1..]; // chop ]

            clean_class(&mut class);
            if sign < 0 {
                negate_class(&mut class);
            }
            re.rune = class;
            self.push(re)?;
            Ok(t)
        }

        /// Handles the `\` cases of the main parse loop.
        fn parse_backslash<'a>(&mut self, t: &'a str) -> Result<&'a str> {
            if self.flags & PERL_X != 0 && t.len() >= 2 {
                match t.as_bytes()[1] {
                    b'A' => {
                        self.op_push(Op::BeginText, 0)?;
                        return Ok(&t[2..]);
                    }
                    b'b' => {
                        self.op_push(Op::WordBoundary, 0)?;
                        return Ok(&t[2..]);
                    }
                    b'B' => {
                        self.op_push(Op::NoWordBoundary, 0)?;
                        return Ok(&t[2..]);
                    }
                    b'C' => {
                        // any byte; not supported
                        return Err(ParseError::InvalidEscape);
                    }
                    b'Q' => {
                        // \Q ... \E: the ... is always literals
                        let rest = &t[2..];
                        let (lit, after) = match rest.find("\\E") {
                            Some(i) => (&rest[..i], &rest[i + 2..]),
                            None => (rest, ""),
                        };
                        for c in lit.chars() {
                            self.literal(c as i32)?;
                        }
                        return Ok(after);
                    }
                    b'z' => {
                        self.op_push(Op::EndText, 0)?;
                        return Ok(&t[2..]);
                    }
                    _ => {}
                }
            }

            // Look for Unicode character group like \p{Han} (unsupported).
            self.check_unicode_class(t)?;

            // Perl character class escape.
            if let Some((g, rest)) = self.parse_perl_class_escape(t) {
                let mut re = self.new_regexp(Op::CharClass);
                re.flags = self.flags;
                append_group(&mut re.rune, &g, self.flags & FOLD_CASE != 0);
                self.push(re)?;
                return Ok(rest);
            }

            // Ordinary single-character escape.
            let (c, rest) = parse_escape(t)?;
            self.literal(c)?;
            Ok(rest)
        }
    }

    /// Port of Go `leadingString`.
    fn leading_string(re: &Regexp) -> (&[i32], u16) {
        let mut re = re;
        if re.op == Op::Concat && !re.sub.is_empty() {
            re = &re.sub[0];
        }
        if re.op != Op::Literal {
            return (&[], 0);
        }
        (&re.rune, re.flags & FOLD_CASE)
    }

    /// Port of Go `removeLeadingString`.
    fn remove_leading_string(mut re: Regexp, n: usize) -> Regexp {
        if re.op == Op::Concat && !re.sub.is_empty() {
            // Removing a leading string in a concatenation
            // might simplify the concatenation.
            let sub0 = remove_leading_string(re.sub.remove(0), n);
            if sub0.op == Op::EmptyMatch {
                return match re.sub.len() {
                    0 => {
                        // Impossible but handle.
                        re.op = Op::EmptyMatch;
                        re.sub.clear();
                        re
                    }
                    1 => re.sub.pop().unwrap(),
                    _ => re,
                };
            }
            re.sub.insert(0, sub0);
            return re;
        }
        if re.op == Op::Literal {
            re.rune.drain(..n.min(re.rune.len()));
            if re.rune.is_empty() {
                re.op = Op::EmptyMatch;
            }
        }
        re
    }

    /// Port of Go `leadingRegexp`.
    fn leading_regexp(re: &Regexp) -> Option<&Regexp> {
        if re.op == Op::EmptyMatch {
            return None;
        }
        if re.op == Op::Concat && !re.sub.is_empty() {
            let sub = &re.sub[0];
            if sub.op == Op::EmptyMatch {
                return None;
            }
            return Some(sub);
        }
        Some(re)
    }

    /// Port of Go `removeLeadingRegexp`; returns `(removed, rest)`.
    fn remove_leading_regexp(mut re: Regexp) -> (Regexp, Regexp) {
        if re.op == Op::Concat && !re.sub.is_empty() {
            let removed = re.sub.remove(0);
            let rest = match re.sub.len() {
                0 => Regexp::new(Op::EmptyMatch),
                1 => re.sub.pop().unwrap(),
                _ => re,
            };
            return (removed, rest);
        }
        (re, Regexp::new(Op::EmptyMatch))
    }

    /// Port of Go `syntax.Parse` (with `Flags` semantics; no `Literal` mode).
    pub fn parse(s: &str, flags: u16) -> Result<Regexp> {
        let mut p = Parser {
            flags,
            stack: Vec::new(),
            num_cap: 0,
            num_regexp: 0,
            num_runes: 0,
            repeats: 0,
        };
        let mut t = s;
        let mut last_repeat = false;
        while !t.is_empty() {
            let mut repeat = false;
            match t.as_bytes()[0] {
                b'(' => {
                    if p.flags & PERL_X != 0 && t.len() >= 2 && t.as_bytes()[1] == b'?' {
                        // Flag changes and non-capturing groups.
                        t = p.parse_perl_flags(t)?;
                    } else {
                        p.num_cap += 1;
                        let cap = p.num_cap;
                        let mut re = p.new_regexp(Op::PseudoLeftParen);
                        re.flags = p.flags;
                        re.cap = cap;
                        p.push(re)?;
                        t = &t[1..];
                    }
                }
                b'|' => {
                    p.parse_vertical_bar()?;
                    t = &t[1..];
                }
                b')' => {
                    p.parse_right_paren()?;
                    t = &t[1..];
                }
                b'^' => {
                    if p.flags & ONE_LINE != 0 {
                        p.op_push(Op::BeginText, 0)?;
                    } else {
                        p.op_push(Op::BeginLine, 0)?;
                    }
                    t = &t[1..];
                }
                b'$' => {
                    if p.flags & ONE_LINE != 0 {
                        p.op_push(Op::EndText, WAS_DOLLAR)?;
                    } else {
                        p.op_push(Op::EndLine, 0)?;
                    }
                    t = &t[1..];
                }
                b'.' => {
                    if p.flags & DOT_NL != 0 {
                        p.op_push(Op::AnyChar, 0)?;
                    } else {
                        p.op_push(Op::AnyCharNotNL, 0)?;
                    }
                    t = &t[1..];
                }
                b'[' => {
                    t = p.parse_class(t)?;
                }
                op_byte @ (b'*' | b'+' | b'?') => {
                    let op = match op_byte {
                        b'*' => Op::Star,
                        b'+' => Op::Plus,
                        _ => Op::Quest,
                    };
                    t = p.repeat(op, 0, 0, &t[1..], last_repeat)?;
                    repeat = true;
                }
                b'{' => match p.parse_repeat(t) {
                    None => {
                        // If the repeat cannot be parsed, { is a literal.
                        p.literal('{' as i32)?;
                        t = &t[1..];
                    }
                    Some((min, max, rest)) => {
                        if !(0..=1000).contains(&min) || max > 1000 || (max >= 0 && min > max) {
                            // Numbers were too big, or max is present and
                            // min > max.
                            return Err(ParseError::InvalidRepeatSize);
                        }
                        t = p.repeat(Op::Repeat, min, max, rest, last_repeat)?;
                        repeat = true;
                    }
                },
                b'\\' => {
                    t = p.parse_backslash(t)?;
                }
                _ => {
                    let c = t.chars().next().unwrap();
                    t = &t[c.len_utf8()..];
                    p.literal(c as i32)?;
                }
            }
            last_repeat = repeat;
        }

        p.concat()?;
        if p.swap_vertical_bar() {
            // pop vertical bar
            p.stack.pop();
        }
        p.alternate()?;
        if p.stack.len() != 1 {
            return Err(ParseError::MissingParen);
        }
        Ok(p.stack.pop().unwrap())
    }

    // --- Simplify (port of Go simplify.go) ----------------------------------

    /// Port of Go `Regexp.Simplify`.
    ///
    /// Deviation from Go: Go shares subtree pointers in the expansion of
    /// counted repetitions; this port deep-clones instead.
    pub fn simplify(re: &Regexp) -> Regexp {
        match re.op {
            Op::Capture | Op::Concat | Op::Alternate => {
                let mut nre = re.clone();
                nre.sub = re.sub.iter().map(simplify).collect();
                nre
            }
            Op::Star | Op::Plus | Op::Quest => {
                let sub = simplify(&re.sub[0]);
                simplify1(re.op, re.flags, sub)
            }
            Op::Repeat => {
                // Special special case: x{0} matches only the empty string.
                if re.min == 0 && re.max == 0 {
                    return Regexp::new(Op::EmptyMatch);
                }
                let sub = simplify(&re.sub[0]);
                // x{n,} means at least n matches of x.
                if re.max == -1 {
                    if re.min == 0 {
                        return simplify1(Op::Star, re.flags, sub);
                    }
                    if re.min == 1 {
                        return simplify1(Op::Plus, re.flags, sub);
                    }
                    // General case: x{4,} is xxxx+.
                    let mut nre = Regexp::new(Op::Concat);
                    for _ in 0..re.min - 1 {
                        nre.sub.push(sub.clone());
                    }
                    nre.sub.push(simplify1(Op::Plus, re.flags, sub));
                    return nre;
                }
                // Special case: x{1} is just x.
                if re.min == 1 && re.max == 1 {
                    return sub;
                }
                // General case: x{n,m} means n copies of x and m copies of x?.
                // Nest the final m copies: x{2,5} = xx(x(x(x)?)?)?
                let mut prefix: Option<Regexp> = None;
                if re.min > 0 {
                    let mut p = Regexp::new(Op::Concat);
                    for _ in 0..re.min {
                        p.sub.push(sub.clone());
                    }
                    prefix = Some(p);
                }
                if re.max > re.min {
                    let mut suffix = simplify1(Op::Quest, re.flags, sub.clone());
                    for _ in re.min + 1..re.max {
                        let mut nre2 = Regexp::new(Op::Concat);
                        nre2.sub.push(sub.clone());
                        nre2.sub.push(suffix);
                        suffix = simplify1(Op::Quest, re.flags, nre2);
                    }
                    match prefix {
                        None => return suffix,
                        Some(ref mut p) => p.sub.push(suffix),
                    }
                }
                if let Some(p) = prefix {
                    return p;
                }
                // Some degenerate case; handle as impossible match.
                Regexp::new(Op::NoMatch)
            }
            _ => re.clone(),
        }
    }

    /// Port of Go `simplify1`.
    fn simplify1(op: Op, flags: u16, sub: Regexp) -> Regexp {
        // Special case: repeat the empty string as much as you want,
        // but it's still the empty string.
        if sub.op == Op::EmptyMatch {
            return sub;
        }
        // The operators are idempotent if the flags match.
        if op == sub.op && flags & NON_GREEDY == sub.flags & NON_GREEDY {
            return sub;
        }
        let mut re = Regexp::new(op);
        re.flags = flags;
        re.sub.push(sub);
        re
    }

    // --- String serialization (port of Go regexp.go, Go 1.22+ behavior) ----

    type PrintFlags = u8;
    const FLAG_I: PrintFlags = 1 << 0; // (?i:
    const FLAG_M: PrintFlags = 1 << 1; // (?m:
    const FLAG_S: PrintFlags = 1 << 2; // (?s:
    const FLAG_OFF: PrintFlags = 1 << 3; // )
    const FLAG_PREC: PrintFlags = 1 << 4; // (?: )
    const NEG_SHIFT: u32 = 5; // FLAG_I << NEG_SHIFT is (?-i:

    /// Port of Go `addSpan`.
    fn add_span(
        start: *const Regexp,
        last: *const Regexp,
        f: PrintFlags,
        flags: &mut HashMap<*const Regexp, PrintFlags>,
    ) {
        flags.insert(start, f);
        *flags.entry(last).or_insert(0) |= FLAG_OFF; // maybe start == last
    }

    /// Port of Go `calcFlags`.
    fn calc_flags(
        re: &Regexp,
        flags: &mut HashMap<*const Regexp, PrintFlags>,
    ) -> (PrintFlags, PrintFlags) {
        match re.op {
            Op::Literal => {
                // If literal is fold-sensitive, return (FLAG_I, 0) or
                // (0, FLAG_I) according to whether (?i) is active.
                for &r in &re.rune {
                    if (MIN_FOLD..=MAX_FOLD).contains(&r) && simple_fold(r) != r {
                        if re.flags & FOLD_CASE != 0 {
                            return (FLAG_I, 0);
                        }
                        return (0, FLAG_I);
                    }
                }
                (0, 0)
            }
            Op::CharClass => {
                // If class is fold-sensitive, return (0, FLAG_I) - (?i) has
                // been compiled out.
                for i in (0..re.rune.len()).step_by(2) {
                    let lo = MIN_FOLD.max(re.rune[i]);
                    let hi = MAX_FOLD.min(re.rune[i + 1]);
                    let mut r = lo;
                    while r <= hi {
                        let mut f = simple_fold(r);
                        let mut steps = 0;
                        while f != r && steps < MAX_FOLD_ORBIT {
                            if !(lo..=hi).contains(&f) && !in_char_class(f, &re.rune) {
                                return (0, FLAG_I);
                            }
                            f = simple_fold(f);
                            steps += 1;
                        }
                        r += 1;
                    }
                }
                (0, 0)
            }
            Op::AnyCharNotNL => (0, FLAG_S),            // (?-s).
            Op::AnyChar => (FLAG_S, 0),                 // (?s).
            Op::BeginLine | Op::EndLine => (FLAG_M, 0), // (?m)^ (?m)$
            Op::EndText => {
                if re.flags & WAS_DOLLAR != 0 {
                    return (0, FLAG_M); // (?-m)$
                }
                (0, 0)
            }
            Op::Capture | Op::Star | Op::Plus | Op::Quest | Op::Repeat => {
                calc_flags(&re.sub[0], flags)
            }
            Op::Concat | Op::Alternate => {
                // Gather the must and cant for each subexpression.
                // When we find a conflicting subexpression, insert the
                // necessary flags around the previously identified span
                // and start over.
                let mut must: PrintFlags = 0;
                let mut cant: PrintFlags = 0;
                let mut all_cant: PrintFlags = 0;
                let mut start = 0usize;
                let mut last = 0usize;
                let mut did = false;
                for (i, sub) in re.sub.iter().enumerate() {
                    let (sub_must, sub_cant) = calc_flags(sub, flags);
                    if must & sub_cant != 0 || sub_must & cant != 0 {
                        if must != 0 {
                            add_span(&re.sub[start], &re.sub[last], must, flags);
                        }
                        must = 0;
                        cant = 0;
                        start = i;
                        did = true;
                    }
                    must |= sub_must;
                    cant |= sub_cant;
                    all_cant |= sub_cant;
                    if sub_must != 0 {
                        last = i;
                    }
                    if must == 0 && start == i {
                        start += 1;
                    }
                }
                if !did {
                    // No conflicts: pass the accumulated must and cant upward.
                    return (must, cant);
                }
                if must != 0 {
                    // Conflicts found; need to finish final span.
                    add_span(&re.sub[start], &re.sub[last], must, flags);
                }
                (0, all_cant)
            }
            _ => (0, 0),
        }
    }

    /// Approximation of Go `unicode.IsPrint`.
    ///
    /// Deviation from Go: Go checks the L/M/N/P/S Unicode categories plus
    /// ASCII space; Rust std lacks a category API, so this uses
    /// "not control and not non-space whitespace", which agrees on all
    /// characters reachable from the supported syntax in practice.
    fn is_print(r: i32) -> bool {
        match char::from_u32(r as u32) {
            None => false,
            Some(c) => {
                if c == ' ' {
                    return true;
                }
                !c.is_control() && !c.is_whitespace()
            }
        }
    }

    const META: &str = "\\.+*?()|[]{}^$";

    /// Port of Go `escape`.
    fn escape(b: &mut String, r: i32, force: bool) {
        if is_print(r) {
            let c = char::from_u32(r as u32).unwrap();
            if META.contains(c) || force {
                b.push('\\');
            }
            b.push(c);
            return;
        }
        match r {
            0x07 => b.push_str("\\a"),
            0x0C => b.push_str("\\f"),
            0x0A => b.push_str("\\n"),
            0x0D => b.push_str("\\r"),
            0x09 => b.push_str("\\t"),
            0x0B => b.push_str("\\v"),
            _ => {
                if r < 0x100 {
                    b.push_str("\\x");
                    let s = format!("{r:x}");
                    if s.len() == 1 {
                        b.push('0');
                    }
                    b.push_str(&s);
                } else {
                    b.push_str(&format!("\\x{{{r:x}}}"));
                }
            }
        }
    }

    /// Port of Go `writeRegexp`.
    fn write_regexp(
        b: &mut String,
        re: &Regexp,
        f: PrintFlags,
        flags: &HashMap<*const Regexp, PrintFlags>,
    ) {
        let mut f = f | flags.get(&(re as *const Regexp)).copied().unwrap_or(0);
        if f & FLAG_PREC != 0 && f & !(FLAG_OFF | FLAG_PREC) != 0 && f & FLAG_OFF != 0 {
            // FLAG_PREC is redundant with other flags being added and
            // terminated.
            f &= !FLAG_PREC;
        }
        if f & !(FLAG_OFF | FLAG_PREC) != 0 {
            b.push_str("(?");
            if f & FLAG_I != 0 {
                b.push('i');
            }
            if f & FLAG_M != 0 {
                b.push('m');
            }
            if f & FLAG_S != 0 {
                b.push('s');
            }
            if f & ((FLAG_M | FLAG_S) << NEG_SHIFT) != 0 {
                b.push('-');
                if f & (FLAG_M << NEG_SHIFT) != 0 {
                    b.push('m');
                }
                if f & (FLAG_S << NEG_SHIFT) != 0 {
                    b.push('s');
                }
            }
            b.push(':');
        }
        if f & FLAG_PREC != 0 {
            b.push_str("(?:");
        }

        match re.op {
            Op::NoMatch => b.push_str("[^\\x00-\\x{10FFFF}]"),
            Op::EmptyMatch => b.push_str("(?:)"),
            Op::Literal => {
                for &r in &re.rune {
                    escape(b, r, false);
                }
            }
            Op::CharClass => {
                if re.rune.len() % 2 != 0 {
                    b.push_str("[invalid char class]");
                } else {
                    b.push('[');
                    if re.rune.is_empty() {
                        b.push_str("^\\x00-\\x{10FFFF}");
                    } else if re.rune[0] == 0
                        && re.rune[re.rune.len() - 1] == MAX_RUNE
                        && re.rune.len() > 2
                    {
                        // Contains 0 and MaxRune. Probably a negated class.
                        // Print the gaps.
                        b.push('^');
                        let mut i = 1;
                        while i < re.rune.len() - 1 {
                            let (lo, hi) = (re.rune[i] + 1, re.rune[i + 1] - 1);
                            escape(b, lo, lo == '-' as i32);
                            if lo != hi {
                                if hi != lo + 1 {
                                    b.push('-');
                                }
                                escape(b, hi, hi == '-' as i32);
                            }
                            i += 2;
                        }
                    } else {
                        let mut i = 0;
                        while i < re.rune.len() {
                            let (lo, hi) = (re.rune[i], re.rune[i + 1]);
                            escape(b, lo, lo == '-' as i32);
                            if lo != hi {
                                if hi != lo + 1 {
                                    b.push('-');
                                }
                                escape(b, hi, hi == '-' as i32);
                            }
                            i += 2;
                        }
                    }
                    b.push(']');
                }
            }
            Op::AnyCharNotNL | Op::AnyChar => b.push('.'),
            Op::BeginLine => b.push('^'),
            Op::EndLine => b.push('$'),
            Op::BeginText => b.push_str("\\A"),
            Op::EndText => {
                if re.flags & WAS_DOLLAR != 0 {
                    b.push('$');
                } else {
                    b.push_str("\\z");
                }
            }
            Op::WordBoundary => b.push_str("\\b"),
            Op::NoWordBoundary => b.push_str("\\B"),
            Op::Capture => {
                if !re.name.is_empty() {
                    b.push_str("(?P<");
                    b.push_str(&re.name);
                    b.push('>');
                } else {
                    b.push('(');
                }
                if re.sub[0].op != Op::EmptyMatch {
                    let sub_flags = flags
                        .get(&(&re.sub[0] as *const Regexp))
                        .copied()
                        .unwrap_or(0);
                    write_regexp(b, &re.sub[0], sub_flags, flags);
                }
                b.push(')');
            }
            Op::Star | Op::Plus | Op::Quest | Op::Repeat => {
                let sub = &re.sub[0];
                let p: PrintFlags =
                    if sub.op > Op::Capture || (sub.op == Op::Literal && sub.rune.len() > 1) {
                        FLAG_PREC
                    } else {
                        0
                    };
                write_regexp(b, sub, p, flags);
                match re.op {
                    Op::Star => b.push('*'),
                    Op::Plus => b.push('+'),
                    Op::Quest => b.push('?'),
                    Op::Repeat => {
                        b.push('{');
                        b.push_str(&re.min.to_string());
                        if re.max != re.min {
                            b.push(',');
                            if re.max >= 0 {
                                b.push_str(&re.max.to_string());
                            }
                        }
                        b.push('}');
                    }
                    _ => unreachable!(),
                }
                if re.flags & NON_GREEDY != 0 {
                    b.push('?');
                }
            }
            Op::Concat => {
                for sub in &re.sub {
                    let p: PrintFlags = if sub.op == Op::Alternate {
                        FLAG_PREC
                    } else {
                        0
                    };
                    write_regexp(b, sub, p, flags);
                }
            }
            Op::Alternate => {
                for (i, sub) in re.sub.iter().enumerate() {
                    if i > 0 {
                        b.push('|');
                    }
                    write_regexp(b, sub, 0, flags);
                }
            }
            _ => b.push_str("<invalid op>"),
        }

        if f & FLAG_PREC != 0 {
            b.push(')');
        }
        if f & FLAG_OFF != 0 {
            b.push(')');
        }
    }

    /// Port of Go `Regexp.String` (Go 1.22+ flag-hoisting behavior).
    pub fn regexp_string(re: &Regexp) -> String {
        let mut flags: HashMap<*const Regexp, PrintFlags> = HashMap::new();
        let (must, cant) = calc_flags(re, &mut flags);
        let mut must = must | ((cant & !FLAG_I) << NEG_SHIFT);
        if must != 0 {
            must |= FLAG_OFF;
        }
        let mut b = String::new();
        write_regexp(&mut b, re, must, &flags);
        b
    }
}

use crate::bytesutil::FastStringMatcher;
use syntax::{Op, ParseError, Regexp};

/// Error returned by [`PromRegex::new`] and [`Regex::new`].
#[derive(Debug)]
pub struct RegexError(String);

impl std::fmt::Display for RegexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RegexError {}

/// Port of Go `regexutil.RemoveStartEndAnchors`.
///
/// Removes '^' at the start of expr and '$' at the end of the expr.
pub fn remove_start_end_anchors(expr: &str) -> &str {
    let mut expr = expr;
    while let Some(rest) = expr.strip_prefix('^') {
        expr = rest;
    }
    while expr.ends_with('$') && !expr.ends_with("\\$") {
        expr = &expr[..expr.len() - 1];
    }
    expr
}

const MAX_OR_VALUES: usize = 100;

fn parse_regexp(expr: &str) -> syntax::Result<Regexp> {
    syntax::parse(expr, syntax::PERL | syntax::DOT_NL)
}

/// Port of Go `regexutil.GetOrValuesRegex`.
///
/// Returns "or" values from the given regexp expr:
/// `["foo", "bar"]` for `foo|bar`, `["foo"]` for `foo`, `[""]` for ``.
/// Returns an empty list if it is impossible to extract "or" values
/// (Go returns nil in that case).
pub fn get_or_values_regex(expr: &str) -> Vec<String> {
    get_or_values_regex_internal(expr, true)
}

/// Port of Go `regexutil.GetOrValuesPromRegex`.
///
/// Like [`get_or_values_regex`], but ignores start/end anchors
/// (`^` and `$`) at the start and the end of expr.
pub fn get_or_values_prom_regex(expr: &str) -> Vec<String> {
    let expr = remove_start_end_anchors(expr);
    get_or_values_regex_internal(expr, false)
}

fn get_or_values_regex_internal(expr: &str, keep_anchors: bool) -> Vec<String> {
    let (prefix, tail_expr) = simplify_regex_internal(expr, keep_anchors);
    if tail_expr.is_empty() {
        return vec![prefix];
    }
    let Ok(sre) = parse_regexp(&tail_expr) else {
        return Vec::new();
    };
    let Some(mut or_values) = get_or_values(&sre) else {
        return Vec::new();
    };
    // Sort or_values for faster index seek later.
    or_values.sort();
    if !prefix.is_empty() {
        // Add prefix to or_values.
        for v in or_values.iter_mut() {
            *v = format!("{prefix}{v}");
        }
    }
    or_values
}

/// Port of Go `string(rune)` conversion: invalid runes map to U+FFFD.
fn rune_to_string(r: i32) -> String {
    char::from_u32(r as u32).unwrap_or('\u{FFFD}').to_string()
}

/// Port of Go `regexutil.getOrValues`. `None` corresponds to Go's nil result.
fn get_or_values(sre: &Regexp) -> Option<Vec<String>> {
    match sre.op {
        Op::Capture => get_or_values(&sre.sub[0]),
        Op::Literal => get_literal(sre).map(|v| vec![v]),
        Op::EmptyMatch => Some(vec![String::new()]),
        Op::Alternate => {
            let mut a: Vec<String> = Vec::with_capacity(sre.sub.len());
            for re_sub in &sre.sub {
                let ca = get_or_values(re_sub)?;
                if ca.is_empty() {
                    return None;
                }
                a.extend(ca);
                if a.len() > MAX_OR_VALUES {
                    // It is cheaper to use regexp here.
                    return None;
                }
            }
            Some(a)
        }
        Op::CharClass => {
            let mut a: Vec<String> = Vec::with_capacity(sre.rune.len() / 2);
            for pair in sre.rune.chunks(2) {
                let (mut start, end) = (pair[0], pair[1]);
                while start <= end {
                    a.push(rune_to_string(start));
                    start += 1;
                    if a.len() > MAX_OR_VALUES {
                        // It is cheaper to use regexp here.
                        return None;
                    }
                }
            }
            Some(a)
        }
        Op::Concat => get_or_values_concat(&sre.sub),
        _ => None,
    }
}

/// The `OpConcat` arm of Go `getOrValues` (Go shortens `sre.Sub` in place;
/// this port recurses over the sub-slice instead).
fn get_or_values_concat(subs: &[Regexp]) -> Option<Vec<String>> {
    if subs.is_empty() {
        return Some(vec![String::new()]);
    }
    let prefixes = get_or_values(&subs[0])?;
    if prefixes.is_empty() {
        return None;
    }
    if subs.len() == 1 {
        return Some(prefixes);
    }
    let suffixes = get_or_values_concat(&subs[1..])?;
    if suffixes.is_empty() {
        return None;
    }
    if prefixes.len() * suffixes.len() > MAX_OR_VALUES {
        // It is cheaper to use regexp here.
        return None;
    }
    let mut a = Vec::with_capacity(prefixes.len() * suffixes.len());
    for prefix in &prefixes {
        for suffix in &suffixes {
            a.push(format!("{prefix}{suffix}"));
        }
    }
    Some(a)
}

/// Port of Go `regexutil.getLiteral`.
fn get_literal(sre: &Regexp) -> Option<String> {
    if sre.op == Op::Capture {
        return get_literal(&sre.sub[0]);
    }
    if sre.op == Op::Literal && sre.flags & syntax::FOLD_CASE == 0 {
        return Some(sre.rune.iter().map(|&r| rune_to_string(r)).collect());
    }
    None
}

/// Port of Go `regexutil.SimplifyRegex`.
///
/// Simplifies the given regexp expr and returns the plaintext prefix and the
/// remaining regular expression without capturing parens.
pub fn simplify_regex(expr: &str) -> (String, String) {
    let (prefix, suffix) = simplify_regex_internal(expr, true);
    // Go uses mustParseRegexp here; this port can also see suffixes it cannot
    // parse (unsupported Unicode classes), which are returned unchanged.
    let Ok(mut sre) = parse_regexp(&suffix) else {
        return (prefix, suffix);
    };
    if is_dot_op(&sre, Op::Star) {
        return (prefix, String::new());
    }
    if sre.op == Op::Concat {
        let mut subs = std::mem::take(&mut sre.sub);
        if prefix.is_empty() {
            // Drop .* at the start.
            while !subs.is_empty() && is_dot_op(&subs[0], Op::Star) {
                subs.remove(0);
            }
        }
        // Drop .* at the end.
        while !subs.is_empty() && is_dot_op(subs.last().unwrap(), Op::Star) {
            subs.pop();
        }
        if subs.is_empty() {
            return (prefix, String::new());
        }
        sre.sub = subs;
        let suffix = syntax::regexp_string(&sre);
        return (prefix, suffix);
    }
    (prefix, suffix)
}

/// Port of Go `regexutil.SimplifyPromRegex`.
///
/// Simplifies the given Prometheus-like expr, returning the plaintext prefix
/// and the remaining regular expression with dropped '^' and '$' anchors at
/// the beginning and the end. Removes capturing parens from the expr.
pub fn simplify_prom_regex(expr: &str) -> (String, String) {
    simplify_regex_internal(expr, false)
}

/// Port of Go `regexutil.simplifyRegex` (the private one).
fn simplify_regex_internal(expr: &str, keep_anchors: bool) -> (String, String) {
    let sre = match parse_regexp(expr) {
        Ok(sre) => sre,
        // Deviation from Go: constructs this port does not implement
        // (Unicode classes) are valid in Go, so they are treated as
        // "valid but not simplifiable" instead of as a literal prefix.
        Err(ParseError::Unsupported) => return (String::new(), expr.to_string()),
        // Cannot parse the regexp. Return it all as prefix.
        Err(_) => return (expr.to_string(), String::new()),
    };
    let Some(mut sre) = simplify_regexp(sre, keep_anchors, keep_anchors) else {
        // The regexp is valid but cannot be simplified. Return it as suffix.
        return (String::new(), expr.to_string());
    };
    if sre.op == Op::EmptyMatch {
        return (String::new(), String::new());
    }
    if let Some(v) = get_literal(&sre) {
        return (v, String::new());
    }
    let mut prefix = String::new();
    if sre.op == Op::Concat {
        if let Some(v) = get_literal(&sre.sub[0]) {
            prefix = v;
            sre.sub.remove(0);
            if sre.sub.is_empty() {
                return (prefix, String::new());
            }
            if let Some(sre_new) = simplify_regexp(sre.clone(), true, keep_anchors) {
                sre = sre_new;
            }
        }
    }
    let s = syntax::regexp_string(&sre);
    // Go checks syntax.Compile(sre); this port checks that the serialized
    // form compiles with the regex crate (deviation).
    if regex::Regex::new(&s).is_err() {
        // Cannot compile the regexp. Return it all as prefix.
        return (expr.to_string(), String::new());
    }
    let s = s
        .replace("(?:)", "")
        .replace("(?s:.)", ".")
        .replace("(?m:$)", "$");
    (prefix, s)
}

/// Port of Go `regexutil.simplifyRegexp`. `None` corresponds to Go's
/// `(nil, false)` result.
fn simplify_regexp(sre: Regexp, keep_begin_op: bool, keep_end_op: bool) -> Option<Regexp> {
    let mut s = syntax::regexp_string(&sre);
    let mut sre = sre;
    loop {
        let mut new = simplify_regexp_ext(sre, keep_begin_op, keep_end_op);
        new = syntax::simplify(&new);
        if (!keep_begin_op && new.op == Op::BeginText) || (!keep_end_op && new.op == Op::EndText) {
            new = Regexp::new(Op::EmptyMatch);
        }
        let s_new = syntax::regexp_string(&new);
        if s_new == s {
            return Some(new);
        }
        s = s_new;
        match parse_regexp(&s) {
            Ok(parsed) => sre = parsed,
            // Parsing errors can occur due to deep nesting limits or other
            // validation parameters (see VictoriaLogs issue #1112).
            Err(_) => return None,
        }
    }
}

/// Port of Go `regexutil.simplifyRegexpExt`.
///
/// Go uses pointer identity with a shared `emptyRegexp` node; this port
/// checks `Op::EmptyMatch` instead, which is equivalent because every empty
/// match encountered is normalized by the recursion.
fn simplify_regexp_ext(mut sre: Regexp, keep_begin_op: bool, keep_end_op: bool) -> Regexp {
    match sre.op {
        Op::Capture => {
            // Substitute all the capture regexps with non-capture regexps.
            sre.op = Op::Alternate;
            let sub0 = simplify_regexp_ext(sre.sub.remove(0), keep_begin_op, keep_end_op);
            if sub0.op == Op::EmptyMatch {
                return Regexp::new(Op::EmptyMatch);
            }
            sre.sub.insert(0, sub0);
            sre
        }
        Op::Star | Op::Plus | Op::Quest | Op::Repeat => {
            let sub0 = simplify_regexp_ext(sre.sub.remove(0), keep_begin_op, keep_end_op);
            if sub0.op == Op::EmptyMatch {
                return Regexp::new(Op::EmptyMatch);
            }
            sre.sub.insert(0, sub0);
            sre
        }
        Op::Alternate => {
            // Do not remove empty captures from OpAlternate, since this may
            // break the regexp.
            for sub in sre.sub.iter_mut() {
                let s = simplify_regexp_ext(
                    std::mem::replace(sub, Regexp::new(Op::EmptyMatch)),
                    keep_begin_op,
                    keep_end_op,
                );
                *sub = s;
            }
            sre
        }
        Op::Concat => {
            let old = std::mem::take(&mut sre.sub);
            let total = old.len();
            let mut subs: Vec<Regexp> = Vec::with_capacity(total);
            for (i, sub) in old.into_iter().enumerate() {
                let sub = simplify_regexp_ext(
                    sub,
                    keep_begin_op || !subs.is_empty(),
                    keep_end_op || i + 1 < total,
                );
                if sub.op != Op::EmptyMatch {
                    subs.push(sub);
                }
            }
            // Remove anchors from the beginning and the end of regexp,
            // since they will be added later.
            if !keep_begin_op {
                while !subs.is_empty() && subs[0].op == Op::BeginText {
                    subs.remove(0);
                }
            }
            if !keep_end_op {
                while !subs.is_empty() && subs.last().unwrap().op == Op::EndText {
                    subs.pop();
                }
            }
            if subs.is_empty() {
                return Regexp::new(Op::EmptyMatch);
            }
            if subs.len() == 1 {
                return subs.pop().unwrap();
            }
            sre.sub = subs;
            sre
        }
        Op::EmptyMatch => Regexp::new(Op::EmptyMatch),
        _ => sre,
    }
}

/// Port of Go `regexutil.getSubstringLiteral`.
fn get_substring_literal(sre: &Regexp, prefix_suffix_op: Op) -> String {
    if sre.op != Op::Concat || sre.sub.len() != 3 {
        return String::new();
    }
    if !is_dot_op(&sre.sub[0], prefix_suffix_op) || !is_dot_op(&sre.sub[2], prefix_suffix_op) {
        return String::new();
    }
    get_literal(&sre.sub[1]).unwrap_or_default()
}

/// Port of Go `regexutil.isDotOp`.
fn is_dot_op(sre: &Regexp, op: Op) -> bool {
    if sre.op != op {
        return false;
    }
    sre.sub.first().is_some_and(|sub| sub.op == Op::AnyChar)
}

/// Byte-level substring search (Go `strings.Index` semantics).
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Port of Go `regexutil.PromRegex`: optimized matching for Prometheus-like
/// regexps, automatically anchored to the whole string.
///
/// Optimized cases: plain strings, alternations ("foo|bar|baz"), prefix
/// matches ("foo.*", "foo.+") and substring matches (".*foo.*", ".+foo.+").
/// Everything else falls back to a cached full regexp match.
pub struct PromRegex {
    expr_str: String,
    prefix: String,
    is_only_prefix: bool,
    is_suffix_dot_star: bool,
    is_suffix_dot_plus: bool,
    substr_dot_star: String,
    substr_dot_plus: String,
    or_values: Vec<String>,
    re_suffix_matcher: FastStringMatcher,
}

impl PromRegex {
    /// Port of Go `regexutil.NewPromRegex`.
    pub fn new(expr: &str) -> Result<PromRegex, RegexError> {
        // Go validates with regexp.Compile; this port validates with the
        // ported Go-syntax parser so Go-specific limits (e.g. max repeat
        // count 1000) are enforced identically. Constructs the port does not
        // implement fall through to the regex-crate compilation below.
        match parse_regexp(expr) {
            Ok(_) | Err(ParseError::Unsupported) => {}
            Err(e) => return Err(RegexError(format!("cannot parse {expr:?}: {e:?}"))),
        }
        let (prefix, suffix) = simplify_prom_regex(expr);
        // Go uses mustParseRegexp(suffix); the suffix may be unparseable by
        // this port only in the Unsupported case, where all fast paths are
        // disabled and the fallback matcher is used.
        let sre = parse_regexp(&suffix).ok();
        let or_values = sre.as_ref().and_then(get_or_values).unwrap_or_default();
        let is_only_prefix = or_values.len() == 1 && or_values[0].is_empty();
        let is_suffix_dot_star = sre.as_ref().is_some_and(|s| is_dot_op(s, Op::Star));
        let is_suffix_dot_plus = sre.as_ref().is_some_and(|s| is_dot_op(s, Op::Plus));
        let substr_dot_star = sre
            .as_ref()
            .map_or(String::new(), |s| get_substring_literal(s, Op::Star));
        let substr_dot_plus = sre
            .as_ref()
            .map_or(String::new(), |s| get_substring_literal(s, Op::Plus));
        // Anchor suffix to the beginning and the end of the matching string.
        let suffix_expr = format!("^(?:{suffix})$");
        let re_suffix = regex::Regex::new(&suffix_expr)
            .map_err(|e| RegexError(format!("cannot compile {suffix_expr:?}: {e}")))?;
        let re_suffix_matcher = FastStringMatcher::new(move |s: &str| re_suffix.is_match(s));
        Ok(PromRegex {
            expr_str: expr.to_string(),
            prefix,
            is_only_prefix,
            is_suffix_dot_star,
            is_suffix_dot_plus,
            substr_dot_star,
            substr_dot_plus,
            or_values,
            re_suffix_matcher,
        })
    }

    /// Port of Go `PromRegex.MatchString`.
    ///
    /// Returns true if s matches the regex, which is automatically anchored
    /// to the beginning and the end of the matching string.
    pub fn match_string(&self, s: &str) -> bool {
        if self.is_only_prefix {
            return s == self.prefix;
        }
        let mut s = s;
        if !self.prefix.is_empty() {
            match s.strip_prefix(self.prefix.as_str()) {
                Some(rest) => s = rest,
                // Fast path - s has another prefix.
                None => return false,
            }
        }
        if self.is_suffix_dot_star {
            // Fast path - the regex contains "prefix.*"
            return true;
        }
        if self.is_suffix_dot_plus {
            // Fast path - the regex contains "prefix.+"
            return !s.is_empty();
        }
        if !self.substr_dot_star.is_empty() {
            // Fast path - the regex contains ".*someText.*"
            return s.contains(&self.substr_dot_star);
        }
        if !self.substr_dot_plus.is_empty() {
            // Fast path - the regex contains ".+someText.+"
            return match s.find(&self.substr_dot_plus) {
                Some(n) => n > 0 && n + self.substr_dot_plus.len() < s.len(),
                None => false,
            };
        }
        if !self.or_values.is_empty() {
            // Fast path - the regex contains only alternate strings.
            return self.or_values.iter().any(|v| v == s);
        }
        // Fall back to slow path by matching the original regexp.
        self.re_suffix_matcher.matches(s)
    }
}

impl std::fmt::Display for PromRegex {
    /// Port of Go `PromRegex.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.expr_str)
    }
}

/// Port of Go `regexutil.Regex`: optimized (unanchored) matching for Go
/// regexps, with the same fast paths as [`PromRegex`].
pub struct Regex {
    expr_str: String,
    prefix: String,
    is_only_prefix: bool,
    is_suffix_dot_star: bool,
    is_suffix_dot_plus: bool,
    substr_dot_star: String,
    substr_dot_plus: String,
    or_values: Vec<String>,
    suffix_re: regex::bytes::Regex,
}

impl Regex {
    /// Port of Go `regexutil.NewRegex`.
    pub fn new(expr: &str) -> Result<Regex, RegexError> {
        match parse_regexp(expr) {
            Ok(_) | Err(ParseError::Unsupported) => {}
            Err(e) => return Err(RegexError(format!("cannot parse {expr:?}: {e:?}"))),
        }
        let (prefix, suffix) = simplify_regex(expr);
        let sre = parse_regexp(&suffix).ok();
        let or_values = sre.as_ref().and_then(get_or_values).unwrap_or_default();
        let is_only_prefix = or_values.len() == 1 && or_values[0].is_empty();
        let is_suffix_dot_star = sre.as_ref().is_some_and(|s| is_dot_op(s, Op::Star));
        let is_suffix_dot_plus = sre.as_ref().is_some_and(|s| is_dot_op(s, Op::Plus));
        let substr_dot_star = sre
            .as_ref()
            .map_or(String::new(), |s| get_substring_literal(s, Op::Star));
        let substr_dot_plus = sre
            .as_ref()
            .map_or(String::new(), |s| get_substring_literal(s, Op::Plus));
        let suffix_anchored = if !prefix.is_empty() {
            format!("^(?:{suffix})")
        } else {
            suffix.clone()
        };
        // Go uses regexp.MustCompile here (a failure is a bug); this port
        // returns an error instead of panicking (deviation).
        let suffix_re = regex::bytes::Regex::new(&suffix_anchored)
            .map_err(|e| RegexError(format!("cannot compile {suffix_anchored:?}: {e}")))?;
        Ok(Regex {
            expr_str: expr.to_string(),
            prefix,
            is_only_prefix,
            is_suffix_dot_star,
            is_suffix_dot_plus,
            substr_dot_star,
            substr_dot_plus,
            or_values,
            suffix_re,
        })
    }

    /// Port of Go `Regex.MatchString`.
    pub fn match_string(&self, s: &str) -> bool {
        if self.is_only_prefix {
            if self.prefix.is_empty() {
                return true;
            }
            return s.contains(&self.prefix);
        }
        if self.prefix.is_empty() {
            return self.match_string_no_prefix(s);
        }
        self.match_string_with_prefix(s)
    }

    /// Port of Go `Regex.GetLiterals`.
    pub fn get_literals(&self) -> Vec<String> {
        // Go uses mustParseRegexp; this port tolerates unsupported
        // constructs by returning no literals.
        let Ok(sre) = parse_regexp(&self.expr_str) else {
            return Vec::new();
        };
        let mut node = &sre;
        while node.op == Op::Capture {
            node = &node.sub[0];
        }
        if let Some(v) = get_literal(node) {
            return vec![v];
        }
        if node.op != Op::Concat {
            return Vec::new();
        }
        let mut a = Vec::new();
        for sub in &node.sub {
            if let Some(v) = get_literal(sub) {
                a.push(v);
            }
        }
        a
    }

    fn match_string_no_prefix(&self, s: &str) -> bool {
        if self.is_suffix_dot_star {
            return true;
        }
        if self.is_suffix_dot_plus {
            return !s.is_empty();
        }
        if !self.substr_dot_star.is_empty() {
            // Fast path - the regex contains ".*someText.*"
            return s.contains(&self.substr_dot_star);
        }
        if !self.substr_dot_plus.is_empty() {
            // Fast path - the regex contains ".+someText.+"
            return match s.find(&self.substr_dot_plus) {
                Some(n) => n > 0 && n + self.substr_dot_plus.len() < s.len(),
                None => false,
            };
        }
        if self.or_values.is_empty() {
            // Fall back to slow path by matching the suffix regexp.
            return self.suffix_re.is_match(s.as_bytes());
        }
        // Fast path - compare s to or_values.
        self.or_values.iter().any(|v| s.contains(v.as_str()))
    }

    fn match_string_with_prefix(&self, s: &str) -> bool {
        // This loop works on byte slices, exactly like Go's string slicing:
        // the "next char" restart position may fall inside a multi-byte
        // UTF-8 sequence and is only ever used for byte-level searching.
        let sb = s.as_bytes();
        let pb = self.prefix.as_bytes();
        let Some(n) = find_bytes(sb, pb) else {
            // Fast path - s doesn't contain the needed prefix.
            return false;
        };
        let mut s_next = &sb[n + 1..];
        let mut cur = &sb[n + pb.len()..];

        if self.is_suffix_dot_star {
            return true;
        }
        if self.is_suffix_dot_plus {
            return !cur.is_empty();
        }
        if !self.substr_dot_star.is_empty() {
            // Fast path - the regex contains ".*someText.*"
            return find_bytes(cur, self.substr_dot_star.as_bytes()).is_some();
        }
        if !self.substr_dot_plus.is_empty() {
            // Fast path - the regex contains ".+someText.+"
            let needle = self.substr_dot_plus.as_bytes();
            return match find_bytes(cur, needle) {
                Some(n) => n > 0 && n + needle.len() < cur.len(),
                None => false,
            };
        }

        loop {
            if self.or_values.is_empty() {
                // Fall back to slow path by matching the suffix regexp.
                if self.suffix_re.is_match(cur) {
                    return true;
                }
            } else {
                // Fast path - compare the current position to or_values.
                for v in &self.or_values {
                    if cur.starts_with(v.as_bytes()) {
                        return true;
                    }
                }
            }
            // Mismatch. Try again starting from the next char.
            let Some(n) = find_bytes(s_next, pb) else {
                // Fast path - s doesn't contain the needed prefix.
                return false;
            };
            let base = s_next;
            s_next = &base[n + 1..];
            cur = &base[n + pb.len()..];
        }
    }
}

impl std::fmt::Display for Regex {
    /// Port of Go `Regex.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.expr_str)
    }
}

#[cfg(test)]
mod tests {
    // Regression test for the simple_fold orbit-walk hang: negated char
    // classes cover the whole fold range; asymmetric std-derived fold
    // orbits (e.g. U+212A KELVIN SIGN) must not loop forever.
    #[test]
    fn negated_char_class_simplify_terminates() {
        let (prefix, suffix) = super::simplify_regex("[^.]");
        assert_eq!(prefix, "");
        assert!(!suffix.is_empty());
        let ov = super::get_or_values_regex("[^.]");
        assert!(ov.is_empty());
        let re = super::Regex::new(r"foo\.[^.]*\.bar\.ba(xx|zz)[^.]*\.a").unwrap();
        assert!(re.match_string("foo.ss.bar.baxx.a"));
        assert!(!re.match_string("foo.ss.xar.baxx.a"));
    }

    use super::*;

    // Port of Go TestGetOrValuesRegex.
    #[test]
    fn test_get_or_values_regex() {
        fn f(s: &str, values_expected: &[&str]) {
            let values = get_or_values_regex(s);
            assert_eq!(
                values, values_expected,
                "unexpected values for s={s:?}; got {values:?}; want {values_expected:?}"
            );
        }

        f("", &[""]);
        f("foo", &["foo"]);
        f("^foo$", &[]);
        f("|foo", &["", "foo"]);
        f("|foo|", &["", "", "foo"]);
        f("foo.+", &[]);
        f("foo.*", &[]);
        f(".*", &[]);
        f("foo|.*", &[]);
        f("(fo((o)))|(bar)", &["bar", "foo"]);
        f("foobar", &["foobar"]);
        f("z|x|c", &["c", "x", "z"]);
        f("foo|bar", &["bar", "foo"]);
        f("(foo|bar)", &["bar", "foo"]);
        f("(foo|bar)baz", &["barbaz", "foobaz"]);
        f("[a-z][a-z]", &[]);
        f("[a-d]", &["a", "b", "c", "d"]);
        f("x[a-d]we", &["xawe", "xbwe", "xcwe", "xdwe"]);
        f("foo(bar|baz)", &["foobar", "foobaz"]);
        f("foo(ba[rz]|(xx|o))", &["foobar", "foobaz", "fooo", "fooxx"]);
        f(
            "foo(?:bar|baz)x(qwe|rt)",
            &["foobarxqwe", "foobarxrt", "foobazxqwe", "foobazxrt"],
        );
        f("foo(bar||baz)", &["foo", "foobar", "foobaz"]);
        f("(a|b|c)(d|e|f|0|1|2)(g|h|k|x|y|z)", &[]);
        f("(?i)foo", &[]);
        f("(?i)(foo|bar)", &[]);
        f("^foo|bar$", &[]);
        f("^(foo|bar)$", &[]);
        f("^a(foo|b(?:a|r))$", &[]);
        f("^a(foo$|b(?:a$|r))$", &[]);
        f("^a(^foo|bar$)z$", &[]);
    }

    // Port of Go TestGetOrValuesPromRegex.
    #[test]
    fn test_get_or_values_prom_regex() {
        fn f(s: &str, values_expected: &[&str]) {
            let values = get_or_values_prom_regex(s);
            assert_eq!(
                values, values_expected,
                "unexpected values for s={s:?}; got {values:?}; want {values_expected:?}"
            );
        }

        f("", &[""]);
        f("foo", &["foo"]);
        f("^foo$", &["foo"]);
        f("|foo", &["", "foo"]);
        f("|foo|", &["", "", "foo"]);
        f("foo.+", &[]);
        f("foo.*", &[]);
        f(".*", &[]);
        f("foo|.*", &[]);
        f("(fo((o)))|(bar)", &["bar", "foo"]);
        f("foobar", &["foobar"]);
        f("z|x|c", &["c", "x", "z"]);
        f("foo|bar", &["bar", "foo"]);
        f("(foo|bar)", &["bar", "foo"]);
        f("(foo|bar)baz", &["barbaz", "foobaz"]);
        f("[a-z][a-z]", &[]);
        f("[a-d]", &["a", "b", "c", "d"]);
        f("x[a-d]we", &["xawe", "xbwe", "xcwe", "xdwe"]);
        f("foo(bar|baz)", &["foobar", "foobaz"]);
        f("foo(ba[rz]|(xx|o))", &["foobar", "foobaz", "fooo", "fooxx"]);
        f(
            "foo(?:bar|baz)x(qwe|rt)",
            &["foobarxqwe", "foobarxrt", "foobazxqwe", "foobazxrt"],
        );
        f("foo(bar||baz)", &["foo", "foobar", "foobaz"]);
        f("(a|b|c)(d|e|f|0|1|2)(g|h|k|x|y|z)", &[]);
        f("(?i)foo", &[]);
        f("(?i)(foo|bar)", &[]);
        f("^foo|bar$", &["bar", "foo"]);
        f("^(foo|bar)$", &["bar", "foo"]);
        f("^a(foo|b(?:a|r))$", &["aba", "abr", "afoo"]);
        f("^a(foo$|b(?:a$|r))$", &["aba", "abr", "afoo"]);
        f("^a(^foo|bar$)z$", &[]);
    }

    // Port of Go TestSimplifyRegex.
    #[test]
    fn test_simplify_regex() {
        fn f(s: &str, expected_prefix: &str, expected_suffix: &str) {
            let (prefix, suffix) = simplify_regex(s);
            assert_eq!(
                prefix, expected_prefix,
                "unexpected prefix for s={s:?}; got {prefix:?}; want {expected_prefix:?}"
            );
            assert_eq!(
                suffix, expected_suffix,
                "unexpected suffix for s={s:?}; got {suffix:?}; want {expected_suffix:?}"
            );
        }

        f("", "", "");
        f(".*", "", "");
        f(".*(.*).*", "", "");
        f("foo.*", "foo", "");
        f(".*foo.*", "", "foo");
        f("^", "", "\\A");
        f("$", "", "(?-m:$)");
        f("^()$", "", "(?-m:\\A$)");
        f("^(?:)$", "", "(?-m:\\A$)");
        f("^foo|^bar$|baz", "", "(?-m:\\Afoo|\\Abar$|baz)");
        f("^(foo$|^bar)$", "", "(?-m:\\A(?:foo$|\\Abar)$)");
        f("^a(foo$|bar)$", "", "(?-m:\\Aa(?:foo$|bar)$)");
        f("^a(^foo|bar$)z$", "", "(?-m:\\Aa(?:\\Afoo|bar$)z$)");
        f("foobar", "foobar", "");
        f("foo$|^foobar", "", "(?-m:foo$|\\Afoobar)");
        f("^(foo$|^foobar)$", "", "(?-m:\\A(?:foo$|\\Afoobar)$)");
        f("foobar|foobaz", "fooba", "[rz]");
        f("(fo|(zar|bazz)|x)", "", "fo|zar|bazz|x");
        f("(тестЧЧ|тест)", "тест", "ЧЧ|");
        f("foo(bar|baz|bana)", "fooba", "[rz]|na");
        f("^foobar|foobaz", "", "\\Afoobar|foobaz");
        f("^foobar|^foobaz$", "", "(?-m:\\Afoobar|\\Afoobaz$)");
        f("foobar|foobaz", "fooba", "[rz]");
        f("(?:^foobar|^foobaz)aa.*", "", "(?:\\Afoobar|\\Afoobaz)aa");
        f("foo[bar]+", "foo", "[abr]+");
        f("foo[a-z]+", "foo", "[a-z]+");
        f("foo[bar]*", "foo", "[abr]*");
        f("foo[a-z]*", "foo", "[a-z]*");
        f("foo[x]+", "foo", "x+");
        f("foo[^x]+", "foo", "[^x]+");
        f("foo[x]*", "foo", "x*");
        f("foo[^x]*", "foo", "[^x]*");
        f("foo[x]*bar", "foo", "x*bar");
        f("fo\\Bo[x]*bar?", "fo", "\\Box*bar?");
        f("foo.+bar", "foo", "(?s:.+bar)");
        f("a(b|c.*).+", "a", "(?s:(?:b|c.*).+)");
        f("ab|ac", "a", "[bc]");
        f("(?i)xyz", "", "(?i:XYZ)");
        f("(?i)foo|bar", "", "(?i:FOO|BAR)");
        f("(?i)up.+x", "", "(?is:UP.+X)");
        f("(?smi)xy.*z$", "", "(?ims:XY.*Z$)");

        // test invalid regexps
        f("a(", "a(", "");
        f("a[", "a[", "");
        f("a[]", "a[]", "");
        f("a{", "a{", "");
        f("a{}", "a{}", "");
        f("invalid(regexp", "invalid(regexp", "");

        // The transformed regexp mustn't match aba
        f("a?(^ba|c)", "", "a?(?:\\Aba|c)");

        // The transformed regexp mustn't match barx
        f("(foo|bar$)x*", "", "(?-m:(?:foo|bar$)x*)");

        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5297
        f(".+;|;.+", "", "(?s:.+;|;.+)");
        f("^(.+);|;(.+)$", "", "(?s-m:\\A.+;|;.+$)");
        f("^(.+);$|^;(.+)$", "", "(?s-m:\\A.+;$|\\A;.+$)");
        f(".*;|;.*", "", "(?s:.*;|;.*)");
        f("^(.*);|;(.*)$", "", "(?s-m:\\A.*;|;.*$)");
        f("^(.*);$|^;(.*)$", "", "(?s-m:\\A.*;$|\\A;.*$)");
    }

    // Port of Go TestSimplifyPromRegex.
    #[test]
    fn test_simplify_prom_regex() {
        fn f(s: &str, expected_prefix: &str, expected_suffix: &str) {
            let (prefix, suffix) = simplify_prom_regex(s);
            assert_eq!(
                prefix, expected_prefix,
                "unexpected prefix for s={s:?}; got {prefix:?}; want {expected_prefix:?}"
            );
            assert_eq!(
                suffix, expected_suffix,
                "unexpected suffix for s={s:?}; got {suffix:?}; want {expected_suffix:?}"
            );
        }

        f("", "", "");
        f("^", "", "");
        f("$", "", "");
        f("^()$", "", "");
        f("^(?:)$", "", "");
        f("^foo|^bar$|baz", "", "foo|ba[rz]");
        f("^(foo$|^bar)$", "", "foo|bar");
        f("^a(foo$|bar)$", "a", "foo|bar");
        f("^a(^foo|bar$)z$", "a", "(?-m:(?:\\Afoo|bar$)z)");
        f("foobar", "foobar", "");
        f("foo$|^foobar", "foo", "|bar");
        f("^(foo$|^foobar)$", "foo", "|bar");
        f("foobar|foobaz", "fooba", "[rz]");
        f("(fo|(zar|bazz)|x)", "", "fo|zar|bazz|x");
        f("(тестЧЧ|тест)", "тест", "ЧЧ|");
        f("foo(bar|baz|bana)", "fooba", "[rz]|na");
        f("^foobar|foobaz", "fooba", "[rz]");
        f("^foobar|^foobaz$", "fooba", "[rz]");
        f("foobar|foobaz", "fooba", "[rz]");
        f("(?:^foobar|^foobaz)aa.*", "fooba", "(?s:[rz]aa.*)");
        f("foo[bar]+", "foo", "[abr]+");
        f("foo[a-z]+", "foo", "[a-z]+");
        f("foo[bar]*", "foo", "[abr]*");
        f("foo[a-z]*", "foo", "[a-z]*");
        f("foo[x]+", "foo", "x+");
        f("foo[^x]+", "foo", "[^x]+");
        f("foo[x]*", "foo", "x*");
        f("foo[^x]*", "foo", "[^x]*");
        f("foo[x]*bar", "foo", "x*bar");
        f("fo\\Bo[x]*bar?", "fo", "\\Box*bar?");
        f("foo.+bar", "foo", "(?s:.+bar)");
        f("a(b|c.*).+", "a", "(?s:(?:b|c.*).+)");
        f("ab|ac", "a", "[bc]");
        f("(?i)xyz", "", "(?i:XYZ)");
        f("(?i)foo|bar", "", "(?i:FOO|BAR)");
        f("(?i)up.+x", "", "(?is:UP.+X)");
        f("(?smi)xy.*z$", "", "(?ims:XY.*Z$)");

        // test invalid regexps
        f("a(", "a(", "");
        f("a[", "a[", "");
        f("a[]", "a[]", "");
        f("a{", "a{", "");
        f("a{}", "a{}", "");
        f("invalid(regexp", "invalid(regexp", "");

        // The transformed regexp mustn't match aba
        f("a?(^ba|c)", "", "a?(?:\\Aba|c)");

        // The transformed regexp mustn't match barx
        f("(foo|bar$)x*", "", "(?-m:(?:foo|bar$)x*)");

        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5297
        f(".+;|;.+", "", "(?s:.+;|;.+)");
        f("^(.+);|;(.+)$", "", "(?s:.+;|;.+)");
        f("^(.+);$|^;(.+)$", "", "(?s:.+;|;.+)");
        f(".*;|;.*", "", "(?s:.*;|;.*)");
        f("^(.*);|;(.*)$", "", "(?s:.*;|;.*)");
        f("^(.*);$|^;(.*)$", "", "(?s:.*;|;.*)");
    }

    // Port of Go TestRemoveStartEndAnchors.
    #[test]
    fn test_remove_start_end_anchors() {
        fn f(s: &str, result_expected: &str) {
            let result = remove_start_end_anchors(s);
            assert_eq!(
                result, result_expected,
                "unexpected result for remove_start_end_anchors({s:?})"
            );
        }
        f("", "");
        f("a", "a");
        f("^^abc", "abc");
        f("a^b$c", "a^b$c");
        f("$$abc^", "$$abc^");
        f("^abc|de$", "abc|de");
        f("abc\\$", "abc\\$");
        f("^abc\\$$$", "abc\\$");
        f("^a\\$b\\$$", "a\\$b\\$");
    }

    // Port of Go TestNewRegexFailure.
    #[test]
    fn test_new_regex_failure() {
        fn f(expr: &str) {
            if let Ok(r) = Regex::new(expr) {
                panic!("expecting non-nil error when parsing {expr:?}; got {r}");
            }
        }

        f("[foo");
        f("(foo");
        // Trigger syntax.ErrInvalidRepeatOp equivalent (repeat count limit).
        f("a{0,10000}");
    }

    // Port of Go TestRegexMatchString.
    #[test]
    fn test_regex_match_string() {
        fn f(expr: &str, s: &str, result_expected: bool) {
            let r = Regex::new(expr).unwrap_or_else(|e| panic!("cannot parse {expr:?}: {e}"));
            let expr_result = r.to_string();
            assert_eq!(
                expr_result, expr,
                "unexpected string representation for {expr:?}: {expr_result:?}"
            );
            let result = r.match_string(s);
            assert_eq!(
                result, result_expected,
                "unexpected result when matching {s:?} against regex={expr:?}; got {result}; want {result_expected}"
            );
        }

        f("", "", true);
        f("", "foo", true);
        f("foo", "", false);
        f(".*", "", true);
        f(".*", "foo", true);
        f(".+", "", false);
        f(".+", "foo", true);
        f("foo.*", "bar", false);
        f("foo.*", "foo", true);
        f("foo.*", "a foo", true);
        f("foo.*", "a foo a", true);
        f("foo.*", "foobar", true);
        f("foo.*", "a foobar", true);
        f("foo.+", "bar", false);
        f("foo.+", "foo", false);
        f("foo.+", "a foo", false);
        f("foo.+", "foobar", true);
        f("foo.+", "a foobar", true);
        f("foo|bar", "", false);
        f("foo|bar", "a", false);
        f("foo|bar", "foo", true);
        f("foo|bar", "a foo", true);
        f("foo|bar", "foo a", true);
        f("foo|bar", "a foo a", true);
        f("foo|bar", "bar", true);
        f("foo|bar", "foobar", true);
        f("foo(bar|baz)", "a", false);
        f("foo(bar|baz)", "foobar", true);
        f("foo(bar|baz)", "foobaz", true);
        f("foo(bar|baz)", "foobaza", true);
        f("foo(bar|baz)", "a foobaz a", true);
        f("foo(bar|baz)", "foobal", false);
        f("^foo|b(ar)$", "foo", true);
        f("^foo|b(ar)$", "foo a", true);
        f("^foo|b(ar)$", "a foo", false);
        f("^foo|b(ar)$", "bar", true);
        f("^foo|b(ar)$", "a bar", true);
        f("^foo|b(ar)$", "barz", false);
        f("^foo|b(ar)$", "ar", false);
        f(".*foo.*", "foo", true);
        f(".*foo.*", "afoobar", true);
        f(".*foo.*", "abc", false);
        f("foo.*bar.*", "foobar", true);
        f("foo.*bar.*", "foo_bar_", true);
        f("foo.*bar.*", "a foo bar baz", true);
        f("foo.*bar.*", "foobaz", false);
        f("foo.*bar.*", "baz foo", false);
        f(".+foo.+", "foo", false);
        f(".+foo.+", "afoobar", true);
        f(".+foo.+", "afoo", false);
        f(".+foo.+", "abc", false);
        f("foo.+bar.+", "foobar", false);
        f("foo.+bar.+", "foo_bar_", true);
        f("foo.+bar.+", "a foo_bar_", true);
        f("foo.+bar.+", "foobaz", false);
        f("foo.+bar.+", "abc", false);
        f(".+foo.*", "foo", false);
        f(".+foo.*", "afoo", true);
        f(".+foo.*", "afoobar", true);
        f(".*(a|b).*", "a", true);
        f(".*(a|b).*", "ax", true);
        f(".*(a|b).*", "xa", true);
        f(".*(a|b).*", "xay", true);
        f(".*(a|b).*", "xzy", false);
        f("^(?:true)$", "true", true);
        f("^(?:true)$", "false", false);

        f(".+;|;.+", ";", false);
        f(".+;|;.+", "foo", false);
        f(".+;|;.+", "foo;bar", true);
        f(".+;|;.+", "foo;", true);
        f(".+;|;.+", ";foo", true);
        f(".+foo|bar|baz.+", "foo", false);
        f(".+foo|bar|baz.+", "afoo", true);
        f(".+foo|bar|baz.+", "fooa", false);
        f(".+foo|bar|baz.+", "afooa", true);
        f(".+foo|bar|baz.+", "bar", true);
        f(".+foo|bar|baz.+", "abar", true);
        f(".+foo|bar|baz.+", "abara", true);
        f(".+foo|bar|baz.+", "bara", true);
        f(".+foo|bar|baz.+", "baz", false);
        f(".+foo|bar|baz.+", "baza", true);
        f(".+foo|bar|baz.+", "abaz", false);
        f(".+foo|bar|baz.+", "abaza", true);
        f(".+foo|bar|baz.+", "afoo|bar|baza", true);
        f(".+(foo|bar|baz).+", "bar", false);
        f(".+(foo|bar|baz).+", "bara", false);
        f(".+(foo|bar|baz).+", "abar", false);
        f(".+(foo|bar|baz).+", "abara", true);
        f(".+(foo|bar|baz).+", "afooa", true);
        f(".+(foo|bar|baz).+", "abaza", true);

        f(".*;|;.*", ";", true);
        f(".*;|;.*", "foo", false);
        f(".*;|;.*", "foo;bar", true);
        f(".*;|;.*", "foo;", true);
        f(".*;|;.*", ";foo", true);

        f("^bar", "foobarbaz", false);
        f("^foo", "foobarbaz", true);
        f("bar$", "foobarbaz", false);
        f("baz$", "foobarbaz", true);
        f("(bar$|^foo)", "foobarbaz", true);
        f("(bar$^boo)", "foobarbaz", false);
        f("foo(bar|baz)", "a fooxfoobaz a", true);
        f("foo(bar|baz)", "a fooxfooban a", false);
        f("foo(bar|baz)", "a fooxfooban foobar a", true);

        // Trigger syntax.ErrNestingDepth equivalent
        // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1112
        f("a{0,1000}", "a", true);
    }

    // Port of Go TestGetLiterals.
    #[test]
    fn test_get_literals() {
        fn f(expr: &str, literals_expected: &[&str]) {
            let r = Regex::new(expr).unwrap_or_else(|e| panic!("cannot parse {expr:?}: {e}"));
            let literals = r.get_literals();
            assert_eq!(
                literals, literals_expected,
                "unexpected literals; got {literals:?}; want {literals_expected:?}"
            );
        }

        f("", &[]);
        f("foo bar baz", &["foo bar baz"]);
        f("foo.*bar(a|b)baz.+", &["foo", "bar", "baz"]);
        f("(foo[ab](?:bar))", &["foo", "bar"]);
        f("foo|bar", &[]);
        f("(?i)foo", &[]);
        f("foo((?i)bar)baz", &["foo", "baz"]);
        f("((foo|bar)baz xxx(?:yzabc))", &["baz xxxyzabc"]);
        f("((foo|bar)baz xxx(?:yzabc)*)", &["baz xxx"]);
        f("((foo|bar)baz? xxx(?:yzabc)*)", &["ba", " xxx"]);
    }

    // Port of Go TestPromRegexParseFailure.
    #[test]
    fn test_prom_regex_parse_failure() {
        fn f(expr: &str) {
            if PromRegex::new(expr).is_ok() {
                panic!("expecting non-nil error for expr={expr}");
            }
        }
        f("fo[bar");
        f("foo(bar");
    }

    // Port of Go TestPromRegex.
    #[test]
    fn test_prom_regex() {
        fn f(expr: &str, s: &str, result_expected: bool) {
            let pr = PromRegex::new(expr).unwrap_or_else(|e| panic!("unexpected error: {e}"));
            let expr_result = pr.to_string();
            assert_eq!(
                expr_result, expr,
                "unexpected string representation for {expr:?}: {expr_result:?}"
            );
            let result = pr.match_string(s);
            assert_eq!(
                result, result_expected,
                "unexpected result when matching {expr:?} against {s:?}; got {result}; want {result_expected}"
            );

            // Make sure the result is the same for the regular regexp.
            let expr_anchored = format!("^(?:{expr})$");
            let re = regex::Regex::new(&expr_anchored).unwrap();
            let result = re.is_match(s);
            assert_eq!(
                result, result_expected,
                "unexpected result when matching {expr_anchored:?} against {s:?} during sanity check; got {result}; want {result_expected}"
            );
        }

        f("", "", true);
        f("", "foo", false);
        f("foo", "", false);
        f(".*", "", true);
        f(".*", "foo", true);
        f(".+", "", false);
        f(".+", "foo", true);
        f("foo.*", "bar", false);
        f("foo.*", "foo", true);
        f("foo.*", "foobar", true);
        f("foo.+", "bar", false);
        f("foo.+", "foo", false);
        f("foo.+", "foobar", true);
        f("foo|bar", "", false);
        f("foo|bar", "a", false);
        f("foo|bar", "foo", true);
        f("foo|bar", "bar", true);
        f("foo|bar", "foobar", false);
        f("foo(bar|baz)", "a", false);
        f("foo(bar|baz)", "foobar", true);
        f("foo(bar|baz)", "foobaz", true);
        f("foo(bar|baz)", "foobaza", false);
        f("foo(bar|baz)", "foobal", false);
        f("^foo|b(ar)$", "foo", true);
        f("^foo|b(ar)$", "bar", true);
        f("^foo|b(ar)$", "ar", false);
        f(".*foo.*", "foo", true);
        f(".*foo.*", "afoobar", true);
        f(".*foo.*", "abc", false);
        f("foo.*bar.*", "foobar", true);
        f("foo.*bar.*", "foo_bar_", true);
        f("foo.*bar.*", "foobaz", false);
        f(".+foo.+", "foo", false);
        f(".+foo.+", "afoobar", true);
        f(".+foo.+", "afoo", false);
        f(".+foo.+", "abc", false);
        f("foo.+bar.+", "foobar", false);
        f("foo.+bar.+", "foo_bar_", true);
        f("foo.+bar.+", "foobaz", false);
        f(".+foo.*", "foo", false);
        f(".+foo.*", "afoo", true);
        f(".+foo.*", "afoobar", true);
        f(".*(a|b).*", "a", true);
        f(".*(a|b).*", "ax", true);
        f(".*(a|b).*", "xa", true);
        f(".*(a|b).*", "xay", true);
        f(".*(a|b).*", "xzy", false);
        f("^(?:true)$", "true", true);
        f("^(?:true)$", "false", false);

        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5297
        f(".+;|;.+", ";", false);
        f(".+;|;.+", "foo", false);
        f(".+;|;.+", "foo;bar", false);
        f(".+;|;.+", "foo;", true);
        f(".+;|;.+", ";foo", true);
        f(".+foo|bar|baz.+", "foo", false);
        f(".+foo|bar|baz.+", "afoo", true);
        f(".+foo|bar|baz.+", "fooa", false);
        f(".+foo|bar|baz.+", "afooa", false);
        f(".+foo|bar|baz.+", "bar", true);
        f(".+foo|bar|baz.+", "abar", false);
        f(".+foo|bar|baz.+", "abara", false);
        f(".+foo|bar|baz.+", "bara", false);
        f(".+foo|bar|baz.+", "baz", false);
        f(".+foo|bar|baz.+", "baza", true);
        f(".+foo|bar|baz.+", "abaz", false);
        f(".+foo|bar|baz.+", "abaza", false);
        f(".+foo|bar|baz.+", "afoo|bar|baza", false);
        f(".+(foo|bar|baz).+", "abara", true);
        f(".+(foo|bar|baz).+", "afooa", true);
        f(".+(foo|bar|baz).+", "abaza", true);

        f(".*;|;.*", ";", true);
        f(".*;|;.*", "foo", false);
        f(".*;|;.*", "foo;bar", false);
        f(".*;|;.*", "foo;", true);
        f(".*;|;.*", ";foo", true);

        f(".*foo(bar|baz)", "fooxfoobaz", true);
        f(".*foo(bar|baz)", "fooxfooban", false);
        f(".*foo(bar|baz)", "fooxfooban foobar", true);
    }
}
