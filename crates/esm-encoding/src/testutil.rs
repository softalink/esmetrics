//! Shared test helpers: a deterministic RNG replacing Go's math/rand, hex
//! formatting, and checkPrecisionBits from nearest_delta2_test.go.

/// Deterministic xorshift64* RNG. Replaces Go's rand.New(rand.NewSource(seed));
/// values only need coverage, not the exact Go sequences.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in [0, 1). Replaces Go's rand.Float64.
    pub(crate) fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Approximate standard normal via Irwin–Hall (sum of 12 uniforms − 6).
    /// Replaces Go's rand.NormFloat64.
    pub(crate) fn norm_f64(&mut self) -> f64 {
        (0..12).map(|_| self.f64()).sum::<f64>() - 6.0
    }

    /// Uniform in [0, n). Replaces Go's rand.Int63n.
    pub(crate) fn i64n(&mut self, n: i64) -> i64 {
        (self.next_u64() % n as u64) as i64
    }

    /// Uniform byte. Replaces Go's rand.Int31n(256).
    pub(crate) fn byte(&mut self) -> u8 {
        (self.next_u64() & 0xff) as u8
    }
}

/// Formats `b` as a lowercase hex string, like Go's fmt.Sprintf("%x", b).
pub(crate) fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Port of checkPrecisionBits from nearest_delta2_test.go.
pub(crate) fn check_precision_bits_arrays(
    a: &[i64],
    b: &[i64],
    precision_bits: u8,
) -> Result<(), String> {
    if a.len() != b.len() {
        return Err(format!(
            "different-sized arrays: {} vs {}",
            a.len(),
            b.len()
        ));
    }
    for (i, (&av0, &bv0)) in a.iter().zip(b.iter()).enumerate() {
        let (mut av, bv) = if av0 < bv0 { (bv0, av0) } else { (av0, bv0) };
        let eps = av - bv;
        if eps == 0 {
            continue;
        }
        if av < 0 {
            av = -av;
        }
        let mut pbe = 1u8;
        while eps < av {
            av >>= 1;
            pbe += 1;
        }
        if pbe < precision_bits {
            return Err(format!(
                "too low precisionBits; got {pbe}; expecting {precision_bits}; compared values: {} vs {}, eps={eps}",
                a[i], b[i]
            ));
        }
    }
    Ok(())
}
