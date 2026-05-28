//! `Tsid` — 24-byte unique identifier for a time series.
//!
//! Format reference: `docs/format/timeseries-part.md` §6.
//! VM source: `lib/storage/tsid.go:17-86`.

use esm_compress::int::{
    DecodeError, marshal_uint32, marshal_uint64, unmarshal_uint32, unmarshal_uint64,
};

/// 24-byte time-series identifier.
///
/// Lex-sort order over the marshalled bytes corresponds to VM's `Less`
/// (compare MetricGroupID, then JobID, then InstanceID, then MetricID).
/// `MetricID` is the only field required to be unique; the others are
/// grouping hints.
// Field names match VM's TSID; the shared `_id` suffix is intentional.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tsid {
    pub metric_group_id: u64,
    pub job_id: u32,
    pub instance_id: u32,
    pub metric_id: u64,
}

impl Tsid {
    /// Fixed on-disk size in bytes.
    pub const SIZE: usize = 24;

    /// Append the on-disk byte representation to `dst`.
    /// Field order: `BE-u64(metric_group_id) || BE-u32(job_id) ||
    /// BE-u32(instance_id) || BE-u64(metric_id)`.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_uint64(dst, self.metric_group_id);
        marshal_uint32(dst, self.job_id);
        marshal_uint32(dst, self.instance_id);
        marshal_uint64(dst, self.metric_id);
    }

    /// Parse one TSID from the head of `src`. Returns the TSID and the
    /// unconsumed remainder.
    ///
    /// # Errors
    /// Returns [`DecodeError::Truncated`] if `src.len() < Self::SIZE`.
    pub fn unmarshal(src: &[u8]) -> Result<(Self, &[u8]), DecodeError> {
        if src.len() < Self::SIZE {
            return Err(DecodeError::Truncated { needed: Self::SIZE, have: src.len() });
        }
        let (metric_group_id, n) = unmarshal_uint64(src)?;
        let src = &src[n..];
        let (job_id, n) = unmarshal_uint32(src)?;
        let src = &src[n..];
        let (instance_id, n) = unmarshal_uint32(src)?;
        let src = &src[n..];
        let (metric_id, n) = unmarshal_uint64(src)?;
        let src = &src[n..];
        Ok((Self { metric_group_id, job_id, instance_id, metric_id }, src))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_size_is_24_bytes() {
        let mut buf = Vec::new();
        Tsid::default().marshal(&mut buf);
        assert_eq!(buf.len(), Tsid::SIZE);
        assert_eq!(buf.len(), 24);
    }

    #[test]
    fn roundtrip() {
        let original = Tsid {
            metric_group_id: 0x0102_0304_0506_0708,
            job_id: 0x090a_0b0c,
            instance_id: 0x0d0e_0f10,
            metric_id: 0x1112_1314_1516_1718,
        };
        let mut buf = Vec::new();
        original.marshal(&mut buf);
        let (decoded, rest) = Tsid::unmarshal(&buf).unwrap();
        assert!(rest.is_empty());
        assert_eq!(decoded, original);
    }

    #[test]
    fn truncated_input_errors() {
        let bad = [0u8; 10];
        assert!(matches!(
            Tsid::unmarshal(&bad),
            Err(DecodeError::Truncated { needed: 24, have: 10 })
        ));
    }

    #[test]
    fn lex_order_matches_field_order() {
        let a = Tsid { metric_group_id: 1, ..Default::default() };
        let b = Tsid { metric_group_id: 2, ..Default::default() };
        assert!(a < b);

        let c = Tsid { metric_group_id: 5, job_id: 0, ..Default::default() };
        let d = Tsid { metric_group_id: 5, job_id: 1, ..Default::default() };
        assert!(c < d);
    }

    #[test]
    fn marshal_layout_be() {
        let t = Tsid { metric_group_id: 1, job_id: 2, instance_id: 3, metric_id: 4 };
        let mut buf = Vec::new();
        t.marshal(&mut buf);
        let expected = [
            0, 0, 0, 0, 0, 0, 0, 1, // metric_group_id BE u64
            0, 0, 0, 2, // job_id BE u32
            0, 0, 0, 3, // instance_id BE u32
            0, 0, 0, 0, 0, 0, 0, 4, // metric_id BE u64
        ];
        assert_eq!(buf, expected);
    }
}
