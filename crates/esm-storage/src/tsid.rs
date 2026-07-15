//! Port of the upstream VictoriaMetrics v1.146.0 lib/storage/tsid.go.

use esm_encoding as encoding;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// The size of marshaled [`Tsid`]. Go: marshaledTSIDSize.
pub const MARSHALED_TSID_SIZE: usize = 24;

/// Tsid is unique id for a time series. Go: TSID.
///
/// Time series blocks are sorted by Tsid.
///
/// All the fields except `metric_id` are optional. They exist solely for
/// better grouping of related metrics. It is OK if their meaning differ from
/// their naming.
///
/// The derived `Ord` matches Go's `TSID.Less()` exactly: the fields are
/// compared lexicographically in declaration order
/// (metric_group_id, job_id, instance_id, metric_id). Do NOT reorder the
/// fields — this ordering is the physical sort key of blocks in parts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tsid {
    /// The id of the metric group (`xxhash64(mn.metric_group)`).
    pub metric_group_id: u64,
    /// The id of an individual job (aka service); derived from `tags[0]`.
    pub job_id: u32,
    /// The id of an instance (aka process); derived from `tags[1]`.
    pub instance_id: u32,
    /// The unique id of the metric (time series).
    pub metric_id: u64,
}

impl Tsid {
    /// Appends marshaled `self` to `dst`. Go: TSID.Marshal.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        encoding::marshal_uint64(dst, self.metric_group_id);
        encoding::marshal_uint32(dst, self.job_id);
        encoding::marshal_uint32(dst, self.instance_id);
        encoding::marshal_uint64(dst, self.metric_id);
    }

    /// Unmarshals `self` from `src` and returns the rest of `src`.
    /// Go: TSID.Unmarshal.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        if src.len() < MARSHALED_TSID_SIZE {
            return Err(format!(
                "too short src; got {} bytes; want {} bytes",
                src.len(),
                MARSHALED_TSID_SIZE
            ));
        }
        self.metric_group_id = encoding::unmarshal_uint64(src);
        let src = &src[8..];
        self.job_id = encoding::unmarshal_uint32(src);
        let src = &src[4..];
        self.instance_id = encoding::unmarshal_uint32(src);
        let src = &src[4..];
        self.metric_id = encoding::unmarshal_uint64(src);
        Ok(&src[8..])
    }

    /// Returns true if `self < other`. Go: TSID.Less.
    ///
    /// Kept for parity with the Go API; identical to the derived `Ord`.
    pub fn less(&self, other: &Tsid) -> bool {
        self < other
    }
}

/// Merges sorted Tsid slices into one. Duplicates are removed.
/// Go: mergeSortedTSIDs.
pub fn merge_sorted_tsids(tsidss: &[Vec<Tsid>]) -> Vec<Tsid> {
    let mut heap = BinaryHeap::new();
    let mut n = 0;
    for (list_idx, tsids) in tsidss.iter().enumerate() {
        if !tsids.is_empty() {
            heap.push(Reverse((tsids[0], list_idx, 0usize)));
            n += tsids.len();
        }
    }
    let mut all: Vec<Tsid> = Vec::with_capacity(n);

    while let Some(Reverse((tsid, list_idx, elem_idx))) = heap.pop() {
        if all.last() != Some(&tsid) {
            all.push(tsid);
        }
        let next_idx = elem_idx + 1;
        if next_idx < tsidss[list_idx].len() {
            heap.push(Reverse((tsidss[list_idx][next_idx], list_idx, next_idx)));
        }
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::splitmix64;

    // Port of TestMarshaledTSIDSize: makes sure marshaled format isn't
    // changed. If this test breaks then the storage format has been changed,
    // so it may become incompatible with the previously written data.
    #[test]
    fn marshaled_tsid_size() {
        let mut dst = Vec::new();
        Tsid::default().marshal(&mut dst);
        assert_eq!(dst.len(), 24);
        assert_eq!(MARSHALED_TSID_SIZE, 24);
    }

    // Golden byte layout: MetricGroupID u64be | JobID u32be | InstanceID
    // u32be | MetricID u64be.
    #[test]
    fn marshal_golden_bytes() {
        let tsid = Tsid {
            metric_group_id: 0x0102030405060708,
            job_id: 0x090a0b0c,
            instance_id: 0x0d0e0f10,
            metric_id: 0x1112131415161718,
        };
        let mut dst = Vec::new();
        tsid.marshal(&mut dst);
        let expected: Vec<u8> = (1u8..=0x18).collect();
        assert_eq!(dst, expected);
    }

    // Port of TestTSIDLess.
    #[test]
    fn tsid_less() {
        let mut t1 = Tsid::default();
        let mut t2 = Tsid::default();
        assert!(!t1.less(&t2));
        assert!(!t2.less(&t1));

        t1.metric_id = 124;
        t2.metric_id = 126;
        t1.metric_group_id = 847;
        assert!(!t1.less(&t2));
        assert!(t2.less(&t1));

        t2 = t1;
        t2.metric_id = 123;
        t1.job_id = 84;
        assert!(!t1.less(&t2));
        assert!(t2.less(&t1));

        t2 = t1;
        t2.metric_id = 123;
        t1.instance_id = 8478;
        assert!(!t1.less(&t2));
        assert!(t2.less(&t1));

        t2 = t1;
        t1.metric_id = 123847;
        assert!(!t1.less(&t2));
        assert!(t2.less(&t1));

        t2 = t1;
        assert!(!t1.less(&t2));
        assert!(!t2.less(&t1));
    }

    // Port of TestTSIDMarshalUnmarshal.
    #[test]
    fn tsid_marshal_unmarshal() {
        check_marshal_unmarshal(&Tsid::default());

        let mut state = 1u64;
        for _ in 0..1000 {
            let tsid = Tsid {
                metric_group_id: splitmix64(&mut state),
                job_id: splitmix64(&mut state) as u32,
                instance_id: splitmix64(&mut state) as u32,
                metric_id: splitmix64(&mut state),
            };
            check_marshal_unmarshal(&tsid);
        }
    }

    fn check_marshal_unmarshal(tsid: &Tsid) {
        let mut dst = Vec::new();
        tsid.marshal(&mut dst);
        assert_eq!(dst.len(), MARSHALED_TSID_SIZE);

        let mut tsid1 = Tsid::default();
        let tail = tsid1.unmarshal(&dst).expect("cannot unmarshal tsid");
        assert!(tail.is_empty(), "non-zero tail left: {tail:x?}");
        assert_eq!(*tsid, tsid1);

        // Marshal with a pre-existing prefix.
        let prefix = b"foo";
        let mut dst_new = prefix.to_vec();
        tsid.marshal(&mut dst_new);
        assert_eq!(&dst_new[..prefix.len()], prefix);
        assert_eq!(&dst_new[prefix.len()..], &dst[..]);

        // Unmarshal with a suffix.
        let suffix = b"bar";
        dst.extend_from_slice(suffix);
        let mut tsid2 = Tsid::default();
        let tail = tsid2
            .unmarshal(&dst)
            .expect("cannot unmarshal tsid from suffixed dst");
        assert_eq!(tail, suffix);
        assert_eq!(*tsid, tsid2);
    }

    #[test]
    fn tsid_unmarshal_truncated() {
        let mut dst = Vec::new();
        Tsid {
            metric_id: 42,
            ..Default::default()
        }
        .marshal(&mut dst);
        for n in 0..MARSHALED_TSID_SIZE {
            let mut tsid = Tsid::default();
            assert!(
                tsid.unmarshal(&dst[..n]).is_err(),
                "expected error for {n}-byte src"
            );
        }
    }

    // Port of TestMergeSortedTSIDs.
    #[test]
    fn merge_sorted_tsids_table() {
        fn id(metric_id: u64) -> Tsid {
            Tsid {
                metric_id,
                ..Default::default()
            }
        }

        // empty slice
        assert_eq!(merge_sorted_tsids(&[]), Vec::<Tsid>::new());

        // slice of empty slices
        assert_eq!(
            merge_sorted_tsids(&[vec![], vec![], vec![]]),
            Vec::<Tsid>::new()
        );

        // all unique
        let tsidss = vec![
            vec![id(3), id(7), id(11), id(15)],
            vec![id(1), id(5), id(9), id(13)],
            vec![id(4), id(8), id(12), id(16)],
            vec![id(2), id(6), id(10), id(14)],
        ];
        let want: Vec<Tsid> = (1..=16).map(id).collect();
        assert_eq!(merge_sorted_tsids(&tsidss), want);

        // with duplicates
        let tsidss = vec![
            vec![id(3), id(5), id(7), id(11), id(15)],
            vec![id(1), id(5), id(8), id(9), id(13)],
            vec![id(4), id(6), id(8), id(12), id(16)],
            vec![id(2), id(6), id(7), id(10), id(14)],
        ];
        assert_eq!(merge_sorted_tsids(&tsidss), want);

        // variable length
        let tsidss = vec![
            vec![id(3), id(7)],
            vec![id(1), id(5), id(9), id(13)],
            vec![],
            vec![id(4), id(8), id(16)],
            vec![id(2)],
        ];
        let want = vec![
            id(1),
            id(2),
            id(3),
            id(4),
            id(5),
            id(7),
            id(8),
            id(9),
            id(13),
            id(16),
        ];
        assert_eq!(merge_sorted_tsids(&tsidss), want);
    }
}
