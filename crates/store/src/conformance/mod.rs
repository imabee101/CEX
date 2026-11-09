//! The backend conformance suite: one set of checks that every
//! [`MetaStore`](crate::MetaStore) and every [`BlobStore`](crate::BlobStore)
//! backend must pass.
//!
//! A backend's test file instantiates the suite with
//! [`meta_store_conformance!`](crate::meta_store_conformance) or
//! [`blob_store_conformance!`](crate::blob_store_conformance), giving an async
//! function that returns a fresh store and a guard kept alive for the test.
//! Each check panics on a violation.

pub mod blob;
pub mod linearizability;
pub mod meta;

/// The concurrency the suite's concurrent checks run at.
pub const CONCURRENCY: usize = 64;

/// Generates one `#[tokio::test]` per [`meta`] check, each on a store made
/// by `$make().await`, which returns `(Arc<impl MetaStore>, guard)`.
#[macro_export]
macro_rules! meta_store_conformance {
    ($make:path) => {
        $crate::meta_store_conformance!(@tests $make;
            records_round_trip,
            put_never_overwrites,
            kinds_are_separate_namespaces,
            repos_are_isolated,
            batch_naming_a_key_twice_is_rejected,
            concurrent_puts_of_one_key_create_it_once,
            concurrent_overlapping_batches_all_succeed,
            large_records_round_trip,
            ref_cas_creates_moves_and_rejects,
            list_refs_in_byte_order_with_pagination,
            ref_log_pages_by_seq,
            invalid_names_and_limits_are_rejected,
            concurrent_ref_creation_has_one_winner,
            ref_cas_is_linearizable_at_64_way_concurrency,
        );
    };
    (@tests $make:path; $($check:ident,)*) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
            async fn $check() {
                let (store, _guard) = $make().await;
                $crate::conformance::meta::$check(store).await;
            }
        )*
    };
}

/// Generates one `#[tokio::test]` per [`blob`] check, each on a store made
/// by `$make().await`, which returns `(Arc<impl BlobStore>, guard)`.
#[macro_export]
macro_rules! blob_store_conformance {
    ($make:path) => {
        $crate::blob_store_conformance!(@tests $make;
            objects_round_trip,
            put_never_overwrites,
            kinds_are_separate_namespaces,
            repos_are_isolated,
            concurrent_puts_of_one_key_create_it_once,
            large_objects_round_trip,
        );
    };
    (@tests $make:path; $($check:ident,)*) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
            async fn $check() {
                let (store, _guard) = $make().await;
                $crate::conformance::blob::$check(store).await;
            }
        )*
    };
}

/// Deterministic pseudo-random bytes, so failures reproduce.
pub(crate) fn pseudo_random(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = SplitMix64(seed);
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    out.truncate(len);
    out
}

/// A small seeded generator (SplitMix64).
#[derive(Clone, Debug)]
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    /// The next 64 pseudo-random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `0..n`; `n` must be positive.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// An index into a slice of length `n`, which must be positive.
    pub fn index(&mut self, n: usize) -> usize {
        self.below(n as u64) as usize
    }

    /// True with probability `1 / n`; `n` must be positive.
    pub fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{SplitMix64, pseudo_random};

    #[test]
    fn splitmix64_matches_the_reference_sequence() {
        // The first outputs of Vigna's reference SplitMix64 from seed 0.
        let mut rng = SplitMix64(0);
        assert_eq!(rng.next_u64(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(rng.next_u64(), 0x6e78_9e6a_a1b9_65f4);
        assert_eq!(rng.next_u64(), 0x06c4_5d18_8009_454f);
        let mut rng = SplitMix64(7);
        assert!((0..1000).all(|_| rng.below(3) < 3));
    }

    #[test]
    fn pseudo_random_has_the_requested_length_and_varies() {
        for len in [0, 1, 7, 8, 9, 1000] {
            assert_eq!(pseudo_random(1, len).len(), len);
        }
        let a = pseudo_random(1, 64);
        assert_eq!(a, pseudo_random(1, 64), "not deterministic");
        assert_ne!(a, pseudo_random(2, 64), "ignores the seed");
        assert_eq!(&a[..8], &SplitMix64(1).next_u64().to_le_bytes());
        let distinct: std::collections::HashSet<u8> = a.iter().copied().collect();
        assert!(distinct.len() > 16, "too few distinct bytes: {a:?}");
    }
}
