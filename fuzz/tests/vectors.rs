//! Runs every fuzz check over the golden vectors, their truncations, and
//! known-bad inputs, asserting each check's outcome: a check accepts exactly
//! what its decoder must accept, and flags a decoder that accepts a
//! non-canonical encoding.

#[path = "../../crates/scion-core/tests/support/mod.rs"]
mod support;

use fuzz::{FrameOutcome, Outcome};
use support::vectors::{self, Ty};

const VECTORS: &str = include_str!("../../crates/scion-core/vectors/v1.txt");

fn outcome(accept: bool) -> Outcome {
    if accept {
        Outcome::Accepted
    } else {
        Outcome::Rejected
    }
}

/// The language-neutral vectors, which scion-core's tests check equal its
/// golden vectors.
fn golden() -> Vec<vectors::Vector> {
    let all = vectors::parse(VECTORS).expect("the vectors file parses");
    assert!(!all.is_empty());
    all
}

/// Each decoder's check gives the vector's expected outcome on the vectors
/// of its type, and rejects the vectors of every other decoded type.
#[test]
fn checks_give_each_vectors_expected_outcome() {
    type Check = fn(&[u8]) -> Outcome;
    let checks: [(Ty, Check); 3] = [
        (Ty::Changeset, fuzz::check_changeset),
        (Ty::Tree, fuzz::check_tree),
        (Ty::ChunkList, fuzz::check_chunk_list),
    ];
    let mut seen = [[0usize; 2]; 4];
    for v in golden() {
        for (i, (ty, check)) in checks.iter().enumerate() {
            if v.ty == *ty {
                assert_eq!(check(&v.bytes), outcome(v.accept), "{}", v.name);
                seen[i][usize::from(v.accept)] += 1;
            } else if v.ty != Ty::Blob && v.ty != Ty::Chunk && v.accept {
                // An accepted encoding of another format is never also a
                // canonical encoding of this one: the leading bytes differ.
                assert_eq!(check(&v.bytes), Outcome::Rejected, "{} as {ty:?}", v.name);
            }
        }
        if v.ty == Ty::Frame {
            let got = fuzz::check_frame(&v.bytes);
            assert_eq!(got.given, outcome(v.accept), "{}", v.name);
            if v.accept {
                assert_eq!(got.resigned, Outcome::Accepted, "{}", v.name);
            }
            seen[3][usize::from(v.accept)] += 1;
        }
    }
    for (i, [rejected, accepted]) in seen.into_iter().enumerate() {
        assert!(rejected > 0 && accepted > 0, "check {i}: {seen:?}");
    }
}

/// The checks hold on every prefix of every vector: a truncation is either
/// rejected or, like an empty chunk list, canonical in its own right.
#[test]
fn checks_hold_on_every_truncation() {
    for v in golden() {
        // Every prefix of the small vectors; a few of the large ones.
        let step = (v.bytes.len() / 64).max(1);
        for end in (0..v.bytes.len()).step_by(step) {
            let b = &v.bytes[..end];
            let _ = fuzz::check_changeset(b);
            let _ = fuzz::check_tree(b);
            let _ = fuzz::check_chunk_list(b);
            let _ = fuzz::check_id_hex(b);
            let got = fuzz::check_frame(b);
            if v.ty == Ty::Frame && v.accept {
                assert_eq!(got.given, Outcome::Rejected, "{} cut at {end}", v.name);
            }
        }
    }
}

/// A frame whose digest is wrong is rejected as given and accepted once
/// re-signed; input shorter than a digest is rejected both ways.
#[test]
fn the_frame_check_re_signs_its_input() {
    let frames: Vec<_> = golden()
        .into_iter()
        .filter(|v| v.ty == Ty::Frame && v.accept)
        .collect();
    assert!(!frames.is_empty());
    for v in frames {
        let mut bad = v.bytes.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert_eq!(
            fuzz::check_frame(&bad),
            FrameOutcome {
                given: Outcome::Rejected,
                resigned: Outcome::Accepted
            },
            "{}",
            v.name
        );
    }
    for len in [0, 1, 31] {
        assert_eq!(
            fuzz::check_frame(&vec![0; len]),
            FrameOutcome {
                given: Outcome::Rejected,
                resigned: Outcome::Rejected
            }
        );
    }
}

/// Every id in the vectors is accepted as its lowercase hex, and rejected
/// in upper case, truncated, extended, or as invalid UTF-8.
#[test]
fn the_id_check_accepts_exactly_lowercase_hex_ids() {
    let ids: Vec<String> = golden()
        .into_iter()
        .filter_map(|v| v.id)
        .map(|id| support::to_hex(&id))
        .collect();
    assert!(!ids.is_empty());
    for id in ids {
        assert_eq!(fuzz::check_id_hex(id.as_bytes()), Outcome::Accepted, "{id}");
        let upper = id.to_uppercase();
        if upper != id {
            assert_eq!(fuzz::check_id_hex(upper.as_bytes()), Outcome::Rejected);
        }
        assert_eq!(fuzz::check_id_hex(&id.as_bytes()[1..]), Outcome::Rejected);
        assert_eq!(
            fuzz::check_id_hex(format!("{id}0").as_bytes()),
            Outcome::Rejected
        );
        let mut not_utf8 = id.into_bytes();
        not_utf8[0] = 0xff;
        assert_eq!(fuzz::check_id_hex(&not_utf8), Outcome::Rejected);
    }
}

/// The derived decoders' checks accept their own encodings, reject the empty
/// input, a truncation, a trailing byte, and a non-minimal varint, and hold
/// on every single-bit change, which must either be rejected or decode to a
/// value that re-encodes to the changed bytes.
#[test]
fn derived_decoder_checks_accept_encodings_and_reject_bad_input() {
    use std::collections::BTreeMap;

    use scion_core::{ChangesetId, RepoPath, TreeId};

    // A record of size 300, whose varint is the two bytes after the version.
    let record = derived::BlobRecord {
        size: 300,
        sha256: [7; 32],
        git_sha1: Some([9; 20]),
        text: true,
    }
    .encode();
    let roots = derived::RestrictedRoots::encode(&BTreeMap::from([
        (
            RepoPath::new(&b"a"[..]).unwrap(),
            TreeId::from_bytes([1; 32]),
        ),
        (
            RepoPath::new(&b"a/b"[..]).unwrap(),
            TreeId::from_bytes([2; 32]),
        ),
    ]));
    let node = derived::GraphNode {
        parents: vec![
            ChangesetId::from_bytes([3; 32]),
            ChangesetId::from_bytes([4; 32]),
        ],
        generation: 200,
        p1_depth: 199,
        p1_skew: ChangesetId::from_bytes([3; 32]),
        skip_depth: 7,
        skip_parent: Some(ChangesetId::from_bytes([5; 32])),
        skip_skew: ChangesetId::from_bytes([5; 32]),
    }
    .encode();
    // Each value's first varint, written with a redundant continuation byte.
    let non_minimal = |bytes: &[u8], at: usize, len: usize| {
        let mut out = bytes[..at].to_vec();
        out.extend_from_slice(&bytes[at..at + len]);
        *out.last_mut().unwrap() |= 0x80;
        out.push(0);
        out.extend_from_slice(&bytes[at + len..]);
        out
    };
    type Check<'a> = (&'a str, &'a [u8], Vec<u8>, fn(&[u8]) -> Outcome);
    let checks: [Check<'_>; 3] = [
        (
            "blob record",
            &record,
            non_minimal(&record, 1, 2),
            fuzz::check_blob_record,
        ),
        (
            "RestrictedRoots",
            &roots,
            non_minimal(&roots, 0, 1),
            fuzz::check_restricted_roots,
        ),
        (
            "CommitGraph",
            &node,
            non_minimal(&node, 0, 1),
            fuzz::check_graph_node,
        ),
    ];
    for (what, bytes, non_minimal, check) in checks {
        assert_eq!(check(bytes), Outcome::Accepted, "{what}");
        assert_eq!(check(&[]), Outcome::Rejected, "{what} empty");
        assert_eq!(
            check(&bytes[..bytes.len() - 1]),
            Outcome::Rejected,
            "{what} truncated"
        );
        assert_eq!(
            check(&[bytes, &[0][..]].concat()),
            Outcome::Rejected,
            "{what} with a trailing byte"
        );
        assert_eq!(check(&non_minimal), Outcome::Rejected, "{what} non-minimal");
        for end in 0..bytes.len() {
            let _ = check(&bytes[..end]);
        }
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut changed = bytes.to_vec();
                changed[i] ^= 1 << bit;
                let _ = check(&changed);
            }
        }
    }
}

/// A decoder that ignores a trailing byte accepts a non-canonical input,
/// and the shared check flags it.
#[test]
#[should_panic(expected = "accepted a non-canonical lenient format")]
fn the_canonical_check_flags_a_lenient_decoder() {
    let lenient = |data: &[u8]| -> Result<Vec<u8>, ()> {
        data.strip_suffix(&[0]).map(<[u8]>::to_vec).ok_or(())
    };
    let data = [1, 2, 0];
    let _ = fuzz::canonical(&data, lenient(&data), |v| v.clone(), "lenient format");
}

/// A canonical decoder passes the shared check, which reports its outcome.
#[test]
fn the_canonical_check_reports_outcomes() {
    let exact = |data: &[u8]| -> Result<Vec<u8>, ()> {
        if data.is_empty() {
            Err(())
        } else {
            Ok(data.to_vec())
        }
    };
    let (got, value) = fuzz::canonical(&[1, 2], exact(&[1, 2]), |v| v.clone(), "exact");
    assert_eq!((got, value), (Outcome::Accepted, Some(vec![1, 2])));
    let (got, value) = fuzz::canonical(&[], exact(&[]), |v| v.clone(), "exact");
    assert_eq!((got, value), (Outcome::Rejected, None));
}
