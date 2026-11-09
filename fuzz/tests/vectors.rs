//! Runs every fuzz check over the golden vectors, the seeds `cargo xtask`
//! gives the fuzzers, and their truncations.

const VECTORS: &str = include_str!("../../crates/scion-core/vectors/v1.txt");

#[test]
fn checks_pass_on_golden_vectors_and_their_prefixes() {
    let mut inputs = 0;
    for line in VECTORS.lines() {
        let Some(hex) = line.strip_prefix("bytes:") else {
            continue;
        };
        let hex = hex.trim();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        // Every prefix of the small vectors; a few of the large ones.
        let step = (bytes.len() / 64).max(1);
        for end in (0..=bytes.len()).step_by(step).chain([bytes.len()]) {
            let b = &bytes[..end];
            fuzz::check_changeset(b);
            fuzz::check_tree(b);
            fuzz::check_frame(b);
            fuzz::check_chunk_list(b);
            fuzz::check_id_hex(b);
        }
        inputs += 1;
    }
    let records = VECTORS.lines().filter(|l| l.starts_with("name:")).count();
    assert!(records > 0);
    assert_eq!(inputs, records, "every record has bytes");
}

#[test]
fn id_check_passes_on_every_vector_id_and_its_variants() {
    let ids: Vec<&str> = VECTORS
        .lines()
        .filter_map(|l| l.strip_prefix("id:"))
        .map(str::trim)
        .collect();
    assert!(!ids.is_empty());
    for id in ids {
        fuzz::check_id_hex(id.as_bytes());
        fuzz::check_id_hex(id.to_uppercase().as_bytes());
        fuzz::check_id_hex(&id.as_bytes()[1..]);
        fuzz::check_id_hex(format!("{id}0").as_bytes());
    }
}

/// The derived decoders' checks pass on their own encodings, on every
/// truncation of them, and on single-byte changes, which must either be
/// rejected or decode to a value that re-encodes to the changed bytes.
#[test]
fn derived_decoder_checks_pass_on_encodings_and_their_changes() {
    use std::collections::BTreeMap;

    use scion_core::{ChangesetId, RepoPath, TreeId};

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
    type Check<'a> = (&'a [u8], fn(&[u8]));
    let checks: [Check<'_>; 3] = [
        (&record, fuzz::check_blob_record),
        (&roots, fuzz::check_restricted_roots),
        (&node, fuzz::check_graph_node),
    ];
    for (bytes, check) in checks {
        for end in 0..=bytes.len() {
            check(&bytes[..end]);
        }
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut changed = bytes.to_vec();
                changed[i] ^= 1 << bit;
                check(&changed);
            }
        }
    }
}
