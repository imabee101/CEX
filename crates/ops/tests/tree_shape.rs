//! Equal directory contents give equal tree ids regardless of history
//! (INV-13), on Postgres and the object store: every changeset's derived
//! root equals the root a reference builder makes from the files alone,
//! including directories split into shards.

mod common;

use bytes::Bytes;
use common::*;
use scion_core::{FileKind, MAX_TREE_ENTRIES};

/// Random histories, with merges, over random paths at random depths whose
/// files become directories and back, and a directory whose size crosses
/// `MAX_TREE_ENTRIES` both ways; after each changeset the derived root
/// equals the reference root of its files.
async fn random_histories_match(seeds: std::ops::Range<u64>, wide: u64) {
    let env = env().await;
    let ops = ops(&env.meta, &env.blobs);
    let max = MAX_TREE_ENTRIES;
    // (size in the first parent, size in the child) of the wide directory.
    let mut steps = std::collections::BTreeSet::new();
    for seed in seeds {
        let mut rng = Rng(0x7eee_0000 + seed);
        let label = format!("seed {seed}");
        let h = random_history(&ops, &mut rng, 40, (6, wide), &label).await;
        let size = |m: &Model| {
            m.files
                .keys()
                .filter(|p| p.starts_with(format!("{WIDE_DIR}/").as_bytes()))
                .count()
        };
        for (i, (id, model)) in h.steps.iter().enumerate() {
            assert_eq!(root_of(&ops, *id).await, model.root(), "{label} step {i}");
            if let Some(p) = h.first_parents[i] {
                steps.insert((size(&h.steps[p].1), size(model)));
            }
        }
    }
    if wide > 0 {
        // The directory splits, and collapses back to a flat node.
        assert!(
            steps.iter().any(|&(a, b)| a <= max && b > max)
                && steps.iter().any(|&(a, b)| a > max && b <= max),
            "the wide directory did not cross the split both ways: {steps:?}"
        );
    }
}

#[tokio::test]
async fn random_histories_match_the_reference_builder() {
    random_histories_match(0..8, 0).await;
}

#[tokio::test]
async fn random_histories_across_the_split_match_the_reference_builder() {
    random_histories_match(0..3, MAX_TREE_ENTRIES as u64 + 4).await;
}

/// Two different histories that end with the same files end with the same
/// root, and the same subtree ids.
#[tokio::test]
async fn different_histories_reach_the_same_ids() {
    let env = env().await;
    let ops = ops(&env.meta, &env.blobs);
    let mut m1 = Model::default();
    let (c, b) = m1.apply(&[
        ("src/a.rs", Some((FileKind::Regular, b"a"))),
        ("src/b.rs", Some((FileKind::Regular, b"b"))),
        ("doc/x", Some((FileKind::Regular, b"x"))),
    ]);
    let one = commit(&ops, &[], "at once", c, &b).await.unwrap();

    let mut m2 = Model::default();
    let (c, b) = m2.apply(&[
        ("doc/x", Some((FileKind::Executable, b"x0"))),
        ("tmp/junk", Some((FileKind::Regular, b"j"))),
        ("src/b.rs", Some((FileKind::Regular, b"b"))),
    ]);
    let s1 = commit(&ops, &[], "step 1", c, &b).await.unwrap();
    let (c, b) = m2.apply(&[
        ("tmp/junk", None),
        ("doc/x", Some((FileKind::Regular, b"x"))),
        ("src/a.rs", Some((FileKind::Regular, b"a"))),
    ]);
    let s2 = commit(&ops, &[s1], "step 2", c, &b).await.unwrap();
    assert_eq!(root_of(&ops, one).await, root_of(&ops, s2).await);
    assert_eq!(root_of(&ops, one).await, m1.root());
}

fn names(range: std::ops::Range<u32>, prefix: &str) -> Vec<String> {
    range.map(|i| format!("big/{prefix}{i:05}")).collect()
}

/// A directory crosses `MAX_TREE_ENTRIES` both ways, its common prefix
/// shrinks and grows, and a shard splits again; every step equals the
/// reference.
#[tokio::test]
async fn sharded_directories_match_the_reference_builder() {
    let env = env().await;
    let ops = ops(&env.meta, &env.blobs);
    let mut model = Model::default();
    let mut parent: Option<scion_core::ChangesetId> = None;
    let max = MAX_TREE_ENTRIES as u32;
    let steps: Vec<(Vec<String>, Vec<String>)> = vec![
        // Exactly the flat maximum, then one more: the node splits.
        (names(0..max, "x"), vec![]),
        (names(max..max + 1, "x"), vec![]),
        // A name equal to the common prefix, and modifications inside shards.
        (
            vec!["big/x".into(), "big/x00007".into(), "big/x04000".into()],
            vec![],
        ),
        // A name outside the prefix shortens it to "" and reshapes the node.
        (names(0..5, "y"), vec![]),
        // One shard grows past the maximum and splits again.
        (names(10_000..10_000 + max + 10, "x"), vec![]),
        // Deleting every name outside one group lengthens the prefix.
        (vec![], names(0..5, "y")),
        // Back under the maximum: the node collapses to one flat node.
        (
            vec![],
            [
                names(0..max + 1, "x"),
                names(10_000..10_000 + max + 10, "x"),
            ]
            .concat()[..max as usize * 2 - 3990]
                .to_vec(),
        ),
        // One past the maximum again, then back to exactly the maximum: the
        // node splits, then collapses to one flat node of the most names.
        (names(20_000..20_095, "x"), vec![]),
        (vec![], names(20_000..20_001, "x")),
    ];
    for (i, (writes, deletes)) in steps.into_iter().enumerate() {
        let content = format!("v{i}");
        let spec: Vec<Spec<'_>> = writes
            .iter()
            .map(|p| (p.as_str(), Some((FileKind::Regular, content.as_bytes()))))
            .chain(deletes.iter().map(|p| (p.as_str(), None)))
            .collect();
        let (fc, contents) = model.apply(&spec);
        let contents: Vec<Bytes> = contents.into_iter().take(1).collect();
        let parents: Vec<_> = parent.into_iter().collect();
        let id = commit(&ops, &parents, &format!("step {i}"), fc, &contents)
            .await
            .unwrap();
        assert_eq!(root_of(&ops, id).await, model.root(), "step {i}");
        parent = Some(id);
    }
    let big = model
        .files
        .keys()
        .filter(|k| k.starts_with(b"big/"))
        .count();
    assert_eq!(big, MAX_TREE_ENTRIES);
}

/// A deleted file can come back unchanged and the tree returns to its old id.
#[tokio::test]
async fn reverting_a_change_restores_the_tree_id() {
    let env = env().await;
    let ops = ops(&env.meta, &env.blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        ("a/b", Some((FileKind::Regular, b"1"))),
        ("c", Some((FileKind::Regular, b"2"))),
    ]);
    let first = commit(&ops, &[], "1", c, &b).await.unwrap();
    let (c, b) = m.apply(&[("a/b", None)]);
    let second = commit(&ops, &[first], "2", c, &b).await.unwrap();
    let (c, b) = m.apply(&[("a/b", Some((FileKind::Regular, b"1")))]);
    let third = commit(&ops, &[second], "3", c, &b).await.unwrap();
    assert_ne!(root_of(&ops, first).await, root_of(&ops, second).await);
    assert_eq!(root_of(&ops, first).await, root_of(&ops, third).await);
}
