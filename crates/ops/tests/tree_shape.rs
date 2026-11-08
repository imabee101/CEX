//! Equal directory contents give equal tree ids regardless of history
//! (INV-13), on Postgres and the object store: every changeset's derived
//! root equals the root a reference builder makes from the files alone,
//! including directories split into shards.

mod common;

use bytes::Bytes;
use common::*;
use scion_core::{FileChange, FileKind, MAX_TREE_ENTRIES};

const PATHS: &[&str] = &[
    "a", "b", "a.txt", "d/a", "d/b", "d/e/f", "d/e/g", "d/e/h/i", "x/y/z/w", "x/y/q", "Q", "d/e",
    "x/y", "x", "d/e/h",
];

/// Random histories over paths that turn files into directories and back;
/// after each changeset the derived root equals the reference root.
#[tokio::test]
async fn random_histories_match_the_reference_builder() {
    let env = env().await;
    let ops = ops(&env.meta, &env.blobs);
    for seed in 0..6u64 {
        let mut rng = Rng(seed);
        let mut model = Model::default();
        let mut parent = None;
        let mut done = 0;
        while done < 12 {
            let mut changes: Vec<OwnedSpec> = Vec::new();
            let mut next = model.clone();
            for _ in 0..1 + rng.below(4) {
                let p = PATHS[rng.below(PATHS.len() as u64) as usize];
                if changes.iter().any(|(q, _)| q == p) {
                    continue;
                }
                let c = if next.files.contains_key(p.as_bytes()) && rng.below(2) == 0 {
                    None
                } else {
                    let kind = [FileKind::Regular, FileKind::Executable, FileKind::Symlink]
                        [rng.below(3) as usize];
                    Some((kind, format!("{seed}-{}", rng.below(3)).into_bytes()))
                };
                changes.push((p.to_owned(), c));
            }
            let spec: Vec<Spec<'_>> = changes
                .iter()
                .map(|(p, c)| (p.as_str(), c.as_ref().map(|(k, b)| (*k, b.as_slice()))))
                .collect();
            let (fc, contents) = next.apply(&spec);
            let parents: Vec<_> = parent.into_iter().collect();
            match commit(&ops, &parents, &format!("{seed}/{done}"), fc, &contents).await {
                Ok(id) => {
                    assert_eq!(
                        root_of(&ops, id).await,
                        next.root(),
                        "seed {seed} step {done}"
                    );
                    model = next;
                    parent = Some(id);
                    done += 1;
                }
                // Invalid random change sets (no-ops, file/dir conflicts)
                // are rejected; the model is left as it was.
                Err(e) => assert!(
                    matches!(e.code, ops::Code::NoOpChange | ops::Code::PathConflict),
                    "seed {seed}: {e}"
                ),
            }
        }
    }
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
    assert!(big <= MAX_TREE_ENTRIES && big > 0, "{big}");
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
    let _ = FileChange::Deleted;
}
