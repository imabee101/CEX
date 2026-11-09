//! The case-conflict check (INV-19) keeps create cost at O(changed files ×
//! path depth) (INV-1), on Postgres and the object store: adding one name to
//! a sharded directory reads only nodes on the spines it touches, however
//! many names the directory holds, and still finds conflicts with names in
//! other shards.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::{REPO, commit, env, path, write};
use derived::{CacheSizes, Deriver};
use ops::blob::upload_blobs;
use ops::{Code, CreateOptions, Ops};
use scion_core::{ChangesetId, FileChange, RepoPath};
use store::fault::{FaultBlobStore, FaultMetaStore, Faults};
use store::{BlobStore, MetaStore};

/// A repo holding `dir/f<i:06>` for `i < n`, all with one content.
async fn big_dir<M: MetaStore, B: BlobStore>(
    meta: &Arc<M>,
    blobs: &Arc<B>,
    n: usize,
) -> ChangesetId {
    let ops = common::ops(meta, blobs);
    let changes: Vec<(RepoPath, FileChange)> = (0..n)
        .map(|i| (path(&format!("dir/f{i:06}")), write(b"same\n")))
        .collect();
    commit(&ops, &[], "big", changes, &[Bytes::from_static(b"same\n")])
        .await
        .unwrap()
}

/// Metadata keys a cold server reads to add `name` to `dir` on `parent`,
/// and the create's result code.
async fn add_cost<M: MetaStore, B: BlobStore>(
    meta: &Arc<M>,
    blobs: &Arc<B>,
    parent: ChangesetId,
    name: &str,
) -> (u64, Option<Code>) {
    upload_blobs(&**meta, blobs, REPO, &[Bytes::from_static(b"new\n")])
        .await
        .unwrap();
    let faults = Faults::new();
    let fm = Arc::new(FaultMetaStore::new(meta.clone(), faults.clone()));
    let fb = Arc::new(FaultBlobStore::new(blobs.clone(), faults.clone()));
    let ops = Ops::new(Arc::new(Deriver::new(fm, fb, CacheSizes::default())));
    let cs = common::changeset(
        &[parent],
        "add",
        vec![(path(&format!("dir/{name}")), write(b"new\n"))],
    );
    let r = ops
        .create_changeset(REPO, cs, &CreateOptions::default())
        .await;
    (faults.counts().meta_get_keys, r.err().map(|e| e.code))
}

#[tokio::test(flavor = "multi_thread")]
async fn case_check_cost_does_not_grow_with_directory_size() {
    let e = env().await;
    // 5,000 names split into 1 + 5 nodes (prefix "f00"); 100,000 names split
    // into 1 + 10 + 100 nodes (prefixes "f0", then "f0X"): one more trie level.
    let small = big_dir(&e.meta, &e.blobs, 5_000).await;
    let large = big_dir(&e.meta, &e.blobs, 100_000).await;
    let (s, sc) = add_cost(&e.meta, &e.blobs, small, "f001234x").await;
    let (l, lc) = add_cost(&e.meta, &e.blobs, large, "f001234x").await;
    assert_eq!((sc, lc), (None, None));
    eprintln!("metadata keys read: 5,000 names {s}, 100,000 names {l}");
    // One more trie level costs at most one more node read on each tree the
    // create walks; reading the whole directory costs ~100 more.
    assert!(
        l <= s + 4,
        "adding one name read {l} keys in a 100,000-name directory, {s} in a 5,000-name one"
    );
    // Conflicts are still found across shards and trie levels.
    for (parent, n) in [(small, "F000007"), (large, "F099999")] {
        let (_, code) = add_cost(&e.meta, &e.blobs, parent, n).await;
        assert_eq!(code, Some(Code::CaseConflict), "{n}");
    }
}

/// The index check equals a naive model that folds every name of the
/// directory, on random adds and deletes in a sharded directory and a small
/// flat one, with conflicting pairs already present (created with the
/// check off) that later adds must still see.
#[tokio::test(flavor = "multi_thread")]
async fn case_check_equals_naive_model() {
    use ops::case::fold_key;
    use std::collections::BTreeSet;
    let e = env().await;
    let ops = common::ops(&e.meta, &e.blobs);
    let mut tip = big_dir(&e.meta, &e.blobs, 4_500).await;
    let mut names: BTreeSet<String> = (0..4_500).map(|i| format!("dir/f{i:06}")).collect();
    let stems = [
        "f000123",
        "F000123",
        "x",
        "X",
        "\u{212a}",
        "k",
        "caf\u{e9}",
        "cafe\u{301}",
    ];
    let on = CreateOptions::default();
    let off = CreateOptions {
        case_check: false,
        ..CreateOptions::default()
    };
    upload_blobs(&*e.meta, &e.blobs, REPO, &[Bytes::from_static(b"v\n")])
        .await
        .unwrap();
    let mut rng = common::Rng(0x5eed_ca5e);
    for round in 0..120 {
        let dir = if rng.below(3) == 0 { "small" } else { "dir" };
        let stem = stems[rng.below(stems.len() as u64) as usize];
        let suffix = ["", "1", "Q", "q"][rng.below(4) as usize];
        let p = format!("{dir}/{stem}{suffix}");
        let delete = names.contains(&p);
        let change = if delete {
            FileChange::Deleted
        } else {
            write(b"v\n")
        };
        let base = |n: &str| n.rsplit('/').next().unwrap().as_bytes().to_vec();
        let conflict = !delete
            && names.iter().any(|n| {
                n.starts_with(&format!("{dir}/"))
                    && *n != p
                    && fold_key(&base(n)) == fold_key(&base(&p))
            });
        let unchecked = !delete && rng.below(5) == 0;
        let cs = common::changeset(&[tip], &format!("r{round}"), vec![(path(&p), change)]);
        let r = ops
            .create_changeset(REPO, cs, if unchecked { &off } else { &on })
            .await;
        if conflict && !unchecked {
            let err = r.expect_err(&p);
            assert_eq!(err.code, Code::CaseConflict, "round {round} {p}");
            assert_eq!(err.paths, vec![p.as_bytes().to_vec()]);
            continue;
        }
        tip = r.unwrap_or_else(|e| panic!("round {round} {p}: {e}"));
        if delete {
            names.remove(&p);
        } else {
            names.insert(p);
        }
    }
}

/// Removing a name from a sharded directory that stays sharded rewrites only
/// the spine (INV-1): it does not read the whole directory.
#[tokio::test(flavor = "multi_thread")]
async fn removing_a_name_does_not_grow_with_directory_size() {
    let e = env().await;
    let small = big_dir(&e.meta, &e.blobs, 5_000).await;
    let large = big_dir(&e.meta, &e.blobs, 100_000).await;
    let mut costs = Vec::new();
    for parent in [small, large] {
        let faults = Faults::new();
        let fm = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let fb = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let ops = Ops::new(Arc::new(Deriver::new(fm, fb, CacheSizes::default())));
        let cs = common::changeset(
            &[parent],
            "remove",
            vec![(path("dir/f001234"), FileChange::Deleted)],
        );
        ops.create_changeset(REPO, cs, &CreateOptions::default())
            .await
            .unwrap();
        costs.push(faults.counts().meta_get_keys);
    }
    eprintln!(
        "metadata keys read: 5,000 names {}, 100,000 names {}",
        costs[0], costs[1]
    );
    assert!(costs[1] <= costs[0] + 4, "{costs:?}");
}

/// Adding a name outside a sharded directory's common prefix (here "g…" to
/// a directory of "f…" names) keeps the existing shards: the cost does not
/// grow with the directory's size (INV-1).
#[tokio::test(flavor = "multi_thread")]
async fn adding_a_name_outside_the_common_prefix_does_not_grow_with_directory_size() {
    let e = env().await;
    let small = big_dir(&e.meta, &e.blobs, 5_000).await;
    let large = big_dir(&e.meta, &e.blobs, 100_000).await;
    upload_blobs(&*e.meta, &e.blobs, REPO, &[Bytes::from_static(b"new\n")])
        .await
        .unwrap();
    let mut costs = Vec::new();
    let mut model_roots = Vec::new();
    for (parent, n) in [(small, 5_000), (large, 100_000)] {
        let faults = Faults::new();
        let fm = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let fb = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let ops = Ops::new(Arc::new(Deriver::new(fm, fb, CacheSizes::default())));
        let cs = common::changeset(
            &[parent],
            "outside",
            vec![
                (path("dir/g"), write(b"new\n")),
                (path("dir/0"), write(b"new\n")),
            ],
        );
        let id = ops
            .create_changeset(REPO, cs, &CreateOptions::default())
            .await
            .unwrap();
        let c = faults.counts();
        costs.push((c.meta_get_keys, c.meta_put_entries));
        // The result is still the canonical tree.
        let files = (0..n)
            .map(|i| {
                (
                    format!("dir/f{i:06}").into_bytes(),
                    scion_core::FileKind::Regular,
                    b"same\n".to_vec(),
                )
            })
            .chain(["dir/g", "dir/0"].map(|p| {
                (
                    p.as_bytes().to_vec(),
                    scion_core::FileKind::Regular,
                    b"new\n".to_vec(),
                )
            }));
        model_roots.push((
            common::root_of(&ops, id).await,
            common::reference_root(files),
        ));
    }
    for (got, want) in &model_roots {
        assert_eq!(got, want);
    }
    eprintln!(
        "(keys read, rows written): 5,000 names {:?}, 100,000 names {:?}",
        costs[0], costs[1]
    );
    assert!(costs[1].0 <= costs[0].0 + 4, "{costs:?}");
    assert!(costs[1].1 <= costs[0].1 + 4, "{costs:?}");
}

/// Adding a name to a sharded directory whose groups are small and flat
/// reads only the group it lands in: a split directory that gains names
/// stays split without counting its other groups (INV-1).
#[tokio::test(flavor = "multi_thread")]
async fn adding_a_name_to_a_directory_of_many_small_groups_reads_one_group() {
    let e = env().await;
    let ops = common::ops(&e.meta, &e.blobs);
    let alphabet: Vec<char> = ('a'..='z')
        .chain('0'..='9')
        .chain("!#$%&'()+,-;=@[]^_`{}~".chars())
        .collect();
    // 58 groups of 73 names (4,234 names, split, every group flat; no two
    // differ only by case) in
    // `dir`, and 4,000 names (flat) in `flat`.
    let mut changes: Vec<(RepoPath, FileChange)> = Vec::new();
    for c in &alphabet {
        for i in 0..73 {
            changes.push((path(&format!("dir/{c}{i:03}")), write(b"same\n")));
        }
    }
    for i in 0..4_000 {
        changes.push((path(&format!("flat/n{i:04}")), write(b"same\n")));
    }
    let base = commit(
        &ops,
        &[],
        "groups",
        changes,
        &[Bytes::from_static(b"same\n")],
    )
    .await
    .unwrap();
    upload_blobs(&*e.meta, &e.blobs, REPO, &[Bytes::from_static(b"new\n")])
        .await
        .unwrap();
    // Each scenario on the flat directory, then on the split one: one added
    // name; and one name removed with another added after it, which leaves
    // the count unchanged.
    let scenarios: [[(&str, Option<&str>); 2]; 2] = [
        [("flat/zz", None), ("dir/q500", None)],
        [
            ("flat/n0001", Some("flat/zz")),
            ("dir/a001", Some("dir/q500")),
        ],
    ];
    for scenario in scenarios {
        let mut costs = Vec::new();
        for (first, second) in scenario {
            let faults = Faults::new();
            let fm = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
            let fb = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
            let cold = Ops::new(Arc::new(Deriver::new(fm, fb, CacheSizes::default())));
            let changes = match second {
                None => vec![(path(first), write(b"new\n"))],
                Some(added) => vec![
                    (path(first), FileChange::Deleted),
                    (path(added), write(b"new\n")),
                ],
            };
            let cs = common::changeset(&[base], first, changes);
            cold.create_changeset(REPO, cs, &CreateOptions::default())
                .await
                .unwrap();
            costs.push(faults.counts().meta_get_keys);
        }
        eprintln!(
            "metadata keys read: flat directory {}, 58-group directory {}",
            costs[0], costs[1]
        );
        // One more trie level on each tree the create walks; counting every
        // group would read dozens more.
        assert!(costs[1] <= costs[0] + 4, "{costs:?}");
    }
}
