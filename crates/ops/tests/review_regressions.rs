//! Cost and isolation regressions from the M3 review, on Postgres and the
//! object store with cold server caches.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::*;
use derived::{AclConfig, DerivedType, RestrictedRoots};
use ops::{Code, CreateOptions, DiffMode};
use scion_core::{EntryTarget, Tree};
use store::fault::{FaultBlobStore, FaultMetaStore, Faults, Write};
use store::{BlobStore, MetaKey, MetaStore, ObjectKey};

#[tokio::test]
async fn old_case_index_versions_require_explicit_backfill() {
    let e = env().await;
    let parent = changeset(&[], "legacy parent", vec![]);
    let id = parent.id();
    let old_key = store::DerivedKey {
        version: 1,
        ..ops::case::CaseIndex.key(id)
    };
    e.meta
        .put(
            REPO,
            &[
                (MetaKey::Changeset(id), Bytes::from(parent.encode())),
                (
                    MetaKey::Derived(old_key),
                    Bytes::copy_from_slice(Tree::new(vec![]).unwrap().id().as_bytes()),
                ),
                (
                    MetaKey::Derived(RestrictedRoots::new(Arc::new(AclConfig::initial())).key(id)),
                    Bytes::from_static(&[0]),
                ),
            ],
        )
        .await
        .unwrap();
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
    let checked = ops(&meta, &blobs);
    let child = changeset(&[id], "child", vec![]);
    let err = checked
        .create_changeset(REPO, child.clone(), &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::DataLoss);
    assert!(
        !err.code.retryable(),
        "missing index cannot repair itself on retry"
    );
    assert_eq!(faults.counts().meta_get_keys, 4);
    assert!(faults.writes().is_empty());
    checked
        .deriver()
        .get(REPO, &ops::case::CaseIndex, id)
        .await
        .unwrap();
    checked
        .create_changeset(REPO, child, &CreateOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn create_waits_for_acl_backfill_without_walking_history() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let mut parent = None;
    let mut tips = Vec::new();
    for i in 0..64 {
        let content = Bytes::from(format!("{i}\n"));
        let id = commit(
            &plain,
            &parent.into_iter().collect::<Vec<_>>(),
            &format!("c{i}"),
            vec![(path("private/file"), write(&content))],
            &[content],
        )
        .await
        .unwrap();
        parent = Some(id);
        if i == 0 || i == 63 {
            tips.push(id);
        }
    }
    let config = Arc::new(AclConfig {
        version: 1,
        default_domain: "default".into(),
        domains: [(path("private"), "secret".into())].into(),
    });
    let opts = CreateOptions {
        acl: config.clone(),
        ..CreateOptions::default()
    };
    let restricted = RestrictedRoots::new(config);
    let mut costs = Vec::new();
    for tip in tips {
        let faults = Faults::new();
        let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let checked = ops(&meta, &blobs);
        let cs = changeset(&[tip], "new ACL", vec![(path("public"), write(b"0\n"))]);
        let err = checked
            .create_changeset(REPO, cs.clone(), &opts)
            .await
            .unwrap_err();
        assert_eq!(err.code, Code::AclPending);
        assert!(err.code.retryable());
        let counts = faults.counts();
        assert_eq!(
            (counts.meta_puts, counts.blob_puts, counts.blob_reads),
            (0, 0, 0)
        );
        assert_eq!(counts.meta_get_keys, 3, "create read ancestors: {counts:?}");
        costs.push(counts.meta_get_keys);
        assert!(
            e.meta
                .get(REPO, &[MetaKey::Changeset(cs.id())])
                .await
                .unwrap()[0]
                .is_none()
        );
        // Backfill explicitly outside create, then retry the identical request.
        plain.deriver().get(REPO, &restricted, tip).await.unwrap();
        let id = checked.create_changeset(REPO, cs, &opts).await.unwrap();
        let roots =
            RestrictedRoots::decode(&checked.deriver().get(REPO, &restricted, id).await.unwrap())
                .unwrap();
        let root = root_of(&checked, id).await;
        let mut cx = checked.deriver().cx(REPO);
        assert_eq!(
            cx.trees.lookup_path(root, &path("private")).await.unwrap(),
            Some(EntryTarget::Tree(roots[&path("private")]))
        );
    }
    assert_eq!(costs[0], costs[1]);
}

#[tokio::test]
async fn concurrent_creates_cannot_bypass_another_acl_versions_gate() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = plain
        .create_changeset(
            REPO,
            changeset(&[], "base", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    let current = CreateOptions::default();
    let pending = CreateOptions {
        acl: Arc::new(AclConfig {
            version: 1,
            ..AclConfig::initial()
        }),
        ..CreateOptions::default()
    };
    let cs = changeset(&[base], "same id", vec![]);
    let (a, b) = tokio::join!(
        plain.create_changeset(REPO, cs.clone(), &current),
        plain.create_changeset(REPO, cs.clone(), &pending)
    );
    a.unwrap();
    assert_eq!(b.unwrap_err().code, Code::AclPending);
    // Retrying the already stored changeset must also check the new version.
    assert_eq!(
        plain
            .create_changeset(REPO, cs, &pending)
            .await
            .unwrap_err()
            .code,
        Code::AclPending
    );
}

#[tokio::test]
async fn case_nodes_are_private_and_have_no_object_frames() {
    let e = env().await;
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
    ops::blob::upload_blobs(&*e.meta, &e.blobs, REPO, &[Bytes::from_static(b"x")])
        .await
        .unwrap();
    let checked = ops(&meta, &blobs);
    let id = checked
        .create_changeset(
            REPO,
            changeset(
                &[],
                "private",
                vec![(path("restricted/Secret"), write(b"x"))],
            ),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    let mut tree_count = 0;
    let mut index_count = 0;
    for write in faults.writes() {
        if let Write::Meta(keys) = write {
            for key in keys {
                match key {
                    MetaKey::Tree(_) => tree_count += 1,
                    MetaKey::CaseNode(node) => {
                        index_count += 1;
                        let got = e.meta.get(REPO, &[key, MetaKey::Tree(node)]).await.unwrap();
                        assert_eq!(Tree::decode(got[0].as_ref().unwrap()).unwrap().id(), node);
                        assert!(got[1].is_none(), "private index escaped into trees");
                        assert!(!e.blobs.contains(REPO, ObjectKey::tree(node)).await.unwrap());
                    }
                    _ => {}
                }
            }
        }
    }
    assert!(index_count >= 2);
    assert_eq!(faults.counts().blob_puts, tree_count);
    // A new server must read the private namespace, and still reject conflicts.
    let cold = ops(&e.meta, &e.blobs);
    let err = cold
        .create_changeset(
            REPO,
            changeset(
                &[id],
                "conflict",
                vec![(path("restricted/secret"), write(b"x"))],
            ),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::CaseConflict);
    // Warming the private cache cannot make an index readable as a public tree.
    let value = cold
        .deriver()
        .get(REPO, &ops::case::CaseIndex, id)
        .await
        .unwrap();
    let index = scion_core::TreeId::from_bytes(value.as_ref().try_into().unwrap());
    let mut cx = cold.deriver().cx(REPO);
    cx.case_nodes.get(index).await.unwrap();
    assert!(cx.trees.get(index).await.is_err());
}

#[tokio::test]
async fn diff_skips_equal_shards_even_when_the_split_prefix_changes() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "wide",
        (0..50_000)
            .map(|i| (path(&format!("f{i:06}")), write(b"x")))
            .collect(),
        &[Bytes::from_static(b"x")],
    )
    .await
    .unwrap();
    for (name, content) in [("f012345", &b"y"[..]), ("a", &b"x"[..])] {
        let next = commit(
            &plain,
            &[base],
            name,
            vec![(path(name), write(content))],
            &[Bytes::copy_from_slice(content)],
        )
        .await
        .unwrap();
        for (a, b) in [(base, next), (next, base)] {
            let faults = Faults::new();
            let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
            let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
            let cold = ops(&meta, &blobs);
            let diff = diff_all(&cold, a, b, DiffMode::Names, &[]).await.unwrap();
            assert_eq!(diff.len(), 1);
            assert_eq!(diff[0].path, name.as_bytes());
            assert!(
                faults.counts().meta_get_keys <= 10,
                "read unchanged shards: {:?}",
                faults.counts()
            );
            assert_eq!(faults.counts().blob_reads, 0);
        }
    }
}

#[tokio::test]
async fn binary_diff_uses_metadata_without_reading_content() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "text",
        vec![(path("f"), write(b"text\n"))],
        &[Bytes::from_static(b"text\n")],
    )
    .await
    .unwrap();
    for bytes in [vec![0; 32], vec![0; 9 << 20], vec![b'x'; (16 << 20) + 1]] {
        let next = commit(
            &plain,
            &[base],
            "binary",
            vec![(path("f"), write(&bytes))],
            &[Bytes::from(bytes)],
        )
        .await
        .unwrap();
        for mode in [DiffMode::Stat, DiffMode::Patch] {
            for (a, b) in [(base, next), (next, base)] {
                let faults = Faults::new();
                let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
                let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
                let cold = ops(&meta, &blobs);
                let out = diff_all(&cold, a, b, mode, &[]).await.unwrap();
                assert_eq!(out.len(), 1);
                assert!(out[0].binary);
                assert_eq!((out[0].added_lines, out[0].removed_lines), (0, 0));
                assert!(out[0].patch.is_empty());
                assert_eq!(faults.counts().blob_reads, 0);
            }
        }
    }
}

#[tokio::test]
async fn diff_classification_batches_metadata_reads() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    // Distinct blobs on both sides rule out caching or deduplication alone
    // satisfying the round-trip bound. More files than one batch exercises
    // the boundary and the partial final batch.
    let n = 1025;
    let old: Vec<_> = (0..n).map(|i| Bytes::from(format!("old\0{i}"))).collect();
    let new: Vec<_> = (0..n).map(|i| Bytes::from(format!("new\0{i}"))).collect();
    let changes = |contents: &[Bytes]| {
        contents
            .iter()
            .enumerate()
            .map(|(i, b)| (path(&format!("f{i:04}")), write(b)))
            .collect()
    };
    let base = commit(&plain, &[], "old", changes(&old), &old)
        .await
        .unwrap();
    let next = commit(&plain, &[base], "new", changes(&new), &new)
        .await
        .unwrap();
    let mut names_counts = None;
    for mode in [DiffMode::Names, DiffMode::Stat, DiffMode::Patch] {
        let faults = Faults::new();
        let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let cold = ops(&meta, &blobs);
        let out = diff_paged(&cold, base, next, mode, &[], ops::MAX_DIFF_PAGE)
            .await
            .unwrap();
        assert_eq!(out.len(), n);
        assert_eq!(faults.counts().blob_reads, 0);
        if mode == DiffMode::Names {
            names_counts = Some(faults.counts());
        } else {
            let counts = faults.counts();
            let baseline = names_counts.unwrap();
            let reads = counts.meta_gets - baseline.meta_gets;
            assert!(
                (2..=10).contains(&reads),
                "2050 records must use bounded batches, not {reads} calls"
            );
            assert_eq!(counts.meta_get_keys - baseline.meta_get_keys, 2 * n as u64);
            for (i, diff) in out.iter().enumerate() {
                assert!(diff.binary);
                assert_eq!(diff.path, format!("f{i:04}").as_bytes());
                assert_eq!(diff.old.unwrap().blob, scion_core::BlobId::hash(&old[i]));
                assert_eq!(diff.new.unwrap().blob, scion_core::BlobId::hash(&new[i]));
                assert!(diff.patch.is_empty());
                assert_eq!((diff.added_lines, diff.removed_lines), (0, 0));
            }
        }
    }
}

#[tokio::test]
async fn diff_classification_deduplicates_blobs_within_each_batch() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(&plain, &[], "empty", vec![], &[]).await.unwrap();
    let next = commit(
        &plain,
        &[base],
        "copies",
        (0..100)
            .map(|i| (path(&format!("f{i:03}")), write(b"\0binary")))
            .collect(),
        &[Bytes::from_static(b"\0binary")],
    )
    .await
    .unwrap();
    let mut baseline = None;
    for mode in [DiffMode::Names, DiffMode::Stat] {
        let faults = Faults::new();
        let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let out = diff_paged(&ops(&meta, &blobs), base, next, mode, &[], 100)
            .await
            .unwrap();
        assert_eq!(out.len(), 100);
        if mode == DiffMode::Names {
            baseline = Some(faults.counts());
        } else {
            assert!(out.iter().all(|f| f.binary));
            assert_eq!(
                faults.counts().meta_get_keys - baseline.unwrap().meta_get_keys,
                1
            );
            assert_eq!(faults.counts().blob_reads, 0);
        }
    }
}

#[tokio::test]
async fn diff_handles_shard_collapse_and_directory_file_replacement() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "split",
        (0..4097)
            .map(|i| (path(&format!("d/f{i:04}")), write(b"x")))
            .collect(),
        &[Bytes::from_static(b"x")],
    )
    .await
    .unwrap();
    let collapsed = commit(
        &plain,
        &[base],
        "collapse",
        vec![(path("d/f4096"), scion_core::FileChange::Deleted)],
        &[],
    )
    .await
    .unwrap();
    let replaced = commit(&plain, &[], "file", vec![(path("d"), write(b"x"))], &[])
        .await
        .unwrap();
    for (a, b) in [(base, collapsed), (collapsed, base)] {
        let out = diff_all(&ops(&e.meta, &e.blobs), a, b, DiffMode::Names, &[])
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, b"d/f4096");
    }
    for (a, b) in [(collapsed, replaced), (replaced, collapsed)] {
        let out = diff_all(&ops(&e.meta, &e.blobs), a, b, DiffMode::Names, &[])
            .await
            .unwrap();
        assert_eq!(out.len(), 4097);
        assert_eq!(out[0].path, b"d");
        for (i, f) in out[1..].iter().enumerate() {
            assert_eq!(f.path, format!("d/f{i:04}").as_bytes());
        }
    }
}
