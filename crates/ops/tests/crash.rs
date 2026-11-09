//! A crash at every write step of uploading blobs and creating a changeset
//! leaves no broken reference (INV-22), on Postgres and the object store:
//! after the crash every stored record's references are stored, and rerunning
//! the same work completes it.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use common::*;
use derived::{AclConfig, CommitGraph, DerivedType, GraphNode, RestrictedRoots, RootTree};
use ops::CreateOptions;
use ops::blob::upload_blobs;
use ops::case::CaseIndex;
use scion_core::{
    CHUNK_BYTES, ChangesetId, ChunkList, EntryTarget, FileChange, FileKind, Frame, FrameKind, Tree,
};
use store::fault::{CrashPoint, FaultBlobStore, FaultMetaStore, Faults, Write};
use store::{BlobStore, MetaKey, MetaStore, ObjectBlobStore, ObjectKey, PgMetaStore};

/// Uploads three blobs (one chunked) and creates a changeset on `base`.
async fn flow<M: MetaStore, B: BlobStore>(
    meta: &Arc<M>,
    blobs: &Arc<B>,
    base: ChangesetId,
    variant: u32,
) -> Result<ChangesetId, ops::OpError> {
    let small = format!("small {variant}\n").into_bytes();
    let mut big = vec![b'z'; CHUNK_BYTES + 10];
    big[..4].copy_from_slice(&variant.to_le_bytes());
    let link = format!("target-{variant}").into_bytes();
    let contents = vec![
        Bytes::from(small.clone()),
        Bytes::from(big.clone()),
        Bytes::from(link.clone()),
    ];
    upload_blobs(&**meta, blobs, REPO, &contents).await?;
    let changes = vec![
        (path("dir/a"), write(&small)),
        (path("new/deep/big"), write(&big)),
        (
            path(&format!("new/link{variant}")),
            FileChange::File {
                kind: FileKind::Symlink,
                blob: scion_core::BlobId::hash(&link),
                size: link.len() as u64,
            },
        ),
        (path("dir/old"), FileChange::Deleted),
    ];
    ops(meta, blobs)
        .create_changeset(
            REPO,
            changeset(&[base], &format!("v{variant}"), changes),
            &CreateOptions::default(),
        )
        .await
}

async fn meta_has(meta: &PgMetaStore, key: MetaKey) -> bool {
    meta.get(REPO, &[key]).await.unwrap()[0].is_some()
}

/// Checks that every stored record among `keys` has its references stored.
async fn check_references(meta: &PgMetaStore, blobs: &ObjectBlobStore, keys: &BTreeSet<Key>) {
    let restricted = RestrictedRoots::new(Arc::new(AclConfig::initial()));
    for key in keys {
        match key {
            Key::Object(k) => {
                let Some(bytes) = blobs.get(REPO, *k).await.unwrap() else {
                    continue;
                };
                let f = Frame::decode(&bytes).unwrap();
                if f.header().kind() == FrameKind::ChunkList {
                    for c in ChunkList::decode(f.payload()).unwrap().chunks {
                        assert!(
                            blobs.contains(REPO, ObjectKey::chunk(c)).await.unwrap(),
                            "chunk list without its chunk"
                        );
                    }
                }
            }
            Key::Meta(k) => {
                let Some(bytes) = meta.get(REPO, &[*k]).await.unwrap().pop().flatten() else {
                    continue;
                };
                match k {
                    MetaKey::Blob(id) => {
                        assert!(
                            blobs.contains(REPO, ObjectKey::blob(*id)).await.unwrap(),
                            "blob record without its frame"
                        );
                    }
                    MetaKey::Tree(id) => {
                        assert!(
                            blobs.contains(REPO, ObjectKey::tree(*id)).await.unwrap(),
                            "tree row without its frame"
                        );
                        for e in Tree::decode(&bytes).unwrap().entries() {
                            let need = match e.target {
                                EntryTarget::Tree(t) | EntryTarget::Shard(t) => MetaKey::Tree(t),
                                EntryTarget::File { blob, .. } => MetaKey::Blob(blob),
                            };
                            assert!(
                                meta_has(meta, need).await,
                                "tree refers to missing {need:?}"
                            );
                        }
                    }
                    MetaKey::CaseNode(_) => {
                        for e in Tree::decode(&bytes).unwrap().entries() {
                            let need = match e.target {
                                EntryTarget::Tree(t) | EntryTarget::Shard(t) => {
                                    MetaKey::CaseNode(t)
                                }
                                EntryTarget::File { blob, .. } => MetaKey::Blob(blob),
                            };
                            assert!(
                                meta_has(meta, need).await,
                                "index refers to missing {need:?}"
                            );
                        }
                    }
                    MetaKey::Changeset(id) => {
                        let cs = scion_core::Changeset::decode(&bytes).unwrap();
                        for p in &cs.parts().parents {
                            assert!(
                                meta_has(meta, MetaKey::Changeset(*p)).await,
                                "missing parent"
                            );
                        }
                        for c in cs.parts().file_changes.values() {
                            if let FileChange::File { blob, .. } = c {
                                assert!(
                                    meta_has(meta, MetaKey::Blob(*blob)).await,
                                    "changeset refers to a missing blob"
                                );
                            }
                        }
                        for d in [
                            RootTree.key(*id),
                            CommitGraph.key(*id),
                            restricted.key(*id),
                            CaseIndex::default().key(*id),
                        ] {
                            assert!(
                                meta_has(meta, MetaKey::Derived(d)).await,
                                "changeset without eager value {d:?}"
                            );
                        }
                    }
                    MetaKey::Derived(d) => {
                        assert!(
                            meta_has(meta, MetaKey::Changeset(d.changeset)).await,
                            "value without its changeset"
                        );
                        if d.kind == CaseIndex::default().key(d.changeset).kind {
                            let id: [u8; 32] = bytes.as_ref().try_into().unwrap();
                            assert!(
                                meta_has(
                                    meta,
                                    MetaKey::CaseNode(scion_core::TreeId::from_bytes(id))
                                )
                                .await,
                                "CaseIndex without its tree"
                            );
                        }
                        if d.kind == RootTree.key(d.changeset).kind {
                            let root = RootTree::decode(&bytes).unwrap();
                            assert!(
                                meta_has(meta, MetaKey::Tree(root)).await,
                                "RootTree without its tree"
                            );
                        }
                        if d.kind == CommitGraph.key(d.changeset).kind {
                            for p in GraphNode::decode(&bytes).unwrap().parents {
                                assert!(meta_has(meta, MetaKey::Derived(CommitGraph.key(p))).await);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Key {
    Meta(MetaKey),
    Object(ObjectKey),
}

fn written(faults: &Faults) -> BTreeSet<Key> {
    let mut out = BTreeSet::new();
    for w in faults.writes() {
        match w {
            Write::Meta(keys) => out.extend(keys.into_iter().map(Key::Meta)),
            Write::Blob(k) => {
                out.insert(Key::Object(k));
            }
            Write::Ref(name) => panic!("creating a changeset wrote the ref {name} (INV-2)"),
        }
    }
    out
}

#[tokio::test]
async fn a_crash_at_every_write_step_leaves_no_broken_reference() {
    let env = env().await;
    let plain = ops(&env.meta, &env.blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        ("dir/a", Some((FileKind::Regular, b"a"))),
        ("dir/old", Some((FileKind::Regular, b"o"))),
    ]);
    let base = commit(&plain, &[], "base", c, &b).await.unwrap();

    // Count the write steps of one complete run.
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(env.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(env.blobs.clone(), faults.clone()));
    flow(&meta, &blobs, base, 0).await.unwrap();
    assert!(
        !faults.crashed(),
        "a run with no crash point reports a crash"
    );
    let steps = faults.writes().len() as u64;
    assert!(steps >= 10, "{steps} write steps");

    let mut variant = 1;
    for step in 0..steps {
        for point in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            variant += 1;
            let faults = Faults::new();
            faults.crash_at(step, point);
            let meta = Arc::new(FaultMetaStore::new(env.meta.clone(), faults.clone()));
            let blobs = Arc::new(FaultBlobStore::new(env.blobs.clone(), faults.clone()));
            let crashed = flow(&meta, &blobs, base, variant).await;
            assert!(
                crashed.is_err() && faults.crashed(),
                "step {step} {point:?} did not crash"
            );
            // Writes run concurrently, so others may start alongside, but the
            // crash comes no earlier than the chosen write.
            assert!(
                faults.writes().len() as u64 > step,
                "step {step} {point:?} crashed at an earlier write"
            );
            let mut keys = written(&faults);
            check_references(&env.meta, &env.blobs, &keys).await;
            // The process restarts and the client retries.
            faults.restart();
            let id = flow(&meta, &blobs, base, variant).await.unwrap();
            keys.extend(written(&faults));
            keys.insert(Key::Meta(MetaKey::Changeset(id)));
            check_references(&env.meta, &env.blobs, &keys).await;
            assert!(meta_has(&env.meta, MetaKey::Changeset(id)).await);
        }
    }
}
