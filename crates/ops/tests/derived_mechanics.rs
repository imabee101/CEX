//! The derived-data mechanism's bounds and caches, on Postgres and the
//! object store: value keys keep their stored layout, a warm server answers
//! repeated reads from RAM, tree frames are written with bounded parallelism,
//! frames are compressed when that shrinks them, and name lookups are
//! batched one round trip per directory level.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::*;
use derived::{
    AclConfig, CacheSizes, CommitGraph, DerivedType, Deriver, FRAME_WRITES, RestrictedRoots,
    RootTree,
};
use ops::Ops;
use ops::case::CaseIndex;
use scion_core::{ChangesetId, Codec, EntryTarget, FileKind, Frame};
use store::fault::{FaultBlobStore, FaultMetaStore, Faults};
use store::{BlobStore, DerivedKey, ObjectKey, PutOutcome, RepoId, StoreError};

/// Stored value keys are part of the metadata format: type codes, versions,
/// and the param are fixed.
#[test]
fn value_keys_keep_their_layout() {
    let cs = ChangesetId::from_bytes([5; 32]);
    let key = |kind, param| DerivedKey {
        kind,
        version: 1,
        param,
        changeset: cs,
    };
    assert_eq!(RootTree.key(cs), key(1, 0));
    assert_eq!(CommitGraph.key(cs), key(2, 0));
    let mut acl = AclConfig::initial();
    acl.version = 9;
    assert_eq!(RestrictedRoots::new(Arc::new(acl)).key(cs), key(3, 9));
    assert_eq!(
        CaseIndex::default().key(cs),
        DerivedKey {
            version: 2,
            ..key(4, 0)
        }
    );
}

#[tokio::test]
async fn a_warm_server_reads_values_and_trees_from_ram() {
    let e = env().await;
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
    let ops = ops(&meta, &blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[("d/e/f", Some((FileKind::Regular, b"1")))]);
    let id = commit(&ops, &[], "one", c, &b).await.unwrap();
    faults.reset_counts();
    let d = ops.deriver();
    let root = RootTree::decode(&d.get(REPO, &RootTree, id).await.unwrap()).unwrap();
    d.get(REPO, &CommitGraph, id).await.unwrap();
    let mut cx = d.cx(REPO);
    let found = cx.trees.lookup_path(root, &path("d/e/f")).await.unwrap();
    assert!(matches!(found, Some(EntryTarget::File { .. })));
    assert_eq!(faults.counts().meta_gets, 0, "{:?}", faults.counts());
}

/// A blob store whose puts wait, without timers, until `FRAME_WRITES` puts
/// are in flight at once (or every expected put has started), and which
/// records the most in flight. A writer with a lower bound never fills the
/// gate and hangs; one with no bound starts more than `FRAME_WRITES`.
struct Gate<B> {
    inner: Arc<B>,
    total: usize,
    state: std::sync::Mutex<GateState>,
    opened: tokio::sync::Notify,
}

#[derive(Default)]
struct GateState {
    now: usize,
    max: usize,
    started: usize,
    /// Bumped each time the gate opens, releasing every put waiting on it.
    generation: u64,
}

impl<B: BlobStore> BlobStore for Gate<B> {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        let waiting = {
            let mut s = self.state.lock().unwrap();
            s.now += 1;
            s.started += 1;
            s.max = s.max.max(s.now);
            if s.now == FRAME_WRITES || s.started == self.total {
                s.generation += 1;
                self.opened.notify_waiters();
                None
            } else {
                Some(s.generation)
            }
        };
        if let Some(generation) = waiting {
            loop {
                let opened = self.opened.notified();
                tokio::pin!(opened);
                opened.as_mut().enable();
                if self.state.lock().unwrap().generation != generation {
                    break;
                }
                opened.await;
            }
        }
        let r = self.inner.put_if_absent(repo, key, bytes).await;
        self.state.lock().unwrap().now -= 1;
        r
    }

    async fn get(&self, repo: RepoId, key: ObjectKey) -> Result<Option<Bytes>, StoreError> {
        self.inner.get(repo, key).await
    }

    async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, StoreError> {
        self.inner.contains(repo, key).await
    }
}

/// On a single-threaded runtime, a writer spawns every frame write it may
/// before any is polled, so the most in flight is exactly its bound.
#[tokio::test]
async fn tree_frames_are_written_in_parallel_up_to_exactly_the_bound() {
    let e = env().await;
    // 300 directories with different contents: 300 new nodes plus the
    // root, each with a frame. Private index nodes have none.
    let files: Vec<(Vec<u8>, FileKind, Vec<u8>)> = (0..300)
        .map(|i| {
            (
                format!("d{i:03}/f").into_bytes(),
                FileKind::Regular,
                format!("{i}\n").into_bytes(),
            )
        })
        .collect();
    // Upload first, so every counted put is a tree frame.
    let contents: Vec<Bytes> = files.iter().map(|f| Bytes::from(f.2.clone())).collect();
    ops::blob::upload_blobs(&*e.meta, &e.blobs, REPO, &contents)
        .await
        .unwrap();
    let total = reference_nodes(files.clone())
        .1
        .into_values()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    assert_eq!(total, 301);
    let blobs = Arc::new(Gate {
        inner: e.blobs.clone(),
        total,
        state: std::sync::Mutex::default(),
        opened: tokio::sync::Notify::new(),
    });
    let ops = Ops::new(Arc::new(Deriver::new(
        e.meta.clone(),
        blobs.clone(),
        CacheSizes::default(),
    )));
    let changes = files
        .iter()
        .map(|(p, _, c)| (path(std::str::from_utf8(p).unwrap()), write(c)))
        .collect();
    ops.create_changeset(
        REPO,
        changeset(&[], "many", changes),
        &ops::CreateOptions::default(),
    )
    .await
    .unwrap();
    let s = blobs.state.lock().unwrap();
    assert_eq!((s.started, s.max), (total, FRAME_WRITES));
}

#[tokio::test]
async fn frames_are_compressed_when_that_shrinks_them() {
    let e = env().await;
    let ops = ops(&e.meta, &e.blobs);
    let text = Bytes::from("line\n".repeat(2000));
    let random: Vec<u8> = {
        let mut r = Rng(7);
        (0..64).map(|_| r.next_u64() as u8).collect()
    };
    let id = commit(
        &ops,
        &[],
        "c",
        vec![
            (path("text"), write(&text)),
            (path("random"), write(&random)),
        ],
        &[text.clone(), Bytes::from(random.clone())],
    )
    .await
    .unwrap();
    let blobs = e.blobs.clone();
    let frame = |k| {
        let blobs = blobs.clone();
        async move { blobs.get(REPO, k).await.unwrap().unwrap() }
    };
    let f = frame(ObjectKey::blob(scion_core::BlobId::hash(&text))).await;
    assert_eq!(Frame::decode(&f).unwrap().header().codec(), Codec::Zstd);
    let f = frame(ObjectKey::blob(scion_core::BlobId::hash(&random))).await;
    assert_eq!(Frame::decode(&f).unwrap().header().codec(), Codec::Raw);
    let root = root_of(&ops, id).await;
    let f = frame(ObjectKey::tree(root)).await;
    Frame::decode(&f).unwrap();
}

/// Metadata reads a cold server makes to change `a` and `b` on `parent`.
async fn reads_for(e: &Env, parent: ChangesetId, a: &str, b: &str) -> u64 {
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
    let ops = ops(&meta, &blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        (a, Some((FileKind::Regular, b"2"))),
        (b, Some((FileKind::Regular, b"2"))),
    ]);
    commit(&ops, &[parent], "two", c, &b).await.unwrap();
    faults.counts().meta_gets
}

#[tokio::test]
async fn lookups_are_batched_per_directory_level() {
    let e = env().await;
    let ops = ops(&e.meta, &e.blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        ("a/x/1", Some((FileKind::Regular, b"1"))),
        ("b/y/1", Some((FileKind::Regular, b"1"))),
        ("a/xxxxxxxx/1", Some((FileKind::Regular, b"1"))),
        ("bbbbbbbbb/y/1", Some((FileKind::Regular, b"1"))),
    ]);
    let base = commit(&ops, &[], "base", c, &b).await.unwrap();
    // Paths of equal depth whose directory names differ in length take as
    // many round trips as paths whose names are equally long.
    let same = reads_for(&e, base, "a/x/1", "b/y/1").await;
    let mixed = reads_for(&e, base, "a/xxxxxxxx/1", "bbbbbbbbb/y/1").await;
    assert_eq!(same, mixed);
}

#[tokio::test]
async fn prefix_search_reads_only_what_it_returns() {
    let e = env().await;
    let ops = ops(&e.meta, &e.blobs);
    let changes = (0..5000)
        .map(|i| (path(&format!("d/f{i:05}")), write(b"s")))
        .collect();
    let id = commit(&ops, &[], "big", changes, &[Bytes::from_static(b"s")])
        .await
        .unwrap();
    let root = root_of(&ops, id).await;
    let mut cx = ops.deriver().cx(REPO);
    let Some(EntryTarget::Tree(dir)) = cx.trees.lookup_path(root, &path("d")).await.unwrap() else {
        panic!("d is not a directory");
    };
    let names = |v: Vec<Vec<u8>>| -> Vec<String> {
        v.into_iter()
            .map(|n| String::from_utf8(n).unwrap())
            .collect()
    };
    let t = &mut cx.trees;
    // A prefix shorter than the node's own: every name matches.
    assert_eq!(
        names(t.find_prefixed(dir, b"f", 2).await.unwrap()),
        ["f00000", "f00001"]
    );
    // A prefix inside one shard, across a shard boundary, and none.
    assert_eq!(
        names(t.find_prefixed(dir, b"f0299", 3).await.unwrap()),
        ["f02990", "f02991", "f02992"]
    );
    assert_eq!(
        names(t.find_prefixed(dir, b"f0099", 20).await.unwrap()).len(),
        10
    );
    assert!(t.find_prefixed(dir, b"g", 2).await.unwrap().is_empty());
    assert!(t.find_prefixed(dir, b"f1", 2).await.unwrap().is_empty());
}

/// A stored frame whose header names another object is data loss, even when
/// its payload hashes to the id it is stored under.
#[tokio::test]
async fn reads_reject_frames_that_name_another_object() {
    use scion_core::{BlobId, FrameKind};
    let e = env().await;
    let content = b"payload\n";
    let id = BlobId::hash(content);
    let other = BlobId::hash(b"other");
    for (kind, named) in [
        (FrameKind::Blob, other.as_bytes()),
        (FrameKind::Chunk, id.as_bytes()),
    ] {
        let repo = RepoId(REPO.0 + 1 + kind as u64);
        let frame = Frame::encode(
            scion_core::FrameId::new(kind, *named),
            Codec::Raw,
            content.len() as u64,
            content,
        )
        .unwrap();
        e.blobs
            .put_if_absent(repo, ObjectKey::blob(id), Bytes::from(frame))
            .await
            .unwrap();
        let err = ops::blob::read_blob(&*e.blobs, repo, id, content.len() as u64)
            .await
            .expect_err("a frame naming another object was read");
        assert_eq!(err.code, ops::Code::DataLoss, "{kind:?}: {err}");
    }
}

/// Diff reads only the trees that differ and that the path filter can
/// reach: an identical pair and a filtered-out subtree cost no tree reads.
#[tokio::test]
async fn diff_reads_only_differing_reachable_trees() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        ("a/x", Some((FileKind::Regular, b"1"))),
        ("b/c/d/e", Some((FileKind::Regular, b"1"))),
    ]);
    let one = commit(&plain, &[], "one", c, &b).await.unwrap();
    let (c, b) = m.apply(&[
        ("a/x", Some((FileKind::Regular, b"2"))),
        ("b/c/d/e", Some((FileKind::Regular, b"2"))),
    ]);
    let two = commit(&plain, &[one], "two", c, &b).await.unwrap();
    let reads = |a: ChangesetId, b: ChangesetId, paths: Vec<scion_core::RepoPath>| {
        let e = &e;
        async move {
            let faults = Faults::new();
            let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
            let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
            let ops = ops(&meta, &blobs);
            let out = diff_all(&ops, a, b, ops::DiffMode::Names, &paths)
                .await
                .unwrap();
            (out.len(), faults.counts().meta_get_keys)
        }
    };
    // One RootTree value (read once, then cached), and no trees.
    assert_eq!(reads(two, two, vec![]).await, (0, 1));
    let (all, all_keys) = reads(one, two, vec![]).await;
    let (some, some_keys) = reads(one, two, vec![path("a")]).await;
    assert_eq!((all, some), (2, 1));
    // b, b/c, and b/c/d on each side are not read.
    assert_eq!(all_keys - some_keys, 6, "{all_keys} {some_keys}");
}
