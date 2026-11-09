//! Creating changesets reads and writes no refs and holds no locks (INV-2),
//! on Postgres and the object store: creation succeeds through a store
//! wrapper that fails every ref access, and a create stalled in the middle of
//! its writes does not hold up other creates on the same parent.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use ops::CreateOptions;
use ops::blob::upload_blobs;
use scion_core::{ChangesetId, FileKind};
use store::fault::{FaultBlobStore, FaultMetaStore, Faults};
use store::{
    BlobStore, CasOutcome, MetaKey, MetaStore, ObjectKey, PutOutcome, RefLogEntry, RefState,
    RefUpdate, RepoId, StoreError,
};
use tokio::sync::{Notify, Semaphore};

#[tokio::test]
async fn creation_never_touches_refs() {
    let env = env().await;
    let faults = Faults::new();
    faults.deny_refs();
    let meta = Arc::new(FaultMetaStore::new(env.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(env.blobs.clone(), faults.clone()));
    let ops = ops(&meta, &blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        ("a/b", Some((FileKind::Regular, b"1"))),
        ("c", Some((FileKind::Regular, b"2"))),
    ]);
    let root = commit(&ops, &[], "root", c, &b).await.unwrap();
    let (c, b) = m.clone().apply(&[("a/d", Some((FileKind::Regular, b"3")))]);
    let left = commit(&ops, &[root], "left", c, &b).await.unwrap();
    let (c, b) = m.apply(&[("e", Some((FileKind::Symlink, b"a/b")))]);
    let right = commit(&ops, &[root], "right", c, &b).await.unwrap();
    let (c, b) = Model::default().apply(&[("e", Some((FileKind::Symlink, b"a/b")))]);
    commit(&ops, &[left, right], "merge", c, &b).await.unwrap();
    // Diff reads no refs either.
    diff_all(&ops, root, right, ops::DiffMode::Patch, &[])
        .await
        .unwrap();
    assert_eq!(faults.ref_violations(), Vec::<String>::new());
    assert_eq!(faults.counts().ref_calls, 0);
    // The wrapper does fail ref access, so the check above can fail.
    assert!(meta.get_ref(REPO, "main").await.is_err());
    assert_eq!(faults.ref_violations().len(), 1);
}

/// Where a [`Gated`] store stops the next matching call.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stop {
    MetaPut,
    TreeFrame,
}

/// Wraps both stores; once armed, the first matching call waits until it is
/// released, holding whatever its caller holds.
struct Gated<S> {
    inner: Arc<S>,
    stop: Stop,
    armed: Arc<Semaphore>,
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl<S> Gated<S> {
    async fn maybe_stop(&self, here: Stop) {
        if here == self.stop
            && let Ok(p) = self.armed.try_acquire()
        {
            p.forget();
            self.reached.notify_one();
            self.release.notified().await;
        }
    }
}

impl<S: MetaStore> MetaStore for Gated<S> {
    async fn put(
        &self,
        repo: RepoId,
        entries: &[(MetaKey, Bytes)],
    ) -> Result<Vec<PutOutcome>, StoreError> {
        if entries
            .iter()
            .any(|(k, _)| matches!(k, MetaKey::Changeset(_)))
        {
            self.maybe_stop(Stop::MetaPut).await;
        }
        self.inner.put(repo, entries).await
    }
    async fn get(&self, repo: RepoId, keys: &[MetaKey]) -> Result<Vec<Option<Bytes>>, StoreError> {
        self.inner.get(repo, keys).await
    }
    async fn get_ref(&self, repo: RepoId, name: &str) -> Result<Option<RefState>, StoreError> {
        self.inner.get_ref(repo, name).await
    }
    async fn list_refs(
        &self,
        repo: RepoId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RefState>, StoreError> {
        self.inner.list_refs(repo, prefix, after, limit).await
    }
    async fn update_ref(
        &self,
        repo: RepoId,
        update: &RefUpdate<'_>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.update_ref(repo, update).await
    }
    async fn ref_log(
        &self,
        repo: RepoId,
        name: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>, StoreError> {
        self.inner.ref_log(repo, name, after_seq, limit).await
    }
}

impl<S: BlobStore> BlobStore for Gated<S> {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        if key.kind() == store::ObjectKind::Tree {
            self.maybe_stop(Stop::TreeFrame).await;
        }
        self.inner.put_if_absent(repo, key, bytes).await
    }
    async fn get(&self, repo: RepoId, key: ObjectKey) -> Result<Option<Bytes>, StoreError> {
        self.inner.get(repo, key).await
    }
    async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, StoreError> {
        self.inner.contains(repo, key).await
    }
}

#[tokio::test]
async fn a_stalled_create_does_not_block_other_creates() {
    for stop in [Stop::MetaPut, Stop::TreeFrame] {
        let env = env().await;
        let armed = Arc::new(Semaphore::new(0));
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let gate = |s| (armed.clone(), reached.clone(), release.clone(), s);
        let (a, r, l, s) = gate(stop);
        let meta = Arc::new(Gated {
            inner: env.meta.clone(),
            stop: s,
            armed: a,
            reached: r,
            release: l,
        });
        let (a, r, l, s) = gate(stop);
        let blobs = Arc::new(Gated {
            inner: env.blobs.clone(),
            stop: s,
            armed: a,
            reached: r,
            release: l,
        });
        // One server: anything create shares between calls is shared here.
        let server = Arc::new(ops(&meta, &blobs));
        let mut m = Model::default();
        let (c, b) = m.apply(&[("dir/a", Some((FileKind::Regular, b"a")))]);
        let base = commit(&server, &[], "base", c, &b).await.unwrap();
        let contents: Vec<Bytes> = ["x", "y"]
            .iter()
            .map(|s| Bytes::from_static(s.as_bytes()))
            .collect();
        upload_blobs(&*env.meta, &env.blobs, REPO, &contents)
            .await
            .unwrap();
        let make = |name: &str, content: &[u8]| {
            changeset(
                &[base],
                name,
                vec![(path(&format!("dir/{name}")), write(content))],
            )
        };

        armed.add_permits(1);
        let stalled = {
            let server = server.clone();
            let cs = make("x", b"x");
            tokio::spawn(async move {
                server
                    .create_changeset(REPO, cs, &CreateOptions::default())
                    .await
            })
        };
        reached.notified().await;
        let other: ChangesetId = tokio::time::timeout(
            Duration::from_secs(10),
            server.create_changeset(REPO, make("y", b"y"), &CreateOptions::default()),
        )
        .await
        .expect("a stalled create blocked another create")
        .unwrap();
        assert!(!stalled.is_finished());
        release.notify_one();
        let first = stalled.await.unwrap().unwrap();
        assert_ne!(first, other);
        for id in [first, other] {
            assert!(env.meta.get(REPO, &[MetaKey::Changeset(id)]).await.unwrap()[0].is_some());
        }
    }
}
