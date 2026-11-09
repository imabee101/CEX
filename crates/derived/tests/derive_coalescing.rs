//! Concurrent derivations on one server, on the in-memory store, coalesce
//! per value (INV-20): requests whose underived histories overlap each
//! finish, with every value computed and written once, and a request never
//! waits on a batch that holds none of its keys.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use derived::{CacheSizes, DeriveCx, DeriveError, DerivedType, Deriver, Mode};
use scion_core::{Changeset, ChangesetId, ChangesetParts, Timestamp};
use store::{
    BlobStore, CasOutcome, MemoryBlobStore, MemoryMetaStore, MetaKey, MetaStore, PutOutcome,
    RefLogEntry, RefState, RefUpdate, RepoId, StoreError,
};
use tokio::sync::Notify;

const REPO: RepoId = RepoId(1);

/// Holds the derivation of one changeset until released.
struct Gate {
    at: ChangesetId,
    entered: Notify,
    release: Notify,
}

/// A type whose value is the changeset's depth, 1 plus its deepest parent's
/// value, whose derivation of the gate's changeset waits for the gate.
struct Depth(Arc<Gate>);

impl DerivedType for Depth {
    const KIND: u16 = 0xfff1;
    const VERSION: u16 = 1;
    const MODE: Mode = Mode::Deferred;

    async fn derive<'c, 'd, M: MetaStore, B: BlobStore>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        id: ChangesetId,
        cs: &'c Changeset,
    ) -> Result<Bytes, DeriveError> {
        if id == self.0.at {
            self.0.entered.notify_one();
            self.0.release.notified().await;
        }
        let mut depth = 0u64;
        for p in &cs.parts().parents {
            let v = cx.value(self, *p).await?;
            depth = depth.max(u64::from_le_bytes(v[..].try_into().unwrap()));
        }
        Ok(Bytes::copy_from_slice(&(depth + 1).to_le_bytes()))
    }
}

/// A metadata store that counts the writes of each key.
#[derive(Default)]
struct Counting {
    inner: MemoryMetaStore,
    writes: Mutex<HashMap<MetaKey, usize>>,
}

impl MetaStore for Counting {
    async fn put(
        &self,
        repo: RepoId,
        entries: &[(MetaKey, Bytes)],
    ) -> Result<Vec<PutOutcome>, StoreError> {
        {
            let mut w = self.writes.lock().unwrap();
            for (k, _) in entries {
                *w.entry(*k).or_default() += 1;
            }
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

/// Stores a chain of `n` changesets on `base` (a root when `None`), with no
/// derived values, and returns it oldest first.
async fn chain(
    meta: &Counting,
    base: Option<ChangesetId>,
    n: usize,
    name: &str,
) -> Vec<ChangesetId> {
    let mut ids: Vec<ChangesetId> = Vec::new();
    let mut rows = Vec::new();
    for i in 0..n {
        let mut p = ChangesetParts::default();
        p.parents = ids.last().copied().or(base).into_iter().collect();
        p.author = b"Test <test@example.com>".to_vec();
        p.author_date = Timestamp::new(1_700_000_000, 0).unwrap();
        p.message = format!("{name} {i}").into_bytes();
        let cs = Changeset::new(p).unwrap();
        ids.push(cs.id());
        rows.push((MetaKey::Changeset(cs.id()), Bytes::from(cs.encode())));
    }
    meta.put(REPO, &rows).await.unwrap();
    meta.writes.lock().unwrap().clear();
    ids
}

fn depth(v: &Bytes) -> u64 {
    u64::from_le_bytes(v[..].try_into().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_with_overlapping_histories_each_finish_writing_every_value_once() {
    let meta = Arc::new(Counting::default());
    let shared = chain(&meta, None, 100, "shared").await;
    let a = chain(&meta, Some(shared[99]), 10, "a").await;
    let b = chain(&meta, Some(shared[99]), 10, "b").await;
    let gate = Arc::new(Gate {
        at: shared[50],
        entered: Notify::new(),
        release: Notify::new(),
    });
    let d = Arc::new(Deriver::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
        CacheSizes::default(),
    ));
    let t = Arc::new(Depth(gate.clone()));
    let run = |tip: ChangesetId| {
        let (d, t) = (d.clone(), t.clone());
        tokio::spawn(async move { d.get(REPO, &*t, tip).await })
    };
    // The first request holds the shared history, mid-batch.
    let first = run(a[9]);
    gate.entered.notified().await;
    assert_eq!(d.in_flight(), 110);
    // The second needs the shared history too: it joins those entries and
    // registers only its own branch.
    let second = run(b[9]);
    while d.in_flight() < 120 {
        tokio::task::yield_now().await;
    }
    assert_eq!(d.in_flight(), 120);
    gate.release.notify_one();
    assert_eq!(depth(&first.await.unwrap().unwrap()), 110);
    assert_eq!(depth(&second.await.unwrap().unwrap()), 110);
    let writes = meta.writes.lock().unwrap();
    for id in shared.iter().chain(&a).chain(&b) {
        let key = MetaKey::Derived(t.key(*id));
        assert_eq!(
            writes.get(&key),
            Some(&1),
            "{id} written {:?} times",
            writes.get(&key)
        );
    }
    assert_eq!(d.in_flight(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_never_waits_on_a_batch_without_its_keys() {
    let meta = Arc::new(Counting::default());
    let x = chain(&meta, None, 10, "x").await;
    let y = chain(&meta, None, 10, "y").await;
    let gate = Arc::new(Gate {
        at: x[5],
        entered: Notify::new(),
        release: Notify::new(),
    });
    let d = Arc::new(Deriver::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
        CacheSizes::default(),
    ));
    let t = Arc::new(Depth(gate.clone()));
    let held = {
        let (d, t) = (d.clone(), t.clone());
        tokio::spawn(async move { d.get(REPO, &*t, x[9]).await })
    };
    gate.entered.notified().await;
    // While the batch for x is held, a request for y finishes.
    assert_eq!(depth(&d.get(REPO, &*t, y[9]).await.unwrap()), 10);
    assert!(!held.is_finished());
    gate.release.notify_one();
    assert_eq!(depth(&held.await.unwrap().unwrap()), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_waiting_on_a_cancelled_batch_derives_the_keys_itself() {
    let meta = Arc::new(Counting::default());
    let shared = chain(&meta, None, 100, "shared").await;
    let b = chain(&meta, Some(shared[99]), 10, "b").await;
    let gate = Arc::new(Gate {
        at: shared[50],
        entered: Notify::new(),
        release: Notify::new(),
    });
    let d = Arc::new(Deriver::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
        CacheSizes::default(),
    ));
    let t = Arc::new(Depth(gate.clone()));
    let first = {
        let (d, t) = (d.clone(), t.clone());
        let tip = shared[99];
        tokio::spawn(async move { d.get(REPO, &*t, tip).await })
    };
    gate.entered.notified().await;
    let second = {
        let (d, t) = (d.clone(), t.clone());
        let tip = b[9];
        tokio::spawn(async move { d.get(REPO, &*t, tip).await })
    };
    while d.in_flight() < 110 {
        tokio::task::yield_now().await;
    }
    // The first request is cancelled mid-batch, having written nothing; the
    // second derives every key it waited for, the held one included.
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    gate.release.notify_one();
    assert_eq!(depth(&second.await.unwrap().unwrap()), 110);
    let writes = meta.writes.lock().unwrap();
    for id in shared.iter().chain(&b) {
        let key = MetaKey::Derived(t.key(*id));
        assert_eq!(writes.get(&key), Some(&1), "{id}");
    }
    assert_eq!(d.in_flight(), 0);
}
