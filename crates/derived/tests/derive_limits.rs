//! The per-request derivation bounds, at and past each limit, on the
//! in-memory store: one request derives at most `MAX_DERIVE_PER_REQUEST`
//! changesets (the oldest), and fails retryably past that with exactly that
//! many derived; and one request explores at most `MAX_DERIVE_EXPLORE`
//! underived changesets, failing for good, having derived nothing, past
//! that. The Postgres acceptance tests check the same bounds through create.

use std::sync::Arc;

use bytes::Bytes;
use derived::{
    CacheSizes, DERIVE_BATCH, DeriveCx, DeriveError, DerivedType, Deriver, MAX_DERIVE_EXPLORE,
    MAX_DERIVE_PER_REQUEST, Mode,
};
use scion_core::{Changeset, ChangesetId, ChangesetParts, Timestamp};
use store::{BlobStore, MemoryBlobStore, MemoryMetaStore, MetaKey, MetaStore, RepoId};

const REPO: RepoId = RepoId(1);

/// A type whose value is the changeset's depth: 1 plus its deepest
/// parent's value, so it reads every parent's value.
struct Depth;

impl DerivedType for Depth {
    const KIND: u16 = 0xfff0;
    const VERSION: u16 = 1;
    const MODE: Mode = Mode::Deferred;

    async fn derive<'c, 'd, M: MetaStore, B: BlobStore>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        _id: ChangesetId,
        cs: &'c Changeset,
    ) -> Result<Bytes, DeriveError> {
        let mut depth = 0u64;
        for p in &cs.parts().parents {
            let v = cx.value(self, *p).await?;
            depth = depth.max(u64::from_le_bytes(v[..].try_into().unwrap()));
        }
        Ok(Bytes::copy_from_slice(&(depth + 1).to_le_bytes()))
    }
}

fn changeset(parents: &[ChangesetId], i: usize) -> Changeset {
    let mut p = ChangesetParts::default();
    p.parents = parents.to_vec();
    p.author = b"Test <test@example.com>".to_vec();
    p.author_date = Timestamp::new(1_700_000_000, 0).unwrap();
    p.message = format!("{i}").into_bytes();
    Changeset::new(p).unwrap()
}

struct Repo {
    meta: Arc<MemoryMetaStore>,
    blobs: Arc<MemoryBlobStore>,
    /// Every changeset, parents first.
    ids: Vec<ChangesetId>,
}

impl Repo {
    /// A chain of `n` stored changesets with no derived values.
    async fn chain(n: usize) -> Self {
        let meta = Arc::new(MemoryMetaStore::new());
        let mut ids: Vec<ChangesetId> = Vec::with_capacity(n);
        let mut rows = Vec::new();
        for i in 0..n {
            let cs = changeset(&ids[ids.len().saturating_sub(1)..], i);
            ids.push(cs.id());
            rows.push((MetaKey::Changeset(cs.id()), Bytes::from(cs.encode())));
        }
        for batch in rows.chunks(4096) {
            meta.put(REPO, batch).await.unwrap();
        }
        Self {
            meta,
            blobs: Arc::new(MemoryBlobStore::new()),
            ids,
        }
    }

    /// A fresh server.
    fn deriver(&self) -> Deriver<MemoryMetaStore, MemoryBlobStore> {
        Deriver::new(self.meta.clone(), self.blobs.clone(), CacheSizes::default())
    }

    /// Which changesets have a stored value, in order.
    async fn stored(&self) -> Vec<bool> {
        let mut out = Vec::with_capacity(self.ids.len());
        for chunk in self.ids.chunks(DERIVE_BATCH) {
            let keys: Vec<MetaKey> = chunk
                .iter()
                .map(|id| MetaKey::Derived(Depth.key(*id)))
                .collect();
            out.extend(
                self.meta
                    .get(REPO, &keys)
                    .await
                    .unwrap()
                    .iter()
                    .map(Option::is_some),
            );
        }
        out
    }

    fn tip(&self) -> ChangesetId {
        *self.ids.last().unwrap()
    }
}

#[tokio::test]
async fn a_request_derives_exactly_its_bound_of_changesets() {
    let repo = Repo::chain(MAX_DERIVE_PER_REQUEST).await;
    let v = repo.deriver().get(REPO, &Depth, repo.tip()).await.unwrap();
    assert_eq!(v[..], (MAX_DERIVE_PER_REQUEST as u64).to_le_bytes());
    assert!(repo.stored().await.iter().all(|s| *s));
}

#[tokio::test]
async fn one_more_is_pending_after_deriving_the_oldest_up_to_the_bound() {
    let repo = Repo::chain(MAX_DERIVE_PER_REQUEST + 1).await;
    let err = repo
        .deriver()
        .get(REPO, &Depth, repo.tip())
        .await
        .unwrap_err();
    assert!(matches!(err, DeriveError::Pending(_)), "{err}");
    // Exactly the oldest MAX_DERIVE_PER_REQUEST, so the retry has one left.
    let stored = repo.stored().await;
    assert!(stored[..MAX_DERIVE_PER_REQUEST].iter().all(|s| *s));
    assert!(!stored[MAX_DERIVE_PER_REQUEST]);
    let v = repo.deriver().get(REPO, &Depth, repo.tip()).await.unwrap();
    assert_eq!(v[..], (MAX_DERIVE_PER_REQUEST as u64 + 1).to_le_bytes());
}

#[tokio::test]
async fn exploring_exactly_the_limit_is_pending_and_makes_progress() {
    let repo = Repo::chain(MAX_DERIVE_EXPLORE).await;
    let err = repo
        .deriver()
        .get(REPO, &Depth, repo.tip())
        .await
        .unwrap_err();
    assert!(matches!(err, DeriveError::Pending(_)), "{err}");
    let stored = repo.stored().await;
    assert_eq!(
        stored.iter().filter(|s| **s).count(),
        MAX_DERIVE_PER_REQUEST
    );
    assert!(stored[..MAX_DERIVE_PER_REQUEST].iter().all(|s| *s));
}

#[tokio::test]
async fn exploring_past_the_limit_fails_for_good_and_derives_nothing() {
    let repo = Repo::chain(MAX_DERIVE_EXPLORE + 1).await;
    let err = repo
        .deriver()
        .get(REPO, &Depth, repo.tip())
        .await
        .unwrap_err();
    assert!(matches!(err, DeriveError::LimitExceeded(_)), "{err}");
    assert!(repo.stored().await.iter().all(|s| !*s));
    // With one ancestor derived, the rest is within the limit again.
    let d = repo.deriver();
    d.get(REPO, &Depth, repo.ids[0]).await.unwrap();
    let err = d.get(REPO, &Depth, repo.tip()).await.unwrap_err();
    assert!(matches!(err, DeriveError::Pending(_)), "{err}");
}
