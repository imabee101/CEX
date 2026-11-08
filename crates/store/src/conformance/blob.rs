//! Checks every [`BlobStore`] backend must pass.

use std::sync::Arc;

use bytes::Bytes;
use scion_core::{BlobId, ChunkId, TreeId};

use super::{CONCURRENCY, pseudo_random};
use crate::{BlobStore, ObjectKey, PutOutcome, RepoId, StoreError};

const REPO: RepoId = RepoId(7);
const OTHER_REPO: RepoId = RepoId(8);

fn keys(seed: &[u8]) -> [ObjectKey; 3] {
    [
        ObjectKey::blob(BlobId::hash(seed)),
        ObjectKey::chunk(ChunkId::hash(seed)),
        ObjectKey::tree(TreeId::hash(seed)),
    ]
}

/// Frames of every kind read back exactly as written; absent keys read as
/// `None` and are not contained.
pub async fn objects_round_trip<S: BlobStore>(store: Arc<S>) {
    for (i, key) in keys(b"round trip").into_iter().enumerate() {
        assert_eq!(store.get(REPO, key).await.unwrap(), None);
        assert!(!store.contains(REPO, key).await.unwrap());
        let bytes = Bytes::from(pseudo_random(i as u64, 1000 + i));
        assert_eq!(
            store.put_if_absent(REPO, key, bytes.clone()).await.unwrap(),
            PutOutcome::Created
        );
        assert!(store.contains(REPO, key).await.unwrap());
        assert_eq!(store.get(REPO, key).await.unwrap(), Some(bytes));
    }
}

/// A put of an existing key never changes its stored bytes.
pub async fn put_never_overwrites<S: BlobStore>(store: Arc<S>) {
    for key in keys(b"overwrite") {
        let first = Bytes::from_static(b"first");
        assert_eq!(
            store.put_if_absent(REPO, key, first.clone()).await.unwrap(),
            PutOutcome::Created
        );
        for other in [&b"second"[..], b"", &pseudo_random(1, 10_000)] {
            assert_eq!(
                store
                    .put_if_absent(REPO, key, Bytes::copy_from_slice(other))
                    .await
                    .unwrap(),
                PutOutcome::Existed
            );
        }
        assert_eq!(store.get(REPO, key).await.unwrap(), Some(first));
    }
}

/// Blob, chunk, and tree frames with the same id bytes are separate objects.
pub async fn kinds_are_separate_namespaces<S: BlobStore>(store: Arc<S>) {
    let raw = *BlobId::hash(b"shared").as_bytes();
    let keys = [
        ObjectKey::blob(BlobId::from_bytes(raw)),
        ObjectKey::chunk(ChunkId::from_bytes(raw)),
        ObjectKey::tree(TreeId::from_bytes(raw)),
    ];
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            store
                .put_if_absent(REPO, *key, Bytes::from(vec![i as u8; 3]))
                .await
                .unwrap(),
            PutOutcome::Created,
            "{key:?} collided with an earlier kind"
        );
    }
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            store.get(REPO, *key).await.unwrap(),
            Some(Bytes::from(vec![i as u8; 3]))
        );
    }
}

/// An object in one repo is invisible to another and does not deduplicate
/// against it.
pub async fn repos_are_isolated<S: BlobStore>(store: Arc<S>) {
    for key in keys(b"isolated") {
        store
            .put_if_absent(REPO, key, Bytes::from_static(b"a"))
            .await
            .unwrap();
        assert_eq!(store.get(OTHER_REPO, key).await.unwrap(), None);
        assert!(!store.contains(OTHER_REPO, key).await.unwrap());
        assert_eq!(
            store
                .put_if_absent(OTHER_REPO, key, Bytes::from_static(b"b"))
                .await
                .unwrap(),
            PutOutcome::Created
        );
        assert_eq!(
            store.get(REPO, key).await.unwrap(),
            Some(Bytes::from_static(b"a"))
        );
        assert_eq!(
            store.get(OTHER_REPO, key).await.unwrap(),
            Some(Bytes::from_static(b"b"))
        );
    }
}

/// Concurrent puts of one key with different bytes create it exactly once,
/// and every later read returns the winner's bytes. All three kinds race at
/// once. Under contention a put may fail with the retryable `Unavailable`,
/// never with anything else; retrying it then finds the key and must not
/// create it again.
pub async fn concurrent_puts_of_one_key_create_it_once<S: BlobStore>(store: Arc<S>) {
    let keys = keys(b"concurrent");
    let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY * keys.len()));
    let mut tasks = Vec::new();
    for key in keys {
        for i in 0..CONCURRENCY {
            let (store, barrier) = (store.clone(), barrier.clone());
            tasks.push(tokio::spawn(async move {
                let value = Bytes::from(pseudo_random(i as u64, 4096));
                barrier.wait().await;
                let outcome = store.put_if_absent(REPO, key, value.clone()).await;
                (key, outcome, value)
            }));
        }
    }
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    for key in keys {
        let mut winners = Vec::new();
        for (k, outcome, value) in &results {
            if *k != key {
                continue;
            }
            let outcome = match outcome {
                Ok(o) => *o,
                Err(StoreError::Unavailable(_)) => {
                    let retried = store.put_if_absent(REPO, key, value.clone()).await;
                    assert!(
                        matches!(retried, Ok(PutOutcome::Existed)),
                        "{key:?}: a retry after losing the race returned {retried:?}"
                    );
                    PutOutcome::Existed
                }
                Err(e) => panic!("{key:?}: a racing put failed with {e:?}"),
            };
            if outcome == PutOutcome::Created {
                winners.push(value.clone());
            }
        }
        assert_eq!(
            winners.len(),
            1,
            "{key:?} was created {} times",
            winners.len()
        );
        assert!(store.get(REPO, key).await.unwrap() == Some(winners[0].clone()));
    }
}

/// Frames larger than a chunk round-trip byte for byte.
pub async fn large_objects_round_trip<S: BlobStore>(store: Arc<S>) {
    let big = Bytes::from(pseudo_random(9, (4 << 20) + 4096));
    let key = ObjectKey::chunk(ChunkId::hash(&big));
    store.put_if_absent(REPO, key, big.clone()).await.unwrap();
    assert!(store.get(REPO, key).await.unwrap() == Some(big));
}
