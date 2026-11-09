//! The `BlobStore` conformance suite on every backend: in-memory, the dev
//! environment's S3-compatible object store (one key prefix per test), and
//! the local filesystem used in single-node mode.

mod memory {
    use std::sync::Arc;

    use store::MemoryBlobStore;

    async fn make() -> (Arc<MemoryBlobStore>, ()) {
        (Arc::new(MemoryBlobStore::new()), ())
    }

    store::blob_store_conformance!(make);
}

mod s3 {
    use std::sync::Arc;

    use store::ObjectBlobStore;
    use store::testing::TestPrefix;

    async fn make() -> (Arc<ObjectBlobStore>, TestPrefix) {
        let prefix = TestPrefix::create();
        (
            Arc::new(ObjectBlobStore::new(Arc::new(prefix.store()))),
            prefix,
        )
    }

    store::blob_store_conformance!(make);
}

mod local_fs {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use store::ObjectBlobStore;
    use store::testing::TempDir;

    async fn make() -> (Arc<ObjectBlobStore>, TempDir) {
        let dir = TempDir::create();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        (Arc::new(ObjectBlobStore::new(Arc::new(fs))), dir)
    }

    store::blob_store_conformance!(make);
}

/// Separate clients of one store, as separate servers are, racing to write
/// one key with different bytes: every put succeeds, exactly one creates the
/// key, and its bytes are the ones stored. Unlike the in-process conformance
/// check, nothing here coalesces the writers, so the store itself sees the
/// contention.
mod separate_clients {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::ObjectStore;
    use scion_core::BlobId;
    use store::{BlobStore, ObjectBlobStore, ObjectKey, PutOutcome, RepoId};

    /// Concurrent writers of one key, enough that their conditional puts
    /// collide. The dev object store queues conditional writers of one key
    /// and answers them in one-second steps, so this race takes seconds there;
    /// S3 and the local filesystem answer at once.
    const CLIENTS: usize = 8;

    async fn race(stores: Vec<ObjectBlobStore>) {
        let key = ObjectKey::blob(BlobId::hash(b"raced by separate clients"));
        let barrier = Arc::new(tokio::sync::Barrier::new(stores.len()));
        let mut tasks = Vec::new();
        for (i, store) in stores.into_iter().enumerate() {
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let bytes = Bytes::from(format!("client {i}").repeat(512));
                barrier.wait().await;
                let outcome = store.put_if_absent(RepoId(1), key, bytes.clone()).await;
                (store, outcome, bytes)
            }));
        }
        let mut created = Vec::new();
        let mut reader = None;
        for task in tasks {
            let (store, outcome, bytes) = task.await.unwrap();
            match outcome {
                Ok(PutOutcome::Created) => created.push(bytes),
                Ok(PutOutcome::Existed) => {}
                Err(e) => panic!("a racing put failed: {e:?}"),
            }
            reader = Some(store);
        }
        assert_eq!(created.len(), 1, "created {} times", created.len());
        let stored = reader.unwrap().get(RepoId(1), key).await.unwrap();
        assert!(stored == Some(created[0].clone()));
    }

    fn clients(store: Arc<dyn ObjectStore>) -> Vec<ObjectBlobStore> {
        (0..CLIENTS)
            .map(|_| ObjectBlobStore::new(store.clone()))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn on_s3() {
        let prefix = store::testing::TestPrefix::create();
        // Each client gets its own HTTP client, as separate servers do.
        let separate: Vec<ObjectBlobStore> = (0..CLIENTS)
            .map(|_| ObjectBlobStore::new(Arc::new(prefix.store())))
            .collect();
        race(separate).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn on_the_local_filesystem() {
        let dir = store::testing::TempDir::create();
        let fs = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        race(clients(Arc::new(fs))).await;
    }
}
