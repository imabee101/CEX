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
    use store::testing::object_store;

    async fn make() -> (Arc<ObjectBlobStore>, ()) {
        (Arc::new(ObjectBlobStore::new(Arc::new(object_store()))), ())
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
