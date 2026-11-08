//! Blob storage: stored frames, keyed by repo, object kind, and id.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use scion_core::{BlobId, ChunkId, TreeId};

use crate::{PutOutcome, RepoId, StoreError};

/// The namespace of a stored frame. A blob's key holds its blob frame or,
/// for a chunked blob, its chunk-list frame.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ObjectKind {
    /// A blob or chunk-list frame, keyed by `BlobId`.
    Blob,
    /// A chunk frame, keyed by `ChunkId`.
    Chunk,
    /// A tree frame, keyed by `TreeId`.
    Tree,
}

/// The key of one stored frame within a repo.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ObjectKey {
    kind: ObjectKind,
    id: [u8; 32],
}

impl ObjectKey {
    /// The key of a blob's frame (a blob frame or a chunk-list frame).
    pub fn blob(id: BlobId) -> Self {
        Self {
            kind: ObjectKind::Blob,
            id: *id.as_bytes(),
        }
    }

    /// The key of a chunk's frame.
    pub fn chunk(id: ChunkId) -> Self {
        Self {
            kind: ObjectKind::Chunk,
            id: *id.as_bytes(),
        }
    }

    /// The key of a tree's frame.
    pub fn tree(id: TreeId) -> Self {
        Self {
            kind: ObjectKind::Tree,
            id: *id.as_bytes(),
        }
    }

    /// The object's kind.
    pub fn kind(&self) -> ObjectKind {
        self.kind
    }

    /// The object's id bytes.
    pub fn id(&self) -> &[u8; 32] {
        &self.id
    }
}

/// Blob storage for frames. Every write is put-if-absent: a put of an
/// existing key leaves its stored bytes unchanged.
pub trait BlobStore: Send + Sync + 'static {
    /// Stores `bytes` under `key` unless the key exists.
    fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> impl Future<Output = Result<PutOutcome, StoreError>> + Send;

    /// Reads the stored bytes, or `None` when the key is absent.
    fn get(
        &self,
        repo: RepoId,
        key: ObjectKey,
    ) -> impl Future<Output = Result<Option<Bytes>, StoreError>> + Send;

    /// Whether the key exists.
    fn contains(
        &self,
        repo: RepoId,
        key: ObjectKey,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

/// A [`BlobStore`] held in process memory. For tests only.
#[derive(Debug, Default)]
pub struct MemoryBlobStore {
    objects: Mutex<Objects>,
}

type Objects = HashMap<(RepoId, ObjectKey), Bytes>;

impl MemoryBlobStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Objects>, StoreError> {
        self.objects
            .lock()
            .map_err(|_| StoreError::Unavailable("in-memory store lock poisoned".into()))
    }
}

impl BlobStore for MemoryBlobStore {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        Ok(match self.lock()?.entry((repo, key)) {
            std::collections::hash_map::Entry::Occupied(_) => PutOutcome::Existed,
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(bytes);
                PutOutcome::Created
            }
        })
    }

    async fn get(&self, repo: RepoId, key: ObjectKey) -> Result<Option<Bytes>, StoreError> {
        Ok(self.lock()?.get(&(repo, key)).cloned())
    }

    async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, StoreError> {
        Ok(self.lock()?.contains_key(&(repo, key)))
    }
}

/// A [`BlobStore`] over an `object_store` store: S3-compatible storage in a
/// cluster, the local filesystem in single-node mode. The store must support
/// atomic put-if-absent (`PutMode::Create`).
///
/// Keys are `<repo>/<b|c|t>/<hex id>`.
pub struct ObjectBlobStore {
    store: Arc<dyn ObjectStore>,
}

impl fmt::Debug for ObjectBlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectBlobStore")
            .field("store", &format_args!("{}", self.store))
            .finish()
    }
}

impl ObjectBlobStore {
    /// Wraps an object store.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn path(repo: RepoId, key: ObjectKey) -> Path {
        let kind = match key.kind {
            ObjectKind::Blob => "b",
            ObjectKind::Chunk => "c",
            ObjectKind::Tree => "t",
        };
        let mut hex = String::with_capacity(64);
        for b in key.id {
            use fmt::Write as _;
            // Writing to a String cannot fail.
            let _ = write!(hex, "{b:02x}");
        }
        Path::from(format!("{}/{kind}/{hex}", repo.0))
    }
}

fn object_err(e: object_store::Error) -> StoreError {
    StoreError::Unavailable(format!("object store: {e}"))
}

impl BlobStore for ObjectBlobStore {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        let path = Self::path(repo, key);
        match self
            .store
            .put_opts(&path, bytes.into(), PutOptions::from(PutMode::Create))
            .await
        {
            Ok(_) => Ok(PutOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(PutOutcome::Existed),
            // Anything else, including a conflict with a concurrent writer
            // still in flight, is not proof the key holds bytes: fail closed
            // and let the caller retry.
            Err(e) => Err(object_err(e)),
        }
    }

    async fn get(&self, repo: RepoId, key: ObjectKey) -> Result<Option<Bytes>, StoreError> {
        match self.store.get(&Self::path(repo, key)).await {
            Ok(result) => Ok(Some(result.bytes().await.map_err(object_err)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(object_err(e)),
        }
    }

    async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, StoreError> {
        match self.store.head(&Self::path(repo, key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(object_err(e)),
        }
    }
}
