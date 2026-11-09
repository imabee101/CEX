//! The in-memory blob store, for tests only.

use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;

use crate::{BlobStore, ObjectKey, PutOutcome, RepoId, StoreError};

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
            .map_err(|_| StoreError::Internal("in-memory store lock poisoned".into()))
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
