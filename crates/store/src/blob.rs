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

#[cfg(feature = "testing")]
mod memory;
#[cfg(feature = "testing")]
pub use memory::MemoryBlobStore;

/// How many times [`ObjectBlobStore::put_if_absent`] tries a write before it
/// reports the store unavailable.
const PUT_ATTEMPTS: u32 = 8;

/// The first retry's delay; each later retry doubles it, up to
/// [`PUT_BACKOFF_MAX`], and every delay is jittered down by up to half.
const PUT_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_millis(50);

/// The longest delay between two attempts of one write.
const PUT_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(2);

/// A [`BlobStore`] over an `object_store` store: S3-compatible storage in a
/// cluster, the local filesystem in single-node mode. The store must support
/// atomic put-if-absent (`PutMode::Create`).
///
/// Keys are `<repo>/<b|c|t>/<hex id>`.
///
/// Writes are robust to contention and transient failures. Concurrent puts
/// of one key in this process are serialized: they run one at a time, so at
/// most one is in flight, and each later one finds the key written. A put that fails transiently is retried with
/// bounded, jittered backoff; that is safe because a key is content-addressed
/// and every frame for an id is byte-identical (INV-44), so a retry can only
/// find the same bytes. A conditional write that reports the key exists is
/// confirmed with a read before it is believed: S3 also answers a write that
/// races another writer still in flight with a conflict, and that is not
/// proof the key holds bytes (INV-22).
pub struct ObjectBlobStore {
    store: Arc<dyn ObjectStore>,
    writing: KeyLocks,
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
        Self {
            store,
            writing: KeyLocks::default(),
        }
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

    /// Reads the key back after the store said it exists. Returns `None` when
    /// it does not exist yet: the store reported a writer still in flight.
    async fn confirm(
        &self,
        path: &Path,
        ours: &Bytes,
        ambiguous: bool,
    ) -> Result<Option<PutOutcome>, Attempt> {
        if !ambiguous {
            return match self.store.head(path).await {
                Ok(_) => Ok(Some(PutOutcome::Existed)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(classify(e)),
            };
        }
        // An earlier attempt failed after it may have reached the store, so
        // the bytes there may be this call's own.
        let stored = match self.store.get(path).await {
            Ok(r) => r.bytes().await.map_err(classify)?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(classify(e)),
        };
        Ok(Some(if stored == *ours {
            PutOutcome::Created
        } else {
            PutOutcome::Existed
        }))
    }
}

/// How one failed attempt at a store operation should be handled.
enum Attempt {
    /// The failure may be transient, and the request may have reached the
    /// store.
    Transient(object_store::Error),
    /// Retrying cannot succeed.
    Permanent(StoreError),
}

/// Sorts an `object_store` error into what a retry can and cannot fix.
fn classify(e: object_store::Error) -> Attempt {
    use object_store::Error as E;
    match e {
        // Network failures, timeouts, and server errors left after the
        // client's own retries all surface as `Generic`.
        E::Generic { .. } => Attempt::Transient(e),
        E::InvalidPath { .. } => {
            Attempt::Permanent(StoreError::invalid(format!("object store: {e}")))
        }
        // Missing permissions or credentials, a store without conditional
        // writes, or a bad configuration: an operator must fix these.
        _ => Attempt::Permanent(StoreError::Internal(format!("object store: {e}"))),
    }
}

fn object_err(e: object_store::Error) -> StoreError {
    match classify(e) {
        Attempt::Transient(e) => StoreError::Unavailable(format!("object store: {e}")),
        Attempt::Permanent(e) => e,
    }
}

/// The delay before retry `attempt` (0 for the first retry) of the write of
/// `path`: exponential, capped, and jittered down by up to half so that
/// writers that collided do not collide again in step.
fn backoff(path: &Path, attempt: u32) -> std::time::Duration {
    let full = PUT_BACKOFF_BASE
        .saturating_mul(1 << attempt.min(16))
        .min(PUT_BACKOFF_MAX);
    let mut seed = std::hash::DefaultHasher::new();
    std::hash::Hash::hash(
        &(path.as_ref(), attempt, std::time::SystemTime::now()),
        &mut seed,
    );
    let jitter = std::hash::Hasher::finish(&seed) % 1000;
    full.mul_f64(1.0 - jitter as f64 / 2000.0)
}

/// One key's write lock.
type KeyLock = Arc<tokio::sync::Mutex<()>>;

/// Per-key async locks for the keys being written in this process; an entry
/// lives only while some put of its key runs or waits.
#[derive(Default)]
struct KeyLocks {
    locks: Mutex<HashMap<(RepoId, ObjectKey), KeyLock>>,
}

/// Holds one key's write lock, and forgets the key when no one else waits.
/// A turn drops its guard first, then its own reference to the lock under the
/// table's lock: every reference still alive when a turn checks the count
/// belongs to a turn that checks later, so the last check sees exactly one.
struct KeyTurn<'a> {
    locks: &'a KeyLocks,
    key: (RepoId, ObjectKey),
    lock: Option<KeyLock>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl KeyLocks {
    async fn turn(&self, key: (RepoId, ObjectKey)) -> Result<KeyTurn<'_>, StoreError> {
        let lock = self
            .locks
            .lock()
            .map_err(|_| StoreError::Internal("the key lock table is poisoned".into()))?
            .entry(key)
            .or_default()
            .clone();
        // The turn exists before the wait, so a caller cancelled while it
        // waits still releases its reference and lets the entry go.
        let mut turn = KeyTurn {
            locks: self,
            key,
            lock: Some(lock),
            guard: None,
        };
        let lock = turn.lock.clone().expect("set above, taken only on drop");
        turn.guard = Some(lock.lock_owned().await);
        Ok(turn)
    }
}

impl Drop for KeyTurn<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        if let Ok(mut locks) = self.locks.locks.lock() {
            drop(self.lock.take());
            // Only the table's reference is left: nobody is writing or
            // waiting for this key.
            if locks
                .get(&self.key)
                .is_some_and(|l| Arc::strong_count(l) == 1)
            {
                locks.remove(&self.key);
            }
        }
    }
}

impl BlobStore for ObjectBlobStore {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        let path = Self::path(repo, key);
        let _turn = self.writing.turn((repo, key)).await?;
        let mut ambiguous = false;
        let mut last = None;
        for attempt in 0..PUT_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(backoff(&path, attempt - 1)).await;
            }
            let written = self
                .store
                .put_opts(
                    &path,
                    bytes.clone().into(),
                    PutOptions::from(PutMode::Create),
                )
                .await;
            let failure = match written {
                Ok(_) => return Ok(PutOutcome::Created),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    match self.confirm(&path, &bytes, ambiguous).await {
                        Ok(Some(outcome)) => return Ok(outcome),
                        // A concurrent writer is still in flight; try again.
                        Ok(None) => continue,
                        Err(f) => f,
                    }
                }
                Err(e) => classify(e),
            };
            match failure {
                Attempt::Transient(e) => {
                    ambiguous = true;
                    last = Some(e);
                }
                Attempt::Permanent(e) => return Err(e),
            }
        }
        Err(StoreError::Unavailable(match last {
            Some(e) => {
                format!("object store: writing {path} failed {PUT_ATTEMPTS} times; last: {e}")
            }
            None => format!(
                "object store: {path} stayed locked by another writer for {PUT_ATTEMPTS} attempts"
            ),
        }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_keys_expose_their_kind_and_id() {
        let raw = *BlobId::hash(b"key").as_bytes();
        let cases = [
            (ObjectKey::blob(BlobId::from_bytes(raw)), ObjectKind::Blob),
            (
                ObjectKey::chunk(ChunkId::from_bytes(raw)),
                ObjectKind::Chunk,
            ),
            (ObjectKey::tree(TreeId::from_bytes(raw)), ObjectKind::Tree),
        ];
        for (key, kind) in cases {
            assert_eq!(key.kind(), kind);
            assert_eq!(key.id(), &raw);
        }
    }

    #[test]
    fn object_paths_are_repo_kind_and_hex_id() {
        let id = BlobId::from_bytes([0xab; 32]);
        assert_eq!(
            ObjectBlobStore::path(RepoId(42), ObjectKey::blob(id)).as_ref(),
            format!("42/b/{}", "ab".repeat(32))
        );
        let store = ObjectBlobStore::new(Arc::new(object_store::memory::InMemory::new()));
        assert!(format!("{store:?}").starts_with("ObjectBlobStore"));
    }
}

#[cfg(test)]
mod key_lock_tests {
    use super::*;

    #[tokio::test]
    async fn a_caller_cancelled_while_waiting_leaves_no_entry() {
        let locks = KeyLocks::default();
        let key = (RepoId(1), ObjectKey::blob(BlobId::hash(b"k")));
        let first = locks.turn(key).await.unwrap();
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(10), locks.turn(key)).await;
        assert!(waited.is_err(), "the key was held");
        drop(first);
        assert!(locks.locks.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod key_lock_race_tests {
    use super::*;

    /// Many writers of one key on many threads, finishing together, leave
    /// no entry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn turns_finishing_together_leave_no_entry() {
        let locks = Arc::new(KeyLocks::default());
        for round in 0..2000u32 {
            let key = (
                RepoId(1),
                ObjectKey::blob(BlobId::hash(&(round % 4).to_le_bytes())),
            );
            let gate = Arc::new(tokio::sync::Barrier::new(8));
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..8 {
                let (locks, gate) = (locks.clone(), gate.clone());
                tasks.spawn(async move {
                    gate.wait().await;
                    drop(locks.turn(key).await.unwrap());
                });
            }
            while let Some(r) = tasks.join_next().await {
                r.unwrap();
            }
            assert!(locks.locks.lock().unwrap().is_empty(), "round {round}");
        }
    }
}
