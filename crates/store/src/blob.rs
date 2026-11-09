//! Blob storage: stored frames, keyed by repo, object kind, and id.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use scion_core::{BlobId, ChunkId, TreeId};

use crate::{Coalescer, PutOutcome, RepoId, StoreError};

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
/// Writes take no locks and are robust to contention and transient failures.
/// Concurrent puts of one key in this process are coalesced: they share one
/// write to the store, the first caller's, and every other caller reports
/// `Existed` once it succeeds; no put ever waits for another put's turn, and
/// puts of different keys never wait for each other. A put that fails
/// transiently is retried with bounded, jittered backoff; that is safe because
/// a key is content-addressed and every frame for an id is byte-identical
/// (INV-44), so a retry can only find the same bytes. A conditional write that
/// reports the key exists is confirmed with a read before it is believed: S3
/// also answers a write that races another writer still in flight with a
/// conflict, and that is not proof the key holds bytes (INV-22).
pub struct ObjectBlobStore {
    store: Arc<dyn ObjectStore>,
    writes: Writes,
}

/// The puts in flight in this process, by key.
type Writes = Coalescer<(RepoId, ObjectKey), PutOutcome>;

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
            writes: Writes::default(),
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
}

/// A boxed, sendable future.
type Pending<'a, T> =
    std::pin::Pin<Box<dyn Future<Output = Result<T, object_store::Error>> + Send + 'a>>;

/// The three object-store operations [`ObjectBlobStore`] is built on.
trait Objects: Send + Sync {
    /// Writes `bytes` at `path` unless an object exists there.
    fn create<'a>(&'a self, path: &'a Path, bytes: Bytes) -> Pending<'a, ()>;

    /// Whether an object exists at `path`.
    fn exists<'a>(&'a self, path: &'a Path) -> Pending<'a, bool>;

    /// The object at `path`, or `None` when there is none.
    fn read<'a>(&'a self, path: &'a Path) -> Pending<'a, Option<Bytes>>;
}

impl Objects for dyn ObjectStore {
    fn create<'a>(&'a self, path: &'a Path, bytes: Bytes) -> Pending<'a, ()> {
        Box::pin(async move {
            self.put_opts(path, bytes.into(), PutOptions::from(PutMode::Create))
                .await
                .map(drop)
        })
    }

    fn exists<'a>(&'a self, path: &'a Path) -> Pending<'a, bool> {
        Box::pin(async move {
            match self.head(path).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(e),
            }
        })
    }

    fn read<'a>(&'a self, path: &'a Path) -> Pending<'a, Option<Bytes>> {
        Box::pin(async move {
            match self.get(path).await {
                Ok(r) => r.bytes().await.map(Some),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e),
            }
        })
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

/// Puts `bytes` under `key`, sharing one write with every concurrent put of
/// the same key through `writes`. Only the caller whose write ran can have
/// created the key; every other caller found it written.
async fn put_coalesced<O: Objects + ?Sized>(
    writes: &Writes,
    objects: &O,
    repo: RepoId,
    key: ObjectKey,
    bytes: Bytes,
) -> Result<PutOutcome, StoreError> {
    let path = ObjectBlobStore::path(repo, key);
    let mut wrote = false;
    let outcome = writes
        .run((repo, key), || {
            wrote = true;
            write(objects, &path, &bytes)
        })
        .await?;
    Ok(if wrote { outcome } else { PutOutcome::Existed })
}

/// Writes `bytes` at `path` unless an object exists there, retrying transient
/// failures and conflicts with writers still in flight.
async fn write<O: Objects + ?Sized>(
    objects: &O,
    path: &Path,
    bytes: &Bytes,
) -> Result<PutOutcome, StoreError> {
    let mut ambiguous = false;
    let mut last = None;
    for attempt in 0..PUT_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(backoff(path, attempt - 1)).await;
        }
        let failure = match objects.create(path, bytes.clone()).await {
            Ok(()) => return Ok(PutOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                match confirm(objects, path, bytes, ambiguous).await {
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

/// Reads the key back after the store said it exists. Returns `None` when it
/// does not exist yet: the store reported a writer still in flight. After an
/// `ambiguous` attempt, one that failed after it may have reached the store,
/// the stored bytes are compared with `ours` to tell whether they are this
/// call's own.
async fn confirm<O: Objects + ?Sized>(
    objects: &O,
    path: &Path,
    ours: &Bytes,
    ambiguous: bool,
) -> Result<Option<PutOutcome>, Attempt> {
    if !ambiguous {
        return match objects.exists(path).await {
            Ok(true) => Ok(Some(PutOutcome::Existed)),
            Ok(false) => Ok(None),
            Err(e) => Err(classify(e)),
        };
    }
    match objects.read(path).await {
        Ok(Some(stored)) => Ok(Some(if stored == *ours {
            PutOutcome::Created
        } else {
            PutOutcome::Existed
        })),
        Ok(None) => Ok(None),
        Err(e) => Err(classify(e)),
    }
}

impl BlobStore for ObjectBlobStore {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        put_coalesced(&self.writes, &*self.store, repo, key, bytes).await
    }

    async fn get(&self, repo: RepoId, key: ObjectKey) -> Result<Option<Bytes>, StoreError> {
        Objects::read(&*self.store, &Self::path(repo, key))
            .await
            .map_err(object_err)
    }

    async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, StoreError> {
        Objects::exists(&*self.store, &Self::path(repo, key))
            .await
            .map_err(object_err)
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
mod write_tests {
    //! Every branch of the write path, over an object store whose calls
    //! follow a script: transient failures, conflicts with writers still in
    //! flight, writes that land but report failure, and every outcome of
    //! the confirming read. Time is paused, so backoff costs nothing.

    use std::collections::VecDeque;
    use std::sync::Mutex;

    use object_store::memory::InMemory;
    use tokio::sync::Notify;

    use super::*;

    /// What one scripted call does.
    enum Act {
        /// Runs against the in-memory store.
        Pass,
        /// Fails with a transient error, without reaching the store.
        Transient,
        /// Writes the object, then fails with a transient error.
        LandThenTransient,
        /// Reports that the object exists without writing it, as S3 does
        /// for a writer racing another still in flight.
        Conflict,
        /// Fails with an error a retry cannot fix.
        Denied,
        /// Fails with an invalid-path error.
        BadPath,
        /// Signals `reached`, waits for `release`, then runs.
        Stall {
            reached: Arc<Notify>,
            release: Arc<Notify>,
        },
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Call {
        Create,
        Exists,
        Read,
    }

    /// An in-memory object store whose calls follow per-operation scripts;
    /// a call with no scripted act left passes.
    #[derive(Default)]
    struct Scripted {
        inner: InMemory,
        script: Mutex<Vec<(Call, VecDeque<Act>)>>,
        calls: Mutex<Vec<(Call, Path)>>,
    }

    fn transient() -> object_store::Error {
        object_store::Error::Generic {
            store: "scripted",
            source: "injected transient failure".into(),
        }
    }

    impl Scripted {
        fn with(script: Vec<(Call, Vec<Act>)>) -> Self {
            Self {
                script: Mutex::new(script.into_iter().map(|(c, a)| (c, a.into())).collect()),
                ..Self::default()
            }
        }

        fn next(&self, call: Call, path: &Path) -> Act {
            self.calls.lock().unwrap().push((call, path.clone()));
            let mut script = self.script.lock().unwrap();
            script
                .iter_mut()
                .find(|(c, _)| *c == call)
                .and_then(|(_, acts)| acts.pop_front())
                .unwrap_or(Act::Pass)
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().iter().map(|(c, _)| *c).collect()
        }

        fn creates_of(&self, path: &Path) -> usize {
            let calls = self.calls.lock().unwrap();
            calls
                .iter()
                .filter(|(c, p)| *c == Call::Create && p == path)
                .count()
        }

        /// Runs `act`, before the call proceeds to the in-memory store.
        async fn act(
            &self,
            act: Act,
            path: &Path,
            bytes: &Bytes,
        ) -> Result<(), object_store::Error> {
            match act {
                Act::Pass => Ok(()),
                Act::Transient => Err(transient()),
                Act::LandThenTransient => {
                    let inner: &dyn ObjectStore = &self.inner;
                    inner.create(path, bytes.clone()).await?;
                    Err(transient())
                }
                Act::Conflict => Err(object_store::Error::AlreadyExists {
                    path: path.to_string(),
                    source: "a conditional write is in flight".into(),
                }),
                Act::Denied => Err(object_store::Error::PermissionDenied {
                    path: path.to_string(),
                    source: "injected".into(),
                }),
                Act::BadPath => Err(Path::parse("a//b").unwrap_err().into()),
                Act::Stall { reached, release } => {
                    reached.notify_one();
                    release.notified().await;
                    Ok(())
                }
            }
        }
    }

    impl Objects for Scripted {
        fn create<'a>(&'a self, path: &'a Path, bytes: Bytes) -> Pending<'a, ()> {
            Box::pin(async move {
                let act = self.next(Call::Create, path);
                self.act(act, path, &bytes).await?;
                let inner: &dyn ObjectStore = &self.inner;
                inner.create(path, bytes).await
            })
        }

        fn exists<'a>(&'a self, path: &'a Path) -> Pending<'a, bool> {
            Box::pin(async move {
                let act = self.next(Call::Exists, path);
                self.act(act, path, &Bytes::new()).await?;
                let inner: &dyn ObjectStore = &self.inner;
                inner.exists(path).await
            })
        }

        fn read<'a>(&'a self, path: &'a Path) -> Pending<'a, Option<Bytes>> {
            Box::pin(async move {
                let act = self.next(Call::Read, path);
                self.act(act, path, &Bytes::new()).await?;
                let inner: &dyn ObjectStore = &self.inner;
                inner.read(path).await
            })
        }
    }

    fn key(seed: &[u8]) -> ObjectKey {
        ObjectKey::blob(BlobId::hash(seed))
    }

    const REPO: RepoId = RepoId(3);
    const OURS: &[u8] = b"ours";
    const THEIRS: &[u8] = b"theirs";

    /// Puts `OURS` under one key through `objects`, after `existing` (if
    /// any) was stored there, and returns the outcome and the stored bytes.
    async fn put(
        objects: &Scripted,
        existing: Option<&[u8]>,
    ) -> (Result<PutOutcome, StoreError>, Option<Bytes>) {
        let k = key(b"k");
        let path = ObjectBlobStore::path(REPO, k);
        if let Some(b) = existing {
            let inner: &dyn ObjectStore = &objects.inner;
            inner
                .create(&path, Bytes::copy_from_slice(b))
                .await
                .unwrap();
        }
        let writes = Writes::default();
        let outcome = put_coalesced(&writes, objects, REPO, k, Bytes::from_static(OURS)).await;
        assert_eq!(writes.in_flight(), 0);
        let inner: &dyn ObjectStore = &objects.inner;
        (outcome, inner.read(&path).await.unwrap())
    }

    use Act::*;
    use Call::{Create, Exists, Read};

    #[tokio::test(start_paused = true)]
    async fn a_first_write_creates() {
        let s = Scripted::default();
        let (outcome, stored) = put(&s, None).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Created);
        assert_eq!(stored.as_deref(), Some(OURS));
        assert_eq!(s.calls(), [Create]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_existing_key_is_confirmed_by_a_head_and_kept() {
        let s = Scripted::default();
        let (outcome, stored) = put(&s, Some(THEIRS)).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Existed);
        assert_eq!(stored.as_deref(), Some(THEIRS));
        assert_eq!(s.calls(), [Create, Exists]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_transient_failure_is_retried() {
        let s = Scripted::with(vec![(Create, vec![Transient, Transient])]);
        let (outcome, stored) = put(&s, None).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Created);
        assert_eq!(stored.as_deref(), Some(OURS));
        assert_eq!(s.calls(), [Create, Create, Create]);
    }

    /// A conflict with a writer still in flight is not believed: the key is
    /// read back, found absent, and written again.
    #[tokio::test(start_paused = true)]
    async fn a_conflict_with_no_object_behind_it_is_retried() {
        let s = Scripted::with(vec![(Create, vec![Conflict])]);
        let (outcome, stored) = put(&s, None).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Created);
        assert_eq!(stored.as_deref(), Some(OURS));
        assert_eq!(s.calls(), [Create, Exists, Create]);
    }

    /// A write that landed but reported failure is recognized on retry as
    /// this call's own, by its bytes.
    #[tokio::test(start_paused = true)]
    async fn a_write_that_landed_but_failed_is_found_to_be_ours() {
        let s = Scripted::with(vec![(Create, vec![LandThenTransient])]);
        let (outcome, stored) = put(&s, None).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Created);
        assert_eq!(stored.as_deref(), Some(OURS));
        assert_eq!(s.calls(), [Create, Create, Read]);
    }

    /// After an ambiguous failure, other bytes in the store mean another
    /// writer created the key.
    #[tokio::test(start_paused = true)]
    async fn after_an_ambiguous_failure_other_bytes_are_another_writers() {
        let s = Scripted::with(vec![(Create, vec![Transient])]);
        let (outcome, stored) = put(&s, Some(THEIRS)).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Existed);
        assert_eq!(stored.as_deref(), Some(THEIRS));
        assert_eq!(s.calls(), [Create, Create, Read]);
    }

    /// After an ambiguous failure, a conflict whose read finds nothing is a
    /// writer still in flight: the write is retried.
    #[tokio::test(start_paused = true)]
    async fn after_an_ambiguous_failure_a_conflict_with_no_object_is_retried() {
        let s = Scripted::with(vec![(Create, vec![Transient, Conflict])]);
        let (outcome, stored) = put(&s, None).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Created);
        assert_eq!(stored.as_deref(), Some(OURS));
        assert_eq!(s.calls(), [Create, Create, Read, Create]);
    }

    /// A confirming head or read that fails transiently is retried, and the
    /// failure makes the next confirmation compare bytes.
    #[tokio::test(start_paused = true)]
    async fn a_confirmation_that_fails_transiently_is_retried() {
        let s = Scripted::with(vec![(Exists, vec![Transient])]);
        let (outcome, _) = put(&s, Some(THEIRS)).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Existed);
        assert_eq!(s.calls(), [Create, Exists, Create, Read]);

        let s = Scripted::with(vec![(Create, vec![Transient]), (Read, vec![Transient])]);
        let (outcome, _) = put(&s, Some(OURS)).await;
        assert_eq!(outcome.unwrap(), PutOutcome::Created);
        assert_eq!(s.calls(), [Create, Create, Read, Create, Read]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_confirmation_that_fails_permanently_fails_the_put() {
        let s = Scripted::with(vec![(Exists, vec![Denied])]);
        let (outcome, _) = put(&s, Some(THEIRS)).await;
        assert!(
            matches!(outcome, Err(StoreError::Internal(_))),
            "{outcome:?}"
        );
        assert_eq!(s.calls(), [Create, Exists]);

        let s = Scripted::with(vec![(Create, vec![Transient]), (Read, vec![Denied])]);
        let (outcome, _) = put(&s, Some(THEIRS)).await;
        assert!(
            matches!(outcome, Err(StoreError::Internal(_))),
            "{outcome:?}"
        );
        assert_eq!(s.calls(), [Create, Create, Read]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_is_not_retried() {
        let s = Scripted::with(vec![(Create, vec![Denied])]);
        let (outcome, stored) = put(&s, None).await;
        assert!(
            matches!(outcome, Err(StoreError::Internal(_))),
            "{outcome:?}"
        );
        assert_eq!(stored, None);
        assert_eq!(s.calls(), [Create]);

        let s = Scripted::with(vec![(Create, vec![BadPath])]);
        let (outcome, _) = put(&s, None).await;
        assert!(
            matches!(outcome, Err(StoreError::InvalidArgument(_))),
            "{outcome:?}"
        );
        assert_eq!(s.calls(), [Create]);
    }

    #[tokio::test(start_paused = true)]
    async fn attempts_are_bounded() {
        let s = Scripted::with(vec![(
            Create,
            (0..PUT_ATTEMPTS).map(|_| Transient).collect(),
        )]);
        let (outcome, stored) = put(&s, None).await;
        match outcome {
            Err(StoreError::Unavailable(m)) => assert!(m.contains("failed 8 times"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(stored, None);
        assert_eq!(s.creates_of(&ObjectBlobStore::path(REPO, key(b"k"))), 8);

        let s = Scripted::with(vec![(
            Create,
            (0..PUT_ATTEMPTS).map(|_| Conflict).collect(),
        )]);
        let (outcome, _) = put(&s, None).await;
        match outcome {
            Err(StoreError::Unavailable(m)) => assert!(m.contains("another writer"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(s.calls().len(), 2 * PUT_ATTEMPTS as usize);
    }

    #[test]
    fn backoff_doubles_up_to_its_cap_and_jitters_down_by_at_most_half() {
        let path = Path::from("p");
        for attempt in 0..40 {
            let full = PUT_BACKOFF_BASE
                .saturating_mul(1 << attempt.min(16))
                .min(PUT_BACKOFF_MAX);
            let d = backoff(&path, attempt);
            assert!(d <= full && d >= full / 2, "attempt {attempt}: {d:?}");
        }
        assert_eq!(
            PUT_BACKOFF_BASE.saturating_mul(1 << 6).min(PUT_BACKOFF_MAX),
            PUT_BACKOFF_MAX
        );
    }

    /// Puts take no lock. While one put is stalled inside the store: a put
    /// of another key completes, and a put of the same key joins the
    /// stalled write instead of waiting to issue its own, so the store sees
    /// one write of that key.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_put_blocks_no_other_key_and_is_shared_by_its_own() {
        let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let s = Arc::new(Scripted::with(vec![(
            Create,
            vec![Stall {
                reached: reached.clone(),
                release: release.clone(),
            }],
        )]));
        let writes = Arc::new(Writes::default());
        let a = key(b"a");
        let first = {
            let (s, writes) = (s.clone(), writes.clone());
            tokio::spawn(async move {
                put_coalesced(&writes, &*s, REPO, a, Bytes::from_static(OURS)).await
            })
        };
        reached.notified().await;

        // Time is paused: if this waited on the stalled put, the runtime
        // would go idle and the timeout would fire at once.
        let other = tokio::time::timeout(
            std::time::Duration::from_secs(3600),
            put_coalesced(&writes, &*s, REPO, key(b"b"), Bytes::from_static(OURS)),
        )
        .await
        .expect("a put of another key waited for a stalled put");
        assert_eq!(other.unwrap(), PutOutcome::Created);

        let mut second = Box::pin(put_coalesced(
            &writes,
            &*s,
            REPO,
            a,
            Bytes::from_static(THEIRS),
        ));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                second.as_mut().poll(cx).is_pending()
            ))
            .await,
            "the second put of the key finished while the first was stalled"
        );
        release.notify_one();
        assert_eq!(first.await.unwrap().unwrap(), PutOutcome::Created);
        assert_eq!(second.await.unwrap(), PutOutcome::Existed);
        assert_eq!(s.creates_of(&ObjectBlobStore::path(REPO, a)), 1);
        assert_eq!(writes.in_flight(), 0);
        let inner: &dyn ObjectStore = &s.inner;
        assert_eq!(
            inner
                .read(&ObjectBlobStore::path(REPO, a))
                .await
                .unwrap()
                .as_deref(),
            Some(OURS)
        );
    }
}
