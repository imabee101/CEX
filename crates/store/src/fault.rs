//! Fault-injection and observation wrappers for tests: [`FaultMetaStore`] and
//! [`FaultBlobStore`] wrap any backend and share one [`Faults`] controller,
//! which numbers every write across both stores, can crash the "process" at
//! any write step (a ref compare-and-swap is a write step too), can make one
//! write fail transiently, can forbid ref access, and counts every call.
//!
//! A crash makes the chosen write fail, before or after it is applied, and
//! every later call through either wrapper fail too, as if the process had
//! died; [`Faults::restart`] models the process coming back.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;

use crate::{
    BlobStore, CasOutcome, MetaKey, MetaStore, ObjectKey, PutOutcome, RefLogEntry, RefState,
    RefUpdate, RepoId, StoreError,
};

/// Where a crash happens relative to the write it is placed at.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CrashPoint {
    /// The write is not applied.
    BeforeWrite,
    /// The write is applied, but the caller never learns it.
    AfterWrite,
}

/// One write, as recorded in [`Faults::writes`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Write {
    /// A [`MetaStore::put`] of these keys.
    Meta(Vec<MetaKey>),
    /// A [`BlobStore::put_if_absent`] of this key.
    Blob(ObjectKey),
    /// A [`MetaStore::update_ref`] of this ref.
    Ref(String),
}

/// Call counts through the wrappers since the last [`Faults::reset_counts`].
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Counts {
    /// `MetaStore::get` calls.
    pub meta_gets: u64,
    /// Keys requested by those calls.
    pub meta_get_keys: u64,
    /// Bytes returned by those calls.
    pub meta_get_bytes: u64,
    /// `MetaStore::put` calls.
    pub meta_puts: u64,
    /// Entries written by those calls.
    pub meta_put_entries: u64,
    /// Bytes written by those calls.
    pub meta_put_bytes: u64,
    /// Calls to any ref method.
    pub ref_calls: u64,
    /// `BlobStore::get` and `BlobStore::contains` calls.
    pub blob_reads: u64,
    /// `BlobStore::put_if_absent` calls.
    pub blob_puts: u64,
}

#[derive(Debug, Default)]
struct State {
    next_write: u64,
    crash_at: Option<(u64, CrashPoint)>,
    fail_at: Option<u64>,
    crashed: bool,
    deny_refs: bool,
    ref_violations: Vec<String>,
    writes: Vec<Write>,
    counts: Counts,
}

/// The controller shared by a [`FaultMetaStore`] and a [`FaultBlobStore`].
#[derive(Debug, Default)]
pub struct Faults {
    state: Mutex<State>,
}

impl Faults {
    /// A controller that injects nothing.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic while holding the lock only interrupts counting.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Crashes at write step `step` (0-based, counting every write through
    /// either wrapper from now on, including writes already counted).
    pub fn crash_at(&self, step: u64, point: CrashPoint) {
        self.lock().crash_at = Some((step, point));
    }

    /// Makes write step `step` fail once with the retryable `Unavailable`,
    /// without applying it and without crashing: a transient failure that
    /// a caller may retry.
    pub fn fail_transiently_at(&self, step: u64) {
        self.lock().fail_at = Some(step);
    }

    /// Clears the crash, so calls succeed again, like a restarted process.
    pub fn restart(&self) {
        let mut s = self.lock();
        s.crashed = false;
        s.crash_at = None;
    }

    /// Whether a crash has happened and not been cleared.
    pub fn crashed(&self) -> bool {
        self.lock().crashed
    }

    /// Makes every ref method fail and be recorded as a violation.
    pub fn deny_refs(&self) {
        self.lock().deny_refs = true;
    }

    /// The ref calls made while refs were denied.
    pub fn ref_violations(&self) -> Vec<String> {
        self.lock().ref_violations.clone()
    }

    /// Every write attempted so far, in order, including a crashed one.
    pub fn writes(&self) -> Vec<Write> {
        self.lock().writes.clone()
    }

    /// The call counts.
    pub fn counts(&self) -> Counts {
        self.lock().counts
    }

    /// Zeroes the call counts and forgets the recorded writes; write steps
    /// keep counting from where they were.
    pub fn reset_counts(&self) {
        let mut s = self.lock();
        s.counts = Counts::default();
        s.writes.clear();
    }

    fn check_alive(&self) -> Result<(), StoreError> {
        if self.lock().crashed {
            return Err(crashed());
        }
        Ok(())
    }

    /// Counts a write and decides its fate: `Ok(true)` to apply it and
    /// report success, `Ok(false)` to apply it and then crash, or `Err` to
    /// crash without applying it.
    fn begin_write(&self, write: Write) -> Result<bool, StoreError> {
        let mut s = self.lock();
        if s.crashed {
            return Err(crashed());
        }
        let step = s.next_write;
        s.next_write += 1;
        s.writes.push(write);
        if s.fail_at == Some(step) {
            s.fail_at = None;
            return Err(StoreError::Unavailable("injected transient failure".into()));
        }
        match s.crash_at {
            Some((at, CrashPoint::BeforeWrite)) if at == step => {
                s.crashed = true;
                Err(crashed())
            }
            Some((at, CrashPoint::AfterWrite)) if at == step => Ok(false),
            _ => Ok(true),
        }
    }

    fn finish_write(&self, report: bool) -> Result<(), StoreError> {
        if report {
            return Ok(());
        }
        self.lock().crashed = true;
        Err(crashed())
    }

    fn ref_call(&self, what: String) -> Result<(), StoreError> {
        let mut s = self.lock();
        if s.crashed {
            return Err(crashed());
        }
        s.counts.ref_calls += 1;
        if s.deny_refs {
            s.ref_violations.push(what.clone());
            return Err(StoreError::Unavailable(format!(
                "ref access is forbidden here: {what}"
            )));
        }
        Ok(())
    }
}

fn crashed() -> StoreError {
    StoreError::Unavailable("injected crash".into())
}

/// A [`MetaStore`] wrapper controlled by a [`Faults`].
pub struct FaultMetaStore<S> {
    inner: Arc<S>,
    faults: Arc<Faults>,
}

impl<S> FaultMetaStore<S> {
    /// Wraps `inner`.
    pub fn new(inner: Arc<S>, faults: Arc<Faults>) -> Self {
        Self { inner, faults }
    }
}

impl<S> fmt::Debug for FaultMetaStore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultMetaStore").finish_non_exhaustive()
    }
}

impl<S: MetaStore> MetaStore for FaultMetaStore<S> {
    async fn put(
        &self,
        repo: RepoId,
        entries: &[(MetaKey, Bytes)],
    ) -> Result<Vec<PutOutcome>, StoreError> {
        let report = self
            .faults
            .begin_write(Write::Meta(entries.iter().map(|(k, _)| *k).collect()))?;
        let result = self.inner.put(repo, entries).await;
        {
            let mut s = self.faults.lock();
            s.counts.meta_puts += 1;
            s.counts.meta_put_entries += entries.len() as u64;
            s.counts.meta_put_bytes += entries.iter().map(|(_, b)| b.len() as u64).sum::<u64>();
        }
        let outcomes = result?;
        self.faults.finish_write(report)?;
        Ok(outcomes)
    }

    async fn get(&self, repo: RepoId, keys: &[MetaKey]) -> Result<Vec<Option<Bytes>>, StoreError> {
        self.faults.check_alive()?;
        let got = self.inner.get(repo, keys).await?;
        let mut s = self.faults.lock();
        s.counts.meta_gets += 1;
        s.counts.meta_get_keys += keys.len() as u64;
        s.counts.meta_get_bytes += got.iter().flatten().map(|b| b.len() as u64).sum::<u64>();
        Ok(got)
    }

    async fn get_ref(&self, repo: RepoId, name: &str) -> Result<Option<RefState>, StoreError> {
        self.faults.ref_call(format!("get_ref({name})"))?;
        self.inner.get_ref(repo, name).await
    }

    async fn list_refs(
        &self,
        repo: RepoId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RefState>, StoreError> {
        self.faults.ref_call(format!("list_refs({prefix})"))?;
        self.inner.list_refs(repo, prefix, after, limit).await
    }

    async fn update_ref(
        &self,
        repo: RepoId,
        update: &RefUpdate<'_>,
    ) -> Result<CasOutcome, StoreError> {
        self.faults
            .ref_call(format!("update_ref({})", update.name))?;
        let report = self
            .faults
            .begin_write(Write::Ref(update.name.to_owned()))?;
        let outcome = self.inner.update_ref(repo, update).await?;
        self.faults.finish_write(report)?;
        Ok(outcome)
    }

    async fn ref_log(
        &self,
        repo: RepoId,
        name: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>, StoreError> {
        self.faults.ref_call(format!("ref_log({name})"))?;
        self.inner.ref_log(repo, name, after_seq, limit).await
    }
}

/// A [`BlobStore`] wrapper controlled by a [`Faults`].
pub struct FaultBlobStore<S> {
    inner: Arc<S>,
    faults: Arc<Faults>,
}

impl<S> FaultBlobStore<S> {
    /// Wraps `inner`.
    pub fn new(inner: Arc<S>, faults: Arc<Faults>) -> Self {
        Self { inner, faults }
    }
}

impl<S> fmt::Debug for FaultBlobStore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultBlobStore").finish_non_exhaustive()
    }
}

impl<S: BlobStore> BlobStore for FaultBlobStore<S> {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        let report = self.faults.begin_write(Write::Blob(key))?;
        let result = self.inner.put_if_absent(repo, key, bytes).await;
        self.faults.lock().counts.blob_puts += 1;
        let outcome = result?;
        self.faults.finish_write(report)?;
        Ok(outcome)
    }

    async fn get(&self, repo: RepoId, key: ObjectKey) -> Result<Option<Bytes>, StoreError> {
        self.faults.check_alive()?;
        self.faults.lock().counts.blob_reads += 1;
        self.inner.get(repo, key).await
    }

    async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, StoreError> {
        self.faults.check_alive()?;
        self.faults.lock().counts.blob_reads += 1;
        self.inner.contains(repo, key).await
    }
}

#[cfg(test)]
mod tests {
    use scion_core::{BlobId, ChangesetId};

    use super::*;
    use crate::{MemoryBlobStore, MemoryMetaStore, RefLogReason};

    const REPO: RepoId = RepoId(1);

    /// The shared faults, the wrapped stores, and the raw stores beneath them.
    type Stores = (
        Arc<Faults>,
        FaultMetaStore<MemoryMetaStore>,
        FaultBlobStore<MemoryBlobStore>,
        Arc<MemoryMetaStore>,
        Arc<MemoryBlobStore>,
    );

    fn stores() -> Stores {
        let faults = Faults::new();
        let meta = Arc::new(MemoryMetaStore::new());
        let blobs = Arc::new(MemoryBlobStore::new());
        (
            faults.clone(),
            FaultMetaStore::new(meta.clone(), faults.clone()),
            FaultBlobStore::new(blobs.clone(), faults),
            meta,
            blobs,
        )
    }

    fn key(n: u8) -> MetaKey {
        MetaKey::Changeset(ChangesetId::from_bytes([n; 32]))
    }

    #[tokio::test]
    async fn blob_reads_are_forwarded_and_counted() {
        let (faults, _, blobs, _, raw_blobs) = stores();
        let there = ObjectKey::blob(BlobId::hash(b"there"));
        let absent = ObjectKey::blob(BlobId::hash(b"absent"));
        raw_blobs
            .put_if_absent(REPO, there, Bytes::from_static(b"f"))
            .await
            .unwrap();
        assert!(blobs.contains(REPO, there).await.unwrap());
        assert!(!blobs.contains(REPO, absent).await.unwrap());
        assert_eq!(
            blobs.get(REPO, there).await.unwrap(),
            Some(Bytes::from_static(b"f"))
        );
        assert_eq!(blobs.get(REPO, absent).await.unwrap(), None);
        assert_eq!(faults.counts().blob_reads, 4);
        faults.crash_at(0, CrashPoint::BeforeWrite);
        assert!(
            blobs
                .put_if_absent(REPO, absent, Bytes::new())
                .await
                .is_err()
        );
        assert!(
            blobs.contains(REPO, there).await.is_err(),
            "a dead process reads nothing"
        );
    }

    #[tokio::test]
    async fn crash_before_a_write_skips_it_and_kills_later_calls() {
        let (faults, meta, blobs, raw_meta, raw_blobs) = stores();
        faults.crash_at(1, CrashPoint::BeforeWrite);
        meta.put(REPO, &[(key(1), Bytes::from_static(b"a"))])
            .await
            .unwrap();
        let obj = ObjectKey::blob(BlobId::hash(b"x"));
        assert!(blobs.put_if_absent(REPO, obj, Bytes::new()).await.is_err());
        assert!(!raw_blobs.contains(REPO, obj).await.unwrap());
        assert!(faults.crashed());
        assert!(meta.get(REPO, &[key(1)]).await.is_err());
        assert!(
            meta.put(REPO, &[(key(2), Bytes::new())]).await.is_err(),
            "a dead process writes nothing"
        );
        assert_eq!(raw_meta.get(REPO, &[key(2)]).await.unwrap(), vec![None]);
        faults.restart();
        assert!(meta.get(REPO, &[key(1)]).await.unwrap()[0].is_some());
        assert_eq!(
            faults.writes(),
            vec![Write::Meta(vec![key(1)]), Write::Blob(obj)]
        );
    }

    #[tokio::test]
    async fn crash_after_a_write_applies_it_and_reports_failure() {
        let (faults, meta, _blobs, raw_meta, _) = stores();
        faults.crash_at(0, CrashPoint::AfterWrite);
        assert!(meta.put(REPO, &[(key(3), Bytes::new())]).await.is_err());
        assert!(raw_meta.get(REPO, &[key(3)]).await.unwrap()[0].is_some());
        assert!(faults.crashed());
    }

    fn update(new: u8, expected: Option<u8>) -> RefUpdate<'static> {
        RefUpdate {
            name: "main",
            expected: expected.map(|e| ChangesetId::from_bytes([e; 32])),
            new: ChangesetId::from_bytes([new; 32]),
            actor: "a",
            time_micros: 0,
            reason: RefLogReason::Manual,
        }
    }

    #[tokio::test]
    async fn a_ref_cas_is_a_write_step_that_can_crash_on_either_side() {
        let (faults, meta, _, raw_meta, _) = stores();
        faults.crash_at(0, CrashPoint::BeforeWrite);
        assert!(meta.update_ref(REPO, &update(1, None)).await.is_err());
        assert_eq!(raw_meta.get_ref(REPO, "main").await.unwrap(), None);
        faults.restart();
        faults.crash_at(1, CrashPoint::AfterWrite);
        assert!(meta.update_ref(REPO, &update(1, None)).await.is_err());
        assert!(faults.crashed());
        let applied = raw_meta.get_ref(REPO, "main").await.unwrap().unwrap();
        assert_eq!(applied.target, ChangesetId::from_bytes([1; 32]));
        assert_eq!(
            faults.writes(),
            vec![Write::Ref("main".into()), Write::Ref("main".into())]
        );
    }

    #[tokio::test]
    async fn a_transient_failure_skips_one_write_without_crashing() {
        let (faults, meta, blobs, raw_meta, _) = stores();
        faults.fail_transiently_at(1);
        meta.put(REPO, &[(key(1), Bytes::new())]).await.unwrap();
        let failed = meta.put(REPO, &[(key(2), Bytes::new())]).await;
        assert!(
            matches!(failed, Err(StoreError::Unavailable(_))),
            "{failed:?}"
        );
        assert!(!faults.crashed());
        assert_eq!(raw_meta.get(REPO, &[key(2)]).await.unwrap(), vec![None]);
        // The retry and every later call succeed.
        meta.put(REPO, &[(key(2), Bytes::new())]).await.unwrap();
        let obj = ObjectKey::blob(BlobId::hash(b"y"));
        blobs.put_if_absent(REPO, obj, Bytes::new()).await.unwrap();
        meta.update_ref(REPO, &update(1, None)).await.unwrap();
    }

    #[tokio::test]
    async fn denied_refs_fail_and_are_recorded() {
        let (faults, meta, _, _, _) = stores();
        meta.get_ref(REPO, "main").await.unwrap();
        faults.deny_refs();
        assert!(meta.get_ref(REPO, "main").await.is_err());
        assert!(meta.list_refs(REPO, "", None, 10).await.is_err());
        assert!(meta.ref_log(REPO, "main", 0, 10).await.is_err());
        let update = RefUpdate {
            name: "main",
            expected: None,
            new: ChangesetId::from_bytes([1; 32]),
            actor: "a",
            time_micros: 0,
            reason: RefLogReason::Manual,
        };
        assert!(meta.update_ref(REPO, &update).await.is_err());
        assert_eq!(faults.ref_violations().len(), 4);
        assert_eq!(faults.counts().ref_calls, 5);
    }

    #[tokio::test]
    async fn counts_calls_keys_and_bytes() {
        let (faults, meta, blobs, _, _) = stores();
        meta.put(REPO, &[(key(1), Bytes::from_static(b"abc"))])
            .await
            .unwrap();
        meta.get(REPO, &[key(1), key(2)]).await.unwrap();
        let obj = ObjectKey::blob(BlobId::hash(b"y"));
        blobs.put_if_absent(REPO, obj, Bytes::new()).await.unwrap();
        blobs.get(REPO, obj).await.unwrap();
        let c = faults.counts();
        assert_eq!(
            c,
            Counts {
                meta_gets: 1,
                meta_get_keys: 2,
                meta_get_bytes: 3,
                meta_puts: 1,
                meta_put_entries: 1,
                meta_put_bytes: 3,
                ref_calls: 0,
                blob_reads: 1,
                blob_puts: 1,
            }
        );
        faults.reset_counts();
        assert_eq!(faults.counts(), Counts::default());
        assert!(faults.writes().is_empty());
    }
}
