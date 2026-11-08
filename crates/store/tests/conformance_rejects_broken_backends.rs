//! The conformance suite is not hollow: each check passes on a correct
//! reference backend and panics on a backend with the one defect it exists to
//! catch.
//!
//! The reference backends here are deliberately simple in-memory models with
//! switchable defects; they exist only to test the suite.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use store::conformance::{blob, meta};
use store::{
    BlobStore, CasOutcome, MAX_NAME_BYTES, MetaKey, MetaStore, ObjectKey, PutOutcome, RefLogEntry,
    RefState, RefUpdate, RepoId, StoreError,
};

/// Values longer than this are silently truncated by `Bug::Truncate`: one
/// chunk, so a "large" check must use values larger than a chunk.
const TRUNCATE_AT: usize = 4 << 20;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bug {
    None,
    /// A put of an existing key replaces its bytes.
    Overwrite,
    /// Keys of different kinds with the same id bytes share one record.
    MergeKinds,
    /// Every repo shares one key space.
    IgnoreRepo,
    /// A batch naming a key twice is accepted.
    AcceptDuplicates,
    /// A put checks for the key, waits, then writes: racing puts all create.
    RacyPut,
    /// Empty values are not stored.
    DropEmpty,
    /// Values are truncated to `TRUNCATE_AT` bytes.
    Truncate,
    /// A successful CAS writes no ref-log row.
    NoLog,
    /// `list_refs` ignores its cursor.
    ListIgnoresCursor,
    /// `ref_log` ignores `after_seq`.
    LogIgnoresAfter,
    /// Names, actors, and limits are not validated.
    NoValidation,
    /// A CAS compares, waits, then writes: racing CASes all succeed.
    RacyCas,
    /// `contains` always says yes.
    ContainsAlways,
    /// Rejections of bad input are reported as retryable `Unavailable`.
    WrongErrorKind,
}

#[derive(Default)]
struct MetaState {
    records: HashMap<(RepoId, MetaKey), Bytes>,
    refs: BTreeMap<(RepoId, String), RefState>,
    logs: HashMap<(RepoId, String), Vec<RefLogEntry>>,
}

struct BuggyMeta {
    bug: Bug,
    state: Mutex<MetaState>,
}

impl BuggyMeta {
    fn new(bug: Bug) -> Arc<Self> {
        Arc::new(Self {
            bug,
            state: Mutex::new(MetaState::default()),
        })
    }

    /// A rejection: `InvalidArgument`, or the wrong kind under
    /// `WrongErrorKind`.
    fn invalid(&self, msg: &str) -> StoreError {
        if self.bug == Bug::WrongErrorKind {
            StoreError::Unavailable(msg.to_owned())
        } else {
            StoreError::InvalidArgument(msg.to_owned())
        }
    }

    fn repo(&self, repo: RepoId) -> RepoId {
        if self.bug == Bug::IgnoreRepo {
            RepoId(0)
        } else {
            repo
        }
    }

    fn key(&self, key: MetaKey) -> MetaKey {
        if self.bug != Bug::MergeKinds {
            return key;
        }
        match key {
            MetaKey::Changeset(id) => MetaKey::Changeset(id),
            MetaKey::Tree(id) | MetaKey::CaseNode(id) => {
                MetaKey::Changeset(scion_core::ChangesetId::from_bytes(*id.as_bytes()))
            }
            MetaKey::Derived(d) => MetaKey::Changeset(d.changeset),
            MetaKey::Blob(id) => {
                MetaKey::Changeset(scion_core::ChangesetId::from_bytes(*id.as_bytes()))
            }
        }
    }

    fn value(&self, bytes: &Bytes) -> Option<Bytes> {
        match self.bug {
            Bug::DropEmpty if bytes.is_empty() => None,
            Bug::Truncate if bytes.len() > TRUNCATE_AT => Some(bytes.slice(..TRUNCATE_AT)),
            _ => Some(bytes.clone()),
        }
    }

    fn check_name(&self, name: &str) -> Result<(), StoreError> {
        if self.bug == Bug::NoValidation {
            return Ok(());
        }
        if name.is_empty() {
            return Err(self.invalid("empty"));
        }
        self.check_prefix(name)
    }

    fn check_prefix(&self, name: &str) -> Result<(), StoreError> {
        if self.bug != Bug::NoValidation && (name.len() > MAX_NAME_BYTES || name.contains('\0')) {
            return Err(self.invalid("bad name"));
        }
        Ok(())
    }

    fn check_limit(&self, limit: usize) -> Result<(), StoreError> {
        if self.bug != Bug::NoValidation && !(1..=1000).contains(&limit) {
            return Err(self.invalid("bad limit"));
        }
        Ok(())
    }
}

impl MetaStore for BuggyMeta {
    async fn put(
        &self,
        repo: RepoId,
        entries: &[(MetaKey, Bytes)],
    ) -> Result<Vec<PutOutcome>, StoreError> {
        if self.bug != Bug::AcceptDuplicates {
            let mut seen = HashSet::new();
            if !entries.iter().all(|(k, _)| seen.insert(*k)) {
                return Err(self.invalid("duplicate key"));
            }
        }
        let repo = self.repo(repo);
        if self.bug == Bug::RacyPut {
            let absent: Vec<bool> = {
                let s = self.state.lock().unwrap();
                entries
                    .iter()
                    .map(|(k, _)| !s.records.contains_key(&(repo, *k)))
                    .collect()
            };
            tokio::time::sleep(Duration::from_millis(5)).await;
            let mut s = self.state.lock().unwrap();
            for (k, v) in entries {
                s.records.insert((repo, *k), v.clone());
            }
            return Ok(absent
                .into_iter()
                .map(|a| {
                    if a {
                        PutOutcome::Created
                    } else {
                        PutOutcome::Existed
                    }
                })
                .collect());
        }
        let mut s = self.state.lock().unwrap();
        Ok(entries
            .iter()
            .map(|(k, v)| {
                let key = (repo, self.key(*k));
                let existed = s.records.contains_key(&key);
                if (!existed || self.bug == Bug::Overwrite)
                    && let Some(v) = self.value(v)
                {
                    s.records.insert(key, v);
                }
                if existed {
                    PutOutcome::Existed
                } else {
                    PutOutcome::Created
                }
            })
            .collect())
    }

    async fn get(&self, repo: RepoId, keys: &[MetaKey]) -> Result<Vec<Option<Bytes>>, StoreError> {
        let repo = self.repo(repo);
        let s = self.state.lock().unwrap();
        Ok(keys
            .iter()
            .map(|k| s.records.get(&(repo, self.key(*k))).cloned())
            .collect())
    }

    async fn get_ref(&self, repo: RepoId, name: &str) -> Result<Option<RefState>, StoreError> {
        self.check_name(name)?;
        let repo = self.repo(repo);
        Ok(self
            .state
            .lock()
            .unwrap()
            .refs
            .get(&(repo, name.to_owned()))
            .cloned())
    }

    async fn list_refs(
        &self,
        repo: RepoId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RefState>, StoreError> {
        self.check_prefix(prefix)?;
        if let Some(a) = after {
            self.check_prefix(a)?;
        }
        self.check_limit(limit)?;
        let repo = self.repo(repo);
        let after = if self.bug == Bug::ListIgnoresCursor {
            None
        } else {
            after
        };
        let s = self.state.lock().unwrap();
        Ok(s.refs
            .iter()
            .filter(|((r, n), _)| *r == repo && n.starts_with(prefix))
            .filter(|((_, n), _)| after.is_none_or(|a| n.as_str() > a))
            .take(limit)
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn update_ref(
        &self,
        repo: RepoId,
        update: &RefUpdate<'_>,
    ) -> Result<CasOutcome, StoreError> {
        self.check_name(update.name)?;
        self.check_name(update.actor)?;
        let repo = self.repo(repo);
        let key = (repo, update.name.to_owned());
        let current = self.state.lock().unwrap().refs.get(&key).cloned();
        if current.as_ref().map(|c| c.target) != update.expected {
            return Ok(CasOutcome::Mismatch { current });
        }
        if self.bug == Bug::RacyCas {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let mut s = self.state.lock().unwrap();
        // Correct backends compare and write atomically; `RacyCas` writes
        // based on the state it read before waiting.
        let base = if self.bug == Bug::RacyCas {
            current
        } else {
            let now = s.refs.get(&key).cloned();
            if now.as_ref().map(|c| c.target) != update.expected {
                return Ok(CasOutcome::Mismatch { current: now });
            }
            now
        };
        let seq = base.map_or(1, |c| c.seq + 1);
        s.refs.insert(
            key.clone(),
            RefState {
                name: update.name.to_owned(),
                target: update.new,
                seq,
            },
        );
        if self.bug != Bug::NoLog {
            s.logs.entry(key).or_default().push(RefLogEntry {
                seq,
                old: update.expected,
                new: update.new,
                actor: update.actor.to_owned(),
                time_micros: update.time_micros,
                reason: update.reason,
            });
        }
        Ok(CasOutcome::Updated { seq })
    }

    async fn ref_log(
        &self,
        repo: RepoId,
        name: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>, StoreError> {
        self.check_name(name)?;
        self.check_limit(limit)?;
        let repo = self.repo(repo);
        let after = if self.bug == Bug::LogIgnoresAfter {
            0
        } else {
            after_seq
        };
        let s = self.state.lock().unwrap();
        Ok(s.logs
            .get(&(repo, name.to_owned()))
            .map(|l| {
                l.iter()
                    .filter(|e| e.seq > after)
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
}

struct BuggyBlob {
    bug: Bug,
    objects: Mutex<HashMap<(RepoId, ObjectKey), Bytes>>,
}

impl BuggyBlob {
    fn new(bug: Bug) -> Arc<Self> {
        Arc::new(Self {
            bug,
            objects: Mutex::new(HashMap::new()),
        })
    }

    fn key(&self, repo: RepoId, key: ObjectKey) -> (RepoId, ObjectKey) {
        let repo = if self.bug == Bug::IgnoreRepo {
            RepoId(0)
        } else {
            repo
        };
        let key = if self.bug == Bug::MergeKinds {
            ObjectKey::blob(scion_core::BlobId::from_bytes(*key.id()))
        } else {
            key
        };
        (repo, key)
    }
}

impl BlobStore for BuggyBlob {
    async fn put_if_absent(
        &self,
        repo: RepoId,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<PutOutcome, StoreError> {
        let k = self.key(repo, key);
        let bytes = if self.bug == Bug::Truncate && bytes.len() > TRUNCATE_AT {
            bytes.slice(..TRUNCATE_AT)
        } else {
            bytes
        };
        if self.bug == Bug::RacyPut {
            let existed = self.objects.lock().unwrap().contains_key(&k);
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.objects.lock().unwrap().insert(k, bytes);
            return Ok(if existed {
                PutOutcome::Existed
            } else {
                PutOutcome::Created
            });
        }
        let mut o = self.objects.lock().unwrap();
        let existed = o.contains_key(&k);
        if !existed || self.bug == Bug::Overwrite {
            o.insert(k, bytes);
        }
        Ok(if existed {
            PutOutcome::Existed
        } else {
            PutOutcome::Created
        })
    }

    async fn get(&self, repo: RepoId, key: ObjectKey) -> Result<Option<Bytes>, StoreError> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(&self.key(repo, key))
            .cloned())
    }

    async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, StoreError> {
        Ok(self.bug == Bug::ContainsAlways
            || self
                .objects
                .lock()
                .unwrap()
                .contains_key(&self.key(repo, key)))
    }
}

/// Runs a check to completion on its own task; true if it panicked.
async fn panics<F: Future<Output = ()> + Send + 'static>(check: F) -> bool {
    match tokio::spawn(check).await {
        Ok(()) => false,
        Err(e) => e.is_panic(),
    }
}

macro_rules! meta_cases {
    ($($check:ident: $bug:ident,)*) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn meta_checks_pass_on_the_correct_reference() {
            $(
                assert!(
                    !panics(meta::$check(BuggyMeta::new(Bug::None))).await,
                    "{} failed on a correct backend",
                    stringify!($check)
                );
            )*
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn meta_checks_reject_their_defect() {
            $(
                assert!(
                    panics(meta::$check(BuggyMeta::new(Bug::$bug))).await,
                    "{} accepted a backend with {:?}",
                    stringify!($check),
                    Bug::$bug
                );
            )*
        }
    };
}

meta_cases! {
    records_round_trip: DropEmpty,
    put_never_overwrites: Overwrite,
    kinds_are_separate_namespaces: MergeKinds,
    repos_are_isolated: IgnoreRepo,
    batch_naming_a_key_twice_is_rejected: AcceptDuplicates,
    concurrent_puts_of_one_key_create_it_once: RacyPut,
    concurrent_overlapping_batches_all_succeed: RacyPut,
    large_records_round_trip: Truncate,
    ref_cas_creates_moves_and_rejects: NoLog,
    list_refs_in_byte_order_with_pagination: ListIgnoresCursor,
    ref_log_pages_by_seq: LogIgnoresAfter,
    invalid_names_and_limits_are_rejected: NoValidation,
    invalid_names_and_limits_are_rejected: WrongErrorKind,
    batch_naming_a_key_twice_is_rejected: WrongErrorKind,
    concurrent_ref_creation_has_one_winner: RacyCas,
    ref_cas_is_linearizable_at_64_way_concurrency: RacyCas,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn linearizability_check_also_rejects_a_missing_ref_log() {
    assert!(
        panics(meta::ref_cas_is_linearizable_at_64_way_concurrency(
            BuggyMeta::new(Bug::NoLog)
        ))
        .await
    );
}

macro_rules! blob_cases {
    ($($check:ident: $bug:ident,)*) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn blob_checks_pass_on_the_correct_reference() {
            $(
                assert!(
                    !panics(blob::$check(BuggyBlob::new(Bug::None))).await,
                    "{} failed on a correct backend",
                    stringify!($check)
                );
            )*
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn blob_checks_reject_their_defect() {
            $(
                assert!(
                    panics(blob::$check(BuggyBlob::new(Bug::$bug))).await,
                    "{} accepted a backend with {:?}",
                    stringify!($check),
                    Bug::$bug
                );
            )*
        }
    };
}

blob_cases! {
    objects_round_trip: ContainsAlways,
    put_never_overwrites: Overwrite,
    kinds_are_separate_namespaces: MergeKinds,
    repos_are_isolated: IgnoreRepo,
    concurrent_puts_of_one_key_create_it_once: RacyPut,
    large_objects_round_trip: Truncate,
}
