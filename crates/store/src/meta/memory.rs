//! The in-memory metadata store, for tests only.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};

use bytes::Bytes;

use super::{
    CasOutcome, MetaKey, MetaStore, RefLogEntry, RefState, RefUpdate, check_distinct, check_limit,
    check_name, check_prefix, check_update,
};
use crate::{PutOutcome, RepoId, StoreError};

/// A [`MetaStore`] held in process memory. For tests only.
#[derive(Debug, Default)]
pub struct MemoryMetaStore {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    records: HashMap<(RepoId, MetaKey), Bytes>,
    refs: BTreeMap<(RepoId, String), RefState>,
    logs: HashMap<(RepoId, String), Vec<RefLogEntry>>,
}

impl MemoryMetaStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, StoreError> {
        self.inner
            .lock()
            .map_err(|_| StoreError::Unavailable("in-memory store lock poisoned".into()))
    }
}

impl MetaStore for MemoryMetaStore {
    async fn put(
        &self,
        repo: RepoId,
        entries: &[(MetaKey, Bytes)],
    ) -> Result<Vec<PutOutcome>, StoreError> {
        check_distinct(entries.iter().map(|(k, _)| k))?;
        let mut inner = self.lock()?;
        Ok(entries
            .iter()
            .map(|(key, bytes)| match inner.records.entry((repo, *key)) {
                std::collections::hash_map::Entry::Occupied(_) => PutOutcome::Existed,
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(bytes.clone());
                    PutOutcome::Created
                }
            })
            .collect())
    }

    async fn get(&self, repo: RepoId, keys: &[MetaKey]) -> Result<Vec<Option<Bytes>>, StoreError> {
        let inner = self.lock()?;
        Ok(keys
            .iter()
            .map(|key| inner.records.get(&(repo, *key)).cloned())
            .collect())
    }

    async fn get_ref(&self, repo: RepoId, name: &str) -> Result<Option<RefState>, StoreError> {
        check_name("ref name", name)?;
        Ok(self.lock()?.refs.get(&(repo, name.to_owned())).cloned())
    }

    async fn list_refs(
        &self,
        repo: RepoId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RefState>, StoreError> {
        check_prefix("ref prefix", prefix)?;
        if let Some(after) = after {
            check_prefix("ref cursor", after)?;
        }
        check_limit(limit)?;
        let inner = self.lock()?;
        Ok(inner
            .refs
            .range((repo, prefix.to_owned())..)
            .take_while(|((r, name), _)| *r == repo && name.starts_with(prefix))
            .filter(|((_, name), _)| after.is_none_or(|a| name.as_str() > a))
            .take(limit)
            .map(|(_, state)| state.clone())
            .collect())
    }

    async fn update_ref(
        &self,
        repo: RepoId,
        update: &RefUpdate<'_>,
    ) -> Result<CasOutcome, StoreError> {
        check_update(update)?;
        let mut inner = self.lock()?;
        let key = (repo, update.name.to_owned());
        let current = inner.refs.get(&key).cloned();
        if current.as_ref().map(|c| c.target) != update.expected {
            return Ok(CasOutcome::Mismatch { current });
        }
        let seq = current.map_or(1, |c| c.seq + 1);
        inner.refs.insert(
            key.clone(),
            RefState {
                name: update.name.to_owned(),
                target: update.new,
                seq,
            },
        );
        inner.logs.entry(key).or_default().push(RefLogEntry {
            seq,
            old: update.expected,
            new: update.new,
            actor: update.actor.to_owned(),
            time_micros: update.time_micros,
            reason: update.reason,
        });
        Ok(CasOutcome::Updated { seq })
    }

    async fn ref_log(
        &self,
        repo: RepoId,
        name: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>, StoreError> {
        check_name("ref name", name)?;
        check_limit(limit)?;
        let inner = self.lock()?;
        Ok(inner
            .logs
            .get(&(repo, name.to_owned()))
            .map(|log| {
                log.iter()
                    .filter(|e| e.seq > after_seq)
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
}
