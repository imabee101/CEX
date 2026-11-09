//! The metadata database: changesets, trees, derived values, refs, and the
//! ref log.

mod memory;
mod postgres;

use std::future::Future;

use bytes::Bytes;
use scion_core::{BlobId, ChangesetId, TreeId};

use crate::{PutOutcome, RepoId, StoreError};

pub use memory::MemoryMetaStore;
pub use postgres::PgMetaStore;

/// The longest ref name, ref-name prefix, or actor a store accepts, in bytes.
pub const MAX_NAME_BYTES: usize = 1024;

/// The most rows one `list_refs` or `ref_log` call returns.
pub(crate) const MAX_PAGE: usize = 1000;

/// The key of a derived value: `(type, version, param, changeset)`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct DerivedKey {
    /// The derived-data type's code.
    pub kind: u16,
    /// The type's format version; each version is its own key space.
    pub version: u16,
    /// An extra key component for types keyed by more than the changeset,
    /// such as an ACL config version; 0 for every other type. Each value is
    /// its own key space.
    pub param: u64,
    /// The changeset the value belongs to.
    pub changeset: ChangesetId,
}

/// The key of an immutable metadata record.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum MetaKey {
    /// A blob's record: its size, SHA-256, and, when imported, git SHA-1.
    /// Written only after the blob's frames are in blob storage.
    Blob(BlobId),
    /// A changeset's canonical bytes.
    Changeset(ChangesetId),
    /// A tree node's canonical bytes.
    Tree(TreeId),
    /// A derived value.
    Derived(DerivedKey),
}

/// A ref's current state.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RefState {
    /// The ref's name.
    pub name: String,
    /// The changeset the ref points to.
    pub target: ChangesetId,
    /// The `seq` of the ref-log row written by the CAS that set `target`.
    pub seq: u64,
}

/// Why a ref moved, recorded in its ref-log row.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RefLogReason {
    /// A land.
    Land,
    /// A backout.
    Backout,
    /// A direct `UpdateRef`.
    Manual,
}

impl RefLogReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Land => "land",
            Self::Backout => "backout",
            Self::Manual => "manual",
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, StoreError> {
        match s {
            "land" => Ok(Self::Land),
            "backout" => Ok(Self::Backout),
            "manual" => Ok(Self::Manual),
            _ => Err(StoreError::DataLoss(format!(
                "unknown ref-log reason {s:?}"
            ))),
        }
    }
}

/// One ref-log row: a successful CAS of one ref.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RefLogEntry {
    /// Gap-free per ref, starting at 1.
    pub seq: u64,
    /// The target before the CAS; `None` when the CAS created the ref.
    pub old: Option<ChangesetId>,
    /// The target after the CAS.
    pub new: ChangesetId,
    /// The principal that moved the ref.
    pub actor: String,
    /// When the ref moved, in microseconds since the Unix epoch.
    pub time_micros: i64,
    /// Why the ref moved.
    pub reason: RefLogReason,
}

/// A compare-and-swap of one ref.
#[derive(Clone, Copy, Debug)]
pub struct RefUpdate<'a> {
    /// The ref to update.
    pub name: &'a str,
    /// The target the ref must have now; `None` means the ref must not exist.
    pub expected: Option<ChangesetId>,
    /// The new target.
    pub new: ChangesetId,
    /// The principal moving the ref.
    pub actor: &'a str,
    /// The time recorded in the ref log, in microseconds since the Unix epoch.
    pub time_micros: i64,
    /// The reason recorded in the ref log.
    pub reason: RefLogReason,
}

/// The result of a ref CAS.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CasOutcome {
    /// The ref now points to the new target; `seq` is its new ref-log row.
    Updated {
        /// The `seq` of the ref-log row this CAS wrote.
        seq: u64,
    },
    /// The ref did not have the expected target; nothing was written.
    Mismatch {
        /// The ref's state when the CAS was evaluated.
        current: Option<RefState>,
    },
}

/// The metadata database.
///
/// Immutable records ([`MetaKey`]) are written insert-if-absent: a put of an
/// existing key leaves its stored bytes unchanged. Refs change only through
/// [`MetaStore::update_ref`], a single-row CAS that writes exactly one
/// ref-log row in the same transaction. Ref reads are strong.
pub trait MetaStore: Send + Sync + 'static {
    /// Inserts every absent key, atomically: either every entry is applied or
    /// none is. Returns one outcome per entry, in order. A batch that names a
    /// key twice is rejected.
    fn put(
        &self,
        repo: RepoId,
        entries: &[(MetaKey, Bytes)],
    ) -> impl Future<Output = Result<Vec<PutOutcome>, StoreError>> + Send;

    /// Reads records; one result per key, in order.
    fn get(
        &self,
        repo: RepoId,
        keys: &[MetaKey],
    ) -> impl Future<Output = Result<Vec<Option<Bytes>>, StoreError>> + Send;

    /// Reads one ref.
    fn get_ref(
        &self,
        repo: RepoId,
        name: &str,
    ) -> impl Future<Output = Result<Option<RefState>, StoreError>> + Send;

    /// Lists refs whose names start with `prefix` and sort after `after`, in
    /// name byte order, at most `limit` (1 to 1,000) of them.
    fn list_refs(
        &self,
        repo: RepoId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<RefState>, StoreError>> + Send;

    /// Compare-and-swap of one ref, writing its ref-log row in the same
    /// transaction.
    fn update_ref(
        &self,
        repo: RepoId,
        update: &RefUpdate<'_>,
    ) -> impl Future<Output = Result<CasOutcome, StoreError>> + Send;

    /// Reads a ref's log rows with `seq > after_seq`, in `seq` order, at most
    /// `limit` (1 to 1,000) of them.
    fn ref_log(
        &self,
        repo: RepoId,
        name: &str,
        after_seq: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<RefLogEntry>, StoreError>> + Send;
}

/// Checks a ref name or actor: 1 to [`MAX_NAME_BYTES`] bytes, no NUL.
pub(crate) fn check_name(what: &str, name: &str) -> Result<(), StoreError> {
    if name.is_empty() {
        return Err(StoreError::invalid(format!("{what} is empty")));
    }
    check_prefix(what, name)
}

/// Checks a ref-name prefix: at most [`MAX_NAME_BYTES`] bytes, no NUL.
pub(crate) fn check_prefix(what: &str, name: &str) -> Result<(), StoreError> {
    if name.len() > MAX_NAME_BYTES {
        return Err(StoreError::invalid(format!(
            "{what} is longer than {MAX_NAME_BYTES} bytes"
        )));
    }
    if name.contains('\0') {
        return Err(StoreError::invalid(format!("{what} contains NUL")));
    }
    Ok(())
}

pub(crate) fn check_limit(limit: usize) -> Result<(), StoreError> {
    if limit == 0 || limit > MAX_PAGE {
        return Err(StoreError::invalid(format!(
            "limit {limit} is outside 1..={MAX_PAGE}"
        )));
    }
    Ok(())
}

pub(crate) fn check_update(update: &RefUpdate<'_>) -> Result<(), StoreError> {
    check_name("ref name", update.name)?;
    check_name("actor", update.actor)
}

/// Rejects a batch that names a key twice.
pub(crate) fn check_distinct<'a>(
    keys: impl Iterator<Item = &'a MetaKey>,
) -> Result<(), StoreError> {
    let mut seen = std::collections::HashSet::new();
    for key in keys {
        if !seen.insert(key) {
            return Err(StoreError::invalid(format!("batch names {key:?} twice")));
        }
    }
    Ok(())
}
