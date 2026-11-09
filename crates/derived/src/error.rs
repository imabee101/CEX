//! Errors from deriving values.

use store::StoreError;

/// Why a changeset's file changes cannot be applied to its first parent's
/// tree.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InvalidKind {
    /// The change leaves the path as it was: a delete of an absent file, or
    /// a write of the kind and blob the path already has. (INV-11)
    NoOp,
    /// The change puts a file and a directory at one path: a file beneath a
    /// file that stays, a file over a directory, a delete of a directory, or
    /// two changed paths where one is a prefix of the other and the shorter
    /// one is not a deleted file. (INV-12)
    PathConflict,
    /// A change's size differs from its blob's recorded size.
    SizeMismatch,
}

/// An error from deriving a value.
#[derive(Debug, thiserror::Error)]
pub enum DeriveError {
    /// The metadata or blob store failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The changeset's file changes are invalid against its first parent.
    #[error("{kind:?} at {}", String::from_utf8_lossy(path))]
    Invalid {
        /// What is wrong.
        kind: InvalidKind,
        /// The offending path.
        path: Vec<u8>,
    },
    /// A changeset or blob the derivation needs does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// Stored data is missing or malformed.
    #[error("data loss: {0}")]
    DataLoss(String),
    /// The changeset adds a name that conflicts with another name of its
    /// directory after normalization and case folding (INV-19).
    #[error(
        "{} conflicts with {} in the same directory",
        String::from_utf8_lossy(path),
        String::from_utf8_lossy(other)
    )]
    CaseConflict {
        /// The added path.
        path: Vec<u8>,
        /// The name in the same directory it conflicts with.
        other: Vec<u8>,
    },
    /// A value cannot be derived within one request's bound: too many
    /// ancestors lack it. The background deriver will derive them; a retry
    /// may succeed.
    #[error("derivation pending: {0}")]
    Pending(String),
    /// A defect: an internal invariant does not hold, or a task failed.
    #[error("internal error: {0}")]
    Internal(String),
}

impl DeriveError {
    pub(crate) fn invalid(kind: InvalidKind, path: &[u8]) -> Self {
        Self::Invalid {
            kind,
            path: path.to_vec(),
        }
    }
}
