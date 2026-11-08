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
}

impl DeriveError {
    pub(crate) fn invalid(kind: InvalidKind, path: &[u8]) -> Self {
        Self::Invalid {
            kind,
            path: path.to_vec(),
        }
    }
}
