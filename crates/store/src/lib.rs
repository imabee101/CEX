//! Scion storage: the `MetaStore` and `BlobStore` traits, their backends, the shared backend conformance suite, fault-injection wrappers, and per-test isolation for tests that use the dev environment.
//!
//! - [`MetaStore`] holds the metadata database: changesets, trees, derived
//!   values, refs, and the ref log. Backends: [`PgMetaStore`] (Postgres, and
//!   CockroachDB through the Postgres wire protocol) and, for tests only,
//!   [`MemoryMetaStore`].
//! - [`BlobStore`] holds frames in blob storage. Backends: [`ObjectBlobStore`]
//!   over any `object_store` store (S3-compatible or the local filesystem)
//!   and, for tests only, [`MemoryBlobStore`].
//!
//! Every key includes a [`RepoId`], so deduplication never crosses repos.
//! Immutable records are written insert-if-absent and never overwritten; refs
//! change only by single-row compare-and-swap, which writes its ref-log row in
//! the same transaction.

mod blob;
pub mod conformance;
mod error;
pub mod fault;
mod meta;
pub mod testing;

pub use blob::{BlobStore, MemoryBlobStore, ObjectBlobStore, ObjectKey, ObjectKind};
pub use error::StoreError;
pub use meta::{
    CasOutcome, DerivedKey, MAX_NAME_BYTES, MemoryMetaStore, MetaKey, MetaStore, PgMetaStore,
    RefLogEntry, RefLogReason, RefState, RefUpdate,
};

/// A repository's id. Every stored key includes it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RepoId(pub u64);

/// Whether an insert-if-absent write stored new bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PutOutcome {
    /// The key was absent and now holds the given bytes.
    Created,
    /// The key already existed; its stored bytes are unchanged.
    Existed,
}
