//! Scion storage: the `MetaStore` and `BlobStore` traits and their backends.
//!
//! With the `testing` feature, which only dev-dependencies enable, it also
//! provides the in-memory backends, the shared backend conformance suite,
//! fault-injection wrappers, and per-test isolation for tests that use the
//! dev environment. None of these is compiled into production binaries.
//!
//! - [`MetaStore`] holds the metadata database: changesets, trees, derived
//!   values, refs, and the ref log. Backends: [`PgMetaStore`] (Postgres, and
//!   CockroachDB through the Postgres wire protocol) and, for tests only with
//!   the `testing` feature, `MemoryMetaStore`.
//! - [`BlobStore`] holds frames in blob storage. Backends: [`ObjectBlobStore`]
//!   over any `object_store` store (S3-compatible or the local filesystem)
//!   and, for tests only with the `testing` feature, `MemoryBlobStore`.
//!
//! Every key includes a [`RepoId`], so deduplication never crosses repos.
//! Immutable records are written insert-if-absent and never overwritten; refs
//! change only by single-row compare-and-swap, which writes its ref-log row in
//! the same transaction.

mod blob;
mod coalesce;
#[cfg(feature = "testing")]
pub mod conformance;
mod error;
#[cfg(feature = "testing")]
pub mod fault;
mod meta;
#[cfg(feature = "testing")]
pub mod testing;

#[cfg(feature = "testing")]
pub use blob::MemoryBlobStore;
pub use blob::{BlobStore, ObjectBlobStore, ObjectKey, ObjectKind};
pub use coalesce::Coalescer;
pub use error::StoreError;
#[cfg(feature = "testing")]
pub use meta::MemoryMetaStore;
pub use meta::{
    CasOutcome, DerivedKey, MAX_NAME_BYTES, MetaKey, MetaStore, PgMetaStore, PgTls, PoolLimits,
    RefLogEntry, RefLogReason, RefState, RefUpdate, SCHEMA_VERSION,
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
