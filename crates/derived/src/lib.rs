//! The derived-data framework and every derived data type.
//!
//! Everything that is not a changeset is derived data, and all of it goes
//! through one mechanism, [`Deriver`]: a value is keyed by `(type, version,
//! param, changeset)`, computed by [`DerivedType::derive`] from the
//! changeset and its parents' values, written insert-if-absent, cached in
//! RAM, and derived at most once at a time per key on each server (INV-20).
//!
//! Eager types, derived inside create: [`RootTree`], [`CommitGraph`],
//! [`RestrictedRoots`], and the case-conflict index (`ops::case::CaseIndex`),
//! which lives in the ops crate with the name folding it uses.

mod blob_record;
mod coalesce;
mod codec;
mod commit_graph;
mod deriver;
mod error;
mod restricted_roots;
mod root_tree;
mod serial;
#[cfg(feature = "testing")]
pub mod testing;
mod trees;

pub use blob_record::BlobRecord;
pub use coalesce::Coalescer;
pub use commit_graph::{CommitGraph, GraphNode};
pub use deriver::{
    AnyType, CacheSizes, DERIVE_BATCH, DeriveCx, DerivedType, Deriver, FRAME_WRITES,
    FRAME_ZSTD_LEVEL, MAX_DERIVE_EXPLORE, MAX_DERIVE_PER_REQUEST, Mode, encode_frame, tree_frame,
};
pub use error::{DeriveError, InvalidKind};
pub use restricted_roots::{AclConfig, RestrictedRoots};
pub use root_tree::RootTree;
pub use trees::{AddedNames, Applied, Change, TreeCache, TreeWork};

/// The type codes in [`store::DerivedKey::kind`]. Codes are never reused.
pub mod kinds {
    /// [`crate::RootTree`].
    pub const ROOT_TREE: u16 = 1;
    /// [`crate::CommitGraph`].
    pub const COMMIT_GRAPH: u16 = 2;
    /// [`crate::RestrictedRoots`].
    pub const RESTRICTED_ROOTS: u16 = 3;
    /// The case-conflict index, defined in the ops crate, which owns the
    /// name folding it uses.
    pub const CASE_INDEX: u16 = 4;
}
