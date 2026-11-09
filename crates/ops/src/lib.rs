//! Server-side operations: create, diff, merge, rebase, land, backout, and
//! graft. Every operation works on a [`MetaStore`](store::MetaStore) and a
//! [`BlobStore`](store::BlobStore) and fails with an [`OpError`] carrying a
//! `scion.v1` reason code.

pub mod blob;
pub mod case;
mod create;
mod diff;
mod error;

pub use create::{
    CreateOptions, MAX_AUTHOR_BYTES, MAX_COMPONENT_BYTES, MAX_MESSAGE_BYTES, MAX_PARENTS,
    MAX_PATH_BYTES, MAX_SIGNATURE_BYTES, Ops,
};
pub use diff::{
    DiffCursor, DiffMode, DiffOptions, DiffPage, FileDiff, FileState, MAX_DIFF_PAGE, PATCH_CONTEXT,
};
pub use error::{Code, OpError};
