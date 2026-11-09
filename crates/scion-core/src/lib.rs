//! Scion's core object model: typed ids, the canonical binary encoding of
//! changesets and trees, and the frame format. It performs no I/O and no
//! async work.
//!
//! This documentation is the normative specification of the formats. The
//! file `vectors/v1.txt` in this crate holds language-neutral test vectors
//! for them; see [Test vectors](#test-vectors).
//!
//! ```
//! use scion_core::{BlobId, Changeset, ChangesetParts, FileChange, FileKind, RepoPath};
//!
//! let mut parts = ChangesetParts::default();
//! parts.author = b"Ada <ada@example.com>".to_vec();
//! parts.message = b"Add greeting\n".to_vec();
//! let blob = BlobId::hash(b"hello\n");
//! parts.file_changes.insert(
//!     RepoPath::new("docs/hello.txt")?,
//!     FileChange::File { kind: FileKind::Regular, blob, size: 6 },
//! );
//! let cs = Changeset::new(parts)?;
//! let bytes = cs.encode();
//! assert_eq!(Changeset::decode(&bytes)?, cs);
//!
//! // Anything but the canonical encoding is rejected.
//! let mut longer = bytes.clone();
//! longer.push(0);
//! assert!(Changeset::decode(&longer).is_err());
//! # Ok::<(), scion_core::FormatError>(())
//! ```
//!
//! # Ids
//!
//! An id is 32 bytes: BLAKE3 in `derive_key` mode, with a context string per
//! kind, over the object's canonical bytes.
//!
//! | Type | Context | Hashed bytes |
//! |---|---|---|
//! | [`BlobId`] | `scion v1 blob` | the file's full raw, uncompressed bytes, even when stored as chunks |
//! | [`ChunkId`] | `scion v1 chunk` | the chunk's raw bytes |
//! | [`TreeId`] | `scion v1 tree` | the tree node's canonical encoding |
//! | [`ChangesetId`] | `scion v1 changeset` | the changeset's canonical encoding |
//!
//! Objects are immutable, so an id names the same bytes forever. Ids are
//! written in text as 64 lowercase hex digits.
//!
//! # Primitives
//!
//! - `u8`: one byte.
//! - `uvarint`: unsigned LEB128 of a 64-bit value, in its minimal form: at
//!   most 10 bytes, the last byte non-zero unless it is the only byte, and a
//!   tenth byte of at most `0x01`.
//! - `svarint`: a signed 64-bit value `n`, zigzag-mapped to
//!   `(n << 1) ^ (n >> 63)`, then written as a `uvarint`.
//! - `bytes`: a `uvarint` length, then that many raw bytes. A length over
//!   [`MAX_FIELD_BYTES`] (64 MiB) is rejected before anything is allocated.
//! - `opt<T>`: `0x00` for absent, or `0x01` followed by `T`. Any other tag
//!   is rejected.
//! - `timestamp`: `svarint` Unix seconds, then `svarint` UTC offset in
//!   seconds east, with `|offset| < 86,400`.
//! - `id`: 32 raw bytes.
//!
//! Every encoding is canonical: each value has exactly one encoding, and
//! decoders reject every other byte string, including non-minimal varints,
//! unsorted or duplicate keys, unknown versions, kinds, or tags, and
//! trailing bytes.
//!
//! # Paths and names
//!
//! A path is a byte string of `/`-separated components. No component is
//! empty, `.`, or `..`, and no byte is NUL; so a path never starts or ends
//! with `/`. Paths and names order by their bytes.
//!
//! A tree entry name is non-empty and contains no `/` or NUL. Except for
//! shard entries, it is not `.` or `..`.
//!
//! # Changeset encoding
//!
//! In order:
//!
//! 1. `u8` version `0x01`.
//! 2. `uvarint` parent count, then each parent's 32-byte [`ChangesetId`],
//!    first parent first.
//! 3. `author` as `bytes` (`Name <email>`, verbatim).
//! 4. `author_date` as `timestamp`.
//! 5. `committer` as `opt<bytes>`.
//! 6. `committer_date` as `opt<timestamp>`, present exactly when
//!    `committer` is.
//! 7. `message` as `bytes`.
//! 8. `signature` as `opt<bytes>`.
//! 9. `git_headers` as `opt<bytes>`.
//! 10. `uvarint` change count, then each change in strictly increasing path
//!     byte order: the path as `bytes`, a `u8` kind (0 Deleted, 1 Regular,
//!     2 Executable, 3 Symlink), and, unless Deleted, the 32-byte [`BlobId`]
//!     and a `uvarint` size.
//!
//! Changes are relative to the first parent only. A changeset has no tree
//! pointer.
//!
//! # Tree node encoding
//!
//! 1. `u8` version `0x01`.
//! 2. `uvarint` entry count, at most [`MAX_TREE_ENTRIES`] (4,096).
//! 3. Each entry in strictly increasing name byte order: the name as
//!    `bytes`; a `u8` kind (0 Tree, 1 Shard, 2 Regular, 3 Executable,
//!    4 Symlink); the 32-byte id (a [`TreeId`] for Tree and Shard, a
//!    [`BlobId`] otherwise); and, for Regular, Executable, and Symlink
//!    only, a `uvarint` size and the content's 32-byte SHA-256.
//!
//! A directory with more than [`MAX_TREE_ENTRIES`] names is split: with `P`
//! the longest common prefix of its names, a name equal to `P` stays an
//! entry, and the rest are grouped by their first byte after `P` into one
//! Shard entry (named `P` plus that byte) per group; each shard node holds
//! full names and is split by the same rule. Only the root tree may be empty.
//!
//! A node with any Shard entry is therefore rejected unless every Shard name
//! is the same prefix `P` plus one byte, the only other entry (if any) is
//! named exactly `P`, and, when there is no entry named `P`, there are at
//! least two Shard entries.
//!
//! # Frames
//!
//! A frame carries one object everywhere objects are stored or sent: header,
//! payload, then the 32-byte BLAKE3 hash (plain `hash` mode) of every
//! preceding byte. The header is, in order:
//!
//! 1. The magic `SCF1` ([`FRAME_MAGIC`]).
//! 2. `u8` kind: 1 blob, 2 chunk, 3 tree, 4 chunk list.
//! 3. The object's 32-byte id: a [`BlobId`] for kinds 1 and 4, a
//!    [`ChunkId`] for kind 2, a [`TreeId`] for kind 3.
//! 4. `u8` codec: 0 raw, 1 zstd.
//! 5. `uvarint` raw length: the length of the object's raw bytes.
//! 6. `uvarint` payload length.
//!
//! Frame rules:
//!
//! - A blob of at most [`CHUNK_BYTES`] (4 MiB) has a kind-1 frame. A larger
//!   blob is stored as consecutive `CHUNK_BYTES` chunks (the last may be
//!   shorter), each in its own kind-2 frame, and the blob's own frame is a
//!   kind-4 chunk-list frame: codec raw, raw length equal to the blob's
//!   length, and a payload of a `uvarint` count then the [`ChunkId`]s in
//!   order. The count is the blob length divided by `CHUNK_BYTES`, rounded
//!   up.
//! - A chunk holds 1 to `CHUNK_BYTES` bytes. A tree frame's raw length is at
//!   most [`MAX_FIELD_BYTES`].
//! - With codec raw (kinds 1–3) the payload is the raw bytes, so the payload
//!   length equals the raw length. With codec zstd the payload is one zstd
//!   compression of the raw bytes and is strictly shorter than them.
//! - The frame ends exactly after the digest.
//!
//! Decoding a frame checks its header rules, length, and digest. Checking
//! that the raw bytes hash to the id requires decompressing them, and is
//! done by whoever materializes or accepts the object.
//!
//! # Test vectors
//!
//! `vectors/v1.txt` is a UTF-8 text file of records separated by blank
//! lines. Lines starting with `#` are comments. Each record has these
//! `key: value` lines, in this order:
//!
//! - `name`: a unique name.
//! - `type`: `blob`, `chunk`, `tree`, `changeset`, `frame`, or `chunk_list`
//!   (the payload of a chunk-list frame on its own).
//! - `expect`: `accept` or `reject`.
//! - `id`: for an accepted `blob`, `chunk`, `tree`, or `changeset`, the
//!   expected id of `bytes` as 64 hex digits; for an accepted `frame`, the
//!   id in its header; absent for `chunk_list` and for `reject`.
//! - `bytes`: the input as lowercase hex (possibly empty).
//!
//! A conforming decoder accepts exactly the `accept` records, re-encodes
//! each decoded value to exactly `bytes`, and computes exactly `id`.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod changeset;
mod error;
mod frame;
mod id;
mod path;
mod tree;
pub mod wire;

pub use changeset::{
    Changeset, ChangesetParts, Committer, FileChange, FileKind, Timestamp, UTC_OFFSET_LIMIT_SECONDS,
};
pub use error::FormatError;
pub use frame::{
    ChunkList, Codec, FRAME_DIGEST_LEN, FRAME_MAGIC, Frame, FrameHeader, FrameId, FrameKind,
    FrameLengthRule, MAX_FRAME_HEADER_LEN, chunk_count,
};
pub use id::{
    BLOB_CONTEXT, BlobId, CHANGESET_CONTEXT, CHUNK_CONTEXT, ChangesetId, ChunkId, IdHasher,
    TREE_CONTEXT, TreeId,
};
pub use path::RepoPath;
pub use tree::{EntryTarget, Tree, TreeEntry};

/// The version byte that starts every changeset encoding.
pub const CHANGESET_FORMAT_VERSION: u8 = 0x01;

/// The version byte that starts every tree node encoding.
pub const TREE_FORMAT_VERSION: u8 = 0x01;

/// The longest `bytes` field, name, or path any decoder accepts: 64 MiB.
pub const MAX_FIELD_BYTES: usize = 64 << 20;

/// The most entries a tree node holds before it is split into shards.
pub const MAX_TREE_ENTRIES: usize = 4096;

/// The size of each chunk of a blob larger than this: 4 MiB.
pub const CHUNK_BYTES: usize = 4 << 20;
