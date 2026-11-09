//! Tree nodes and their canonical encoding.

use crate::path::check_name;
use crate::wire::{Reader, put_bytes, put_uvarint};
use crate::{BlobId, FileKind, FormatError, MAX_TREE_ENTRIES, TREE_FORMAT_VERSION, TreeId};

/// What a tree entry points to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryTarget {
    /// A subdirectory.
    Tree(TreeId),
    /// One part of a directory with more than `MAX_TREE_ENTRIES` names; the
    /// entry's name is the prefix shared by every name in the shard.
    Shard(TreeId),
    /// A file, with the size and SHA-256 of its content.
    File {
        /// The file's kind.
        kind: FileKind,
        /// The file's content.
        blob: BlobId,
        /// The content's length in bytes.
        size: u64,
        /// The SHA-256 digest of the content.
        sha256: [u8; 32],
    },
}

/// One entry of a tree node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TreeEntry {
    /// The entry's name: a path component, or a name prefix for a shard.
    pub name: Vec<u8>,
    /// What the entry points to.
    pub target: EntryTarget,
}

/// A tree node: at most `MAX_TREE_ENTRIES` entries in strictly increasing
/// name byte order.
///
/// Trees are derived by the server. This type checks only what a single node
/// can show about itself; whether a node is correctly split into shards
/// depends on the whole directory and is checked where trees are built.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Tree {
    entries: Vec<TreeEntry>,
}

impl Tree {
    /// Builds a tree node from entries in strictly increasing name order.
    pub fn new(entries: Vec<TreeEntry>) -> Result<Self, FormatError> {
        if entries.len() > MAX_TREE_ENTRIES {
            return Err(FormatError::TooManyEntries(entries.len() as u64));
        }
        for e in &entries {
            check_name(&e.name, matches!(e.target, EntryTarget::Shard(_)))?;
        }
        if entries.windows(2).any(|w| w[0].name >= w[1].name) {
            return Err(FormatError::NotSorted);
        }
        check_shard_shape(&entries)?;
        Ok(Self { entries })
    }

    /// The entries, in name byte order.
    pub fn entries(&self) -> &[TreeEntry] {
        &self.entries
    }

    /// Returns the entries.
    pub fn into_entries(self) -> Vec<TreeEntry> {
        self.entries
    }

    /// The node's id: BLAKE3 `derive_key("scion v1 tree", ...)` over its
    /// canonical encoding.
    pub fn id(&self) -> TreeId {
        TreeId::hash(&self.encode())
    }

    /// The canonical encoding.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(TREE_FORMAT_VERSION);
        put_uvarint(&mut out, self.entries.len() as u64);
        for e in &self.entries {
            put_bytes(&mut out, &e.name);
            match &e.target {
                EntryTarget::Tree(id) => {
                    out.push(0);
                    out.extend_from_slice(id.as_bytes());
                }
                EntryTarget::Shard(id) => {
                    out.push(1);
                    out.extend_from_slice(id.as_bytes());
                }
                EntryTarget::File {
                    kind,
                    blob,
                    size,
                    sha256,
                } => {
                    out.push(match kind {
                        FileKind::Regular => 2,
                        FileKind::Executable => 3,
                        FileKind::Symlink => 4,
                    });
                    out.extend_from_slice(blob.as_bytes());
                    put_uvarint(&mut out, *size);
                    out.extend_from_slice(sha256);
                }
            }
        }
        out
    }

    /// Decodes a tree node, rejecting anything but its canonical encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != TREE_FORMAT_VERSION {
            return Err(FormatError::UnsupportedVersion(version));
        }
        let n = r.uvarint()?;
        if n > MAX_TREE_ENTRIES as u64 {
            return Err(FormatError::TooManyEntries(n));
        }
        // Smallest entry: a 1-byte length, a 1-byte name, a kind, and an id.
        let n = r.fits(n, 35)?;
        let mut entries: Vec<TreeEntry> = Vec::with_capacity(n);
        for _ in 0..n {
            let name = r.bytes()?;
            if entries.last().is_some_and(|l| l.name.as_slice() >= name) {
                return Err(FormatError::NotSorted);
            }
            let code = r.u8()?;
            let target = match code {
                0 => EntryTarget::Tree(TreeId::from_bytes(r.array32()?)),
                1 => EntryTarget::Shard(TreeId::from_bytes(r.array32()?)),
                2..=4 => {
                    let kind = match code {
                        2 => FileKind::Regular,
                        3 => FileKind::Executable,
                        _ => FileKind::Symlink,
                    };
                    let blob = BlobId::from_bytes(r.array32()?);
                    let size = r.uvarint()?;
                    let sha256 = r.array32()?;
                    EntryTarget::File {
                        kind,
                        blob,
                        size,
                        sha256,
                    }
                }
                code => {
                    return Err(FormatError::InvalidCode {
                        field: "tree entry kind",
                        code,
                    });
                }
            };
            check_name(name, code == 1)?;
            entries.push(TreeEntry {
                name: name.to_vec(),
                target,
            });
        }
        r.finish()?;
        check_shard_shape(&entries)?;
        Ok(Self { entries })
    }
}

/// The split rule as far as one node shows it. A node with Shard entries was
/// split with `P` the longest common prefix of the directory's names: each
/// Shard is named `P` plus one byte, the only other entry is one named `P`,
/// and since `P` is the longest common prefix, a node with no entry named `P`
/// has at least two shards. Entries must already be strictly sorted.
fn check_shard_shape(entries: &[TreeEntry]) -> Result<(), FormatError> {
    let is_shard = |e: &TreeEntry| matches!(e.target, EntryTarget::Shard(_));
    let Some(first) = entries.iter().find(|e| is_shard(e)) else {
        return Ok(());
    };
    // Shard names are non-empty, so the prefix is the name minus its last byte.
    let prefix = &first.name[..first.name.len() - 1];
    let mut shards = 0;
    for e in entries {
        if is_shard(e) {
            if e.name.len() != first.name.len() || !e.name.starts_with(prefix) {
                return Err(FormatError::InvalidShard);
            }
            shards += 1;
        } else if e.name != prefix {
            return Err(FormatError::InvalidShard);
        }
    }
    if shards == entries.len() && shards < 2 {
        return Err(FormatError::InvalidShard);
    }
    Ok(())
}
