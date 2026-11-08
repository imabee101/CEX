//! Tree nodes and their canonical encoding.

use crate::path::check_name;
use crate::wire::{Reader, put_bytes, put_uvarint};
use crate::{BlobId, FORMAT_VERSION, FileKind, FormatError, MAX_TREE_ENTRIES, TreeId};

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
        out.push(FORMAT_VERSION);
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
        if version != FORMAT_VERSION {
            return Err(FormatError::UnsupportedVersion(version));
        }
        // Peek the count first so an oversized node is rejected as such.
        let n = Reader::new(&bytes[1..]).uvarint()?;
        if n > MAX_TREE_ENTRIES as u64 {
            return Err(FormatError::TooManyEntries(n));
        }
        // Smallest entry: a 1-byte length, a 1-byte name, a kind, and an id.
        let n = r.count(35)?;
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
        Ok(Self { entries })
    }
}
