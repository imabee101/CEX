//! Changesets (commits) and their canonical encoding.

use std::collections::BTreeMap;

use crate::wire::{Reader, put_bytes, put_svarint, put_uvarint};
use crate::{
    BlobId, CHANGESET_FORMAT_VERSION, ChangesetId, FormatError, MAX_FIELD_BYTES, RepoPath,
};

/// The exclusive bound on a timestamp's UTC offset: every offset obeys
/// `|offset| < UTC_OFFSET_LIMIT_SECONDS`.
pub const UTC_OFFSET_LIMIT_SECONDS: i32 = 86_400;

/// A point in time with the author's or committer's timezone: Unix seconds
/// and a UTC offset in seconds east, with `|offset| < 86,400`.
///
/// Encoded as `svarint` seconds then `svarint` offset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Timestamp {
    seconds: i64,
    offset: i32,
}

impl Timestamp {
    /// A timestamp at `seconds` since the Unix epoch with UTC offset
    /// `offset_seconds` east.
    pub fn new(seconds: i64, offset_seconds: i32) -> Result<Self, FormatError> {
        if offset_seconds.unsigned_abs() >= UTC_OFFSET_LIMIT_SECONDS as u32 {
            return Err(FormatError::OffsetOutOfRange(offset_seconds.into()));
        }
        Ok(Self {
            seconds,
            offset: offset_seconds,
        })
    }

    /// Seconds since the Unix epoch.
    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    /// The UTC offset, in seconds east.
    pub fn offset_seconds(&self) -> i32 {
        self.offset
    }

    fn encode(&self, out: &mut Vec<u8>) {
        put_svarint(out, self.seconds);
        put_svarint(out, self.offset.into());
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self, FormatError> {
        let seconds = r.svarint()?;
        let offset = r.svarint()?;
        if offset.unsigned_abs() >= UTC_OFFSET_LIMIT_SECONDS as u64 {
            return Err(FormatError::OffsetOutOfRange(offset));
        }
        Self::new(seconds, offset as i32)
    }
}

/// The committer of a changeset and when they committed it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Committer {
    /// `Name <email>`, verbatim.
    pub ident: Vec<u8>,
    /// The commit time.
    pub date: Timestamp,
}

/// The kind of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileKind {
    /// A regular file.
    Regular,
    /// An executable file.
    Executable,
    /// A symbolic link; its blob holds the link target.
    Symlink,
}

/// What a changeset does to one path, relative to its first parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileChange {
    /// The file is removed.
    Deleted,
    /// The file is added or replaced with this content.
    File {
        /// The file's kind.
        kind: FileKind,
        /// The file's content.
        blob: BlobId,
        /// The content's length in bytes.
        size: u64,
    },
}

/// The fields of a changeset, for building one with [`Changeset::new`] or
/// reading one with [`Changeset::parts`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChangesetParts {
    /// Parents in order, first parent first.
    pub parents: Vec<ChangesetId>,
    /// The author, `Name <email>` verbatim.
    pub author: Vec<u8>,
    /// When the change was authored.
    pub author_date: Timestamp,
    /// Who committed the change, and when.
    pub committer: Option<Committer>,
    /// The commit message.
    pub message: Vec<u8>,
    /// A signature over the change.
    pub signature: Option<Vec<u8>>,
    /// For imported changesets only: the git commit object's bytes after its
    /// `tree` and `parent` lines, verbatim.
    pub git_headers: Option<Vec<u8>>,
    /// The changed paths, relative to the first parent only.
    pub file_changes: BTreeMap<RepoPath, FileChange>,
}

/// A changeset: an immutable commit. It records its parents, metadata, and
/// file changes relative to its first parent, and has no tree pointer.
///
/// A `Changeset` always has a canonical encoding: every field fits within
/// `MAX_FIELD_BYTES`.
///
/// The format accepts any strictly sorted set of valid paths. The rules on
/// what a set of file changes means are checked when the changeset is
/// created on the server, not here: no-op entries (INV-11), nested changed
/// paths (INV-12, which forbids nesting except a deleted file whose path
/// becomes a directory, whatever the repository), conflicts with the first
/// parent's tree, and size limits for non-imported changesets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changeset {
    parts: ChangesetParts,
}

fn check_field(b: &[u8]) -> Result<(), FormatError> {
    if b.len() > MAX_FIELD_BYTES {
        return Err(FormatError::FieldTooLong {
            len: b.len() as u64,
        });
    }
    Ok(())
}

impl Changeset {
    /// Builds a changeset, checking that it has a canonical encoding.
    pub fn new(parts: ChangesetParts) -> Result<Self, FormatError> {
        check_field(&parts.author)?;
        check_field(&parts.message)?;
        if let Some(c) = &parts.committer {
            check_field(&c.ident)?;
        }
        if let Some(s) = &parts.signature {
            check_field(s)?;
        }
        if let Some(h) = &parts.git_headers {
            check_field(h)?;
        }
        Ok(Self { parts })
    }

    /// The changeset's fields.
    pub fn parts(&self) -> &ChangesetParts {
        &self.parts
    }

    /// Returns the changeset's fields.
    pub fn into_parts(self) -> ChangesetParts {
        self.parts
    }

    /// The changeset's id: BLAKE3 `derive_key("scion v1 changeset", ...)`
    /// over its canonical encoding.
    pub fn id(&self) -> ChangesetId {
        ChangesetId::hash(&self.encode())
    }

    /// The canonical encoding.
    pub fn encode(&self) -> Vec<u8> {
        let p = &self.parts;
        let mut out = Vec::new();
        out.push(CHANGESET_FORMAT_VERSION);
        put_uvarint(&mut out, p.parents.len() as u64);
        for parent in &p.parents {
            out.extend_from_slice(parent.as_bytes());
        }
        put_bytes(&mut out, &p.author);
        p.author_date.encode(&mut out);
        match &p.committer {
            None => out.extend_from_slice(&[0, 0]),
            Some(c) => {
                out.push(1);
                put_bytes(&mut out, &c.ident);
                out.push(1);
                c.date.encode(&mut out);
            }
        }
        put_bytes(&mut out, &p.message);
        for opt in [&p.signature, &p.git_headers] {
            match opt {
                None => out.push(0),
                Some(b) => {
                    out.push(1);
                    put_bytes(&mut out, b);
                }
            }
        }
        put_uvarint(&mut out, p.file_changes.len() as u64);
        for (path, change) in &p.file_changes {
            put_bytes(&mut out, path.as_bytes());
            match change {
                FileChange::Deleted => out.push(0),
                FileChange::File { kind, blob, size } => {
                    out.push(match kind {
                        FileKind::Regular => 1,
                        FileKind::Executable => 2,
                        FileKind::Symlink => 3,
                    });
                    out.extend_from_slice(blob.as_bytes());
                    put_uvarint(&mut out, *size);
                }
            }
        }
        out
    }

    /// Decodes a changeset, rejecting anything but its canonical encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != CHANGESET_FORMAT_VERSION {
            return Err(FormatError::UnsupportedVersion(version));
        }
        let n = r.count(32)?;
        let mut parents = Vec::with_capacity(n);
        for _ in 0..n {
            parents.push(ChangesetId::from_bytes(r.array32()?));
        }
        let author = r.bytes()?.to_vec();
        let author_date = Timestamp::decode(&mut r)?;
        let ident = if r.opt_tag()? {
            Some(r.bytes()?.to_vec())
        } else {
            None
        };
        let date = if r.opt_tag()? {
            Some(Timestamp::decode(&mut r)?)
        } else {
            None
        };
        let committer = match (ident, date) {
            (None, None) => None,
            (Some(ident), Some(date)) => Some(Committer { ident, date }),
            _ => return Err(FormatError::CommitterDateMismatch),
        };
        let message = r.bytes()?.to_vec();
        let signature = if r.opt_tag()? {
            Some(r.bytes()?.to_vec())
        } else {
            None
        };
        let git_headers = if r.opt_tag()? {
            Some(r.bytes()?.to_vec())
        } else {
            None
        };
        // Smallest change: a 1-byte length, a 1-byte path, and a kind byte.
        let n = r.count(3)?;
        let mut file_changes = BTreeMap::new();
        let mut last: Option<&[u8]> = None;
        for _ in 0..n {
            let path = r.bytes()?;
            if last.is_some_and(|l| l >= path) {
                return Err(FormatError::NotSorted);
            }
            last = Some(path);
            let change = match r.u8()? {
                0 => FileChange::Deleted,
                code @ 1..=3 => {
                    let kind = match code {
                        1 => FileKind::Regular,
                        2 => FileKind::Executable,
                        _ => FileKind::Symlink,
                    };
                    let blob = BlobId::from_bytes(r.array32()?);
                    let size = r.uvarint()?;
                    FileChange::File { kind, blob, size }
                }
                code => {
                    return Err(FormatError::InvalidCode {
                        field: "file change kind",
                        code,
                    });
                }
            };
            file_changes.insert(RepoPath::new(path)?, change);
        }
        r.finish()?;
        Ok(Self {
            parts: ChangesetParts {
                parents,
                author,
                author_date,
                committer,
                message,
                signature,
                git_headers,
                file_changes,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_offset_bounds() {
        assert!(Timestamp::new(0, 86_399).is_ok());
        assert!(Timestamp::new(0, -86_399).is_ok());
        assert_eq!(
            Timestamp::new(0, 86_400),
            Err(FormatError::OffsetOutOfRange(86_400))
        );
        assert_eq!(
            Timestamp::new(0, -86_400),
            Err(FormatError::OffsetOutOfRange(-86_400))
        );
        assert_eq!(
            Timestamp::new(0, i32::MIN),
            Err(FormatError::OffsetOutOfRange(i32::MIN.into()))
        );
    }

    #[test]
    fn decode_rejects_huge_offset_without_truncating() {
        // An offset that does not fit in i32 must be rejected, not wrapped.
        let mut b = vec![CHANGESET_FORMAT_VERSION, 0, 0, 0];
        put_svarint(&mut b, i64::from(i32::MAX) + 86_400 * 2);
        assert!(matches!(
            Changeset::decode(&b),
            Err(FormatError::OffsetOutOfRange(_))
        ));
    }

    #[test]
    fn new_rejects_oversized_fields() {
        let p = ChangesetParts {
            message: vec![b'x'; MAX_FIELD_BYTES + 1],
            ..ChangesetParts::default()
        };
        assert_eq!(
            Changeset::new(p),
            Err(FormatError::FieldTooLong {
                len: MAX_FIELD_BYTES as u64 + 1
            })
        );
    }
}
