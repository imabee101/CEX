//! Maps scion-core values to the reference encoder's plain types, so the two
//! encoders can be compared on the same value.

use scion_core::{Changeset, EntryTarget, FileChange, FileKind, Timestamp, Tree};

use super::reference::{RefChange, RefChangeset, RefEntry};

fn ts(t: &Timestamp) -> (i64, i64) {
    (t.seconds(), t.offset_seconds().into())
}

pub(crate) fn changeset(c: &Changeset) -> RefChangeset {
    let p = c.parts();
    RefChangeset {
        parents: p.parents.iter().map(|id| *id.as_bytes()).collect(),
        author: p.author.clone(),
        author_date: ts(&p.author_date),
        committer: p.committer.as_ref().map(|c| (c.ident.clone(), ts(&c.date))),
        message: p.message.clone(),
        signature: p.signature.clone(),
        git_headers: p.git_headers.clone(),
        changes: p
            .file_changes
            .iter()
            .map(|(path, ch)| {
                let ch = match ch {
                    FileChange::Deleted => RefChange::Deleted,
                    FileChange::File { kind, blob, size } => RefChange::File(
                        match kind {
                            FileKind::Regular => 1,
                            FileKind::Executable => 2,
                            FileKind::Symlink => 3,
                        },
                        *blob.as_bytes(),
                        *size,
                    ),
                };
                (path.as_bytes().to_vec(), ch)
            })
            .collect(),
    }
}

pub(crate) fn tree(t: &Tree) -> Vec<(Vec<u8>, RefEntry)> {
    t.entries()
        .iter()
        .map(|e| {
            let r = match &e.target {
                EntryTarget::Tree(id) => RefEntry::Tree(*id.as_bytes()),
                EntryTarget::Shard(id) => RefEntry::Shard(*id.as_bytes()),
                EntryTarget::File {
                    kind,
                    blob,
                    size,
                    sha256,
                } => RefEntry::File(
                    match kind {
                        FileKind::Regular => 2,
                        FileKind::Executable => 3,
                        FileKind::Symlink => 4,
                    },
                    *blob.as_bytes(),
                    *size,
                    *sha256,
                ),
            };
            (e.name.clone(), r)
        })
        .collect()
}
