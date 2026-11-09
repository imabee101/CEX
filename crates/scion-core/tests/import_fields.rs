//! The changeset format and the fields AGENTS.md allows only on imported
//! changesets (those with `git_headers`): an empty author, an author over
//! 1,024 bytes, more than 64 parents, a message over 1 MiB, a signature
//! over 64 KiB, and paths over 4,096 bytes or with components over 255.
//!
//! The format is the same with and without `git_headers`: both encode and
//! decode such fields canonically, so an imported changeset round-trips
//! exactly, and refusing them on a non-imported changeset is CreateChangeset's
//! rule, not the decoder's. What the format refuses, a field over
//! `MAX_FIELD_BYTES`, it refuses with and without `git_headers`.

use scion_core::{
    BlobId, Changeset, ChangesetId, ChangesetParts, FileChange, FileKind, FormatError,
    MAX_FIELD_BYTES, RepoPath,
};

/// A changeset with every import-only field, with or without `git_headers`.
fn import_only_fields(git_headers: Option<&[u8]>) -> ChangesetParts {
    let component = vec![b'c'; 256];
    let long_path = vec![&component[..]; 17].join(&b'/');
    assert!(long_path.len() > 4096);
    let mut p = ChangesetParts::default();
    p.parents = (0..65u8)
        .map(|i| ChangesetId::from_bytes([i; 32]))
        .collect();
    p.author = vec![b'a'; 1025];
    p.message = vec![b'm'; (1 << 20) + 1];
    p.signature = Some(vec![b's'; (64 << 10) + 1]);
    p.git_headers = git_headers.map(<[u8]>::to_vec);
    p.file_changes = [
        (RepoPath::new(component).unwrap(), FileChange::Deleted),
        (
            RepoPath::new(long_path).unwrap(),
            FileChange::File {
                kind: FileKind::Regular,
                blob: BlobId::from_bytes([1; 32]),
                size: 1,
            },
        ),
    ]
    .into();
    p
}

fn round_trips(parts: ChangesetParts) -> Changeset {
    let cs = Changeset::new(parts.clone()).expect("a canonical changeset");
    let bytes = cs.encode();
    let back = Changeset::decode(&bytes).expect("decodes");
    assert_eq!(back, cs);
    assert_eq!(back.encode(), bytes);
    assert_eq!(back.into_parts(), parts);
    cs
}

#[test]
fn an_empty_author_is_canonical_with_and_without_git_headers() {
    let mut p = ChangesetParts::default();
    let plain = round_trips(p.clone());
    // The author is `bytes`: a zero length, then nothing.
    assert_eq!(plain.encode()[..3], [0x01, 0x00, 0x00]);
    p.git_headers = Some(b"author  0 +0000\n\nm".to_vec());
    let imported = round_trips(p.clone());
    assert_ne!(imported.id(), plain.id());
    // Present but empty headers are not absent headers.
    p.git_headers = Some(Vec::new());
    let empty_headers = round_trips(p);
    assert_ne!(empty_headers.id(), plain.id());
    assert_ne!(empty_headers.id(), imported.id());
}

#[test]
fn import_only_fields_are_canonical_with_and_without_git_headers() {
    let imported = round_trips(import_only_fields(Some(b"author x 0 +0000\n\nm")));
    let plain = round_trips(import_only_fields(None));
    assert_ne!(imported.id(), plain.id());
    assert_eq!(imported.parts().parents.len(), 65);
    assert_eq!(plain.parts().author.len(), 1025);
}

#[test]
fn a_field_over_max_field_bytes_is_rejected_with_and_without_git_headers() {
    let too_long = Err(FormatError::FieldTooLong {
        len: MAX_FIELD_BYTES as u64 + 1,
    });
    for headers in [None, Some(&b"h"[..])] {
        let mut p = ChangesetParts::default();
        p.git_headers = headers.map(<[u8]>::to_vec);
        p.author = vec![b'a'; MAX_FIELD_BYTES + 1];
        assert_eq!(Changeset::new(p).map(|_| ()), too_long, "{headers:?}");
    }
    let mut p = ChangesetParts::default();
    p.git_headers = Some(vec![b'h'; MAX_FIELD_BYTES + 1]);
    assert_eq!(Changeset::new(p).map(|_| ()), too_long);
}
