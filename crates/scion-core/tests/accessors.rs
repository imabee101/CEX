//! Accessors, bounds, and error details that the format tests do not reach.

use scion_core::{
    BlobId, Changeset, ChangesetParts, Codec, EntryTarget, FormatError, Frame, FrameKind,
    MAX_FIELD_BYTES, MAX_FRAME_HEADER_LEN, RepoPath, Tree, TreeEntry, TreeId,
};

#[test]
fn repo_path_accessors() {
    let p = RepoPath::new(b"dir/sub/file.txt".to_vec()).unwrap();
    assert_eq!(p.as_ref(), b"dir/sub/file.txt");
    assert_eq!(
        p.components().collect::<Vec<_>>(),
        vec![&b"dir"[..], b"sub", b"file.txt"]
    );
    assert_eq!(format!("{p:?}"), "RepoPath(\"dir/sub/file.txt\")");
    assert_eq!(p.into_bytes(), b"dir/sub/file.txt");
}

#[test]
fn changeset_and_tree_into_accessors() {
    let mut parts = ChangesetParts::default();
    parts.author = b"A <a>".to_vec();
    parts.message = b"m".to_vec();
    let cs = Changeset::new(parts.clone()).unwrap();
    assert_eq!(cs.into_parts(), parts);

    let entries = vec![TreeEntry {
        name: b"d".to_vec(),
        target: EntryTarget::Tree(TreeId::hash(b"x")),
    }];
    assert_eq!(Tree::new(entries.clone()).unwrap().into_entries(), entries);
}

#[test]
fn id_hasher_debug_names_the_type() {
    let h = BlobId::hasher();
    assert!(format!("{h:?}").starts_with("IdHasher"));
}

#[test]
fn fields_of_exactly_max_field_bytes_are_accepted() {
    let max = vec![b'a'; MAX_FIELD_BYTES];
    // A changeset field.
    let mut parts = ChangesetParts::default();
    parts.message = max.clone();
    Changeset::new(parts).expect("message of MAX_FIELD_BYTES");
    // A path.
    RepoPath::new(max.clone()).expect("path of MAX_FIELD_BYTES");
    // A tree entry name.
    Tree::new(vec![TreeEntry {
        name: max,
        target: EntryTarget::Tree(TreeId::hash(b"x")),
    }])
    .expect("name of MAX_FIELD_BYTES");
    // One byte more is rejected everywhere.
    let over = vec![b'a'; MAX_FIELD_BYTES + 1];
    let too_long = Err(FormatError::FieldTooLong {
        len: MAX_FIELD_BYTES as u64 + 1,
    });
    assert_eq!(RepoPath::new(over.clone()).map(|_| ()), too_long);
    assert_eq!(
        Tree::new(vec![TreeEntry {
            name: over,
            target: EntryTarget::Tree(TreeId::hash(b"x")),
        }])
        .map(|_| ()),
        too_long
    );
}

#[test]
fn max_frame_header_len_is_magic_kind_id_codec_and_two_varints() {
    assert_eq!(MAX_FRAME_HEADER_LEN, 58);
}

#[test]
fn trailing_byte_count_is_reported() {
    let f = Frame::encode(FrameKind::Blob, [1; 32], Codec::Raw, 3, b"abc").unwrap();
    for extra in [1usize, 2, 100] {
        let mut long = f.clone();
        long.extend(std::iter::repeat_n(0, extra));
        assert_eq!(Frame::decode(&long), Err(FormatError::TrailingBytes(extra)));
    }
}
