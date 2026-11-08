//! INV-10: decoders accept only canonical encodings. Random valid values
//! round-trip and match the reference encoder; any mutation that still
//! decodes must re-encode to exactly the mutated bytes; and every varint
//! written in a non-minimal form is rejected.

mod support;

use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use scion_core::{
    BlobId, CHUNK_BYTES, Changeset, ChangesetId, ChangesetParts, ChunkId, ChunkList, Codec,
    Committer, EntryTarget, FileChange, FileKind, Frame, FrameKind, RepoPath, Timestamp, Tree,
    TreeEntry, TreeId,
};
use support::{convert, reference};

fn component() -> impl Strategy<Value = Vec<u8>> {
    vec(1u8..=255, 1..8).prop_map(|mut c| {
        for b in &mut c {
            if *b == b'/' {
                *b = b'_';
            }
        }
        if c == b"." || c == b".." {
            c.push(b'x');
        }
        c
    })
}

fn repo_path() -> impl Strategy<Value = RepoPath> {
    vec(component(), 1..5).prop_map(|cs| RepoPath::new(cs.join(&b'/')).unwrap())
}

fn timestamp() -> impl Strategy<Value = Timestamp> {
    (any::<i64>(), -86_399i32..=86_399).prop_map(|(s, o)| Timestamp::new(s, o).unwrap())
}

fn id32() -> impl Strategy<Value = [u8; 32]> {
    any::<[u8; 32]>()
}

fn size() -> impl Strategy<Value = u64> {
    prop_oneof![0u64..300, any::<u64>()]
}

fn file_kind() -> impl Strategy<Value = FileKind> {
    prop_oneof![
        Just(FileKind::Regular),
        Just(FileKind::Executable),
        Just(FileKind::Symlink)
    ]
}

fn file_change() -> impl Strategy<Value = FileChange> {
    prop_oneof![
        Just(FileChange::Deleted),
        (file_kind(), id32(), size()).prop_map(|(kind, b, size)| FileChange::File {
            kind,
            blob: BlobId::from_bytes(b),
            size
        }),
    ]
}

fn bytes(max: usize) -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 0..max)
}

prop_compose! {
    fn changeset()(
        parents in vec(id32(), 0..4),
        author in bytes(200),
        author_date in timestamp(),
        committer in proptest::option::of((bytes(20), timestamp())),
        message in bytes(300),
        signature in proptest::option::of(bytes(150)),
        git_headers in proptest::option::of(bytes(20)),
        file_changes in btree_map(repo_path(), file_change(), 0..12),
    ) -> Changeset {
        let mut p = ChangesetParts::default();
        p.parents = parents.into_iter().map(ChangesetId::from_bytes).collect();
        p.author = author;
        p.author_date = author_date;
        p.committer = committer.map(|(ident, date)| Committer { ident, date });
        p.message = message;
        p.signature = signature;
        p.git_headers = git_headers;
        p.file_changes = file_changes;
        Changeset::new(p).unwrap()
    }
}

fn target(shard_ok: bool) -> impl Strategy<Value = EntryTarget> {
    let shard = if shard_ok { 1u8 } else { 0 };
    (0u8..=(2 + shard), file_kind(), id32(), size(), id32()).prop_map(
        move |(k, kind, id, size, sha)| match k {
            0 => EntryTarget::Tree(TreeId::from_bytes(id)),
            1 if shard == 1 => EntryTarget::Shard(TreeId::from_bytes(id)),
            _ => EntryTarget::File {
                kind,
                blob: BlobId::from_bytes(id),
                size,
                sha256: sha,
            },
        },
    )
}

fn plain_tree() -> impl Strategy<Value = Tree> {
    btree_map(component(), target(false), 0..20).prop_map(|m| {
        Tree::new(
            m.into_iter()
                .map(|(name, target)| TreeEntry { name, target })
                .collect(),
        )
        .unwrap()
    })
}

/// A node split by the shard rule: Shards named prefix `P` plus one byte,
/// and possibly one entry named exactly `P`.
fn sharded_tree() -> impl Strategy<Value = Tree> {
    let name_byte = (1u8..=255).prop_map(|b| if b == b'/' { b'_' } else { b });
    (
        vec(name_byte.clone(), 0..4),
        proptest::option::of(target(false)),
        proptest::collection::btree_set(name_byte, 1..10),
        id32(),
    )
        .prop_filter_map("needs a valid shape", |(prefix, p_entry, next, seed)| {
            let p_entry = p_entry.filter(|_| RepoPath::new(prefix.clone()).is_ok());
            if p_entry.is_none() && next.len() < 2 {
                return None;
            }
            let mut entries: Vec<TreeEntry> = p_entry
                .map(|target| TreeEntry {
                    name: prefix.clone(),
                    target,
                })
                .into_iter()
                .collect();
            entries.extend(next.into_iter().map(|b| TreeEntry {
                name: [&prefix[..], &[b]].concat(),
                target: EntryTarget::Shard(TreeId::hash(&[&seed[..], &[b]].concat())),
            }));
            Some(Tree::new(entries).expect("a valid sharded node"))
        })
}

fn tree() -> impl Strategy<Value = Tree> {
    prop_oneof![plain_tree(), sharded_tree()]
}

fn frame() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Raw blob, chunk, and tree frames.
        (
            prop_oneof![
                Just(FrameKind::Blob),
                Just(FrameKind::Chunk),
                Just(FrameKind::Tree)
            ],
            id32(),
            bytes(64)
        )
            .prop_filter("chunks are non-empty", |(k, _, p)| *k != FrameKind::Chunk
                || !p.is_empty())
            .prop_map(|(k, id, p)| Frame::encode(k, id, Codec::Raw, p.len() as u64, &p).unwrap()),
        // Zstd frames: the payload is shorter than the raw length.
        (
            prop_oneof![
                Just(FrameKind::Blob),
                Just(FrameKind::Chunk),
                Just(FrameKind::Tree)
            ],
            id32(),
            bytes(64),
            0u64..=CHUNK_BYTES as u64
        )
            .prop_filter("zstd shrinks", |(_, _, p, extra)| (p.len() as u64) < *extra)
            .prop_map(|(k, id, p, raw)| Frame::encode(k, id, Codec::Zstd, raw, &p).unwrap()),
        // Chunk lists for blobs of up to 8 chunks.
        (
            id32(),
            (CHUNK_BYTES as u64 + 1)..=(8 * CHUNK_BYTES as u64),
            id32()
        )
            .prop_map(|(id, raw, seed)| {
                let n = raw.div_ceil(CHUNK_BYTES as u64) as usize;
                let chunks = (0..n)
                    .map(|i| ChunkId::hash(&[&seed[..], &[i as u8]].concat()))
                    .collect();
                Frame::encode(
                    FrameKind::ChunkList,
                    id,
                    Codec::Raw,
                    raw,
                    &ChunkList { chunks }.encode(),
                )
                .unwrap()
            }),
    ]
}

#[derive(Debug, Clone)]
enum Mutation {
    Flip(usize, u8),
    Insert(usize, u8),
    Remove(usize),
    Truncate(usize),
    Splice(usize, Vec<u8>),
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (any::<usize>(), 1u8..=255).prop_map(|(i, x)| Mutation::Flip(i, x)),
        (any::<usize>(), any::<u8>()).prop_map(|(i, b)| Mutation::Insert(i, b)),
        any::<usize>().prop_map(Mutation::Remove),
        any::<usize>().prop_map(Mutation::Truncate),
        (any::<usize>(), bytes(6)).prop_map(|(i, b)| Mutation::Splice(i, b)),
    ]
}

fn apply(mut b: Vec<u8>, m: &Mutation) -> Vec<u8> {
    let at = |i: usize, len: usize| if len == 0 { 0 } else { i % len };
    match m {
        Mutation::Flip(i, x) if !b.is_empty() => {
            let i = at(*i, b.len());
            b[i] ^= x;
        }
        Mutation::Insert(i, x) => {
            let i = at(*i, b.len() + 1);
            b.insert(i, *x);
        }
        Mutation::Remove(i) if !b.is_empty() => {
            b.remove(at(*i, b.len()));
        }
        Mutation::Truncate(i) => b.truncate(at(*i, b.len())),
        Mutation::Splice(i, x) => {
            let i = at(*i, b.len() + 1);
            b.splice(i..i, x.iter().copied());
        }
        _ => {}
    }
    b
}

/// Whatever a decoder accepts must be the canonical encoding of the value
/// it returns.
fn assert_canonical_if_accepted(b: &[u8]) {
    if let Ok(c) = Changeset::decode(b) {
        assert_eq!(c.encode(), b);
    }
    if let Ok(t) = Tree::decode(b) {
        assert_eq!(t.encode(), b);
    }
    if let Ok(f) = Frame::decode(b) {
        let h = f.header();
        assert_eq!(
            Frame::encode(h.kind, h.id, h.codec, h.raw_len, f.payload()).unwrap(),
            b
        );
    }
    if let Ok(l) = ChunkList::decode(b) {
        assert_eq!(l.encode(), b);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn changesets_round_trip_and_match_reference(c in changeset()) {
        let bytes = c.encode();
        prop_assert_eq!(&reference::changeset(&convert::changeset(&c)), &bytes);
        prop_assert_eq!(&Changeset::decode(&bytes).unwrap(), &c);
        prop_assert_eq!(*c.id().as_bytes(), blake3::derive_key("scion v1 changeset", &bytes));
    }

    #[test]
    fn trees_round_trip_and_match_reference(t in tree()) {
        let bytes = t.encode();
        prop_assert_eq!(&reference::tree(&convert::tree(&t)), &bytes);
        prop_assert_eq!(&Tree::decode(&bytes).unwrap(), &t);
        prop_assert_eq!(*t.id().as_bytes(), blake3::derive_key("scion v1 tree", &bytes));
    }

    #[test]
    fn frames_round_trip_and_match_reference(f in frame()) {
        let d = Frame::decode(&f).unwrap();
        let h = d.header();
        prop_assert_eq!(reference::frame(h.kind.code(), &h.id, h.codec.code(), h.raw_len, d.payload()), f);
    }

    #[test]
    fn mutated_changesets_decode_only_canonically(c in changeset(), ms in vec(mutation(), 1..4)) {
        let b = ms.iter().fold(c.encode(), apply);
        assert_canonical_if_accepted(&b);
    }

    #[test]
    fn mutated_trees_decode_only_canonically(t in tree(), ms in vec(mutation(), 1..4)) {
        let b = ms.iter().fold(t.encode(), apply);
        assert_canonical_if_accepted(&b);
    }

    #[test]
    fn mutated_frames_decode_only_canonically(f in frame(), ms in vec(mutation(), 1..4)) {
        let b = ms.iter().fold(f, apply);
        assert_canonical_if_accepted(&b);
    }

    #[test]
    fn arbitrary_bytes_decode_only_canonically(b in bytes(200)) {
        assert_canonical_if_accepted(&b);
        let mut versioned = vec![1u8];
        versioned.extend(&b);
        assert_canonical_if_accepted(&versioned);
    }

    #[test]
    fn every_non_minimal_changeset_varint_is_rejected(c in changeset()) {
        let r = convert::changeset(&c);
        let (_, n) = reference::with_padded_varint(None, || reference::changeset(&r));
        prop_assert!(n > 0);
        for k in 0..n {
            let (padded, _) = reference::with_padded_varint(Some(k), || reference::changeset(&r));
            prop_assert!(Changeset::decode(&padded).is_err(), "varint {} padded was accepted", k);
        }
    }

    #[test]
    fn every_non_minimal_tree_varint_is_rejected(t in tree()) {
        let r = convert::tree(&t);
        let (_, n) = reference::with_padded_varint(None, || reference::tree(&r));
        for k in 0..n {
            let (padded, _) = reference::with_padded_varint(Some(k), || reference::tree(&r));
            prop_assert!(Tree::decode(&padded).is_err(), "varint {} padded was accepted", k);
        }
    }

    #[test]
    fn every_non_minimal_frame_varint_is_rejected(f in frame()) {
        let d = Frame::decode(&f).unwrap();
        let h = *d.header();
        let (_, n) = reference::with_padded_varint(None, || reference::frame(h.kind.code(), &h.id, h.codec.code(), h.raw_len, d.payload()));
        prop_assert_eq!(n, 2);
        for k in 0..n {
            let (padded, _) = reference::with_padded_varint(Some(k), || reference::frame(h.kind.code(), &h.id, h.codec.code(), h.raw_len, d.payload()));
            prop_assert!(Frame::decode(&padded).is_err(), "varint {} padded was accepted", k);
        }
    }

    #[test]
    fn unsorted_or_duplicate_changes_are_rejected(c in changeset(), i in any::<usize>(), j in any::<usize>()) {
        let mut r = convert::changeset(&c);
        prop_assume!(r.changes.len() >= 2);
        let (i, j) = (i % r.changes.len(), j % r.changes.len());
        prop_assume!(i != j);
        if i < j { r.changes[j] = r.changes[i].clone(); } else { r.changes.swap(i, j); }
        prop_assert!(Changeset::decode(&reference::changeset(&r)).is_err());
    }

    #[test]
    fn unsorted_or_duplicate_tree_entries_are_rejected(t in tree(), i in any::<usize>(), j in any::<usize>()) {
        let mut r = convert::tree(&t);
        prop_assume!(r.len() >= 2);
        let (i, j) = (i % r.len(), j % r.len());
        prop_assume!(i != j);
        if i < j { r[j].0 = r[i].0.clone(); } else { r.swap(i, j); }
        prop_assert!(Tree::decode(&reference::tree(&r)).is_err());
    }
}
