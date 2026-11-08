//! Golden vectors: scion-core must accept exactly the accepted vectors,
//! re-encode them to the same bytes, and compute the same ids; the
//! language-neutral vectors file must hold exactly these vectors.

mod support;

use std::collections::{BTreeMap, HashSet};

use scion_core::{
    BlobId, Changeset, ChangesetId, ChangesetParts, ChunkId, Codec, Committer, EntryTarget,
    FileChange, FileKind, Frame, FrameHeader, FrameKind, RepoPath, Timestamp, Tree, TreeEntry,
    TreeId,
};
use support::vectors::{self, Ty, Vector};
use support::{convert, hx, id, reference, to_hex};

const VECTORS_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors/v1.txt");

/// What scion-core decodes `v` to, re-encoded, with its id; or the error.
fn decode(v: &Vector) -> Result<(Vec<u8>, [u8; 32]), scion_core::FormatError> {
    Ok(match v.ty {
        Ty::Blob => (v.bytes.clone(), *BlobId::hash(&v.bytes).as_bytes()),
        Ty::Chunk => (v.bytes.clone(), *ChunkId::hash(&v.bytes).as_bytes()),
        Ty::Tree => {
            let t = Tree::decode(&v.bytes)?;
            (t.encode(), *t.id().as_bytes())
        }
        Ty::Changeset => {
            let c = Changeset::decode(&v.bytes)?;
            (c.encode(), *c.id().as_bytes())
        }
        Ty::Frame => {
            let f = Frame::decode(&v.bytes)?;
            let h = f.header();
            let again = Frame::encode(h.kind, h.id, h.codec, h.raw_len, f.payload())
                .expect("a decoded frame re-encodes");
            (again, h.id)
        }
    })
}

#[test]
fn vector_names_are_unique_and_both_outcomes_are_covered() {
    let all = vectors::all();
    let names: HashSet<_> = all.iter().map(|v| v.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    for ty in [Ty::Tree, Ty::Changeset, Ty::Frame] {
        assert!(all.iter().any(|v| v.ty == ty && v.accept));
        assert!(all.iter().any(|v| v.ty == ty && !v.accept));
    }
}

#[test]
fn scion_core_accepts_exactly_the_accepted_vectors() {
    for v in vectors::all() {
        let got = decode(&v);
        if v.accept {
            let (bytes, id) = got.unwrap_or_else(|e| panic!("{}: rejected: {e}", v.name));
            assert!(bytes == v.bytes, "{}: re-encoding differs", v.name);
            assert_eq!(Some(id), v.id, "{}: id differs", v.name);
        } else {
            assert!(got.is_err(), "{}: accepted a reject vector", v.name);
        }
    }
}

#[test]
fn ids_and_digests_are_plain_blake3() {
    for v in vectors::all().into_iter().filter(|v| v.accept) {
        if let Some(ctx) = v.ty.context() {
            assert_eq!(Some(blake3::derive_key(ctx, &v.bytes)), v.id, "{}", v.name);
        } else {
            let (body, digest) = v.bytes.split_at(v.bytes.len() - 32);
            assert_eq!(blake3::hash(body).as_bytes(), digest, "{}", v.name);
        }
    }
}

#[test]
fn reference_encoder_reproduces_hand_written_vectors() {
    // The hand-written bytes and the reference encoder are independent
    // derivations from the layout, so each checks the other.
    for v in vectors::small().into_iter().filter(|v| v.accept) {
        let again = match v.ty {
            Ty::Changeset => {
                reference::changeset(&convert::changeset(&Changeset::decode(&v.bytes).unwrap()))
            }
            Ty::Tree => reference::tree(&convert::tree(&Tree::decode(&v.bytes).unwrap())),
            Ty::Frame => {
                let f = Frame::decode(&v.bytes).unwrap();
                let h = f.header();
                reference::frame(h.kind.code(), &h.id, h.codec.code(), h.raw_len, f.payload())
            }
            Ty::Blob | Ty::Chunk => continue,
        };
        assert!(again == v.bytes, "{}: reference encoder disagrees", v.name);
    }
}

fn ts(s: i64, o: i32) -> Timestamp {
    Timestamp::new(s, o).unwrap()
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p.as_bytes()).unwrap()
}

fn file(kind: FileKind, b: u8, size: u64) -> FileChange {
    FileChange::File {
        kind,
        blob: BlobId::from_bytes([b; 32]),
        size,
    }
}

fn changeset_values() -> BTreeMap<&'static str, Changeset> {
    let mut m = BTreeMap::new();
    let mut p = ChangesetParts::default();
    p.author = b"x".to_vec();
    m.insert("cs-minimal", Changeset::new(p.clone()).unwrap());
    p.author_date = ts(0, 86_399);
    m.insert("cs-offset-max", Changeset::new(p).unwrap());

    let mut p = ChangesetParts::default();
    p.parents = vec![
        ChangesetId::from_bytes([0x11; 32]),
        ChangesetId::from_bytes([0x12; 32]),
    ];
    p.author = b"A <a@x>".to_vec();
    p.author_date = ts(300, 3600);
    p.committer = Some(Committer {
        ident: b"c".to_vec(),
        date: ts(-1, -3600),
    });
    p.message = b"m\n".to_vec();
    p.signature = Some(b"sig".to_vec());
    p.file_changes = [
        (path("a"), file(FileKind::Regular, 0x22, 5)),
        (path("b/c"), FileChange::Deleted),
        (path("b/d"), file(FileKind::Symlink, 0x23, 128)),
    ]
    .into();
    m.insert("cs-full", Changeset::new(p).unwrap());

    let mut p = ChangesetParts::default();
    p.author_date = ts(i64::MIN, 86_399);
    p.committer = Some(Committer {
        ident: b"c".to_vec(),
        date: ts(i64::MAX, -86_399),
    });
    p.git_headers = Some(b"h".to_vec());
    p.file_changes = [(path("x"), file(FileKind::Executable, 0x22, 0))].into();
    m.insert("cs-imported", Changeset::new(p).unwrap());
    m
}

fn entry(name: &[u8], target: EntryTarget) -> TreeEntry {
    TreeEntry {
        name: name.to_vec(),
        target,
    }
}

fn tfile(kind: FileKind, b: u8, size: u64, sha: u8) -> EntryTarget {
    EntryTarget::File {
        kind,
        blob: BlobId::from_bytes([b; 32]),
        size,
        sha256: [sha; 32],
    }
}

fn tree_values() -> BTreeMap<&'static str, Tree> {
    let t = |b: u8| TreeId::from_bytes([b; 32]);
    BTreeMap::from([
        ("tree-empty", Tree::new(vec![]).unwrap()),
        (
            "tree-mixed",
            Tree::new(vec![
                entry(b"a", tfile(FileKind::Regular, 0x22, 3, 0x33)),
                entry(b"b", tfile(FileKind::Executable, 0x23, 0, 0x34)),
                entry(b"d", EntryTarget::Tree(t(0x44))),
                entry(b"l", tfile(FileKind::Symlink, 0x24, 1, 0x35)),
            ])
            .unwrap(),
        ),
        (
            "tree-shard-dots",
            Tree::new(vec![
                entry(b".", EntryTarget::Shard(t(0x45))),
                entry(b"..", EntryTarget::Shard(t(0x46))),
            ])
            .unwrap(),
        ),
        (
            "tree-dotted-names",
            Tree::new(vec![
                entry(b"...", EntryTarget::Tree(t(0x44))),
                entry(b".a", tfile(FileKind::Regular, 0x22, 0, 0x33)),
            ])
            .unwrap(),
        ),
        (
            "tree-size-multibyte",
            Tree::new(vec![entry(
                b"f",
                tfile(FileKind::Regular, 0x22, 65_535, 0x33),
            )])
            .unwrap(),
        ),
    ])
}

fn frame_values() -> BTreeMap<&'static str, (FrameHeader, Vec<u8>)> {
    let h = |kind, id_hex: &str, codec, raw, payload: &[u8]| {
        (
            FrameHeader::new(kind, id(id_hex), codec, raw, payload.len() as u64).unwrap(),
            payload.to_vec(),
        )
    };
    BTreeMap::from([
        (
            "frame-blob-raw",
            h(
                FrameKind::Blob,
                "ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27",
                Codec::Raw,
                2,
                b"hi",
            ),
        ),
        (
            "frame-blob-empty",
            h(
                FrameKind::Blob,
                "395e5ff2c5ebbdece5384a16c163976f3037d28f4b4b889bdf0c103fcd199bdc",
                Codec::Raw,
                0,
                b"",
            ),
        ),
        (
            "frame-tree",
            h(
                FrameKind::Tree,
                "7c007800ec20b9f58d356d2136b30da8a2194d7e0bdc697c4b74d8b592f354e4",
                Codec::Raw,
                2,
                &[1, 0],
            ),
        ),
        (
            "frame-chunk-zstd",
            h(
                FrameKind::Chunk,
                "8c17519b35ab651973a544b881cb0ec8288a14ec8cdfdc0f98126fac3d8d7e21",
                Codec::Zstd,
                100,
                &hx("28b52ffd206445000010616101003f012c"),
            ),
        ),
        (
            "frame-chunk-list",
            h(
                FrameKind::ChunkList,
                "6a0b5c0a7ea6a142a9cddda39fb8fcdaa63e3b4238ce1f669cd26e44748eed09",
                Codec::Raw,
                (4 << 20) + 1,
                &hx(
                    "02 d6328e383be99a1be1977473af3d76d2ac58f9f9f31a61f5707a3fec0b24505c b5cebad219f9c79a2517ff900e9b957c8f18d2af7820f86c848e9b9994eeeddf",
                ),
            ),
        ),
    ])
}

#[test]
fn hand_written_vectors_are_the_expected_values() {
    let small = vectors::small();
    let find = |name: &str| {
        small
            .iter()
            .find(|v| v.name == name)
            .unwrap_or_else(|| panic!("no vector {name}"))
    };
    let (cs, trees, frames) = (changeset_values(), tree_values(), frame_values());
    for (name, value) in &cs {
        let v = find(name);
        assert!(value.encode() == v.bytes, "{name}: encoding differs");
        assert_eq!(&Changeset::decode(&v.bytes).unwrap(), value, "{name}");
        assert_eq!(Some(*value.id().as_bytes()), v.id, "{name}");
    }
    for (name, value) in &trees {
        let v = find(name);
        assert!(value.encode() == v.bytes, "{name}: encoding differs");
        assert_eq!(&Tree::decode(&v.bytes).unwrap(), value, "{name}");
        assert_eq!(Some(*value.id().as_bytes()), v.id, "{name}");
    }
    for (name, (header, payload)) in &frames {
        let v = find(name);
        let f = Frame::decode(&v.bytes).unwrap();
        assert_eq!(f.header(), header, "{name}");
        assert_eq!(f.payload(), payload.as_slice(), "{name}");
        let encoded = Frame::encode(
            header.kind,
            header.id,
            header.codec,
            header.raw_len,
            payload,
        )
        .unwrap();
        assert!(encoded == v.bytes, "{name}: encoding differs");
    }
    // Every accepted structured vector has an expected value.
    for v in small.iter().filter(|v| v.accept) {
        let covered = match v.ty {
            Ty::Changeset => cs.contains_key(v.name.as_str()),
            Ty::Tree => trees.contains_key(v.name.as_str()),
            Ty::Frame => frames.contains_key(v.name.as_str()),
            Ty::Blob | Ty::Chunk => true,
        };
        assert!(covered, "{} has no expected value", v.name);
    }
}

#[test]
fn chunk_list_vector_ids_are_of_its_chunks() {
    // The chunk-list vector describes 4 MiB + 1 zero bytes; check its ids
    // with BLAKE3 alone so the vector is self-consistent.
    let zeros = vec![0u8; (4 << 20) + 1];
    let expect = [
        to_hex(&blake3::derive_key("scion v1 blob", &zeros)),
        to_hex(&blake3::derive_key("scion v1 chunk", &zeros[..4 << 20])),
        to_hex(&blake3::derive_key("scion v1 chunk", &zeros[4 << 20..])),
    ];
    let v = vectors::small()
        .into_iter()
        .find(|v| v.name == "frame-chunk-list")
        .unwrap();
    let hex = to_hex(&v.bytes);
    for e in expect {
        assert!(hex.contains(&e), "frame-chunk-list lacks {e}");
    }
}

#[test]
fn vectors_file_matches_golden_vectors() {
    let expected = vectors::all();
    let rendered = vectors::render(&expected);
    let text = std::fs::read_to_string(VECTORS_FILE).unwrap_or_default();
    let parsed = vectors::parse(&text);
    if parsed.as_ref() != Ok(&expected) {
        let out = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("v1.txt");
        std::fs::write(&out, &rendered).unwrap();
        panic!(
            "{VECTORS_FILE} does not hold the golden vectors (parse: {:?}); the expected file is at {}",
            parsed.as_ref().map(|v| v.len()),
            out.display()
        );
    }
    assert_eq!(vectors::parse(&rendered), Ok(expected));
}

#[test]
fn vectors_file_parser_is_strict() {
    let good = "name: n\ntype: blob\nexpect: reject\nbytes: 00\n";
    assert!(vectors::parse(good).is_ok());
    for bad in [
        "name: n\ntype: blob\nexpect: reject\nbytes: 0\n",
        "name: n\ntype: blob\nexpect: reject\nbytes: 0G\n",
        "name: n\ntype: blob\nexpect: reject\nbytes: 0A\n",
        "type: blob\nname: n\nexpect: reject\nbytes: 00\n",
        "name: n\ntype: blob\nexpect: accept\nbytes: 00\n",
        "name: n\ntype: blob\nexpect: maybe\nbytes: 00\n",
        "name: n\ntype: thing\nexpect: reject\nbytes: 00\n",
    ] {
        assert!(vectors::parse(bad).is_err(), "{bad:?}");
    }
}
