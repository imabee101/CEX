//! Independent M1 review vectors.
//!
//! Every expected byte string here is assembled by hand from the byte layouts in
//! the design (not from the encoder), and every expected id is computed directly
//! with `blake3::derive_key` and the per-kind context string. The implementation
//! must accept exactly these bytes, re-encode them byte-identically, and produce
//! the same id; and it must reject every non-canonical sibling.

use scion_core::{BlobId, Changeset, ChunkId, Frame, Tree};

fn dk(context: &str, bytes: &[u8]) -> [u8; 32] {
    blake3::derive_key(context, bytes)
}

/// Minimal unsigned LEB128, written independently of the crate.
fn uv(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn sv(v: i64, out: &mut Vec<u8>) {
    uv(((v << 1) ^ (v >> 63)) as u64, out);
}

fn by(b: &[u8], out: &mut Vec<u8>) {
    uv(b.len() as u64, out);
    out.extend_from_slice(b);
}

fn sha(seed: u8) -> [u8; 32] {
    [seed; 32]
}

fn accept_changeset(bytes: &[u8]) {
    let cs = Changeset::decode(bytes).expect("hand-built canonical changeset must decode");
    assert_eq!(cs.encode(), bytes, "re-encoding must be byte-identical");
    assert_eq!(cs.id().as_bytes(), &dk("scion v1 changeset", bytes));
}

fn accept_tree(bytes: &[u8]) -> [u8; 32] {
    let t = Tree::decode(bytes).expect("hand-built canonical tree must decode");
    assert_eq!(t.encode(), bytes, "re-encoding must be byte-identical");
    let id = dk("scion v1 tree", bytes);
    assert_eq!(t.id().as_bytes(), &id);
    id
}

#[test]
fn v01_blob_and_chunk_ids() {
    for data in [&b""[..], b"hello\n", &[0u8; 9000][..]] {
        assert_eq!(BlobId::hash(data).as_bytes(), &dk("scion v1 blob", data));
        assert_eq!(ChunkId::hash(data).as_bytes(), &dk("scion v1 chunk", data));
        assert_ne!(
            BlobId::hash(data).as_bytes(),
            ChunkId::hash(data).as_bytes()
        );
    }
    // A known BLAKE3 derive_key output pins the primitive itself: an id is never
    // the plain hash of the bytes.
    assert_ne!(BlobId::hash(b"").as_bytes(), blake3::hash(b"").as_bytes());
}

#[test]
fn v02_empty_root_tree() {
    accept_tree(&[0x01, 0x00]);
}

#[test]
fn v03_tree_every_kind() {
    let mut b = vec![0x01];
    uv(5, &mut b);
    // Name byte order: "a" < "b" < "c" < "d" < "e".
    by(b"a", &mut b);
    b.push(0); // Tree
    b.extend_from_slice(&[0x11; 32]);
    by(b"b", &mut b);
    b.push(2); // Regular
    b.extend_from_slice(&[0x22; 32]);
    uv(300, &mut b); // two-byte varint
    b.extend_from_slice(&sha(1));
    by(b"c", &mut b);
    b.push(3); // Executable
    b.extend_from_slice(&[0x33; 32]);
    uv(0, &mut b);
    b.extend_from_slice(&sha(2));
    by(b"d", &mut b);
    b.push(4); // Symlink
    b.extend_from_slice(&[0x44; 32]);
    uv(1, &mut b);
    b.extend_from_slice(&sha(3));
    by("é".as_bytes(), &mut b);
    b.push(2);
    b.extend_from_slice(&[0x55; 32]);
    uv(u64::MAX, &mut b);
    b.extend_from_slice(&sha(4));
    accept_tree(&b);
}

fn regular_entry(name: &[u8], b: &mut Vec<u8>) {
    by(name, b);
    b.push(2);
    b.extend_from_slice(&dk("scion v1 blob", name));
    uv(name.len() as u64, b);
    b.extend_from_slice(&sha(9));
}

#[test]
fn v04_sharded_directory() {
    // 4,097 names f0000..f4096: longest common prefix "f", groups by the next byte
    // '0'..'4'. Each shard holds full names and is small enough to be a leaf.
    let names: Vec<Vec<u8>> = (0..4097).map(|i| format!("f{i:04}").into_bytes()).collect();
    let mut shard_ids = Vec::new();
    for g in b'0'..=b'4' {
        let group: Vec<&Vec<u8>> = names.iter().filter(|n| n[1] == g).collect();
        let mut b = vec![0x01];
        uv(group.len() as u64, &mut b);
        for n in group {
            regular_entry(n, &mut b);
        }
        shard_ids.push((g, accept_tree(&b)));
    }
    let mut root = vec![0x01];
    uv(5, &mut root);
    for (g, id) in &shard_ids {
        by(&[b'f', *g], &mut root);
        root.push(1); // Shard
        root.extend_from_slice(id);
    }
    accept_tree(&root);

    // A shard node whose names do not all share the length |P|+1 breaks the rule.
    let mut bad = vec![0x01];
    uv(2, &mut bad);
    by(b"f0", &mut bad);
    bad.push(1);
    bad.extend_from_slice(&shard_ids[0].1);
    by(b"g12", &mut bad);
    bad.push(1);
    bad.extend_from_slice(&shard_ids[1].1);
    assert!(
        Tree::decode(&bad).is_err(),
        "inconsistent shard names must be rejected"
    );
}

fn minimal_changeset() -> Vec<u8> {
    let mut b = vec![0x01];
    uv(0, &mut b); // parents
    by(b"A <a@b>", &mut b);
    sv(0, &mut b);
    sv(0, &mut b);
    b.push(0); // committer
    b.push(0); // committer_date
    by(b"", &mut b); // message
    b.push(0); // signature
    b.push(0); // git_headers
    uv(0, &mut b); // changes
    b
}

#[test]
fn v05_minimal_changeset() {
    let b = minimal_changeset();
    assert_eq!(
        b,
        [
            1, 0, 7, b'A', b' ', b'<', b'a', b'@', b'b', b'>', 0, 0, 0, 0, 0, 0, 0, 0
        ]
    );
    accept_changeset(&b);
}

fn full_changeset(seconds: i64, offset: i64) -> Vec<u8> {
    let mut b = vec![0x01];
    uv(2, &mut b);
    b.extend_from_slice(&[0xaa; 32]);
    b.extend_from_slice(&[0x01; 32]); // order is first-parent first, not sorted
    by(b"Ada Lovelace <ada@example.com>", &mut b);
    sv(seconds, &mut b);
    sv(offset, &mut b);
    b.push(1);
    by(b"Bot <bot@example.com>", &mut b);
    b.push(1);
    sv(1_700_000_000, &mut b);
    sv(-18_000, &mut b);
    by(b"subject\n\nbody\n", &mut b);
    b.push(1);
    by(b"-----BEGIN SIG-----", &mut b);
    b.push(1);
    by(b"author x\ncommitter y\n\nmsg", &mut b);
    uv(5, &mut b);
    // Path byte order: "a" < "a/b" is a prefix pair, allowed only as Deleted "a"
    // becoming a directory.
    by(b"a", &mut b);
    b.push(0);
    by(b"a/b", &mut b);
    b.push(1);
    b.extend_from_slice(&[0x10; 32]);
    uv(128, &mut b);
    by(b"bin/run", &mut b);
    b.push(2);
    b.extend_from_slice(&[0x20; 32]);
    uv(0, &mut b);
    by(b"link", &mut b);
    b.push(3);
    b.extend_from_slice(&[0x30; 32]);
    uv(7, &mut b);
    by(b"z", &mut b);
    b.push(0);
    b
}

#[test]
fn v06_full_changeset() {
    accept_changeset(&full_changeset(1_234_567_890, 3_600));
}

#[test]
fn v07_negative_and_extreme_timestamps() {
    let b = full_changeset(-1, -3_600);
    // seconds -1 -> zigzag 1; offset -3600 -> zigzag 7199 -> 0x9f 0x38.
    let at = 1 + 1 + 64 + 1 + 30;
    assert_eq!(&b[at..at + 3], &[0x01, 0x9f, 0x38]);
    accept_changeset(&b);
    accept_changeset(&full_changeset(i64::MIN, 86_399));
    accept_changeset(&full_changeset(i64::MAX, -86_399));
}

#[test]
fn v08_raw_blob_frame() {
    let payload = b"hello\n";
    let id = dk("scion v1 blob", payload);
    let mut f = b"SCF1".to_vec();
    f.push(1);
    f.extend_from_slice(&id);
    f.push(0);
    uv(6, &mut f);
    uv(6, &mut f);
    f.extend_from_slice(payload);
    let digest = *blake3::hash(&f).as_bytes();
    f.extend_from_slice(&digest);
    let fr = Frame::decode(&f).expect("hand-built frame must decode");
    assert_eq!(fr.payload(), payload);
    assert_eq!(fr.header().id, id);
    assert_eq!(fr.header().raw_len, 6);

    // Flip any one byte and the frame is rejected.
    for i in 0..f.len() {
        let mut g = f.clone();
        g[i] ^= 0x01;
        assert!(Frame::decode(&g).is_err(), "flipped byte {i} accepted");
    }
    // Truncation and trailing bytes are rejected.
    assert!(Frame::decode(&f[..f.len() - 1]).is_err());
    let mut t = f.clone();
    t.push(0);
    assert!(Frame::decode(&t).is_err());
}

#[test]
fn v09_tree_frame_and_chunk_list_frame() {
    let tree = [0x01u8, 0x00];
    let id = dk("scion v1 tree", &tree);
    let mut f = b"SCF1".to_vec();
    f.push(3);
    f.extend_from_slice(&id);
    f.push(0);
    uv(2, &mut f);
    uv(2, &mut f);
    f.extend_from_slice(&tree);
    let d = *blake3::hash(&f).as_bytes();
    f.extend_from_slice(&d);
    assert!(Frame::decode(&f).is_ok());

    // Chunk list for a blob of CHUNK_BYTES + 1 raw bytes: two chunks.
    let raw_len = (4u64 << 20) + 1;
    let mut payload = Vec::new();
    uv(2, &mut payload);
    payload.extend_from_slice(&[0x61; 32]);
    payload.extend_from_slice(&[0x62; 32]);
    let mut f = b"SCF1".to_vec();
    f.push(4);
    f.extend_from_slice(&[0x77; 32]);
    f.push(0);
    uv(raw_len, &mut f);
    uv(payload.len() as u64, &mut f);
    f.extend_from_slice(&payload);
    let d = *blake3::hash(&f).as_bytes();
    f.extend_from_slice(&d);
    let fr = Frame::decode(&f).expect("chunk-list frame must decode");
    assert_eq!(fr.header().raw_len, raw_len);
}

#[test]
fn v10_noncanonical_changesets_rejected() {
    let good = minimal_changeset();
    accept_changeset(&good);
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    let mut v = good.clone();
    v[0] = 0x02;
    cases.push(("version", v));
    let mut v = good.clone();
    v.push(0);
    cases.push(("trailing byte", v));
    cases.push(("truncated", good[..good.len() - 1].to_vec()));
    // Non-minimal varint for the parent count: 0x80 0x00.
    let mut v = vec![0x01, 0x80, 0x00];
    v.extend_from_slice(&good[2..]);
    cases.push(("non-minimal uvarint", v));
    // Non-minimal svarint seconds.
    let mut v = good[..10].to_vec();
    v.extend_from_slice(&[0x80, 0x00]);
    v.extend_from_slice(&good[11..]);
    cases.push(("non-minimal svarint", v));
    // opt tag 2.
    let mut v = good.clone();
    v[12] = 0x02;
    cases.push(("opt tag", v));
    // committer present without committer_date.
    let mut v = good[..12].to_vec();
    v.push(1);
    by(b"C <c@d>", &mut v);
    v.push(0);
    v.extend_from_slice(&good[14..]);
    cases.push(("committer without date", v));
    // offset of exactly 86,400.
    let mut v = good[..11].to_vec();
    sv(86_400, &mut v);
    v.extend_from_slice(&good[12..]);
    cases.push(("offset bound", v));
    // Author length limits are not decoder rules: a server rewrite of an imported
    // changeset drops git_headers but keeps its verbatim author, so they are
    // checked by CreateChangeset, not here.
    for (what, bytes) in cases {
        assert!(Changeset::decode(&bytes).is_err(), "{what} accepted");
    }

    // Change-list violations, built on the tail of the minimal changeset.
    let head = &good[..good.len() - 1];
    let change_cases: Vec<(&str, Vec<u8>)> = vec![
        ("unsorted", {
            let mut v = Vec::new();
            uv(2, &mut v);
            by(b"b", &mut v);
            v.push(0);
            by(b"a", &mut v);
            v.push(0);
            v
        }),
        ("duplicate", {
            let mut v = Vec::new();
            uv(2, &mut v);
            by(b"a", &mut v);
            v.push(0);
            by(b"a", &mut v);
            v.push(0);
            v
        }),
        ("bad kind", {
            let mut v = Vec::new();
            uv(1, &mut v);
            by(b"a", &mut v);
            v.push(4);
            v
        }),
        ("dot component", {
            let mut v = Vec::new();
            uv(1, &mut v);
            by(b"a/./b", &mut v);
            v.push(0);
            v
        }),
        ("empty component", {
            let mut v = Vec::new();
            uv(1, &mut v);
            by(b"a//b", &mut v);
            v.push(0);
            v
        }),
        ("nul", {
            let mut v = Vec::new();
            uv(1, &mut v);
            by(b"a\0", &mut v);
            v.push(0);
            v
        }),
    ];
    for (what, tail) in change_cases {
        let mut v = head.to_vec();
        v.extend_from_slice(&tail);
        assert!(Changeset::decode(&v).is_err(), "{what} accepted");
    }
}

#[test]
fn v11_noncanonical_trees_rejected() {
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    cases.push(("trailing", vec![0x01, 0x00, 0x00]));
    cases.push(("version", vec![0x02, 0x00]));
    cases.push(("non-minimal count", vec![0x01, 0x80, 0x00]));
    let mut two = vec![0x01];
    uv(2, &mut two);
    regular_entry(b"b", &mut two);
    regular_entry(b"a", &mut two);
    cases.push(("unsorted", two));
    let mut dup = vec![0x01];
    uv(2, &mut dup);
    regular_entry(b"a", &mut dup);
    regular_entry(b"a", &mut dup);
    cases.push(("duplicate", dup));
    let mut slash = vec![0x01];
    uv(1, &mut slash);
    regular_entry(b"a/b", &mut slash);
    cases.push(("slash in name", slash));
    let mut dot = vec![0x01];
    uv(1, &mut dot);
    regular_entry(b"..", &mut dot);
    cases.push(("dotdot name", dot));
    let mut kind = vec![0x01];
    uv(1, &mut kind);
    by(b"a", &mut kind);
    kind.push(5);
    kind.extend_from_slice(&[0; 32]);
    cases.push(("bad kind", kind));
    // A Tree entry carrying size and sha256 leaves trailing bytes.
    let mut t = vec![0x01];
    uv(1, &mut t);
    by(b"a", &mut t);
    t.push(0);
    t.extend_from_slice(&[0; 32]);
    uv(1, &mut t);
    t.extend_from_slice(&[0; 32]);
    cases.push(("tree entry with size", t));
    // A sub-tree node (non-root) can be empty only as the root: a single-entry
    // tree pointing at the empty tree is still a valid encoding, so only the
    // purely local rules are checked here.
    for (what, bytes) in cases {
        assert!(Tree::decode(&bytes).is_err(), "{what} accepted");
    }
}
