//! Vectors re-derived by hand from the byte layouts and id rules in the
//! design, with no reference to the crate's own encoder or vector file. Each
//! test builds the canonical bytes with the tiny encoder below, computes the
//! expected id directly with BLAKE3, and checks that the crate decodes the
//! bytes, re-encodes them identically, and derives the same id. A second set
//! of tests checks that each one-byte deviation from canonical form is
//! rejected.

use scion_core::{
    BlobId, Changeset, ChangesetId, ChunkId, ChunkList, Frame, FrameKind, Tree, TreeId,
};

fn uvarint(out: &mut Vec<u8>, mut v: u64) {
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

fn svarint(out: &mut Vec<u8>, v: i64) {
    uvarint(out, ((v << 1) ^ (v >> 63)) as u64);
}

fn bytes(out: &mut Vec<u8>, b: &[u8]) {
    uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn ts(out: &mut Vec<u8>, secs: i64, off: i64) {
    svarint(out, secs);
    svarint(out, off);
}

fn kdf(ctx: &str, b: &[u8]) -> [u8; 32] {
    blake3::derive_key(ctx, b)
}

fn fill(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[3] ^ seed)
        .collect()
}

fn frame(kind: u8, id: [u8; 32], codec: u8, raw_len: u64, payload: &[u8]) -> Vec<u8> {
    let mut f = b"SCF1".to_vec();
    f.push(kind);
    f.extend_from_slice(&id);
    f.push(codec);
    uvarint(&mut f, raw_len);
    uvarint(&mut f, payload.len() as u64);
    f.extend_from_slice(payload);
    let d = *blake3::hash(&f).as_bytes();
    f.extend_from_slice(&d);
    f
}

fn check_changeset(b: &[u8]) -> ChangesetId {
    let cs = Changeset::decode(b).expect("canonical changeset decodes");
    assert_eq!(cs.encode(), b, "re-encoding is byte-identical");
    assert_eq!(cs.id().as_bytes(), &kdf("scion v1 changeset", b));
    cs.id()
}

fn check_tree(b: &[u8]) -> TreeId {
    let t = Tree::decode(b).expect("canonical tree decodes");
    assert_eq!(t.encode(), b, "re-encoding is byte-identical");
    assert_eq!(t.id().as_bytes(), &kdf("scion v1 tree", b));
    t.id()
}

// ------------------------------------------------------------------ ids

#[test]
fn v01_blob_ids() {
    for content in [&b""[..], b"hello\n", &fill(10_000, 7)] {
        assert_eq!(
            BlobId::hash(content).as_bytes(),
            &kdf("scion v1 blob", content)
        );
    }
    // Per-kind contexts give different ids for the same bytes.
    let b = b"same bytes";
    let ids = [
        *BlobId::hash(b).as_bytes(),
        *ChunkId::hash(b).as_bytes(),
        *TreeId::hash(b).as_bytes(),
        *ChangesetId::hash(b).as_bytes(),
    ];
    assert_eq!(ids[1], kdf("scion v1 chunk", b));
    assert_eq!(ids[2], kdf("scion v1 tree", b));
    assert_eq!(ids[3], kdf("scion v1 changeset", b));
    for i in 0..4 {
        for j in i + 1..4 {
            assert_ne!(ids[i], ids[j]);
        }
    }
}

#[test]
fn v02_blob_id_pinned_hex() {
    // Independent constant: derive_key("scion v1 blob", "") computed once and
    // pinned, so a change of context string or hash construction is caught
    // even if both sides of the test above changed together.
    let hex = BlobId::hash(b"").to_string();
    let expect: String = kdf("scion v1 blob", b"")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(hex, expect);
    assert_eq!(hex.len(), 64);
    assert_eq!(
        hex,
        "395e5ff2c5ebbdece5384a16c163976f3037d28f4b4b889bdf0c103fcd199bdc"
    );
    assert_eq!(
        BlobId::hash(b"hi").to_string(),
        "ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27"
    );
    assert_eq!(hex.parse::<BlobId>().unwrap(), BlobId::hash(b""));
}

// ------------------------------------------------------------------ trees

#[test]
fn v03_empty_root_tree() {
    let id = check_tree(&[0x01, 0x00]);
    assert_eq!(id.as_bytes(), &kdf("scion v1 tree", &[1, 0]));
}

#[test]
fn v04_tree_every_kind() {
    let blob = kdf("scion v1 blob", b"hello\n");
    let sha: [u8; 32] = [0xab; 32];
    let sub = kdf("scion v1 tree", &[1, 0]);
    let mut b = vec![0x01];
    uvarint(&mut b, 4);
    // Names in byte order: "a" < "bin" < "dir" < "ln".
    bytes(&mut b, b"a");
    b.push(2); // Regular
    b.extend_from_slice(&blob);
    uvarint(&mut b, 6);
    b.extend_from_slice(&sha);
    bytes(&mut b, b"bin");
    b.push(3); // Executable
    b.extend_from_slice(&blob);
    uvarint(&mut b, 300); // two-byte varint
    b.extend_from_slice(&sha);
    bytes(&mut b, b"dir");
    b.push(0); // Tree
    b.extend_from_slice(&sub);
    bytes(&mut b, b"ln");
    b.push(4); // Symlink
    b.extend_from_slice(&blob);
    uvarint(&mut b, 0);
    b.extend_from_slice(&sha);
    check_tree(&b);
}

#[test]
fn v04b_sharded_node() {
    // A directory of more than MAX_TREE_ENTRIES names, all starting "src",
    // one of them exactly "src": P = "src", that entry stays, and the rest
    // become one Shard per next byte ('-' and '_').
    let blob = kdf("scion v1 blob", b"");
    let s1 = kdf("scion v1 tree", b"s1");
    let s2 = kdf("scion v1 tree", b"s2");
    let mut b = vec![0x01];
    uvarint(&mut b, 3);
    bytes(&mut b, b"src");
    b.push(2);
    b.extend_from_slice(&blob);
    uvarint(&mut b, 0);
    b.extend_from_slice(&[0xe3; 32]);
    bytes(&mut b, b"src-");
    b.push(1);
    b.extend_from_slice(&s1);
    bytes(&mut b, b"src_");
    b.push(1);
    b.extend_from_slice(&s2);
    check_tree(&b);
}

#[test]
fn rejects_shard_nodes_not_shaped_by_the_split_rule() {
    let shard = |out: &mut Vec<u8>, name: &[u8]| {
        bytes(out, name);
        out.push(1);
        out.extend_from_slice(&[5; 32]);
    };
    let dir = |out: &mut Vec<u8>, name: &[u8]| {
        bytes(out, name);
        out.push(0);
        out.extend_from_slice(&[6; 32]);
    };
    let node = |parts: &[(&[u8], bool)]| {
        let mut b = vec![0x01];
        uvarint(&mut b, parts.len() as u64);
        for (n, is_shard) in parts {
            if *is_shard {
                shard(&mut b, n);
            } else {
                dir(&mut b, n);
            }
        }
        b
    };
    // Accepted shapes.
    for ok in [
        node(&[(b"a", true), (b"b", true)]),
        node(&[(b"x", false), (b"xa", true)]),
        node(&[(b"x", false), (b"xa", true), (b"xb", true)]),
        node(&[(b".", true), (b"0", true)]),
    ] {
        Tree::decode(&ok).expect("valid sharded node");
    }
    // Rejected shapes.
    for (bad, what) in [
        (node(&[(b"a", true)]), "single shard, no prefix entry"),
        (
            node(&[(b"a", true), (b"bc", true)]),
            "unequal shard name lengths",
        ),
        (node(&[(b"ab", true), (b"cd", true)]), "different prefixes"),
        (
            node(&[(b"a", true), (b"b", false)]),
            "plain entry beside shards",
        ),
        (
            node(&[(b"x", false), (b"xa", true), (b"y", false)]),
            "two plain entries",
        ),
        (
            node(&[(b"xa", true), (b"xb", true), (b"xc", false)]),
            "plain entry not the prefix",
        ),
        (
            node(&[(b"x", false), (b"xab", true)]),
            "shard more than one byte past P",
        ),
    ] {
        assert!(Tree::decode(&bad).is_err(), "{what}");
    }
}

// ------------------------------------------------------------------ changesets

#[test]
fn v05_minimal_changeset() {
    let mut b = vec![0x01, 0x00];
    bytes(&mut b, b"A <a@example.com>");
    ts(&mut b, 0, 0);
    b.extend_from_slice(&[0, 0]); // committer, committer_date absent
    bytes(&mut b, b"");
    b.extend_from_slice(&[0, 0]); // signature, git_headers absent
    uvarint(&mut b, 0);
    check_changeset(&b);
}

#[test]
fn v06_full_changeset() {
    let p1 = kdf("scion v1 changeset", b"p1");
    let p2 = kdf("scion v1 changeset", b"p2");
    let blob = kdf("scion v1 blob", b"x");
    let mut b = vec![0x01];
    uvarint(&mut b, 2);
    b.extend_from_slice(&p1);
    b.extend_from_slice(&p2);
    bytes(&mut b, b"Ann Author <ann@example.com>");
    ts(&mut b, -1_234_567_890, -18_000);
    b.push(1);
    bytes(&mut b, b"scion-server <server@scion>");
    b.push(1);
    ts(&mut b, 1_767_225_600, 86_399);
    bytes(&mut b, &fill(200, 3)); // two-byte length
    b.push(1);
    bytes(&mut b, b"sig");
    b.push(0);
    uvarint(&mut b, 5);
    // Path byte order; "a" Deleted becomes a directory "a/b".
    bytes(&mut b, b"a");
    b.push(0);
    bytes(&mut b, b"a/b");
    b.push(1);
    b.extend_from_slice(&blob);
    uvarint(&mut b, 1);
    bytes(&mut b, b"bin/tool");
    b.push(2);
    b.extend_from_slice(&blob);
    uvarint(&mut b, 16_384); // three-byte varint
    bytes(&mut b, b"link");
    b.push(3);
    b.extend_from_slice(&blob);
    uvarint(&mut b, 127);
    bytes(&mut b, b"z\xff\xfe");
    b.push(0);
    let id = check_changeset(&b);
    let cs = Changeset::decode(&b).unwrap();
    assert_eq!(cs.parts().parents.len(), 2);
    assert_eq!(cs.parts().parents[0].as_bytes(), &p1, "first parent first");
    assert_eq!(cs.parts().author_date.seconds(), -1_234_567_890);
    assert_eq!(cs.parts().author_date.offset_seconds(), -18_000);
    assert_eq!(id, cs.id());
}

#[test]
fn v07_imported_changeset_with_git_headers() {
    let mut b = vec![0x01, 0x00];
    bytes(&mut b, b"");
    ts(&mut b, 0, 0);
    b.push(1);
    bytes(&mut b, b"");
    b.push(1);
    ts(&mut b, 0, 0);
    bytes(&mut b, b"msg\n");
    b.push(0);
    b.push(1);
    bytes(
        &mut b,
        b"author x <y> 1 +0000\ncommitter x <y> 1 +0000\n\nmsg\n",
    );
    uvarint(&mut b, 0);
    check_changeset(&b);
}

// ------------------------------------------------------------------ frames

#[test]
fn v08_blob_frame_raw() {
    let content = b"hello\n";
    let id = kdf("scion v1 blob", content);
    let f = frame(1, id, 0, 6, content);
    let d = Frame::decode(&f).expect("frame decodes");
    assert_eq!(d.header().kind, FrameKind::Blob);
    assert_eq!(d.header().id, id);
    assert_eq!(d.payload(), content);
    assert_eq!(
        Frame::encode(FrameKind::Blob, id, d.header().codec, 6, content).unwrap(),
        f
    );
}

#[test]
fn v09_tree_frame() {
    let tree = [0x01u8, 0x00];
    let id = kdf("scion v1 tree", &tree);
    let f = frame(3, id, 0, 2, &tree);
    let d = Frame::decode(&f).expect("tree frame decodes");
    assert_eq!(d.header().kind, FrameKind::Tree);
    assert_eq!(d.payload(), &tree);
}

#[test]
fn v10_chunk_list_frame() {
    let chunk_bytes = 4usize << 20;
    let blob = fill(chunk_bytes + 1, 9);
    let c0 = kdf("scion v1 chunk", &blob[..chunk_bytes]);
    let c1 = kdf("scion v1 chunk", &blob[chunk_bytes..]);
    let id = kdf("scion v1 blob", &blob);
    let mut payload = Vec::new();
    uvarint(&mut payload, 2);
    payload.extend_from_slice(&c0);
    payload.extend_from_slice(&c1);
    let f = frame(4, id, 0, blob.len() as u64, &payload);
    let d = Frame::decode(&f).expect("chunk list frame decodes");
    assert_eq!(d.header().kind, FrameKind::ChunkList);
    assert_eq!(d.header().raw_len, blob.len() as u64);
    let list = ChunkList::decode(d.payload()).unwrap();
    assert_eq!(
        list.chunks,
        vec![ChunkId::from_bytes(c0), ChunkId::from_bytes(c1)]
    );
    assert_eq!(BlobId::hash(&blob).as_bytes(), &id);

    // A full-size chunk frame is valid; one byte more is not.
    let chunk = frame(2, c0, 0, chunk_bytes as u64, &blob[..chunk_bytes]);
    Frame::decode(&chunk).expect("CHUNK_BYTES chunk frame decodes");
    let big = frame(1, id, 0, blob.len() as u64, &blob);
    assert!(Frame::decode(&big).is_err(), "blob frame over CHUNK_BYTES");
    // A chunk list for a blob that fits in one chunk is not canonical.
    let small = frame(4, id, 0, chunk_bytes as u64, &payload);
    assert!(Frame::decode(&small).is_err(), "chunk list for small blob");
    // The chunk count must match the blob length.
    let mut one = Vec::new();
    uvarint(&mut one, 1);
    one.extend_from_slice(&c0);
    let short = frame(4, id, 0, blob.len() as u64, &one);
    assert!(Frame::decode(&short).is_err(), "chunk count mismatch");
    // A chunk list frame uses codec raw.
    let zstd = frame(4, id, 1, blob.len() as u64, &payload);
    assert!(Frame::decode(&zstd).is_err(), "chunk list with codec zstd");
}

// ------------------------------------------------------------------ rejection

fn minimal_changeset() -> Vec<u8> {
    let mut b = vec![0x01, 0x00];
    bytes(&mut b, b"A <a>");
    ts(&mut b, 0, 0);
    b.extend_from_slice(&[0, 0]);
    bytes(&mut b, b"");
    b.extend_from_slice(&[0, 0]);
    uvarint(&mut b, 0);
    b
}

fn changeset_with(changes: &[(&[u8], u8)]) -> Vec<u8> {
    let mut b = minimal_changeset();
    b.pop();
    uvarint(&mut b, changes.len() as u64);
    for (p, k) in changes {
        bytes(&mut b, p);
        b.push(*k);
        if *k != 0 {
            b.extend_from_slice(&[7; 32]);
            uvarint(&mut b, 1);
        }
    }
    b
}

#[test]
fn rejects_noncanonical_changesets() {
    let good = minimal_changeset();
    Changeset::decode(&good).unwrap();

    let mut v = good.clone();
    v[0] = 0x02;
    assert!(Changeset::decode(&v).is_err(), "version 2");
    v[0] = 0x00;
    assert!(Changeset::decode(&v).is_err(), "version 0");

    let mut v = good.clone();
    v.push(0);
    assert!(Changeset::decode(&v).is_err(), "trailing byte");
    assert!(
        Changeset::decode(&good[..good.len() - 1]).is_err(),
        "truncated"
    );

    // Non-minimal parent count: 0x80 0x00 for zero.
    let mut v = vec![0x01, 0x80, 0x00];
    v.extend_from_slice(&good[2..]);
    assert!(Changeset::decode(&v).is_err(), "non-minimal varint");

    // Non-minimal author length: 0x85 0x00 for 5.
    let mut v = vec![0x01, 0x00, 0x85, 0x00];
    v.extend_from_slice(&good[3..]);
    assert!(Changeset::decode(&v).is_err(), "non-minimal bytes length");

    // Offset at the bound.
    for off in [86_400i64, -86_400] {
        let mut v = vec![0x01, 0x00];
        bytes(&mut v, b"A <a>");
        ts(&mut v, 0, off);
        v.extend_from_slice(&good[2 + 6 + 2..]);
        assert!(Changeset::decode(&v).is_err(), "offset {off}");
    }

    // Option tag 2; committer without committer_date and vice versa.
    let at = 2 + 6 + 2; // version, count, author bytes, timestamp
    for (tags, what) in [
        (vec![2u8, 0], "opt tag 2"),
        (vec![0, 1, 0, 0], "date without committer"),
    ] {
        let mut v = good[..at].to_vec();
        v.extend_from_slice(&tags);
        v.extend_from_slice(&good[at + 2..]);
        assert!(Changeset::decode(&v).is_err(), "{what}");
    }
    let mut v = good[..at].to_vec();
    v.push(1);
    bytes(&mut v, b"C <c>");
    v.push(0);
    v.extend_from_slice(&good[at + 2..]);
    assert!(Changeset::decode(&v).is_err(), "committer without date");

    // Signature and git_headers option tags.
    let mut v = good.clone();
    let n = v.len();
    v[n - 3] = 2;
    assert!(Changeset::decode(&v).is_err(), "signature tag 2");
    let mut v = good.clone();
    v[n - 2] = 2;
    assert!(Changeset::decode(&v).is_err(), "git_headers tag 2");

    // File change rules.
    Changeset::decode(&changeset_with(&[
        (b"a", 1),
        (b"b", 2),
        (b"c", 3),
        (b"d", 0),
    ]))
    .unwrap();
    assert!(
        Changeset::decode(&changeset_with(&[(b"b", 1), (b"a", 1)])).is_err(),
        "unsorted"
    );
    assert!(
        Changeset::decode(&changeset_with(&[(b"a", 1), (b"a", 1)])).is_err(),
        "duplicate"
    );
    assert!(
        Changeset::decode(&changeset_with(&[(b"a", 4)])).is_err(),
        "kind 4"
    );
    for bad in [
        &b""[..],
        b"/a",
        b"a/",
        b"a//b",
        b".",
        b"..",
        b"a/./b",
        b"a/../b",
        b"a\0b",
    ] {
        assert!(
            Changeset::decode(&changeset_with(&[(bad, 1)])).is_err(),
            "path {bad:?}"
        );
    }
    // Names that merely contain dots are fine.
    Changeset::decode(&changeset_with(&[(b".a/..b/...", 1)])).unwrap();
}

#[test]
fn rejects_noncanonical_trees() {
    let entry = |out: &mut Vec<u8>, name: &[u8], kind: u8| {
        bytes(out, name);
        out.push(kind);
        out.extend_from_slice(&[3; 32]);
        if kind >= 2 {
            uvarint(out, 1);
            out.extend_from_slice(&[4; 32]);
        }
    };
    let build = |es: &[(&[u8], u8)]| {
        let mut b = vec![0x01];
        uvarint(&mut b, es.len() as u64);
        for (n, k) in es {
            entry(&mut b, n, *k);
        }
        b
    };
    Tree::decode(&build(&[(b"a", 0), (b"b", 2)])).unwrap();
    assert!(
        Tree::decode(&build(&[(b"b", 0), (b"a", 2)])).is_err(),
        "unsorted"
    );
    assert!(
        Tree::decode(&build(&[(b"a", 0), (b"a", 2)])).is_err(),
        "duplicate"
    );
    assert!(Tree::decode(&build(&[(b"a", 5)])).is_err(), "kind 5");
    for bad in [&b""[..], b"a/b", b"a\0", b".", b".."] {
        assert!(Tree::decode(&build(&[(bad, 2)])).is_err(), "name {bad:?}");
    }
    let mut v = build(&[(b"a", 2)]);
    v.push(0);
    assert!(Tree::decode(&v).is_err(), "trailing");
    let mut v = build(&[]);
    v[0] = 2;
    assert!(Tree::decode(&v).is_err(), "version");
    // Count over MAX_TREE_ENTRIES is rejected before reading entries.
    let mut v = vec![0x01];
    uvarint(&mut v, 4097);
    assert!(Tree::decode(&v).is_err(), "too many entries");
    // Tree entries carry no size/sha: a Tree-kind entry followed by size
    // bytes leaves trailing bytes.
    let mut v = build(&[(b"a", 0)]);
    uvarint(&mut v, 1);
    v.extend_from_slice(&[4; 32]);
    assert!(Tree::decode(&v).is_err(), "size on Tree entry");
}

#[test]
fn rejects_noncanonical_frames() {
    let content = b"hello\n";
    let id = kdf("scion v1 blob", content);
    let good = frame(1, id, 0, 6, content);
    Frame::decode(&good).unwrap();

    let mut v = good.clone();
    v[0] = b'X';
    assert!(Frame::decode(&v).is_err(), "magic");
    for kind in [0u8, 5] {
        let v = frame(kind, id, 0, 6, content);
        assert!(Frame::decode(&v).is_err(), "kind {kind}");
    }
    let v = frame(1, id, 2, 6, content);
    assert!(Frame::decode(&v).is_err(), "codec 2");
    let v = frame(1, id, 0, 5, content);
    assert!(
        Frame::decode(&v).is_err(),
        "raw codec with raw_len != payload_len"
    );
    let mut v = good.clone();
    let n = v.len();
    v[n - 1] ^= 1;
    assert!(Frame::decode(&v).is_err(), "digest");
    let mut v = good.clone();
    v[40] ^= 1; // a payload byte changes; digest no longer matches
    assert!(Frame::decode(&v).is_err(), "payload tamper");
    let mut v = good.clone();
    v.push(0);
    assert!(Frame::decode(&v).is_err(), "trailing");
    assert!(Frame::decode(&good[..good.len() - 1]).is_err(), "truncated");

    // Non-minimal raw length varint (6 as 0x86 0x00), digest recomputed.
    let mut f = b"SCF1".to_vec();
    f.push(1);
    f.extend_from_slice(&id);
    f.push(0);
    f.extend_from_slice(&[0x86, 0x00]);
    f.push(6);
    f.extend_from_slice(content);
    let d = *blake3::hash(&f).as_bytes();
    f.extend_from_slice(&d);
    assert!(Frame::decode(&f).is_err(), "non-minimal raw_len");

    // Chunk list payload trailing / non-minimal count.
    let mut p = Vec::new();
    uvarint(&mut p, 1);
    p.extend_from_slice(&[1; 32]);
    ChunkList::decode(&p).unwrap();
    let mut t = p.clone();
    t.push(0);
    assert!(ChunkList::decode(&t).is_err(), "chunk list trailing");
    let mut nm = vec![0x81, 0x00];
    nm.extend_from_slice(&[1; 32]);
    assert!(
        ChunkList::decode(&nm).is_err(),
        "chunk list non-minimal count"
    );
}
