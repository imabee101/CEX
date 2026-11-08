//! Review vectors: bytes and ids worked out from the byte layouts and id
//! rules in the design alone (in a separate program, before reading the
//! crate's encoder or vector file), pinned here as hex. Each test builds the
//! same value through the crate's public API and checks that it encodes to
//! the pinned bytes, hashes to the pinned id, and decodes back to itself.
//! Sharded directories are split by an independent implementation of the
//! split rule below, and every node it produces must be accepted.

use std::collections::BTreeMap;

use scion_core::{
    BlobId, Changeset, ChangesetId, ChangesetParts, ChunkId, ChunkList, Codec, Committer,
    EntryTarget, FileChange, FileKind, Frame, FrameKind, RepoPath, Timestamp, Tree, TreeEntry,
    TreeId,
};
use sha2::Digest;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn sha(b: &[u8]) -> [u8; 32] {
    sha2::Sha256::digest(b).into()
}

fn file(content: &[u8], kind: FileKind) -> EntryTarget {
    EntryTarget::File {
        kind,
        blob: BlobId::hash(content),
        size: content.len() as u64,
        sha256: sha(content),
    }
}

fn tree(mut entries: Vec<(Vec<u8>, EntryTarget)>) -> Tree {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let t = Tree::new(
        entries
            .into_iter()
            .map(|(name, target)| TreeEntry { name, target })
            .collect(),
    )
    .unwrap();
    assert_eq!(Tree::decode(&t.encode()).unwrap(), t);
    t
}

fn ts(s: i64, o: i32) -> Timestamp {
    Timestamp::new(s, o).unwrap()
}

#[test]
fn r01_blob_and_chunk_ids() {
    for (content, id) in [
        (
            &b""[..],
            "395e5ff2c5ebbdece5384a16c163976f3037d28f4b4b889bdf0c103fcd199bdc",
        ),
        (
            b"hello\n",
            "51e544b6f36c847592ac0fcfe1bb7fb5de6076d81868227fdc7c34f777b78eb5",
        ),
        (
            b"\x00\x01\xff",
            "e2bcc38dbc6a2c2f742b11ff47833a4f1bce08baab11596eff56673ea6fe69fe",
        ),
    ] {
        assert_eq!(hex(BlobId::hash(content).as_bytes()), id);
    }
    assert_eq!(
        hex(ChunkId::hash(b"abc").as_bytes()),
        "70be81d82dcb201b6aaa98f6bf855415f46738b5bd99c39d14993f2a9376b0b0"
    );
}

#[test]
fn r02_empty_root_tree() {
    let t = tree(vec![]);
    assert_eq!(hex(&t.encode()), "0100");
    assert_eq!(
        hex(t.id().as_bytes()),
        "7c007800ec20b9f58d356d2136b30da8a2194d7e0bdc697c4b74d8b592f354e4"
    );
}

#[test]
fn r03_one_file_tree() {
    let t = tree(vec![(b"a".to_vec(), file(b"hello\n", FileKind::Regular))]);
    assert_eq!(
        hex(&t.encode()),
        "010101610251e544b6f36c847592ac0fcfe1bb7fb5de6076d81868227fdc7c34f777b78eb5\
         065891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
    );
    assert_eq!(
        hex(t.id().as_bytes()),
        "ac9d7a032410f8de147e413130ad7bdf6ee2473a309825c59fd30725f57a1dd1"
    );
}

#[test]
fn r04_mixed_kind_tree() {
    let sub = tree(vec![(b"a".to_vec(), file(b"hello\n", FileKind::Regular))]).id();
    let t = tree(vec![
        (b"z".to_vec(), file(b"target", FileKind::Symlink)),
        (b"B".to_vec(), EntryTarget::Tree(sub)),
        (b"run.sh".to_vec(), file(&[b'x'; 300], FileKind::Executable)),
        (b"\xff\xfe".to_vec(), file(b"", FileKind::Regular)),
    ]);
    assert_eq!(
        hex(t.id().as_bytes()),
        "17fd5461cd929bb5d331e8b0f550226e471fca0fd3860f61eb79fff9f524d522"
    );
}

fn minimal_changeset() -> Changeset {
    let mut p = ChangesetParts::default();
    p.author = b"A <a@x>".to_vec();
    Changeset::new(p).unwrap()
}

fn check_changeset(c: &Changeset, id: &str) {
    assert_eq!(hex(c.id().as_bytes()), id);
    assert_eq!(&Changeset::decode(&c.encode()).unwrap(), c);
}

#[test]
fn r05_minimal_changeset() {
    let c = minimal_changeset();
    assert_eq!(hex(&c.encode()), "01000741203c6140783e0000000000000000");
    check_changeset(
        &c,
        "55d134602ecbc2061ea243f2e045efb532fc6f3be392dbb9349f153b1f6c26ba",
    );
}

#[test]
fn r06_full_changeset() {
    let mut p = ChangesetParts::default();
    p.parents = vec![
        ChangesetId::from_bytes([0xab; 32]),
        minimal_changeset().id(),
    ];
    p.author = b"Ada Lovelace <ada@example.com>".to_vec();
    p.author_date = ts(-1, -18_000);
    p.committer = Some(Committer {
        ident: b"scion <server@scion>".to_vec(),
        date: ts(1_700_000_000, 19_800),
    });
    p.message = vec![b'm'; 200];
    p.signature = Some(b"sig".to_vec());
    p.git_headers = Some(b"author x\n".to_vec());
    let f = |kind, content: &[u8]| FileChange::File {
        kind,
        blob: BlobId::hash(content),
        size: content.len() as u64,
    };
    let mut m = BTreeMap::new();
    m.insert(
        RepoPath::new("src/main.rs").unwrap(),
        f(FileKind::Regular, b"fn main(){}"),
    );
    m.insert(RepoPath::new("old").unwrap(), FileChange::Deleted);
    m.insert(
        RepoPath::new("bin/tool").unwrap(),
        f(FileKind::Executable, &[b'x'; 300]),
    );
    m.insert(RepoPath::new("link").unwrap(), f(FileKind::Symlink, b"src"));
    p.file_changes = m;
    check_changeset(
        &Changeset::new(p).unwrap(),
        "21a2f67e466bdf53151fe7a6c89b416fbcfdcbd28a2ef1b1f8db55bdad85379b",
    );
}

#[test]
fn r07_extreme_timestamps() {
    let mut p = ChangesetParts::default();
    p.author = b"A <a@x>".to_vec();
    p.author_date = ts(i64::MIN, -86_399);
    p.committer = Some(Committer {
        ident: b"C <c@x>".to_vec(),
        date: ts(i64::MAX, 86_399),
    });
    p.message = b"x".to_vec();
    p.file_changes
        .insert(RepoPath::new("d").unwrap(), FileChange::Deleted);
    let c = Changeset::new(p).unwrap();
    assert_eq!(
        hex(&c.encode()),
        "01000741203c6140783effffffffffffffffff01fdc50a010743203c6340783e01feffffffff\
         ffffffff01fec50a0178000001016400"
    );
    check_changeset(
        &c,
        "8607e53291a453835ae3805c37c944eb2ba9e61eb0a471837fabf96ca41d16aa",
    );
}

#[test]
fn r08_raw_blob_and_tree_frames() {
    let hello = BlobId::hash(b"hello\n");
    let f = Frame::encode(
        FrameKind::Blob,
        *hello.as_bytes(),
        Codec::Raw,
        6,
        b"hello\n",
    )
    .unwrap();
    let want = "534346310151e544b6f36c847592ac0fcfe1bb7fb5de6076d81868227fdc7c34f777b78eb50006\
                0668656c6c6f0a60c18d7fb852d91e6f5fead4fa7f153fe74b933de2dfb05983b912d1a3a7b629";
    assert_eq!(hex(&f), want);
    assert_eq!(Frame::decode(&unhex(want)).unwrap().payload(), b"hello\n");

    let empty = tree(vec![]);
    let f = Frame::encode(
        FrameKind::Tree,
        *empty.id().as_bytes(),
        Codec::Raw,
        2,
        &empty.encode(),
    )
    .unwrap();
    assert_eq!(
        hex(&f),
        "53434631037c007800ec20b9f58d356d2136b30da8a2194d7e0bdc697c4b74d8b592f354e40002\
         0201007910780432e543a2e4533416304ccaa1680b4f9e49b6c6aefb81cf46c34e9073"
    );
}

#[test]
fn r09_chunk_list_frame() {
    let blob: Vec<u8> = (0..(4 * 1024 * 1024 + 1))
        .map(|i| (i % 251) as u8)
        .collect();
    let id = BlobId::hash(&blob);
    assert_eq!(
        hex(id.as_bytes()),
        "848aeb6a9d92deefebb270b1f511e754d927e008a3ec5d5397ddcebb455e3b64"
    );
    let chunks: Vec<ChunkId> = blob.chunks(4 << 20).map(ChunkId::hash).collect();
    assert_eq!(
        hex(chunks[0].as_bytes()),
        "ed13a13d40f30d0f8cabf99fe983dd81899d6e95c2bd5ab4bc9445adff680e7f"
    );
    assert_eq!(
        hex(chunks[1].as_bytes()),
        "bb47928d3ccb6bd7c2122b7e4ee4f8f0c1a370d698e5ef9e25a4b68326305d22"
    );
    let payload = ChunkList { chunks }.encode();
    let f = Frame::encode(
        FrameKind::ChunkList,
        *id.as_bytes(),
        Codec::Raw,
        blob.len() as u64,
        &payload,
    )
    .unwrap();
    assert_eq!(
        hex(&f),
        "5343463104848aeb6a9d92deefebb270b1f511e754d927e008a3ec5d5397ddcebb455e3b6400\
         818080024102ed13a13d40f30d0f8cabf99fe983dd81899d6e95c2bd5ab4bc9445adff680e7f\
         bb47928d3ccb6bd7c2122b7e4ee4f8f0c1a370d698e5ef9e25a4b68326305d2285b17cf05c66\
         c02e1242a99a8e3ff10fc4dbb61a7883dd1178c183fa6748e5ea"
    );
    assert!(Frame::decode(&f).is_ok());
}

/// The split rule, written from the design: a directory of more than 4,096
/// names keeps a name equal to the longest common prefix `P` as an entry and
/// groups the rest into one Shard per first byte after `P`, recursively.
fn split(entries: Vec<(Vec<u8>, EntryTarget)>, nodes: &mut usize) -> TreeId {
    *nodes += 1;
    if entries.len() <= 4096 {
        return tree(entries).id();
    }
    let first = entries.iter().map(|e| &e.0).min().unwrap();
    let last = entries.iter().map(|e| &e.0).max().unwrap();
    let p = first.iter().zip(last).take_while(|(a, b)| a == b).count();
    let prefix = first[..p].to_vec();
    let mut out = Vec::new();
    let mut groups: BTreeMap<u8, Vec<(Vec<u8>, EntryTarget)>> = BTreeMap::new();
    for (n, t) in entries {
        if n == prefix {
            out.push((n, t));
        } else {
            groups.entry(n[p]).or_default().push((n, t));
        }
    }
    for (b, g) in groups {
        let mut name = prefix.clone();
        name.push(b);
        out.push((name, EntryTarget::Shard(split(g, nodes))));
    }
    tree(out).id()
}

fn dir(names: impl IntoIterator<Item = Vec<u8>>) -> Vec<(Vec<u8>, EntryTarget)> {
    names
        .into_iter()
        .map(|n| {
            let t = file(&n, FileKind::Regular);
            (n, t)
        })
        .collect()
}

#[test]
fn r10_sharded_directories() {
    let names: Vec<Vec<u8>> = (0..4096).map(|i| format!("f{i:05}").into_bytes()).collect();
    let mut nodes = 0;
    let at_limit = split(dir(names.clone()), &mut nodes);
    assert_eq!(nodes, 1);
    assert_eq!(
        hex(at_limit.as_bytes()),
        "22f9b35db9ba3f207c8b4e0c7bb6c3f54b8736de683f6184247a283b90784c94"
    );

    // One more name, equal to the common prefix: it stays an entry, and the
    // other 4,096 names form one shard that is the node above.
    let mut more = names.clone();
    more.push(b"f".to_vec());
    let mut nodes = 0;
    let top = split(dir(more), &mut nodes);
    assert_eq!(nodes, 2);
    assert_eq!(
        hex(top.as_bytes()),
        "89dee163f926e13573dfa9b178b3877764988c43fe6d0fc32d581dd21ea7ee54"
    );

    // A shard over the limit is split again; a one-name group is still a shard.
    let mut nested: Vec<Vec<u8>> = (0..5000).map(|i| format!("a{i:04}").into_bytes()).collect();
    nested.push(b"b".to_vec());
    let mut nodes = 0;
    let top = split(dir(nested.clone()), &mut nodes);
    assert_eq!(nodes, 8);
    assert_eq!(
        hex(top.as_bytes()),
        "82dc8fdd9bf85c8feebf151f91741d37e1a364c41e46c70084e706c1a668d7a6"
    );

    // The trie depends only on the entry set, not the order it is given in.
    nested.reverse();
    let mut nodes = 0;
    assert_eq!(split(dir(nested), &mut nodes), top);
}
