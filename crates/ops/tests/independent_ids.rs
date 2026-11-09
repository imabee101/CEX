//! Tree and changeset ids re-derived from the byte layout and id rules in
//! AGENTS.md with a separate, minimal encoder written here: it uses neither
//! scion-core's encoders nor the reference builder in `common`. Each case
//! creates changesets through `Ops` on Postgres plus the object store and
//! checks the stored root tree id, and the returned changeset id, against
//! the bytes built here.

mod common;

use std::collections::BTreeMap;

use bytes::Bytes;
use common::{commit, env, ops, path, root_of};
use scion_core::{BlobId, ChangesetId, FileChange, FileKind, RepoPath};
use sha2::{Digest, Sha256};

// ---- Primitives, from <data_model> ----

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

fn bytes_field(out: &mut Vec<u8>, b: &[u8]) {
    uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn blob_id(content: &[u8]) -> [u8; 32] {
    blake3::derive_key("scion v1 blob", content)
}

// ---- Trees ----

#[derive(Clone, Copy)]
enum K {
    Regular,
    Executable,
    Symlink,
}

enum Node {
    File(K, Vec<u8>),
    Dir(BTreeMap<Vec<u8>, Node>),
}

/// One encoded entry: (name, kind byte, id, optional (size, sha256)).
type Entry = (Vec<u8>, u8, [u8; 32], Option<(u64, [u8; 32])>);

fn encode_node(entries: &[Entry]) -> Vec<u8> {
    let mut out = vec![0x01];
    uvarint(&mut out, entries.len() as u64);
    for (name, kind, id, file) in entries {
        bytes_field(&mut out, name);
        out.push(*kind);
        out.extend_from_slice(id);
        if let Some((size, sha)) = file {
            uvarint(&mut out, *size);
            out.extend_from_slice(sha);
        }
    }
    out
}

fn tree_id_of(bytes: &[u8]) -> [u8; 32] {
    blake3::derive_key("scion v1 tree", bytes)
}

/// The node for a set of entries (sorted by name), split per the rule.
fn node(mut entries: Vec<Entry>) -> [u8; 32] {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    if entries.len() <= 4096 {
        return tree_id_of(&encode_node(&entries));
    }
    let first = entries.first().unwrap().0.clone();
    let last = entries.last().unwrap().0.clone();
    // In sorted order, the common prefix of all names is that of the first
    // and last.
    let p = first.iter().zip(&last).take_while(|(a, b)| a == b).count();
    let prefix = first[..p].to_vec();
    let mut out: Vec<Entry> = Vec::new();
    let mut groups: BTreeMap<u8, Vec<Entry>> = BTreeMap::new();
    for e in entries {
        if e.0 == prefix {
            out.push(e);
        } else {
            groups.entry(e.0[p]).or_default().push(e);
        }
    }
    for (b, g) in groups {
        let mut name = prefix.clone();
        name.push(b);
        out.push((name, 1, node(g), None));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    tree_id_of(&encode_node(&out))
}

fn dir_id(dir: &BTreeMap<Vec<u8>, Node>) -> [u8; 32] {
    let entries = dir
        .iter()
        .map(|(name, n)| match n {
            Node::Dir(d) => (name.clone(), 0u8, dir_id(d), None),
            Node::File(k, c) => {
                let kind = match k {
                    K::Regular => 2,
                    K::Executable => 3,
                    K::Symlink => 4,
                };
                let sha: [u8; 32] = Sha256::digest(c).into();
                (name.clone(), kind, blob_id(c), Some((c.len() as u64, sha)))
            }
        })
        .collect();
    node(entries)
}

fn expected_root(files: &BTreeMap<String, (K, Vec<u8>)>) -> [u8; 32] {
    let mut root: BTreeMap<Vec<u8>, Node> = BTreeMap::new();
    for (p, (k, c)) in files {
        let comps: Vec<&str> = p.split('/').collect();
        let mut d = &mut root;
        for c in &comps[..comps.len() - 1] {
            let n = d
                .entry(c.as_bytes().to_vec())
                .or_insert_with(|| Node::Dir(BTreeMap::new()));
            d = match n {
                Node::Dir(x) => x,
                Node::File(..) => unreachable!("file over directory in a test case"),
            };
        }
        d.insert(
            comps[comps.len() - 1].as_bytes().to_vec(),
            Node::File(*k, c.clone()),
        );
    }
    dir_id(&root)
}

// ---- Changesets ----

/// One change: a path and its new (kind, content), or `None` to delete.
type Ch = (String, Option<(K, Vec<u8>)>);

/// A committer and its optional date (seconds, offset).
type Committer<'a> = (&'a [u8], Option<(i64, i64)>);

/// The changeset bytes `common::changeset` describes, encoded by hand.
fn expected_changeset(parents: &[ChangesetId], message: &str, changes: &[Ch]) -> [u8; 32] {
    let mut out = vec![0x01];
    uvarint(&mut out, parents.len() as u64);
    for p in parents {
        out.extend_from_slice(p.as_bytes());
    }
    bytes_field(&mut out, b"Test <test@example.com>");
    svarint(&mut out, 1_700_000_000);
    svarint(&mut out, 3600);
    out.push(0); // committer
    out.push(0); // committer_date
    bytes_field(&mut out, message.as_bytes());
    out.push(0); // signature
    out.push(0); // git_headers
    let mut sorted: Vec<_> = changes.iter().collect();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    uvarint(&mut out, sorted.len() as u64);
    for (p, c) in sorted {
        bytes_field(&mut out, p.as_bytes());
        match c {
            None => out.push(0),
            Some((k, content)) => {
                out.push(match k {
                    K::Regular => 1,
                    K::Executable => 2,
                    K::Symlink => 3,
                });
                out.extend_from_slice(&blob_id(content));
                uvarint(&mut out, content.len() as u64);
            }
        }
    }
    blake3::derive_key("scion v1 changeset", &out)
}

// ---- Driver ----

struct Repo<M: store::MetaStore, B: store::BlobStore> {
    ops: ops::Ops<M, B>,
    files: BTreeMap<String, (K, Vec<u8>)>,
    tip: Option<ChangesetId>,
}

impl<M: store::MetaStore, B: store::BlobStore> Repo<M, B> {
    /// Commits the changes, checks the changeset id and root tree id against
    /// the hand encoding, and returns the root tree id.
    async fn step(&mut self, changes: Vec<Ch>) -> [u8; 32] {
        let mut fc: Vec<(RepoPath, FileChange)> = Vec::new();
        let mut contents: Vec<Bytes> = Vec::new();
        for (p, c) in &changes {
            match c {
                None => {
                    self.files.remove(p);
                    fc.push((path(p), FileChange::Deleted));
                }
                Some((k, content)) => {
                    self.files.insert(p.clone(), (*k, content.clone()));
                    contents.push(Bytes::copy_from_slice(content));
                    fc.push((
                        path(p),
                        FileChange::File {
                            kind: match k {
                                K::Regular => FileKind::Regular,
                                K::Executable => FileKind::Executable,
                                K::Symlink => FileKind::Symlink,
                            },
                            blob: BlobId::hash(content),
                            size: content.len() as u64,
                        },
                    ));
                }
            }
        }
        contents.sort();
        contents.dedup();
        let parents: Vec<ChangesetId> = self.tip.into_iter().collect();
        let msg = format!("step {}", self.files.len());
        let cs = commit(&self.ops, &parents, &msg, fc, &contents)
            .await
            .unwrap();
        assert_eq!(
            *cs.as_bytes(),
            expected_changeset(&parents, &msg, &changes),
            "changeset id differs from the hand encoding"
        );
        self.tip = Some(cs);
        let root = *root_of(&self.ops, cs).await.as_bytes();
        assert_eq!(
            root,
            expected_root(&self.files),
            "root tree id differs from the hand encoding"
        );
        root
    }
}

fn reg(s: &str) -> Option<(K, Vec<u8>)> {
    Some((K::Regular, s.as_bytes().to_vec()))
}

fn ch(p: &str, c: Option<(K, Vec<u8>)>) -> Ch {
    (p.to_owned(), c)
}

#[tokio::test(flavor = "multi_thread")]
async fn small_trees_match_hand_encoding() {
    let e = env().await;
    let mut r = Repo {
        ops: ops(&e.meta, &e.blobs),
        files: BTreeMap::new(),
        tip: None,
    };
    // 1. A single regular file.
    r.step(vec![ch("a", reg("hello\n"))]).await;
    // 2. Nested directories, every file kind, a name that sorts by bytes
    //    (uppercase before lowercase, non-ASCII last).
    r.step(vec![
        ch("dir/sub/x.txt", reg("x\n")),
        ch("dir/run.sh", Some((K::Executable, b"#!/bin/sh\n".to_vec()))),
        ch("dir/link", Some((K::Symlink, b"sub/x.txt".to_vec()))),
        ch("B", reg("")),
        ch("\u{e9}t\u{e9}", reg("\u{e9}")),
    ])
    .await;
    // 3. A modification deep in the tree and a kind change.
    r.step(vec![
        ch("dir/sub/x.txt", reg("y\n")),
        ch("a", Some((K::Executable, b"hello\n".to_vec()))),
    ])
    .await;
    // 4. A delete that empties a directory removes it.
    r.step(vec![ch("dir/sub/x.txt", None)]).await;
    // 5. A file whose path becomes a directory.
    r.step(vec![ch("B", None), ch("B/inner", reg("i"))]).await;
    // 6. A file with a size needing a multi-byte varint.
    r.step(vec![ch("big", reg(&"z".repeat(300)))]).await;
    // 7. Deleting everything leaves the empty root.
    let all: Vec<String> = r.files.keys().cloned().collect();
    let root = r.step(all.iter().map(|p| ch(p, None)).collect()).await;
    assert_eq!(root, tree_id_of(&[0x01, 0x00]));
}

/// Names for a sharded directory: `n` names `<stem><i:05>`.
fn names(stem: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{stem}{i:05}")).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn sharded_trees_match_hand_encoding_across_histories() {
    let e = env().await;
    // The final directory `d`: a name equal to the common prefix "a", 5,000
    // names "ab…" (a shard that splits again by its own prefix "ab0"), and
    // ten names "ac…"; plus `flat` with exactly 4,096 names (unsplit).
    let mut big: Vec<String> = vec!["a".into()];
    big.extend(names("ab", 5000));
    big.extend(names("ac", 10));
    let flat = names("f", 4096);
    let final_files: Vec<String> = big
        .iter()
        .map(|n| format!("d/{n}"))
        .chain(flat.iter().map(|n| format!("flat/{n}")))
        .collect();

    // History one: everything in one changeset.
    let mut one = Repo {
        ops: ops(&e.meta, &e.blobs),
        files: BTreeMap::new(),
        tip: None,
    };
    let r1 = one
        .step(final_files.iter().map(|p| ch(p, reg("same"))).collect())
        .await;

    // History two, on fresh caches: start from a different, larger set
    // (with extra names that change the prefix and a shard "ad…"), reach
    // the same set over several changesets, passing through a flat state.
    let mut two = Repo {
        ops: ops(&e.meta, &e.blobs),
        files: BTreeMap::new(),
        tip: None,
    };
    two.step(
        names("ab", 3000)
            .iter()
            .map(|n| ch(&format!("d/{n}"), reg("old")))
            .collect(),
    )
    .await;
    two.step(
        names("ad", 2000)
            .iter()
            .map(|n| ch(&format!("d/{n}"), reg("same")))
            .chain(std::iter::once(ch("d/zzz", reg("same"))))
            .chain(
                names("f", 4097)
                    .iter()
                    .map(|n| ch(&format!("flat/{n}"), reg("same"))),
            )
            .collect(),
    )
    .await;
    let mut rest: Vec<Ch> = Vec::new();
    rest.extend(
        names("ad", 2000)
            .iter()
            .map(|n| ch(&format!("d/{n}"), None)),
    );
    rest.push(ch("d/zzz", None));
    rest.push(ch("flat/f04096", None));
    rest.extend(big.iter().map(|n| ch(&format!("d/{n}"), reg("same"))));
    two.step(rest).await;
    // The name equal to the split prefix ("a") is found, changed, removed,
    // and restored inside the sharded node.
    two.step(vec![ch("d/a", reg("changed"))]).await;
    two.step(vec![ch("d/a", None)]).await;
    two.step(vec![ch("d/a", reg("same"))]).await;
    let r2 = two.root_check().await;
    assert_eq!(r1, r2, "equal contents gave different root ids");

    // Every directory id agrees too, not just the root: `d` alone in a
    // third history built in reverse name order, one batch at a time.
    let mut three = Repo {
        ops: ops(&e.meta, &e.blobs),
        files: BTreeMap::new(),
        tip: None,
    };
    let mut rev = final_files.clone();
    rev.reverse();
    for batch in rev.chunks(2500) {
        three
            .step(batch.iter().map(|p| ch(p, reg("same"))).collect())
            .await;
    }
    assert_eq!(three.root_check().await, r1);
}

impl<M: store::MetaStore, B: store::BlobStore> Repo<M, B> {
    async fn root_check(&self) -> [u8; 32] {
        let root = *root_of(&self.ops, self.tip.unwrap()).await.as_bytes();
        assert_eq!(root, expected_root(&self.files));
        root
    }
}

/// Every hand-built encoding is accepted and every near variant of it is
/// rejected: one accepted encoding per value (INV-10).
#[test]
fn decoders_accept_only_the_canonical_encoding() {
    use scion_core::{Changeset, Tree};
    let sha = [7u8; 32];
    let a = (b"a".to_vec(), 2u8, blob_id(b"x"), Some((1u64, sha)));
    let b = (b"b".to_vec(), 0u8, [9u8; 32], None);
    let good = encode_node(&[a.clone(), b.clone()]);
    let t = Tree::decode(&good).unwrap();
    assert_eq!(t.encode(), good);
    assert_eq!(*t.id().as_bytes(), tree_id_of(&good));
    let mut bad: Vec<(&str, Vec<u8>)> = vec![
        ("unsorted", encode_node(&[b.clone(), a.clone()])),
        ("duplicate", encode_node(&[a.clone(), a.clone()])),
        (
            "tree with size",
            encode_node(&[(b"t".to_vec(), 0, [1; 32], Some((1, sha)))]),
        ),
        (
            "file without size",
            encode_node(&[(b"f".to_vec(), 2, [1; 32], None)]),
        ),
        (
            "bad kind",
            encode_node(&[(b"f".to_vec(), 5, [1; 32], None)]),
        ),
        (
            "small node with a shard",
            encode_node(&[(b"s".to_vec(), 1, [1; 32], None)]),
        ),
        ("empty name", encode_node(&[(Vec::new(), 0, [1; 32], None)])),
        (
            "slash in name",
            encode_node(&[(b"a/b".to_vec(), 0, [1; 32], None)]),
        ),
        (
            "dot name",
            encode_node(&[(b".".to_vec(), 0, [1; 32], None)]),
        ),
    ];
    let mut v = good.clone();
    v[0] = 2;
    bad.push(("version", v));
    let mut v = good.clone();
    v.push(0);
    bad.push(("trailing byte", v));
    bad.push(("truncated", good[..good.len() - 1].to_vec()));
    // The entry count 2 as a two-byte varint.
    let mut v = vec![0x01, 0x82, 0x00];
    v.extend_from_slice(&good[2..]);
    bad.push(("non-minimal count", v));
    // The size 1 as a two-byte varint.
    let mut v = encode_node(std::slice::from_ref(&a));
    let at = 2 + 2 + 1 + 32;
    assert_eq!(v[at], 1);
    v.splice(at..=at, [0x81, 0x00]);
    bad.push(("non-minimal size", v));
    for (what, bytes) in &bad {
        assert!(Tree::decode(bytes).is_err(), "tree decoder accepted {what}");
    }

    // Changesets: one with every optional field, encoded by hand.
    let enc = |committer: Option<Committer<'_>>, opt_tag: u8, offset: i64| {
        let mut out = vec![0x01, 0x01];
        out.extend_from_slice(&[3u8; 32]);
        bytes_field(&mut out, b"A <a@b>");
        svarint(&mut out, -5);
        svarint(&mut out, offset);
        match committer {
            None => out.extend_from_slice(&[0, 0]),
            Some((c, date)) => {
                out.push(1);
                bytes_field(&mut out, c);
                match date {
                    None => out.push(0),
                    Some((s, o)) => {
                        out.push(1);
                        svarint(&mut out, s);
                        svarint(&mut out, o);
                    }
                }
            }
        }
        bytes_field(&mut out, b"msg");
        out.push(opt_tag);
        if opt_tag == 1 {
            bytes_field(&mut out, b"sig");
        }
        out.push(0);
        uvarint(&mut out, 2);
        bytes_field(&mut out, b"a");
        out.push(0);
        bytes_field(&mut out, b"b");
        out.push(3);
        out.extend_from_slice(&blob_id(b"t"));
        uvarint(&mut out, 1);
        out
    };
    let good = enc(Some((b"C <c@d>", Some((7, -3600)))), 1, 3600);
    let cs = Changeset::decode(&good).unwrap();
    assert_eq!(cs.encode(), good);
    assert_eq!(
        *cs.id().as_bytes(),
        blake3::derive_key("scion v1 changeset", &good)
    );
    let mut bad: Vec<(&str, Vec<u8>)> = vec![
        (
            "committer without date",
            enc(Some((b"C <c@d>", None)), 1, 0),
        ),
        ("option tag 2", enc(None, 2, 0)),
        ("offset of a day", enc(None, 0, 86_400)),
        ("offset of minus a day", enc(None, 0, -86_400)),
    ];
    let mut v = good.clone();
    v.push(0);
    bad.push(("trailing byte", v));
    // Swap the two changes' order.
    let n = good.len();
    let tail_b = good[n - (2 + 1 + 32 + 1)..].to_vec();
    let tail_a = good[n - (2 + 1 + 32 + 1) - 3..n - (2 + 1 + 32 + 1)].to_vec();
    let mut v = good[..n - tail_b.len() - tail_a.len()].to_vec();
    v.extend_from_slice(&tail_b);
    v.extend_from_slice(&tail_a);
    bad.push(("unsorted changes", v));
    for (what, bytes) in &bad {
        assert!(
            Changeset::decode(bytes).is_err(),
            "changeset decoder accepted {what}"
        );
    }
    // The boundary offsets inside the limit are accepted.
    for o in [86_399, -86_399] {
        Changeset::decode(&enc(None, 0, o)).unwrap();
    }
}
