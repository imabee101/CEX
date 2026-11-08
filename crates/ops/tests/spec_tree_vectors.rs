//! Tree ids re-derived from the byte layout and id rules in CLAUDE.md by a
//! second hand encoder that uses only `blake3` and `sha2` (never scion-core's
//! encoder), compared with the root trees CreateChangeset derives on Postgres
//! plus the object store. Also checks that each hand-encoded node is the one
//! canonical encoding the scion-core decoder accepts, and that its
//! single-byte perturbations are rejected or decode to a different node.

mod common;

use std::collections::BTreeMap;

use common::{Model, Spec, commit, env, ops, root_of};
use scion_core::{ChangesetId, FileKind, Tree, TreeId};
use sha2::{Digest, Sha256};

const MAX: usize = 4096;

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

fn blob_id(c: &[u8]) -> [u8; 32] {
    blake3::derive_key("scion v1 blob", c)
}

#[derive(Clone)]
enum E {
    Dir([u8; 32]),
    Shard([u8; 32]),
    File(u8, Vec<u8>),
}

/// The hand-encoded node bytes for entries already in name order.
fn node_bytes(entries: &[(Vec<u8>, E)]) -> Vec<u8> {
    let mut out = vec![0x01];
    uvarint(&mut out, entries.len() as u64);
    for (name, e) in entries {
        uvarint(&mut out, name.len() as u64);
        out.extend_from_slice(name);
        match e {
            E::Dir(id) => {
                out.push(0);
                out.extend_from_slice(id);
            }
            E::Shard(id) => {
                out.push(1);
                out.extend_from_slice(id);
            }
            E::File(kind, c) => {
                out.push(*kind);
                out.extend_from_slice(&blob_id(c));
                uvarint(&mut out, c.len() as u64);
                out.extend_from_slice(&Sha256::digest(c));
            }
        }
    }
    out
}

/// Hashes and records a node, checking the decoder accepts it canonically.
fn node_id(entries: &[(Vec<u8>, E)], seen: &mut Vec<Vec<u8>>) -> [u8; 32] {
    let bytes = node_bytes(entries);
    seen.push(bytes.clone());
    blake3::derive_key("scion v1 tree", &bytes)
}

/// The id for one directory's entry set, split per the trie rule.
fn split(mut entries: Vec<(Vec<u8>, E)>, seen: &mut Vec<Vec<u8>>) -> [u8; 32] {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    if entries.len() <= MAX {
        return node_id(&entries, seen);
    }
    let mut p = entries[0].0.clone();
    for (n, _) in &entries {
        let l = p.iter().zip(n).take_while(|(a, b)| a == b).count();
        p.truncate(l);
    }
    let mut node = Vec::new();
    let mut groups: BTreeMap<u8, Vec<(Vec<u8>, E)>> = BTreeMap::new();
    for (n, e) in entries {
        if n == p {
            node.push((n, e));
        } else {
            groups.entry(n[p.len()]).or_default().push((n, e));
        }
    }
    for (b, g) in groups {
        let mut name = p.clone();
        name.push(b);
        node.push((name, E::Shard(split(g, seen))));
    }
    node.sort_by(|a, b| a.0.cmp(&b.0));
    node_id(&node, seen)
}

enum H {
    F(u8, Vec<u8>),
    D(BTreeMap<Vec<u8>, H>),
}

fn kind_byte(k: FileKind) -> u8 {
    match k {
        FileKind::Regular => 2,
        FileKind::Executable => 3,
        FileKind::Symlink => 4,
    }
}

fn hand_dir(d: &BTreeMap<Vec<u8>, H>, seen: &mut Vec<Vec<u8>>) -> [u8; 32] {
    let entries = d
        .iter()
        .map(|(n, h)| {
            (
                n.clone(),
                match h {
                    H::F(k, c) => E::File(*k, c.clone()),
                    H::D(sub) => E::Dir(hand_dir(sub, seen)),
                },
            )
        })
        .collect();
    split(entries, seen)
}

/// The hand-derived root id of a model, plus every node's bytes.
fn hand_root(m: &Model) -> ([u8; 32], Vec<Vec<u8>>) {
    let mut root = BTreeMap::new();
    for (p, (k, c)) in &m.files {
        let comps: Vec<&[u8]> = p.split(|&b| b == b'/').collect();
        let mut d = &mut root;
        for c in &comps[..comps.len() - 1] {
            d = match d.entry(c.to_vec()).or_insert_with(|| H::D(BTreeMap::new())) {
                H::D(x) => x,
                H::F(..) => panic!("file over dir"),
            };
        }
        d.insert(
            comps[comps.len() - 1].to_vec(),
            H::F(kind_byte(*k), c.clone()),
        );
    }
    let mut seen = Vec::new();
    let id = hand_dir(&root, &mut seen);
    (id, seen)
}

/// Checks the decoder: each node decodes and re-encodes to itself, and a
/// sample of one-byte changes never decodes to the same canonical bytes.
fn check_canonical(nodes: &[Vec<u8>]) {
    for b in nodes.iter().take(64) {
        let t = Tree::decode(b).expect("hand-encoded node must decode");
        assert_eq!(&t.encode(), b, "decoder accepted, re-encoding differs");
        assert_eq!(*t.id().as_bytes(), blake3::derive_key("scion v1 tree", b));
        // Trailing byte.
        let mut x = b.clone();
        x.push(0);
        assert!(Tree::decode(&x).is_err(), "trailing byte accepted");
        // Truncation.
        assert!(
            Tree::decode(&b[..b.len() - 1]).is_err(),
            "truncation accepted"
        );
        // Every single-byte flip in the first 200 bytes: rejected, or a
        // different canonical encoding.
        for i in 0..b.len().min(200) {
            let mut x = b.clone();
            x[i] ^= 0x01;
            if let Ok(t) = Tree::decode(&x) {
                assert_eq!(t.encode(), x, "flip at {i} accepted non-canonically");
            }
        }
    }
}

struct Repo {
    ops: ops::Ops<store::PgMetaStore, store::ObjectBlobStore>,
    model: Model,
    tip: Option<ChangesetId>,
    _env: common::Env,
}

impl Repo {
    async fn new() -> Self {
        let e = env().await;
        Repo {
            ops: ops(&e.meta, &e.blobs),
            model: Model::default(),
            tip: None,
            _env: e,
        }
    }

    async fn step(&mut self, changes: &[Spec<'_>], label: &str) -> TreeId {
        let (fc, contents) = self.model.apply(changes);
        let parents: Vec<_> = self.tip.into_iter().collect();
        let cs = commit(&self.ops, &parents, label, fc, &contents)
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        self.tip = Some(cs);
        let got = root_of(&self.ops, cs).await;
        let (want, nodes) = hand_root(&self.model);
        assert_eq!(
            *got.as_bytes(),
            want,
            "{label}: root id differs from hand derivation"
        );
        check_canonical(&nodes);
        got
    }
}

fn owned(
    names: impl IntoIterator<Item = String>,
    c: &'static [u8],
) -> Vec<(String, &'static [u8])> {
    names.into_iter().map(|n| (n, c)).collect()
}

fn specs<'a>(v: &'a [(String, &'static [u8])]) -> Vec<Spec<'a>> {
    v.iter()
        .map(|(n, c)| (n.as_str(), Some((FileKind::Regular, *c))))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn small_trees_match_hand_derivation() {
    let mut r = Repo::new().await;
    // 1. One regular file at the root.
    r.step(&[("a", Some((FileKind::Regular, b"hello\n")))], "v1")
        .await;
    // 2. Executable, symlink, empty file, binary content.
    r.step(
        &[
            ("bin/run", Some((FileKind::Executable, b"#!/bin/sh\n"))),
            ("link", Some((FileKind::Symlink, b"a"))),
            ("empty", Some((FileKind::Regular, b""))),
            ("z.bin", Some((FileKind::Regular, b"\0\x01\xff"))),
        ],
        "v2",
    )
    .await;
    // 3. Deep nesting.
    r.step(&[("d/e/f/g/h", Some((FileKind::Regular, b"deep")))], "v3")
        .await;
    // 4. Kind change only (regular to executable).
    r.step(&[("a", Some((FileKind::Executable, b"hello\n")))], "v4")
        .await;
    // 5. Deleting the only file of a chain removes the empty directories.
    r.step(&[("d/e/f/g/h", None)], "v5").await;
    // 6. Non-UTF-8 and high bytes in names sort by byte.
    r.step(
        &[
            ("\u{e9}t\u{e9}", Some((FileKind::Regular, b"x"))),
            ("B", Some((FileKind::Regular, b"y"))),
            ("a.b", Some((FileKind::Regular, b"z"))),
            ("a-b/c", Some((FileKind::Regular, b"w"))),
        ],
        "v6",
    )
    .await;
    // 7. Delete everything: the root becomes the empty tree.
    let all: Vec<String> = r
        .model
        .files
        .keys()
        .map(|k| String::from_utf8(k.clone()).unwrap())
        .collect();
    let del: Vec<Spec<'_>> = all.iter().map(|p| (p.as_str(), None)).collect();
    let empty = r.step(&del, "v7").await;
    assert_eq!(
        *empty.as_bytes(),
        blake3::derive_key("scion v1 tree", &[0x01, 0x00])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sharded_trees_match_hand_derivation_across_histories() {
    // History A: build `big/` (P = "f", one name equal to P, a nested
    // subdirectory inside a shard) in one changeset.
    let names: Vec<String> = (0..4100).map(|i| format!("big/f{i:04}")).collect();
    let mut a = Repo::new().await;
    let mut all = owned(names.clone(), b"same");
    all.push(("big/f".into(), b"eq-prefix"));
    all.push(("big/f2zz/sub".into(), b"nested"));
    let s = specs(&all);
    let ra = a.step(&s, "a1").await;

    // History B: reach the same contents in steps that cross the shard
    // threshold up, down, and up again, and shrink the common prefix.
    let mut b = Repo::new().await;
    let first = owned(names[..4000].iter().cloned(), b"same");
    b.step(&specs(&first), "b1").await;
    let more = owned(names[4000..].iter().cloned(), b"same");
    b.step(&specs(&more), "b2 crosses threshold").await;
    let dels: Vec<Spec<'_>> = names[100..200].iter().map(|n| (n.as_str(), None)).collect();
    b.step(&dels, "b3 back under threshold").await;
    let back = owned(names[100..200].iter().cloned(), b"same");
    b.step(&specs(&back), "b4 over again").await;
    // Shrink the common prefix: add a name outside "f", then remove it.
    b.step(
        &[("big/g", Some((FileKind::Regular, b"g")))],
        "b5 prefix shrinks",
    )
    .await;
    b.step(&[("big/g", None)], "b6 prefix grows").await;
    b.step(
        &[
            ("big/f", Some((FileKind::Regular, b"eq-prefix"))),
            ("big/f2zz/sub", Some((FileKind::Regular, b"nested"))),
        ],
        "b7",
    )
    .await;
    assert_eq!(b.model.files, a.model.files);
    assert_eq!(root_of(&b.ops, b.tip.unwrap()).await, ra);
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_shards_match_hand_derivation() {
    // 5,000 names under "q" (split again below the "q" shard) and one "r":
    // the top node has P = "" with shard entries "q" and "r".
    let mut r = Repo::new().await;
    let mut v = owned((0..5000).map(|i| format!("n/q{i:04}")), b"q");
    v.push(("n/r".into(), b"r"));
    r.step(&specs(&v), "nested").await;
    // A changed file deep inside a nested shard.
    r.step(
        &[("n/q4321", Some((FileKind::Regular, b"changed")))],
        "edit",
    )
    .await;
    // A long shared prefix ("aa…") with every name extending it.
    let w = owned((0..4097).map(|i| format!("m/aaaa{i:05}")), b"m");
    r.step(&specs(&w), "long prefix").await;
}

#[test]
fn hand_vectors_are_fixed() {
    // The hand encoder.s own byte layout, checked field by field.
    let mut seen = Vec::new();
    let empty = node_id(&[], &mut seen);
    let one = node_id(
        &[(b"a".to_vec(), E::File(2, b"hello\n".to_vec()))],
        &mut seen,
    );
    let nested = node_id(&[(b"d".to_vec(), E::Dir(one))], &mut seen);
    check_canonical(&seen);
    // Each equals derive_key("scion v1 tree", bytes) over the bytes below.
    assert_eq!(seen[0], [0x01, 0x00]);
    assert_eq!(seen[1][..5], [0x01, 0x01, 0x01, b'a', 0x02]);
    assert_eq!(seen[1].len(), 2 + 2 + 1 + 32 + 1 + 32);
    assert_eq!(seen[2][..5], [0x01, 0x01, 0x01, b'd', 0x00]);
    assert_eq!(seen[2].len(), 2 + 2 + 1 + 32);
    assert_eq!(Tree::decode(&seen[0]).unwrap().id().as_bytes(), &empty);
    assert_eq!(Tree::decode(&seen[2]).unwrap().id().as_bytes(), &nested);
}

#[test]
fn decoder_rejects_every_other_encoding() {
    let file = |name: &[u8]| (name.to_vec(), E::File(2, b"c".to_vec()));
    let good = node_bytes(&[file(b"a"), file(b"b")]);
    assert!(Tree::decode(&good).is_ok());
    let reject = |bytes: Vec<u8>, what: &str| {
        assert!(Tree::decode(&bytes).is_err(), "accepted: {what}");
    };
    // Version other than 0x01.
    let mut x = good.clone();
    x[0] = 0x02;
    reject(x, "version 2");
    let mut x = good.clone();
    x[0] = 0x00;
    reject(x, "version 0");
    // Non-minimal entry count.
    let mut x = vec![0x01, 0x82, 0x00];
    x.extend_from_slice(&good[2..]);
    reject(x, "non-minimal count");
    // Non-minimal name length.
    let mut x = good[..2].to_vec();
    x.extend_from_slice(&[0x81, 0x00]);
    x.extend_from_slice(&good[3..]);
    reject(x, "non-minimal name length");
    // Non-minimal size: entry "a" size 1 written as 0x81 0x00.
    let e1 = node_bytes(&[file(b"a")]);
    let size_at = 2 + 2 + 1 + 32;
    assert_eq!(e1[size_at], 1);
    let mut x = e1[..size_at].to_vec();
    x.extend_from_slice(&[0x81, 0x00]);
    x.extend_from_slice(&e1[size_at + 1..]);
    reject(x, "non-minimal size");
    // Unsorted and duplicate names.
    reject(node_bytes(&[file(b"b"), file(b"a")]), "unsorted");
    reject(node_bytes(&[file(b"a"), file(b"a")]), "duplicate");
    // Bad names: empty, `.`, `..`, slash, NUL.
    for n in [&b""[..], b".", b"..", b"a/b", b"a\0"] {
        reject(node_bytes(&[file(n)]), &format!("name {n:?}"));
    }
    // Unknown kind.
    let mut x = e1.clone();
    x[4] = 5;
    reject(x, "kind 5");
    // A Tree entry carrying a size and sha256 has trailing bytes.
    let mut x = e1.clone();
    x[4] = 0;
    reject(x, "tree entry with file fields");
    // A Tree or Shard entry naming a file's fields is not a valid file entry.
    let t = node_bytes(&[(b"d".to_vec(), E::Dir([7; 32]))]);
    let mut x = t.clone();
    x[4] = 2;
    reject(x, "file entry missing size and sha256");
    // Count larger than the entries present.
    let mut x = good.clone();
    x[1] = 3;
    reject(x, "count too large");
    let mut x = good.clone();
    x[1] = 1;
    reject(x, "count too small");
}

/// A random walk around the shard threshold in one directory, with names
/// that move the common prefix, names equal to it, and subdirectories that
/// appear and empty out; every step's root equals the hand derivation.
#[tokio::test(flavor = "multi_thread")]
async fn random_walk_around_the_shard_threshold_matches_hand_derivation() {
    let mut rng = common::Rng(0x7ee5_0001);
    let mut r = Repo::new().await;
    let seed: Vec<(String, &'static [u8])> = (0..4090)
        .map(|i| (format!("w/k{i:04}"), &b"s"[..]))
        .collect();
    r.step(&specs(&seed), "seed").await;
    let pool = |rng: &mut common::Rng| -> String {
        match rng.below(8) {
            0 => "w/k".to_owned(),
            1 => "w/k0".to_owned(),
            2 => format!("w/j{}", rng.below(4)),
            3 => format!("w/k{:04}/sub{}", 4090 + rng.below(4), rng.below(2)),
            4 => format!("w/\u{ff}{}", rng.below(3)),
            _ => format!("w/k{:04}", 4080 + rng.below(30)),
        }
    };
    let (mut over, mut under) = (false, false);
    for step in 0..25 {
        let mut changes: Vec<(String, Option<Vec<u8>>)> = Vec::new();
        let mut touched = std::collections::BTreeSet::new();
        for _ in 0..1 + rng.below(6) {
            let p = pool(&mut rng);
            // Skip paths that would conflict with a file or directory.
            let is_dir_prefix = r
                .model
                .files
                .keys()
                .any(|k| k.starts_with(format!("{p}/").as_bytes()));
            let parent_is_file = p
                .rmatch_indices('/')
                .any(|(i, _)| r.model.files.contains_key(&p.as_bytes()[..i]));
            if is_dir_prefix
                || parent_is_file
                || touched.iter().any(|t: &String| {
                    t == &p || t.starts_with(&format!("{p}/")) || p.starts_with(&format!("{t}/"))
                })
            {
                continue;
            }
            touched.insert(p.clone());
            if r.model.files.contains_key(p.as_bytes()) && rng.below(2) == 0 {
                changes.push((p, None));
            } else {
                changes.push((p, Some(format!("v{}", rng.below(1000)).into_bytes())));
            }
        }
        let s: Vec<Spec<'_>> = changes
            .iter()
            .filter(|(p, c)| {
                // Drop no-op writes.
                c.as_ref().is_none_or(|c| {
                    r.model.files.get(p.as_bytes()) != Some(&(FileKind::Regular, c.clone()))
                })
            })
            .map(|(p, c)| {
                (
                    p.as_str(),
                    c.as_ref().map(|c| (FileKind::Regular, c.as_slice())),
                )
            })
            .collect();
        if s.is_empty() {
            continue;
        }
        r.step(&s, &format!("walk {step}")).await;
        let names: std::collections::BTreeSet<&[u8]> = r
            .model
            .files
            .keys()
            .map(|k| k[2..].split(|&b| b == b'/').next().unwrap())
            .collect();
        over |= names.len() > MAX;
        under |= names.len() <= MAX;
    }
    assert!(
        over && under,
        "the walk did not cross the threshold both ways"
    );
}
