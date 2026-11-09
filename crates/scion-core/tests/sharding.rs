//! Tree sharding against an independent implementation of the split rule:
//! every node it produces must be accepted by scion-core, the trie depends
//! only on the directory's entry set (INV-13), and the root ids are pinned.

mod support;

use std::collections::BTreeMap;

use scion_core::{BlobId, EntryTarget, FileKind, Tree, TreeEntry, TreeId};
use sha2::Digest;
use support::to_hex;

fn file(content: &[u8]) -> EntryTarget {
    EntryTarget::File {
        kind: FileKind::Regular,
        blob: BlobId::hash(content),
        size: content.len() as u64,
        sha256: sha2::Sha256::digest(content).into(),
    }
}

fn node(mut entries: Vec<(Vec<u8>, EntryTarget)>) -> TreeId {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let t = Tree::new(
        entries
            .into_iter()
            .map(|(name, target)| TreeEntry { name, target })
            .collect(),
    )
    .expect("every node the split rule produces is valid");
    assert_eq!(Tree::decode(&t.encode()).unwrap(), t);
    t.id()
}

/// The split rule, written from the design: a directory of more than 4,096
/// names keeps a name equal to the longest common prefix `P` as an entry and
/// groups the rest into one Shard per first byte after `P`, recursively.
fn split(entries: Vec<(Vec<u8>, EntryTarget)>, nodes: &mut usize) -> TreeId {
    *nodes += 1;
    if entries.len() <= 4096 {
        return node(entries);
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
    node(out)
}

fn dir(names: impl IntoIterator<Item = Vec<u8>>) -> Vec<(Vec<u8>, EntryTarget)> {
    names
        .into_iter()
        .map(|n| {
            let t = file(&n);
            (n, t)
        })
        .collect()
}

#[test]
fn a_directory_at_the_limit_is_one_node() {
    let names: Vec<Vec<u8>> = (0..4096).map(|i| format!("f{i:05}").into_bytes()).collect();
    let mut nodes = 0;
    let id = split(dir(names), &mut nodes);
    assert_eq!(nodes, 1);
    assert_eq!(
        to_hex(id.as_bytes()),
        "22f9b35db9ba3f207c8b4e0c7bb6c3f54b8736de683f6184247a283b90784c94"
    );
}

#[test]
fn a_name_equal_to_the_common_prefix_stays_an_entry() {
    // The other 4,096 names form one shard, which is the node above.
    let mut names: Vec<Vec<u8>> = (0..4096).map(|i| format!("f{i:05}").into_bytes()).collect();
    names.push(b"f".to_vec());
    let mut nodes = 0;
    let id = split(dir(names), &mut nodes);
    assert_eq!(nodes, 2);
    assert_eq!(
        to_hex(id.as_bytes()),
        "89dee163f926e13573dfa9b178b3877764988c43fe6d0fc32d581dd21ea7ee54"
    );
}

#[test]
fn shards_over_the_limit_split_again_and_order_does_not_matter() {
    // A shard over the limit is split again; a one-name group is still a
    // shard.
    let mut names: Vec<Vec<u8>> = (0..5000).map(|i| format!("a{i:04}").into_bytes()).collect();
    names.push(b"b".to_vec());
    let mut nodes = 0;
    let id = split(dir(names.clone()), &mut nodes);
    assert_eq!(nodes, 8);
    assert_eq!(
        to_hex(id.as_bytes()),
        "82dc8fdd9bf85c8feebf151f91741d37e1a364c41e46c70084e706c1a668d7a6"
    );
    // The trie depends only on the entry set (INV-13).
    names.reverse();
    let mut nodes = 0;
    assert_eq!(split(dir(names), &mut nodes), id);
}
