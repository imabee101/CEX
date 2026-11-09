//! Shared helpers for the ops acceptance tests: stores on the dev
//! environment's Postgres and object store, changeset builders, a model of a
//! repo's files, and a reference tree builder written directly from the
//! tree rules, sharing no code with the derivation it checks.

#![allow(dead_code, unreachable_pub)] // Each test binary uses a different subset.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use derived::{CacheSizes, Deriver, RootTree};
use ops::{CreateOptions, Ops, blob::upload_blobs};
use scion_core::{
    BlobId, Changeset, ChangesetId, ChangesetParts, EntryTarget, FileChange, FileKind,
    MAX_TREE_ENTRIES, RepoPath, Timestamp, Tree, TreeEntry, TreeId,
};
use sha2::{Digest, Sha256};
use store::testing::{TestDatabase, object_store};
use store::{BlobStore, MetaStore, ObjectBlobStore, PgMetaStore, RepoId};

pub const REPO: RepoId = RepoId(7);

/// A change to one path: a write of (kind, content), or a delete.
pub type Spec<'a> = (&'a str, Option<(FileKind, &'a [u8])>);
/// An owned [`Spec`].
pub type OwnedSpec = (String, Option<(FileKind, Vec<u8>)>);

/// Stores on the dev environment, owned by one test.
pub struct Env {
    pub db: TestDatabase,
    pub meta: Arc<PgMetaStore>,
    pub blobs: Arc<ObjectBlobStore>,
}

pub async fn env() -> Env {
    let db = TestDatabase::create().await;
    let meta = PgMetaStore::new(db.config().clone(), 16).unwrap();
    meta.migrate().await.unwrap();
    Env {
        db,
        meta: Arc::new(meta),
        blobs: Arc::new(ObjectBlobStore::new(Arc::new(object_store()))),
    }
}

/// Fresh operations (a fresh server, with cold caches) over the stores.
pub fn ops<M: MetaStore, B: BlobStore>(meta: &Arc<M>, blobs: &Arc<B>) -> Ops<M, B> {
    Ops::new(Arc::new(Deriver::new(
        meta.clone(),
        blobs.clone(),
        CacheSizes::default(),
    )))
}

pub fn path(s: &str) -> RepoPath {
    RepoPath::new(s.as_bytes()).unwrap()
}

/// A changeset with the given parents and changes.
pub fn changeset(
    parents: &[ChangesetId],
    message: &str,
    changes: Vec<(RepoPath, FileChange)>,
) -> Changeset {
    let mut p = ChangesetParts::default();
    p.parents = parents.to_vec();
    p.author = b"Test <test@example.com>".to_vec();
    p.author_date = Timestamp::new(1_700_000_000, 3600).unwrap();
    p.message = message.as_bytes().to_vec();
    p.file_changes = changes.into_iter().collect();
    Changeset::new(p).unwrap()
}

/// A file change writing `content` as a regular file.
pub fn write(content: &[u8]) -> FileChange {
    FileChange::File {
        kind: FileKind::Regular,
        blob: BlobId::hash(content),
        size: content.len() as u64,
    }
}

/// A model of a repo's files: path → (kind, content).
#[derive(Clone, Default, Debug)]
pub struct Model {
    pub files: BTreeMap<Vec<u8>, (FileKind, Vec<u8>)>,
}

impl Model {
    /// Applies a change list, returning the changes as file changes and the
    /// contents to upload.
    pub fn apply(&mut self, changes: &[Spec<'_>]) -> (Vec<(RepoPath, FileChange)>, Vec<Bytes>) {
        let mut out = Vec::new();
        let mut contents = Vec::new();
        for (p, c) in changes {
            match c {
                None => {
                    self.files.remove(p.as_bytes());
                    out.push((path(p), FileChange::Deleted));
                }
                Some((kind, content)) => {
                    self.files
                        .insert(p.as_bytes().to_vec(), (*kind, content.to_vec()));
                    contents.push(Bytes::copy_from_slice(content));
                    out.push((
                        path(p),
                        FileChange::File {
                            kind: *kind,
                            blob: BlobId::hash(content),
                            size: content.len() as u64,
                        },
                    ));
                }
            }
        }
        (out, contents)
    }

    /// The root tree id of the model's files, by the reference builder.
    pub fn root(&self) -> TreeId {
        reference_root(
            self.files
                .iter()
                .map(|(p, (k, c))| (p.clone(), *k, c.clone())),
        )
    }
}

/// Uploads contents and creates a changeset.
pub async fn commit<M: MetaStore, B: BlobStore>(
    ops: &Ops<M, B>,
    parents: &[ChangesetId],
    message: &str,
    changes: Vec<(RepoPath, FileChange)>,
    contents: &[Bytes],
) -> Result<ChangesetId, ops::OpError> {
    let d = ops.deriver();
    upload_blobs(&**d.meta(), d.blobs(), REPO, contents).await?;
    ops.create_changeset(
        REPO,
        changeset(parents, message, changes),
        &CreateOptions::default(),
    )
    .await
}

/// The stored root tree of a changeset.
pub async fn root_of<M: MetaStore, B: BlobStore>(ops: &Ops<M, B>, cs: ChangesetId) -> TreeId {
    RootTree::decode(&ops.deriver().get(REPO, &RootTree, cs).await.unwrap()).unwrap()
}

// ---- Reference tree builder ----

enum RefNode {
    File(FileKind, Vec<u8>),
    Dir(BTreeMap<Vec<u8>, RefNode>),
}

/// The id of the root tree holding exactly these files, built from the
/// rules in AGENTS.md without the incremental code.
pub fn reference_root(files: impl IntoIterator<Item = (Vec<u8>, FileKind, Vec<u8>)>) -> TreeId {
    let mut root = BTreeMap::new();
    for (p, kind, content) in files {
        let comps: Vec<&[u8]> = p.split(|&b| b == b'/').collect();
        let mut dir = &mut root;
        for c in &comps[..comps.len() - 1] {
            let n = dir
                .entry(c.to_vec())
                .or_insert_with(|| RefNode::Dir(BTreeMap::new()));
            dir = match n {
                RefNode::Dir(d) => d,
                RefNode::File(..) => panic!("model has a file over a directory"),
            };
        }
        dir.insert(
            comps[comps.len() - 1].to_vec(),
            RefNode::File(kind, content),
        );
    }
    reference_dir(&root)
}

fn reference_dir(dir: &BTreeMap<Vec<u8>, RefNode>) -> TreeId {
    let entries: Vec<TreeEntry> = dir
        .iter()
        .map(|(name, n)| TreeEntry {
            name: name.clone(),
            target: match n {
                RefNode::File(kind, c) => EntryTarget::File {
                    kind: *kind,
                    blob: BlobId::hash(c),
                    size: c.len() as u64,
                    sha256: Sha256::digest(c).into(),
                },
                RefNode::Dir(d) => EntryTarget::Tree(reference_dir(d)),
            },
        })
        .collect();
    reference_node(entries)
}

/// The id of the node for a set of names: flat up to `MAX_TREE_ENTRIES`,
/// else split by the longest common prefix of all names.
pub fn reference_node(entries: Vec<TreeEntry>) -> TreeId {
    if entries.len() <= MAX_TREE_ENTRIES {
        return Tree::new(entries).unwrap().id();
    }
    let mut prefix = entries[0].name.clone();
    for e in &entries {
        let n = prefix
            .iter()
            .zip(&e.name)
            .take_while(|(a, b)| a == b)
            .count();
        prefix.truncate(n);
    }
    let mut node = Vec::new();
    let mut groups: BTreeMap<u8, Vec<TreeEntry>> = BTreeMap::new();
    for e in entries {
        if e.name == prefix {
            node.push(e);
        } else {
            groups.entry(e.name[prefix.len()]).or_default().push(e);
        }
    }
    for (b, g) in groups {
        let mut name = prefix.clone();
        name.push(b);
        node.push(TreeEntry {
            name,
            target: EntryTarget::Shard(reference_node(g)),
        });
    }
    Tree::new(node).unwrap().id()
}

/// A small deterministic PRNG (SplitMix64) for randomized tests; a failing
/// case prints its seed.
pub struct Rng(pub u64);

impl Rng {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}
