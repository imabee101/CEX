//! Shared helpers for the ops acceptance tests: stores on the dev
//! environment's Postgres and object store, changeset builders, a model of a
//! repo's files, and a reference tree builder written directly from the
//! tree rules, sharing no code with the derivation it checks.

#![allow(dead_code, unreachable_pub, unused_imports)] // Each test binary uses a different subset.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
pub use derived::testing::Rng;
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
    let meta = PgMetaStore::new(
        db.config().clone(),
        store::PgTls::LoopbackPlaintext,
        store::PoolLimits::new(16),
    )
    .unwrap();
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

/// Each directory's tree nodes (its node and every shard node below it), by
/// directory path ("" for the root).
pub type DirNodes = BTreeMap<Vec<u8>, Vec<TreeId>>;

/// The id of the root tree holding exactly these files, built from the
/// rules in AGENTS.md without the incremental code.
pub fn reference_root(files: impl IntoIterator<Item = (Vec<u8>, FileKind, Vec<u8>)>) -> TreeId {
    reference_nodes(files).0
}

/// [`reference_root`], with every directory's nodes.
pub fn reference_nodes(
    files: impl IntoIterator<Item = (Vec<u8>, FileKind, Vec<u8>)>,
) -> (TreeId, DirNodes) {
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
    let mut nodes = DirNodes::new();
    let id = reference_dir(&root, Vec::new(), &mut nodes);
    (id, nodes)
}

fn reference_dir(dir: &BTreeMap<Vec<u8>, RefNode>, path: Vec<u8>, out: &mut DirNodes) -> TreeId {
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
                RefNode::Dir(d) => {
                    let mut sub = path.clone();
                    if !sub.is_empty() {
                        sub.push(b'/');
                    }
                    sub.extend_from_slice(name);
                    EntryTarget::Tree(reference_dir(d, sub, out))
                }
            },
        })
        .collect();
    let mut ids = Vec::new();
    let id = reference_node_into(entries, &mut ids);
    out.insert(path, ids);
    id
}

/// The id of the node for a set of names: flat up to `MAX_TREE_ENTRIES`,
/// else split by the longest common prefix of all names.
pub fn reference_node(entries: Vec<TreeEntry>) -> TreeId {
    reference_node_into(entries, &mut Vec::new())
}

/// [`reference_node`], appending the id of every node it builds to `ids`.
fn reference_node_into(entries: Vec<TreeEntry>, ids: &mut Vec<TreeId>) -> TreeId {
    let id = if entries.len() <= MAX_TREE_ENTRIES {
        Tree::new(entries).unwrap().id()
    } else {
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
                target: EntryTarget::Shard(reference_node_into(g, ids)),
            });
        }
        Tree::new(node).unwrap().id()
    };
    ids.push(id);
    id
}

// ---- Random valid changes ----

/// Names that sort around a directory's contents (`a-` and `a.x` before
/// `a/`, `a0` after it) and never conflict by case.
pub const NAMES: &[&str] = &["a", "a-", "a.x", "a0", "b", "c"];

/// The directory where [`random_path`] picks among many names, so it can
/// pass `MAX_TREE_ENTRIES`.
pub const WIDE_DIR: &str = "w";

/// A random path: up to `max_depth` components from [`NAMES`], or, when
/// `wide` is not 0, a third of the time one of `wide` names in [`WIDE_DIR`].
pub fn random_path(rng: &mut Rng, max_depth: u64, wide: u64) -> Vec<u8> {
    if wide > 0 && rng.one_in(3) {
        // Half of them among the last names, which the first changeset
        // leaves out, so the directory grows as well as shrinks.
        let n = if rng.one_in(2) {
            rng.below(wide)
        } else {
            wide - 1 - rng.below(wide.min(8))
        };
        return format!("{WIDE_DIR}/n{n:05}").into_bytes();
    }
    let depth = 1 + rng.below(max_depth);
    let comps: Vec<&str> = (0..depth).map(|_| NAMES[rng.index(NAMES.len())]).collect();
    comps.join("/").into_bytes()
}

/// A random file kind.
pub fn random_kind(rng: &mut Rng) -> FileKind {
    [FileKind::Regular, FileKind::Executable, FileKind::Symlink][rng.index(3)]
}

/// Random content: a few lines from a small set (so blobs repeat and diffs
/// have shared lines), now and then binary, now and then empty.
pub fn random_content(rng: &mut Rng) -> Vec<u8> {
    let mut out: Vec<u8> = (0..rng.below(12))
        .flat_map(|_| format!("line {}\n", rng.below(6)).into_bytes())
        .collect();
    if rng.one_in(8) {
        out.insert(rng.index(out.len() + 1), 0);
    }
    out
}

/// One changeset's changes against its first parent's files, built so that
/// create accepts them: no no-op entries (INV-11), and no changed path under
/// another unless the shorter is a deleted file that becomes a directory
/// (INV-12). A write over a file's directory deletes the file in the same
/// changeset; a directory becomes a file only in a later changeset, after
/// its files are deleted.
#[derive(Clone, Debug)]
pub struct Changes {
    pub base: Model,
    pub next: Model,
    pub specs: BTreeMap<Vec<u8>, Option<(FileKind, Vec<u8>)>>,
}

fn is_under(p: &[u8], dir: &[u8]) -> bool {
    p.len() > dir.len() && p.starts_with(dir) && p[dir.len()] == b'/'
}

/// Whether any key of `map` lies under the directory `dir`: keys under it
/// sort from `dir/` up to `dir0` (`0` follows `/`).
fn has_under<V>(map: &BTreeMap<Vec<u8>, V>, dir: &[u8]) -> bool {
    let mut lo = dir.to_vec();
    lo.push(b'/');
    let mut hi = dir.to_vec();
    hi.push(b'0');
    map.range(lo..hi).next().is_some()
}

impl Changes {
    pub fn new(base: &Model) -> Self {
        Self {
            base: base.clone(),
            next: base.clone(),
            specs: BTreeMap::new(),
        }
    }

    /// Writes `p`, deleting any file over it, unless that cannot be done
    /// validly in this changeset. Returns whether it did.
    pub fn write(&mut self, p: &[u8], kind: FileKind, content: &[u8]) -> bool {
        let file = (kind, content.to_vec());
        if self.specs.contains_key(p)
            || self.base.files.get(p) == Some(&file)
            || has_under(&self.specs, p)
            || has_under(&self.next.files, p)
        {
            return false;
        }
        let ancestors: Vec<&[u8]> = p
            .iter()
            .enumerate()
            .filter(|&(_, &b)| b == b'/')
            .map(|(i, _)| &p[..i])
            .collect();
        let mut deletes = Vec::new();
        for q in ancestors {
            match self.specs.get(q) {
                Some(None) => {}
                Some(Some(_)) => return false,
                None if self.next.files.contains_key(q) => deletes.push(q.to_vec()),
                None => {}
            }
        }
        for q in deletes {
            self.next.files.remove(&q);
            self.specs.insert(q, None);
        }
        self.next.files.insert(p.to_vec(), file.clone());
        self.specs.insert(p.to_vec(), Some(file));
        true
    }

    /// Deletes the file at `p`, unless this changeset already changed it.
    pub fn delete(&mut self, p: &[u8]) -> bool {
        if self.specs.contains_key(p) || !self.next.files.contains_key(p) {
            return false;
        }
        self.next.files.remove(p);
        self.specs.insert(p.to_vec(), None);
        true
    }

    /// Up to `ops` random changes: writes at random paths, deletes of
    /// random files, and deletes of every file in a random directory.
    pub fn random(&mut self, rng: &mut Rng, ops: u64, max_depth: u64, wide: u64) {
        for _ in 0..ops {
            let files: Vec<Vec<u8>> = self.next.files.keys().cloned().collect();
            match rng.below(8) {
                0 | 1 if !files.is_empty() => {
                    let f = files[rng.index(files.len())].clone();
                    self.delete(&f);
                }
                2 if !files.is_empty() => {
                    // A directory picked uniformly, however many files it holds.
                    let dirs: BTreeSet<&[u8]> = files
                        .iter()
                        .filter_map(|f| f.iter().rposition(|&b| b == b'/').map(|i| &f[..i]))
                        .collect();
                    if let Some(&dir) = dirs.iter().nth(rng.index(dirs.len().max(1))) {
                        for g in files.iter().filter(|g| is_under(g, dir)) {
                            self.delete(g);
                        }
                    }
                }
                _ => {
                    let p = random_path(rng, max_depth, wide);
                    let kind = random_kind(rng);
                    let content = random_content(rng);
                    self.write(&p, kind, &content);
                }
            }
        }
    }

    /// The changes as create takes them, and the contents to upload.
    pub fn file_changes(&self) -> (Vec<(RepoPath, FileChange)>, Vec<Bytes>) {
        let mut contents = Vec::new();
        let changes = self
            .specs
            .iter()
            .map(|(p, c)| {
                let change = match c {
                    None => FileChange::Deleted,
                    Some((kind, content)) => {
                        contents.push(Bytes::copy_from_slice(content));
                        FileChange::File {
                            kind: *kind,
                            blob: BlobId::hash(content),
                            size: content.len() as u64,
                        }
                    }
                };
                (RepoPath::new(p.as_slice()).unwrap(), change)
            })
            .collect();
        (changes, contents)
    }
}

/// Every file `wide` names in [`WIDE_DIR`], past `MAX_TREE_ENTRIES` when
/// `wide` is, so the directory is sharded.
pub fn fill_wide(changes: &mut Changes, wide: u64) {
    for i in 0..wide {
        let p = format!("{WIDE_DIR}/n{i:05}");
        changes.write(p.as_bytes(), FileKind::Regular, b"same\n");
    }
}

/// A random history made by [`random_history`].
pub struct RandomHistory {
    /// Each changeset and its files, parents first.
    pub steps: Vec<(ChangesetId, Model)>,
    /// Each changeset's first parent, as an index into `steps`.
    pub first_parents: Vec<Option<usize>>,
    /// How many changesets are merges.
    pub merges: usize,
}

/// A random history of `n` changesets on one repo: new roots, linear
/// steps, and merges taking files from their other parent, with random
/// writes, deletes, and kind changes at paths up to `max_depth` deep, files
/// that become directories and back, and, when `wide` is not 0, a directory
/// of `wide - 4` files in the first changeset that random changes grow and
/// shrink among `wide` names.
pub async fn random_history<M: MetaStore, B: BlobStore>(
    o: &Ops<M, B>,
    rng: &mut Rng,
    n: usize,
    (max_depth, wide): (u64, u64),
    label: &str,
) -> RandomHistory {
    let mut h = RandomHistory {
        steps: Vec::new(),
        first_parents: Vec::new(),
        merges: 0,
    };
    while h.steps.len() < n {
        let i = h.steps.len();
        let (first, parents, mut changes) = if i == 0 || rng.one_in(12) {
            (None, Vec::new(), Changes::new(&Model::default()))
        } else {
            let p1 = i - 1 - rng.index(i.min(4));
            let mut changes = Changes::new(&h.steps[p1].1);
            let mut parents = vec![h.steps[p1].0];
            let p2 = rng.index(i);
            if p2 != p1 && rng.one_in(4) {
                parents.push(h.steps[p2].0);
                let theirs: Vec<_> = h.steps[p2].1.files.iter().collect();
                for _ in 0..3.min(theirs.len()) {
                    let (p, (k, c)) = theirs[rng.index(theirs.len())];
                    changes.write(p, *k, c);
                }
            }
            (Some(p1), parents, changes)
        };
        if i == 0 {
            fill_wide(&mut changes, wide.saturating_sub(4));
        }
        let ops = 1 + rng.below(6);
        changes.random(rng, ops, max_depth, wide);
        if changes.specs.is_empty() && parents.len() < 2 {
            continue;
        }
        let (fc, contents) = changes.file_changes();
        let id = commit(o, &parents, &format!("{label}/{i}"), fc, &contents)
            .await
            .unwrap_or_else(|e| panic!("{label}: changeset {i}: {e}"));
        h.merges += usize::from(parents.len() > 1);
        h.steps.push((id, changes.next));
        h.first_parents.push(first);
    }
    h
}

// ---- Recording reads ----

/// A metadata store that records the tree nodes read through it.
pub struct TreeReads<M> {
    pub inner: Arc<M>,
    pub trees: std::sync::Mutex<Vec<TreeId>>,
}

impl<M: MetaStore> TreeReads<M> {
    pub fn new(inner: Arc<M>) -> Self {
        Self {
            inner,
            trees: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn take(&self) -> Vec<TreeId> {
        std::mem::take(&mut self.trees.lock().unwrap())
    }
}

impl<M: MetaStore> MetaStore for TreeReads<M> {
    async fn put(
        &self,
        repo: RepoId,
        entries: &[(store::MetaKey, Bytes)],
    ) -> Result<Vec<store::PutOutcome>, store::StoreError> {
        self.inner.put(repo, entries).await
    }

    async fn get(
        &self,
        repo: RepoId,
        keys: &[store::MetaKey],
    ) -> Result<Vec<Option<Bytes>>, store::StoreError> {
        self.trees
            .lock()
            .unwrap()
            .extend(keys.iter().filter_map(|k| match k {
                store::MetaKey::Tree(t) => Some(*t),
                _ => None,
            }));
        self.inner.get(repo, keys).await
    }

    async fn get_ref(
        &self,
        repo: RepoId,
        name: &str,
    ) -> Result<Option<store::RefState>, store::StoreError> {
        self.inner.get_ref(repo, name).await
    }

    async fn list_refs(
        &self,
        repo: RepoId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<store::RefState>, store::StoreError> {
        self.inner.list_refs(repo, prefix, after, limit).await
    }

    async fn update_ref(
        &self,
        repo: RepoId,
        update: &store::RefUpdate<'_>,
    ) -> Result<store::CasOutcome, store::StoreError> {
        self.inner.update_ref(repo, update).await
    }

    async fn ref_log(
        &self,
        repo: RepoId,
        name: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<store::RefLogEntry>, store::StoreError> {
        self.inner.ref_log(repo, name, after_seq, limit).await
    }
}

/// The page size [`diff_all`] reads with: small, so every diff in the tests
/// that has more than a few files crosses page boundaries.
pub const DIFF_PAGE: usize = 3;

/// Every file of a diff, read [`DIFF_PAGE`] files at a time through the cursor.
pub async fn diff_all<M: MetaStore, B: BlobStore>(
    ops: &Ops<M, B>,
    a: ChangesetId,
    b: ChangesetId,
    mode: ops::DiffMode,
    paths: &[RepoPath],
) -> Result<Vec<ops::FileDiff>, ops::OpError> {
    diff_paged(ops, a, b, mode, paths, DIFF_PAGE).await
}

/// Every file of a diff, read `page` files at a time through the cursor.
pub async fn diff_paged<M: MetaStore, B: BlobStore>(
    ops: &Ops<M, B>,
    a: ChangesetId,
    b: ChangesetId,
    mode: ops::DiffMode,
    paths: &[RepoPath],
    page: usize,
) -> Result<Vec<ops::FileDiff>, ops::OpError> {
    let mut files = Vec::new();
    let mut after = None;
    loop {
        let options = ops::DiffOptions {
            mode,
            paths,
            after: after.as_ref(),
            limit: page,
        };
        let got = ops.diff(REPO, a, b, &options).await?;
        assert!(got.files.len() <= page);
        files.extend(got.files);
        match got.next {
            Some(next) => {
                // The cursor survives its byte form, as a client returns it.
                after = Some(ops::DiffCursor::from_bytes(&next.to_bytes()).unwrap());
            }
            None => return Ok(files),
        }
    }
}
