//! Tree nodes: loading them in batches through a bounded cache, looking up
//! names through sharded nodes, and rewriting a tree for a set of file
//! changes.
//!
//! A directory's node is built from its entry set alone (INV-13): up to
//! `MAX_TREE_ENTRIES` names make one flat node; more are split by their
//! longest common prefix `P`, where a name equal to `P` stays an entry and
//! the rest are grouped by their first byte after `P` into `Shard` entries,
//! each group built by the same rule. [`TreeWork::apply`] rewrites only the
//! nodes on each changed path's spine (INV-1), and keeps the result
//! identical to building every touched directory from scratch.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use scion_core::{EntryTarget, MAX_TREE_ENTRIES, RepoPath, Tree, TreeEntry, TreeId};
use store::{MetaKey, MetaStore, RepoId};

use crate::{DeriveError, InvalidKind};

/// A bounded cache of decoded tree nodes, shared by every derivation and
/// read on a server. Trees are immutable, so entries never go stale.
#[derive(Clone)]
pub struct TreeCache {
    inner: moka::sync::Cache<(RepoId, TreeId), Arc<Tree>>,
}

impl std::fmt::Debug for TreeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeCache")
            .field("entries", &self.inner.entry_count())
            .finish()
    }
}

/// An estimate of a decoded node's memory: its entries' names plus a fixed
/// cost per entry.
fn tree_weight(tree: &Tree) -> u32 {
    let bytes: usize = tree.entries().iter().map(|e| e.name.len() + 96).sum();
    u32::try_from(bytes + 64).unwrap_or(u32::MAX)
}

impl TreeCache {
    /// A cache holding about `max_bytes` of decoded nodes.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            inner: moka::sync::Cache::builder()
                .max_capacity(max_bytes)
                .weigher(|_, t: &Arc<Tree>| tree_weight(t))
                .build(),
        }
    }

    pub(crate) fn insert(&self, repo: RepoId, id: TreeId, tree: Arc<Tree>) {
        self.inner.insert((repo, id), tree);
    }
}

/// Reads tree nodes in one batch: from the cache, then from the metadata
/// store, verifying each stored node's id.
pub(crate) async fn load_trees<M: MetaStore>(
    meta: &M,
    cache: &TreeCache,
    repo: RepoId,
    ids: &[TreeId],
    private: bool,
) -> Result<Vec<Arc<Tree>>, DeriveError> {
    let mut out: Vec<Option<Arc<Tree>>> =
        ids.iter().map(|id| cache.inner.get(&(repo, *id))).collect();
    let missing: Vec<usize> = (0..ids.len()).filter(|&i| out[i].is_none()).collect();
    if !missing.is_empty() {
        let keys: Vec<MetaKey> = missing
            .iter()
            .map(|&i| {
                if private {
                    MetaKey::CaseNode(ids[i])
                } else {
                    MetaKey::Tree(ids[i])
                }
            })
            .collect();
        let got = meta.get(repo, &keys).await?;
        for (&i, bytes) in missing.iter().zip(got) {
            let bytes = bytes
                .ok_or_else(|| DeriveError::DataLoss(format!("tree {} is missing", ids[i])))?;
            if TreeId::hash(&bytes) != ids[i] {
                return Err(DeriveError::DataLoss(format!(
                    "stored tree {} does not hash to its id",
                    ids[i]
                )));
            }
            let tree = Arc::new(Tree::decode(&bytes).map_err(|e| {
                DeriveError::DataLoss(format!("stored tree {} is malformed: {e}", ids[i]))
            })?);
            cache.insert(repo, ids[i], tree.clone());
            out[i] = Some(tree);
        }
    }
    Ok(out.into_iter().flatten().collect())
}

/// A file change applied to a tree.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Change {
    /// Removes the file.
    Delete,
    /// Sets the path to this file entry (always [`EntryTarget::File`]).
    Set(EntryTarget),
}

/// The result of [`TreeWork::apply`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Applied {
    /// The new root tree.
    pub root: TreeId,
    /// The directories that gained names, with their new nodes.
    pub added: Vec<AddedNames>,
}

/// Names added to one directory by a change set.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AddedNames {
    /// The directory's path; empty for the root.
    pub dir: Vec<u8>,
    /// The directory's new top node.
    pub node: TreeId,
    /// The added names, in byte order.
    pub names: Vec<Vec<u8>>,
}

/// The prefix `P` of a split node, or `None` for a flat node.
pub(crate) fn split_prefix(tree: &Tree) -> Option<&[u8]> {
    tree.entries()
        .iter()
        .find(|e| matches!(e.target, EntryTarget::Shard(_)))
        .map(|e| &e.name[..e.name.len() - 1])
}

enum Step {
    Found(Option<EntryTarget>),
    Descend(TreeId),
}

/// One step of a name lookup within a directory's nodes.
fn step(tree: &Tree, name: &[u8]) -> Step {
    let find = |n: &[u8]| {
        tree.entries()
            .binary_search_by(|e| e.name.as_slice().cmp(n))
            .ok()
            .map(|i| tree.entries()[i].target)
    };
    match split_prefix(tree) {
        None => Step::Found(find(name)),
        Some(p) if name == p => Step::Found(find(name)),
        Some(p) if name.len() > p.len() && name.starts_with(p) => match find(&name[..=p.len()]) {
            Some(EntryTarget::Shard(id)) => Step::Descend(id),
            _ => Step::Found(None),
        },
        Some(_) => Step::Found(None),
    }
}

#[derive(Clone, Copy)]
enum Update {
    Insert(EntryTarget),
    Modify(EntryTarget),
    Delete,
}

/// A built node: its id, and whether it holds more than `MAX_TREE_ENTRIES`
/// names (and so is split).
#[derive(Clone, Copy)]
struct Out {
    id: TreeId,
    big: bool,
}

enum Group {
    Old(TreeId),
    New(Option<Out>),
}

enum Plan {
    File(Change),
    Dir,
    /// The file at this path is deleted and the path becomes a directory.
    FileThenDir,
}

#[derive(Default)]
struct DirState {
    node: Option<TreeId>,
    children: BTreeMap<Vec<u8>, Plan>,
    existing: HashMap<Vec<u8>, EntryTarget>,
    result: Option<TreeId>,
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

fn join(dir: &[u8], name: &[u8]) -> Vec<u8> {
    if dir.is_empty() {
        name.to_vec()
    } else {
        let mut p = Vec::with_capacity(dir.len() + 1 + name.len());
        p.extend_from_slice(dir);
        p.push(b'/');
        p.extend_from_slice(name);
        p
    }
}

fn depth(path: &[u8]) -> usize {
    if path.is_empty() {
        0
    } else {
        1 + path.iter().filter(|&&b| b == b'/').count()
    }
}

/// A violated invariant of the tree code: a defect, not bad stored data.
fn internal(what: &str) -> DeriveError {
    DeriveError::Internal(format!("tree update: {what}"))
}

/// Tree nodes used by one derivation: the nodes it has loaded and the new
/// nodes it has built, which are written before the value that refers to
/// them.
pub struct TreeWork<'a, M> {
    meta: &'a M,
    cache: &'a TreeCache,
    repo: RepoId,
    known: HashMap<TreeId, Arc<Tree>>,
    new: BTreeMap<TreeId, Arc<Tree>>,
    private: bool,
}

impl<M> std::fmt::Debug for TreeWork<'_, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeWork")
            .field("known", &self.known.len())
            .field("new", &self.new.len())
            .finish_non_exhaustive()
    }
}

impl<'a, M: MetaStore> TreeWork<'a, M> {
    pub(crate) fn new(meta: &'a M, cache: &'a TreeCache, repo: RepoId) -> Self {
        Self {
            meta,
            cache,
            repo,
            known: HashMap::new(),
            new: BTreeMap::new(),
            private: false,
        }
    }

    pub(crate) fn private(meta: &'a M, cache: &'a TreeCache, repo: RepoId) -> Self {
        Self {
            private: true,
            ..Self::new(meta, cache, repo)
        }
    }

    pub(crate) fn take_new(&mut self) -> BTreeMap<TreeId, Arc<Tree>> {
        std::mem::take(&mut self.new)
    }

    /// Loads, in one batch, every node in `ids` not already at hand.
    async fn ensure(&mut self, ids: impl IntoIterator<Item = TreeId>) -> Result<(), DeriveError> {
        let mut seen = HashSet::new();
        let need: Vec<TreeId> = ids
            .into_iter()
            .filter(|id| !self.known.contains_key(id) && seen.insert(*id))
            .collect();
        if need.is_empty() {
            return Ok(());
        }
        let trees = load_trees(self.meta, self.cache, self.repo, &need, self.private).await?;
        self.known.extend(need.into_iter().zip(trees));
        Ok(())
    }

    fn node(&self, id: TreeId) -> Result<Arc<Tree>, DeriveError> {
        self.known
            .get(&id)
            .cloned()
            .ok_or_else(|| internal("node not loaded"))
    }

    /// Reads one node.
    pub async fn get(&mut self, id: TreeId) -> Result<Arc<Tree>, DeriveError> {
        self.ensure([id]).await?;
        self.node(id)
    }

    fn put_node(&mut self, entries: Vec<TreeEntry>) -> Result<TreeId, DeriveError> {
        let tree = Tree::new(entries).map_err(|e| internal(&format!("invalid node: {e}")))?;
        let id = tree.id();
        let tree = Arc::new(tree);
        self.known.insert(id, tree.clone());
        self.new.insert(id, tree);
        Ok(id)
    }

    /// Looks up names, each in the directory whose top node is given,
    /// descending through shards in batches across all queries.
    pub async fn lookup_many(
        &mut self,
        queries: &[(TreeId, &[u8])],
    ) -> Result<Vec<Option<EntryTarget>>, DeriveError> {
        let mut cur: Vec<Option<TreeId>> = queries.iter().map(|q| Some(q.0)).collect();
        let mut out = vec![None; queries.len()];
        loop {
            let need: Vec<TreeId> = cur.iter().flatten().copied().collect();
            if need.is_empty() {
                return Ok(out);
            }
            self.ensure(need).await?;
            for (i, (_, name)) in queries.iter().enumerate() {
                if let Some(id) = cur[i] {
                    match step(&*self.node(id)?, name) {
                        Step::Found(t) => {
                            out[i] = t;
                            cur[i] = None;
                        }
                        Step::Descend(s) => cur[i] = Some(s),
                    }
                }
            }
        }
    }

    /// Looks up several paths from one root tree, one batched read per
    /// level of depth across all of them.
    pub async fn lookup_paths(
        &mut self,
        root: TreeId,
        paths: &[RepoPath],
    ) -> Result<Vec<Option<EntryTarget>>, DeriveError> {
        let comps: Vec<Vec<&[u8]>> = paths.iter().map(|p| p.components().collect()).collect();
        let mut dirs: Vec<Option<TreeId>> = vec![Some(root); paths.len()];
        let mut out = vec![None; paths.len()];
        let mut depth = 0;
        loop {
            let live: Vec<usize> = (0..paths.len())
                .filter(|&i| dirs[i].is_some() && depth < comps[i].len())
                .collect();
            if live.is_empty() {
                return Ok(out);
            }
            let queries: Vec<(TreeId, &[u8])> = live
                .iter()
                .filter_map(|&i| dirs[i].map(|d| (d, comps[i][depth])))
                .collect();
            let found = self.lookup_many(&queries).await?;
            for (&i, f) in live.iter().zip(found) {
                if depth + 1 == comps[i].len() {
                    out[i] = f;
                    dirs[i] = None;
                } else {
                    dirs[i] = match f {
                        Some(EntryTarget::Tree(id)) => Some(id),
                        _ => None,
                    };
                }
            }
            depth += 1;
        }
    }

    /// Looks up a path from a root tree.
    pub async fn lookup_path(
        &mut self,
        root: TreeId,
        path: &RepoPath,
    ) -> Result<Option<EntryTarget>, DeriveError> {
        let mut cur = root;
        let comps: Vec<&[u8]> = path.components().collect();
        for (i, c) in comps.iter().enumerate() {
            let found = self.lookup_many(&[(cur, c)]).await?.pop().flatten();
            if i + 1 == comps.len() {
                return Ok(found);
            }
            match found {
                Some(EntryTarget::Tree(id)) => cur = id,
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Every entry of the directory whose top node is `id`, shards
    /// expanded, in name order.
    pub async fn flatten(&mut self, id: TreeId) -> Result<Vec<TreeEntry>, DeriveError> {
        let mut frontier = vec![id];
        while !frontier.is_empty() {
            self.ensure(frontier.iter().copied()).await?;
            let mut next = Vec::new();
            for id in &frontier {
                for e in self.node(*id)?.entries() {
                    if let EntryTarget::Shard(s) = e.target {
                        next.push(s);
                    }
                }
            }
            frontier = next;
        }
        let mut out = Vec::new();
        self.walk(id, &mut out)?;
        Ok(out)
    }

    fn walk(&self, id: TreeId, out: &mut Vec<TreeEntry>) -> Result<(), DeriveError> {
        for e in self.node(id)?.entries() {
            match e.target {
                EntryTarget::Shard(s) => self.walk(s, out)?,
                _ => out.push(e.clone()),
            }
        }
        Ok(())
    }

    /// Up to `limit` names starting with `prefix` in the directory whose top
    /// node is `id`, in name order. Reads only the shards that can hold such
    /// names, and stops once it has `limit`: every shard is non-empty, so
    /// this reads O(`limit` × trie depth) nodes however large the directory.
    pub async fn find_prefixed(
        &mut self,
        id: TreeId,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, DeriveError> {
        enum Item {
            Node(TreeId),
            Name(Vec<u8>),
        }
        let mut out = Vec::new();
        let mut stack = vec![Item::Node(id)];
        while out.len() < limit {
            match stack.pop() {
                None => break,
                Some(Item::Name(n)) => out.push(n),
                Some(Item::Node(id)) => {
                    let node = self.get(id).await?;
                    for e in node.entries().iter().rev() {
                        match e.target {
                            EntryTarget::Shard(s)
                                if e.name.starts_with(prefix) || prefix.starts_with(&e.name) =>
                            {
                                stack.push(Item::Node(s));
                            }
                            EntryTarget::Shard(_) => {}
                            _ if e.name.starts_with(prefix) => {
                                stack.push(Item::Name(e.name.clone()));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Builds the node for an entry set, splitting it when it is too large.
    fn build(
        &mut self,
        entries: BTreeMap<Vec<u8>, EntryTarget>,
    ) -> Result<Option<Out>, DeriveError> {
        if entries.is_empty() {
            return Ok(None);
        }
        let entries: Vec<TreeEntry> = entries
            .into_iter()
            .map(|(name, target)| TreeEntry { name, target })
            .collect();
        self.build_sorted(entries).map(Some)
    }

    fn build_sorted(&mut self, mut entries: Vec<TreeEntry>) -> Result<Out, DeriveError> {
        if entries.len() <= MAX_TREE_ENTRIES {
            return Ok(Out {
                id: self.put_node(entries)?,
                big: false,
            });
        }
        let first = &entries[0].name;
        let last = &entries[entries.len() - 1].name;
        let lcp = first.iter().zip(last).take_while(|(a, b)| a == b).count();
        let prefix = first[..lcp].to_vec();
        let mut node = Vec::new();
        if entries[0].name == prefix {
            node.push(entries.remove(0));
        }
        let mut rest = entries.into_iter().peekable();
        while let Some(head) = rest.next() {
            let byte = head.name[lcp];
            let mut group = vec![head];
            while let Some(e) = rest.next_if(|e| e.name[lcp] == byte) {
                group.push(e);
            }
            let out = self.build_sorted(group)?;
            let mut name = prefix.clone();
            name.push(byte);
            node.push(TreeEntry {
                name,
                target: EntryTarget::Shard(out.id),
            });
        }
        Ok(Out {
            id: self.put_node(node)?,
            big: true,
        })
    }

    /// Whether the groups and the prefix entry together hold more than
    /// `MAX_TREE_ENTRIES` names, reading at most one node per group.
    async fn exceeds(
        &mut self,
        has_prefix_entry: bool,
        groups: &BTreeMap<u8, Group>,
    ) -> Result<bool, DeriveError> {
        // Nodes are read one at a time on purpose: the check stops as soon as
        // the total passes the limit, usually after the first group, so a
        // batched read of every group would read more keys than it saves in
        // round trips, and the keys read are what INV-1 bounds.
        let mut total = usize::from(has_prefix_entry);
        for g in groups.values() {
            let id = match g {
                Group::New(None) => continue,
                Group::New(Some(out)) if out.big => return Ok(true),
                Group::New(Some(out)) => out.id,
                Group::Old(id) => *id,
            };
            let node = self.get(id).await?;
            if split_prefix(&node).is_some() {
                return Ok(true);
            }
            total += node.entries().len();
            if total > MAX_TREE_ENTRIES {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Applies name updates to the node `id` holding a set of full names (a
    /// whole directory, or one shard group of it) and returns the node for
    /// the updated set.
    fn update_node<'s>(
        &'s mut self,
        id: TreeId,
        ups: &'s [(Vec<u8>, Update)],
    ) -> BoxFuture<'s, Result<Option<Out>, DeriveError>> {
        Box::pin(async move {
            let node = self.get(id).await?;
            let Some(prefix) = split_prefix(&node).map(<[u8]>::to_vec) else {
                let mut map: BTreeMap<Vec<u8>, EntryTarget> = node
                    .entries()
                    .iter()
                    .map(|e| (e.name.clone(), e.target))
                    .collect();
                apply_updates(&mut map, ups)?;
                return self.build(map);
            };
            let (prefix, mut prefix_entry, mut groups) =
                if ups.iter().all(|(n, _)| n.starts_with(&prefix)) {
                    let prefix_entry: BTreeMap<Vec<u8>, EntryTarget> = node
                        .entries()
                        .iter()
                        .filter(|e| e.name == prefix)
                        .map(|e| (e.name.clone(), e.target))
                        .collect();
                    let groups: BTreeMap<u8, Group> = node
                        .entries()
                        .iter()
                        .filter_map(|e| match e.target {
                            EntryTarget::Shard(s) => Some((e.name[prefix.len()], Group::Old(s))),
                            _ => None,
                        })
                        .collect();
                    (prefix, prefix_entry, groups)
                } else {
                    // A new name outside the prefix shortens the common
                    // prefix to the part it shares with it. Every old name
                    // starts with the old prefix, so the old node is the one
                    // group for the old prefix's next byte, and is updated
                    // in place like any group (INV-1): no rebuild.
                    let short = ups.iter().fold(prefix.len(), |n, (name, _)| {
                        prefix[..n]
                            .iter()
                            .zip(name)
                            .take_while(|(a, b)| a == b)
                            .count()
                    });
                    let groups = BTreeMap::from([(prefix[short], Group::Old(id))]);
                    (prefix[..short].to_vec(), BTreeMap::new(), groups)
                };
            let mut by_group: BTreeMap<u8, Vec<(Vec<u8>, Update)>> = BTreeMap::new();
            let mut net: i64 = 0;
            let mut own = Vec::new();
            for (name, u) in ups {
                match u {
                    Update::Insert(_) => net += 1,
                    Update::Delete => net -= 1,
                    Update::Modify(_) => {}
                }
                if *name == prefix {
                    own.push((name.clone(), *u));
                } else {
                    by_group
                        .entry(name[prefix.len()])
                        .or_default()
                        .push((name.clone(), *u));
                }
            }
            apply_updates(&mut prefix_entry, &own)?;
            for (byte, gu) in by_group {
                let out = match groups.get(&byte) {
                    Some(Group::Old(gid)) => self.update_node(*gid, &gu).await?,
                    _ => {
                        let mut map = BTreeMap::new();
                        apply_updates(&mut map, &gu)?;
                        self.build(map)?
                    }
                };
                groups.insert(byte, Group::New(out));
            }
            let big = net >= 0 || self.exceeds(!prefix_entry.is_empty(), &groups).await?;
            if !big {
                let mut map = prefix_entry;
                for g in groups.values() {
                    let gid = match g {
                        Group::Old(gid) => *gid,
                        Group::New(Some(out)) => out.id,
                        Group::New(None) => continue,
                    };
                    for e in self.flatten(gid).await? {
                        map.insert(e.name, e.target);
                    }
                }
                return self.build(map);
            }
            let nonempty: Vec<(u8, TreeId)> = groups
                .iter()
                .filter_map(|(b, g)| match g {
                    Group::Old(id) => Some((*b, *id)),
                    Group::New(Some(out)) => Some((*b, out.id)),
                    Group::New(None) => None,
                })
                .collect();
            if prefix_entry.is_empty() && nonempty.len() == 1 {
                // Every name now shares a longer prefix: the directory's node
                // is that one group's node.
                return Ok(Some(Out {
                    id: nonempty[0].1,
                    big: true,
                }));
            }
            let mut entries: Vec<TreeEntry> = prefix_entry
                .into_iter()
                .map(|(name, target)| TreeEntry { name, target })
                .collect();
            for (byte, gid) in nonempty {
                let mut name = prefix.clone();
                name.push(byte);
                entries.push(TreeEntry {
                    name,
                    target: EntryTarget::Shard(gid),
                });
            }
            Ok(Some(Out {
                id: self.put_node(entries)?,
                big: true,
            }))
        })
    }

    /// Applies file changes to the tree `root` (`None` for the empty tree)
    /// and returns the new root. Only the nodes on each changed path's spine
    /// are read and rewritten; names are looked up level by level, in one
    /// batch per level and shard depth.
    ///
    /// Fails with [`DeriveError::Invalid`] when a change is a no-op or puts a
    /// file and a directory at one path.
    pub async fn apply(
        &mut self,
        root: Option<TreeId>,
        changes: &BTreeMap<RepoPath, Change>,
    ) -> Result<Applied, DeriveError> {
        // Plan: which names change in which directories.
        let mut dirs: BTreeMap<Vec<u8>, DirState> = BTreeMap::new();
        dirs.insert(
            Vec::new(),
            DirState {
                node: root,
                ..DirState::default()
            },
        );
        for (path, change) in changes {
            let comps: Vec<&[u8]> = path.components().collect();
            let mut dir = Vec::new();
            for (i, c) in comps.iter().enumerate() {
                let here = join(&dir, c);
                let st = dirs.entry(dir).or_default();
                let last = i + 1 == comps.len();
                match (st.children.get(*c), last) {
                    (None, true) => {
                        st.children.insert(c.to_vec(), Plan::File(*change));
                    }
                    (None, false) => {
                        st.children.insert(c.to_vec(), Plan::Dir);
                    }
                    (Some(Plan::Dir | Plan::FileThenDir), true) => {
                        if *change != Change::Delete {
                            return Err(DeriveError::invalid(InvalidKind::PathConflict, &here));
                        }
                        st.children.insert(c.to_vec(), Plan::FileThenDir);
                    }
                    (Some(Plan::File(Change::Delete)), false) => {
                        st.children.insert(c.to_vec(), Plan::FileThenDir);
                    }
                    (Some(Plan::File(Change::Set(_))), false) => {
                        return Err(DeriveError::invalid(InvalidKind::PathConflict, &here));
                    }
                    (Some(Plan::Dir | Plan::FileThenDir), false) => {}
                    (Some(Plan::File(_)), true) => {
                        return Err(internal("a path is changed twice"));
                    }
                }
                dir = here;
            }
        }
        let max_depth = dirs.keys().map(|d| depth(d)).max().unwrap_or(0);
        let mut by_depth: Vec<Vec<Vec<u8>>> = vec![Vec::new(); max_depth + 1];
        for d in dirs.keys() {
            by_depth[depth(d)].push(d.clone());
        }

        // Read: look up every changed name, a level at a time, and check
        // each change against what is there.
        for level in &by_depth {
            let mut queries: Vec<(TreeId, Vec<u8>, Vec<u8>)> = Vec::new();
            for d in level {
                let st = &dirs[d];
                if let Some(node) = st.node {
                    for name in st.children.keys() {
                        queries.push((node, d.clone(), name.clone()));
                    }
                }
            }
            let q: Vec<(TreeId, &[u8])> = queries
                .iter()
                .map(|(n, _, name)| (*n, name.as_slice()))
                .collect();
            let found = self.lookup_many(&q).await?;
            for ((_, d, name), t) in queries.iter().zip(found) {
                if let Some(t) = t {
                    dirs.get_mut(d)
                        .ok_or_else(|| internal("dir"))?
                        .existing
                        .insert(name.clone(), t);
                }
            }
            for d in level {
                let mut child_nodes = Vec::new();
                let st = &dirs[d];
                for (name, plan) in &st.children {
                    let path = join(d, name);
                    let ex = st.existing.get(name).copied();
                    let is_file = matches!(ex, Some(EntryTarget::File { .. }));
                    match plan {
                        Plan::File(Change::Delete) => match ex {
                            None => return Err(DeriveError::invalid(InvalidKind::NoOp, &path)),
                            Some(_) if !is_file => {
                                return Err(DeriveError::invalid(InvalidKind::PathConflict, &path));
                            }
                            Some(_) => {}
                        },
                        Plan::File(Change::Set(t)) => match ex {
                            None => {}
                            Some(_) if !is_file => {
                                return Err(DeriveError::invalid(InvalidKind::PathConflict, &path));
                            }
                            Some(old) if same_file(&old, t) => {
                                return Err(DeriveError::invalid(InvalidKind::NoOp, &path));
                            }
                            Some(_) => {}
                        },
                        Plan::Dir => match ex {
                            None => child_nodes.push((path, None)),
                            Some(EntryTarget::Tree(id)) => child_nodes.push((path, Some(id))),
                            Some(_) => {
                                return Err(DeriveError::invalid(InvalidKind::PathConflict, &path));
                            }
                        },
                        Plan::FileThenDir => match ex {
                            None => return Err(DeriveError::invalid(InvalidKind::NoOp, &path)),
                            Some(_) if !is_file => {
                                return Err(DeriveError::invalid(InvalidKind::PathConflict, &path));
                            }
                            Some(_) => child_nodes.push((path, None)),
                        },
                    }
                }
                for (path, node) in child_nodes {
                    dirs.get_mut(&path)
                        .ok_or_else(|| internal("child dir"))?
                        .node = node;
                }
            }
        }

        // Write: rebuild each touched directory, deepest first.
        let mut added = Vec::new();
        for level in by_depth.iter().rev() {
            for d in level {
                let st = &dirs[d];
                let mut ups: Vec<(Vec<u8>, Update)> = Vec::new();
                let mut new_names = Vec::new();
                for (name, plan) in &st.children {
                    let ex = st.existing.get(name).copied();
                    let up = match plan {
                        Plan::File(Change::Delete) => Some(Update::Delete),
                        Plan::File(Change::Set(t)) => Some(if ex.is_some() {
                            Update::Modify(*t)
                        } else {
                            Update::Insert(*t)
                        }),
                        Plan::Dir => {
                            let r = dirs[&join(d, name)].result;
                            match (ex, r) {
                                (Some(_), None) => Some(Update::Delete),
                                (Some(EntryTarget::Tree(old)), Some(new)) if old == new => None,
                                (Some(_), Some(new)) => {
                                    Some(Update::Modify(EntryTarget::Tree(new)))
                                }
                                (None, Some(new)) => Some(Update::Insert(EntryTarget::Tree(new))),
                                (None, None) => None,
                            }
                        }
                        Plan::FileThenDir => Some(match dirs[&join(d, name)].result {
                            Some(new) => Update::Modify(EntryTarget::Tree(new)),
                            None => Update::Delete,
                        }),
                    };
                    if let Some(up) = up {
                        if matches!(up, Update::Insert(_)) {
                            new_names.push(name.clone());
                        }
                        ups.push((name.clone(), up));
                    }
                }
                let node = st.node;
                let out = match node {
                    None => {
                        let mut map = BTreeMap::new();
                        apply_updates(&mut map, &ups)?;
                        self.build(map)?
                    }
                    Some(id) if ups.is_empty() => Some(Out { id, big: false }),
                    Some(id) => self.update_node(id, &ups).await?,
                };
                let result = out.map(|o| o.id);
                if let (Some(node), false) = (result, new_names.is_empty()) {
                    added.push(AddedNames {
                        dir: d.clone(),
                        node,
                        names: new_names,
                    });
                }
                dirs.get_mut(d).ok_or_else(|| internal("dir"))?.result = result;
            }
        }
        let root = match dirs[&Vec::new()].result {
            Some(id) => id,
            None => self.put_node(Vec::new())?,
        };
        added.reverse();
        Ok(Applied { root, added })
    }
}

/// Whether two file entries are the same file: same kind and blob.
fn same_file(a: &EntryTarget, b: &EntryTarget) -> bool {
    match (a, b) {
        (
            EntryTarget::File {
                kind: ka, blob: ba, ..
            },
            EntryTarget::File {
                kind: kb, blob: bb, ..
            },
        ) => ka == kb && ba == bb,
        _ => false,
    }
}

fn apply_updates(
    map: &mut BTreeMap<Vec<u8>, EntryTarget>,
    ups: &[(Vec<u8>, Update)],
) -> Result<(), DeriveError> {
    for (name, u) in ups {
        let ok = match u {
            Update::Insert(t) => map.insert(name.clone(), *t).is_none(),
            Update::Modify(t) => map.insert(name.clone(), *t).is_some(),
            Update::Delete => map.remove(name).is_some(),
        };
        if !ok {
            return Err(internal("update does not match the stored entries"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use scion_core::{BlobId, FileKind};

    use super::*;

    fn tree(names: usize, salt: usize) -> Tree {
        Tree::new(
            (0..names)
                .map(|i| TreeEntry {
                    name: format!("n{i:04}").into_bytes(),
                    target: EntryTarget::File {
                        kind: FileKind::Regular,
                        blob: BlobId::hash(&salt.to_le_bytes()),
                        size: 0,
                        sha256: [0; 32],
                    },
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn a_node_weighs_its_names_plus_a_cost_per_entry_and_per_node() {
        // Two five-byte names: 2 × (5 + 96) + 64.
        assert_eq!(tree_weight(&tree(2, 0)), 266);
        assert_eq!(tree_weight(&tree(0, 0)), 64);
    }

    #[test]
    fn the_tree_cache_stays_within_its_byte_budget() {
        let budget = 200_000;
        let cache = TreeCache::new(budget);
        for salt in 0..200 {
            let t = tree(100, salt);
            cache.insert(RepoId(1), t.id(), Arc::new(t));
        }
        cache.inner.run_pending_tasks();
        assert!(
            cache.inner.weighted_size() <= budget,
            "{}",
            cache.inner.weighted_size()
        );
        // Each node weighs about 10 KB, so far fewer than 200 remain, and
        // the cache does hold some.
        assert!(
            cache.inner.entry_count() <= 20,
            "{}",
            cache.inner.entry_count()
        );
        assert!(cache.inner.entry_count() >= 1);
    }
}
