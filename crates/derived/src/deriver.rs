//! The one generic mechanism every derived type goes through: keyed storage,
//! a bounded RAM cache, per-server coalescing of concurrent derivations, and
//! deriving underived ancestors first.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use scion_core::{Changeset, ChangesetId, Codec, Frame, FrameId, Tree, TreeId};
use store::{BlobStore, DerivedKey, MetaKey, MetaStore, ObjectKey, RepoId};
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

use crate::DeriveError;
use crate::trees::{TreeCache, TreeWork};

/// The zstd level of every frame the server creates. Frames are created once
/// per id (INV-44), so the level only has to be fixed, not reproducible.
pub const FRAME_ZSTD_LEVEL: i32 = 3;

/// The most changesets or values one store read asks for while finding
/// underived ancestors.
pub const DERIVE_BATCH: usize = 256;

/// The most changesets one request derives a type for. A value needing more
/// is pending: the request derives the oldest ones and fails retryably, and
/// each retry continues from where the last stopped.
pub const MAX_DERIVE_PER_REQUEST: usize = 4096;

/// The most ancestors lacking a value that one request explores; a value
/// needing more fails with [`DeriveError::LimitExceeded`].
pub const MAX_DERIVE_EXPLORE: usize = 1 << 16;

/// The most frame writes one derivation has in flight.
pub const FRAME_WRITES: usize = 32;

/// How a type is derived.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    /// Derived inside create, in the transaction that writes the changeset.
    Eager,
    /// Derived on demand or in the background.
    Deferred,
}

/// A derived-data type. A value is a pure function of the changeset and the
/// same type's values for its parents (and, for a type that reads another
/// type, that type's values for the same changeset).
pub trait DerivedType: Send + Sync {
    /// The type's code in [`DerivedKey::kind`].
    const KIND: u16;
    /// The type's format version; a new version is a new key space.
    const VERSION: u16;
    /// Whether create derives it.
    const MODE: Mode;
    /// Whether a value reads its parents' values of this type. A type that
    /// does not is derived for one changeset alone, with no ancestors first.
    const READS_PARENTS: bool = true;

    /// The extra key component, such as an ACL config version; 0 otherwise.
    fn param(&self) -> u64 {
        0
    }

    /// The key of this type's value for `cs`.
    fn key(&self, cs: ChangesetId) -> DerivedKey {
        DerivedKey {
            kind: Self::KIND,
            version: Self::VERSION,
            param: self.param(),
            changeset: cs,
        }
    }

    /// Computes the value for `cs`. New tree nodes it builds go into
    /// `cx.trees` and are written before the value.
    fn derive<'c, 'd, M: MetaStore, B: BlobStore>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        id: ChangesetId,
        cs: &'c Changeset,
    ) -> impl Future<Output = Result<Bytes, DeriveError>> + Send + 'c;
}

/// Sizes of a [`Deriver`]'s RAM caches, in bytes.
#[derive(Clone, Copy, Debug)]
pub struct CacheSizes {
    /// Decoded tree and private index nodes, split evenly between their caches.
    pub trees: u64,
    /// Derived values.
    pub values: u64,
    /// Decoded changesets.
    pub changesets: u64,
}

impl Default for CacheSizes {
    fn default() -> Self {
        Self {
            trees: 256 << 20,
            values: 128 << 20,
            changesets: 128 << 20,
        }
    }
}

type Inflight = Mutex<HashMap<(RepoId, DerivedKey), Arc<OnceCell<Bytes>>>>;

/// Derives and serves derived values for one server. Every value is written
/// insert-if-absent under its `(type, version, param, changeset)` key, and
/// concurrent derivations of one key on this server share one computation
/// and one write (INV-20).
pub struct Deriver<M, B> {
    meta: Arc<M>,
    blobs: Arc<B>,
    trees: TreeCache,
    case_nodes: TreeCache,
    values: moka::sync::Cache<(RepoId, DerivedKey), Bytes>,
    changesets: moka::sync::Cache<(RepoId, ChangesetId), Arc<Changeset>>,
    inflight: Inflight,
}

impl<M, B> std::fmt::Debug for Deriver<M, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deriver").finish_non_exhaustive()
    }
}

/// The working state of one derivation or create: values and tree nodes
/// computed but not yet stored, which later steps read before the store.
pub struct DeriveCx<'d, M, B> {
    deriver: &'d Deriver<M, B>,
    repo: RepoId,
    values: HashMap<DerivedKey, Bytes>,
    /// Tree nodes loaded and built by this derivation.
    pub trees: TreeWork<'d, M>,
    /// Private case-index nodes, with a separate cache and metadata namespace.
    /// These never produce object frames.
    pub case_nodes: TreeWork<'d, M>,
}

impl<M, B> std::fmt::Debug for DeriveCx<'_, M, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeriveCx")
            .field("repo", &self.repo)
            .field("values", &self.values.len())
            .finish_non_exhaustive()
    }
}

impl<'d, M: MetaStore, B: BlobStore> DeriveCx<'d, M, B> {
    /// The repo being derived.
    pub fn repo(&self) -> RepoId {
        self.repo
    }

    /// The deriver.
    pub fn deriver(&self) -> &'d Deriver<M, B> {
        self.deriver
    }

    /// Records a value computed here, for later steps to read.
    pub fn set_value(&mut self, key: DerivedKey, value: Bytes) {
        self.values.insert(key, value);
    }

    /// A value of type `t` for `cs` if it is computed here or stored, without
    /// deriving it.
    pub async fn stored_value<T: DerivedType>(
        &mut self,
        t: &T,
        cs: ChangesetId,
    ) -> Result<Option<Bytes>, DeriveError> {
        if let Some(v) = self.values.get(&t.key(cs)) {
            return Ok(Some(v.clone()));
        }
        Ok(self
            .deriver
            .lookup_many(self.repo, &[t.key(cs)])
            .await?
            .pop()
            .flatten())
    }

    /// A value of type `t` for `cs`: computed here, stored, or derived now
    /// (with its underived ancestors first).
    pub async fn value<T: DerivedType>(
        &mut self,
        t: &T,
        cs: ChangesetId,
    ) -> Result<Bytes, DeriveError> {
        if let Some(v) = self.values.get(&t.key(cs)) {
            return Ok(v.clone());
        }
        let deriver = self.deriver;
        let repo = self.repo;
        // Boxed: deriving a value may derive values of other types.
        let fut: std::pin::Pin<Box<dyn Future<Output = Result<Bytes, DeriveError>> + Send + '_>> =
            Box::pin(deriver.get(repo, t, cs));
        fut.await
    }
}

impl<M: MetaStore, B: BlobStore> Deriver<M, B> {
    /// A deriver over the given stores.
    pub fn new(meta: Arc<M>, blobs: Arc<B>, sizes: CacheSizes) -> Self {
        Self {
            meta,
            blobs,
            trees: TreeCache::new(sizes.trees / 2 + sizes.trees % 2),
            case_nodes: TreeCache::new(sizes.trees / 2),
            values: moka::sync::Cache::builder()
                .max_capacity(sizes.values)
                .weigher(|_, v: &Bytes| u32::try_from(v.len() + 64).unwrap_or(u32::MAX))
                .build(),
            changesets: moka::sync::Cache::builder()
                .max_capacity(sizes.changesets)
                .weigher(|_, c: &Arc<Changeset>| {
                    let p = c.parts();
                    let n = p.message.len()
                        + p.author.len()
                        + p.file_changes
                            .keys()
                            .map(|k| k.as_bytes().len() + 64)
                            .sum::<usize>();
                    u32::try_from(n + 256).unwrap_or(u32::MAX)
                })
                .build(),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// The metadata store.
    pub fn meta(&self) -> &Arc<M> {
        &self.meta
    }

    /// The blob store.
    pub fn blobs(&self) -> &Arc<B> {
        &self.blobs
    }

    /// The tree cache.
    pub fn tree_cache(&self) -> &TreeCache {
        &self.trees
    }

    /// A fresh working state for a derivation or create in `repo`.
    pub fn cx(&self, repo: RepoId) -> DeriveCx<'_, M, B> {
        DeriveCx {
            deriver: self,
            repo,
            values: HashMap::new(),
            trees: TreeWork::new(&*self.meta, &self.trees, repo),
            case_nodes: TreeWork::private(&*self.meta, &self.case_nodes, repo),
        }
    }

    /// Reads a stored changeset, verifying its id.
    pub async fn changeset(
        &self,
        repo: RepoId,
        id: ChangesetId,
    ) -> Result<Arc<Changeset>, DeriveError> {
        self.changesets(repo, &[id])
            .await?
            .pop()
            .ok_or_else(|| DeriveError::Internal("a changeset read was lost".into()))
    }

    /// Stored values, from the cache or one store read.
    pub async fn lookup_many(
        &self,
        repo: RepoId,
        keys: &[DerivedKey],
    ) -> Result<Vec<Option<Bytes>>, DeriveError> {
        let mut out: Vec<Option<Bytes>> =
            keys.iter().map(|k| self.values.get(&(repo, *k))).collect();
        let missing: Vec<usize> = (0..keys.len()).filter(|&i| out[i].is_none()).collect();
        if !missing.is_empty() {
            let mk: Vec<MetaKey> = missing.iter().map(|&i| MetaKey::Derived(keys[i])).collect();
            for (&i, v) in missing.iter().zip(self.meta.get(repo, &mk).await?) {
                if let Some(v) = &v {
                    self.values.insert((repo, keys[i]), v.clone());
                }
                out[i] = v;
            }
        }
        Ok(out)
    }

    /// The value of type `t` for the stored changeset `cs`, deriving it, and
    /// first every ancestor that lacks it when the type reads its parents'
    /// values, if needed.
    ///
    /// The ancestors lacking a value are found level by level from `cs`,
    /// with one batched read of changesets and one of values per level, and
    /// the search stops at ancestors that have one. They are then derived
    /// parents first. One call derives at most [`MAX_DERIVE_PER_REQUEST`]
    /// changesets; a value needing more is [`DeriveError::Pending`] until the
    /// background deriver (or a backfill) has derived the history.
    pub async fn get<T: DerivedType>(
        &self,
        repo: RepoId,
        t: &T,
        cs: ChangesetId,
    ) -> Result<Bytes, DeriveError> {
        self.get_any(repo, t, cs).await
    }

    /// [`Deriver::get`] for a type behind [`AnyType`].
    pub async fn get_any(
        &self,
        repo: RepoId,
        t: &dyn AnyType<M, B>,
        cs: ChangesetId,
    ) -> Result<Bytes, DeriveError> {
        if let Some(v) = self.lookup_many(repo, &[t.key(cs)]).await?.pop().flatten() {
            return Ok(v);
        }
        if t.reads_parents() {
            let order = self.underived(repo, t, cs).await?;
            // Oldest first, so a request that stops at the bound has made
            // progress a retry continues from.
            for chunk in order
                .chunks(DERIVE_BATCH)
                .take(MAX_DERIVE_PER_REQUEST / DERIVE_BATCH)
            {
                for (id, changeset) in chunk.iter().zip(self.changesets(repo, chunk).await?) {
                    self.derive_one(repo, t, *id, &changeset).await?;
                }
            }
            if order.len() > MAX_DERIVE_PER_REQUEST {
                let key = t.key(cs);
                return Err(DeriveError::Pending(format!(
                    "derived type {} version {} for the oldest {MAX_DERIVE_PER_REQUEST} of {} \
                     ancestors of {cs}; retry to continue",
                    key.kind,
                    key.version,
                    order.len()
                )));
            }
        } else {
            let changeset = self.changeset(repo, cs).await?;
            self.derive_one(repo, t, cs, &changeset).await?;
        }
        self.lookup_many(repo, &[t.key(cs)])
            .await?
            .pop()
            .flatten()
            .ok_or_else(|| DeriveError::Internal("a derived value vanished from the cache".into()))
    }

    /// The changesets from `cs` back to the first ancestors that have a
    /// value of type `t`, parents before children (oldest first), or
    /// [`DeriveError::LimitExceeded`] if there are more than
    /// [`MAX_DERIVE_EXPLORE`].
    async fn underived(
        &self,
        repo: RepoId,
        t: &dyn AnyType<M, B>,
        cs: ChangesetId,
    ) -> Result<Vec<ChangesetId>, DeriveError> {
        // Each found changeset's parents; only ids are kept, so memory is
        // bounded by the exploration limit, not by changeset sizes.
        let mut found: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
        let mut level = vec![cs];
        while !level.is_empty() {
            if found.len() + level.len() > MAX_DERIVE_EXPLORE {
                let key = t.key(cs);
                return Err(DeriveError::LimitExceeded(format!(
                    "more than {MAX_DERIVE_EXPLORE} ancestors of {cs} lack derived type {} \
                     version {}",
                    key.kind, key.version
                )));
            }
            let mut parents: Vec<ChangesetId> = Vec::new();
            for chunk in level.chunks(DERIVE_BATCH) {
                for (id, c) in chunk.iter().zip(self.changesets(repo, chunk).await?) {
                    let p = c.parts().parents.clone();
                    parents.extend(&p);
                    found.insert(*id, p);
                }
            }
            parents.sort_unstable();
            parents.dedup();
            parents.retain(|p| !found.contains_key(p));
            let keys: Vec<DerivedKey> = parents.iter().map(|p| t.key(*p)).collect();
            let mut next = Vec::new();
            for chunk in parents.chunks(DERIVE_BATCH).zip(keys.chunks(DERIVE_BATCH)) {
                let values = self.lookup_many(repo, chunk.1).await?;
                next.extend(
                    chunk
                        .0
                        .iter()
                        .zip(values)
                        .filter(|(_, v)| v.is_none())
                        .map(|(p, _)| *p),
                );
            }
            level = next;
        }
        // Parents first: Kahn's algorithm over the found subgraph.
        let mut waiting: HashMap<ChangesetId, usize> = found
            .iter()
            .map(|(id, ps)| (*id, ps.iter().filter(|p| found.contains_key(p)).count()))
            .collect();
        let mut children: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
        for (id, ps) in &found {
            for p in ps {
                if found.contains_key(p) {
                    children.entry(*p).or_default().push(*id);
                }
            }
        }
        let mut ready: Vec<ChangesetId> = waiting
            .iter()
            .filter(|(_, n)| **n == 0)
            .map(|(id, _)| *id)
            .collect();
        ready.sort_unstable();
        let mut order = Vec::with_capacity(found.len());
        while let Some(id) = ready.pop() {
            for child in children.remove(&id).unwrap_or_default() {
                let n = waiting
                    .get_mut(&child)
                    .ok_or_else(|| DeriveError::Internal("a child left the subgraph".into()))?;
                *n -= 1;
                if *n == 0 {
                    ready.push(child);
                }
            }
            order.push(id);
        }
        if order.len() != found.len() {
            return Err(DeriveError::DataLoss(format!(
                "the parents of {cs} form a cycle"
            )));
        }
        Ok(order)
    }

    /// Reads stored changesets, verifying their ids: from the cache, or in
    /// batches of at most [`DERIVE_BATCH`] per store read.
    pub async fn changesets(
        &self,
        repo: RepoId,
        ids: &[ChangesetId],
    ) -> Result<Vec<Arc<Changeset>>, DeriveError> {
        let mut out: Vec<Option<Arc<Changeset>>> = ids
            .iter()
            .map(|id| self.changesets.get(&(repo, *id)))
            .collect();
        let missing: Vec<usize> = (0..ids.len()).filter(|&i| out[i].is_none()).collect();
        for chunk in missing.chunks(DERIVE_BATCH) {
            let keys: Vec<MetaKey> = chunk.iter().map(|&i| MetaKey::Changeset(ids[i])).collect();
            for (&i, bytes) in chunk.iter().zip(self.meta.get(repo, &keys).await?) {
                let id = ids[i];
                let bytes =
                    bytes.ok_or_else(|| DeriveError::NotFound(format!("changeset {id}")))?;
                if ChangesetId::hash(&bytes) != id {
                    return Err(DeriveError::DataLoss(format!(
                        "stored changeset {id} does not hash to its id"
                    )));
                }
                let cs = Arc::new(Changeset::decode(&bytes).map_err(|e| {
                    DeriveError::DataLoss(format!("stored changeset {id} is malformed: {e}"))
                })?);
                self.changesets.insert((repo, id), cs.clone());
                out[i] = Some(cs);
            }
        }
        out.into_iter()
            .map(|c| c.ok_or_else(|| DeriveError::Internal("a changeset read was lost".into())))
            .collect()
    }

    /// Derives and stores one value whose parents' values are stored,
    /// sharing the work with any concurrent derivation of the same key on
    /// this server.
    async fn derive_one(
        &self,
        repo: RepoId,
        t: &dyn AnyType<M, B>,
        id: ChangesetId,
        cs: &Changeset,
    ) -> Result<Bytes, DeriveError> {
        let key = t.key(id);
        let cell = self
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry((repo, key))
            .or_default()
            .clone();
        let result = cell
            .get_or_try_init(|| async {
                // Another server may have stored it since we looked.
                if let Some(v) = self
                    .meta
                    .get(repo, &[MetaKey::Derived(key)])
                    .await?
                    .pop()
                    .flatten()
                {
                    return Ok(v);
                }
                let mut cx = self.cx(repo);
                let value = t.derive_any(&mut cx, id, cs).await?;
                cx.set_value(key, value.clone());
                self.write(cx, Vec::new()).await?;
                Ok::<Bytes, DeriveError>(value)
            })
            .await
            .cloned();
        {
            let mut map = self
                .inflight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if map.get(&(repo, key)).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                map.remove(&(repo, key));
            }
        }
        let value = result?;
        self.values.insert((repo, key), value.clone());
        Ok(value)
    }

    /// Writes a derivation's results: first the frames of its new tree
    /// nodes, then, in one metadata transaction, the tree nodes, the
    /// `extra` rows (such as the changeset itself), and its values. A crash
    /// at any point leaves only unreferenced objects (INV-22).
    pub async fn write(
        &self,
        mut cx: DeriveCx<'_, M, B>,
        extra: Vec<(MetaKey, Bytes)>,
    ) -> Result<(), DeriveError> {
        let repo = cx.repo;
        let new = cx.trees.take_new();
        let case_nodes = cx.case_nodes.take_new();
        self.write_tree_frames(repo, &new).await?;
        let mut rows: Vec<(MetaKey, Bytes)> = new
            .iter()
            .map(|(id, t)| (MetaKey::Tree(*id), Bytes::from(t.encode())))
            .collect();
        rows.extend(extra);
        rows.extend(
            case_nodes
                .iter()
                .map(|(id, t)| (MetaKey::CaseNode(*id), Bytes::from(t.encode()))),
        );
        let values: BTreeMap<DerivedKey, Bytes> = cx.values.drain().collect();
        rows.extend(
            values
                .iter()
                .map(|(k, v)| (MetaKey::Derived(*k), v.clone())),
        );
        self.meta.put(repo, &rows).await?;
        for (id, t) in new {
            self.trees.insert(repo, id, t);
        }
        for (id, t) in case_nodes {
            self.case_nodes.insert(repo, id, t);
        }
        for (k, v) in values {
            self.values.insert((repo, k), v);
        }
        Ok(())
    }

    /// Stores a frame for each tree node, put-if-absent, with bounded
    /// concurrency.
    async fn write_tree_frames(
        &self,
        repo: RepoId,
        nodes: &BTreeMap<TreeId, Arc<Tree>>,
    ) -> Result<(), DeriveError> {
        let mut tasks = JoinSet::new();
        for (id, tree) in nodes {
            if tasks.len() >= FRAME_WRITES
                && let Some(r) = tasks.join_next().await
            {
                r.map_err(|e| DeriveError::Internal(format!("a frame write task failed: {e}")))??;
            }
            let frame = tree_frame(*id, tree)?;
            let blobs = self.blobs.clone();
            let key = ObjectKey::tree(*id);
            tasks.spawn(async move {
                blobs.put_if_absent(repo, key, frame).await?;
                Ok::<(), DeriveError>(())
            });
        }
        while let Some(r) = tasks.join_next().await {
            r.map_err(|e| DeriveError::Internal(format!("a frame write task failed: {e}")))??;
        }
        Ok(())
    }
}

/// Encodes an object's frame: the zstd-compressed bytes when compression
/// shrinks them, else the raw bytes. This is the one place the server picks a
/// frame's codec.
pub fn encode_frame(id: FrameId, raw: &[u8]) -> Result<Bytes, DeriveError> {
    let payload = zstd::bulk::compress(raw, FRAME_ZSTD_LEVEL)
        .map_err(|e| DeriveError::Internal(format!("compressing a frame: {e}")))?;
    let (codec, payload) = if payload.len() < raw.len() {
        (Codec::Zstd, payload.as_slice())
    } else {
        (Codec::Raw, raw)
    };
    Frame::encode(id, codec, raw.len() as u64, payload)
        .map(Bytes::from)
        .map_err(|e| DeriveError::Internal(format!("encoding a frame: {e}")))
}

/// The frame of a tree node.
pub fn tree_frame(id: TreeId, tree: &Tree) -> Result<Bytes, DeriveError> {
    encode_frame(FrameId::Tree(id), &tree.encode())
}

/// A derived type with its parameters fixed, as a trait object, so that a
/// list of types (such as every eager type) can be derived generically.
pub trait AnyType<M, B>: Send + Sync {
    /// The key of this type's value for `cs`.
    fn key(&self, cs: ChangesetId) -> DerivedKey;
    /// Whether create derives it.
    fn mode(&self) -> Mode;
    /// Whether a value reads its parents' values of this type.
    fn reads_parents(&self) -> bool;
    /// Computes the value for `cs`, as [`DerivedType::derive`] does.
    fn derive_any<'c, 'd>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        id: ChangesetId,
        cs: &'c Changeset,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Bytes, DeriveError>> + Send + 'c>>
    where
        'd: 'c;
}

impl<T: DerivedType, M: MetaStore, B: BlobStore> AnyType<M, B> for T {
    fn key(&self, cs: ChangesetId) -> DerivedKey {
        DerivedType::key(self, cs)
    }

    fn mode(&self) -> Mode {
        T::MODE
    }

    fn reads_parents(&self) -> bool {
        T::READS_PARENTS
    }

    fn derive_any<'c, 'd>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        id: ChangesetId,
        cs: &'c Changeset,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Bytes, DeriveError>> + Send + 'c>>
    where
        'd: 'c,
    {
        Box::pin(self.derive(cx, id, cs))
    }
}
