//! The one generic mechanism every derived type goes through: keyed storage,
//! a bounded RAM cache, per-server coalescing of concurrent derivations, and
//! deriving underived ancestors first.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use scion_core::{Changeset, ChangesetId, Codec, Frame, FrameKind, Tree, TreeId};
use store::{BlobStore, DerivedKey, MetaKey, MetaStore, ObjectKey, RepoId};
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

use crate::DeriveError;
use crate::trees::{TreeCache, TreeWork};

/// The zstd level of every frame the server creates. Frames are created once
/// per id (INV-44), so the level only has to be fixed, not reproducible.
pub const FRAME_ZSTD_LEVEL: i32 = 3;

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
    frame_concurrency: usize,
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
    inputs: HashMap<DerivedKey, Bytes>,
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

    /// Supplies an already stored input without writing it again.
    pub fn set_input(&mut self, key: DerivedKey, value: Bytes) {
        self.inputs.insert(key, value);
    }

    /// A value of type `t` for `cs`: computed here, stored, or derived now
    /// (with its underived ancestors first).
    pub async fn value<T: DerivedType>(
        &mut self,
        t: &T,
        cs: ChangesetId,
    ) -> Result<Bytes, DeriveError> {
        if let Some(v) = self
            .values
            .get(&t.key(cs))
            .or_else(|| self.inputs.get(&t.key(cs)))
        {
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
            frame_concurrency: 32,
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
            inputs: HashMap::new(),
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
        if let Some(c) = self.changesets.get(&(repo, id)) {
            return Ok(c);
        }
        let bytes = self
            .meta
            .get(repo, &[MetaKey::Changeset(id)])
            .await?
            .pop()
            .flatten()
            .ok_or_else(|| DeriveError::NotFound(format!("changeset {id}")))?;
        if ChangesetId::hash(&bytes) != id {
            return Err(DeriveError::DataLoss(format!(
                "stored changeset {id} does not hash to its id"
            )));
        }
        let cs = Arc::new(Changeset::decode(&bytes).map_err(|e| {
            DeriveError::DataLoss(format!("stored changeset {id} is malformed: {e}"))
        })?);
        self.changesets.insert((repo, id), cs.clone());
        Ok(cs)
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
    /// first every underived ancestor, if needed. Ancestors are found by
    /// walking parents from `cs`, reading each changeset's parents' values in
    /// one batch, and derived parents first; the walk stops at derived
    /// values.
    pub async fn get<T: DerivedType>(
        &self,
        repo: RepoId,
        t: &T,
        cs: ChangesetId,
    ) -> Result<Bytes, DeriveError> {
        if let Some(v) = self.lookup_many(repo, &[t.key(cs)]).await?.pop().flatten() {
            return Ok(v);
        }
        let mut stack = vec![cs];
        while let Some(&top) = stack.last() {
            if self.values.contains_key(&(repo, t.key(top))) {
                stack.pop();
                continue;
            }
            let changeset = self.changeset(repo, top).await?;
            let parents = &changeset.parts().parents;
            let keys: Vec<DerivedKey> = parents.iter().map(|p| t.key(*p)).collect();
            let present = self.lookup_many(repo, &keys).await?;
            let missing: Vec<ChangesetId> = parents
                .iter()
                .zip(&present)
                .filter(|(_, v)| v.is_none())
                .map(|(p, _)| *p)
                .collect();
            if missing.is_empty() {
                self.derive_one(repo, t, top, &changeset).await?;
                stack.pop();
            } else {
                stack.extend(missing);
            }
        }
        self.lookup_many(repo, &[t.key(cs)])
            .await?
            .pop()
            .flatten()
            .ok_or_else(|| DeriveError::DataLoss("derived value vanished from the cache".into()))
    }

    /// Derives and stores one value whose parents' values are stored,
    /// sharing the work with any concurrent derivation of the same key on
    /// this server.
    async fn derive_one<T: DerivedType>(
        &self,
        repo: RepoId,
        t: &T,
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
                let value = t.derive(&mut cx, id, cs).await?;
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
            if tasks.len() >= self.frame_concurrency
                && let Some(r) = tasks.join_next().await
            {
                r.map_err(|e| DeriveError::DataLoss(format!("frame task failed: {e}")))??;
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
            r.map_err(|e| DeriveError::DataLoss(format!("frame task failed: {e}")))??;
        }
        Ok(())
    }
}

/// Encodes an object's frame: the zstd-compressed bytes when compression
/// shrinks them, else the raw bytes. This is the one place the server picks a
/// frame's codec.
pub fn encode_frame(kind: FrameKind, id: [u8; 32], raw: &[u8]) -> Result<Bytes, DeriveError> {
    let payload = zstd::bulk::compress(raw, FRAME_ZSTD_LEVEL)
        .map_err(|e| DeriveError::DataLoss(format!("compressing a frame: {e}")))?;
    let (codec, payload) = if payload.len() < raw.len() {
        (Codec::Zstd, payload.as_slice())
    } else {
        (Codec::Raw, raw)
    };
    Frame::encode(kind, id, codec, raw.len() as u64, payload)
        .map(Bytes::from)
        .map_err(|e| DeriveError::DataLoss(format!("encoding a frame: {e}")))
}

/// The frame of a tree node.
pub fn tree_frame(id: TreeId, tree: &Tree) -> Result<Bytes, DeriveError> {
    encode_frame(FrameKind::Tree, *id.as_bytes(), &tree.encode())
}
