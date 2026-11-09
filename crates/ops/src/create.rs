//! CreateChangeset: validate, check that the parents and blobs exist, derive
//! the trees and the other eager values, and write the changeset with them
//! in one transaction. It takes no locks and never reads or writes a ref
//! (INV-2); its cost is O(changed files × path depth) (INV-1).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use derived::{AclConfig, AnyType, CommitGraph, Deriver, Mode, RestrictedRoots, RootTree};
use scion_core::{Changeset, ChangesetId};
use store::{BlobStore, MetaKey, MetaStore, RepoId};
use tokio::sync::OnceCell;

use crate::case::CaseIndex;
use crate::{Code, OpError};

/// The longest author, in bytes.
pub const MAX_AUTHOR_BYTES: usize = 1024;
/// The longest path, in bytes.
pub const MAX_PATH_BYTES: usize = 4096;
/// The longest path component, in bytes.
pub const MAX_COMPONENT_BYTES: usize = 255;
/// The longest message, in bytes.
pub const MAX_MESSAGE_BYTES: usize = 1 << 20;
/// The most parents.
pub const MAX_PARENTS: usize = 64;
/// The longest signature, in bytes.
pub const MAX_SIGNATURE_BYTES: usize = 64 << 10;

/// How a changeset is created.
#[derive(Clone, Debug)]
pub struct CreateOptions {
    /// An imported changeset: it must carry `git_headers`, and is exempt from
    /// the size limits and the case-conflict check.
    pub imported: bool,
    /// The repo's case-conflict setting (on by default).
    pub case_check: bool,
    /// The repo's current ACL config, for RestrictedRoots.
    pub acl: Arc<AclConfig>,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            imported: false,
            case_check: true,
            acl: Arc::new(AclConfig::initial()),
        }
    }
}

type CreateKey = (RepoId, ChangesetId, u64, bool, bool);
type Creating = Mutex<HashMap<CreateKey, Arc<OnceCell<()>>>>;

/// Server-side operations over one pair of stores.
pub struct Ops<M, B> {
    deriver: Arc<Deriver<M, B>>,
    creating: Creating,
}

impl<M, B> std::fmt::Debug for Ops<M, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ops").finish_non_exhaustive()
    }
}

fn limit(what: &str, len: usize, max: usize) -> Result<(), OpError> {
    if len > max {
        return Err(OpError::new(
            Code::LimitExceeded,
            format!("{what} is {len} bytes, more than {max}"),
        ));
    }
    Ok(())
}

/// Checks the fields that need no store access.
fn validate(cs: &Changeset, opts: &CreateOptions) -> Result<(), OpError> {
    let p = cs.parts();
    let mut seen = HashSet::new();
    if let Some(dup) = p.parents.iter().find(|x| !seen.insert(**x)) {
        return Err(OpError::new(
            Code::InvalidArgument,
            format!("parent {dup} is listed twice"),
        ));
    }
    if opts.imported {
        if p.git_headers.is_none() {
            return Err(OpError::new(
                Code::InvalidArgument,
                "an imported changeset needs git_headers",
            ));
        }
        return Ok(());
    }
    if p.git_headers.is_some() {
        return Err(OpError::new(
            Code::InvalidArgument,
            "only imported changesets carry git_headers",
        ));
    }
    if p.author.is_empty() {
        return Err(OpError::new(Code::InvalidArgument, "author is empty"));
    }
    limit("author", p.author.len(), MAX_AUTHOR_BYTES)?;
    limit("message", p.message.len(), MAX_MESSAGE_BYTES)?;
    if let Some(s) = &p.signature {
        limit("signature", s.len(), MAX_SIGNATURE_BYTES)?;
    }
    if p.parents.len() > MAX_PARENTS {
        return Err(OpError::new(
            Code::LimitExceeded,
            format!("{} parents, more than {MAX_PARENTS}", p.parents.len()),
        ));
    }
    for path in p.file_changes.keys() {
        let b = path.as_bytes();
        if b.len() > MAX_PATH_BYTES || path.components().any(|c| c.len() > MAX_COMPONENT_BYTES) {
            return Err(OpError::at(Code::LimitExceeded, "path too long", b));
        }
    }
    Ok(())
}

impl<M: MetaStore, B: BlobStore> Ops<M, B> {
    /// Operations using `deriver` for derived data.
    pub fn new(deriver: Arc<Deriver<M, B>>) -> Self {
        Self {
            deriver,
            creating: Mutex::new(HashMap::new()),
        }
    }

    /// The deriver.
    pub fn deriver(&self) -> &Arc<Deriver<M, B>> {
        &self.deriver
    }

    /// Creates a changeset from its canonical bytes.
    pub async fn create_changeset_bytes(
        &self,
        repo: RepoId,
        bytes: &[u8],
        opts: &CreateOptions,
    ) -> Result<ChangesetId, OpError> {
        let cs = Changeset::decode(bytes)
            .map_err(|e| OpError::new(Code::NotCanonical, e.to_string()))?;
        self.create_changeset(repo, cs, opts).await
    }

    /// Creates a changeset. Creating an existing changeset again writes
    /// only the eager values it lacks for these options (such as
    /// RestrictedRoots for a newer ACL config version). Concurrent creates
    /// with the same id, ACL version, case-check setting, and import setting
    /// on this server share one computation and one write.
    pub async fn create_changeset(
        &self,
        repo: RepoId,
        cs: Changeset,
        opts: &CreateOptions,
    ) -> Result<ChangesetId, OpError> {
        validate(&cs, opts)?;
        let id = cs.id();
        let key = (repo, id, opts.acl.version, opts.case_check, opts.imported);
        let cell = self
            .creating
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(key)
            .or_default()
            .clone();
        let result = cell
            .get_or_try_init(|| self.create_inner(repo, id, &cs, opts))
            .await
            .map(|_| ());
        {
            let mut map = self
                .creating
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if map.get(&key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                map.remove(&key);
            }
        }
        result.map(|()| id)
    }

    async fn create_inner(
        &self,
        repo: RepoId,
        id: ChangesetId,
        cs: &Changeset,
        opts: &CreateOptions,
    ) -> Result<(), OpError> {
        let parts = cs.parts();
        let eager = eager_types::<M, B>(opts);
        let mut keys = vec![MetaKey::Changeset(id)];
        keys.extend(parts.parents.iter().map(|p| MetaKey::Changeset(*p)));
        let found = self.deriver.meta().get(repo, &keys).await?;
        if found[0].is_some() {
            // Created before, perhaps under another ACL config version: make
            // sure every eager value these options name exists. Each is one
            // changeset's derivation, from values its parents already have.
            for t in &eager {
                self.deriver.get_any(repo, &**t, id).await?;
            }
            return Ok(());
        }
        if let Some((p, _)) = parts
            .parents
            .iter()
            .zip(&found[1..])
            .find(|(_, f)| f.is_none())
        {
            return Err(OpError::new(
                Code::NotFound,
                format!("parent changeset {p}"),
            ));
        }
        let mut cx = self.deriver.cx(repo);
        for t in &eager {
            let value = t.derive_any(&mut cx, id, cs).await?;
            cx.set_value(t.key(id), value);
        }
        self.deriver
            .write(cx, vec![(MetaKey::Changeset(id), Bytes::from(cs.encode()))])
            .await?;
        Ok(())
    }
}

/// Every eager derived type, in the order create derives them; later types
/// read earlier types' values for the same changeset. This is the one list
/// of eager types: create derives exactly these, in one transaction with the
/// changeset.
pub fn eager_types<M: MetaStore, B: BlobStore>(
    opts: &CreateOptions,
) -> Vec<Box<dyn AnyType<M, B>>> {
    let types: Vec<Box<dyn AnyType<M, B>>> = vec![
        Box::new(RootTree),
        Box::new(CaseIndex {
            check: opts.case_check && !opts.imported,
        }),
        Box::new(CommitGraph),
        Box::new(RestrictedRoots::new(opts.acl.clone())),
    ];
    debug_assert!(types.iter().all(|t| t.mode() == Mode::Eager));
    types
}
