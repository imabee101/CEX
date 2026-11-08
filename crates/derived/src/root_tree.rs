//! RootTree: the root tree id of each changeset, built by rewriting only the
//! trees on each changed path's spine of the first parent's tree.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use scion_core::{BlobId, Changeset, ChangesetId, EntryTarget, FileChange, TreeId};
use store::{BlobStore, MetaKey, MetaStore};

use crate::trees::{Applied, Change};
use crate::{BlobRecord, DeriveCx, DeriveError, DerivedType, InvalidKind, Mode, kinds};

/// The RootTree type. Its value is the 32-byte root [`TreeId`].
#[derive(Clone, Copy, Debug, Default)]
pub struct RootTree;

impl RootTree {
    /// Decodes a stored value.
    pub fn decode(value: &[u8]) -> Result<TreeId, DeriveError> {
        let id: [u8; 32] = value
            .try_into()
            .map_err(|_| DeriveError::DataLoss("RootTree value is not 32 bytes".into()))?;
        Ok(TreeId::from_bytes(id))
    }

    /// Applies `cs`'s file changes to its first parent's root tree. The new
    /// nodes are left in `cx.trees`. Each written file's size must equal its
    /// blob's recorded size.
    pub async fn apply<M: MetaStore, B: BlobStore>(
        cx: &mut DeriveCx<'_, M, B>,
        cs: &Changeset,
    ) -> Result<Applied, DeriveError> {
        let parts = cs.parts();
        let parent_root = match parts.parents.first() {
            Some(p) => Some(Self::decode(&cx.value(&RootTree, *p).await?)?),
            None => None,
        };
        let mut seen = HashSet::new();
        let blobs: Vec<BlobId> = parts
            .file_changes
            .values()
            .filter_map(|c| match c {
                FileChange::File { blob, .. } if seen.insert(*blob) => Some(*blob),
                _ => None,
            })
            .collect();
        let keys: Vec<MetaKey> = blobs.iter().map(|b| MetaKey::Blob(*b)).collect();
        let got = cx.deriver().meta().get(cx.repo(), &keys).await?;
        let mut records = std::collections::HashMap::new();
        for (blob, rec) in blobs.iter().zip(got) {
            let rec = rec.ok_or_else(|| DeriveError::NotFound(format!("blob {blob}")))?;
            records.insert(*blob, BlobRecord::decode(&rec)?);
        }
        let mut changes = BTreeMap::new();
        for (path, change) in &parts.file_changes {
            let c = match change {
                FileChange::Deleted => Change::Delete,
                FileChange::File { kind, blob, size } => {
                    let rec = records[blob];
                    if rec.size != *size {
                        return Err(DeriveError::invalid(
                            InvalidKind::SizeMismatch,
                            path.as_bytes(),
                        ));
                    }
                    Change::Set(EntryTarget::File {
                        kind: *kind,
                        blob: *blob,
                        size: *size,
                        sha256: rec.sha256,
                    })
                }
            };
            changes.insert(path.clone(), c);
        }
        cx.trees.apply(parent_root, &changes).await
    }
}

impl DerivedType for RootTree {
    const KIND: u16 = kinds::ROOT_TREE;
    const VERSION: u16 = 1;
    const MODE: Mode = Mode::Eager;

    async fn derive<'c, 'd, M: MetaStore, B: BlobStore>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        _id: ChangesetId,
        cs: &'c Changeset,
    ) -> Result<Bytes, DeriveError> {
        let applied = Self::apply(cx, cs).await?;
        Ok(Bytes::copy_from_slice(applied.root.as_bytes()))
    }
}
