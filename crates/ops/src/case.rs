//! Case-conflict detection for names in one directory (INV-19).
//!
//! Two names conflict when they are equal after NFC normalization followed
//! by Unicode simple case folding, both per Unicode 18.0 as implemented by
//! `icu_normalizer` and `icu_casemap` 2.4. Names that are not valid UTF-8
//! compare by bytes.

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::HashMap;

use bytes::Bytes;
use derived::{
    AddedNames, Applied, Change, DeriveCx, DeriveError, DerivedType, Mode, TreeWork, kinds,
};
use scion_core::{Changeset, ChangesetId, EntryTarget, FileChange, RepoPath, TreeId};
use store::{BlobStore, MetaStore};

/// The Unicode version case-conflict checks implement.
pub const UNICODE_VERSION: &str = "18.0.0";

/// A name's comparison key: two names conflict exactly when their keys are
/// equal.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FoldKey {
    /// A UTF-8 name, NFC-normalized and simple-case-folded.
    Text(String),
    /// A name that is not valid UTF-8, as is.
    Bytes(Vec<u8>),
}

/// The comparison key of a name.
pub fn fold_key(name: &[u8]) -> FoldKey {
    match std::str::from_utf8(name) {
        Ok(s) => {
            let nfc = icu_normalizer::ComposingNormalizer::new_nfc().normalize(s);
            let cm = icu_casemap::CaseMapper::new();
            FoldKey::Text(nfc.chars().map(|c| cm.simple_fold(c)).collect())
        }
        Err(_) => FoldKey::Bytes(name.to_vec()),
    }
}

/// The first of `added` that conflicts with another name in `all` (the
/// directory's resulting names, which include `added`), with the name it
/// conflicts with: the naive model of [`CaseIndex::find_conflict`].
#[cfg(test)]
fn find_conflict<'a>(
    all: impl IntoIterator<Item = &'a [u8]>,
    added: &'a [Vec<u8>],
) -> Option<(&'a [u8], &'a [u8])> {
    let mut by_key: HashMap<FoldKey, Vec<&[u8]>> = HashMap::new();
    for name in all {
        by_key.entry(fold_key(name)).or_default().push(name);
    }
    for name in added {
        if let Some(other) = by_key[&fold_key(name)]
            .iter()
            .find(|n| **n != name.as_slice())
        {
            return Some((name, other));
        }
    }
    None
}

/// The index name of a directory entry: a tag (`t` for a UTF-8 name, `b`
/// otherwise), the lowercase hex of its [`FoldKey`], `.`, then the name.
/// Names that conflict share everything up to and including the `.`, and
/// the map is injective, so a path maps to exactly one index path.
fn index_name(name: &[u8]) -> Vec<u8> {
    let (tag, key) = match fold_key(name) {
        FoldKey::Text(s) => (b't', s.into_bytes()),
        FoldKey::Bytes(b) => (b'b', b),
    };
    let mut out = Vec::with_capacity(2 + 2 * key.len() + name.len());
    out.push(tag);
    for b in key {
        out.extend_from_slice(format!("{b:02x}").as_bytes());
    }
    out.push(b'.');
    out.extend_from_slice(name);
    out
}

/// The index path of a repo path: each component mapped by [`index_name`].
fn index_path(path: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, c) in path.split(|&b| b == b'/').enumerate() {
        if i > 0 {
            out.push(b'/');
        }
        out.extend(index_name(c));
    }
    out
}

/// The case-conflict index, an eager derived type. Its value is the root of
/// a tree mirroring the changeset's tree with every name replaced by its
/// index name (a tag, the hex of its [`FoldKey`], `.`, then the name), so
/// names that conflict are adjacent and one prefix search finds them. It is
/// derived by rewriting only the spines of the changed paths, like
/// RootTree, which keeps the check O(changed files × path depth) however
/// large a directory is (INV-1, INV-19). It is kept for every changeset,
/// including imported ones and repos with the check off, so later checks
/// can use it.
///
/// With `check` set, deriving it also fails with
/// [`DeriveError::CaseConflict`] when the changeset adds a name that
/// conflicts with another name of its directory. The value and key do not
/// depend on `check`.
#[derive(Clone, Copy, Debug, Default)]
pub struct CaseIndex {
    /// Whether deriving rejects a changeset that adds a conflicting name.
    pub check: bool,
}

impl CaseIndex {
    fn decode(value: &[u8]) -> Result<TreeId, DeriveError> {
        let id: [u8; 32] = value
            .try_into()
            .map_err(|_| DeriveError::DataLoss("CaseIndex value is not 32 bytes".into()))?;
        Ok(TreeId::from_bytes(id))
    }

    /// Applies `cs`'s file changes to its first parent's index; the new
    /// nodes are left in `cx.case_nodes`, never public tree storage. File
    /// entries carry the kind, blob, and size; their SHA-256 is unused and
    /// zero. The added names are index names.
    async fn apply<M: MetaStore, B: BlobStore>(
        cx: &mut DeriveCx<'_, M, B>,
        cs: &Changeset,
    ) -> Result<Applied, DeriveError> {
        let parts = cs.parts();
        let parent = match parts.parents.first() {
            Some(p) => Some(Self::decode(&cx.value(&CaseIndex::default(), *p).await?)?),
            None => None,
        };
        let mut changes = BTreeMap::new();
        for (path, change) in &parts.file_changes {
            let p = RepoPath::new(index_path(path.as_bytes()))
                .map_err(|e| DeriveError::Internal(format!("index path: {e}")))?;
            let c = match change {
                FileChange::Deleted => Change::Delete,
                FileChange::File { kind, blob, size } => Change::Set(EntryTarget::File {
                    kind: *kind,
                    blob: *blob,
                    size: *size,
                    sha256: [0; 32],
                }),
            };
            changes.insert(p, c);
        }
        cx.case_nodes.apply(parent, &changes).await
    }

    /// The path of the first added name (in order) that conflicts with
    /// another name of its directory, with the name it conflicts with. Each
    /// directory's new index node holds the added names next to any name
    /// they conflict with, so one prefix search per added name finds it.
    async fn find_conflict<M: MetaStore>(
        trees: &mut TreeWork<'_, M>,
        added: &[AddedNames],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, DeriveError> {
        for a in added {
            for own in &a.names {
                let key_len = key_len(own)?;
                // At most two names share the key before one differs from
                // `own` itself.
                let found = trees.find_prefixed(a.node, &own[..key_len], 2).await?;
                if let Some(other) = found.into_iter().find(|n| n != own) {
                    let mut path = original_path(&a.dir)?;
                    if !path.is_empty() {
                        path.push(b'/');
                    }
                    path.extend_from_slice(&own[key_len..]);
                    return Ok(Some((path, other[key_len..].to_vec())));
                }
            }
        }
        Ok(None)
    }
}

/// The length of an index name's key: its tag, hex fold key, and `.`. Hex
/// digits and tags hold no `.`, so the key ends at the first one.
fn key_len(index_name: &[u8]) -> Result<usize, DeriveError> {
    index_name
        .iter()
        .position(|&b| b == b'.')
        .map(|i| i + 1)
        .ok_or_else(|| DeriveError::DataLoss("an index name has no key".into()))
}

/// The repo path an index path stands for.
fn original_path(index_path: &[u8]) -> Result<Vec<u8>, DeriveError> {
    let mut out = Vec::new();
    if index_path.is_empty() {
        return Ok(out);
    }
    for (i, c) in index_path.split(|&b| b == b'/').enumerate() {
        if i > 0 {
            out.push(b'/');
        }
        out.extend_from_slice(&c[key_len(c)?..]);
    }
    Ok(out)
}

impl DerivedType for CaseIndex {
    const KIND: u16 = kinds::CASE_INDEX;
    const VERSION: u16 = 2;
    const MODE: Mode = Mode::Eager;

    async fn derive<'c, 'd, M: MetaStore, B: BlobStore>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        _id: ChangesetId,
        cs: &'c Changeset,
    ) -> Result<Bytes, DeriveError> {
        let applied = Self::apply(cx, cs).await?;
        if self.check
            && let Some((path, other)) =
                Self::find_conflict(&mut cx.case_nodes, &applied.added).await?
        {
            return Err(DeriveError::CaseConflict { path, other });
        }
        Ok(Bytes::copy_from_slice(applied.root.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conflict(a: &str, b: &str) -> bool {
        fold_key(a.as_bytes()) == fold_key(b.as_bytes())
    }

    #[test]
    fn case_and_normalization_variants_conflict() {
        assert!(conflict("README", "readme"));
        assert!(conflict("caf\u{e9}", "cafe\u{301}"));
        assert!(conflict("CAF\u{c9}", "cafe\u{301}"));
        assert!(conflict("\u{212a}elvin", "kelvin"));
        assert!(conflict("stra\u{df}e", "STRA\u{1e9e}E"));
        assert!(conflict("\u{3a3}", "\u{3c3}"));
    }

    #[test]
    fn full_folding_and_other_names_do_not_conflict() {
        // Simple folding never expands: sharp s is not "ss".
        assert!(!conflict("stra\u{df}e", "strasse"));
        assert!(!conflict("a", "b"));
        assert!(!conflict("a.txt", "a.TXT "));
        // Simple folding maps final sigma to sigma.
        assert!(conflict("\u{3c2}", "\u{3c3}"));
    }

    #[test]
    fn non_utf8_names_compare_by_bytes() {
        assert_eq!(fold_key(b"\xff"), FoldKey::Bytes(vec![0xff]));
        assert_ne!(fold_key(b"A\xff"), fold_key(b"a\xff"));
        assert_ne!(fold_key(b"\xff"), fold_key(b"\xfe"));
    }

    #[test]
    fn finds_a_conflict_with_an_existing_name_only() {
        let all: Vec<&[u8]> = vec![b"Foo", b"bar", b"foo"];
        let added = vec![b"foo".to_vec()];
        assert_eq!(
            find_conflict(all.iter().copied(), &added),
            Some((&b"foo"[..], &b"Foo"[..]))
        );
        let all: Vec<&[u8]> = vec![b"Foo", b"bar"];
        let added = vec![b"bar".to_vec()];
        assert_eq!(find_conflict(all.iter().copied(), &added), None);
    }
}
