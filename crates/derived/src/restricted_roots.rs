//! RestrictedRoots: for each changeset and ACL config version, the tree id
//! of every restricted directory root, a directory whose path prefix maps to
//! a non-default ACL domain.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use scion_core::wire::{put_bytes, put_uvarint};
use scion_core::{Changeset, ChangesetId, EntryTarget, FormatError, RepoPath, TreeId};
use store::{BlobStore, MetaStore};

use crate::codec::decode;
use crate::{DeriveCx, DeriveError, DerivedType, Mode, RootTree, kinds};

/// One version of a repo's ACL config: path prefixes mapped to ACL domains,
/// with one default domain for every other path.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AclConfig {
    /// The config's version; RestrictedRoots values are keyed by it.
    pub version: u64,
    /// The domain of every path no prefix maps elsewhere.
    pub default_domain: String,
    /// Directory path prefixes and their domains.
    pub domains: BTreeMap<RepoPath, String>,
}

impl AclConfig {
    /// A repo's config before any `SetAclConfig`: version 0, every path in
    /// the default domain.
    pub fn initial() -> Self {
        Self {
            version: 0,
            default_domain: "default".into(),
            domains: BTreeMap::new(),
        }
    }

    /// The prefixes that map to a non-default domain.
    pub fn restricted(&self) -> impl Iterator<Item = &RepoPath> {
        self.domains
            .iter()
            .filter(|(_, d)| **d != self.default_domain)
            .map(|(p, _)| p)
    }
}

/// The RestrictedRoots type for one ACL config version.
#[derive(Clone, Debug)]
pub struct RestrictedRoots {
    config: Arc<AclConfig>,
}

impl RestrictedRoots {
    /// The type keyed by `config`'s version.
    pub fn new(config: Arc<AclConfig>) -> Self {
        Self { config }
    }

    /// Encodes a value: each restricted root present as a directory, in
    /// path order.
    pub fn encode(roots: &BTreeMap<RepoPath, TreeId>) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, roots.len() as u64);
        for (p, id) in roots {
            put_bytes(&mut out, p.as_bytes());
            out.extend_from_slice(id.as_bytes());
        }
        out
    }

    /// Decodes a stored value.
    pub fn decode(bytes: &[u8]) -> Result<BTreeMap<RepoPath, TreeId>, DeriveError> {
        decode("RestrictedRoots value", bytes, |r| {
            let n = r.count(34)?;
            let mut out = BTreeMap::new();
            for _ in 0..n {
                let p = RepoPath::new(r.bytes()?)?;
                // Paths are strictly increasing, so each value has one encoding.
                if out.last_key_value().is_some_and(|(last, _)| *last >= p) {
                    return Err(FormatError::NotSorted);
                }
                out.insert(p, TreeId::from_bytes(r.array32()?));
            }
            Ok(out)
        })
    }
}

/// Whether a change at `changed` can alter the directory at `root`: it is
/// the root, lies beneath it, or is one of its ancestors.
fn touches(changes: &BTreeMap<RepoPath, scion_core::FileChange>, root: &RepoPath) -> bool {
    let r = root.as_bytes();
    let mut under = r.to_vec();
    under.push(b'/');
    // Paths beneath the root sort after `root/`, but names such as
    // `root.c` sort between `root` and `root/`; step over those (each one
    // is a change, so this is O(changed files)).
    let beneath = changes
        .range(root.clone()..)
        .take_while(|(p, _)| p.as_bytes() < under.as_slice() || p.as_bytes().starts_with(&under))
        .any(|(p, _)| p.as_bytes() == r || p.as_bytes().starts_with(&under));
    beneath
        || r.iter()
            .enumerate()
            .filter(|&(_, &b)| b == b'/')
            .any(|(i, _)| RepoPath::new(&r[..i]).is_ok_and(|a| changes.contains_key(&a)))
}

impl DerivedType for RestrictedRoots {
    const KIND: u16 = kinds::RESTRICTED_ROOTS;
    const VERSION: u16 = 1;
    const MODE: Mode = Mode::Eager;
    // The first parent's value is only a shortcut, used when stored.
    const READS_PARENTS: bool = false;

    fn param(&self) -> u64 {
        self.config.version
    }

    async fn derive<'c, 'd, M: MetaStore, B: BlobStore>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        id: ChangesetId,
        cs: &'c Changeset,
    ) -> Result<Bytes, DeriveError> {
        let root = RootTree::decode(&cx.value(&RootTree, id).await?)?;
        let parts = cs.parts();
        // The first parent's value is a shortcut for roots this changeset
        // does not touch, used only when it is already stored: the value
        // never depends on deriving history, so an ACL change costs one
        // lookup per restricted prefix per changeset, not a walk.
        let parent = match parts.parents.first() {
            Some(p) => match cx.stored_value(self, *p).await? {
                Some(v) => Some(Self::decode(&v)?),
                None => None,
            },
            None => None,
        };
        let mut out = BTreeMap::new();
        let mut look = Vec::new();
        for prefix in self.config.restricted() {
            match &parent {
                Some(parent) if !touches(&parts.file_changes, prefix) => {
                    if let Some(t) = parent.get(prefix) {
                        out.insert(prefix.clone(), *t);
                    }
                }
                _ => look.push(prefix.clone()),
            }
        }
        for (prefix, found) in look.iter().zip(cx.trees.lookup_paths(root, &look).await?) {
            if let Some(EntryTarget::Tree(t)) = found {
                out.insert(prefix.clone(), t);
            }
        }
        Ok(Bytes::from(Self::encode(&out)))
    }
}

#[cfg(test)]
mod tests {
    use scion_core::FileChange;

    use super::*;

    fn p(s: &str) -> RepoPath {
        RepoPath::new(s.as_bytes()).unwrap()
    }

    #[test]
    fn touches_the_root_its_contents_and_its_ancestors_only() {
        let root = p("a/b");
        for (changed, want) in [
            ("a/b", true),
            ("a/b/c", true),
            ("a/b/c/d", true),
            ("a", true),
            ("a/bc", false),
            ("a/b.c", false),
            ("a/c", false),
            ("b", false),
            ("a/a", false),
        ] {
            let changes = BTreeMap::from([(p(changed), FileChange::Deleted)]);
            assert_eq!(touches(&changes, &root), want, "{changed}");
        }
        // Siblings sorting between `a/b` and `a/b/` do not hide a change
        // beneath it.
        for siblings in [&["a/b.c"][..], &["a/b!", "a/b-x", "a/b.c"]] {
            let mut changes: BTreeMap<RepoPath, FileChange> = siblings
                .iter()
                .map(|s| (p(s), FileChange::Deleted))
                .collect();
            assert!(!touches(&changes, &root), "{siblings:?}");
            changes.insert(p("a/b/c"), FileChange::Deleted);
            assert!(touches(&changes, &root), "{siblings:?} with a/b/c");
        }
    }

    #[test]
    fn unsorted_or_duplicate_paths_are_rejected() {
        let entry = |p: &str, b: u8| {
            let mut out = Vec::new();
            put_bytes(&mut out, p.as_bytes());
            out.extend_from_slice(&[b; 32]);
            out
        };
        for pair in [("b", "a"), ("a", "a")] {
            let mut bytes = vec![2];
            bytes.extend(entry(pair.0, 1));
            bytes.extend(entry(pair.1, 2));
            assert!(RestrictedRoots::decode(&bytes).is_err(), "{pair:?}");
        }
    }

    #[test]
    fn values_round_trip() {
        let v = BTreeMap::from([
            (p("x"), TreeId::from_bytes([1; 32])),
            (p("x/y"), TreeId::from_bytes([2; 32])),
        ]);
        assert_eq!(
            RestrictedRoots::decode(&RestrictedRoots::encode(&v)).unwrap(),
            v
        );
    }
}
