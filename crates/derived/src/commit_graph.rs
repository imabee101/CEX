//! CommitGraph: each changeset's generation number and two skew-binary skip
//! pointers, one over the first-parent chain and one over the skip tree,
//! where a changeset's skip-tree parent is the closest common ancestor of its
//! parents in the skip tree. Ancestry queries over these cost O(log n) jumps.

use bytes::Bytes;
use scion_core::{Changeset, ChangesetId};
use store::{BlobStore, MetaStore};

use crate::codec::{Reader, put_uvarint};
use crate::{DeriveCx, DeriveError, DerivedType, Mode, kinds};

/// The CommitGraph type.
#[derive(Clone, Copy, Debug, Default)]
pub struct CommitGraph;

/// One changeset's CommitGraph value.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GraphNode {
    /// The changeset's parents, first parent first.
    pub parents: Vec<ChangesetId>,
    /// 1 for a changeset with no parents, else 1 + the largest parent
    /// generation.
    pub generation: u64,
    /// Depth in the first-parent tree: 0 with no parents.
    pub p1_depth: u64,
    /// Skew-binary jump target in the first-parent tree (itself at depth 0).
    pub p1_skew: ChangesetId,
    /// Depth in the skip tree: 0 with no skip-tree parent.
    pub skip_depth: u64,
    /// The skip-tree parent: the closest common ancestor of the parents in
    /// the skip tree, or `None` when they have none (or there are no
    /// parents).
    pub skip_parent: Option<ChangesetId>,
    /// Skew-binary jump target in the skip tree (itself at depth 0).
    pub skip_skew: ChangesetId,
}

impl GraphNode {
    /// The stored bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, self.parents.len() as u64);
        for p in &self.parents {
            out.extend_from_slice(p.as_bytes());
        }
        put_uvarint(&mut out, self.generation);
        put_uvarint(&mut out, self.p1_depth);
        out.extend_from_slice(self.p1_skew.as_bytes());
        put_uvarint(&mut out, self.skip_depth);
        match &self.skip_parent {
            None => out.push(0),
            Some(p) => {
                out.push(1);
                out.extend_from_slice(p.as_bytes());
            }
        }
        out.extend_from_slice(self.skip_skew.as_bytes());
        out
    }

    /// Decodes stored bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DeriveError> {
        let mut r = Reader::new(bytes, "CommitGraph value");
        let n = r.count(32)?;
        let mut parents = Vec::with_capacity(n);
        for _ in 0..n {
            parents.push(ChangesetId::from_bytes(r.array32()?));
        }
        let generation = r.uvarint()?;
        let p1_depth = r.uvarint()?;
        let p1_skew = ChangesetId::from_bytes(r.array32()?);
        let skip_depth = r.uvarint()?;
        let skip_parent = match r.u8()? {
            0 => None,
            1 => Some(ChangesetId::from_bytes(r.array32()?)),
            _ => return Err(DeriveError::DataLoss("bad CommitGraph option tag".into())),
        };
        let skip_skew = ChangesetId::from_bytes(r.array32()?);
        r.finish()?;
        Ok(Self {
            parents,
            generation,
            p1_depth,
            p1_skew,
            skip_depth,
            skip_parent,
            skip_skew,
        })
    }
}

async fn node<M: MetaStore, B: BlobStore>(
    cx: &mut DeriveCx<'_, M, B>,
    id: ChangesetId,
) -> Result<GraphNode, DeriveError> {
    GraphNode::decode(&cx.value(&CommitGraph, id).await?)
}

/// The skew-binary jump target of a new node whose tree parent is `p`
/// (with `depth`, `skew`, and `skew_depth` reading a node's fields in the
/// tree): the parent's jump target's jump target when the parent's two
/// jumps are equally long, else the parent.
async fn skew_for<M: MetaStore, B: BlobStore>(
    cx: &mut DeriveCx<'_, M, B>,
    p: ChangesetId,
    pn: &GraphNode,
    first_parent_tree: bool,
) -> Result<ChangesetId, DeriveError> {
    let fields = |n: &GraphNode| {
        if first_parent_tree {
            (n.p1_depth, n.p1_skew)
        } else {
            (n.skip_depth, n.skip_skew)
        }
    };
    let (depth, skew) = fields(pn);
    let (jdepth, jj) = fields(&node(cx, skew).await?);
    let (jjdepth, _) = fields(&node(cx, jj).await?);
    Ok(if depth - jdepth == jdepth - jjdepth {
        jj
    } else {
        p
    })
}

/// The closest common ancestor of `a` and `b` in the skip tree, in
/// O(log n) jumps.
async fn skip_lca<M: MetaStore, B: BlobStore>(
    cx: &mut DeriveCx<'_, M, B>,
    a: ChangesetId,
    b: ChangesetId,
) -> Result<Option<ChangesetId>, DeriveError> {
    let mut a = (a, node(cx, a).await?);
    let mut b = (b, node(cx, b).await?);
    if a.1.skip_depth < b.1.skip_depth {
        std::mem::swap(&mut a, &mut b);
    }
    // Raise `a` to `b`'s depth.
    let target = b.1.skip_depth;
    while a.1.skip_depth > target {
        let s = node(cx, a.1.skip_skew).await?;
        a = if s.skip_depth >= target {
            (a.1.skip_skew, s)
        } else {
            let p =
                a.1.skip_parent
                    .ok_or_else(|| DeriveError::DataLoss("skip parent missing".into()))?;
            (p, node(cx, p).await?)
        };
    }
    loop {
        if a.0 == b.0 {
            return Ok(Some(a.0));
        }
        if a.1.skip_depth == 0 {
            return Ok(None);
        }
        if a.1.skip_skew != b.1.skip_skew {
            a = (a.1.skip_skew, node(cx, a.1.skip_skew).await?);
            b = (b.1.skip_skew, node(cx, b.1.skip_skew).await?);
        } else {
            let pa =
                a.1.skip_parent
                    .ok_or_else(|| DeriveError::DataLoss("skip parent missing".into()))?;
            let pb =
                b.1.skip_parent
                    .ok_or_else(|| DeriveError::DataLoss("skip parent missing".into()))?;
            a = (pa, node(cx, pa).await?);
            b = (pb, node(cx, pb).await?);
        }
    }
}

impl DerivedType for CommitGraph {
    const KIND: u16 = kinds::COMMIT_GRAPH;
    const VERSION: u16 = 1;
    const MODE: Mode = Mode::Eager;

    async fn derive<'c, 'd, M: MetaStore, B: BlobStore>(
        &'c self,
        cx: &'c mut DeriveCx<'d, M, B>,
        id: ChangesetId,
        cs: &'c Changeset,
    ) -> Result<Bytes, DeriveError> {
        let parents = cs.parts().parents.clone();
        let mut generation = 1;
        let mut nodes = Vec::with_capacity(parents.len());
        for p in &parents {
            let n = node(cx, *p).await?;
            generation = generation.max(n.generation + 1);
            nodes.push(n);
        }
        let (p1_depth, p1_skew) = match parents.first() {
            None => (0, id),
            Some(p) => (
                nodes[0].p1_depth + 1,
                skew_for(cx, *p, &nodes[0], true).await?,
            ),
        };
        let mut skip_parent = parents.first().copied();
        for p in parents.iter().skip(1) {
            let Some(sp) = skip_parent else { break };
            skip_parent = skip_lca(cx, sp, *p).await?;
        }
        let (skip_depth, skip_skew) = match skip_parent {
            None => (0, id),
            Some(sp) => {
                let spn = node(cx, sp).await?;
                (spn.skip_depth + 1, skew_for(cx, sp, &spn, false).await?)
            }
        };
        Ok(Bytes::from(
            GraphNode {
                parents,
                generation,
                p1_depth,
                p1_skew,
                skip_depth,
                skip_parent,
                skip_skew,
            }
            .encode(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodes_round_trip() {
        let id = |n: u8| ChangesetId::from_bytes([n; 32]);
        for skip_parent in [None, Some(id(4))] {
            let n = GraphNode {
                parents: vec![id(1), id(2)],
                generation: 7,
                p1_depth: 300,
                p1_skew: id(3),
                skip_depth: 2,
                skip_parent,
                skip_skew: id(5),
            };
            assert_eq!(GraphNode::decode(&n.encode()).unwrap(), n);
        }
    }
}
