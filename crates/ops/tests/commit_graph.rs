//! CommitGraph equals a model that shares nothing with it, on Postgres, for
//! random histories of every shape AGENTS.md lists: generation numbers from
//! the longest path to a root; first-parent depths and jump targets by the
//! canonical skew-binary decomposition; and the skip tree as the dominator
//! tree, computed as dominator sets (a commit's dominators are itself and
//! every commit on all of its paths to a root). Queries on random pairs
//! (old against recent, branch tips) answered by jumping along the stored
//! pointers equal brute-force answers within O(log n) jumps.

mod common;

use std::collections::HashMap;

use bytes::Bytes;
use common::*;
use derived::testing::{History, Shape, history};
use derived::{CommitGraph, GraphNode};
use scion_core::ChangesetId;
use store::{MetaKey, MetaStore};

/// A set of commit indices.
#[derive(Clone)]
struct Bits(Vec<u64>);

impl Bits {
    fn new(n: usize) -> Self {
        Self(vec![0; n.div_ceil(64)])
    }
    fn insert(&mut self, i: u32) {
        self.0[i as usize / 64] |= 1 << (i % 64);
    }
    fn intersect(&mut self, other: &Bits) {
        for (a, b) in self.0.iter_mut().zip(&other.0) {
            *a &= b;
        }
    }
    fn len(&self) -> u32 {
        self.0.iter().map(|w| w.count_ones()).sum()
    }
    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().enumerate().flat_map(|(w, &bits)| {
            (0..64)
                .filter(move |b| bits & (1 << b) != 0)
                .map(move |b| (w * 64 + b) as u32)
        })
    }
}

/// The skew-binary jump length at depth `d > 0`: the smallest term
/// `2^k - 1` of `d` written greedily as a sum of such terms, largest first.
fn skew_jump(mut d: u64) -> u64 {
    let mut last = 0;
    while d > 0 {
        let mut w = 1;
        while 2 * w < d {
            w = 2 * w + 1;
        }
        d -= w;
        last = w;
    }
    last
}

/// The model of one history.
struct Model {
    generation: Vec<u64>,
    /// Each commit's first-parent chain, from itself to its root.
    p1_depth: Vec<u64>,
    /// Each commit's dominators, itself included.
    dom: Vec<Bits>,
    /// Each commit's depth in the dominator tree: its strict dominators.
    dom_depth: Vec<u64>,
}

impl Model {
    fn new(h: &History) -> Self {
        let n = h.len();
        let mut generation = vec![0u64; n];
        let mut p1_depth = vec![0u64; n];
        let mut dom: Vec<Bits> = Vec::with_capacity(n);
        for (i, ps) in h.parents.iter().enumerate() {
            generation[i] = 1 + ps
                .iter()
                .map(|&p| generation[p as usize])
                .max()
                .unwrap_or(0);
            p1_depth[i] = ps.first().map_or(0, |&p| p1_depth[p as usize] + 1);
            let mut d = match ps.split_first() {
                None => Bits::new(n),
                Some((&first, rest)) => {
                    let mut d = dom[first as usize].clone();
                    for &p in rest {
                        d.intersect(&dom[p as usize]);
                    }
                    d
                }
            };
            d.insert(i as u32);
            dom.push(d);
        }
        Self {
            generation,
            p1_depth,
            dom_depth: dom.iter().map(|d| u64::from(d.len()) - 1).collect(),
            dom,
        }
    }

    /// The first-parent ancestor of `i` at depth `depth`.
    fn p1_at(&self, h: &History, mut i: u32, depth: u64) -> u32 {
        while self.p1_depth[i as usize] > depth {
            i = h.parents[i as usize][0];
        }
        i
    }

    /// Depth in the dominator tree.
    fn dom_depth(&self, i: u32) -> u64 {
        self.dom_depth[i as usize]
    }

    /// The dominator of `i` at dominator-tree depth `depth`.
    fn dom_at(&self, i: u32, depth: u64) -> u32 {
        self.dom[i as usize]
            .iter()
            .find(|&x| self.dom_depth(x) == depth)
            .expect("dominators form a chain with one commit per depth")
    }

    /// The immediate dominator: the deepest strict dominator.
    fn idom(&self, i: u32) -> Option<u32> {
        let d = self.dom_depth(i);
        (d > 0).then(|| self.dom_at(i, d - 1))
    }
}

/// Whether `v` can reach a root through parents without passing `avoid`.
fn reaches_root_avoiding(h: &History, v: u32, avoid: u32) -> bool {
    let mut seen = vec![false; h.len()];
    let mut stack = vec![v];
    while let Some(x) = stack.pop() {
        if x == avoid || std::mem::replace(&mut seen[x as usize], true) {
            continue;
        }
        if h.parents[x as usize].is_empty() {
            return true;
        }
        stack.extend(&h.parents[x as usize]);
    }
    false
}

/// One of the two trees CommitGraph stores pointers for.
struct TreeView<'a> {
    nodes: &'a [GraphNode],
    index: &'a HashMap<ChangesetId, u32>,
    first_parent: bool,
}

impl TreeView<'_> {
    fn depth(&self, i: u32) -> u64 {
        let n = &self.nodes[i as usize];
        if self.first_parent {
            n.p1_depth
        } else {
            n.skip_depth
        }
    }
    fn skew(&self, i: u32) -> u32 {
        let n = &self.nodes[i as usize];
        self.index[if self.first_parent {
            &n.p1_skew
        } else {
            &n.skip_skew
        }]
    }
    fn parent(&self, i: u32) -> u32 {
        let n = &self.nodes[i as usize];
        let p = if self.first_parent {
            n.parents.first()
        } else {
            n.skip_parent.as_ref()
        };
        self.index[p.expect("a node above depth 0 has a tree parent")]
    }

    /// The ancestor of `i` at `depth`, and the jumps taken.
    fn at(&self, mut i: u32, depth: u64, jumps: &mut u32) -> u32 {
        while self.depth(i) > depth {
            let s = self.skew(i);
            i = if self.depth(s) >= depth {
                s
            } else {
                self.parent(i)
            };
            *jumps += 1;
        }
        i
    }

    /// The lowest common ancestor, and the jumps taken.
    fn lca(&self, a: u32, b: u32, jumps: &mut u32) -> Option<u32> {
        let d = self.depth(a).min(self.depth(b));
        let (mut a, mut b) = (self.at(a, d, jumps), self.at(b, d, jumps));
        loop {
            if a == b {
                return Some(a);
            }
            if self.depth(a) == 0 {
                return None;
            }
            if self.skew(a) == self.skew(b) {
                (a, b) = (self.parent(a), self.parent(b));
            } else {
                (a, b) = (self.skew(a), self.skew(b));
            }
            *jumps += 1;
        }
    }
}

/// Random query commits: old, recent, branch tips, and any.
fn pick(rng: &mut Rng, n: usize, tips: &[u32]) -> u32 {
    match rng.below(4) {
        0 => rng.index(n / 10 + 1) as u32,
        1 => (n - 1 - rng.index(n / 10 + 1)) as u32,
        2 => tips[rng.index(tips.len())],
        _ => rng.index(n) as u32,
    }
}

async fn check(shape: Shape, n: usize, seed: u64) {
    let e = env().await;
    let mut rng = Rng(seed);
    let h = history(shape, n, &mut rng);
    let n = h.len();
    let mut ids: Vec<ChangesetId> = Vec::with_capacity(n);
    let mut rows = Vec::new();
    for (i, ps) in h.parents.iter().enumerate() {
        let parents: Vec<ChangesetId> = ps.iter().map(|&p| ids[p as usize]).collect();
        let cs = changeset(&parents, &format!("{shape:?} {seed} {i}"), vec![]);
        ids.push(cs.id());
        rows.push((MetaKey::Changeset(cs.id()), Bytes::from(cs.encode())));
    }
    for batch in rows.chunks(1000) {
        e.meta.put(REPO, batch).await.unwrap();
    }
    let o = ops(&e.meta, &e.blobs);
    // Derive from the branch tips, which reach every commit, so the deriver
    // finds the underived ancestors in batched reads; a tip with more than
    // one request's bound of them is retried until it is done.
    for &tip in &h.tips() {
        loop {
            match o.deriver().get(REPO, &CommitGraph, ids[tip as usize]).await {
                Ok(_) => break,
                Err(derived::DeriveError::Pending(_)) => {}
                Err(e) => panic!("{shape:?}: {e}"),
            }
        }
    }
    let mut nodes = Vec::with_capacity(n);
    for id in &ids {
        let v = o.deriver().get(REPO, &CommitGraph, *id).await.unwrap();
        nodes.push(GraphNode::decode(&v).unwrap());
    }
    let index: HashMap<ChangesetId, u32> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (*id, i as u32))
        .collect();
    let m = Model::new(&h);
    for i in 0..n as u32 {
        let (node, at) = (
            &nodes[i as usize],
            format!("{shape:?} seed {seed} commit {i}"),
        );
        let parents: Vec<ChangesetId> = h.parents[i as usize]
            .iter()
            .map(|&p| ids[p as usize])
            .collect();
        assert_eq!(node.parents, parents, "{at}");
        assert_eq!(node.generation, m.generation[i as usize], "{at}");
        let d = m.p1_depth[i as usize];
        assert_eq!(node.p1_depth, d, "{at}");
        let jump = if d == 0 { 0 } else { skew_jump(d) };
        assert_eq!(node.p1_skew, ids[m.p1_at(&h, i, d - jump) as usize], "{at}");
        assert_eq!(node.skip_parent, m.idom(i).map(|p| ids[p as usize]), "{at}");
        let d = m.dom_depth(i);
        assert_eq!(node.skip_depth, d, "{at}");
        let jump = if d == 0 { 0 } else { skew_jump(d) };
        assert_eq!(node.skip_skew, ids[m.dom_at(i, d - jump) as usize], "{at}");
    }
    // The dominator sets are what their definition says: removing a
    // commit's immediate dominator cuts it off from every root.
    for _ in 0..20 {
        let v = rng.index(n) as u32;
        if let Some(d) = m.idom(v) {
            assert!(
                !reaches_root_avoiding(&h, v, d),
                "{shape:?}: {v} avoids {d}"
            );
        }
    }
    // Queries along the stored pointers.
    let tips = h.tips();
    let log = u32::BITS - (n as u32).leading_zeros();
    let views = [true, false].map(|first_parent| TreeView {
        nodes: &nodes,
        index: &index,
        first_parent,
    });
    for _ in 0..300 {
        let (a, b) = (pick(&mut rng, n, &tips), pick(&mut rng, n, &tips));
        // The nth first-parent ancestor.
        let p1 = &views[0];
        let depth = m.p1_depth[a as usize];
        let k = rng.below(depth + 1);
        let mut jumps = 0;
        assert_eq!(p1.at(a, depth - k, &mut jumps), m.p1_at(&h, a, depth - k));
        assert!(
            jumps <= 3 * log,
            "{shape:?}: {jumps} jumps for an ancestor of {a}"
        );
        // The first-parent lowest common ancestor.
        let mut chain = std::collections::HashSet::new();
        let mut x = Some(a);
        while let Some(c) = x {
            chain.insert(c);
            x = h.parents[c as usize].first().copied();
        }
        let mut want = Some(b);
        while let Some(c) = want.filter(|c| !chain.contains(c)) {
            want = h.parents[c as usize].first().copied();
        }
        let mut jumps = 0;
        assert_eq!(p1.lca(a, b, &mut jumps), want, "{shape:?}: {a} {b}");
        assert!(jumps <= 6 * log, "{shape:?}: {jumps} jumps for {a} {b}");
        // The closest common dominator.
        let mut common = m.dom[a as usize].clone();
        common.intersect(&m.dom[b as usize]);
        let want = common.iter().max_by_key(|&x| m.dom_depth(x));
        let mut jumps = 0;
        assert_eq!(views[1].lca(a, b, &mut jumps), want, "{shape:?}: {a} {b}");
        assert!(jumps <= 6 * log, "{shape:?}: {jumps} jumps for {a} {b}");
    }
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_on_a_linear_history() {
    check(Shape::Linear, 3000, 1).await;
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_with_short_side_branches() {
    check(Shape::ShortSideBranches, 3000, 2).await;
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_with_overlapping_feature_branches() {
    // Long enough for branches forking 2,000 main-line commits back.
    check(Shape::OverlappingFeatures, 4500, 3).await;
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_with_long_lived_branches() {
    check(Shape::LongLived, 3000, 4).await;
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_with_release_branches() {
    check(Shape::Releases, 3000, 5).await;
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_with_octopus_merges() {
    check(Shape::Octopus, 3000, 6).await;
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_with_criss_cross_merges() {
    check(Shape::CrissCross, 3000, 7).await;
}

#[tokio::test]
async fn slow_commit_graph_equals_the_model_with_several_roots() {
    check(Shape::SeveralRoots, 3000, 8).await;
}
