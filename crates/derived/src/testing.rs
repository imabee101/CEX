//! Test support, compiled only with the `testing` feature: a seeded PRNG,
//! and random commit histories of every shape the system meets (AGENTS.md's
//! working rules): a linear history; short side branches; overlapping
//! feature branches forking 10, 50, 150, 500, and 2,000 commits back with a
//! merge every 100 commits; 16 long-lived branches that merge each other;
//! release branches with backports and merges back; octopus merges;
//! criss-cross merges; and several roots merged together.
//!
//! A history is abstract (commit indices and their parents), so any test
//! can turn it into changesets, and memory is linear in its length.

/// A small deterministic PRNG (SplitMix64) for randomized tests; a failing
/// case prints its seed.
#[derive(Clone, Debug)]
pub struct Rng(pub u64);

impl Rng {
    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number below `n` (which must not be 0).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// An index into a slice of length `n` (which must not be 0).
    pub fn index(&mut self, n: usize) -> usize {
        self.below(n as u64) as usize
    }

    /// True with probability `1 / n`.
    pub fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }
}

/// A shape of commit history.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    /// One chain.
    Linear,
    /// A main line with branches of 1 to 5 commits merged back.
    ShortSideBranches,
    /// Feature branches forking 10, 50, 150, 500, or 2,000 main-line commits
    /// back, many open at once, one merged into the main line every 100
    /// commits.
    OverlappingFeatures,
    /// 16 branches that commit and merge each other's tips.
    LongLived,
    /// A main line with release branches cut from it, backports onto the
    /// releases, and releases merged back into the main line.
    Releases,
    /// A main line merging 2 to 20 branches at once.
    Octopus,
    /// Two branches that repeatedly merge each other at the same time, so
    /// later merges have several merge bases.
    CrissCross,
    /// Several independent roots whose lines merge into each other.
    SeveralRoots,
}

impl Shape {
    /// Every shape.
    pub const ALL: [Shape; 8] = [
        Shape::Linear,
        Shape::ShortSideBranches,
        Shape::OverlappingFeatures,
        Shape::LongLived,
        Shape::Releases,
        Shape::Octopus,
        Shape::CrissCross,
        Shape::SeveralRoots,
    ];
}

/// The most commits one step of a generator adds.
pub const MAX_STEP: usize = 8;

/// How far back overlapping feature branches fork, in main-line commits.
pub const FEATURE_FORKS: [usize; 5] = [10, 50, 150, 500, 2000];

/// A commit history: each commit's parents, first parent first, as indices
/// of earlier commits, so indices are in parents-first order.
#[derive(Clone, Debug, Default)]
pub struct History {
    /// Each commit's parents.
    pub parents: Vec<Vec<u32>>,
}

impl History {
    /// The number of commits.
    pub fn len(&self) -> usize {
        self.parents.len()
    }

    /// Whether there are no commits.
    pub fn is_empty(&self) -> bool {
        self.parents.is_empty()
    }

    /// Adds a commit and returns its index.
    fn add(&mut self, parents: Vec<u32>) -> u32 {
        self.parents.push(parents);
        (self.parents.len() - 1) as u32
    }

    /// The branch tips: the commits no other commit has as a parent.
    pub fn tips(&self) -> Vec<u32> {
        let mut child = vec![false; self.len()];
        for ps in &self.parents {
            for &p in ps {
                child[p as usize] = true;
            }
        }
        (0..self.len() as u32)
            .filter(|&i| !child[i as usize])
            .collect()
    }
}

/// A random history of `shape` with at least `n` commits: its last step
/// (such as a side branch and its merge) may add up to [`MAX_STEP`] more.
pub fn history(shape: Shape, n: usize, rng: &mut Rng) -> History {
    let mut h = History::default();
    if n == 0 {
        return h;
    }
    match shape {
        Shape::Linear => linear(&mut h, n),
        Shape::ShortSideBranches => short_side_branches(&mut h, n, rng),
        Shape::OverlappingFeatures => overlapping_features(&mut h, n, rng),
        Shape::LongLived => long_lived(&mut h, n, rng),
        Shape::Releases => releases(&mut h, n, rng),
        Shape::Octopus => octopus(&mut h, n, rng),
        Shape::CrissCross => criss_cross(&mut h, n, rng),
        Shape::SeveralRoots => several_roots(&mut h, n, rng),
    }
    h
}

fn linear(h: &mut History, n: usize) {
    let mut tip = h.add(vec![]);
    while h.len() < n {
        tip = h.add(vec![tip]);
    }
}

fn short_side_branches(h: &mut History, n: usize, rng: &mut Rng) {
    let mut main = h.add(vec![]);
    while h.len() < n {
        if !rng.one_in(4) {
            main = h.add(vec![main]);
            continue;
        }
        let mut side = main;
        for _ in 0..1 + rng.below(5) {
            side = h.add(vec![side]);
        }
        for _ in 0..rng.below(3) {
            main = h.add(vec![main]);
        }
        main = h.add(vec![main, side]);
    }
}

fn overlapping_features(h: &mut History, n: usize, rng: &mut Rng) {
    let mut main = vec![h.add(vec![])];
    let mut open: Vec<u32> = Vec::new();
    while h.len() < n {
        let tip = *main.last().expect("the main line has a root");
        if h.len() % 100 == 99 && !open.is_empty() {
            let b = open.swap_remove(rng.index(open.len()));
            main.push(h.add(vec![tip, b]));
            continue;
        }
        match rng.below(10) {
            0 if open.len() < 16 => {
                let back = FEATURE_FORKS[rng.index(FEATURE_FORKS.len())].min(main.len() - 1);
                open.push(h.add(vec![main[main.len() - 1 - back]]));
            }
            1..=4 if !open.is_empty() => {
                let b = rng.index(open.len());
                open[b] = h.add(vec![open[b]]);
            }
            _ => main.push(h.add(vec![tip])),
        }
    }
}

fn long_lived(h: &mut History, n: usize, rng: &mut Rng) {
    let root = h.add(vec![]);
    let mut tips = [root; 16];
    while h.len() < n {
        let b = rng.index(tips.len());
        let c = rng.index(tips.len());
        tips[b] = if rng.one_in(6) && tips[b] != tips[c] {
            h.add(vec![tips[b], tips[c]])
        } else {
            h.add(vec![tips[b]])
        };
    }
}

fn releases(h: &mut History, n: usize, rng: &mut Rng) {
    let mut main = h.add(vec![]);
    let mut releases: Vec<u32> = Vec::new();
    let mut since_cut = 0;
    while h.len() < n {
        since_cut += 1;
        if since_cut >= 100 + rng.below(100) {
            since_cut = 0;
            if releases.len() == 4 {
                releases.remove(0);
            }
            releases.push(h.add(vec![main]));
            continue;
        }
        match rng.below(20) {
            0..=2 if !releases.is_empty() => {
                // A backport onto a release.
                let r = rng.index(releases.len());
                releases[r] = h.add(vec![releases[r]]);
            }
            3 if !releases.is_empty() => {
                // A release merged back into the main line.
                let r = releases[rng.index(releases.len())];
                main = h.add(vec![main, r]);
            }
            _ => main = h.add(vec![main]),
        }
    }
}

fn octopus(h: &mut History, n: usize, rng: &mut Rng) {
    let mut main = h.add(vec![]);
    let mut open: Vec<u32> = Vec::new();
    let mut want = 2 + rng.below(19) as usize;
    while h.len() < n {
        if open.len() >= want && rng.one_in(4) {
            let mut parents = vec![main];
            parents.append(&mut open);
            main = h.add(parents);
            want = 2 + rng.below(19) as usize;
            continue;
        }
        match rng.below(4) {
            0 => open.push(h.add(vec![main])),
            1 if !open.is_empty() => {
                let b = rng.index(open.len());
                open[b] = h.add(vec![open[b]]);
            }
            _ => main = h.add(vec![main]),
        }
    }
}

fn criss_cross(h: &mut History, n: usize, rng: &mut Rng) {
    let root = h.add(vec![]);
    let (mut a, mut b) = (h.add(vec![root]), h.add(vec![root]));
    while h.len() < n {
        match rng.below(8) {
            0 => {
                let (old_a, old_b) = (a, b);
                a = h.add(vec![old_a, old_b]);
                b = h.add(vec![old_b, old_a]);
            }
            1..=3 => a = h.add(vec![a]),
            _ => b = h.add(vec![b]),
        }
    }
}

fn several_roots(h: &mut History, n: usize, rng: &mut Rng) {
    let mut lanes: Vec<u32> = (0..2 + rng.below(4)).map(|_| h.add(vec![])).collect();
    while h.len() < n {
        let b = rng.index(lanes.len());
        let c = rng.index(lanes.len());
        lanes[b] = match rng.below(12) {
            0 if lanes.len() < 8 => {
                lanes.push(h.add(vec![]));
                continue;
            }
            1 | 2 if b != c => h.add(vec![lanes[b], lanes[c]]),
            _ => h.add(vec![lanes[b]]),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_shape_is_a_parents_first_history_with_its_features() {
        for shape in Shape::ALL {
            for seed in 0..4 {
                let n = 5000;
                let h = history(shape, n, &mut Rng(seed));
                assert!(
                    (n..n + MAX_STEP).contains(&h.len()),
                    "{shape:?}: {}",
                    h.len()
                );
                for (i, ps) in h.parents.iter().enumerate() {
                    assert!(ps.iter().all(|&p| (p as usize) < i), "{shape:?}");
                    let mut sorted = ps.clone();
                    sorted.sort_unstable();
                    sorted.dedup();
                    assert_eq!(sorted.len(), ps.len(), "{shape:?}: a repeated parent");
                    assert!(ps.len() <= 64, "{shape:?}: more than 64 parents");
                }
                let roots = h.parents.iter().filter(|p| p.is_empty()).count();
                let merges = h.parents.iter().filter(|p| p.len() > 1).count();
                let widest = h.parents.iter().map(Vec::len).max().unwrap_or(0);
                match shape {
                    Shape::Linear => assert_eq!((roots, merges), (1, 0)),
                    Shape::SeveralRoots => assert!(roots > 1 && merges > 0, "{roots} {merges}"),
                    Shape::Octopus => assert!(widest > 2 && roots == 1, "{widest}"),
                    _ => assert!(roots == 1 && merges > 0, "{shape:?}: {roots} {merges}"),
                }
                assert!(!h.tips().is_empty());
            }
        }
    }

    #[test]
    fn feature_branches_fork_every_distance_back_and_merge_every_100() {
        let n = 20_000;
        let h = history(Shape::OverlappingFeatures, n, &mut Rng(1));
        // Merges land only on the main line, so the main line is the
        // first-parent chain of the last merge.
        let last_merge = (0..h.len())
            .rev()
            .find(|&i| h.parents[i].len() > 1)
            .unwrap();
        let mut main = vec![last_merge];
        while let Some(&p) = h.parents[*main.last().unwrap()].first() {
            main.push(p as usize);
        }
        main.reverse();
        let mut pos = vec![None; h.len()];
        for (k, &c) in main.iter().enumerate() {
            pos[c] = Some(k);
        }
        // A fork is a commit off the main line whose one parent is on it;
        // it lies `back` commits behind the main line's tip at the time.
        let mut backs = std::collections::BTreeSet::new();
        let mut tip = 0;
        for i in 1..=last_merge {
            match (pos[i], h.parents[i].as_slice()) {
                (Some(k), _) => tip = k,
                (None, [p]) => {
                    if let Some(k) = pos[*p as usize] {
                        backs.insert(tip - k);
                    }
                }
                _ => {}
            }
        }
        for back in FEATURE_FORKS {
            assert!(backs.contains(&back), "no fork {back} back: {backs:?}");
        }
        let merges: Vec<usize> = (0..h.len()).filter(|&i| h.parents[i].len() > 1).collect();
        assert!(merges.iter().all(|i| i % 100 == 99), "{merges:?}");
        assert!(merges.len() >= n / 100 - 2, "{}", merges.len());
    }
}
