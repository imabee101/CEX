//! The bench repo generator.
//!
//! [`layout`] lays out a deterministic tree of files from [`BenchParams`]:
//! directories of depth at most `max_depth` and at most `max_fanout`
//! entries, file sizes log-normal with the given median and σ, capped, and
//! filled with seeded pseudo-random bytes. [`generate`] builds that tree as
//! a repository tip through the real write path (blob upload and
//! CreateChangeset, in changesets of at most `files_per_changeset` files)
//! into any stores it is given.
//!
//! This builds the bench repo's tip only. The full bench repo, with its
//! 1,000,000-changeset history and a merge every 100 changesets, is built by
//! the benchmark harness of milestone 13.
//!
//! Every computation is integer or IEEE-754 basic arithmetic, so the same
//! parameters give the same files and ids on every machine.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use derived::{CacheSizes, Deriver};

use ops::blob::upload_blobs;
use ops::{CreateOptions, Ops};
use scion_core::{
    BlobId, Changeset, ChangesetId, ChangesetParts, FileChange, FileKind, RepoPath, Timestamp,
};
use store::{BlobStore, MetaStore, RepoId};

/// Bumped whenever the generator's output changes, so old caches are not
/// reused.
pub const GENERATOR_VERSION: u32 = 2;

/// The repo id the bench repo is stored under.
pub const REPO: RepoId = RepoId(1);

/// The bench repo's parameters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BenchParams {
    /// The PRNG seed.
    pub seed: u64,
    /// Files in the tip.
    pub files: u64,
    /// The deepest file path, in components.
    pub max_depth: usize,
    /// The most entries in one directory.
    pub max_fanout: usize,
    /// The median file size, in bytes.
    pub median_size: f64,
    /// The σ of the file sizes' natural logarithm.
    pub sigma: f64,
    /// The largest file, in bytes.
    pub max_size: u64,
    /// The most files one changeset adds.
    pub files_per_changeset: usize,
}

/// The tip of the bench repo of AGENTS.md: 3,000,000 files, depth ≤ 12, fanout
/// ≤ 64, log-normal sizes with median 2 KiB and σ 2.0 capped at 64 MiB, seed
/// `0x5C10_2026`, built in changesets of at most 2,000 files (the spec's
/// largest changeset).
pub const BENCH_TIP: BenchParams = BenchParams {
    seed: 0x5C10_2026,
    files: 3_000_000,
    max_depth: 12,
    max_fanout: 64,
    median_size: 2048.0,
    sigma: 2.0,
    max_size: 64 << 20,
    files_per_changeset: 2000,
};

/// One file of the layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSpec {
    /// The file's path.
    pub path: String,
    /// Its size in bytes.
    pub size: u64,
    /// Its index in the layout, which seeds its contents.
    pub index: u64,
}

impl FileSpec {
    /// The number of components in the path.
    pub fn depth(&self) -> usize {
        self.path.split('/').count()
    }
}

/// SplitMix64.
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

    /// A number in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// A uniform number in [0, 1), exactly representable.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A standard normal sample by Marsaglia's polar method. It has the
    /// normal distribution's full tails, which set the size of the largest
    /// files, and uses only basic IEEE-754 operations, `sqrt` (correctly
    /// rounded), and [`det_ln`], so it is the same on every machine.
    fn normal(&mut self) -> f64 {
        loop {
            let u = 2.0 * self.unit() - 1.0;
            let v = 2.0 * self.unit() - 1.0;
            let s = u * u + v * v;
            if s > 0.0 && s < 1.0 {
                return u * (-2.0 * det_ln(s) / s).sqrt();
            }
        }
    }
}

/// e^x from basic IEEE-754 operations only: range reduction by ln 2 and a
/// Taylor series, so the result is the same on every machine.
fn det_exp(x: f64) -> f64 {
    const LN2: f64 = std::f64::consts::LN_2;
    let k = (x / LN2).round();
    let r = x - k * LN2;
    let mut term = 1.0;
    let mut sum = 1.0;
    for i in 1..=25 {
        term = term * r / f64::from(i);
        sum += term;
    }
    let k = k as i64;
    sum * f64::from_bits(((1023 + k) as u64) << 52)
}

/// ln x for a finite x > 0 from basic IEEE-754 operations only: x = m · 2^e
/// with m in [1, 2), and ln m = 2 atanh((m − 1) / (m + 1)) by its series,
/// so the result is the same on every machine.
fn det_ln(x: f64) -> f64 {
    const LN2: f64 = std::f64::consts::LN_2;
    let bits = x.to_bits();
    let (m, e) = if bits >> 52 == 0 {
        // Subnormal: scale into the normal range first.
        let scaled = x * f64::from_bits(((1023 + 64) as u64) << 52);
        let b = scaled.to_bits();
        (
            f64::from_bits((b & ((1 << 52) - 1)) | (1023 << 52)),
            (b >> 52) as i64 - 1023 - 64,
        )
    } else {
        (
            f64::from_bits((bits & ((1 << 52) - 1)) | (1023 << 52)),
            (bits >> 52) as i64 - 1023,
        )
    };
    let t = (m - 1.0) / (m + 1.0);
    let t2 = t * t;
    let mut term = t;
    let mut sum = 0.0;
    for k in 0..40 {
        sum += term / f64::from(2 * k + 1);
        term *= t2;
    }
    e as f64 * LN2 + 2.0 * sum
}

fn file_size(p: &BenchParams, rng: &mut Rng) -> u64 {
    let s = (p.median_size * det_exp(p.sigma * rng.normal())).round();
    (s as u64).min(p.max_size)
}

/// The most files a directory at `depth` may hold beneath it so that every
/// file stays within `max_depth` and every directory within `max_fanout`.
fn capacity(p: &BenchParams, depth: usize) -> u64 {
    let half = (p.max_fanout / 2) as u64;
    let levels = (p.max_depth - depth) as u32;
    half.saturating_pow(levels)
}

fn fill(p: &BenchParams, rng: &mut Rng, dir: &str, n: u64, depth: usize, out: &mut Vec<FileSpec>) {
    let fanout = p.max_fanout as u64;
    let join = |name: String| {
        if dir.is_empty() {
            name
        } else {
            format!("{dir}/{name}")
        }
    };
    let leaf = n <= fanout / 2 && (depth + 1 == p.max_depth || rng.below(4) != 0);
    if leaf {
        for i in 0..n {
            let size = file_size(p, rng);
            out.push(FileSpec {
                path: join(format!("f{i:02}")),
                size,
                index: out.len() as u64,
            });
        }
        return;
    }
    let direct = rng.below(n.min(fanout / 4) + 1);
    let rest = n - direct;
    let cap = capacity(p, depth + 1);
    let max_subdirs = fanout - direct;
    let min_subdirs = rest.div_ceil(cap).max(1);
    let subdirs = (2 + rng.below(max_subdirs - 1))
        .clamp(min_subdirs, max_subdirs)
        .min(rest.max(1));
    let weights: Vec<u64> = (0..subdirs).map(|_| 1 + rng.below(64)).collect();
    let total: u64 = weights.iter().sum();
    let mut parts: Vec<u64> = weights.iter().map(|w| rest * w / total).collect();
    let mut left = rest - parts.iter().sum::<u64>();
    let mut i = 0;
    while left > 0 {
        if parts[i] < cap {
            parts[i] += 1;
            left -= 1;
        }
        i = (i + 1) % parts.len();
    }
    // Move any excess over a directory's capacity to the smallest part.
    while let Some(big) = parts.iter().position(|&x| x > cap) {
        let small = (0..parts.len()).min_by_key(|&j| parts[j]).unwrap_or(0);
        let excess = parts[big] - cap;
        parts[big] = cap;
        parts[small] += excess;
    }
    for i in 0..direct {
        let size = file_size(p, rng);
        out.push(FileSpec {
            path: join(format!("f{i:02}")),
            size,
            index: out.len() as u64,
        });
    }
    for (j, part) in parts.into_iter().enumerate() {
        if part > 0 {
            fill(p, rng, &join(format!("d{j:02}")), part, depth + 1, out);
        }
    }
}

/// The bench repo's files, in a deterministic order that keeps each
/// directory's files together.
pub fn layout(p: &BenchParams) -> Vec<FileSpec> {
    assert!(
        p.files <= capacity(p, 0),
        "too many files for the depth and fanout"
    );
    let mut rng = Rng(p.seed);
    let mut out = Vec::with_capacity(p.files as usize);
    fill(p, &mut rng, "", p.files, 0, &mut out);
    out
}

/// A file's contents: seeded pseudo-random bytes.
pub fn content(p: &BenchParams, f: &FileSpec) -> Vec<u8> {
    let mut rng = Rng(p.seed ^ f.index.wrapping_mul(0xa076_1d64_78bd_642f));
    let mut out = Vec::with_capacity(f.size as usize + 8);
    while (out.len() as u64) < f.size {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    out.truncate(f.size as usize);
    out
}

/// How many changesets' blobs are uploaded ahead of the create chain.
const UPLOADS_AHEAD: usize = 6;

/// Builds the tip into the given stores: uploads every file and creates a
/// chain of changesets adding them; returns the last changeset.
pub async fn generate<M: MetaStore + 'static, B: BlobStore + 'static>(
    p: &BenchParams,
    meta: Arc<M>,
    blobs: Arc<B>,
) -> ChangesetId {
    let files = Arc::new(layout(p));
    let ops = Ops::new(Arc::new(Deriver::new(
        meta.clone(),
        blobs.clone(),
        CacheSizes {
            trees: 1 << 30,
            ..CacheSizes::default()
        },
    )));
    let batches = files.len().div_ceil(p.files_per_changeset);
    let mut pending = VecDeque::new();
    let mut next_upload = 0;
    let mut parent: Option<ChangesetId> = None;
    for b in 0..batches {
        while next_upload < batches && next_upload < b + UPLOADS_AHEAD {
            let (files, meta, blobs, p) = (files.clone(), meta.clone(), blobs.clone(), *p);
            let range = next_upload * p.files_per_changeset
                ..((next_upload + 1) * p.files_per_changeset).min(files.len());
            pending.push_back(tokio::spawn(async move {
                let contents: Vec<Bytes> = files[range]
                    .iter()
                    .map(|f| Bytes::from(content(&p, f)))
                    .collect();
                upload_blobs(&*meta, &blobs, REPO, &contents)
                    .await
                    .expect("uploading bench blobs failed")
            }));
            next_upload += 1;
        }
        let ids = pending
            .pop_front()
            .expect("upload task")
            .await
            .expect("upload task panicked");
        let range = b * p.files_per_changeset..((b + 1) * p.files_per_changeset).min(files.len());
        let changes = files[range]
            .iter()
            .zip(ids)
            .map(|(f, blob): (&FileSpec, BlobId)| {
                (
                    RepoPath::new(f.path.as_bytes()).expect("generated paths are valid"),
                    FileChange::File {
                        kind: FileKind::Regular,
                        blob,
                        size: f.size,
                    },
                )
            });
        let mut parts = ChangesetParts::default();
        parts.parents = parent.into_iter().collect();
        parts.author = b"Bench <bench@scion.invalid>".to_vec();
        parts.author_date = Timestamp::new(1_767_225_600 + b as i64, 0).expect("valid timestamp");
        parts.message = format!("bench tip {b}").into_bytes();
        parts.file_changes = changes.collect();
        let cs = Changeset::new(parts).expect("valid changeset");
        parent = Some(
            ops.create_changeset(REPO, cs, &CreateOptions::default())
                .await
                .expect("creating a bench changeset failed"),
        );
    }
    parent.expect("the bench repo has files")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small(files: u64) -> BenchParams {
        BenchParams { files, ..BENCH_TIP }
    }

    #[test]
    fn layouts_respect_the_parameters_and_are_deterministic() {
        for files in [1, 50, 10_000, 200_000] {
            let p = small(files);
            let l = layout(&p);
            assert_eq!(l.len() as u64, files);
            assert_eq!(l, layout(&p));
            assert!(
                l.iter()
                    .all(|f| f.depth() <= p.max_depth && f.size <= p.max_size)
            );
            let mut entries: std::collections::HashMap<&str, std::collections::HashSet<&str>> =
                Default::default();
            for f in &l {
                let comps: Vec<&str> = f.path.split('/').collect();
                for i in 0..comps.len() {
                    let dir = if i == 0 {
                        ""
                    } else {
                        &f.path[..comps[..i].iter().map(|c| c.len() + 1).sum::<usize>() - 1]
                    };
                    entries.entry(dir).or_default().insert(comps[i]);
                }
            }
            assert!(entries.values().all(|e| e.len() <= p.max_fanout));
            let paths: std::collections::HashSet<&str> =
                l.iter().map(|f| f.path.as_str()).collect();
            assert_eq!(paths.len(), l.len());
        }
    }

    #[test]
    fn sizes_are_log_normal_around_the_median() {
        let l = layout(&small(200_000));
        let mut sizes: Vec<u64> = l.iter().map(|f| f.size).collect();
        sizes.sort_unstable();
        let median = sizes[sizes.len() / 2] as f64;
        assert!((1800.0..2300.0).contains(&median), "median {median}");
        // For σ = 2, the quantile at z standard deviations is e^(2z) times
        // the median. The tail matters most: it sets the largest files.
        for (per_mille, z) in [(841, 1.0), (977, 2.0), (999, 3.09)] {
            let q = sizes[sizes.len() * per_mille / 1000] as f64 / median;
            let want = f64::exp(2.0 * z);
            assert!(
                (q / want - 1.0).abs() < 0.2,
                "the {per_mille}‰ quantile is {q:.1} times the median, not {want:.1}"
            );
        }
    }

    #[test]
    fn ln_is_accurate() {
        for x in [
            1e-300, 5e-324, 1e-10, 0.001, 0.25, 0.5, 0.999_999, 1.0, 1.5, 2.0, 10.0, 1e300,
        ] {
            let err = (det_ln(x) - f64::ln(x)).abs();
            assert!(err <= 1e-14 * f64::ln(x).abs().max(1.0), "{x}: {err}");
        }
    }

    #[test]
    fn exp_is_accurate() {
        for x in [-10.0, -1.0, -0.3, 0.0, 0.5, 1.0, 7.25, 12.0] {
            let rel = (det_exp(x) - f64::exp(x)).abs() / f64::exp(x);
            assert!(rel < 1e-14, "{x}: {rel}");
        }
    }

    #[test]
    fn contents_are_seeded_and_sized() {
        let p = small(10);
        let l = layout(&p);
        for f in &l {
            assert_eq!(content(&p, f).len() as u64, f.size);
        }
        assert_eq!(content(&p, &l[3]), content(&p, &l[3]));
        let a = FileSpec {
            path: "a".into(),
            size: 64,
            index: 1,
        };
        let b = FileSpec {
            path: "a".into(),
            size: 64,
            index: 2,
        };
        assert_ne!(content(&p, &a), content(&p, &b));
    }

    /// (`GENERATOR_VERSION`, digest of 20,000 files, digest of 37 files).
    const GOLDEN: (u32, &str, &str) = (
        2,
        "ca17fd5ef740c4183e5e4ac3d4e0dbf5f102280cda8f8fdaec4b6d257122a186",
        "dd8a0bbb492620e2e38fa692b9173af4145097fc0be53122cdb5970efb586c7b",
    );

    /// Digests of 64 and 47 files at depth 3 and fanout 8.
    const GOLDEN_TIGHT: (&str, &str) = (
        "1f432fac7c308e96ea8600c5c00ae0af00bcdc9ee2a65cf4eb56fc6d29b97408",
        "6ec754e4a94dcc8521756a9bc7496f1285d028eedba36c839511868d858f5773",
    );

    /// A digest of the layout and of every file's contents for `p`.
    fn digest(p: &BenchParams) -> String {
        let mut h = blake3::Hasher::new();
        for f in layout(p) {
            h.update(f.path.as_bytes());
            h.update(&f.size.to_le_bytes());
            h.update(&f.index.to_le_bytes());
            h.update(blake3::hash(&content(p, &f)).as_bytes());
        }
        h.finalize().to_hex().to_string()
    }

    /// The generator's output is fixed for a `GENERATOR_VERSION`: a change
    /// to it must bump the version, which renames every cached repo.
    #[test]
    fn the_generator_output_is_locked() {
        assert_eq!(GENERATOR_VERSION, GOLDEN.0);
        assert_eq!(digest(&small(20_000)), GOLDEN.1);
        assert_eq!(digest(&small(37)), GOLDEN.2);
        // Shallow, narrow trees, where directory capacity binds.
        for (files, want) in [(64, GOLDEN_TIGHT.0), (47, GOLDEN_TIGHT.1)] {
            let p = BenchParams {
                files,
                max_depth: 3,
                max_fanout: 8,
                ..BENCH_TIP
            };
            let l = layout(&p);
            assert_eq!(l.len() as u64, files);
            assert!(l.iter().all(|f| f.depth() <= 3));
            let mut entries: std::collections::HashMap<String, std::collections::HashSet<String>> =
                std::collections::HashMap::new();
            for f in &l {
                let parts: Vec<&str> = f.path.split('/').collect();
                for i in 0..parts.len() {
                    entries
                        .entry(parts[..i].join("/"))
                        .or_default()
                        .insert(parts[i].to_owned());
                }
            }
            assert!(entries.values().all(|e| e.len() <= 8), "{files}");
            assert_eq!(digest(&p), want, "{files}");
        }
    }

    #[test]
    fn depth_counts_path_components() {
        let f = |path: &str| FileSpec {
            path: path.into(),
            size: 0,
            index: 0,
        };
        assert_eq!(f("a").depth(), 1);
        assert_eq!(f("a/b/c").depth(), 3);
    }

    #[test]
    fn the_rng_is_splitmix64() {
        // The reference outputs of SplitMix64 from seed 0.
        let mut r = Rng(0);
        assert_eq!(r.next_u64(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(r.next_u64(), 0x6e78_9e6a_a1b9_65f4);
        assert_eq!(r.next_u64(), 0x06c4_5d18_8009_454f);
    }

    #[test]
    fn the_bench_tip_is_the_one_agents_md_specifies() {
        assert_eq!(BENCH_TIP.seed, 0x5C10_2026);
        assert_eq!(BENCH_TIP.files, 3_000_000);
        assert_eq!((BENCH_TIP.max_depth, BENCH_TIP.max_fanout), (12, 64));
        assert_eq!((BENCH_TIP.median_size, BENCH_TIP.sigma), (2048.0, 2.0));
        assert_eq!(BENCH_TIP.max_size, 64 * 1024 * 1024);
        assert_eq!(BENCH_TIP.files_per_changeset, 2000);
    }
}
