//! Create cost does not grow with repo size (INV-1): on Postgres and the
//! object store, creating a changeset on a 3,000,000-file repo costs within
//! 1.5× of the same changeset on a repo of about 10,000 files, by wall time
//! and by every store-call measure.
//!
//! INV-1 bounds a commit's cost by changed files × path depth: create reads
//! and rewrites only the tree nodes on each changed path's spine. The other
//! factor in that cost is how large those nodes are, which depends on the
//! directories a change touches, not on the repo's size. The two repos are
//! built so that every directory a change touches is the same in both, and
//! everything else differs:
//!
//! - The big repo is the bench tip's layout, [`BIG`].
//! - The targets are [`TARGETS_PER_DEPTH`] random directories holding files
//!   of each depth the layout has, 1 to `max_depth`: each is the parent of a
//!   file drawn uniformly from the files of that depth.
//! - The small repo holds a subset of the big repo's files: every file
//!   directly in a directory on a target's spine (the target and its
//!   ancestors), and the first file under each other subdirectory of such a
//!   directory. Each spine directory then has the same names, kinds, sizes,
//!   and file ids in both repos, so its node has the same size; its
//!   subdirectories hold a single file in the small repo and up to 64^k
//!   files in the big one.
//! - Both repos then get the same changeset adding a directory of [`WIDE`]
//!   files, more than `MAX_TREE_ENTRIES`, so it is sharded, under a random
//!   target.
//!
//! No directory is chosen by its width or by a cost: targets are drawn by
//! depth alone, and the spine directories are whatever the layout made them.
//!
//! Each run creates, in both repos, the same changeset of [`CHANGED`] random
//! changes in random targets (the sharded directory among them): modifying a
//! file, deleting a file, adding a file beside the existing ones, or adding a
//! file in a new directory. Each create runs on a fresh server, so every tree
//! read reaches the store.
//!
//! File contents are small: create reads no blob or chunk frame, and the
//! test asserts it, so content size cannot change create's cost, and small
//! contents keep the 3M-file fixture within a CI runner's disk.
//!
//! Wall time is noisy where store calls are not. The runs alternate between
//! the repos, each run's ratio is big over small, and the median ratio is
//! compared: load that slows the machine slows both sides of a run alike,
//! and the median ignores the runs a burst of load lands in.

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{BIG, cached_repo};
use derived::{CacheSizes, Deriver};
use object_store::aws::AmazonS3;
use object_store::prefix::PrefixStore;
use ops::blob::upload_blobs;
use ops::{CreateOptions, Ops};
use scion_bench::{FileSpec, REPO, Rng, build, layout};
use scion_core::{
    BlobId, Changeset, ChangesetId, ChangesetParts, FileChange, FileKind, MAX_TREE_ENTRIES,
    RepoPath, Timestamp,
};
use store::fault::{Counts, FaultBlobStore, FaultMetaStore, Faults};
use store::testing::{OverlayBlobStore, TestDatabase, named_object_store, object_store};
use store::{ObjectBlobStore, PgMetaStore};

/// Targets drawn per file depth: enough that the small repo, with the
/// sharded directory, holds at least [`SMALL_FILES`].
const TARGETS_PER_DEPTH: usize = 6;
/// The fewest files the small repo holds.
const SMALL_FILES: usize = 10_000;
/// Files in the sharded directory.
const WIDE: usize = 5_000;
/// Changes per changeset.
const CHANGED: usize = 16;
/// Measured runs per repo, after one that warms the database's caches.
const RUNS: usize = 41;
/// The largest allowed ratio of big-repo cost to small-repo cost.
const MAX_RATIO: f64 = 1.5;
/// The seed of every random choice.
const SEED: u64 = 0x5C10_0001;

fn parent(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(d, _)| d)
}

fn depth(dir: &str) -> usize {
    if dir.is_empty() {
        0
    } else {
        dir.split('/').count()
    }
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

/// [`TARGETS_PER_DEPTH`] parents of files drawn uniformly from the files of
/// each depth.
fn targets(files: &[FileSpec], rng: &mut Rng) -> BTreeSet<String> {
    let mut by_depth: BTreeMap<usize, Vec<&FileSpec>> = BTreeMap::new();
    for f in files {
        by_depth.entry(f.depth()).or_default().push(f);
    }
    assert_eq!(
        by_depth.keys().copied().collect::<Vec<_>>(),
        (1..=BIG.max_depth).collect::<Vec<_>>(),
        "the layout has files of every depth"
    );
    let mut out = BTreeSet::new();
    for of_depth in by_depth.values() {
        for _ in 0..TARGETS_PER_DEPTH {
            let f = of_depth[rng.below(of_depth.len() as u64) as usize];
            out.insert(parent(&f.path).to_owned());
        }
    }
    out
}

/// The small repo's files: those directly in a spine directory of
/// `targets`, and the first file under each other subdirectory of one.
fn skeleton(files: &[FileSpec], targets: &BTreeSet<String>) -> Vec<FileSpec> {
    let mut spine: HashSet<&str> = HashSet::from([""]);
    for t in targets {
        let mut d = t.as_str();
        while !d.is_empty() {
            spine.insert(d);
            d = parent(d);
        }
    }
    let mut represented: HashSet<&str> = HashSet::new();
    files
        .iter()
        .filter(|f| {
            // The shallowest ancestor directory of `f` off the spine, if any.
            let off = f
                .path
                .match_indices('/')
                .map(|(i, _)| &f.path[..i])
                .find(|d| !spine.contains(d));
            off.is_none_or(|d| represented.insert(d))
        })
        .cloned()
        .collect()
}

/// One change of a run.
#[derive(Clone)]
enum Change {
    Write(Bytes),
    Delete,
}

/// The changesets every run makes, the same in both repos, and the files
/// each target holds directly.
struct Workload {
    rng: Rng,
    targets: Vec<String>,
    files: HashMap<String, Vec<String>>,
}

impl Workload {
    /// [`CHANGED`] random changes in random targets.
    fn next(&mut self, run: usize) -> BTreeMap<String, Change> {
        let mut changes = BTreeMap::new();
        let mut k = 0;
        while changes.len() < CHANGED {
            k += 1;
            let dir = self.targets[self.rng.below(self.targets.len() as u64) as usize].clone();
            let here = self.files.entry(dir.clone()).or_default();
            let content = Bytes::from(format!("run {run} change {k}\n"));
            match self.rng.below(4) {
                op @ (0 | 1) => {
                    if here.is_empty() {
                        continue;
                    }
                    let i = self.rng.below(here.len() as u64) as usize;
                    let path = join(&dir, &here[i]);
                    if changes.contains_key(&path) {
                        continue;
                    }
                    if op == 0 {
                        changes.insert(path, Change::Write(content));
                    } else {
                        here.swap_remove(i);
                        changes.insert(path, Change::Delete);
                    }
                }
                2 => {
                    let name = format!("a{run}_{k}");
                    here.push(name.clone());
                    changes.insert(join(&dir, &name), Change::Write(content));
                }
                _ => {
                    if depth(&dir) + 2 > BIG.max_depth {
                        continue;
                    }
                    changes.insert(join(&dir, &format!("n{run}_{k}/f")), Change::Write(content));
                }
            }
        }
        changes
    }
}

/// Both repos' blob stores: a shared base the repo was generated into,
/// under one this test writes to. Both repos use one type of store, so a
/// write costs the same in each.
type Blobs = OverlayBlobStore<ObjectBlobStore, ObjectBlobStore>;

/// A repo under test: its stores and its current tip.
struct Repo {
    _db: TestDatabase,
    meta: Arc<PgMetaStore>,
    blobs: Arc<Blobs>,
    tip: ChangesetId,
}

fn blobs(base: PrefixStore<AmazonS3>) -> Arc<Blobs> {
    Arc::new(OverlayBlobStore::new(
        ObjectBlobStore::new(Arc::new(base)),
        ObjectBlobStore::new(Arc::new(object_store())),
    ))
}

fn pg(db: &TestDatabase) -> Arc<PgMetaStore> {
    Arc::new(
        PgMetaStore::new(
            db.config().clone(),
            store::PgTls::LoopbackPlaintext,
            store::PoolLimits::new(16),
        )
        .unwrap(),
    )
}

#[derive(Debug)]
struct Cost {
    time: Duration,
    counts: Counts,
}

impl Repo {
    /// Uploads the contents `changes` writes, and returns a changeset on the
    /// tip making them.
    async fn changeset(&self, message: &str, changes: &BTreeMap<String, Change>) -> Changeset {
        let writes: Vec<Bytes> = changes
            .values()
            .filter_map(|c| match c {
                Change::Write(b) => Some(b.clone()),
                Change::Delete => None,
            })
            .collect();
        upload_blobs(&*self.meta, &self.blobs, REPO, &writes)
            .await
            .unwrap();
        let mut parts = ChangesetParts::default();
        parts.parents = vec![self.tip];
        parts.author = b"Bench <bench@scion.invalid>".to_vec();
        parts.author_date = Timestamp::new(1_800_000_000, 0).unwrap();
        parts.message = message.as_bytes().to_vec();
        parts.file_changes = changes
            .iter()
            .map(|(p, c)| {
                let change = match c {
                    Change::Write(b) => FileChange::File {
                        kind: FileKind::Regular,
                        blob: BlobId::hash(b),
                        size: b.len() as u64,
                    },
                    Change::Delete => FileChange::Deleted,
                };
                (RepoPath::new(p.as_bytes()).unwrap(), change)
            })
            .collect();
        Changeset::new(parts).unwrap()
    }

    /// Creates `cs` on a fresh server, with cold caches, counting its store
    /// calls, and moves the tip to it.
    async fn create(&mut self, cs: Changeset) -> Cost {
        let faults = Faults::new();
        let fm = Arc::new(FaultMetaStore::new(self.meta.clone(), faults.clone()));
        let fb = Arc::new(FaultBlobStore::new(self.blobs.clone(), faults.clone()));
        let ops = Ops::new(Arc::new(Deriver::new(fm, fb, CacheSizes::default())));
        let started = Instant::now();
        self.tip = ops
            .create_changeset(REPO, cs, &CreateOptions::default())
            .await
            .unwrap();
        Cost {
            time: started.elapsed(),
            counts: faults.counts(),
        }
    }
}

/// A named cost measure.
type Metric = (&'static str, fn(&Counts) -> u64);

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_create_cost_is_independent_of_repo_size() {
    let mut rng = Rng(SEED);
    let big_files = layout(&BIG);
    let targets = targets(&big_files, &mut rng);
    let small_files = skeleton(&big_files, &targets);
    let mut widths: HashMap<&str, BTreeSet<&str>> = HashMap::new();
    for f in &small_files {
        let mut child = f.path.as_str();
        while !child.is_empty() {
            let dir = parent(child);
            if targets.contains(dir) {
                let name = child[dir.len()..].trim_start_matches('/');
                widths.entry(dir).or_default().insert(name);
            }
            child = dir;
        }
    }
    let mut widths: Vec<usize> = widths.values().map(BTreeSet::len).collect();
    widths.sort_unstable();
    eprintln!(
        "{} targets, {} to {} entries wide; the small repo has {} files and \
         {WIDE} more in the sharded directory",
        targets.len(),
        widths[0],
        widths[widths.len() - 1],
        small_files.len()
    );
    assert!(small_files.len() + WIDE >= SMALL_FILES);
    let mut files: HashMap<String, Vec<String>> = HashMap::new();
    for f in &small_files {
        if targets.contains(parent(&f.path)) {
            let name = f.path.rsplit('/').next().unwrap().to_owned();
            files
                .entry(parent(&f.path).to_owned())
                .or_default()
                .push(name);
        }
    }
    drop(big_files);

    let cached = cached_repo(&BIG).await;
    let db = TestDatabase::create_from(&cached.template).await;
    let mut big = Repo {
        meta: pg(&db),
        _db: db,
        blobs: blobs(named_object_store(&cached.prefix)),
        tip: cached.tip,
    };
    let db = TestDatabase::create().await;
    let meta = pg(&db);
    meta.migrate().await.unwrap();
    let small_blobs = blobs(object_store());
    let tip = build(
        &BIG,
        Arc::new(small_files),
        meta.clone(),
        small_blobs.clone(),
    )
    .await;
    let mut small = Repo {
        meta,
        _db: db,
        blobs: small_blobs,
        tip,
    };

    // The sharded directory, under a random target shallow enough for it.
    let shallow: Vec<&String> = targets
        .iter()
        .filter(|t| depth(t) + 2 <= BIG.max_depth)
        .collect();
    let wide_dir = join(shallow[rng.below(shallow.len() as u64) as usize], "wide");
    let names: BTreeSet<String> = (0..WIDE)
        .map(|_| format!("{:012x}", rng.next_u64() >> 16))
        .collect();
    assert!(names.len() > MAX_TREE_ENTRIES);
    let wide: BTreeMap<String, Change> = names
        .iter()
        .map(|n| {
            (
                join(&wide_dir, n),
                Change::Write(Bytes::from(format!("{n}\n"))),
            )
        })
        .collect();
    files.insert(wide_dir.clone(), names.into_iter().collect());
    for repo in [&mut small, &mut big] {
        let cs = repo.changeset("wide directory", &wide).await;
        repo.create(cs).await;
    }

    let mut work = Workload {
        rng,
        targets: files.keys().cloned().collect(),
        files,
    };
    work.targets.sort();
    let mut small_costs = Vec::new();
    let mut big_costs = Vec::new();
    for run in 0..=RUNS {
        let changes = work.next(run);
        let message = format!("cost run {run}");
        let cs_small = small.changeset(&message, &changes).await;
        let cs_big = big.changeset(&message, &changes).await;
        // Alternate which repo goes first, so neither always follows the
        // other's writes.
        let (s, b) = if run % 2 == 0 {
            let s = small.create(cs_small).await;
            (s, big.create(cs_big).await)
        } else {
            let b = big.create(cs_big).await;
            (small.create(cs_small).await, b)
        };
        // The first run warms the database's own caches and is not counted.
        if run > 0 {
            small_costs.push(s);
            big_costs.push(b);
        }
    }

    for c in small_costs.iter().chain(&big_costs) {
        assert_eq!(c.counts.blob_reads, 0, "create read a frame");
    }
    let mut failed = Vec::new();
    let ratios: Vec<f64> = small_costs
        .iter()
        .zip(&big_costs)
        .map(|(s, b)| b.time.as_secs_f64() / s.time.as_secs_f64())
        .collect();
    let ratio = median(ratios);
    let ms = |c: &[Cost]| median(c.iter().map(|c| c.time.as_secs_f64() * 1000.0).collect());
    eprintln!(
        "wall time (ms): 10k-file repo {:.2}, 3M-file repo {:.2}, median ratio {ratio:.2}",
        ms(&small_costs),
        ms(&big_costs)
    );
    if ratio > MAX_RATIO {
        failed.push(format!("wall time ratio {ratio:.2}"));
    }
    let metrics: [Metric; 6] = [
        ("metadata reads", |c| c.meta_gets),
        ("metadata keys read", |c| c.meta_get_keys),
        ("metadata bytes read", |c| c.meta_get_bytes),
        ("metadata rows written", |c| c.meta_put_entries),
        ("metadata bytes written", |c| c.meta_put_bytes),
        ("frames written", |c| c.blob_puts),
    ];
    for (name, f) in metrics {
        let s: u64 = small_costs.iter().map(|c| f(&c.counts)).sum();
        let b: u64 = big_costs.iter().map(|c| f(&c.counts)).sum();
        let ratio = b as f64 / s as f64;
        eprintln!("{name}: 10k-file repo {s}, 3M-file repo {b}, ratio {ratio:.3}");
        if ratio > MAX_RATIO {
            failed.push(format!("{name} ratio {ratio:.3}"));
        }
    }
    assert!(
        failed.is_empty(),
        "create cost grows with repo size: {failed:?}"
    );
}
