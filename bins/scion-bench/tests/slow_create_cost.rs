//! Create cost does not grow with repo size (INV-1): on Postgres and the
//! object store, creating a changeset on a 3,000,000-file repo costs within
//! 1.5× of the same change on a 10,000-file repo from the same generator, by
//! wall time and by every store-call measure.
//!
//! Both repos have the bench repo's layout and small file contents. Create
//! never reads an unchanged file's content, so content size cannot change
//! its cost, and small contents keep the 3M-file fixture within a CI
//! runner's disk.
//!
//! INV-1 bounds cost by changed files × path depth, so each run makes the
//! same changes in both repos. The picked files are one per top-level
//! directory, all at one depth, and paired across the repos so that every
//! directory below the root on the big repo's path is no wider than the small
//! repo's. Per picked file, a run
//! modifies it, adds a name beside it and a file in a new directory beside
//! it, and deletes the previous run's two additions. Each run uses a fresh
//! server, so every tree read reaches the store.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use derived::{CacheSizes, Deriver};
use ops::blob::upload_blobs;
use ops::{CreateOptions, Ops};
mod common;

use common::cached_repo;
use scion_bench::{BENCH_TIP, BenchParams, FileSpec, REPO, layout};
use scion_core::{BlobId, Changeset, ChangesetParts, FileChange, FileKind, RepoPath, Timestamp};
use store::fault::{Counts, FaultBlobStore, FaultMetaStore, Faults};
use store::testing::{OverlayBlobStore, TestDatabase, named_object_store, object_store};
use store::{MetaStore, ObjectBlobStore, PgMetaStore};

/// Files changed per changeset.
const CHANGED: usize = 8;
/// Their path depth.
const DEPTH: usize = 4;
/// Timed creates per repo; the median is compared.
const RUNS: usize = 9;
/// The largest allowed ratio of big-repo cost to small-repo cost.
const MAX_RATIO: f64 = 1.5;

/// The 3M-file repo: the bench repo's layout, with file sizes log-normal
/// around 64 bytes and at most 4 KiB.
const BIG: BenchParams = BenchParams {
    median_size: 64.0,
    sigma: 1.0,
    max_size: 4096,
    ..BENCH_TIP
};

#[derive(Debug)]
struct Cost {
    time: Duration,
    counts: Counts,
}

/// The number of entries in each directory of a layout, by directory path
/// (the root is "").
fn dir_sizes(files: &[FileSpec]) -> HashMap<String, usize> {
    let mut entries: HashMap<String, HashSet<String>> = HashMap::new();
    for f in files {
        let comps: Vec<&str> = f.path.split('/').collect();
        for i in 0..comps.len() {
            entries
                .entry(comps[..i].join("/"))
                .or_default()
                .insert(comps[i].to_owned());
        }
    }
    entries.into_iter().map(|(d, e)| (d, e.len())).collect()
}

/// The sizes of every directory on `f`'s path, from the root down: the
/// shape of the tree nodes a change to `f` reads and rewrites.
fn shape(f: &FileSpec, sizes: &HashMap<String, usize>) -> Vec<usize> {
    let comps: Vec<&str> = f.path.split('/').collect();
    (0..comps.len())
        .map(|i| sizes[&comps[..i].join("/")])
        .collect()
}

/// `CHANGED` files of depth `DEPTH` in each layout, each in a different
/// top-level directory, paired so that at every level below the root the
/// big repo's directory is no wider than the small repo's, and as wide as
/// possible. The same change then reads tree nodes no larger in the big repo,
/// so any extra cost there comes from the repo's size alone, not from the
/// generator making a big repo's directories fuller.
fn pick(small: &[FileSpec], big: &[FileSpec]) -> (Vec<FileSpec>, Vec<FileSpec>) {
    let (ss, bs) = (dir_sizes(small), dir_sizes(big));
    let top = |f: &FileSpec| f.path.split('/').next().unwrap().to_owned();
    let mut big_shapes: Vec<(Vec<usize>, &FileSpec)> = big
        .iter()
        .filter(|f| f.depth() == DEPTH)
        .map(|f| (shape(f, &bs), f))
        .collect();
    // Widest first, so the first fit is the closest.
    big_shapes.sort_by_key(|(s, _)| std::cmp::Reverse(s.iter().sum::<usize>()));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    let (mut used_a, mut used_b) = (HashSet::new(), HashSet::new());
    for f in small.iter().filter(|f| f.depth() == DEPTH) {
        if a.len() == CHANGED || used_a.contains(&top(f)) {
            continue;
        }
        let want = shape(f, &ss);
        let fit = big_shapes
            .iter()
            .find(|(s, g)| !used_b.contains(&top(g)) && s.iter().zip(&want).all(|(b, w)| b <= w));
        if let Some((_, g)) = fit {
            used_a.insert(top(f));
            used_b.insert(top(g));
            a.push(f.clone());
            b.push((*g).clone());
        }
    }
    assert_eq!(
        a.len(),
        CHANGED,
        "not enough pairs of depth-{DEPTH} files whose big-repo directories are no wider"
    );
    (a, b)
}

async fn measure(p: &BenchParams, files: &[FileSpec]) -> Vec<Cost> {
    let repo = cached_repo(p).await;
    let db = TestDatabase::create_from(&repo.template).await;
    let meta = Arc::new(
        PgMetaStore::new(
            db.config().clone(),
            store::PgTls::LoopbackPlaintext,
            store::PoolLimits::new(16),
        )
        .unwrap(),
    );
    let blobs = Arc::new(OverlayBlobStore::new(
        ObjectBlobStore::new(Arc::new(named_object_store(&repo.prefix))),
        ObjectBlobStore::new(Arc::new(object_store())),
    ));
    assert_eq!(
        meta.get_ref(REPO, "main").await.unwrap().unwrap().target,
        repo.tip
    );
    let mut costs = Vec::new();
    let mut tip = repo.tip;
    for run in 0..RUNS + 1 {
        // Per picked file: modify it, add a name beside it, and add a file in
        // a new directory beside it, and delete the two files the previous
        // run added: every kind of change, and new names for the case check.
        let dir = |f: &FileSpec| f.path.rsplit_once('/').unwrap().0.to_owned();
        let mut writes: Vec<String> = Vec::new();
        let mut deletes: Vec<String> = Vec::new();
        for f in files {
            writes.push(f.path.clone());
            writes.push(format!("{}/added{run}", dir(f)));
            writes.push(format!("{}/newdir{run}/file", dir(f)));
            if run > 0 {
                deletes.push(format!("{}/added{}", dir(f), run - 1));
                deletes.push(format!("{}/newdir{}/file", dir(f), run - 1));
            }
        }
        let contents: Vec<Bytes> = writes
            .iter()
            .map(|p| Bytes::from(format!("run {run} of {p}\n")))
            .collect();
        upload_blobs(&*meta, &blobs, REPO, &contents).await.unwrap();
        let mut parts = ChangesetParts::default();
        parts.parents = vec![tip];
        parts.author = b"Bench <bench@scion.invalid>".to_vec();
        parts.author_date = Timestamp::new(1_800_000_000, 0).unwrap();
        parts.message = format!("cost run {run}").into_bytes();
        parts.file_changes = writes
            .iter()
            .zip(&contents)
            .map(|(p, c)| {
                (
                    RepoPath::new(p.as_bytes()).unwrap(),
                    FileChange::File {
                        kind: FileKind::Regular,
                        blob: BlobId::hash(c),
                        size: c.len() as u64,
                    },
                )
            })
            .chain(
                deletes
                    .iter()
                    .map(|p| (RepoPath::new(p.as_bytes()).unwrap(), FileChange::Deleted)),
            )
            .collect();
        let cs = Changeset::new(parts).unwrap();
        let created = cs.id();
        // A fresh server, with cold caches, counting its store calls.
        let faults = Faults::new();
        let fm = Arc::new(FaultMetaStore::new(meta.clone(), faults.clone()));
        let fb = Arc::new(FaultBlobStore::new(blobs.clone(), faults.clone()));
        let ops = Ops::new(Arc::new(Deriver::new(fm, fb, CacheSizes::default())));
        let started = Instant::now();
        ops.create_changeset(REPO, cs, &CreateOptions::default())
            .await
            .unwrap();
        tip = created;
        let time = started.elapsed();
        // The first run warms the database's own caches and is not counted.
        if run > 0 {
            costs.push(Cost {
                time,
                counts: faults.counts(),
            });
        }
    }
    costs
}

/// A named cost measure.
type Metric = (&'static str, fn(&Cost) -> f64);

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_create_cost_is_independent_of_repo_size() {
    let small_params = BenchParams {
        files: 10_000,
        ..BIG
    };
    let (small_files, big_files) = pick(&layout(&small_params), &layout(&BIG));
    let small = measure(&small_params, &small_files).await;
    let big = measure(&BIG, &big_files).await;
    let metrics: [Metric; 6] = [
        ("wall time (ms)", |c| c.time.as_secs_f64() * 1000.0),
        ("metadata reads", |c| c.counts.meta_gets as f64),
        ("metadata keys read", |c| c.counts.meta_get_keys as f64),
        ("metadata bytes read", |c| c.counts.meta_get_bytes as f64),
        ("metadata rows written", |c| {
            c.counts.meta_put_entries as f64
        }),
        ("frames written", |c| c.counts.blob_puts as f64),
    ];
    let mut failed = Vec::new();
    for (name, f) in metrics {
        let s = median(small.iter().map(f).collect());
        let b = median(big.iter().map(f).collect());
        let ratio = b / s;
        eprintln!("{name}: 10k-file repo {s:.2}, 3M-file repo {b:.2}, ratio {ratio:.2}");
        if ratio > MAX_RATIO {
            failed.push(format!("{name} ratio {ratio:.2}"));
        }
    }
    assert!(
        failed.is_empty(),
        "create cost grows with repo size: {failed:?}"
    );
}
