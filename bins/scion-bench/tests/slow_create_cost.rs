//! Create cost does not grow with repo size (INV-1): on Postgres and the
//! object store, creating a changeset on the 3,000,000-file bench repo costs
//! within 1.5× of the same change on a 10,000-file repo from the same
//! generator. INV-1 bounds cost by changed files × path depth, so both
//! changes modify the same number of files at the same depth, one per
//! top-level directory; each run uses a fresh server, so every tree read
//! reaches the store.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use derived::{CacheSizes, Deriver};
use ops::blob::upload_blobs;
use ops::{CreateOptions, Ops};
use scion_bench::{BENCH, BenchParams, FileSpec, REPO, cached_repo, layout};
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

#[derive(Debug)]
struct Cost {
    time: Duration,
    counts: Counts,
}

/// `CHANGED` files of depth `DEPTH`, each in a different top-level
/// directory.
fn pick(files: &[FileSpec]) -> Vec<FileSpec> {
    let mut seen = std::collections::HashSet::new();
    let picked: Vec<FileSpec> = files
        .iter()
        .filter(|f| f.depth() == DEPTH && seen.insert(f.path.split('/').next().unwrap().to_owned()))
        .take(CHANGED)
        .cloned()
        .collect();
    assert_eq!(
        picked.len(),
        CHANGED,
        "not enough top-level directories with files at depth {DEPTH}"
    );
    picked
}

async fn measure(p: &BenchParams) -> Vec<Cost> {
    let repo = cached_repo(p).await;
    let files = pick(&layout(p));
    let db = TestDatabase::create_from(&repo.template).await;
    let meta = Arc::new(PgMetaStore::new(db.config().clone(), 16).unwrap());
    let blobs = Arc::new(OverlayBlobStore::new(
        ObjectBlobStore::new(Arc::new(named_object_store(&repo.prefix))),
        ObjectBlobStore::new(Arc::new(object_store())),
    ));
    assert_eq!(
        meta.get_ref(REPO, "main").await.unwrap().unwrap().target,
        repo.tip
    );
    let mut costs = Vec::new();
    for run in 0..RUNS + 1 {
        let contents: Vec<Bytes> = files
            .iter()
            .map(|f| Bytes::from(format!("run {run} of {}\n", f.path)))
            .collect();
        upload_blobs(&*meta, &blobs, REPO, &contents).await.unwrap();
        let mut parts = ChangesetParts::default();
        parts.parents = vec![repo.tip];
        parts.author = b"Bench <bench@scion.invalid>".to_vec();
        parts.author_date = Timestamp::new(1_800_000_000, 0).unwrap();
        parts.message = format!("cost run {run}").into_bytes();
        parts.file_changes = files
            .iter()
            .zip(&contents)
            .map(|(f, c)| {
                (
                    RepoPath::new(f.path.as_bytes()).unwrap(),
                    FileChange::File {
                        kind: FileKind::Regular,
                        blob: BlobId::hash(c),
                        size: c.len() as u64,
                    },
                )
            })
            .collect();
        let cs = Changeset::new(parts).unwrap();
        // A fresh server, with cold caches, counting its store calls.
        let faults = Faults::new();
        let fm = Arc::new(FaultMetaStore::new(meta.clone(), faults.clone()));
        let fb = Arc::new(FaultBlobStore::new(blobs.clone(), faults.clone()));
        let ops = Ops::new(Arc::new(Deriver::new(fm, fb, CacheSizes::default())));
        let started = Instant::now();
        ops.create_changeset(REPO, cs, &CreateOptions::default())
            .await
            .unwrap();
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
        ..BENCH
    };
    let small = measure(&small_params).await;
    let big = measure(&BENCH).await;
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
