//! The bench repo generator.
//!
//! [`layout`] lays out a deterministic tree of files from [`BenchParams`]:
//! directories of depth at most `max_depth` and at most `max_fanout`
//! entries, file sizes log-normal with the given median and σ, capped, and
//! filled with seeded pseudo-random bytes. [`cached_repo`] builds the repo's
//! tip through the real write path (blob upload and CreateChangeset, in
//! changesets of at most `files_per_changeset` files) in the dev
//! environment, once per parameter set: the result is kept as a Postgres
//! template database and an object-store key prefix, both named by a hash
//! of the parameters, and later calls reuse them.
//!
//! Every computation is integer or IEEE-754 basic arithmetic, so the same
//! parameters give the same files and ids on every machine.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use derived::{CacheSizes, Deriver};
use object_store::{ObjectStoreExt, PutPayload};
use ops::blob::upload_blobs;
use ops::{CreateOptions, Ops};
use scion_core::{
    BlobId, Changeset, ChangesetId, ChangesetParts, FileChange, FileKind, RepoPath, Timestamp,
};
use store::testing::{TEMPLATE_DATABASE, base_config, named_object_store};
use store::{MetaStore, ObjectBlobStore, PgMetaStore, RefLogReason, RefUpdate, RepoId};

/// Bumped whenever the generator's output changes, so old caches are not
/// reused.
pub const GENERATOR_VERSION: u32 = 1;

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

/// The bench repo of CLAUDE.md: 3,000,000 files, depth ≤ 12, fanout ≤ 64,
/// log-normal sizes with median 2 KiB and σ 2.0 capped at 64 MiB, seed
/// `0x5C10_2026`, changesets of at most 2,000 files.
pub const BENCH: BenchParams = BenchParams {
    seed: 0x5C10_2026,
    files: 3_000_000,
    max_depth: 12,
    max_fanout: 64,
    median_size: 2048.0,
    sigma: 2.0,
    max_size: 64 << 20,
    files_per_changeset: 2000,
};

impl BenchParams {
    /// A short name for these parameters and this generator version.
    pub fn key(&self) -> String {
        let desc = format!(
            "v{GENERATOR_VERSION} seed={} files={} depth={} fanout={} median={:?} sigma={:?} max={} per={}",
            self.seed,
            self.files,
            self.max_depth,
            self.max_fanout,
            self.median_size,
            self.sigma,
            self.max_size,
            self.files_per_changeset
        );
        blake3::hash(desc.as_bytes()).to_hex()[..16].to_owned()
    }
}

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

    /// A standard normal sample, by the Irwin–Hall sum of 12 uniforms
    /// (additions only, so it is the same on every machine).
    fn normal(&mut self) -> f64 {
        (0..12).map(|_| self.unit()).sum::<f64>() - 6.0
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
pub async fn generate(
    p: &BenchParams,
    meta: Arc<PgMetaStore>,
    blobs: Arc<ObjectBlobStore>,
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

/// A generated repo in the dev environment.
#[derive(Clone, Debug)]
pub struct CachedRepo {
    /// The template database holding its metadata; create test databases
    /// from it.
    pub template: String,
    /// The object-store key prefix holding its frames; read it as a shared
    /// base and write elsewhere.
    pub prefix: String,
    /// The tip changeset, also the target of the ref `main`.
    pub tip: ChangesetId,
}

const MARKER: &str = "complete";

async fn admin() -> tokio_postgres::Client {
    let mut config = base_config();
    config.dbname("postgres");
    let (client, conn) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("the dev environment is not reachable; run `cargo xtask dev-up`");
    tokio::spawn(conn);
    client
}

async fn marker(prefix: &str) -> Option<ChangesetId> {
    let store = named_object_store(prefix);
    let got = store.get(&MARKER.into()).await.ok()?;
    let bytes = got.bytes().await.ok()?;
    std::str::from_utf8(&bytes).ok()?.parse().ok()
}

/// The repo for `p`, generated on first use and reused afterwards (by every
/// process: generation holds a Postgres advisory lock).
pub async fn cached_repo(p: &BenchParams) -> CachedRepo {
    let key = p.key();
    let template = format!("bench_{key}");
    let prefix = format!("bench-{key}");
    let admin = admin().await;
    let lock = i64::from_str_radix(&key[..15], 16).expect("hex key");
    admin
        .execute("SELECT pg_advisory_lock($1)", &[&lock])
        .await
        .expect("lock");
    let exists = !admin
        .query("SELECT 1 FROM pg_database WHERE datname = $1", &[&template])
        .await
        .expect("query")
        .is_empty();
    let tip = match (exists, marker(&prefix).await) {
        (true, Some(tip)) => tip,
        (exists, _) => {
            if exists {
                admin
                    .batch_execute(&format!("DROP DATABASE \"{template}\" WITH (FORCE)"))
                    .await
                    .expect("drop stale template");
            }
            let build = format!("{template}_build");
            admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS \"{build}\" WITH (FORCE)"))
                .await
                .expect("drop old build database");
            admin
                .batch_execute(&format!(
                    "CREATE DATABASE \"{build}\" TEMPLATE \"{TEMPLATE_DATABASE}\" STRATEGY FILE_COPY"
                ))
                .await
                .expect("create build database");
            let mut config = base_config();
            config.dbname(&build);
            let meta = Arc::new(PgMetaStore::new(config, 32).expect("store"));
            meta.migrate().await.expect("migrate");
            let blobs = Arc::new(ObjectBlobStore::new(Arc::new(named_object_store(&prefix))));
            let started = std::time::Instant::now();
            let tip = generate(p, meta.clone(), blobs).await;
            meta.update_ref(
                REPO,
                &RefUpdate {
                    name: "main",
                    expected: None,
                    new: tip,
                    actor: "scion-bench",
                    time_micros: 0,
                    reason: RefLogReason::Manual,
                },
            )
            .await
            .expect("set main");
            drop(meta);
            eprintln!(
                "generated bench repo {key} ({} files) in {:?}",
                p.files,
                started.elapsed()
            );
            named_object_store(&prefix)
                .put(
                    &MARKER.into(),
                    PutPayload::from(tip.to_string().into_bytes()),
                )
                .await
                .expect("write marker");
            admin
                .batch_execute(&format!(
                    "ALTER DATABASE \"{build}\" RENAME TO \"{template}\""
                ))
                .await
                .expect("rename the built database");
            tip
        }
    };
    admin
        .execute("SELECT pg_advisory_unlock($1)", &[&lock])
        .await
        .expect("unlock");
    CachedRepo {
        template,
        prefix,
        tip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small(files: u64) -> BenchParams {
        BenchParams { files, ..BENCH }
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
        // σ = 2: the 84th percentile is about e² times the median.
        let p84 = sizes[sizes.len() * 841 / 1000] as f64;
        assert!((5.5..9.5).contains(&(p84 / median)), "{}", p84 / median);
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
        1,
        "8fc48253e29cd3b60fa5e8d14a7ef9a1b45effebb00c6a6947b7f2fb4fffc487",
        "58b491cc27a8a3beaeca34ef30b8c227926d53b58b39492df1d29dab51f80c30",
    );

    /// Digests of 64 and 47 files at depth 3 and fanout 8.
    const GOLDEN_TIGHT: (&str, &str) = (
        "6634e30cf8c9036bebd6f20fe1f8915d6c86201b8f3cecd8ecc54530ffd4f218",
        "da941810fb946c6647127844dc1aba3c834409950434a455ede0cec5a839e65e",
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
                ..BENCH
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
    fn the_bench_repo_is_the_one_claude_md_specifies() {
        assert_eq!(BENCH.seed, 0x5C10_2026);
        assert_eq!(BENCH.files, 3_000_000);
        assert_eq!((BENCH.max_depth, BENCH.max_fanout), (12, 64));
        assert_eq!((BENCH.median_size, BENCH.sigma), (2048.0, 2.0));
        assert_eq!(BENCH.max_size, 64 * 1024 * 1024);
        assert_eq!(BENCH.files_per_changeset, 2000);
    }

    #[test]
    fn cache_keys_tell_parameter_sets_apart() {
        assert_eq!(BENCH.key(), BENCH.key());
        let keys: std::collections::HashSet<String> = [
            BENCH,
            small(10_000),
            BenchParams { seed: 1, ..BENCH },
            BenchParams {
                max_depth: 11,
                ..BENCH
            },
            BenchParams {
                max_size: 1,
                ..BENCH
            },
        ]
        .iter()
        .map(BenchParams::key)
        .collect();
        assert_eq!(keys.len(), 5);
    }
}
