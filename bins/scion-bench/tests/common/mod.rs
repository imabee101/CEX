//! The bench tip cached in the dev environment, shared by the bench tests.
//!
//! [`cached_repo`] builds a repo with [`generate`] once per key: the result
//! is kept as a Postgres template database and an object-store key prefix,
//! both named by [`key`], and later calls (from any process) reuse them. The
//! key covers every input that determines the stored bytes: the generator
//! version, the parameters, and the version of every storage and derived
//! format. A format change therefore regenerates the repo; nothing is ever
//! migrated in place.
//!
//! Besides the repos processes are using, the dev environment keeps at most
//! [`MAX_CACHED`]: each use records its time, and the first use in each
//! process removes the least recently used repos beyond that, with the
//! leftovers of interrupted builds, so repos of old keys never accumulate.
//! A repo is removed only while no process builds or uses it.

// Each test binary includes this module and uses only part of it.
#![allow(dead_code)]

use std::sync::Arc;

use derived::{BlobRecord, CommitGraph, DerivedType, RestrictedRoots, RootTree};
use object_store::{ObjectStoreExt, PutPayload};
use scion_bench::{BenchParams, GENERATOR_VERSION, generate};
use scion_core::{CHANGESET_FORMAT_VERSION, ChangesetId, TREE_FORMAT_VERSION};
use store::testing::{TEMPLATE_DATABASE, base_config, named_object_store};
use store::{CasOutcome, ObjectBlobStore, PgMetaStore, PgTls, PoolLimits};

/// A short name for the repo `p` builds with this code's formats.
pub(crate) fn key(p: &BenchParams) -> String {
    let desc = format!(
        "generator={GENERATOR_VERSION} seed={} files={} depth={} fanout={} median={:?} sigma={:?} \
         max={} per={} schema={} changeset={CHANGESET_FORMAT_VERSION} tree={TREE_FORMAT_VERSION} \
         blob_record={} root_tree={} commit_graph={} restricted_roots={} case_index={}",
        p.seed,
        p.files,
        p.max_depth,
        p.max_fanout,
        p.median_size,
        p.sigma,
        p.max_size,
        p.files_per_changeset,
        store::SCHEMA_VERSION,
        BlobRecord::VERSION,
        RootTree::VERSION,
        CommitGraph::VERSION,
        RestrictedRoots::VERSION,
        ops::case::CaseIndex::VERSION,
    );
    blake3::hash(desc.as_bytes()).to_hex()[..16].to_owned()
}

/// The most generated repos the dev environment keeps. Three 3M-file repos
/// and a test database copied from one fit in its Postgres tmpfs.
pub(crate) const MAX_CACHED: usize = 3;

/// The class of the shared advisory locks that mark a repo in use (the
/// two-key form, which never collides with the one-key build locks).
const IN_USE: i32 = 0x5C10;

/// A generated repo in the dev environment.
#[derive(Clone, Debug)]
pub(crate) struct CachedRepo {
    /// The template database holding its metadata; create test databases
    /// from it.
    pub(crate) template: String,
    /// The object-store key prefix holding its frames; read it as a shared
    /// base and write elsewhere.
    pub(crate) prefix: String,
    /// The tip changeset, also the target of the ref `main`.
    pub(crate) tip: ChangesetId,
    /// The connection holding the repo's in-use lock, which keeps it from
    /// being removed while this value lives.
    _in_use: Arc<tokio_postgres::Client>,
}

/// The object that marks a prefix as complete; it holds the tip.
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
    let got = named_object_store(prefix).get(&MARKER.into()).await.ok()?;
    let bytes = got.bytes().await.ok()?;
    std::str::from_utf8(&bytes).ok()?.parse().ok()
}

fn meta_store(dbname: &str) -> Arc<PgMetaStore> {
    let mut config = base_config();
    config.dbname(dbname);
    Arc::new(PgMetaStore::new(config, PgTls::LoopbackPlaintext, PoolLimits::new(32)).unwrap())
}

/// The key of the build lock, held while a repo is checked, built, or
/// removed.
fn build_lock(key: &str) -> i64 {
    i64::from_str_radix(&key[..15], 16).expect("hex key")
}

/// The second key of the in-use lock.
fn in_use_lock(key: &str) -> i32 {
    u32::from_str_radix(&key[..8], 16).expect("hex key") as i32
}

/// The repo for `p`, generated on first use and reused afterwards by every
/// process: generation holds a Postgres advisory lock (dev environment
/// only), and a repo counts as built only once its marker is written.
pub(crate) async fn cached_repo(p: &BenchParams) -> CachedRepo {
    let key = key(p);
    let template = format!("bench_{key}");
    let prefix = format!("bench-{key}");
    let admin = admin().await;
    admin
        .execute("SELECT pg_advisory_lock($1)", &[&build_lock(&key)])
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
                // A build that never wrote its marker is incomplete.
                admin
                    .batch_execute(&format!("DROP DATABASE \"{template}\" WITH (FORCE)"))
                    .await
                    .expect("drop incomplete template");
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
            let meta = meta_store(&build);
            meta.migrate().await.expect("migrate");
            let blobs = Arc::new(ObjectBlobStore::new(Arc::new(named_object_store(&prefix))));
            let started = std::time::Instant::now();
            let tip = generate(p, meta.clone(), blobs).await;
            assert!(matches!(
                scion_bench::create_main(&*meta, tip).await,
                Ok(CasOutcome::Updated { .. })
            ));
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
    // Taken before the build lock is released, so no remover can come
    // between the check above and this use.
    admin
        .execute(
            "SELECT pg_advisory_lock_shared($1, $2)",
            &[&IN_USE, &in_use_lock(&key)],
        )
        .await
        .expect("in-use lock");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs();
    admin
        .batch_execute(&format!("COMMENT ON DATABASE \"{template}\" IS '{now}'"))
        .await
        .expect("record the use");
    admin
        .execute("SELECT pg_advisory_unlock($1)", &[&build_lock(&key)])
        .await
        .expect("unlock");
    remove_least_recently_used_in_background(key);
    CachedRepo {
        template,
        prefix,
        tip,
        _in_use: Arc::new(admin),
    }
}

/// The key a bench database or key prefix is named by: `bench_<key>`,
/// `bench_<key>_build`, or `bench-<key>`.
fn key_of(name: &str) -> Option<&str> {
    let key = name
        .strip_prefix("bench_")
        .map(|k| k.strip_suffix("_build").unwrap_or(k))
        .or_else(|| name.strip_prefix("bench-"))?;
    (key.len() == 16 && key.bytes().all(|b| b.is_ascii_hexdigit())).then_some(key)
}

/// Which repos to remove: every key but the `keep` most recently used of
/// those with a complete database, given each key's last use (`None` for a
/// key with no complete database, such as a build's leftovers).
fn to_remove(mut uses: Vec<(String, Option<u64>)>, keep: usize) -> Vec<String> {
    uses.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let complete = uses.iter().filter(|(_, used)| used.is_some()).count();
    let mut kept = 0;
    uses.into_iter()
        .filter(|(_, used)| {
            if used.is_some() && kept < keep.min(complete) {
                kept += 1;
                false
            } else {
                true
            }
        })
        .map(|(key, _)| key)
        .collect()
}

/// Starts [`remove_least_recently_used`] on its own thread, at most once per
/// process, so removing a repo never delays a test. A process that exits
/// mid-removal leaves a database-less prefix, which a later removal finishes.
fn remove_least_recently_used_in_background(current: String) {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for removal")
                .block_on(async {
                    remove_least_recently_used(&admin().await, &current).await;
                });
        });
    });
}

/// Removes every bench repo but the [`MAX_CACHED`] most recently used,
/// along with the leftovers of interrupted builds and removals, skipping
/// any repo that a process is building or using.
async fn remove_least_recently_used(admin: &tokio_postgres::Client, current: &str) {
    let rows = admin
        .query(
            "SELECT datname, shobj_description(oid, 'pg_database') FROM pg_database \
             WHERE datname LIKE 'bench\\_%'",
            &[],
        )
        .await
        .expect("list bench databases");
    let mut uses: std::collections::BTreeMap<String, Option<u64>> = Default::default();
    for row in rows {
        let name: String = row.get(0);
        let Some(key) = key_of(&name) else { continue };
        let used = (!name.ends_with("_build"))
            .then(|| row.get::<_, Option<String>>(1)?.parse().ok())
            .flatten();
        let entry = uses.entry(key.to_owned()).or_default();
        *entry = (*entry).max(used);
    }
    for prefix in store::testing::top_level_prefixes()
        .await
        .expect("list key prefixes")
    {
        if let Some(key) = key_of(&prefix) {
            uses.entry(key.to_owned()).or_default();
        }
    }
    for key in to_remove(uses.into_iter().collect(), MAX_CACHED) {
        if key == current {
            continue;
        }
        let locks = [
            &build_lock(&key) as &(dyn tokio_postgres::types::ToSql + Sync),
            &IN_USE,
            &in_use_lock(&key),
        ];
        // Both locks, or neither: a build holds the first, a use the second.
        // CASE evaluates its branches in order, only as needed.
        let taken: bool = admin
            .query_one(
                "SELECT CASE WHEN pg_try_advisory_lock($1) THEN \
                   CASE WHEN pg_try_advisory_lock($2, $3) THEN true \
                   ELSE NOT pg_advisory_unlock($1) END \
                 ELSE false END",
                &locks,
            )
            .await
            .expect("try the repo's locks")
            .get(0);
        if !taken {
            continue;
        }
        for db in [format!("bench_{key}"), format!("bench_{key}_build")] {
            admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"))
                .await
                .expect("drop a removed repo's database");
        }
        // The database goes first: a prefix without one is never used, and
        // a later removal finishes it.
        let objects = store::testing::delete_prefix(&format!("bench-{key}"))
            .await
            .expect("delete a removed repo's objects");
        eprintln!("removed bench repo {key} ({objects} objects)");
        admin
            .query_one(
                "SELECT pg_advisory_unlock($1), pg_advisory_unlock($2, $3)",
                &locks,
            )
            .await
            .expect("release the repo's locks");
    }
}

#[test]
fn keys_tell_parameter_sets_apart() {
    use scion_bench::BENCH_TIP;
    assert_eq!(key(&BENCH_TIP), key(&BENCH_TIP));
    let keys: std::collections::HashSet<String> = [
        BENCH_TIP,
        BenchParams {
            files: 10_000,
            ..BENCH_TIP
        },
        BenchParams {
            seed: 1,
            ..BENCH_TIP
        },
        BenchParams {
            max_depth: 11,
            ..BENCH_TIP
        },
        BenchParams {
            max_size: 1,
            ..BENCH_TIP
        },
    ]
    .iter()
    .map(key)
    .collect();
    assert_eq!(keys.len(), 5);
}

#[test]
fn keeps_the_most_recently_used_complete_repos() {
    let uses = |v: &[(&str, Option<u64>)]| {
        v.iter()
            .map(|(k, u)| ((*k).to_owned(), *u))
            .collect::<Vec<_>>()
    };
    let mut removed = to_remove(
        uses(&[
            ("a", Some(1)),
            ("b", Some(5)),
            ("c", None),
            ("d", Some(3)),
            ("e", Some(4)),
        ]),
        2,
    );
    removed.sort();
    assert_eq!(removed, ["a", "c", "d"]);
    assert_eq!(to_remove(uses(&[("a", Some(1))]), 3), Vec::<String>::new());
    assert_eq!(to_remove(uses(&[("a", None)]), 3), ["a"]);
}

#[test]
fn bench_names_give_their_key() {
    let key = "0123456789abcdef";
    for name in [
        format!("bench_{key}"),
        format!("bench_{key}_build"),
        format!("bench-{key}"),
    ] {
        assert_eq!(key_of(&name), Some(key), "{name}");
    }
    for name in [
        "bench_short",
        "t_1_2_3_4",
        "bench-0123456789abcdeg",
        "bench_0123456789abcdef_x",
    ] {
        assert_eq!(key_of(name), None, "{name}");
    }
}
