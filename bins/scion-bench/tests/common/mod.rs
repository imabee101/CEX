//! The bench tip cached in the dev environment, shared by the bench tests.
//!
//! [`cached_repo`] builds a repo with [`generate`] once per key: the result
//! is kept as a Postgres template database and an object-store key prefix,
//! both named by [`key`], and later calls (from any process) reuse them. The
//! key covers every input that determines the stored bytes: the parameters
//! and every entry of [`format_inputs`], among them the version of every
//! eager derived type, listed by create itself. A change to any of them
//! names a new repo, which is generated afresh; nothing is ever migrated.
//!
//! Besides the repos processes are using, the dev environment keeps at most
//! [`MAX_CACHED`]: each use records its time, and the first use in each
//! process removes the least recently used repos beyond that, with the
//! leftovers of interrupted builds, so repos of old keys never accumulate.
//! A repo is removed only while no process builds or uses it.

// Each test binary includes this module and uses only part of it.
#![allow(dead_code)]

use std::sync::Arc;

use derived::{BlobRecord, FRAME_ZSTD_LEVEL};
use object_store::{ObjectStoreExt, PutPayload};
use ops::CreateOptions;
use scion_bench::{BENCH_TIP, BenchParams, GENERATOR_VERSION, generate};
use scion_core::{
    CHANGESET_FORMAT_VERSION, CHUNK_BYTES, ChangesetId, FRAME_MAGIC, MAX_TREE_ENTRIES,
    TREE_FORMAT_VERSION,
};
use store::testing::{TEMPLATE_DATABASE, base_config, named_object_store};
use store::{CasOutcome, ObjectBlobStore, PgMetaStore, PgTls, PoolLimits};

/// The 3M-file repo: the bench tip's layout, with file sizes log-normal
/// around 64 bytes and at most 4 KiB. Create reads no unchanged file's
/// content, which the create-cost test asserts, so small contents keep the
/// fixture within a CI runner's disk without changing what it measures.
pub(crate) const BIG: BenchParams = BenchParams {
    median_size: 64.0,
    sigma: 1.0,
    max_size: 4096,
    ..BENCH_TIP
};

/// A 250-file repo built in changesets of 100 files.
pub(crate) const SMALL: BenchParams = BenchParams {
    files: 250,
    files_per_changeset: 100,
    ..BENCH_TIP
};

/// A 250-file repo in one changeset, as `scion-bench gen --files 250` builds.
pub(crate) const SMALL_GEN: BenchParams = BenchParams {
    files: 250,
    ..BENCH_TIP
};

/// Every repo the tests cache.
pub(crate) const FIXTURES: [BenchParams; 3] = [BIG, SMALL, SMALL_GEN];

/// Every input besides the parameters that determines the bytes a repo is
/// stored as: the generator, the storage schema, every format of a stored
/// record or object, the constants that shape them, and every eager derived
/// type create writes, as listed by create itself.
pub(crate) fn format_inputs() -> Vec<(String, String)> {
    let mut inputs: Vec<(String, String)> = [
        ("generator", GENERATOR_VERSION.to_string()),
        ("schema", store::SCHEMA_VERSION.to_string()),
        ("changeset", CHANGESET_FORMAT_VERSION.to_string()),
        ("tree", TREE_FORMAT_VERSION.to_string()),
        ("frame magic", format!("{FRAME_MAGIC:?}")),
        ("frame zstd level", FRAME_ZSTD_LEVEL.to_string()),
        ("zstd", zstd::zstd_safe::version_number().to_string()),
        ("chunk bytes", CHUNK_BYTES.to_string()),
        ("max tree entries", MAX_TREE_ENTRIES.to_string()),
        ("max text bytes", ops::blob::MAX_TEXT_BYTES.to_string()),
        ("text sniff bytes", ops::blob::TEXT_SNIFF_BYTES.to_string()),
        ("blob record", BlobRecord::VERSION.to_string()),
        ("unicode", ops::case::UNICODE_VERSION.to_owned()),
        ("create options", format!("{:?}", CreateOptions::default())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect();
    let any = ChangesetId::from_bytes([0; 32]);
    for t in ops::eager_types::<PgMetaStore, ObjectBlobStore>(&CreateOptions::default()) {
        let k = t.key(any);
        inputs.push((
            format!("derived type {}", k.kind),
            format!("version {} param {}", k.version, k.param),
        ));
    }
    inputs
}

/// A short name for the repo `p` builds with the format inputs `inputs`.
pub(crate) fn key_for(p: &BenchParams, inputs: &[(String, String)]) -> String {
    let mut desc = format!("{p:?}");
    for (k, v) in inputs {
        desc.push_str(&format!("\n{k}={v}"));
    }
    blake3::hash(desc.as_bytes()).to_hex()[..16].to_owned()
}

/// A short name for the repo `p` builds with this code's formats.
pub(crate) fn key(p: &BenchParams) -> String {
    key_for(p, &format_inputs())
}

/// The most generated repos the dev environment keeps besides those in use:
/// one per fixture the tests use, so a run of the tests never evicts its own
/// fixtures, and one more for a branch with other formats. The Postgres
/// tmpfs in compose.yaml (12 GB) holds them with room for the tests' copies:
/// a 3M-file repo's database is about 1.6 GB, so even if every cached repo
/// were one, four take 6.4 GB, leaving 5.6 GB for three concurrent test
/// copies of one (4.8 GB) and the small databases.
pub(crate) const MAX_CACHED: usize = FIXTURES.len() + 1;

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
pub(crate) fn key_of(name: &str) -> Option<&str> {
    let key = name
        .strip_prefix("bench_")
        .map(|k| k.strip_suffix("_build").unwrap_or(k))
        .or_else(|| name.strip_prefix("bench-"))?;
    (key.len() == 16 && key.bytes().all(|b| b.is_ascii_hexdigit())).then_some(key)
}

/// Which repos to remove: every key but the `keep` most recently used of
/// those with a complete database, given each key's last use (`None` for a
/// key with no complete database, such as a build's leftovers).
pub(crate) fn to_remove(mut uses: Vec<(String, Option<u64>)>, keep: usize) -> Vec<String> {
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
