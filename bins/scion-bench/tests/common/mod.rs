//! The bench tip cached in the dev environment, shared by the bench tests.
//!
//! [`cached_repo`] builds a repo with [`generate`] once per key: the result
//! is kept as a Postgres template database and an object-store key prefix,
//! both named by [`key`], and later calls (from any process) reuse them. The
//! key covers every input that determines the stored bytes: the generator
//! version, the parameters, and the version of every storage and derived
//! format. A format change therefore regenerates the repo; nothing is ever
//! migrated in place.

// Each test binary includes this module and uses only part of it.
#![allow(dead_code)]

use std::sync::Arc;

use derived::{BlobRecord, CommitGraph, DerivedType, RestrictedRoots, RootTree};
use object_store::{ObjectStoreExt, PutPayload};
use scion_bench::{BenchParams, GENERATOR_VERSION, REPO, generate};
use scion_core::{CHANGESET_FORMAT_VERSION, ChangesetId, TREE_FORMAT_VERSION};
use store::testing::{TEMPLATE_DATABASE, base_config, named_object_store};
use store::{MetaStore, ObjectBlobStore, PgMetaStore, PgTls, PoolLimits, RefLogReason, RefUpdate};

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

/// The repo for `p`, generated on first use and reused afterwards by every
/// process: generation holds a Postgres advisory lock (dev environment
/// only), and a repo counts as built only once its marker is written.
pub(crate) async fn cached_repo(p: &BenchParams) -> CachedRepo {
    let key = key(p);
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
