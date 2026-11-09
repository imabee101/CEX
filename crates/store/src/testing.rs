//! Per-test isolation for tests that use the dev environment started by
//! `cargo xtask dev-up`: each test gets its own Postgres database
//! ([`TestDatabase`]), created from the template database, and its own
//! object-store key prefix ([`TestPrefix`]), so tests run in parallel. Each is
//! removed when the test drops it; what a killed process leaves behind is
//! removed by a later process's sweep (see [`Owner`] and [`Sweeper`]).
//!
//! Connection settings default to the values in the workspace `compose.yaml`
//! and can be overridden with `SCION_TEST_PG_URL`, `SCION_TEST_S3_ENDPOINT`,
//! `SCION_TEST_S3_BUCKET`, `SCION_TEST_S3_ACCESS_KEY`, and
//! `SCION_TEST_S3_SECRET_KEY`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::ClientOptions;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::prefix::PrefixStore;

/// The database every test database is created from.
pub const TEMPLATE_DATABASE: &str = "scion_template";

const DEFAULT_PG_URL: &str = "postgres://scion:scion@127.0.0.1:55432";
const DEFAULT_S3_ENDPOINT: &str = "http://127.0.0.1:58333";
const DEFAULT_S3_BUCKET: &str = "scion-test";
const DEFAULT_S3_ACCESS_KEY: &str = "scion";
const DEFAULT_S3_SECRET_KEY: &str = "scion-dev-secret";

const NOT_RUNNING: &str = "the dev environment is not reachable; run `cargo xtask dev-up`";

mod pg_proxy;
pub use pg_proxy::{Direction, PgProxy, Stopped};

fn setting(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

/// The prefix of the application name of an owner's Postgres session; the
/// owner's token follows it.
const OWNER_SESSION: &str = "scion-test-owner:";

/// The owner of the databases and key prefixes a test process names: a
/// Postgres session in the dev environment, held open on its own thread for
/// as long as the owner lives, whose application name carries the owner's
/// token. Every name the owner makes carries the token too, so a sweep can
/// tell the leftovers of a process that has gone (its session ended when
/// the process did, however it ended) from the names of a running one. This
/// works the same on every platform, and across machines, containers, and
/// worktrees that share one dev environment.
#[derive(Debug)]
pub struct Owner {
    token: String,
    backend_pid: i32,
    session: tokio_postgres::Client,
}

impl Owner {
    /// Opens a new owner's session, with a token no other owner has.
    pub fn register() -> Self {
        use std::hash::BuildHasher;
        // Each `RandomState` has its own random keys; the pid and the time
        // only add to them.
        let token = format!(
            "{:016x}",
            std::hash::RandomState::new().hash_one((std::process::id(), SystemTime::now()))
        );
        let (opened, receive) = std::sync::mpsc::channel();
        let mut config = base_config();
        config
            .dbname("postgres")
            .application_name(format!("{OWNER_SESSION}{token}"));
        std::thread::Builder::new()
            .name("scion-test-owner".into())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("building the owner's runtime failed")
                    .block_on(async move {
                        let (session, connection) = config
                            .connect(tokio_postgres::NoTls)
                            .await
                            .expect(NOT_RUNNING);
                        let driver = tokio::spawn(connection);
                        let pid: i32 = session
                            .query_one("SELECT pg_backend_pid()", &[])
                            .await
                            .expect("reading the owner's backend pid failed")
                            .get(0);
                        // The receiver waits for exactly this message.
                        let _ = opened.send((session, pid));
                        // Drives the session until it ends.
                        let _ = driver.await;
                    });
            })
            .expect("starting the owner's thread failed");
        let (session, backend_pid) = receive.recv().expect(NOT_RUNNING);
        Self {
            token,
            backend_pid,
            session,
        }
    }

    /// The `n`th name this owner makes: `t_<token>_<n>`.
    pub fn name(&self, n: u64) -> String {
        assert!(
            !self.session.is_closed(),
            "the owner's session in the dev environment ended (did its Postgres restart?)"
        );
        format!("t_{}_{n}", self.token)
    }

    /// Ends the owner's session, as the server does when the process holding
    /// it dies, and returns once the session is gone.
    pub async fn end(self) {
        let mut admin = base_config();
        admin.dbname("postgres");
        let ended: bool = connect(&admin)
            .await
            .query_one(
                "SELECT pg_terminate_backend($1, 10000)",
                &[&self.backend_pid],
            )
            .await
            .expect("ending the owner's session failed")
            .get(0);
        assert!(ended, "the owner's session did not end");
    }
}

/// Returns a name no other test in any process has used, owned by this
/// process: use it for a database or key prefix that several handles in one
/// test share.
pub fn unique_name() -> String {
    static OWNER: std::sync::OnceLock<Owner> = std::sync::OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    OWNER
        .get_or_init(Owner::register)
        .name(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// The owner's token in `name`, if a test helper made it.
fn owner_token(name: &str) -> Option<&str> {
    let (token, n) = name.strip_prefix("t_")?.split_once('_')?;
    let token_ok = token.len() == 16
        && token
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    (token_ok && n.parse::<u64>().is_ok()).then_some(token)
}

/// Connection settings for the dev environment's Postgres server, with no
/// database selected.
pub fn base_config() -> tokio_postgres::Config {
    setting("SCION_TEST_PG_URL", DEFAULT_PG_URL)
        .parse()
        .expect("SCION_TEST_PG_URL is not a valid Postgres URL")
}

async fn connect(config: &tokio_postgres::Config) -> tokio_postgres::Client {
    let (client, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect(NOT_RUNNING);
    tokio::spawn(connection);
    client
}

/// A Postgres database owned by one test, created from [`TEMPLATE_DATABASE`]
/// and dropped when this value is dropped.
#[derive(Debug)]
pub struct TestDatabase {
    name: String,
    config: tokio_postgres::Config,
}

impl TestDatabase {
    /// Creates a fresh database from the template.
    pub async fn create() -> Self {
        Self::create_from(TEMPLATE_DATABASE).await
    }

    /// Creates a fresh database as a copy of the database `template`, which
    /// must have no open connections.
    pub async fn create_from(template: &str) -> Self {
        sweep_now_and_then();
        let name = unique_name();
        let mut admin = base_config();
        admin.dbname("postgres");
        connect(&admin)
            .await
            .batch_execute(&format!(
                "CREATE DATABASE \"{name}\" TEMPLATE \"{template}\" STRATEGY FILE_COPY"
            ))
            .await
            .expect("creating the test database failed");
        let mut config = base_config();
        config.dbname(&name);
        Self { name, config }
    }

    /// The database's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Connection settings for this database.
    pub fn config(&self) -> &tokio_postgres::Config {
        &self.config
    }

    /// A Postgres connection URL for this database.
    pub fn url(&self) -> String {
        format!(
            "{}/{}",
            setting("SCION_TEST_PG_URL", DEFAULT_PG_URL).trim_end_matches('/'),
            self.name
        )
    }

    /// Opens a connection, driven by a task on the current Tokio runtime.
    pub async fn connect(&self) -> tokio_postgres::Client {
        connect(&self.config).await
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let name = std::mem::take(&mut self.name);
        // Drop may run inside or outside a runtime, so drop the database from
        // a dedicated thread with its own runtime.
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("building a runtime to drop the test database failed");
            runtime.block_on(async {
                let mut admin = base_config();
                admin.dbname("postgres");
                let client = connect(&admin).await;
                client
                    .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
                    .await
            })
        })
        .join();
        if !std::thread::panicking() {
            dropped
                .expect("dropping the test database panicked")
                .expect("dropping the test database failed");
        }
    }
}

/// The dev environment's S3 settings, as the `AWS_*` environment variables a
/// child process reads, plus the endpoint and bucket.
pub fn s3_env() -> (String, String, Vec<(&'static str, String)>) {
    (
        setting("SCION_TEST_S3_ENDPOINT", DEFAULT_S3_ENDPOINT),
        setting("SCION_TEST_S3_BUCKET", DEFAULT_S3_BUCKET),
        vec![
            (
                "AWS_ACCESS_KEY_ID",
                setting("SCION_TEST_S3_ACCESS_KEY", DEFAULT_S3_ACCESS_KEY),
            ),
            (
                "AWS_SECRET_ACCESS_KEY",
                setting("SCION_TEST_S3_SECRET_KEY", DEFAULT_S3_SECRET_KEY),
            ),
            ("AWS_REGION", "us-east-1".to_owned()),
            ("AWS_ALLOW_HTTP", "true".to_owned()),
        ],
    )
}

/// An object-store key prefix owned by one test, in the dev environment's
/// bucket, no other test uses; every object under it is deleted when this
/// value is dropped.
#[derive(Debug)]
pub struct TestPrefix {
    name: String,
}

impl TestPrefix {
    /// Claims a fresh key prefix.
    pub fn create() -> Self {
        sweep_now_and_then();
        Self {
            name: unique_name(),
        }
    }

    /// The prefix's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// An object store confined to the prefix, with its own client.
    pub fn store(&self) -> PrefixStore<AmazonS3> {
        named_object_store(&self.name)
    }
}

impl Drop for TestPrefix {
    fn drop(&mut self) {
        let name = std::mem::take(&mut self.name);
        // Drop may run inside or outside a runtime, so delete from a
        // dedicated thread with its own runtime.
        let deleted = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("building a runtime to delete the test prefix failed")
                .block_on(delete_prefix(&name))
        })
        .join();
        if !std::thread::panicking() {
            deleted
                .expect("deleting the test prefix panicked")
                .expect("deleting the test prefix failed");
        }
    }
}

/// Returns an object store confined to the key prefix `prefix` in the dev
/// environment's bucket. Only data shared on purpose, such as a cached
/// generated repo, uses a fixed prefix; tests write under a [`TestPrefix`].
pub fn named_object_store(prefix: &str) -> PrefixStore<AmazonS3> {
    PrefixStore::new(bucket(), Path::from(prefix))
}

/// The dev environment's bucket, with no key prefix.
fn bucket() -> AmazonS3 {
    AmazonS3Builder::new()
        .with_endpoint(setting("SCION_TEST_S3_ENDPOINT", DEFAULT_S3_ENDPOINT))
        .with_region("us-east-1")
        .with_bucket_name(setting("SCION_TEST_S3_BUCKET", DEFAULT_S3_BUCKET))
        .with_access_key_id(setting("SCION_TEST_S3_ACCESS_KEY", DEFAULT_S3_ACCESS_KEY))
        .with_secret_access_key(setting("SCION_TEST_S3_SECRET_KEY", DEFAULT_S3_SECRET_KEY))
        .with_client_options(ClientOptions::new().with_allow_http(true))
        .build()
        .expect("invalid object store settings")
}

/// A directory under the workspace's `target/test-tmp` (on disk, not in the
/// system temp directory, which may be RAM), owned by one test and removed
/// when this value is dropped.
#[derive(Debug)]
pub struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    /// Creates an empty directory no other test uses.
    pub fn create() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = test_tmp_root().join(format!(
            "scion-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("creating the test directory failed");
        Self { path }
    }

    /// The directory's path.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let removed = std::fs::remove_dir_all(&self.path);
        if !std::thread::panicking() {
            removed.expect("removing the test directory failed");
        }
    }
}

/// A [`BlobStore`](crate::BlobStore) that reads through a shared, read-only
/// `base` (such as a cached generated repo) and writes only to `top`, a
/// store owned by one test. A key in either store exists; a put of a key the
/// base holds is a no-op.
#[derive(Debug)]
pub struct OverlayBlobStore<B, T> {
    base: B,
    top: T,
}

impl<B, T> OverlayBlobStore<B, T> {
    /// Layers `top` over `base`.
    pub fn new(base: B, top: T) -> Self {
        Self { base, top }
    }
}

impl<B: crate::BlobStore, T: crate::BlobStore> crate::BlobStore for OverlayBlobStore<B, T> {
    async fn put_if_absent(
        &self,
        repo: crate::RepoId,
        key: crate::ObjectKey,
        bytes: bytes::Bytes,
    ) -> Result<crate::PutOutcome, crate::StoreError> {
        if self.base.contains(repo, key).await? {
            return Ok(crate::PutOutcome::Existed);
        }
        self.top.put_if_absent(repo, key, bytes).await
    }

    async fn get(
        &self,
        repo: crate::RepoId,
        key: crate::ObjectKey,
    ) -> Result<Option<bytes::Bytes>, crate::StoreError> {
        match self.top.get(repo, key).await? {
            Some(b) => Ok(Some(b)),
            None => self.base.get(repo, key).await,
        }
    }

    async fn contains(
        &self,
        repo: crate::RepoId,
        key: crate::ObjectKey,
    ) -> Result<bool, crate::StoreError> {
        Ok(self.top.contains(repo, key).await? || self.base.contains(repo, key).await?)
    }
}

// ---------------------------------------------------------------- sweeping

/// How often, at most, the test processes of one target directory start a
/// sweep of the dev environment for leftovers.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How many object deletions one sweep runs at once.
const SWEEP_DELETE_CONCURRENCY: usize = 32;

/// The advisory-lock key that serializes sweeps across every process using
/// the dev environment.
const SWEEP_LOCK: i64 = 0x5C10_5EE9;

/// What one sweep removed.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Swept {
    /// Test databases dropped.
    pub databases: usize,
    /// Test key prefixes deleted.
    pub prefixes: usize,
    /// Objects deleted under those prefixes.
    pub objects: usize,
}

/// The holder of the dev environment's sweep lock, a session-level advisory
/// lock in its Postgres: at most one sweep runs at a time, whichever process
/// and machine it runs on. The lock is released when this value is dropped.
///
/// A test drops its own database and deletes its own key prefix when it
/// ends, but a killed test process (a nextest timeout, a cargo-mutants
/// timeout, the OOM killer) never does. Left alone, these accumulate without
/// bound, so sweeps remove them.
#[derive(Debug)]
pub struct Sweeper {
    admin: tokio_postgres::Client,
}

impl Sweeper {
    async fn admin() -> tokio_postgres::Client {
        let mut admin = base_config();
        admin.dbname("postgres");
        connect(&admin).await
    }

    /// Waits for the sweep lock.
    pub async fn lock() -> Self {
        let admin = Self::admin().await;
        admin
            .execute("SELECT pg_advisory_lock($1)", &[&SWEEP_LOCK])
            .await
            .expect("taking the sweep lock failed");
        Self { admin }
    }

    /// Takes the sweep lock, or returns `None` when another sweep holds it.
    pub async fn try_lock() -> Option<Self> {
        let admin = Self::admin().await;
        let taken: bool = admin
            .query_one("SELECT pg_try_advisory_lock($1)", &[&SWEEP_LOCK])
            .await
            .expect("taking the sweep lock failed")
            .get(0);
        taken.then_some(Self { admin })
    }

    /// Drops every test database and deletes every test key prefix whose
    /// name `scope` accepts and whose owner's session has ended. Databases
    /// that still have connections are kept.
    pub async fn sweep_among(&self, scope: impl Fn(&str) -> bool) -> Result<Swept, String> {
        let databases: Vec<String> = self
            .admin
            .query(
                "SELECT d.datname FROM pg_database d \
                 WHERE d.datname LIKE 't\\_%' \
                 AND NOT EXISTS (SELECT 1 FROM pg_stat_activity a WHERE a.datname = d.datname)",
                &[],
            )
            .await
            .map_err(|e| format!("listing test databases failed: {e}"))?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let prefixes = top_level_prefixes().await?;
        // Read after the names: an owner registers before it makes a name,
        // so the owner of every name listed above is live here unless it has
        // ended.
        let live: std::collections::HashSet<String> = self
            .admin
            .query(
                "SELECT substr(application_name, length($1) + 1) FROM pg_stat_activity \
                 WHERE starts_with(application_name, $1)",
                &[&OWNER_SESSION],
            )
            .await
            .map_err(|e| format!("listing live owners failed: {e}"))?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let orphan = |name: &str| {
            scope(name) && owner_token(name).is_some_and(|token| !live.contains(token))
        };
        let mut swept = Swept::default();
        for name in databases.iter().filter(|n| orphan(n)) {
            self.admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\""))
                .await
                .map_err(|e| format!("dropping orphaned database {name} failed: {e}"))?;
            swept.databases += 1;
        }
        for name in prefixes.iter().filter(|n| orphan(n)) {
            swept.objects += delete_prefix(name).await?;
            swept.prefixes += 1;
        }
        Ok(swept)
    }
}

/// The names of the top-level key prefixes in the dev environment's bucket.
pub async fn top_level_prefixes() -> Result<Vec<String>, String> {
    let listed = object_store::ObjectStore::list_with_delimiter(&bucket(), None)
        .await
        .map_err(|e| format!("listing key prefixes failed: {e}"))?;
    Ok(listed
        .common_prefixes
        .iter()
        .filter_map(|p| p.parts().next().map(|n| n.as_ref().to_owned()))
        .collect())
}

/// Deletes every object under the top-level key prefix `name` in the dev
/// environment's bucket, and returns how many it deleted.
pub async fn delete_prefix(name: &str) -> Result<usize, String> {
    delete_tree(&std::sync::Arc::new(bucket()), Path::from(name)).await
}

/// Deletes every object under `prefix`, a bounded number at a time, and
/// returns how many it deleted.
async fn delete_tree(bucket: &std::sync::Arc<AmazonS3>, prefix: Path) -> Result<usize, String> {
    use object_store::ObjectStore;
    let mut deleted = 0;
    let mut pending = vec![prefix];
    let mut tasks = tokio::task::JoinSet::new();
    while let Some(dir) = pending.pop() {
        let level = bucket
            .list_with_delimiter(Some(&dir))
            .await
            .map_err(|e| format!("listing {dir} failed: {e}"))?;
        pending.extend(level.common_prefixes);
        for object in level.objects {
            if tasks.len() == SWEEP_DELETE_CONCURRENCY {
                join_delete(tasks.join_next().await)?;
            }
            let bucket = std::sync::Arc::clone(bucket);
            tasks.spawn(async move {
                object_store::ObjectStoreExt::delete(&*bucket, &object.location)
                    .await
                    .map_err(|e| format!("deleting {} failed: {e}", object.location))
            });
            deleted += 1;
        }
    }
    while let Some(done) = tasks.join_next().await {
        join_delete(Some(done))?;
    }
    Ok(deleted)
}

fn join_delete(
    done: Option<Result<Result<(), String>, tokio::task::JoinError>>,
) -> Result<(), String> {
    match done {
        Some(Ok(result)) => result,
        Some(Err(e)) => Err(format!("an object deletion task failed: {e}")),
        None => Ok(()),
    }
}

/// Starts a background sweep of the dev environment at most once per
/// [`SWEEP_INTERVAL`] across the test processes of one target directory,
/// unless another sweep is running. Called by the per-test helpers, so
/// leftovers of killed runs are removed by later runs with no separate
/// cleanup step. The sweep runs on its own thread and never delays the test;
/// a process that exits mid-sweep leaves the rest to the next sweeper.
fn sweep_now_and_then() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let dir = test_tmp_root();
        std::fs::create_dir_all(&dir).expect("creating the test directory failed");
        let stamp = dir.join("sweep.stamp");
        let recent = std::fs::metadata(&stamp)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age < SWEEP_INTERVAL);
        if recent {
            return;
        }
        std::fs::write(&stamp, b"").expect("writing the sweep stamp failed");
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("building a runtime to sweep the dev environment failed")
                .block_on(async {
                    if let Some(sweeper) = Sweeper::try_lock().await {
                        sweeper
                            .sweep_among(|_| true)
                            .await
                            .expect("sweeping the dev environment failed");
                    }
                });
        });
    });
}

fn test_tmp_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/test-tmp")
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_names_made_by_owners_have_an_owner_token() {
        use super::owner_token;
        assert_eq!(
            owner_token("t_0123456789abcdef_7"),
            Some("0123456789abcdef")
        );
        for name in [
            "t_0123456789abcdef",
            "t_0123456789abcdef_7_8",
            "t_0123456789abcde_7",
            "t_0123456789ABCDEF_7",
            "t_0123456789abcdeg_7",
            "t_0123456789abcdef_x",
            "u_0123456789abcdef_7",
            "bench_7b4173f81314ef72",
            "scion_template",
        ] {
            assert_eq!(owner_token(name), None, "{name}");
        }
    }

    use super::TempDir;

    #[tokio::test]
    async fn an_overlay_reads_through_its_base_and_writes_only_its_top() {
        use crate::{BlobStore, MemoryBlobStore, ObjectKey, PutOutcome, RepoId};
        use bytes::Bytes;
        use scion_core::BlobId;
        let repo = RepoId(1);
        let base = MemoryBlobStore::new();
        let top = MemoryBlobStore::new();
        let k = |s: &[u8]| ObjectKey::blob(BlobId::hash(s));
        base.put_if_absent(repo, k(b"b"), Bytes::from_static(b"base"))
            .await
            .unwrap();
        let o = super::OverlayBlobStore::new(base, top);
        let (base, top) = (&o.base, &o.top);
        // A key the base holds exists, reads from the base, and is not
        // written again.
        assert!(o.contains(repo, k(b"b")).await.unwrap());
        assert_eq!(
            o.put_if_absent(repo, k(b"b"), Bytes::from_static(b"new"))
                .await
                .unwrap(),
            PutOutcome::Existed
        );
        assert!(!top.contains(repo, k(b"b")).await.unwrap());
        assert_eq!(
            o.get(repo, k(b"b")).await.unwrap(),
            Some(Bytes::from_static(b"base"))
        );
        // A new key goes to the top only.
        assert!(!o.contains(repo, k(b"t")).await.unwrap());
        assert_eq!(
            o.put_if_absent(repo, k(b"t"), Bytes::from_static(b"top"))
                .await
                .unwrap(),
            PutOutcome::Created
        );
        assert!(o.contains(repo, k(b"t")).await.unwrap());
        assert!(!base.contains(repo, k(b"t")).await.unwrap());
        assert_eq!(
            o.get(repo, k(b"t")).await.unwrap(),
            Some(Bytes::from_static(b"top"))
        );
        assert_eq!(o.get(repo, k(b"none")).await.unwrap(), None);
    }

    #[test]
    fn temp_dirs_are_removed_on_drop() {
        let dir = TempDir::create();
        let path = dir.path().to_owned();
        std::fs::write(path.join("f"), b"x").unwrap();
        drop(dir);
        assert!(!path.exists());
    }

    #[test]
    fn a_temp_dir_that_cannot_be_removed_fails_loudly() {
        let dir = TempDir::create();
        std::fs::remove_dir_all(dir.path()).unwrap();
        assert!(std::panic::catch_unwind(move || drop(dir)).is_err());
    }
}
