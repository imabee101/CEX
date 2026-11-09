//! Per-test isolation for tests that use the dev environment started by
//! `cargo xtask dev-up`: each test gets its own Postgres database, created from
//! the template database, and its own object-store key prefix, so tests run in
//! parallel.
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

fn setting(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

/// Returns a name no other test in any process has used, of the form the
/// sweeper recognizes as owned by this process: use it for a database or key
/// prefix that several handles in one test share.
pub fn unique_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        "t_{}_{}_{}",
        std::process::id(),
        nanos,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
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

/// Returns an object store confined to a key prefix no other test uses, in the
/// dev environment's bucket.
pub fn object_store() -> PrefixStore<AmazonS3> {
    sweep_now_and_then();
    named_object_store(&unique_name())
}

/// Returns an object store confined to the key prefix `prefix` in the dev
/// environment's bucket. Only data shared on purpose, such as a cached
/// generated repo, uses a fixed prefix; tests write to [`object_store()`].
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
        let path = test_tmp_root().join(format!("scion-{}", unique_name()));
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

/// How often, at most, test processes on one machine sweep the dev
/// environment for leftovers.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How many object deletions one sweep runs at once.
const SWEEP_DELETE_CONCURRENCY: usize = 32;

/// The test process that created a database or key prefix, from its name
/// (`t_<pid>_<nanos>_<n>`); `None` for names no test helper creates.
fn owner_pid(name: &str) -> Option<u32> {
    let mut parts = name.strip_prefix("t_")?.split('_');
    let pid = parts.next()?.parse().ok()?;
    let nanos_ok = parts.next()?.parse::<u128>().is_ok();
    let n_ok = parts.next()?.parse::<u64>().is_ok();
    (nanos_ok && n_ok && parts.next().is_none()).then_some(pid)
}

/// Whether the process `pid` may still be running. Where liveness cannot be
/// checked (no `/proc`), every process counts as running, so nothing is
/// swept.
fn may_be_running(pid: u32) -> bool {
    let proc = std::path::Path::new("/proc");
    !proc.is_dir() || proc.join(pid.to_string()).exists()
}

/// Whether `name` was created by a test process that has exited, so it is
/// garbage: its owner can never drop it.
fn is_orphan(name: &str) -> bool {
    owner_pid(name).is_some_and(|pid| !may_be_running(pid))
}

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

/// Drops every test database and deletes every test key prefix whose
/// creating process has exited.
///
/// A test drops its own database when it ends, but a killed test process
/// (a nextest timeout, a cargo-mutants timeout, the OOM killer) never does,
/// and nothing deletes test key prefixes. Left alone, these accumulate
/// without bound. Databases that still have connections are kept.
pub async fn sweep_orphans() -> Result<Swept, String> {
    sweep_orphans_among(|_| true).await
}

/// Like [`sweep_orphans`], but considers only the databases and key prefixes
/// whose names `scope` accepts.
pub async fn sweep_orphans_among(scope: impl Fn(&str) -> bool) -> Result<Swept, String> {
    let mut swept = Swept::default();
    let mut admin = base_config();
    admin.dbname("postgres");
    let client = connect(&admin).await;
    let rows = client
        .query(
            "SELECT d.datname FROM pg_database d \
             WHERE d.datname LIKE 't\\_%' \
             AND NOT EXISTS (SELECT 1 FROM pg_stat_activity a WHERE a.datname = d.datname)",
            &[],
        )
        .await
        .map_err(|e| format!("listing test databases failed: {e}"))?;
    for row in rows {
        let name: String = row.get(0);
        if scope(&name) && is_orphan(&name) {
            client
                .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\""))
                .await
                .map_err(|e| format!("dropping orphaned database {name} failed: {e}"))?;
            swept.databases += 1;
        }
    }

    let bucket = std::sync::Arc::new(bucket());
    let top = object_store::ObjectStore::list_with_delimiter(&*bucket, None)
        .await
        .map_err(|e| format!("listing test key prefixes failed: {e}"))?;
    for prefix in top.common_prefixes {
        let Some(name) = prefix.parts().next() else {
            continue;
        };
        if !scope(name.as_ref()) || !is_orphan(name.as_ref()) {
            continue;
        }
        swept.objects += delete_tree(&bucket, prefix).await?;
        swept.prefixes += 1;
    }
    Ok(swept)
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
/// [`SWEEP_INTERVAL`] across every test process on this machine. Called by
/// the per-test helpers, so leftovers of killed runs are removed by later
/// runs with no separate cleanup step. The sweep runs on its own thread and
/// never delays the test; a process that exits mid-sweep leaves the rest to
/// the next sweeper.
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
        if recent || !claim_sweep_lock(&dir.join("sweep.lock")) {
            return;
        }
        std::thread::spawn(move || {
            let swept = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("building a runtime to sweep the dev environment failed")
                .block_on(sweep_orphans());
            std::fs::write(&stamp, b"").expect("writing the sweep stamp failed");
            std::fs::remove_file(dir.join("sweep.lock")).expect("removing the sweep lock failed");
            swept.expect("sweeping the dev environment failed");
        });
    });
}

/// Takes the machine-wide sweep lock, a file holding the sweeper's pid,
/// taking it over when that process has exited. Returns whether this
/// process now holds it.
fn claim_sweep_lock(lock: &std::path::Path) -> bool {
    use std::io::Write;
    let holder = std::fs::read_to_string(lock).ok();
    if holder
        .as_deref()
        .and_then(|pid| pid.trim().parse().ok())
        .is_some_and(|pid| !may_be_running(pid))
    {
        // Two processes may both see the dead holder; only one of them wins
        // the `create_new` below.
        match std::fs::remove_file(lock) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("removing a dead sweeper's lock failed: {e}"),
        }
    }
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(lock)
    {
        Ok(mut file) => {
            write!(file, "{}", std::process::id()).expect("writing the sweep lock failed");
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(e) => panic!("creating the sweep lock failed: {e}"),
    }
}

fn test_tmp_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/test-tmp")
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_names_made_by_test_helpers_have_an_owner() {
        use super::owner_pid;
        assert_eq!(owner_pid("t_42_1700000000000000000_7"), Some(42));
        for name in [
            "bench_7b4173f81314ef72",
            "scion_template",
            "t_42_1700000000000000000",
            "t_42_1700000000000000000_7_8",
            "t_x_1_2",
            "t__1_2",
            "t_42_1_x",
        ] {
            assert_eq!(owner_pid(name), None, "{name}");
        }
    }

    #[test]
    fn names_of_running_processes_are_not_orphans() {
        let own = format!("t_{}_1_0", std::process::id());
        assert!(!super::is_orphan(&own));
        // No process has this pid: Linux pids stay below 2^22.
        assert!(super::is_orphan(&format!("t_{}_1_0", u32::MAX)));
        assert!(!super::is_orphan("bench_7b4173f81314ef72"));
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
