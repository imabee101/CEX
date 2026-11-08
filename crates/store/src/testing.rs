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

/// How long one object-store request may take in tests.
pub const TEST_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

const NOT_RUNNING: &str = "the dev environment is not reachable; run `cargo xtask dev-up`";

fn setting(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

/// Returns a name no other test in any process has used.
fn unique_name() -> String {
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

/// Returns an object store confined to a key prefix no other test uses, in the
/// dev environment's bucket.
pub fn object_store() -> PrefixStore<AmazonS3> {
    named_object_store(&unique_name())
}

/// Returns an object store confined to the key prefix `prefix` in the dev
/// environment's bucket. Only data shared on purpose, such as a cached
/// generated repo, uses a fixed prefix; tests write to [`object_store()`].
pub fn named_object_store(prefix: &str) -> PrefixStore<AmazonS3> {
    let store = AmazonS3Builder::new()
        .with_endpoint(setting("SCION_TEST_S3_ENDPOINT", DEFAULT_S3_ENDPOINT))
        .with_region("us-east-1")
        .with_bucket_name(setting("SCION_TEST_S3_BUCKET", DEFAULT_S3_BUCKET))
        .with_access_key_id(setting("SCION_TEST_S3_ACCESS_KEY", DEFAULT_S3_ACCESS_KEY))
        .with_secret_access_key(setting("SCION_TEST_S3_SECRET_KEY", DEFAULT_S3_SECRET_KEY))
        // A request waiting on the object store fails, retryably, after this
        // long rather than the 30 s default; the dev store makes concurrent
        // conditional writers of one key wait in turn.
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_timeout(TEST_REQUEST_TIMEOUT),
        )
        .build()
        .expect("invalid object store settings");
    PrefixStore::new(store, Path::from(prefix))
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
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("scion-{}", unique_name()));
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

#[cfg(test)]
mod tests {
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

#[cfg(test)]
mod overlay_tests {
    use bytes::Bytes;
    use scion_core::BlobId;

    use super::OverlayBlobStore;
    use crate::{BlobStore, MemoryBlobStore, ObjectKey, PutOutcome, RepoId};

    #[tokio::test]
    async fn reads_through_the_base_and_writes_only_the_top() {
        let base = MemoryBlobStore::new();
        let shared = ObjectKey::blob(BlobId::hash(b"shared"));
        base.put_if_absent(RepoId(1), shared, Bytes::from_static(b"b"))
            .await
            .unwrap();
        let overlay = OverlayBlobStore::new(base, MemoryBlobStore::new());
        let own = ObjectKey::blob(BlobId::hash(b"own"));
        assert_eq!(
            overlay
                .put_if_absent(RepoId(1), shared, Bytes::from_static(b"x"))
                .await
                .unwrap(),
            PutOutcome::Existed
        );
        assert_eq!(
            overlay
                .put_if_absent(RepoId(1), own, Bytes::from_static(b"t"))
                .await
                .unwrap(),
            PutOutcome::Created
        );
        assert_eq!(
            overlay.get(RepoId(1), shared).await.unwrap().as_deref(),
            Some(&b"b"[..])
        );
        assert_eq!(
            overlay.get(RepoId(1), own).await.unwrap().as_deref(),
            Some(&b"t"[..])
        );
        assert!(overlay.contains(RepoId(1), own).await.unwrap());
        assert!(!overlay.base.contains(RepoId(1), own).await.unwrap());
        assert!(!overlay.top.contains(RepoId(1), shared).await.unwrap());
    }
}
