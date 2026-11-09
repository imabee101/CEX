//! The Postgres store's connection pool reuses its connections and recovers
//! when the server closes them.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use store::testing::TestDatabase;
use store::{MetaStore, PgMetaStore, PgTls, PoolLimits, RepoId, StoreError};

/// Client backend pids connected to `db`, other than the observer. Server
/// workers (for example autovacuum) are not connections in the tested pool.
async fn backends(observer: &tokio_postgres::Client, db: &TestDatabase) -> Vec<i32> {
    let mut pids: Vec<i32> = observer
        .query(
            "SELECT pid FROM pg_stat_activity WHERE datname = $1 \
             AND backend_type = 'client backend' AND pid <> pg_backend_pid()",
            &[&db.name()],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    pids.sort_unstable();
    pids
}

#[tokio::test]
async fn sequential_calls_reuse_one_connection() {
    let db = TestDatabase::create().await;
    let store = PgMetaStore::new(
        db.config().clone(),
        store::PgTls::LoopbackPlaintext,
        store::PoolLimits::new(4),
    )
    .unwrap();
    store.migrate().await.unwrap();
    let observer = db.connect().await;
    let first = backends(&observer, &db).await;
    assert_eq!(first.len(), 1, "{first:?}");
    for _ in 0..20 {
        store.get_ref(RepoId(1), "main").await.unwrap();
    }
    assert_eq!(backends(&observer, &db).await, first);
}

#[tokio::test]
async fn calls_succeed_after_the_server_closes_pooled_connections() {
    let db = TestDatabase::create().await;
    let store = PgMetaStore::new(
        db.config().clone(),
        store::PgTls::LoopbackPlaintext,
        store::PoolLimits::new(1),
    )
    .unwrap();
    store.migrate().await.unwrap();
    let observer = db.connect().await;
    let old = backends(&observer, &db).await;
    assert_eq!(old.len(), 1);
    // With a timeout, the server blocks until the backend has exited (one
    // call, no polling). The pooled client may not have noticed yet: the
    // store must recover either way.
    let terminated: bool = observer
        .query_one("SELECT pg_terminate_backend($1, 10000)", &[&old[0]])
        .await
        .unwrap()
        .get(0);
    assert!(terminated, "backend not terminated");
    let remaining = backends(&observer, &db).await;
    assert!(
        remaining.is_empty(),
        "client backends remain: {remaining:?}"
    );
    assert_eq!(store.get_ref(RepoId(1), "main").await.unwrap(), None);
    let new = backends(&observer, &db).await;
    assert_eq!(new.len(), 1);
    assert_ne!(new, old);
}

fn limits(max_connections: usize) -> PoolLimits {
    PoolLimits::new(max_connections)
}

#[tokio::test]
async fn plaintext_is_refused_for_hosts_other_than_loopback() {
    let mut config = tokio_postgres::Config::new();
    config.host("db.example.com").user("scion");
    let refused = PgMetaStore::new(config, PgTls::LoopbackPlaintext, limits(1));
    assert!(
        matches!(refused, Err(StoreError::InvalidArgument(_))),
        "{refused:?}"
    );
    let mut config = tokio_postgres::Config::new();
    config.host("127.0.0.1").host("10.0.0.1").user("scion");
    let refused = PgMetaStore::new(config, PgTls::LoopbackPlaintext, limits(1));
    assert!(
        matches!(refused, Err(StoreError::InvalidArgument(_))),
        "{refused:?}"
    );
    let mut config = tokio_postgres::Config::new();
    config.host("::1").user("scion");
    assert!(PgMetaStore::new(config, PgTls::LoopbackPlaintext, limits(1)).is_ok());
}

/// Holds an exclusive lock on the refs table, so every ref read blocks.
async fn lock_refs(db: &TestDatabase) -> tokio_postgres::Client {
    let locker = db.connect().await;
    locker
        .batch_execute("BEGIN; LOCK TABLE refs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    locker
}

#[tokio::test]
async fn a_statement_past_its_timeout_fails_retryably() {
    let db = TestDatabase::create().await;
    let mut l = limits(1);
    l.statement_timeout = Duration::from_millis(200);
    let store = PgMetaStore::new(db.config().clone(), PgTls::LoopbackPlaintext, l).unwrap();
    store.migrate().await.unwrap();
    let _locker = lock_refs(&db).await;
    let started = Instant::now();
    let read = store.get_ref(RepoId(1), "main").await;
    assert!(matches!(read, Err(StoreError::Unavailable(_))), "{read:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
}

/// Polls `call` once, as the caller first runs it.
async fn poll_once<T>(call: &mut Pin<Box<impl Future<Output = T>>>) -> Poll<T> {
    std::future::poll_fn(|cx| Poll::Ready(call.as_mut().poll(cx))).await
}

/// With the only connection held and two callers waiting for it, further
/// callers are refused before they ever wait: on their first poll. The two
/// waiting are refused once the acquire timeout passes, and the holder
/// finishes once the database lets it.
#[tokio::test]
async fn callers_beyond_the_limits_are_refused_not_queued() {
    let db = TestDatabase::create().await;
    let mut l = limits(1);
    l.max_waiters = 2;
    l.acquire_timeout = Duration::from_millis(300);
    let store =
        Arc::new(PgMetaStore::new(db.config().clone(), PgTls::LoopbackPlaintext, l).unwrap());
    store.migrate().await.unwrap();
    let locker = lock_refs(&db).await;
    let call = || {
        let store = store.clone();
        Box::pin(async move { store.get_ref(RepoId(1), "main").await })
    };
    let overloaded = |r: Result<Option<store::RefState>, StoreError>| match r {
        Err(StoreError::Overloaded { retry_after }) => {
            assert_eq!(retry_after, Duration::from_millis(300))
        }
        other => panic!("expected an overload refusal, got {other:?}"),
    };
    // Its first poll takes the only connection and sends the read, which
    // blocks on the lock.
    let mut holder = call();
    assert!(poll_once(&mut holder).await.is_pending());
    let mut waiters = [call(), call()];
    for w in &mut waiters {
        assert!(poll_once(w).await.is_pending(), "a waiter was not queued");
    }
    for _ in 0..3 {
        match poll_once(&mut call()).await {
            Poll::Ready(r) => overloaded(r),
            Poll::Pending => panic!("a caller beyond the waiter limit was queued"),
        }
    }
    let waited = Instant::now();
    for w in waiters {
        overloaded(w.await);
    }
    assert!(waited.elapsed() >= Duration::from_millis(300));
    locker.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(holder.await.unwrap(), None);
}

#[tokio::test]
async fn concurrent_migrations_apply_each_migration_once() {
    let db = TestDatabase::create().await;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store =
            PgMetaStore::new(db.config().clone(), PgTls::LoopbackPlaintext, limits(1)).unwrap();
        tasks.push(tokio::spawn(async move { store.migrate().await }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let observer = db.connect().await;
    let versions: Vec<i64> = observer
        .query("SELECT version FROM schema_version ORDER BY version", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(versions, vec![0, 1]);
}

#[tokio::test]
async fn a_schema_newer_than_the_code_is_refused() {
    let db = TestDatabase::create().await;
    let store = PgMetaStore::new(db.config().clone(), PgTls::LoopbackPlaintext, limits(1)).unwrap();
    store.migrate().await.unwrap();
    db.connect()
        .await
        .batch_execute("INSERT INTO schema_version VALUES (99)")
        .await
        .unwrap();
    let refused = store.migrate().await;
    assert!(
        matches!(refused, Err(StoreError::Internal(_))),
        "{refused:?}"
    );
}

#[tokio::test]
async fn tls_never_falls_back_to_plaintext() {
    // The dev Postgres serves no TLS. A store that requires TLS must fail to
    // connect rather than continue in plaintext.
    let db = TestDatabase::create().await;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(rustls::RootCertStore::empty())
    .with_no_client_auth();
    let store = PgMetaStore::new(
        db.config().clone(),
        PgTls::Rustls(Arc::new(config)),
        limits(1),
    )
    .unwrap();
    assert!(store.get_ref(RepoId(1), "main").await.is_err());
}

#[tokio::test]
async fn a_cas_mismatch_keeps_its_connection() {
    use scion_core::ChangesetId;
    use store::{RefLogReason, RefUpdate};
    let db = TestDatabase::create().await;
    let store = PgMetaStore::new(db.config().clone(), PgTls::LoopbackPlaintext, limits(1)).unwrap();
    store.migrate().await.unwrap();
    let observer = db.connect().await;
    let update = |expected: Option<u8>| RefUpdate {
        name: "main",
        expected: expected.map(|e| ChangesetId::from_bytes([e; 32])),
        new: ChangesetId::from_bytes([1; 32]),
        actor: "test",
        time_micros: 0,
        reason: RefLogReason::Manual,
    };
    store.update_ref(RepoId(1), &update(None)).await.unwrap();
    let before = backends(&observer, &db).await;
    for _ in 0..5 {
        let outcome = store.update_ref(RepoId(1), &update(Some(9))).await.unwrap();
        assert!(matches!(outcome, store::CasOutcome::Mismatch { .. }));
    }
    assert_eq!(
        backends(&observer, &db).await,
        before,
        "a mismatch closed its connection"
    );
}
