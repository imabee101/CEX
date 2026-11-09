//! The Postgres store's connection pool reuses its connections and recovers
//! when the server closes them.

use std::time::Duration;

use store::testing::TestDatabase;
use store::{MetaStore, PgMetaStore, RepoId};

/// Backend pids connected to `db`, other than the observing connection.
async fn backends(observer: &tokio_postgres::Client, db: &TestDatabase) -> Vec<i32> {
    let mut pids: Vec<i32> = observer
        .query(
            "SELECT pid FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
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
    let store = PgMetaStore::new(db.config().clone(), 4).unwrap();
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
    let store = PgMetaStore::new(db.config().clone(), 1).unwrap();
    store.migrate().await.unwrap();
    let observer = db.connect().await;
    let old = backends(&observer, &db).await;
    assert_eq!(old.len(), 1);
    // With a timeout, the server blocks until the backend has exited (one
    // call, no polling); the pooled client notices the closed socket shortly
    // after.
    let terminated: bool = observer
        .query_one("SELECT pg_terminate_backend($1, 10000)", &[&old[0]])
        .await
        .unwrap()
        .get(0);
    assert!(terminated, "backend not terminated");
    assert!(backends(&observer, &db).await.is_empty());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(store.get_ref(RepoId(1), "main").await.unwrap(), None);
    let new = backends(&observer, &db).await;
    assert_eq!(new.len(), 1);
    assert_ne!(new, old);
}
