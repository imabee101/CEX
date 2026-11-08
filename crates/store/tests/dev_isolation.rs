//! Checks the per-test isolation helpers against the dev environment.

use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use store::testing::{TestDatabase, object_store};

#[tokio::test]
async fn each_test_database_is_separate_and_dropped() {
    let a = TestDatabase::create().await;
    let b = TestDatabase::create().await;
    assert_ne!(a.name(), b.name());

    a.connect()
        .await
        .batch_execute("CREATE TABLE only_in_a (x int)")
        .await
        .unwrap();
    let seen_in_b = b
        .connect()
        .await
        .query_one(
            "SELECT count(*) FROM pg_tables WHERE tablename = 'only_in_a'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(seen_in_b, 0);

    let name = a.name().to_owned();
    drop(a);
    let remaining = b
        .connect()
        .await
        .query_one(
            "SELECT count(*) FROM pg_database WHERE datname = $1",
            &[&name],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn each_test_object_prefix_is_separate() {
    let a = object_store();
    let b = object_store();
    let key = "k".into();
    a.put(&key, b"in a".to_vec().into()).await.unwrap();
    assert!(matches!(
        b.get(&key).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn object_store_supports_put_if_absent() {
    let store = object_store();
    let key = "k".into();
    let create = || PutOptions::from(PutMode::Create);
    store
        .put_opts(&key, b"first".to_vec().into(), create())
        .await
        .unwrap();
    assert!(matches!(
        store
            .put_opts(&key, b"second".to_vec().into(), create())
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    let stored = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(&stored[..], b"first");
}
