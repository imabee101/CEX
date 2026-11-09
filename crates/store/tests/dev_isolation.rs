//! Checks the per-test isolation helpers against the dev environment.

use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use store::testing::{Owner, Sweeper, Swept, TestDatabase, TestPrefix};

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
async fn each_test_object_prefix_is_separate_and_deleted() {
    let a = TestPrefix::create();
    let b = TestPrefix::create();
    assert_ne!(a.name(), b.name());
    let key = "k".into();
    a.store().put(&key, b"in a".to_vec().into()).await.unwrap();
    assert!(matches!(
        b.store().get(&key).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let name = a.name().to_owned();
    drop(a);
    let left = store::testing::named_object_store(&name)
        .list_with_delimiter(None)
        .await
        .unwrap();
    assert!(
        left.objects.is_empty() && left.common_prefixes.is_empty(),
        "a dropped test prefix still holds {left:?}"
    );
}

#[tokio::test]
async fn object_store_supports_put_if_absent() {
    let prefix = TestPrefix::create();
    let store = prefix.store();
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

/// A database and a key prefix whose owner's session has ended, as when its
/// process is killed, are swept; those of a live owner are kept. The test
/// holds the sweep lock from before the owner ends until its sweep is done,
/// so no other process's sweep can take the leftovers first.
#[tokio::test]
async fn sweeping_removes_only_what_ended_owners_left() {
    let gone = Owner::register();
    let dead = gone.name(0);
    let live = store::testing::unique_name();
    let mut admin = store::testing::base_config();
    admin.dbname("postgres");
    let (client, connection) = admin.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(connection);
    for name in [&dead, &live] {
        client
            .batch_execute(&format!("CREATE DATABASE \"{name}\""))
            .await
            .unwrap();
        store::testing::named_object_store(name)
            .put(&"r/b/k".into(), b"x".to_vec().into())
            .await
            .unwrap();
    }

    let sweeper = Sweeper::lock().await;
    gone.end().await;
    // Only these two names are considered, so the test does not depend on
    // whatever else the dev environment holds.
    let swept = sweeper
        .sweep_among(|n| n == dead || n == live)
        .await
        .unwrap();
    drop(sweeper);
    assert_eq!(
        swept,
        Swept {
            databases: 1,
            prefixes: 1,
            objects: 1
        }
    );

    let exists = |name: String| {
        let client = &client;
        async move {
            client
                .query_one(
                    "SELECT count(*) FROM pg_database WHERE datname = $1",
                    &[&name],
                )
                .await
                .unwrap()
                .get::<_, i64>(0)
                == 1
        }
    };
    assert!(!exists(dead.clone()).await);
    assert!(exists(live.clone()).await);
    let has_object = |name: &str| {
        let store = store::testing::named_object_store(name);
        async move { store.head(&"r/b/k".into()).await.is_ok() }
    };
    assert!(!has_object(&dead).await);
    assert!(has_object(&live).await);

    client
        .batch_execute(&format!("DROP DATABASE \"{live}\""))
        .await
        .unwrap();
    store::testing::named_object_store(&live)
        .delete(&"r/b/k".into())
        .await
        .unwrap();
}
