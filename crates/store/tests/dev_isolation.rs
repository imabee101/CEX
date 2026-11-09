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

#[tokio::test]
async fn sweeping_removes_only_what_exited_test_processes_left() {
    use object_store::ObjectStoreExt;
    // Names a test helper would make for a process that has exited (Linux
    // pids stay below 2^22) and for this running process.
    let dead = store::testing::name_for(u32::MAX, 1, u64::from(std::process::id()));
    let live = store::testing::name_for(std::process::id(), 1, 999_999);
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

    // Only these two names are considered, so the test does not depend on
    // whatever else the dev environment holds.
    let swept = store::testing::sweep_orphans_among(|n| n == dead || n == live)
        .await
        .unwrap();
    assert_eq!(
        swept,
        store::testing::Swept {
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
