//! The `MetaStore` conformance suite on every backend: in-memory and
//! Postgres (the dev environment's, one fresh database per test).

mod memory {
    use std::sync::Arc;

    use store::MemoryMetaStore;

    async fn make() -> (Arc<MemoryMetaStore>, ()) {
        (Arc::new(MemoryMetaStore::new()), ())
    }

    store::meta_store_conformance!(make);
}

mod postgres {
    use std::sync::Arc;

    use store::PgMetaStore;
    use store::conformance::CONCURRENCY;
    use store::testing::TestDatabase;

    async fn make() -> (Arc<PgMetaStore>, TestDatabase) {
        let db = TestDatabase::create().await;
        // One connection per concurrent client, so the database itself sees
        // the full concurrency.
        let store = PgMetaStore::new(db.config().clone(), CONCURRENCY).unwrap();
        store.migrate().await.unwrap();
        // Migrating again is a no-op.
        store.migrate().await.unwrap();
        (Arc::new(store), db)
    }

    store::meta_store_conformance!(make);
}
