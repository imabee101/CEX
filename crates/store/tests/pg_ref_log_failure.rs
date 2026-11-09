//! A ref CAS whose ref-log insert fails changes nothing: the ref keeps its
//! target and seq, no ref-log row is written, and the next CAS continues the
//! log with no gap (INV-16, INV-18). A trigger in the test's own database
//! makes the insert fail inside the CAS transaction, after the ref row has
//! been written.

use scion_core::ChangesetId;
use store::testing::TestDatabase;
use store::{
    CasOutcome, MetaStore, PgMetaStore, PgTls, PoolLimits, RefLogReason, RefUpdate, RepoId,
    StoreError,
};

const REPO: RepoId = RepoId(1);

fn target(n: u8) -> ChangesetId {
    ChangesetId::from_bytes([n; 32])
}

fn update(name: &str, expected: Option<u8>, new: u8) -> RefUpdate<'_> {
    RefUpdate {
        name,
        expected: expected.map(target),
        new: target(new),
        actor: "user:test",
        time_micros: i64::from(new),
        reason: RefLogReason::Manual,
    }
}

/// Every row of `refs` and `ref_log`, read directly.
async fn rows(db: &tokio_postgres::Client) -> (Vec<(Vec<u8>, Vec<u8>, i64)>, Vec<(Vec<u8>, i64)>) {
    let refs = db
        .query("SELECT name, target, seq FROM refs ORDER BY name", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let log = db
        .query("SELECT name, seq FROM ref_log ORDER BY name, seq", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    (refs, log)
}

#[tokio::test]
async fn a_failed_ref_log_insert_leaves_the_ref_and_its_log_unchanged() {
    let db = TestDatabase::create().await;
    // One connection, so every call after a failure reuses or replaces the
    // one the failed CAS held.
    let store = PgMetaStore::new(
        db.config().clone(),
        PgTls::LoopbackPlaintext,
        PoolLimits::new(1),
    )
    .unwrap();
    store.migrate().await.unwrap();
    let observer = db.connect().await;
    assert_eq!(
        store
            .update_ref(REPO, &update("main", None, 1))
            .await
            .unwrap(),
        CasOutcome::Updated { seq: 1 }
    );
    observer
        .batch_execute(
            "CREATE FUNCTION fail_ref_log() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected ref_log failure'; END $$;
             CREATE TRIGGER fail BEFORE INSERT ON ref_log
               FOR EACH ROW EXECUTE FUNCTION fail_ref_log();",
        )
        .await
        .unwrap();
    let before = rows(&observer).await;

    // Moving an existing ref, and creating a new one: each writes its ref
    // row before the failing insert.
    for (name, expected, new) in [("main", Some(1), 2), ("release/1", None, 3)] {
        let failed = store.update_ref(REPO, &update(name, expected, new)).await;
        assert!(matches!(failed, Err(StoreError::Internal(_))), "{failed:?}");
        assert_eq!(
            rows(&observer).await,
            before,
            "{name}: a failed CAS left rows"
        );
        let state = store.get_ref(REPO, name).await.unwrap();
        assert_eq!(
            state.map(|s| (s.target, s.seq)),
            expected.map(|e| (target(e), 1))
        );
    }

    observer
        .batch_execute("DROP TRIGGER fail ON ref_log")
        .await
        .unwrap();
    assert_eq!(
        store
            .update_ref(REPO, &update("main", Some(1), 2))
            .await
            .unwrap(),
        CasOutcome::Updated { seq: 2 }
    );
    assert_eq!(
        store
            .update_ref(REPO, &update("release/1", None, 3))
            .await
            .unwrap(),
        CasOutcome::Updated { seq: 1 }
    );
    let log = store.ref_log(REPO, "main", 0, 10).await.unwrap();
    let seqs: Vec<_> = log.iter().map(|e| (e.seq, e.old, e.new)).collect();
    assert_eq!(
        seqs,
        [(1, None, target(1)), (2, Some(target(1)), target(2))]
    );
    assert_eq!(
        store.ref_log(REPO, "release/1", 0, 10).await.unwrap().len(),
        1
    );
}
