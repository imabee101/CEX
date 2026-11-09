//! A Postgres call abandoned at any point (the caller's future dropped, as
//! when a client disconnects or a request times out) leaves the database as
//! if the call had either completed or never started: a `put` batch is
//! applied whole or not at all, and a ref CAS moves the ref exactly when it
//! writes its ref-log row. The pooled connection the abandoned call held is
//! reused afterwards without carrying its open transaction into the next
//! call.
//!
//! The store talks to Postgres through a proxy that stops one direction of
//! the connection at an exact message boundary. Each call is abandoned once
//! at every boundary in each direction, until a call completes before its
//! hold is reached; the boundaries include ones inside the call's
//! transaction, between `BEGIN` and `COMMIT`.

use bytes::Bytes;
use scion_core::{ChangesetId, TreeId};
use store::testing::{Direction, PgProxy, TestDatabase};
use store::{
    CasOutcome, DerivedKey, MetaKey, MetaStore, PgMetaStore, RefLogReason, RefUpdate, RepoId,
};

/// A store with one pooled connection, reached through a proxy, and an
/// observer connected directly.
async fn stores(db: &TestDatabase) -> (PgProxy, PgMetaStore, PgMetaStore) {
    let proxy = PgProxy::start(db.config()).await;
    let one = || store::PoolLimits::new(1);
    let store = PgMetaStore::new(
        proxy.config(db.config()),
        store::PgTls::LoopbackPlaintext,
        one(),
    )
    .unwrap();
    store.migrate().await.unwrap();
    let observer =
        PgMetaStore::new(db.config().clone(), store::PgTls::LoopbackPlaintext, one()).unwrap();
    (proxy, store, observer)
}

/// What the rounds of one test saw.
#[derive(Default, Debug)]
struct Tally {
    /// Calls abandoned with none of their effect applied.
    none: usize,
    /// Calls abandoned with all of their effect applied.
    all: usize,
    /// Calls abandoned between their `BEGIN` and their `COMMIT`.
    inside_transaction: usize,
}

impl Tally {
    fn check(&self) {
        assert!(
            self.none > 0 && self.all > 0 && self.inside_transaction > 0,
            "the boundaries did not span the call: {self:?}"
        );
    }
}

fn batch(round: u64) -> Vec<(MetaKey, Bytes)> {
    let mut entries = Vec::new();
    for i in 0..40u64 {
        let seed = (round << 32) | i;
        let id = ChangesetId::hash(&seed.to_le_bytes());
        let value = Bytes::from(vec![(i % 251) as u8; 64]);
        entries.push((MetaKey::Changeset(id), value.clone()));
        entries.push((
            MetaKey::CaseNode(TreeId::from_bytes(*id.as_bytes())),
            value.clone(),
        ));
        entries.push((
            MetaKey::Tree(TreeId::from_bytes(*id.as_bytes())),
            value.clone(),
        ));
        entries.push((
            MetaKey::Derived(DerivedKey {
                kind: 1,
                version: 1,
                param: 0,
                changeset: id,
            }),
            value,
        ));
    }
    entries
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_put_applies_its_whole_batch_or_none_of_it() {
    let db = TestDatabase::create().await;
    let (proxy, store, observer) = stores(&db).await;
    let repo = RepoId(1);
    let mut tally = Tally::default();
    let mut round = 0;
    for direction in [Direction::ToServer, Direction::ToClient] {
        for pass in 0.. {
            round += 1;
            let entries = batch(round);
            let keys: Vec<MetaKey> = entries.iter().map(|(k, _)| *k).collect();
            proxy.hold(direction, pass);
            let ended = proxy.run_or_abandon(store.put(repo, &entries)).await;
            proxy.release();
            // A call on the reused connection runs after anything the
            // abandoned call left queued on it; once it returns, the pool
            // holds one idle connection and every other has closed.
            store.get(repo, &keys[..1]).await.unwrap();
            proxy.wait_until_open(1).await;
            let seen = observer.get(repo, &keys).await.unwrap();
            let present = seen.iter().filter(|v| v.is_some()).count();
            let at = format!("{direction:?} after {pass} messages");
            assert!(
                present == 0 || present == keys.len(),
                "{at}: {present} of {} entries applied",
                keys.len()
            );
            if present == keys.len() {
                for ((_, want), got) in entries.iter().zip(&seen) {
                    assert_eq!(got.as_ref(), Some(want), "{at}");
                }
            }
            match ended {
                Ok(outcome) => {
                    outcome.unwrap();
                    assert_eq!(present, keys.len(), "{at}: a completed put was lost");
                    break;
                }
                Err(stopped) => {
                    tally.inside_transaction += usize::from(stopped.in_transaction);
                    if present == 0 {
                        tally.none += 1;
                    } else {
                        tally.all += 1;
                    }
                }
            }
            // Whatever happened, the batch can be applied now, exactly once.
            let outcomes = store.put(repo, &entries).await.unwrap();
            let created = outcomes
                .iter()
                .filter(|o| **o == store::PutOutcome::Created)
                .count();
            assert_eq!(created, if present == 0 { keys.len() } else { 0 }, "{at}");
        }
    }
    tally.check();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_cas_moves_the_ref_exactly_when_it_writes_its_ref_log_row() {
    let db = TestDatabase::create().await;
    let (proxy, store, observer) = stores(&db).await;
    let repo = RepoId(1);
    let target = |n: u64| ChangesetId::hash(&n.to_le_bytes());
    let mut current = None;
    let mut tally = Tally::default();
    let mut n = 0;
    for direction in [Direction::ToServer, Direction::ToClient] {
        for pass in 0.. {
            n += 1;
            let new = target(n);
            let update = RefUpdate {
                name: "main",
                expected: current,
                new,
                actor: "user:cancel",
                time_micros: n as i64,
                reason: RefLogReason::Land,
            };
            proxy.hold(direction, pass);
            let ended = proxy.run_or_abandon(store.update_ref(repo, &update)).await;
            proxy.release();
            store.get_ref(repo, "main").await.unwrap();
            proxy.wait_until_open(1).await;
            let at = format!("{direction:?} after {pass} messages");
            let state = observer.get_ref(repo, "main").await.unwrap();
            let log = observer.ref_log(repo, "main", 0, 1000).await.unwrap();
            assert_eq!(
                state.as_ref().map(|s| (s.target, s.seq)),
                log.last().map(|e| (e.new, e.seq)),
                "{at}: ref and ref log disagree"
            );
            for (i, e) in log.iter().enumerate() {
                assert_eq!(e.seq, i as u64 + 1, "{at}: ref log has a gap");
            }
            let moved = state.as_ref().map(|s| s.target) == Some(new);
            if moved {
                current = Some(new);
            }
            match ended {
                Ok(outcome) => {
                    assert!(
                        matches!(outcome, Ok(CasOutcome::Updated { .. })),
                        "{at}: {outcome:?}"
                    );
                    assert!(moved, "{at}: a completed CAS did not move the ref");
                    break;
                }
                Err(stopped) => {
                    tally.inside_transaction += usize::from(stopped.in_transaction);
                    if moved {
                        tally.all += 1;
                    } else {
                        tally.none += 1;
                    }
                }
            }
        }
    }
    tally.check();
}
