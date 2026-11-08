//! A Postgres call abandoned at any point (the caller's future dropped, as
//! when a client disconnects or a request times out) leaves the database as
//! if the call had either completed or never started: a `put` batch is
//! applied whole or not at all, and a ref CAS moves the ref exactly when it
//! writes its ref-log row. The pooled connection the abandoned call held is
//! reused afterwards without carrying its open transaction into the next
//! call.

use std::time::Duration;

use bytes::Bytes;
use scion_core::{ChangesetId, TreeId};
use store::testing::TestDatabase;
use store::{
    CasOutcome, DerivedKey, MetaKey, MetaStore, PgMetaStore, RefLogReason, RefUpdate, RepoId,
};

/// Delays at which the caller abandons its call, from before the first
/// statement is sent onwards, then `None`: one call run to completion.
fn delays() -> impl Iterator<Item = Option<Duration>> {
    (0..40u64)
        .map(|i| Some(Duration::from_micros(i * i * 40)))
        .chain([None])
}

/// Runs `call`, abandoning it after `delay` unless `delay` is `None`; `None`
/// when it was abandoned.
async fn run<T>(delay: Option<Duration>, call: impl Future<Output = T>) -> Option<T> {
    match delay {
        Some(d) => tokio::time::timeout(d, call).await.ok(),
        None => Some(call.await),
    }
}

fn batch(round: u64) -> Vec<(MetaKey, Bytes)> {
    let mut entries = Vec::new();
    for i in 0..40u64 {
        let seed = (round << 32) | i;
        let id = ChangesetId::hash(&seed.to_le_bytes());
        // A few large values make the batch take long enough to abandon
        // mid-way.
        let len = if i % 13 == 0 { 512 << 10 } else { 64 };
        let value = Bytes::from(vec![(i % 251) as u8; len]);
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
    // One connection, so every call reuses the one an abandoned call held.
    let store = PgMetaStore::new(db.config().clone(), 1).unwrap();
    store.migrate().await.unwrap();
    let observer = PgMetaStore::new(db.config().clone(), 1).unwrap();
    let repo = RepoId(1);
    let (mut none, mut all) = (0, 0);
    for (round, delay) in delays().enumerate() {
        let entries = batch(round as u64);
        let _ = run(delay, store.put(repo, &entries)).await;
        let keys: Vec<MetaKey> = entries.iter().map(|(k, _)| *k).collect();
        // A call on the reused connection runs after anything the abandoned
        // call left queued on it.
        store.get(repo, &keys[..1]).await.unwrap();
        let seen = observer.get(repo, &keys).await.unwrap();
        let present = seen.iter().filter(|v| v.is_some()).count();
        assert!(
            present == 0 || present == keys.len(),
            "round {round}, abandoned after {delay:?}: {present} of {} entries applied",
            keys.len()
        );
        if present == 0 {
            none += 1;
        } else {
            all += 1;
            for ((_, want), got) in entries.iter().zip(&seen) {
                assert_eq!(got.as_ref(), Some(want));
            }
        }
        // Whatever happened, the batch can be applied now, exactly once.
        let outcomes = store.put(repo, &entries).await.unwrap();
        let created = outcomes
            .iter()
            .filter(|o| **o == store::PutOutcome::Created)
            .count();
        assert_eq!(created, if present == 0 { keys.len() } else { 0 });
    }
    assert!(
        none > 0 && all > 0,
        "the delays did not span the call: {none} abandoned before, {all} after"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_cas_moves_the_ref_exactly_when_it_writes_its_ref_log_row() {
    let db = TestDatabase::create().await;
    let store = PgMetaStore::new(db.config().clone(), 1).unwrap();
    store.migrate().await.unwrap();
    let observer = PgMetaStore::new(db.config().clone(), 1).unwrap();
    let repo = RepoId(1);
    let target = |n: u64| ChangesetId::hash(&n.to_le_bytes());
    let mut current = None;
    let (mut moved, mut unmoved) = (0, 0);
    for (n, delay) in delays().enumerate() {
        let new = target(n as u64 + 1);
        let update = RefUpdate {
            name: "main",
            expected: current,
            new,
            actor: "user:cancel",
            time_micros: n as i64,
            reason: RefLogReason::Land,
        };
        let finished = run(delay, store.update_ref(repo, &update)).await;
        if let Some(outcome) = &finished {
            assert!(
                matches!(outcome, Ok(CasOutcome::Updated { .. })),
                "{outcome:?}"
            );
        }
        store.get_ref(repo, "main").await.unwrap();
        let state = observer.get_ref(repo, "main").await.unwrap();
        let log = observer.ref_log(repo, "main", 0, 1000).await.unwrap();
        assert_eq!(
            state.as_ref().map(|s| (s.target, s.seq)),
            log.last().map(|e| (e.new, e.seq)),
            "step {n}, abandoned after {delay:?}: ref and ref log disagree"
        );
        for (i, e) in log.iter().enumerate() {
            assert_eq!(e.seq, i as u64 + 1, "ref log has a gap");
        }
        if state.as_ref().map(|s| s.target) == Some(new) {
            moved += 1;
            current = Some(new);
        } else {
            assert!(finished.is_none(), "a completed CAS did not move the ref");
            assert_eq!(state.map(|s| s.target), current);
            unmoved += 1;
        }
    }
    assert!(
        moved > 0 && unmoved > 0,
        "the delays did not span the call: {unmoved} abandoned before, {moved} after"
    );
}
