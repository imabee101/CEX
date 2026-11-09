//! Checks every [`MetaStore`] backend must pass.

use std::sync::Arc;

use bytes::Bytes;
use scion_core::{BlobId, ChangesetId, TreeId};

use super::linearizability::{self, Event, Op, Ret, State};
use super::{CONCURRENCY, pseudo_random};
use crate::{
    CasOutcome, DerivedKey, MAX_NAME_BYTES, MetaKey, MetaStore, PutOutcome, RefLogEntry,
    RefLogReason, RefState, RefUpdate, RepoId, StoreError,
};

const REPO: RepoId = RepoId(7);
const OTHER_REPO: RepoId = RepoId(8);

fn cs(n: u64) -> ChangesetId {
    ChangesetId::hash(&n.to_le_bytes())
}

fn bytes(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn derived(kind: u16, version: u16, changeset: ChangesetId) -> MetaKey {
    derived_with(kind, version, 0, changeset)
}

fn derived_with(kind: u16, version: u16, param: u64, changeset: ChangesetId) -> MetaKey {
    MetaKey::Derived(DerivedKey {
        kind,
        version,
        param,
        changeset,
    })
}

fn blob(n: u64) -> MetaKey {
    MetaKey::Blob(BlobId::hash(&n.to_le_bytes()))
}

fn update<'a>(name: &'a str, expected: Option<ChangesetId>, new: ChangesetId) -> RefUpdate<'a> {
    RefUpdate {
        name,
        expected,
        new,
        actor: "user:conformance",
        time_micros: 1_700_000_000_000_000,
        reason: RefLogReason::Manual,
    }
}

async fn get_one<S: MetaStore>(store: &S, repo: RepoId, key: MetaKey) -> Option<Bytes> {
    let mut got = store.get(repo, &[key]).await.expect("get failed");
    assert_eq!(got.len(), 1, "get returned the wrong number of results");
    got.pop().expect("one result")
}

async fn cas<S: MetaStore>(store: &S, repo: RepoId, u: RefUpdate<'_>) -> CasOutcome {
    store.update_ref(repo, &u).await.expect("update_ref failed")
}

async fn full_log<S: MetaStore>(store: &S, repo: RepoId, name: &str) -> Vec<RefLogEntry> {
    let mut out = Vec::new();
    loop {
        let after = out.last().map_or(0, |e: &RefLogEntry| e.seq);
        let page = store
            .ref_log(repo, name, after, 1000)
            .await
            .expect("ref_log failed");
        if page.is_empty() {
            return out;
        }
        out.extend(page);
    }
}

fn assert_invalid<T: std::fmt::Debug>(what: &str, r: Result<T, StoreError>) {
    assert!(
        matches!(r, Err(StoreError::InvalidArgument(_))),
        "{what}: expected InvalidArgument, got {r:?}"
    );
}

/// Records of every kind read back exactly as written, in request order;
/// absent keys read as `None`.
pub async fn records_round_trip<S: MetaStore>(store: Arc<S>) {
    let keys = [
        MetaKey::Changeset(cs(1)),
        MetaKey::Tree(TreeId::hash(b"t1")),
        derived(1, 1, cs(1)),
        blob(1),
        MetaKey::CaseNode(TreeId::hash(b"t1")),
    ];
    let missing = [
        MetaKey::Changeset(cs(2)),
        MetaKey::Tree(TreeId::hash(b"t2")),
        derived(1, 1, cs(2)),
        blob(2),
        MetaKey::CaseNode(TreeId::hash(b"t2")),
    ];
    assert_eq!(store.get(REPO, &missing).await.unwrap(), vec![None; 5]);
    let entries: Vec<(MetaKey, Bytes)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (*k, Bytes::from(pseudo_random(i as u64, 100 + i))))
        .collect();
    assert_eq!(
        store.put(REPO, &entries).await.unwrap(),
        vec![PutOutcome::Created; 5]
    );
    assert_eq!(store.put(REPO, &[]).await.unwrap(), vec![]);
    assert_eq!(store.get(REPO, &[]).await.unwrap(), vec![]);
    // Mixed present and absent keys, out of write order and repeated.
    let query = [
        keys[2], missing[0], keys[0], keys[3], keys[1], keys[2], missing[3], keys[4], missing[4],
    ];
    let got = store.get(REPO, &query).await.unwrap();
    let want: Vec<Option<Bytes>> = vec![
        Some(entries[2].1.clone()),
        None,
        Some(entries[0].1.clone()),
        Some(entries[3].1.clone()),
        Some(entries[1].1.clone()),
        Some(entries[2].1.clone()),
        None,
        Some(entries[4].1.clone()),
        None,
    ];
    assert_eq!(got, want);
    // An empty value is stored as empty, not as absent.
    let empty = derived(2, 1, cs(1));
    store.put(REPO, &[(empty, Bytes::new())]).await.unwrap();
    assert_eq!(get_one(&*store, REPO, empty).await, Some(Bytes::new()));
}

/// A put of an existing key never changes its stored bytes, alone or in a
/// batch with new keys.
pub async fn put_never_overwrites<S: MetaStore>(store: Arc<S>) {
    for key in [
        MetaKey::Changeset(cs(10)),
        MetaKey::Tree(TreeId::hash(b"t10")),
        derived(3, 2, cs(10)),
        blob(10),
        MetaKey::CaseNode(TreeId::hash(b"t10")),
    ] {
        assert_eq!(
            store.put(REPO, &[(key, bytes("first"))]).await.unwrap(),
            vec![PutOutcome::Created]
        );
        assert_eq!(
            store.put(REPO, &[(key, bytes("second"))]).await.unwrap(),
            vec![PutOutcome::Existed]
        );
        assert_eq!(
            store.put(REPO, &[(key, bytes(""))]).await.unwrap(),
            vec![PutOutcome::Existed]
        );
        let fresh = match key {
            MetaKey::Changeset(_) => MetaKey::Changeset(cs(11)),
            MetaKey::Tree(_) => MetaKey::Tree(TreeId::hash(b"t11")),
            MetaKey::CaseNode(_) => MetaKey::CaseNode(TreeId::hash(b"t11")),
            MetaKey::Derived(_) => derived(3, 2, cs(11)),
            MetaKey::Blob(_) => blob(11),
        };
        assert_eq!(
            store
                .put(REPO, &[(key, bytes("third")), (fresh, bytes("fresh"))])
                .await
                .unwrap(),
            vec![PutOutcome::Existed, PutOutcome::Created]
        );
        assert_eq!(get_one(&*store, REPO, key).await, Some(bytes("first")));
        assert_eq!(get_one(&*store, REPO, fresh).await, Some(bytes("fresh")));
    }
}

/// Blob records, changesets, trees, and derived values with the same id bytes
/// are separate records, and each derived type, version, and param is its own
/// key space.
pub async fn kinds_are_separate_namespaces<S: MetaStore>(store: Arc<S>) {
    let raw = *cs(20).as_bytes();
    let keys = [
        MetaKey::Changeset(ChangesetId::from_bytes(raw)),
        MetaKey::Tree(TreeId::from_bytes(raw)),
        MetaKey::CaseNode(TreeId::from_bytes(raw)),
        derived(1, 1, ChangesetId::from_bytes(raw)),
        derived(1, 2, ChangesetId::from_bytes(raw)),
        derived(2, 1, ChangesetId::from_bytes(raw)),
        derived_with(1, 1, 1, ChangesetId::from_bytes(raw)),
        derived_with(1, 1, u64::MAX, ChangesetId::from_bytes(raw)),
        MetaKey::Blob(BlobId::from_bytes(raw)),
    ];
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            store
                .put(REPO, &[(*key, bytes(&format!("v{i}")))])
                .await
                .unwrap(),
            vec![PutOutcome::Created],
            "{key:?} collided with an earlier kind"
        );
    }
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            get_one(&*store, REPO, *key).await,
            Some(bytes(&format!("v{i}")))
        );
    }
}

/// Records, refs, and ref logs in one repo are invisible to another, and a
/// key in one repo does not deduplicate against another.
pub async fn repos_are_isolated<S: MetaStore>(store: Arc<S>) {
    let keys = [
        MetaKey::Changeset(cs(30)),
        MetaKey::Tree(TreeId::hash(b"t30")),
        MetaKey::CaseNode(TreeId::hash(b"t30")),
        derived(1, 1, cs(30)),
        blob(30),
    ];
    for key in keys {
        store.put(REPO, &[(key, bytes("a"))]).await.unwrap();
        assert_eq!(get_one(&*store, OTHER_REPO, key).await, None);
        assert_eq!(
            store.put(OTHER_REPO, &[(key, bytes("b"))]).await.unwrap(),
            vec![PutOutcome::Created]
        );
        assert_eq!(get_one(&*store, REPO, key).await, Some(bytes("a")));
        assert_eq!(get_one(&*store, OTHER_REPO, key).await, Some(bytes("b")));
    }
    assert_eq!(
        cas(&*store, REPO, update("main", None, cs(1))).await,
        CasOutcome::Updated { seq: 1 }
    );
    assert_eq!(store.get_ref(OTHER_REPO, "main").await.unwrap(), None);
    assert_eq!(
        store.list_refs(OTHER_REPO, "", None, 1000).await.unwrap(),
        vec![]
    );
    assert_eq!(full_log(&*store, OTHER_REPO, "main").await, vec![]);
    assert_eq!(
        cas(&*store, OTHER_REPO, update("main", None, cs(2))).await,
        CasOutcome::Updated { seq: 1 }
    );
    assert_eq!(
        store.get_ref(REPO, "main").await.unwrap().map(|r| r.target),
        Some(cs(1))
    );
    assert_eq!(full_log(&*store, REPO, "main").await.len(), 1);
}

/// A batch that names a key twice is rejected and writes nothing.
pub async fn batch_naming_a_key_twice_is_rejected<S: MetaStore>(store: Arc<S>) {
    let fresh = MetaKey::Tree(TreeId::hash(b"t40"));
    let dup = MetaKey::Changeset(cs(40));
    assert_invalid(
        "duplicate key",
        store
            .put(
                REPO,
                &[(fresh, bytes("x")), (dup, bytes("a")), (dup, bytes("b"))],
            )
            .await,
    );
    assert_eq!(get_one(&*store, REPO, fresh).await, None);
    assert_eq!(get_one(&*store, REPO, dup).await, None);
}

/// Concurrent puts of one key with different bytes create it exactly once,
/// and every later read returns the winner's bytes.
pub async fn concurrent_puts_of_one_key_create_it_once<S: MetaStore>(store: Arc<S>) {
    for key in [
        MetaKey::Changeset(cs(50)),
        MetaKey::Tree(TreeId::hash(b"t50")),
        derived(4, 1, cs(50)),
    ] {
        let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
        let tasks: Vec<_> = (0..CONCURRENCY)
            .map(|i| {
                let (store, barrier) = (store.clone(), barrier.clone());
                tokio::spawn(async move {
                    let value = bytes(&format!("writer {i}"));
                    barrier.wait().await;
                    let outcome = store.put(REPO, &[(key, value.clone())]).await.unwrap();
                    (outcome, value)
                })
            })
            .collect();
        let mut winners = Vec::new();
        for task in tasks {
            let (outcome, value) = task.await.unwrap();
            assert_eq!(outcome.len(), 1);
            if outcome[0] == PutOutcome::Created {
                winners.push(value);
            }
        }
        assert_eq!(
            winners.len(),
            1,
            "{key:?} was created {} times",
            winners.len()
        );
        assert_eq!(get_one(&*store, REPO, key).await, Some(winners[0].clone()));
    }
}

/// Concurrent batches over the same keys, each naming them in a different
/// order, all succeed, and each key is created by exactly one of them. A
/// backend that takes per-key locks in request order deadlocks here.
pub async fn concurrent_overlapping_batches_all_succeed<S: MetaStore>(store: Arc<S>) {
    // Several rounds, each on fresh keys; later rounds run on warm
    // connections, so the batches truly overlap in the backend.
    const ROUNDS: u64 = 8;
    const KEYS: u64 = 64;
    for round in 0..ROUNDS {
        let base = 70 + round * KEYS;
        let keys: Vec<MetaKey> = (base..base + KEYS)
            .flat_map(|i| {
                [
                    MetaKey::Changeset(cs(i)),
                    MetaKey::Tree(TreeId::from_bytes(*cs(i).as_bytes())),
                    derived(5, 1, cs(i)),
                ]
            })
            .collect();
        let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
        let tasks: Vec<_> = (0..CONCURRENCY as u64)
            .map(|w| {
                let (store, barrier) = (store.clone(), barrier.clone());
                // A per-writer permutation: every writer rotated, odd ones
                // reversed.
                let mut order = keys.clone();
                let len = order.len();
                order.rotate_left((w as usize * 7) % len);
                if w % 2 == 1 {
                    order.reverse();
                }
                tokio::spawn(async move {
                    let entries: Vec<(MetaKey, Bytes)> = order
                        .iter()
                        .map(|k| (*k, bytes(&format!("writer {w}"))))
                        .collect();
                    barrier.wait().await;
                    let outcome = store.put(REPO, &entries).await;
                    (w, order, outcome)
                })
            })
            .collect();
        let mut created = std::collections::HashMap::new();
        for task in tasks {
            let (w, order, outcome) = task.await.unwrap();
            let outcome = outcome.unwrap_or_else(|e| {
                panic!("round {round}, writer {w}: an overlapping batch failed with {e:?}")
            });
            assert_eq!(outcome.len(), order.len());
            for (k, o) in order.iter().zip(outcome) {
                if o == PutOutcome::Created {
                    assert!(
                        created.insert(*k, w).is_none(),
                        "{k:?} was created more than once"
                    );
                }
            }
        }
        for k in &keys {
            let w = created
                .get(k)
                .unwrap_or_else(|| panic!("{k:?} was never created"));
            assert_eq!(
                get_one(&*store, REPO, *k).await,
                Some(bytes(&format!("writer {w}")))
            );
        }
    }
}

/// Records up to several MiB round-trip byte for byte.
pub async fn large_records_round_trip<S: MetaStore>(store: Arc<S>) {
    let big = Bytes::from(pseudo_random(60, 9 << 20));
    let key = MetaKey::Changeset(cs(60));
    store.put(REPO, &[(key, big.clone())]).await.unwrap();
    assert!(get_one(&*store, REPO, key).await == Some(big));
}

/// CAS creates a ref only when absent, moves it only from the expected
/// target, reports the current state on a mismatch, and writes exactly one
/// ref-log row per successful CAS and none per failed one.
pub async fn ref_cas_creates_moves_and_rejects<S: MetaStore>(store: Arc<S>) {
    let name = "release/1";
    assert_eq!(store.get_ref(REPO, name).await.unwrap(), None);
    // Moving an absent ref fails.
    assert_eq!(
        cas(&*store, REPO, update(name, Some(cs(1)), cs(2))).await,
        CasOutcome::Mismatch { current: None }
    );
    assert_eq!(full_log(&*store, REPO, name).await, vec![]);

    let mut create = update(name, None, cs(1));
    create.actor = "user:alice";
    create.time_micros = 11;
    create.reason = RefLogReason::Land;
    assert_eq!(
        cas(&*store, REPO, create).await,
        CasOutcome::Updated { seq: 1 }
    );
    let at1 = RefState {
        name: name.to_owned(),
        target: cs(1),
        seq: 1,
    };
    assert_eq!(store.get_ref(REPO, name).await.unwrap(), Some(at1.clone()));
    // Creating an existing ref fails.
    assert_eq!(
        cas(&*store, REPO, update(name, None, cs(9))).await,
        CasOutcome::Mismatch {
            current: Some(at1.clone())
        }
    );
    // Moving from the wrong target fails.
    assert_eq!(
        cas(&*store, REPO, update(name, Some(cs(2)), cs(9))).await,
        CasOutcome::Mismatch {
            current: Some(at1.clone())
        }
    );
    let mut mv = update(name, Some(cs(1)), cs(2));
    mv.actor = "agent:bot";
    mv.time_micros = -5;
    mv.reason = RefLogReason::Backout;
    assert_eq!(cas(&*store, REPO, mv).await, CasOutcome::Updated { seq: 2 });
    // Moving back to an earlier target is allowed and gets a new seq.
    assert_eq!(
        cas(&*store, REPO, update(name, Some(cs(2)), cs(1))).await,
        CasOutcome::Updated { seq: 3 }
    );
    assert_eq!(
        store.get_ref(REPO, name).await.unwrap(),
        Some(RefState {
            name: name.to_owned(),
            target: cs(1),
            seq: 3
        })
    );
    assert_eq!(
        full_log(&*store, REPO, name).await,
        vec![
            RefLogEntry {
                seq: 1,
                old: None,
                new: cs(1),
                actor: "user:alice".into(),
                time_micros: 11,
                reason: RefLogReason::Land,
            },
            RefLogEntry {
                seq: 2,
                old: Some(cs(1)),
                new: cs(2),
                actor: "agent:bot".into(),
                time_micros: -5,
                reason: RefLogReason::Backout,
            },
            RefLogEntry {
                seq: 3,
                old: Some(cs(2)),
                new: cs(1),
                actor: "user:conformance".into(),
                time_micros: 1_700_000_000_000_000,
                reason: RefLogReason::Manual,
            },
        ]
    );
    // Another ref has its own seq.
    assert_eq!(
        cas(&*store, REPO, update("main", None, cs(5))).await,
        CasOutcome::Updated { seq: 1 }
    );
}

/// `list_refs` returns refs under a prefix in name byte order, resuming
/// strictly after a cursor, at most `limit` at a time.
pub async fn list_refs_in_byte_order_with_pagination<S: MetaStore>(store: Arc<S>) {
    let names = [
        "main",
        "release/10",
        "release/2",
        "release/1",
        "release/é",
        "release/z",
        "release0",
        "releas",
        "tags/v1",
        "Release/1",
    ];
    for (i, name) in names.iter().enumerate() {
        assert_eq!(
            cas(&*store, REPO, update(name, None, cs(i as u64))).await,
            CasOutcome::Updated { seq: 1 }
        );
    }
    let list = |prefix: &'static str, after: Option<&'static str>, limit: usize| {
        let store = store.clone();
        async move {
            store
                .list_refs(REPO, prefix, after, limit)
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.name)
                .collect::<Vec<_>>()
        }
    };
    let mut all: Vec<&str> = names.to_vec();
    all.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(list("", None, 1000).await, all);
    assert_eq!(
        list("release/", None, 1000).await,
        [
            "release/1",
            "release/10",
            "release/2",
            "release/z",
            "release/é"
        ]
    );
    assert_eq!(list("release/", None, 2).await, ["release/1", "release/10"]);
    assert_eq!(
        list("release/", Some("release/10"), 2).await,
        ["release/2", "release/z"]
    );
    assert_eq!(list("release/", Some("release/z"), 2).await, ["release/é"]);
    assert_eq!(
        list("release/", Some("release/é"), 2).await,
        Vec::<String>::new()
    );
    // A cursor before the prefix range does not widen it.
    assert_eq!(list("tags/", Some("a"), 10).await, ["tags/v1"]);
    assert_eq!(list("nope", None, 10).await, Vec::<String>::new());
    // Listing returns targets and seqs too.
    let main = store.list_refs(REPO, "main", None, 1).await.unwrap();
    assert_eq!(
        main,
        vec![RefState {
            name: "main".into(),
            target: cs(0),
            seq: 1
        }]
    );
}

/// `ref_log` returns rows after a seq, in seq order, at most `limit` at a
/// time.
pub async fn ref_log_pages_by_seq<S: MetaStore>(store: Arc<S>) {
    let mut prev = None;
    for i in 0..5 {
        assert_eq!(
            cas(&*store, REPO, update("main", prev, cs(i))).await,
            CasOutcome::Updated { seq: i + 1 }
        );
        prev = Some(cs(i));
    }
    let seqs = |after: u64, limit: usize| {
        let store = store.clone();
        async move {
            store
                .ref_log(REPO, "main", after, limit)
                .await
                .unwrap()
                .into_iter()
                .map(|e| (e.seq, e.new))
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(seqs(0, 2).await, vec![(1, cs(0)), (2, cs(1))]);
    assert_eq!(seqs(2, 2).await, vec![(3, cs(2)), (4, cs(3))]);
    assert_eq!(seqs(4, 2).await, vec![(5, cs(4))]);
    assert_eq!(seqs(5, 2).await, vec![]);
    assert_eq!(seqs(u64::MAX, 2).await, vec![]);
    assert_eq!(store.ref_log(REPO, "absent", 0, 10).await.unwrap(), vec![]);
}

/// Empty, over-long, or NUL-containing names and actors, and page limits
/// outside 1 to 1,000, are rejected without writing anything.
pub async fn invalid_names_and_limits_are_rejected<S: MetaStore>(store: Arc<S>) {
    let long = "x".repeat(MAX_NAME_BYTES + 1);
    let at_limit = "y".repeat(MAX_NAME_BYTES);
    for bad in ["", "a\0b", long.as_str()] {
        assert_invalid(
            "ref name",
            store.update_ref(REPO, &update(bad, None, cs(1))).await,
        );
        assert_invalid("get_ref", store.get_ref(REPO, bad).await);
        assert_invalid("ref_log", store.ref_log(REPO, bad, 0, 10).await);
        let mut u = update("main", None, cs(1));
        u.actor = bad;
        assert_invalid("actor", store.update_ref(REPO, &u).await);
    }
    assert_invalid("prefix", store.list_refs(REPO, "a\0", None, 10).await);
    assert_invalid("prefix", store.list_refs(REPO, &long, None, 10).await);
    assert_invalid("cursor", store.list_refs(REPO, "", Some("a\0"), 10).await);
    for limit in [0, 1001] {
        assert_invalid("list limit", store.list_refs(REPO, "", None, limit).await);
        assert_invalid("log limit", store.ref_log(REPO, "main", 0, limit).await);
    }
    assert_eq!(store.list_refs(REPO, "", None, 1000).await.unwrap(), vec![]);
    // The bounds themselves are accepted.
    assert_eq!(
        cas(&*store, REPO, update(&at_limit, None, cs(1))).await,
        CasOutcome::Updated { seq: 1 }
    );
    assert_eq!(store.list_refs(REPO, "", None, 1).await.unwrap().len(), 1);
}

/// Concurrent creations of one ref: exactly one succeeds, and every other
/// reports the winner's state.
pub async fn concurrent_ref_creation_has_one_winner<S: MetaStore>(store: Arc<S>) {
    let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
    let tasks: Vec<_> = (0..CONCURRENCY as u64)
        .map(|i| {
            let (store, barrier) = (store.clone(), barrier.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                (
                    i,
                    cas(&*store, REPO, update("tags/race", None, cs(100 + i))).await,
                )
            })
        })
        .collect();
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    let winners: Vec<u64> = outcomes
        .iter()
        .filter(|(_, o)| *o == CasOutcome::Updated { seq: 1 })
        .map(|(i, _)| *i)
        .collect();
    assert_eq!(winners.len(), 1, "outcomes: {outcomes:?}");
    let winner = RefState {
        name: "tags/race".into(),
        target: cs(100 + winners[0]),
        seq: 1,
    };
    for (i, o) in &outcomes {
        if *i != winners[0] {
            assert_eq!(
                *o,
                CasOutcome::Mismatch {
                    current: Some(winner.clone())
                }
            );
        }
    }
    assert_eq!(full_log(&*store, REPO, "tags/race").await.len(), 1);
}

/// Seed of the linearizability workload; printed on failure.
pub const LINEARIZABILITY_SEED: u64 = 0x5C10_2026_0002;
/// Operations per client in the linearizability workload.
pub const LINEARIZABILITY_OPS: usize = 30;
const VALUES: u8 = 3;

fn value(v: u8) -> ChangesetId {
    cs(1_000 + u64::from(v))
}

fn model_state(state: Option<RefState>) -> State {
    state.map(|s| {
        let v = (0..VALUES)
            .find(|v| value(*v) == s.target)
            .unwrap_or_else(|| panic!("ref holds a target no client wrote: {:?}", s.target));
        (v, s.seq)
    })
}

/// 64 clients run random reads and CASes on one ref; the recorded history
/// must be linearizable, and the ref log must hold exactly the successful
/// CASes, gap-free and chained.
pub async fn ref_cas_is_linearizable_at_64_way_concurrency<S: MetaStore>(store: Arc<S>) {
    const NAME: &str = "main";
    let scripts = linearizability::random_scripts(
        LINEARIZABILITY_SEED,
        CONCURRENCY,
        LINEARIZABILITY_OPS,
        VALUES,
    );
    let exec_store = store.clone();
    let exec: linearizability::Exec = Arc::new(move |op| {
        let store = exec_store.clone();
        Box::pin(async move {
            match op {
                Op::Read => Ret::Read(model_state(
                    store.get_ref(REPO, NAME).await.expect("get_ref failed"),
                )),
                Op::Cas { expected, new } => {
                    let u = update(NAME, expected.map(value), value(new));
                    match store.update_ref(REPO, &u).await.expect("update_ref failed") {
                        CasOutcome::Updated { seq } => Ret::CasUpdated(seq),
                        CasOutcome::Mismatch { current } => Ret::CasMismatch(model_state(current)),
                    }
                }
            }
        })
    });
    // The recorder issues every client's first request while all the others
    // are outstanding, so the history is 64-way concurrent by
    // construction; the recorder's own tests check that. A backend may still
    // serve the calls one at a time: a linearizable store is free to.
    let history = linearizability::record(scripts, exec).await;
    let result = linearizability::check(&history, None, 500_000_000);
    assert_eq!(
        result,
        Ok(()),
        "seed {LINEARIZABILITY_SEED:#x}: history is not linearizable"
    );

    let mut updates: Vec<&Event> = history
        .iter()
        .filter(|e| matches!(e.ret, Ret::CasUpdated(_)))
        .collect();
    let mismatches = history
        .iter()
        .filter(|e| matches!(e.ret, Ret::CasMismatch(_)))
        .count();
    assert!(
        updates.len() >= 2 && mismatches >= 2,
        "seed {LINEARIZABILITY_SEED:#x}: the workload needs both successful and failed CASes"
    );
    updates.sort_by_key(|e| match e.ret {
        Ret::CasUpdated(seq) => seq,
        _ => unreachable!("filtered to updates"),
    });
    let log = full_log(&*store, REPO, NAME).await;
    assert_eq!(log.len(), updates.len(), "ref log rows != successful CASes");
    let mut prev = None;
    for (row, e) in log.iter().zip(&updates) {
        let Op::Cas { expected, new } = e.op else {
            unreachable!("only CASes update")
        };
        assert_eq!(Ret::CasUpdated(row.seq), e.ret);
        assert_eq!(row.old, expected.map(value));
        assert_eq!(row.old, prev, "ref log is not chained at seq {}", row.seq);
        assert_eq!(row.new, value(new));
        prev = Some(row.new);
    }
    assert_eq!(
        store
            .get_ref(REPO, NAME)
            .await
            .unwrap()
            .map(|r| (r.target, r.seq)),
        log.last().map(|r| (r.new, r.seq))
    );
}
