//! The linearizability checker accepts linearizable histories and rejects
//! every kind of violation a ref CAS could commit, including a real lost
//! update produced by a non-atomic register.

use std::sync::{Arc, Mutex};

use store::conformance::SplitMix64;
use store::conformance::linearizability::{
    Event, Exec, Op, Ret, State, Violation, check, max_overlap, random_scripts, record, step,
};

const BUDGET: u64 = 10_000_000;

fn ev(client: usize, op: Op, ret: Ret, call: u64, ret_time: u64) -> Event {
    Event {
        client,
        op,
        ret,
        call,
        ret_time,
    }
}

fn cas(expected: Option<u8>, new: u8) -> Op {
    Op::Cas { expected, new }
}

fn not_linearizable(r: Result<(), Violation>) -> bool {
    matches!(r, Err(Violation::NotLinearizable { .. }))
}

#[test]
fn model_steps() {
    assert_eq!(
        step(None, cas(None, 1), Ret::CasUpdated(1)),
        Some(Some((1, 1)))
    );
    assert_eq!(step(None, cas(None, 1), Ret::CasUpdated(2)), None);
    assert_eq!(
        step(Some((1, 4)), cas(Some(1), 2), Ret::CasUpdated(5)),
        Some(Some((2, 5)))
    );
    assert_eq!(
        step(Some((1, 4)), cas(Some(2), 2), Ret::CasUpdated(5)),
        None
    );
    assert_eq!(step(Some((1, 4)), cas(None, 2), Ret::CasUpdated(5)), None);
    assert_eq!(
        step(
            Some((1, 4)),
            cas(Some(2), 0),
            Ret::CasMismatch(Some((1, 4)))
        ),
        Some(Some((1, 4)))
    );
    assert_eq!(
        step(
            Some((1, 4)),
            cas(Some(1), 0),
            Ret::CasMismatch(Some((1, 4)))
        ),
        None
    );
    assert_eq!(
        step(
            Some((1, 4)),
            cas(Some(2), 0),
            Ret::CasMismatch(Some((1, 3)))
        ),
        None
    );
    assert_eq!(
        step(None, cas(Some(2), 0), Ret::CasMismatch(None)),
        Some(None)
    );
    assert_eq!(
        step(Some((1, 4)), Op::Read, Ret::Read(Some((1, 4)))),
        Some(Some((1, 4)))
    );
    assert_eq!(step(Some((1, 4)), Op::Read, Ret::Read(None)), None);
    assert_eq!(step(None, Op::Read, Ret::CasUpdated(1)), None);
}

#[test]
fn accepts_a_sequential_history() {
    let h = [
        ev(0, Op::Read, Ret::Read(None), 0, 1),
        ev(0, cas(None, 1), Ret::CasUpdated(1), 2, 3),
        ev(1, cas(None, 2), Ret::CasMismatch(Some((1, 1))), 4, 5),
        ev(1, cas(Some(1), 2), Ret::CasUpdated(2), 6, 7),
        ev(0, Op::Read, Ret::Read(Some((2, 2))), 8, 9),
    ];
    assert_eq!(check(&h, None, BUDGET), Ok(()));
}

#[test]
fn accepts_overlapping_operations_in_an_order_other_than_invocation() {
    // B is invoked after A but must take effect first; a read overlapping both
    // sees the intermediate state.
    let h = [
        ev(0, cas(Some(2), 3), Ret::CasUpdated(2), 0, 10),
        ev(1, cas(None, 2), Ret::CasUpdated(1), 1, 9),
        ev(2, Op::Read, Ret::Read(Some((2, 1))), 2, 8),
        ev(3, Op::Read, Ret::Read(Some((3, 2))), 11, 12),
    ];
    assert_eq!(check(&h, None, BUDGET), Ok(()));
}

#[test]
fn rejects_a_stale_read() {
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 1),
        ev(1, Op::Read, Ret::Read(None), 2, 3),
    ];
    assert!(not_linearizable(check(&h, None, BUDGET)));
}

#[test]
fn rejects_a_lost_update() {
    // Both CASes succeed from the same expected state.
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 3),
        ev(1, cas(None, 2), Ret::CasUpdated(1), 1, 2),
    ];
    assert!(not_linearizable(check(&h, None, BUDGET)));
    // Even with distinct seqs, the second cannot have expected `None`.
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 3),
        ev(1, cas(None, 2), Ret::CasUpdated(2), 1, 2),
    ];
    assert!(not_linearizable(check(&h, None, BUDGET)));
}

#[test]
fn rejects_a_seq_gap_or_repeat() {
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 1),
        ev(0, cas(Some(1), 2), Ret::CasUpdated(3), 2, 3),
    ];
    assert!(not_linearizable(check(&h, None, BUDGET)));
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 1),
        ev(0, cas(Some(1), 2), Ret::CasUpdated(1), 2, 3),
    ];
    assert!(not_linearizable(check(&h, None, BUDGET)));
}

#[test]
fn rejects_a_mismatch_that_reports_the_expected_value() {
    // The ABA case: the ref is back at 1, yet a CAS expecting 1 failed.
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 1),
        ev(0, cas(Some(1), 2), Ret::CasUpdated(2), 2, 3),
        ev(0, cas(Some(2), 1), Ret::CasUpdated(3), 4, 5),
        ev(1, cas(Some(1), 0), Ret::CasMismatch(Some((1, 3))), 6, 7),
    ];
    assert!(not_linearizable(check(&h, None, BUDGET)));
}

#[test]
fn rejects_a_read_of_a_value_never_written() {
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 5),
        ev(1, Op::Read, Ret::Read(Some((2, 1))), 1, 4),
    ];
    assert!(not_linearizable(check(&h, None, BUDGET)));
}

#[test]
fn rejects_malformed_histories_and_reports_an_exhausted_budget() {
    let h = [ev(0, Op::Read, Ret::Read(None), 3, 3)];
    assert_eq!(check(&h, None, BUDGET), Err(Violation::MalformedHistory));
    let h = [
        ev(0, Op::Read, Ret::Read(None), 0, 2),
        ev(1, Op::Read, Ret::Read(None), 2, 4),
    ];
    assert_eq!(check(&h, None, BUDGET), Err(Violation::MalformedHistory));
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 1),
        ev(0, cas(Some(1), 2), Ret::CasUpdated(2), 2, 3),
    ];
    assert_eq!(check(&h, None, 1), Err(Violation::BudgetExceeded));
    assert_eq!(check(&h, None, BUDGET), Ok(()));
}

/// An atomic in-process register.
fn atomic_register() -> Exec {
    let state: Arc<Mutex<State>> = Arc::new(Mutex::new(None));
    Arc::new(move |op| {
        let state = state.clone();
        Box::pin(async move {
            tokio::task::yield_now().await;
            let mut s = state.lock().unwrap();
            match op {
                Op::Read => Ret::Read(*s),
                Op::Cas { expected, new } => {
                    if s.map(|(v, _)| v) == expected {
                        let seq = s.map_or(1, |(_, q)| q + 1);
                        *s = Some((new, seq));
                        Ret::CasUpdated(seq)
                    } else {
                        Ret::CasMismatch(*s)
                    }
                }
            }
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn accepts_an_atomic_register_at_64_way_concurrency() {
    let scripts = random_scripts(1, 64, 30, 3);
    let history = record(scripts, atomic_register()).await;
    assert_eq!(history.len(), 64 * 30);
    assert_eq!(max_overlap(&history), 64);
    assert_eq!(check(&history, None, 500_000_000), Ok(()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_a_register_whose_cas_is_a_read_then_a_write() {
    // Each CAS reads, waits until the other CAS has read too, then writes:
    // both see `None`, both succeed. The barrier makes the race certain.
    let state: Arc<Mutex<State>> = Arc::new(Mutex::new(None));
    let both_read = Arc::new(tokio::sync::Barrier::new(2));
    let exec: Exec = Arc::new(move |op| {
        let (state, both_read) = (state.clone(), both_read.clone());
        Box::pin(async move {
            let Op::Cas { expected, new } = op else {
                return Ret::Read(*state.lock().unwrap());
            };
            let seen = *state.lock().unwrap();
            both_read.wait().await;
            if seen.map(|(v, _)| v) != expected {
                return Ret::CasMismatch(seen);
            }
            let seq = seen.map_or(1, |(_, q)| q + 1);
            *state.lock().unwrap() = Some((new, seq));
            Ret::CasUpdated(seq)
        })
    });
    let history = record(vec![vec![cas(None, 1)], vec![cas(None, 2)]], exec).await;
    assert!(
        history.iter().all(|e| e.ret == Ret::CasUpdated(1)),
        "{history:?}"
    );
    assert!(not_linearizable(check(&history, None, BUDGET)));
}

#[test]
fn accepts_reads_and_failed_cases_that_must_wait_for_a_later_update() {
    // R1 and F are legal at the start (state None) but R2 and F must also be
    // consistent with the CAS that lands inside their windows.
    let h = [
        ev(0, Op::Read, Ret::Read(None), 0, 10),
        ev(1, cas(None, 1), Ret::CasUpdated(1), 1, 3),
        ev(2, cas(None, 0), Ret::CasMismatch(Some((1, 1))), 2, 9),
        ev(3, Op::Read, Ret::Read(Some((1, 1))), 4, 5),
        ev(1, cas(Some(1), 2), Ret::CasUpdated(2), 6, 8),
        ev(3, Op::Read, Ret::Read(Some((1, 1))), 7, 11),
    ];
    assert_eq!(check(&h, None, BUDGET), Ok(()));
    // The same, but the last read sees the state before the first CAS, which
    // ended before it began.
    let mut bad = h;
    bad[5] = ev(3, Op::Read, Ret::Read(None), 7, 11);
    assert!(not_linearizable(check(&bad, None, BUDGET)));
}

/// Brute force: tries every order consistent with real time.
fn naive(history: &[Event], state: State, done: &mut Vec<bool>) -> bool {
    if done.iter().all(|d| *d) {
        return true;
    }
    for (i, e) in history.iter().enumerate() {
        if done[i] {
            continue;
        }
        // `e` may go next only if no other pending op returned before it began.
        let minimal = history
            .iter()
            .enumerate()
            .all(|(j, f)| done[j] || j == i || f.ret_time > e.call);
        if !minimal {
            continue;
        }
        if let Some(next) = step(state, e.op, e.ret) {
            done[i] = true;
            if naive(history, next, done) {
                done[i] = false;
                return true;
            }
            done[i] = false;
        }
    }
    false
}

/// A random small history: random intervals, results from a random
/// linearization, and sometimes one corrupted result.
fn random_history(rng: &mut SplitMix64, n: usize) -> Vec<Event> {
    let mut times: Vec<u64> = (0..2 * n as u64).collect();
    for i in (1..times.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        times.swap(i, j);
    }
    let mut ops: Vec<(Op, u64, u64)> = (0..n)
        .map(|k| {
            let (a, b) = (times[2 * k], times[2 * k + 1]);
            let op = if rng.below(3) == 0 {
                Op::Read
            } else {
                let expected = match rng.below(4) {
                    0 => None,
                    v => Some(v as u8 - 1),
                };
                cas(expected, rng.below(3) as u8)
            };
            (op, a.min(b), a.max(b))
        })
        .collect();
    // Linearization points: a random point inside each interval, applied in
    // order, give a linearizable history.
    let mut points: Vec<(u64, usize)> = ops
        .iter()
        .enumerate()
        .map(|(k, (_, c, r))| (c * 1000 + 1 + rng.below((r - c) * 1000 - 1), k))
        .collect();
    points.sort_unstable();
    let mut state: State = None;
    let mut rets = vec![Ret::Read(None); n];
    for (_, k) in points {
        let (op, _, _) = ops[k];
        rets[k] = match op {
            Op::Read => Ret::Read(state),
            Op::Cas { expected, new } => {
                if state.map(|(v, _)| v) == expected {
                    let seq = state.map_or(1, |(_, s)| s + 1);
                    state = Some((new, seq));
                    Ret::CasUpdated(seq)
                } else {
                    Ret::CasMismatch(state)
                }
            }
        };
    }
    if rng.below(2) == 0 {
        let k = rng.below(n as u64) as usize;
        let s = |rng: &mut SplitMix64| match rng.below(4) {
            0 => None,
            v => Some((rng.below(3) as u8, v)),
        };
        rets[k] = match rng.below(3) {
            0 => Ret::Read(s(rng)),
            1 => Ret::CasUpdated(1 + rng.below(3)),
            _ => Ret::CasMismatch(s(rng)),
        };
    }
    ops.iter_mut()
        .zip(rets)
        .enumerate()
        .map(|(k, ((op, c, r), ret))| ev(k, *op, ret, *c, *r))
        .collect()
}

#[test]
fn agrees_with_brute_force_on_random_small_histories() {
    let mut rng = SplitMix64(0x11ea_0001);
    let (mut yes, mut no) = (0, 0);
    for round in 0..20_000 {
        let n = 1 + rng.below(8) as usize;
        let h = random_history(&mut rng, n);
        let want = naive(&h, None, &mut vec![false; n]);
        let got = check(&h, None, BUDGET);
        assert!(
            got == Ok(()) && want || not_linearizable(got.clone()) && !want,
            "round {round}: checker {got:?}, brute force {want}: {h:?}"
        );
        if want {
            yes += 1;
        } else {
            no += 1;
        }
    }
    // Both outcomes are well represented.
    assert!(yes > 5_000 && no > 5_000, "yes {yes}, no {no}");
}

#[test]
fn budget_is_a_strict_step_limit() {
    let h = [
        ev(0, cas(None, 1), Ret::CasUpdated(1), 0, 5),
        ev(1, cas(Some(1), 2), Ret::CasUpdated(2), 1, 6),
        ev(2, Op::Read, Ret::Read(Some((2, 2))), 2, 7),
        ev(3, cas(Some(0), 2), Ret::CasMismatch(Some((1, 1))), 3, 8),
    ];
    let min = (1..1000)
        .find(|b| check(&h, None, *b) == Ok(()))
        .expect("the history is linearizable");
    assert!(min > 1);
    assert_eq!(check(&h, None, min - 1), Err(Violation::BudgetExceeded));
    assert_eq!(check(&h, None, min), Ok(()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_runs_scripts_of_different_lengths_all_in_flight_at_the_start() {
    // Instant operations: each first response is taken only once every
    // client has issued its first request.
    let exec: Exec = Arc::new(|_| Box::pin(async { Ret::Read(None) }));
    let scripts = vec![vec![Op::Read], vec![Op::Read; 3], vec![Op::Read; 2]];
    let history = tokio::time::timeout(std::time::Duration::from_secs(10), record(scripts, exec))
        .await
        .expect("recording hung");
    assert_eq!(history.len(), 6);
    let firsts: Vec<Event> = (0..3)
        .map(|c| {
            *history
                .iter()
                .filter(|e| e.client == c)
                .min_by_key(|e| e.call)
                .unwrap()
        })
        .collect();
    assert_eq!(max_overlap(&firsts), 3);
}

#[test]
fn random_scripts_mix_reads_cases_and_creations() {
    let scripts = random_scripts(5, 64, 30, 3);
    assert_eq!(scripts.len(), 64);
    let ops: Vec<Op> = scripts.into_iter().flatten().collect();
    assert_eq!(ops.len(), 64 * 30);
    let reads = ops.iter().filter(|o| **o == Op::Read).count();
    let creates = ops
        .iter()
        .filter(|o| matches!(o, Op::Cas { expected: None, .. }))
        .count();
    let cases = ops.len() - reads;
    // About a third reads; about one CAS in sixteen expects absence.
    assert!((500..800).contains(&reads), "reads {reads}");
    assert!(
        (30..160).contains(&creates),
        "creations {creates} of {cases}"
    );
    assert!(ops.iter().all(|o| match o {
        Op::Read => true,
        Op::Cas { expected, new } => *new < 3 && expected.is_none_or(|e| e < 3),
    }));
}

#[test]
fn a_one_step_search_fits_a_budget_of_one() {
    // One read, linearized by the first step's observer sweep.
    let h = [ev(0, Op::Read, Ret::Read(None), 0, 1)];
    assert_eq!(check(&h, None, 1), Ok(()));
    assert_eq!(check(&h, None, 0), Err(Violation::BudgetExceeded));
}

/// Each call is stamped as its request is issued, with no wait in between:
/// on one thread, ordering the history by call stamp orders it by the
/// moment the store received each request.
#[tokio::test(flavor = "current_thread")]
async fn record_stamps_each_call_as_its_request_is_issued() {
    let issued = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let exec: Exec = {
        let issued = issued.clone();
        Arc::new(move |_| {
            let issued = issued.clone();
            Box::pin(async move {
                let n = issued.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ret::Read(Some((0, n)))
            })
        })
    };
    let mut history = record(random_scripts(3, 8, 5, 3), exec).await;
    history.sort_by_key(|e| e.call);
    let order: Vec<u64> = history
        .iter()
        .map(|e| match e.ret {
            Ret::Read(Some((_, n))) => n,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(order, (0..40).collect::<Vec<_>>());
    assert_eq!(max_overlap(&history), 8);
}
