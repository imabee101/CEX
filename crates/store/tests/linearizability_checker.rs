//! The linearizability checker accepts linearizable histories and rejects
//! every kind of violation a ref CAS could commit, including a real lost
//! update produced by a non-atomic register.

use std::sync::{Arc, Mutex};

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
