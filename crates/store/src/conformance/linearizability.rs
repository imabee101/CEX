//! A linearizability checker for one ref (a register with compare-and-swap),
//! using the Wing–Gong algorithm with Lowe's memoization over recorded
//! operation histories, plus a sound partial-order reduction for operations
//! that leave the state unchanged, and a recorder that runs concurrent clients and
//! records each operation's invocation and response.
//!
//! The model is a ref state `None` or `Some((value, seq))`. A read returns the
//! state. A CAS from `expected` to `new` succeeds when the current value
//! equals `expected`, setting the state to `(new, seq + 1)` (or `(new, 1)`
//! from `None`) and returning the new seq; otherwise it fails and returns the
//! current state. Checking the seq makes the checker also reject lost, extra,
//! or out-of-order ref-log rows.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::SplitMix64;

/// A ref state in the model: absent, or a value index and its seq.
pub type State = Option<(u8, u64)>;

/// An operation on the ref.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    /// Read the ref.
    Read,
    /// Compare-and-swap from `expected` to `new`.
    Cas {
        /// The value the ref must have; `None` means absent.
        expected: Option<u8>,
        /// The new value.
        new: u8,
    },
}

/// An operation's observed result.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ret {
    /// A read's result.
    Read(State),
    /// A successful CAS and the seq it wrote.
    CasUpdated(u64),
    /// A failed CAS and the state it reported.
    CasMismatch(State),
}

/// One completed operation with its invocation and response times, from a
/// single clock shared by every client.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Event {
    /// The client that ran the operation.
    pub client: usize,
    /// The operation.
    pub op: Op,
    /// Its result.
    pub ret: Ret,
    /// The clock value when it was invoked.
    pub call: u64,
    /// The clock value when it returned; greater than `call`.
    pub ret_time: u64,
}

/// Applies one operation to the model, returning the new state if the
/// operation's observed result is possible from `state`.
pub fn step(state: State, op: Op, ret: Ret) -> Option<State> {
    match (op, ret) {
        (Op::Read, Ret::Read(seen)) => (seen == state).then_some(state),
        (Op::Cas { expected, new }, Ret::CasUpdated(seq)) => {
            let current = state.map(|(v, _)| v);
            let next_seq = state.map_or(1, |(_, s)| s + 1);
            (current == expected && seq == next_seq).then_some(Some((new, seq)))
        }
        (Op::Cas { expected, .. }, Ret::CasMismatch(seen)) => {
            (seen == state && state.map(|(v, _)| v) != expected).then_some(state)
        }
        _ => None,
    }
}

/// Why a history was not shown linearizable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Violation {
    /// No order of the operations consistent with real time is a legal
    /// sequential history.
    NotLinearizable {
        /// The most operations any explored order linearized.
        longest_prefix: usize,
    },
    /// The search exceeded its step budget; the history is unproven.
    BudgetExceeded,
    /// An event's response is not after its invocation, or two clock values
    /// are equal.
    MalformedHistory,
}

const NIL: usize = usize::MAX;
const HEAD: usize = 0;

/// Checks that `history`, starting from `init`, is linearizable, exploring
/// at most `budget` search steps.
///
/// This is the Wing–Gong search with Lowe's cache of visited
/// `(linearized set, state)` configurations, plus a partial-order reduction
/// for observers, operations that leave the state unchanged (reads and
/// failed CASes): an observer that is minimal (no pending operation returned
/// before it was invoked) and legal in the current state commutes with every
/// other choice, so if any linearization extends the current configuration,
/// one extends it with that observer next. The search therefore linearizes
/// such observers eagerly and never branches on them, which keeps it close to
/// linear on histories with many concurrent reads and failed CASes.
pub fn check(history: &[Event], init: State, budget: u64) -> Result<(), Violation> {
    let n = history.len();
    let mut endpoints: Vec<(u64, usize)> = Vec::with_capacity(2 * n);
    for (i, e) in history.iter().enumerate() {
        if e.ret_time <= e.call {
            return Err(Violation::MalformedHistory);
        }
        endpoints.push((e.call, call_node(i)));
        endpoints.push((e.ret_time, call_node(i) + 1));
    }
    endpoints.sort_unstable();
    if endpoints.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(Violation::MalformedHistory);
    }
    let mut search = Search {
        history,
        next: vec![NIL; 2 * n + 1],
        prev: vec![NIL; 2 * n + 1],
        linearized: vec![0u64; n.div_ceil(64)],
        cache: HashSet::new(),
        stack: Vec::new(),
        state: init,
        longest: 0,
    };
    let mut last = HEAD;
    for &(_, node) in &endpoints {
        search.next[last] = node;
        search.prev[node] = last;
        last = node;
    }

    let mut steps = 0u64;
    let mut entry = search.next[HEAD];
    let mut fresh = true;
    while search.next[HEAD] != NIL {
        steps += 1;
        if steps > budget {
            return Err(Violation::BudgetExceeded);
        }
        if fresh {
            fresh = false;
            if !search.sweep_observers() {
                entry = search.backtrack()?;
                continue;
            }
            entry = search.next[HEAD];
            continue;
        }
        if entry == NIL {
            // Unreachable: a pending call is always followed by its return.
            // Fail closed rather than loop.
            return Err(Violation::MalformedHistory);
        }
        let i = (entry - 1) / 2;
        if entry == call_node(i) {
            if search.try_linearize(i) {
                entry = search.next[HEAD];
                fresh = true;
            } else {
                entry = search.next[entry];
            }
        } else {
            // The return of an operation not yet linearized: backtrack.
            entry = search.backtrack()?;
        }
    }
    Ok(())
}

/// Op i's call node; its return node is the next index. Node 0 is the head.
fn call_node(i: usize) -> usize {
    1 + 2 * i
}

struct Search<'h> {
    history: &'h [Event],
    next: Vec<usize>,
    prev: Vec<usize>,
    linearized: Vec<u64>,
    cache: HashSet<(Vec<u64>, State)>,
    stack: Vec<(usize, State)>,
    state: State,
    longest: usize,
}

impl Search<'_> {
    /// Linearizes op `i` next if it is legal and the resulting configuration
    /// is new; a configuration seen before has already failed.
    fn try_linearize(&mut self, i: usize) -> bool {
        let e = &self.history[i];
        let Some(after) = step(self.state, e.op, e.ret) else {
            return false;
        };
        self.linearized[i / 64] |= 1 << (i % 64);
        if !self.cache.insert((self.linearized.clone(), after)) {
            self.linearized[i / 64] &= !(1 << (i % 64));
            return false;
        }
        self.stack.push((i, self.state));
        self.state = after;
        self.longest = self.longest.max(self.stack.len());
        for node in [call_node(i), call_node(i) + 1] {
            self.next[self.prev[node]] = self.next[node];
            if self.next[node] != NIL {
                self.prev[self.next[node]] = self.prev[node];
            }
        }
        true
    }

    /// Linearizes every minimal observer legal in the current state. Returns
    /// false if one leads to a configuration that already failed, which
    /// means the current one fails too.
    fn sweep_observers(&mut self) -> bool {
        'restart: loop {
            let mut node = self.next[HEAD];
            while node != NIL && node == call_node((node - 1) / 2) {
                let i = (node - 1) / 2;
                let e = &self.history[i];
                if step(self.state, e.op, e.ret) == Some(self.state) {
                    if !self.try_linearize(i) {
                        return false;
                    }
                    continue 'restart;
                }
                node = self.next[node];
            }
            return true;
        }
    }

    /// Undoes linearizations until one can be replaced by a later choice,
    /// returning the entry to resume scanning from.
    fn backtrack(&mut self) -> Result<usize, Violation> {
        loop {
            let Some((j, before)) = self.stack.pop() else {
                return Err(Violation::NotLinearizable {
                    longest_prefix: self.longest,
                });
            };
            let observer = before == self.state;
            self.state = before;
            self.linearized[j / 64] &= !(1 << (j % 64));
            for node in [call_node(j) + 1, call_node(j)] {
                self.next[self.prev[node]] = node;
                if self.next[node] != NIL {
                    self.prev[self.next[node]] = node;
                }
            }
            // Observers are never branched on (see `check`): if the
            // configuration after one failed, so did the one before it.
            if !observer {
                return Ok(self.next[call_node(j)]);
            }
        }
    }
}

/// The largest number of operations in flight at one time.
pub fn max_overlap(history: &[Event]) -> usize {
    let mut points: Vec<(u64, bool)> = history
        .iter()
        .flat_map(|e| [(e.call, true), (e.ret_time, false)])
        .collect();
    points.sort_unstable();
    let (mut now, mut max) = (0usize, 0usize);
    for (_, is_call) in points {
        if is_call {
            now += 1;
            max = max.max(now);
        } else {
            now -= 1;
        }
    }
    max
}

/// Executes one operation against the system under test.
pub type Exec = Arc<dyn Fn(Op) -> Pin<Box<dyn Future<Output = Ret> + Send>> + Send + Sync>;

/// Runs each script on its own task, all concurrently, and records every
/// operation. An operation's call is stamped immediately before its request
/// is issued (its future first polled) and its return immediately after its
/// response, so each interval holds the store call and nothing else. Each
/// client issues its first request, then waits until every client has issued
/// its own before it takes the response: every first request is issued
/// while all the others are outstanding, so all clients are in flight
/// together at the start, with each request already in the store wherever
/// the store has not answered it on the spot.
pub async fn record(scripts: Vec<Vec<Op>>, exec: Exec) -> Vec<Event> {
    let clock = Arc::new(AtomicU64::new(0));
    let issued = Arc::new(tokio::sync::Barrier::new(scripts.len()));
    let mut tasks = Vec::new();
    for (client, script) in scripts.into_iter().enumerate() {
        let (clock, issued, exec) = (clock.clone(), issued.clone(), exec.clone());
        tasks.push(tokio::spawn(async move {
            let mut events = Vec::with_capacity(script.len());
            for (k, op) in script.into_iter().enumerate() {
                let call = clock.fetch_add(1, Ordering::SeqCst);
                let mut request = exec(op);
                let ret = if k == 0 {
                    let first = std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(request.as_mut().poll(cx))
                    })
                    .await;
                    issued.wait().await;
                    match first {
                        std::task::Poll::Ready(ret) => ret,
                        std::task::Poll::Pending => request.await,
                    }
                } else {
                    request.await
                };
                let ret_time = clock.fetch_add(1, Ordering::SeqCst);
                events.push(Event {
                    client,
                    op,
                    ret,
                    call,
                    ret_time,
                });
            }
            events
        }));
    }
    let mut history = Vec::new();
    for task in tasks {
        history.extend(task.await.expect("a client task panicked"));
    }
    history
}

/// Random scripts over `values` distinct values: about a third reads, the
/// rest CASes whose expected value is usually one of the values.
pub fn random_scripts(seed: u64, clients: usize, ops: usize, values: u8) -> Vec<Vec<Op>> {
    let mut rng = SplitMix64(seed);
    let values = u64::from(values);
    let pick = |rng: &mut SplitMix64| {
        u8::try_from(rng.below(values)).expect("values is a u8, so every pick fits")
    };
    (0..clients)
        .map(|_| {
            (0..ops)
                .map(|_| {
                    if rng.below(3) == 0 {
                        Op::Read
                    } else {
                        let expected = if rng.below(16) == 0 {
                            None
                        } else {
                            Some(pick(&mut rng))
                        };
                        Op::Cas {
                            expected,
                            new: pick(&mut rng),
                        }
                    }
                })
                .collect()
        })
        .collect()
}
