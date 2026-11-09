//! Coalescing of identical concurrent computations on one server.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tokio::sync::OnceCell;

/// Concurrent requests for one key share one computation and its result. A
/// key's entry lives only while some caller holds it: it is removed when
/// the last caller leaves, whether that caller finished, failed, or was
/// cancelled, so the table holds only keys in flight.
pub struct Coalescer<K, V> {
    cells: Mutex<HashMap<K, Arc<OnceCell<V>>>>,
}

impl<K, V> Default for Coalescer<K, V> {
    fn default() -> Self {
        Self {
            cells: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V> std::fmt::Debug for Coalescer<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coalescer").finish_non_exhaustive()
    }
}

/// One caller's hold on a key's entry. References to an entry are cloned
/// and dropped only under the table's lock, so the count the last holder
/// sees there is exact.
struct Hold<'a, K: Eq + Hash + Clone, V> {
    table: &'a Coalescer<K, V>,
    key: K,
    cell: Option<Arc<OnceCell<V>>>,
}

impl<K: Eq + Hash + Clone, V> Drop for Hold<'_, K, V> {
    fn drop(&mut self) {
        let mut cells = self
            .table
            .cells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(self.cell.take());
        // Only the table's reference is left: no caller is computing or
        // waiting.
        if cells
            .get(&self.key)
            .is_some_and(|c| Arc::strong_count(c) == 1)
        {
            cells.remove(&self.key);
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> Coalescer<K, V> {
    /// The result of `compute` for `key`, computed once for all concurrent
    /// callers. A failed computation is retried by the next caller waiting.
    pub async fn run<E, F, Fut>(&self, key: K, compute: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        let mut hold = Hold {
            table: self,
            key: key.clone(),
            cell: None,
        };
        hold.cell = Some(
            self.cells
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(key)
                .or_default()
                .clone(),
        );
        let cell = hold.cell.as_ref().expect("set above, taken only on drop");
        cell.get_or_try_init(compute).await.cloned()
    }

    /// How many keys are in flight.
    pub fn in_flight(&self) -> usize {
        self.cells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_callers_share_one_computation() {
        let table: Coalescer<u8, u32> = Coalescer::default();
        let runs = std::sync::atomic::AtomicU32::new(0);
        let gate = tokio::sync::Notify::new();
        let call = || {
            table.run(1, || async {
                runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                gate.notified().await;
                Ok::<_, ()>(7)
            })
        };
        let (a, b, ()) = tokio::join!(call(), call(), async {
            tokio::task::yield_now().await;
            gate.notify_one();
        });
        assert_eq!((a, b), (Ok(7), Ok(7)));
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(table.in_flight(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_caller_leaves_nothing_behind() {
        let table: Coalescer<u8, u32> = Coalescer::default();
        let never = table.run(1, std::future::pending::<Result<u32, ()>>);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), never)
                .await
                .is_err()
        );
        assert_eq!(table.in_flight(), 0);
        // A failed computation leaves nothing behind either.
        assert_eq!(table.run(2, || async { Err::<u32, _>(()) }).await, Err(()));
        assert_eq!(table.in_flight(), 0);
    }
}

#[cfg(test)]
mod race_tests {
    use super::*;

    /// Many callers on many threads, finishing together, leave no entry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn callers_finishing_together_leave_nothing_behind() {
        let table: Arc<Coalescer<u8, u32>> = Arc::new(Coalescer::default());
        for round in 0..5000u32 {
            let gate = Arc::new(tokio::sync::Barrier::new(16));
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..16 {
                let (table, gate) = (table.clone(), gate.clone());
                tasks.spawn(async move {
                    gate.wait().await;
                    table
                        .run((round % 4) as u8, || async { Ok::<_, ()>(7) })
                        .await
                });
            }
            while let Some(r) = tasks.join_next().await {
                assert_eq!(r.unwrap(), Ok(7));
            }
            assert_eq!(table.in_flight(), 0, "round {round}");
        }
    }
}
