//! Mutual exclusion per key on one server: runs for one key take turns, in
//! arrival order, and runs for different keys proceed together.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};

/// Runs futures one at a time per key. A key's entry lives only while some
/// caller runs or waits on it: it is removed when the last one leaves,
/// whether that caller finished, failed, or was cancelled.
pub(crate) struct Serial<K> {
    locks: Mutex<HashMap<K, Arc<tokio::sync::Mutex<()>>>>,
}

impl<K> Default for Serial<K> {
    fn default() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }
}

/// One caller's hold on a key's entry. References to an entry are cloned
/// and dropped only under the table's lock, so the count the last holder
/// sees there is exact.
struct Hold<'a, K: Eq + Hash> {
    table: &'a Serial<K>,
    key: K,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl<K: Eq + Hash> Drop for Hold<'_, K> {
    fn drop(&mut self) {
        let mut locks = self
            .table
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        drop(self.lock.take());
        if locks
            .get(&self.key)
            .is_some_and(|l| Arc::strong_count(l) == 1)
        {
            locks.remove(&self.key);
        }
    }
}

impl<K: Eq + Hash + Clone> Serial<K> {
    /// Runs `f` once no earlier run for `key` is running.
    pub(crate) async fn run<T>(&self, key: K, f: impl Future<Output = T>) -> T {
        let lock = self
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key.clone())
            .or_default()
            .clone();
        let hold = Hold {
            table: self,
            key,
            lock: Some(lock),
        };
        let _turn = hold
            .lock
            .as_ref()
            .expect("set above, taken only on drop")
            .lock()
            .await;
        f.await
    }

    /// How many keys have a caller running or waiting.
    #[cfg(test)]
    fn in_use(&self) -> usize {
        self.locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Runs of one key never overlap, runs of two keys do, and nothing is
    /// left behind.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn runs_of_one_key_take_turns() {
        let table: Arc<Serial<u8>> = Arc::new(Serial::default());
        let now = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let max = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let start = Arc::new(tokio::sync::Barrier::new(64));
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..64usize {
            let (table, now, max, start) = (table.clone(), now.clone(), max.clone(), start.clone());
            tasks.spawn(async move {
                start.wait().await;
                let k = i % 2;
                table
                    .run(k as u8, async {
                        let n = now[k].fetch_add(1, Ordering::SeqCst) + 1;
                        max[k].fetch_max(n, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        now[k].fetch_sub(1, Ordering::SeqCst);
                    })
                    .await;
            });
        }
        while let Some(r) = tasks.join_next().await {
            r.unwrap();
        }
        assert_eq!(max[0].load(Ordering::SeqCst), 1);
        assert_eq!(max[1].load(Ordering::SeqCst), 1);
        assert_eq!(table.in_use(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_waiter_leaves_nothing_behind() {
        let table: Serial<u8> = Serial::default();
        let (release, held) = tokio::sync::oneshot::channel::<()>();
        let first = table.run(1, async {
            held.await.unwrap();
        });
        tokio::pin!(first);
        // The first run takes the key and waits; a second waits behind it
        // and is cancelled.
        assert!(futures_poll(first.as_mut()).is_pending());
        let mut second = Box::pin(table.run(1, async {}));
        assert!(futures_poll(second.as_mut()).is_pending());
        drop(second);
        assert_eq!(table.in_use(), 1);
        release.send(()).unwrap();
        first.await;
        assert_eq!(table.in_use(), 0);
    }

    /// Polls a future once.
    fn futures_poll<F: Future>(f: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        f.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }
}
