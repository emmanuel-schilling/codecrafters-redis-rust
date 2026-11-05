use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::Notify;
use tokio::time::{self, Duration};
use tracing::debug;

/// A registry for tasks waiting for list notifications (for BLPOP).
/// It also serves as a general notification mechanism for the WAIT command.
#[derive(Debug, Default)]
pub struct WaiterRegistry {
    /// Maps a list key to a queue of `Notify` objects (for BLPOP).
    waiters: Mutex<HashMap<Bytes, VecDeque<Arc<Notify>>>>,
    /// `Notify` objects for tasks waiting on `WAIT` conditions.
    wait_command_notifiers: Mutex<VecDeque<Arc<Notify>>>,
}

/// Locks `mutex`, recovering the data if another task panicked while holding it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl WaiterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wakes up one waiting task for a given key (for BLPOP).
    pub fn notify_one(&self, key: &Bytes) {
        let mut waiters = lock(&self.waiters);
        if let Some(queue) = waiters.get_mut(key) {
            if let Some(notify) = queue.pop_front() {
                debug!("notifying BLPOP waiter for key: {:?}", key);
                notify.notify_one();
            }
            if queue.is_empty() {
                waiters.remove(key);
            }
        }
    }

    /// Blocks the current task until notified for one of the given keys or until the timeout expires (for BLPOP).
    pub async fn wait_for_any(&self, keys: &[Bytes], timeout: Option<Duration>) {
        let notify = Arc::new(Notify::new());

        {
            let mut waiters = lock(&self.waiters);
            for key in keys {
                waiters
                    .entry(key.clone())
                    .or_default()
                    .push_back(notify.clone());
            }
        }

        let notified_future = notify.notified();

        if let Some(duration) = timeout {
            let _ = time::timeout(duration, notified_future).await;
        } else {
            notified_future.await;
        }

        let mut waiters = lock(&self.waiters);
        for key in keys {
            if let Some(queue) = waiters.get_mut(key) {
                queue.retain(|n| !Arc::ptr_eq(n, &notify));
                if queue.is_empty() {
                    waiters.remove(key);
                }
            }
        }
    }

    /// Registers a `Notify` object for a `WAIT` command.
    pub async fn register_wait_notifier(&self, notify: Arc<Notify>) {
        lock(&self.wait_command_notifiers).push_back(notify);
    }

    /// Notifies all tasks waiting on a `WAIT` command (e.g., after a propagation).
    pub fn notify_all(&self) {
        // Drain and notify all current waiters.
        // New WAIT commands will register their own notify objects.
        for notify in lock(&self.wait_command_notifiers).drain(..) {
            notify.notify_one();
        }
    }
}
