use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{oneshot, Notify};

/// An element handed to a blocked BLPOP client: the key it came from and the value.
pub type Popped = (Bytes, Bytes);

/// Clients blocked on keys: BLPOP clients on lists, XREAD BLOCK clients on streams.
#[derive(Debug, Default)]
pub struct WaiterRegistry {
    /// BLPOP clients per list key, oldest first.
    lists: Mutex<HashMap<Bytes, VecDeque<Arc<ListWaiter>>>>,
    /// XREAD BLOCK clients per stream key.
    streams: Mutex<HashMap<Bytes, Vec<Arc<Notify>>>>,
}

/// A BLPOP client's delivery slot. It is emptied exactly once: either an
/// element is handed over through it, or the client gives up.
#[derive(Debug)]
pub struct ListWaiter {
    slot: Mutex<Option<oneshot::Sender<Popped>>>,
}

impl ListWaiter {
    /// Withdraws the client, unless an element was already handed over.
    /// Returns whether the client was withdrawn.
    pub fn cancel(&self) -> bool {
        lock(&self.slot).take().is_some()
    }
}

/// A BLPOP client's registration. Dropping it removes the client from every key.
pub struct ListWait<'a> {
    registry: &'a WaiterRegistry,
    keys: Vec<Bytes>,
    pub waiter: Arc<ListWaiter>,
    pub delivery: oneshot::Receiver<Popped>,
}

impl Drop for ListWait<'_> {
    fn drop(&mut self) {
        let mut lists = lock(&self.registry.lists);
        for key in &self.keys {
            if let Some(queue) = lists.get_mut(key) {
                queue.retain(|w| !Arc::ptr_eq(w, &self.waiter));
                if queue.is_empty() {
                    lists.remove(key);
                }
            }
        }
    }
}

/// An XREAD BLOCK client's registration. Dropping it removes the client from every key.
pub struct StreamWatch<'a> {
    registry: &'a WaiterRegistry,
    keys: Vec<Bytes>,
    /// Signalled by every XADD to one of the keys; a signal sent before the
    /// client waits is kept until it does.
    pub changed: Arc<Notify>,
}

impl Drop for StreamWatch<'_> {
    fn drop(&mut self) {
        let mut streams = lock(&self.registry.streams);
        for key in &self.keys {
            if let Some(watchers) = streams.get_mut(key) {
                watchers.retain(|n| !Arc::ptr_eq(n, &self.changed));
                if watchers.is_empty() {
                    streams.remove(key);
                }
            }
        }
    }
}

/// Locks `mutex`, recovering the data if another task panicked while holding it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl WaiterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a BLPOP client on `keys`, behind any clients already blocked on them.
    pub fn block_on_lists(&self, keys: &[Bytes]) -> ListWait<'_> {
        let (sender, delivery) = oneshot::channel();
        let waiter = Arc::new(ListWaiter {
            slot: Mutex::new(Some(sender)),
        });
        let mut lists = lock(&self.lists);
        for key in keys {
            lists
                .entry(key.clone())
                .or_default()
                .push_back(Arc::clone(&waiter));
        }
        ListWait {
            registry: self,
            keys: keys.to_vec(),
            waiter,
            delivery,
        }
    }

    /// Hands elements from the front of `list` to clients blocked on `key`,
    /// oldest client first, until either runs out. Returns how many were handed over.
    pub fn serve_list(&self, key: &Bytes, list: &mut VecDeque<Bytes>) -> usize {
        let mut lists = lock(&self.lists);
        let Some(queue) = lists.get_mut(key) else {
            return 0;
        };
        let mut served = 0;
        while !list.is_empty() {
            let Some(waiter) = queue.pop_front() else {
                break;
            };
            // A client that gave up, or was already served from another key, is skipped.
            let Some(sender) = lock(&waiter.slot).take() else {
                continue;
            };
            let Some(value) = list.pop_front() else {
                break;
            };
            match sender.send((key.clone(), value)) {
                Ok(()) => served += 1,
                // The client's task is gone; keep the element for the next one.
                Err((_, value)) => list.push_front(value),
            }
        }
        if queue.is_empty() {
            lists.remove(key);
        }
        served
    }

    /// Registers an XREAD BLOCK client on `keys`.
    pub fn watch_streams(&self, keys: &[Bytes]) -> StreamWatch<'_> {
        let changed = Arc::new(Notify::new());
        let mut streams = lock(&self.streams);
        for key in keys {
            streams
                .entry(key.clone())
                .or_default()
                .push(Arc::clone(&changed));
        }
        StreamWatch {
            registry: self,
            keys: keys.to_vec(),
            changed,
        }
    }

    /// Wakes every XREAD BLOCK client watching `key`.
    pub fn stream_changed(&self, key: &Bytes) {
        if let Some(watchers) = lock(&self.streams).get(key) {
            for changed in watchers {
                changed.notify_one();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn key(k: &'static str) -> Bytes {
        Bytes::from_static(k.as_bytes())
    }

    fn list(items: &[&'static str]) -> VecDeque<Bytes> {
        items.iter().map(|s| key(s)).collect()
    }

    #[test]
    fn serves_blocked_clients_oldest_first() {
        let registry = WaiterRegistry::new();
        let mut first = registry.block_on_lists(&[key("q")]);
        let mut second = registry.block_on_lists(&[key("q")]);
        let mut items = list(&["x", "y", "z"]);

        assert_eq!(registry.serve_list(&key("q"), &mut items), 2);
        assert_eq!(first.delivery.try_recv().ok(), Some((key("q"), key("x"))));
        assert_eq!(second.delivery.try_recv().ok(), Some((key("q"), key("y"))));
        assert_eq!(items, list(&["z"]));
    }

    #[test]
    fn skips_withdrawn_and_already_served_clients() {
        let registry = WaiterRegistry::new();
        let withdrawn = registry.block_on_lists(&[key("a")]);
        assert!(withdrawn.waiter.cancel());
        let mut both = registry.block_on_lists(&[key("a"), key("b")]);

        let mut a = list(&["x"]);
        assert_eq!(registry.serve_list(&key("a"), &mut a), 1);
        assert_eq!(both.delivery.try_recv().ok(), Some((key("a"), key("x"))));
        assert!(!both.waiter.cancel(), "already served");

        let mut b = list(&["y"]);
        assert_eq!(registry.serve_list(&key("b"), &mut b), 0);
        assert_eq!(b, list(&["y"]));
    }

    #[test]
    fn dropping_a_registration_removes_it_from_every_key() {
        let registry = WaiterRegistry::new();
        drop(registry.block_on_lists(&[key("a"), key("b")]));
        drop(registry.watch_streams(&[key("s")]));
        assert!(lock(&registry.lists).is_empty());
        assert!(lock(&registry.streams).is_empty());
    }

    #[test]
    fn a_stream_change_before_the_wait_is_not_lost() {
        let registry = WaiterRegistry::new();
        let first = registry.watch_streams(&[key("s")]);
        let second = registry.watch_streams(&[key("s"), key("t")]);
        registry.stream_changed(&key("s"));
        assert!(first.changed.notified().now_or_never().is_some());
        assert!(second.changed.notified().now_or_never().is_some());
        assert!(first.changed.notified().now_or_never().is_none());
    }
}
