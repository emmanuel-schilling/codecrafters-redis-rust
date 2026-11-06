//! Keys that clients WATCH for optimistic locking: a client's EXEC aborts if
//! any key it watches was written after it started watching it.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

type ClientId = u64;

#[derive(Debug, Default)]
struct Watches {
    /// The clients watching each key.
    watchers: HashMap<Bytes, HashSet<ClientId>>,
    /// The keys each client watches.
    watched: HashMap<ClientId, HashSet<Bytes>>,
    /// Clients with a watched key that was written since they watched it.
    dirty: HashSet<ClientId>,
}

#[derive(Debug, Default)]
pub struct WatchRegistry {
    inner: Mutex<Watches>,
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Locks the watches, recovering them if another task panicked while holding them.
    fn inner(&self) -> MutexGuard<'_, Watches> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts watching `keys` for a client, whether or not they exist.
    pub fn watch(&self, client_id: ClientId, keys: &[Bytes]) {
        let mut inner = self.inner();
        for key in keys {
            inner
                .watchers
                .entry(key.clone())
                .or_default()
                .insert(client_id);
            inner
                .watched
                .entry(client_id)
                .or_default()
                .insert(key.clone());
        }
    }

    /// Records a write to `key`: every client watching it will fail its EXEC.
    pub fn touch(&self, key: &Bytes) {
        let mut inner = self.inner();
        let Watches {
            watchers, dirty, ..
        } = &mut *inner;
        if let Some(clients) = watchers.get(key) {
            dirty.extend(clients);
        }
    }

    /// Stops watching all of a client's keys, and returns whether any of them
    /// was written while it watched them.
    pub fn unwatch(&self, client_id: ClientId) -> bool {
        let mut inner = self.inner();
        for key in inner.watched.remove(&client_id).unwrap_or_default() {
            if let Some(clients) = inner.watchers.get_mut(&key) {
                clients.remove(&client_id);
                if clients.is_empty() {
                    inner.watchers.remove(&key);
                }
            }
        }
        inner.dirty.remove(&client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(names: &[&'static str]) -> Vec<Bytes> {
        names
            .iter()
            .map(|n| Bytes::from_static(n.as_bytes()))
            .collect()
    }

    #[test]
    fn a_write_to_any_watched_key_marks_only_its_watchers() {
        let registry = WatchRegistry::new();
        registry.watch(1, &keys(&["foo", "bar"]));
        registry.watch(2, &keys(&["foo"]));
        registry.watch(3, &keys(&["baz"]));
        registry.touch(&Bytes::from_static(b"bar"));
        registry.touch(&Bytes::from_static(b"unwatched"));

        assert!(registry.unwatch(1));
        assert!(!registry.unwatch(2));
        assert!(!registry.unwatch(3));
    }

    #[test]
    fn unwatching_clears_the_keys_and_the_dirty_flag() {
        let registry = WatchRegistry::new();
        registry.watch(1, &keys(&["foo"]));
        registry.touch(&Bytes::from_static(b"foo"));
        assert!(registry.unwatch(1));
        assert!(!registry.unwatch(1));

        // Writes after unwatching no longer count.
        registry.touch(&Bytes::from_static(b"foo"));
        assert!(!registry.unwatch(1));
        let inner = registry.inner();
        assert!(inner.watchers.is_empty() && inner.watched.is_empty());
    }
}
