use crate::app::error::AppError;
use crate::app::sorted_set::SortedSet;
use crate::app::stream::Stream;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

#[derive(Debug)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(Stream),
    SortedSet(SortedSet),
}

impl DataType {
    pub fn as_string(&mut self) -> Option<&mut Bytes> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list(&mut self) -> Option<&mut VecDeque<Bytes>> {
        match self {
            Self::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn as_stream(&mut self) -> Option<&mut Stream> {
        match self {
            Self::Stream(stream) => Some(stream),
            _ => None,
        }
    }

    pub fn as_sorted_set(&mut self) -> Option<&mut SortedSet> {
        match self {
            Self::SortedSet(zset) => Some(zset),
            _ => None,
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::List(_) => "list",
            Self::Stream(_) => "stream",
            Self::SortedSet(_) => "zset",
        }
    }

    /// Whether this is a list or sorted set with no elements left; Redis deletes those.
    fn is_empty_collection(&self) -> bool {
        match self {
            Self::List(list) => list.is_empty(),
            Self::SortedSet(zset) => zset.cardinality() == 0,
            Self::String(_) | Self::Stream(_) => false,
        }
    }
}

#[derive(Debug)]
pub struct StoreValue {
    pub data: DataType,
    pub expires_at: Option<Instant>,
}

impl StoreValue {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|t| now > t)
    }
}

/// The keyspace behind a single lock, so each command sees and changes it
/// without interference from other clients, as in Redis.
#[derive(Debug, Default)]
pub struct Store {
    keyspace: Mutex<Keyspace>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_from_rdb(&self, rdb_store: HashMap<Bytes, StoreValue>) {
        self.with_keyspace(|keyspace| keyspace.map.extend(rdb_store));
    }

    /// Runs `f` with exclusive access to the keyspace. `f` is synchronous, so the
    /// lock is never held across an `.await`.
    pub fn with_keyspace<R>(&self, f: impl FnOnce(&mut Keyspace) -> R) -> R {
        // If a command panicked while holding the lock, keep serving the
        // keyspace as it was left rather than failing every later command.
        let mut keyspace = self.keyspace.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut keyspace)
    }
}

/// The keys and values. Every lookup treats an expired key as missing and deletes it.
#[derive(Debug, Default)]
pub struct Keyspace {
    map: HashMap<Bytes, StoreValue>,
}

impl Keyspace {
    /// The value at `key`, or `None` if it is missing or has expired.
    pub fn get(&mut self, key: &Bytes) -> Option<&mut DataType> {
        self.evict_if_expired(key);
        self.map.get_mut(key).map(|value| &mut value.data)
    }

    /// The value at `key` as the type `view` selects: `Ok(None)` if the key is
    /// missing or expired, WRONGTYPE if it holds another type.
    pub fn get_as<T>(
        &mut self,
        key: &Bytes,
        view: fn(&mut DataType) -> Option<&mut T>,
    ) -> Result<Option<&mut T>, AppError> {
        self.get(key)
            .map(|data| view(data).ok_or(AppError::WrongType))
            .transpose()
    }

    /// Like [`Keyspace::get_as`], but first stores `empty()` (with no expiry)
    /// when the key is missing or expired.
    pub fn get_or_create_as<T>(
        &mut self,
        key: &Bytes,
        view: fn(&mut DataType) -> Option<&mut T>,
        empty: impl FnOnce() -> DataType,
    ) -> Result<&mut T, AppError> {
        self.evict_if_expired(key);
        let value = self.map.entry(key.clone()).or_insert_with(|| StoreValue {
            data: empty(),
            expires_at: None,
        });
        view(&mut value.data).ok_or(AppError::WrongType)
    }

    /// Stores `data` at `key`, replacing any existing value of any type.
    pub fn set(&mut self, key: Bytes, data: DataType, expires_at: Option<Instant>) {
        self.map.insert(key, StoreValue { data, expires_at });
    }

    /// Deletes `key`, whatever it holds.
    pub fn remove(&mut self, key: &Bytes) {
        self.map.remove(key);
    }

    /// Deletes `key` if it holds a list or sorted set with no elements left.
    pub fn remove_if_empty(&mut self, key: &Bytes) {
        if self
            .map
            .get(key)
            .is_some_and(|value| value.data.is_empty_collection())
        {
            self.map.remove(key);
        }
    }

    /// All keys that haven't expired.
    pub fn keys(&self) -> Vec<Bytes> {
        let now = Instant::now();
        self.map
            .iter()
            .filter(|(_, value)| !value.is_expired(now))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// The type name TYPE reports for `key`, or "none" if it is missing.
    pub fn type_name(&mut self, key: &Bytes) -> &'static str {
        self.get(key).map_or("none", |data| data.type_name())
    }

    fn evict_if_expired(&mut self, key: &Bytes) {
        if self
            .map
            .get(key)
            .is_some_and(|value| value.is_expired(Instant::now()))
        {
            self.map.remove(key);
        }
    }
}

/// Resolves Redis-style inclusive `start` and `stop` indexes, where negative
/// values count back from the end, against `len` elements. Returns the
/// inclusive index range, or `None` when it selects nothing.
pub fn resolve_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = i64::try_from(len).ok()?;
    let start = if start < 0 {
        (start + len).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        stop + len
    } else {
        stop.min(len - 1)
    };
    if start > stop || start >= len {
        return None;
    }
    Some((usize::try_from(start).ok()?, usize::try_from(stop).ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn key(k: &'static str) -> Bytes {
        Bytes::from_static(k.as_bytes())
    }

    fn string(s: &'static str) -> DataType {
        DataType::String(key(s))
    }

    fn past() -> Option<Instant> {
        Instant::now().checked_sub(Duration::from_secs(1))
    }

    #[test]
    fn expired_keys_are_missing_and_deleted_on_lookup() {
        let mut ks = Keyspace::default();
        ks.set(key("old"), string("v"), past());
        ks.set(key("live"), string("v"), None);
        assert_eq!(ks.keys(), vec![key("live")]);
        assert!(ks.get(&key("old")).is_none());
        assert!(!ks.map.contains_key(&key("old")));
        assert_eq!(ks.type_name(&key("live")), "string");
        assert_eq!(ks.type_name(&key("old")), "none");
    }

    #[test]
    fn typed_access_reports_wrongtype_for_live_keys_only() {
        let mut ks = Keyspace::default();
        ks.set(key("s"), string("v"), None);
        assert!(matches!(
            ks.get_as(&key("s"), DataType::as_list),
            Err(AppError::WrongType)
        ));
        assert!(matches!(
            ks.get_as(&key("missing"), DataType::as_list),
            Ok(None)
        ));

        ks.set(key("e"), string("v"), past());
        let list = ks
            .get_or_create_as(&key("e"), DataType::as_list, || {
                DataType::List(VecDeque::new())
            })
            .expect("expired key replaced");
        list.push_back(key("x"));
        assert_eq!(ks.type_name(&key("e")), "list");
    }

    #[test]
    fn remove_if_empty_deletes_only_empty_collections() {
        let mut ks = Keyspace::default();
        ks.set(key("l"), DataType::List(VecDeque::new()), None);
        ks.set(key("z"), DataType::SortedSet(SortedSet::new()), None);
        ks.set(key("s"), string(""), None);
        ks.set(
            key("full"),
            DataType::List(VecDeque::from([key("x")])),
            None,
        );
        for k in ["l", "z", "s", "full"] {
            ks.remove_if_empty(&key(k));
        }
        let mut keys = ks.keys();
        keys.sort();
        assert_eq!(keys, vec![key("full"), key("s")]);
    }

    #[test]
    fn resolve_range_matches_redis_index_rules() {
        let cases = [
            ((0, -1, 3), Some((0, 2))),
            ((-2, -1, 3), Some((1, 2))),
            ((1, 100, 3), Some((1, 2))),
            ((-100, 0, 3), Some((0, 0))),
            ((0, -100, 3), None),
            ((3, 5, 3), None),
            ((2, 1, 3), None),
            ((0, -1, 0), None),
            ((i64::MIN, i64::MAX, 3), Some((0, 2))),
        ];
        for ((start, stop, len), expected) in cases {
            assert_eq!(
                resolve_range(start, stop, len),
                expected,
                "{start} {stop} {len}"
            );
        }
    }
}
