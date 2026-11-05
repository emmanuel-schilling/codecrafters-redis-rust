use crate::app::protocol::RespValue;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};
use tokio::sync::mpsc;
use tracing::{debug, warn};

type ClientId = u64;

/// Most messages queued for one subscriber. A subscriber that falls further
/// behind is disconnected rather than silently missing messages, as Redis
/// disconnects clients that exceed its pub/sub output buffer limit.
pub const SUBSCRIBER_QUEUE_LEN: usize = 1024;

#[derive(Debug, Default)]
struct Subscriptions {
    /// The only sender for each subscribed client's queue. Dropping it ends the
    /// client's subscribed mode once its queued messages are delivered.
    clients: HashMap<ClientId, mpsc::Sender<RespValue>>,
    /// The clients subscribed to each channel.
    channels: HashMap<Bytes, HashSet<ClientId>>,
}

impl Subscriptions {
    fn remove_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
        self.channels.retain(|_, subscribers| {
            subscribers.remove(&client_id);
            !subscribers.is_empty()
        });
    }
}

#[derive(Debug, Default)]
pub struct PubSubHub {
    inner: Mutex<Subscriptions>,
}

impl PubSubHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Locks the subscriptions, recovering them if another task panicked while holding them.
    fn inner(&self) -> MutexGuard<'_, Subscriptions> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts a client's message queue and returns its receiving end. The
    /// queue closes when the client is removed.
    pub fn register(&self, client_id: ClientId) -> mpsc::Receiver<RespValue> {
        let (sender, receiver) = mpsc::channel(SUBSCRIBER_QUEUE_LEN);
        self.inner().clients.insert(client_id, sender);
        receiver
    }

    /// Subscribes a registered client to a channel.
    pub fn subscribe(&self, channel: Bytes, client_id: ClientId) {
        self.inner()
            .channels
            .entry(channel)
            .or_default()
            .insert(client_id);
    }

    /// Unsubscribes a client from a specific channel.
    pub fn unsubscribe(&self, channel: &Bytes, client_id: ClientId) {
        let mut inner = self.inner();
        if let Some(subscribers) = inner.channels.get_mut(channel) {
            subscribers.remove(&client_id);
            if subscribers.is_empty() {
                inner.channels.remove(channel);
            }
        }
    }

    /// Removes a client from every channel and closes its queue.
    pub fn remove_client(&self, client_id: ClientId) {
        self.inner().remove_client(client_id);
    }

    /// Publishes a message to all subscribers of a channel and returns how many
    /// received it. A subscriber whose queue is full is disconnected.
    pub fn publish(&self, channel: Bytes, message: Bytes) -> usize {
        let mut inner = self.inner();
        let Subscriptions { clients, channels } = &mut *inner;
        let Some(subscribers) = channels.get(&channel) else {
            return 0;
        };

        let message_payload = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"message")),
            RespValue::BulkString(channel),
            RespValue::BulkString(message),
        ]);

        let mut delivered = 0;
        let mut too_slow = Vec::new();
        for client_id in subscribers {
            let sent = clients
                .get(client_id)
                .map(|sender| sender.try_send(message_payload.clone()));
            match sent {
                Some(Ok(())) => delivered += 1,
                // A full queue means the client fell too far behind; a closed
                // one means it has gone. Either way it stops receiving.
                _ => too_slow.push(*client_id),
            }
        }
        for client_id in too_slow {
            warn!("disconnecting subscriber {client_id}: its message queue is full or closed");
            inner.remove_client(client_id);
        }

        debug!("Published message to {delivered} subscribers.");
        delivered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(name: &'static str) -> Bytes {
        Bytes::from_static(name.as_bytes())
    }

    #[test]
    fn publishes_to_every_subscriber_of_the_channel() {
        let hub = PubSubHub::new();
        let mut first = hub.register(1);
        let mut second = hub.register(2);
        hub.subscribe(channel("a"), 1);
        hub.subscribe(channel("a"), 2);
        hub.subscribe(channel("b"), 2);

        assert_eq!(hub.publish(channel("a"), channel("hi")), 2);
        assert_eq!(hub.publish(channel("c"), channel("hi")), 0);
        assert!(first.try_recv().is_ok());
        assert!(second.try_recv().is_ok());
    }

    #[test]
    fn a_subscriber_whose_queue_is_full_is_disconnected() {
        let hub = PubSubHub::new();
        let mut slow = hub.register(1);
        let mut fast = hub.register(2);
        hub.subscribe(channel("a"), 1);
        hub.subscribe(channel("a"), 2);

        for _ in 0..SUBSCRIBER_QUEUE_LEN {
            assert_eq!(hub.publish(channel("a"), channel("m")), 2);
            assert!(fast.try_recv().is_ok());
        }
        // The slow client's queue is full: it misses this message and is removed.
        assert_eq!(hub.publish(channel("a"), channel("m")), 1);
        assert_eq!(hub.publish(channel("a"), channel("m")), 1);

        // It still gets what was queued, then its queue reports closed.
        for _ in 0..SUBSCRIBER_QUEUE_LEN {
            assert!(slow.try_recv().is_ok());
        }
        assert_eq!(
            slow.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[test]
    fn removing_a_client_unsubscribes_it_everywhere() {
        let hub = PubSubHub::new();
        let mut receiver = hub.register(1);
        hub.subscribe(channel("a"), 1);
        hub.subscribe(channel("b"), 1);
        hub.remove_client(1);

        assert_eq!(hub.publish(channel("a"), channel("m")), 0);
        assert!(hub.inner().channels.is_empty());
        assert_eq!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        );
    }
}
