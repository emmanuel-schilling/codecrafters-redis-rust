use crate::app::{
    command::{Command, ParsedCommand},
    error::AppError,
    handlers::Reply,
    protocol::{RespDecoder, RespValue},
    pubsub::PubSubHub,
};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::collections::HashSet;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::error;

/// How a client left subscribed mode.
#[derive(Debug, PartialEq, Eq)]
pub enum SubscribedExit {
    /// It unsubscribed from its last channel and continues in normal mode.
    Unsubscribed,
    /// It disconnected, or the connection failed.
    Disconnected,
}

pub fn publish(parsed: &ParsedCommand, pubsub: &PubSubHub) -> Reply {
    let channel = parsed.required(0)?;
    let message = parsed.required(1)?;
    let subscriber_count = pubsub.publish(channel.clone(), message.clone());
    Ok(RespValue::Integer(subscriber_count as i64))
}

/// Subscribes a client, already registered with the hub, to each named
/// channel. Returns one reply per channel, as Redis sends, each with the
/// number of channels subscribed so far.
pub fn subscribe(
    parsed: &ParsedCommand,
    pubsub: &PubSubHub,
    client_id: u64,
    subscriptions: &mut HashSet<Bytes>,
) -> Vec<RespValue> {
    let channels = parsed.args_from(0);
    if channels.is_empty() {
        return vec![RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'subscribe' command",
        ))];
    }
    channels
        .iter()
        .map(|channel| {
            if subscriptions.insert(channel.clone()) {
                pubsub.subscribe(channel.clone(), client_id);
            }
            subscription_reply(
                b"subscribe",
                RespValue::BulkString(channel.clone()),
                subscriptions,
            )
        })
        .collect()
}

/// Unsubscribes from each named channel, or from every channel when none are
/// named. Returns one reply per channel, each with the number left.
pub fn unsubscribe(
    parsed: &ParsedCommand,
    pubsub: &PubSubHub,
    client_id: u64,
    subscriptions: &mut HashSet<Bytes>,
) -> Vec<RespValue> {
    let channels: Vec<Bytes> = if parsed.args_from(0).is_empty() {
        subscriptions.iter().cloned().collect()
    } else {
        parsed.args_from(0).to_vec()
    };
    if channels.is_empty() {
        return vec![subscription_reply(
            b"unsubscribe",
            RespValue::NullBulkString,
            subscriptions,
        )];
    }
    channels
        .into_iter()
        .map(|channel| {
            if subscriptions.remove(&channel) {
                pubsub.unsubscribe(&channel, client_id);
            }
            subscription_reply(
                b"unsubscribe",
                RespValue::BulkString(channel),
                subscriptions,
            )
        })
        .collect()
}

fn subscription_reply(
    kind: &'static [u8],
    channel: RespValue,
    subscriptions: &HashSet<Bytes>,
) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(kind)),
        channel,
        RespValue::Integer(subscriptions.len() as i64),
    ])
}

/// Sends several replies and flushes once.
pub async fn send_all(
    framed: &mut Framed<TcpStream, RespDecoder>,
    replies: Vec<RespValue>,
) -> Result<(), AppError> {
    for reply in replies {
        framed.feed(reply).await?;
    }
    framed.flush().await
}

/// Serves a client in subscribed mode until it unsubscribes from its last
/// channel, disconnects, or is dropped by the hub for falling behind.
pub async fn subscribed_loop(
    framed: &mut Framed<TcpStream, RespDecoder>,
    subscriptions: &mut HashSet<Bytes>,
    rx: &mut mpsc::Receiver<RespValue>,
    pubsub: &PubSubHub,
    client_id: u64,
) -> SubscribedExit {
    loop {
        tokio::select! {
            message = rx.recv() => {
                // The hub closed the queue: the client fell too far behind.
                let Some(message) = message else {
                    return SubscribedExit::Disconnected;
                };
                if let Err(e) = framed.send(message).await {
                    error!("failed to send published message to client: {e}");
                    return SubscribedExit::Disconnected;
                }
            }
            result = framed.next() => {
                let parsed_command = match result {
                    Some(Ok(value)) => match ParsedCommand::from_resp(value) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            if let Err(e) = framed.send(e.into()).await {
                                error!("failed to send error response: {e}");
                                return SubscribedExit::Disconnected;
                            }
                            continue;
                        }
                    },
                    Some(Err(e)) => {
                        error!("error reading from stream in subscribed mode: {e}");
                        return SubscribedExit::Disconnected;
                    }
                    None => return SubscribedExit::Disconnected,
                };

                let replies = match parsed_command.command() {
                    Command::Subscribe => {
                        subscribe(&parsed_command, pubsub, client_id, subscriptions)
                    }
                    Command::Unsubscribe => {
                        unsubscribe(&parsed_command, pubsub, client_id, subscriptions)
                    }
                    Command::Ping => vec![RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from_static(b"pong")),
                        RespValue::BulkString(Bytes::from_static(b"")),
                    ])],
                    cmd => vec![RespValue::Error(Bytes::from(format!(
                        "ERR Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                    )))],
                };
                if let Err(e) = send_all(framed, replies).await {
                    error!("failed to send response in subscribed mode: {e}");
                    return SubscribedExit::Disconnected;
                }
                if subscriptions.is_empty() {
                    return SubscribedExit::Unsubscribed;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn connected_pair() -> (TcpStream, Framed<TcpStream, RespDecoder>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let client = TcpStream::connect(listener.local_addr().expect("addr"))
            .await
            .expect("connect");
        let (server, _) = listener.accept().await.expect("accept");
        (client, Framed::new(server, RespDecoder))
    }

    #[tokio::test]
    async fn subscribed_loop_returns_when_client_disconnects() {
        let (client, mut framed) = connected_pair().await;
        let hub = PubSubHub::new();
        let mut rx = hub.register(1);
        let mut subscriptions = HashSet::new();
        drop(client);

        assert_eq!(
            subscribed_loop(&mut framed, &mut subscriptions, &mut rx, &hub, 1).await,
            SubscribedExit::Disconnected
        );
    }

    #[tokio::test]
    async fn subscribed_loop_delivers_messages_until_disconnect() {
        let (client, mut framed) = connected_pair().await;
        let hub = PubSubHub::new();
        let mut rx = hub.register(1);
        let mut subscriptions = HashSet::new();
        hub.subscribe(Bytes::from_static(b"ch"), 1);
        let message = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"message")),
            RespValue::BulkString(Bytes::from_static(b"ch")),
            RespValue::BulkString(Bytes::from_static(b"hi")),
        ]);
        assert_eq!(
            hub.publish(Bytes::from_static(b"ch"), Bytes::from_static(b"hi")),
            1
        );

        let client_task = tokio::spawn(async move {
            let mut client = Framed::new(client, RespDecoder);
            let received = client.next().await.expect("frame").expect("decode");
            drop(client);
            received
        });

        assert_eq!(
            subscribed_loop(&mut framed, &mut subscriptions, &mut rx, &hub, 1).await,
            SubscribedExit::Disconnected
        );
        assert_eq!(client_task.await.expect("client task"), message);
    }

    fn command(parts: &[&str]) -> ParsedCommand {
        ParsedCommand::from_resp(RespValue::Array(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Bytes::copy_from_slice(p.as_bytes())))
                .collect(),
        ))
        .expect("valid command")
    }

    fn reply(kind: &str, channel: Option<&str>, count: i64) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::copy_from_slice(kind.as_bytes())),
            channel.map_or(RespValue::NullBulkString, |c| {
                RespValue::BulkString(Bytes::copy_from_slice(c.as_bytes()))
            }),
            RespValue::Integer(count),
        ])
    }

    #[test]
    fn replies_once_per_channel_with_running_counts() {
        let hub = PubSubHub::new();
        let _rx = hub.register(1);
        let mut subs = HashSet::new();
        assert_eq!(
            subscribe(&command(&["SUBSCRIBE", "a", "b", "a"]), &hub, 1, &mut subs),
            vec![
                reply("subscribe", Some("a"), 1),
                reply("subscribe", Some("b"), 2),
                reply("subscribe", Some("a"), 2),
            ]
        );
        assert_eq!(
            unsubscribe(&command(&["UNSUBSCRIBE", "b", "zzz"]), &hub, 1, &mut subs),
            vec![
                reply("unsubscribe", Some("b"), 1),
                reply("unsubscribe", Some("zzz"), 1),
            ]
        );
        assert_eq!(
            unsubscribe(&command(&["UNSUBSCRIBE"]), &hub, 1, &mut subs),
            vec![reply("unsubscribe", Some("a"), 0)]
        );
        assert_eq!(
            unsubscribe(&command(&["UNSUBSCRIBE"]), &hub, 1, &mut subs),
            vec![reply("unsubscribe", None, 0)]
        );
        assert!(matches!(
            subscribe(&command(&["SUBSCRIBE"]), &hub, 1, &mut subs)[..],
            [RespValue::Error(_)]
        ));
    }

    #[tokio::test]
    async fn unsubscribing_from_the_last_channel_leaves_subscribed_mode() {
        let (client, mut framed) = connected_pair().await;
        let hub = PubSubHub::new();
        let mut rx = hub.register(1);
        let mut subscriptions = HashSet::new();
        subscribe(
            &command(&["SUBSCRIBE", "a", "b"]),
            &hub,
            1,
            &mut subscriptions,
        );

        let client_task = tokio::spawn(async move {
            let mut client = Framed::new(client, RespDecoder);
            client
                .send(command(&["UNSUBSCRIBE"]).into_resp_array())
                .await
                .expect("send");
            let first = client.next().await.expect("frame").expect("decode");
            let second = client.next().await.expect("frame").expect("decode");
            (first, second, client)
        });

        assert_eq!(
            subscribed_loop(&mut framed, &mut subscriptions, &mut rx, &hub, 1).await,
            SubscribedExit::Unsubscribed
        );
        let (first, second, _client) = client_task.await.expect("client task");
        let mut counts = [first, second].map(|r| match r {
            RespValue::Array(parts) => parts[2].clone(),
            other => panic!("unexpected reply {other:?}"),
        });
        counts.sort_by_key(|c| format!("{c:?}"));
        assert_eq!(counts, [RespValue::Integer(0), RespValue::Integer(1)]);
        assert_eq!(
            hub.publish(Bytes::from_static(b"a"), Bytes::from_static(b"m")),
            0
        );
    }

    #[tokio::test]
    async fn a_subscriber_dropped_by_the_hub_leaves_subscribed_mode() {
        let (_client, mut framed) = connected_pair().await;
        let hub = PubSubHub::new();
        let mut rx = hub.register(1);
        let mut subscriptions = HashSet::new();
        subscribe(&command(&["SUBSCRIBE", "a"]), &hub, 1, &mut subscriptions);
        hub.remove_client(1);

        assert_eq!(
            subscribed_loop(&mut framed, &mut subscriptions, &mut rx, &hub, 1).await,
            SubscribedExit::Disconnected
        );
    }
}
