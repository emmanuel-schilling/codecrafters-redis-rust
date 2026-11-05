use crate::app::{
    command::{Command, ParsedCommand},
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

pub fn publish(parsed: ParsedCommand, pubsub: &PubSubHub) -> RespValue {
    let (Some(channel), Some(message)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'publish' command",
        ));
    };

    let subscriber_count = pubsub.publish(channel.clone(), message.clone());
    RespValue::Integer(subscriber_count as i64)
}

pub fn subscribe(
    parsed: ParsedCommand,
    pubsub: &PubSubHub,
    client_id: u64,
    sender: mpsc::Sender<RespValue>,
    current_subs: &HashSet<Bytes>,
) -> (RespValue, HashSet<Bytes>) {
    let channels = parsed.args_from(0);
    let mut new_subs = HashSet::new();
    for channel in channels {
        if !current_subs.contains(channel) {
            pubsub.subscribe(channel.clone(), client_id, sender.clone());
            new_subs.insert(channel.clone());
        }
    }

    let first_channel = channels.first().cloned().unwrap_or_default();
    let count = current_subs.len() + new_subs.len();

    let response = RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"subscribe")),
        RespValue::BulkString(first_channel),
        RespValue::Integer(count as i64),
    ]);
    (response, new_subs)
}

pub fn unsubscribe(
    parsed: ParsedCommand,
    pubsub: &PubSubHub,
    client_id: u64,
    current_subs: &HashSet<Bytes>,
) -> (RespValue, Vec<Bytes>) {
    let channels_to_unsub = if parsed.args_from(0).is_empty() {
        current_subs.iter().cloned().collect()
    } else {
        parsed.args_from(0).to_vec()
    };

    let mut removed_subs = Vec::new();
    let mut count = current_subs.len();
    for channel in &channels_to_unsub {
        if current_subs.contains(channel) {
            pubsub.unsubscribe(channel, client_id);
            removed_subs.push(channel.clone());
            count -= 1;
        }
    }

    let first_channel = channels_to_unsub.first().cloned().unwrap_or_default();
    let response = RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"unsubscribe")),
        RespValue::BulkString(first_channel),
        RespValue::Integer(count as i64),
    ]);
    (response, removed_subs)
}

/// Serves a client in subscribed mode. Returns when the client disconnects
/// or the connection fails; the caller then removes its subscriptions.
pub async fn subscribed_loop(
    framed: &mut Framed<TcpStream, RespDecoder>,
    subscriptions: &mut HashSet<Bytes>,
    tx: &mpsc::Sender<RespValue>,
    rx: &mut mpsc::Receiver<RespValue>,
    pubsub: &PubSubHub,
    client_id: u64,
) {
    loop {
        tokio::select! {
            Some(message) = rx.recv() => {
                if let Err(e) = framed.send(message).await {
                    error!("failed to send published message to client: {e}");
                    return;
                }
            }
            result = framed.next() => {
                match result {
                    Some(Ok(value)) => {
                         let parsed_command = match ParsedCommand::from_resp(value) {
                             Ok(cmd) => cmd,
                             Err(e) => {
                                 if let Err(e) = framed.send(RespValue::Error(Bytes::from(e.to_string()))).await { error!("failed to send error response: {e}"); }
                                 continue;
                             }
                         };

                        let cmd = parsed_command.command();
                        let allowed_in_sub_mode = matches!(cmd, Command::Subscribe | Command::Unsubscribe | Command::Ping);

                        if !allowed_in_sub_mode {
                             let err_msg = format!("ERR Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context");
                             if let Err(e) = framed.send(RespValue::Error(Bytes::from(err_msg))).await { error!("failed to send subscribed mode error: {e}"); return; }
                             continue;
                        }

                        let response = if cmd == Command::Subscribe {
                             let (resp, new_subs) = subscribe(parsed_command, pubsub, client_id, tx.clone(), subscriptions);
                             subscriptions.extend(new_subs);
                             resp
                        } else if cmd == Command::Unsubscribe {
                             let (resp, removed_subs) = unsubscribe(parsed_command, pubsub, client_id, subscriptions);
                             for sub in removed_subs {
                                  subscriptions.remove(&sub);
                             }
                             resp
                        } else if cmd == Command::Ping {
                             RespValue::Array(vec![
                                  RespValue::BulkString(Bytes::from_static(b"pong")),
                                  RespValue::BulkString(Bytes::from_static(b"")),
                             ])
                        } else { unreachable!() };

                         if let Err(e) = framed.send(response).await {
                              error!("failed to send response in subscribed mode: {e}");
                              return;
                         }
                    }
                    Some(Err(e)) => { error!("error reading from stream in subscribed mode: {e}"); return; }
                    None => return,
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
        let (tx, mut rx) = mpsc::channel(1);
        let mut subscriptions = HashSet::new();
        drop(client);

        subscribed_loop(
            &mut framed,
            &mut subscriptions,
            &tx,
            &mut rx,
            &PubSubHub::new(),
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn subscribed_loop_delivers_messages_until_disconnect() {
        let (client, mut framed) = connected_pair().await;
        let (tx, mut rx) = mpsc::channel(1);
        let mut subscriptions = HashSet::new();
        let message = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"message")),
            RespValue::BulkString(Bytes::from_static(b"ch")),
            RespValue::BulkString(Bytes::from_static(b"hi")),
        ]);
        tx.send(message.clone()).await.expect("queue message");

        let client_task = tokio::spawn(async move {
            let mut client = Framed::new(client, RespDecoder);
            let received = client.next().await.expect("frame").expect("decode");
            drop(client);
            received
        });

        subscribed_loop(
            &mut framed,
            &mut subscriptions,
            &tx,
            &mut rx,
            &PubSubHub::new(),
            1,
        )
        .await;
        assert_eq!(client_task.await.expect("client task"), message);
    }
}
