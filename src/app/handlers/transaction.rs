use crate::app::{
    command::{Command, ParsedCommand},
    exec, read_only_error,
    replication::Role,
    AppContext, ConnectionState, RespValue,
};
use bytes::Bytes;

/// Handles MULTI, EXEC, DISCARD, WATCH and (outside a transaction) UNWATCH,
/// which act on the connection's transaction state. Returns `None` for any
/// other command.
pub fn handle_state(
    parsed: &ParsedCommand,
    client_id: u64,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    ctx: &AppContext,
) -> Option<RespValue> {
    let (in_transaction, aborted) = match state {
        ConnectionState::InTransaction { aborted } => (true, *aborted),
        _ => (false, false),
    };
    let reply = match parsed.command() {
        Command::Multi if in_transaction => error("ERR MULTI calls can not be nested"),
        Command::Multi => {
            *state = ConnectionState::InTransaction { aborted: false };
            queue.clear();
            ok()
        }
        Command::Exec if !in_transaction => error("ERR EXEC without MULTI"),
        Command::Exec => {
            *state = ConnectionState::Normal;
            let queue = std::mem::take(queue);
            if aborted {
                ctx.watches.unwatch(client_id);
                error("EXECABORT Transaction discarded because of previous errors.")
            } else {
                exec(queue, client_id, ctx)
            }
        }
        Command::Discard if !in_transaction => error("ERR DISCARD without MULTI"),
        Command::Discard => {
            *state = ConnectionState::Normal;
            queue.clear();
            ctx.watches.unwatch(client_id);
            ok()
        }
        Command::Watch => match parsed.required(0) {
            Err(e) => e.into(),
            Ok(_) if in_transaction => error("ERR WATCH inside MULTI is not allowed"),
            Ok(_) => {
                ctx.watches.watch(client_id, parsed.args_from(0));
                ok()
            }
        },
        // Inside a transaction UNWATCH is queued, as in Redis.
        Command::Unwatch if !in_transaction => {
            ctx.watches.unwatch(client_id);
            ok()
        }
        _ => return None,
    };
    Some(reply)
}

/// Records that a command was rejected (unknown, or with the wrong number of
/// arguments); if a transaction is being queued, its EXEC will fail.
pub fn reject_while_queuing(state: &mut ConnectionState) {
    if let ConnectionState::InTransaction { aborted } = state {
        *aborted = true;
    }
}

/// Runs a transaction's queued commands in one locked section, so no other
/// client sees or changes the keyspace between them, or runs none of them
/// (replying with a null array) if a key the client watches was written since
/// it watched it. Either way the client stops watching its keys. Replicas
/// receive the transaction's writes wrapped in MULTI and EXEC.
fn exec(queue: Vec<ParsedCommand>, client_id: u64, ctx: &AppContext) -> RespValue {
    exec::locked(ctx, |ks, outbox| {
        if ctx.watches.unwatch(client_id) {
            return RespValue::NullArray;
        }
        let mut applied = exec::Outbox::new();
        let replies = queue
            .iter()
            .map(|parsed| {
                if parsed.command().is_write() && ctx.replication.role() == Role::Replica {
                    read_only_error()
                } else {
                    exec::run(ctx, ks, parsed, &mut applied)
                }
            })
            .collect();
        outbox.extend(exec::as_transaction(applied));
        RespValue::Array(replies)
    })
}

fn ok() -> RespValue {
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

fn error(message: &'static str) -> RespValue {
    RespValue::Error(Bytes::from_static(message.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{error, ok};
    use crate::app::{handle_command, test_command, test_context, AppContext, ConnectionState};
    use crate::app::{run_test as run, RespValue};
    use bytes::Bytes;
    use std::sync::Arc;

    /// One client connection's transaction state, for driving commands.
    struct Client {
        id: u64,
        state: ConnectionState,
        queue: Vec<crate::app::command::ParsedCommand>,
    }

    impl Client {
        fn new(id: u64) -> Self {
            Self {
                id,
                state: ConnectionState::Normal,
                queue: vec![],
            }
        }

        async fn run(&mut self, parts: &[&str], ctx: &Arc<AppContext>) -> RespValue {
            handle_command(
                test_command(parts),
                ctx,
                self.id,
                &mut self.state,
                &mut self.queue,
            )
            .await
        }
    }

    fn bulk(value: &'static str) -> RespValue {
        RespValue::BulkString(Bytes::from_static(value.as_bytes()))
    }

    fn queued() -> RespValue {
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    }

    #[tokio::test]
    async fn watch_replies_ok_outside_a_transaction_and_errors_inside_one() {
        let ctx = test_context();
        let mut client = Client::new(1);
        assert_eq!(client.run(&["WATCH", "key"], &ctx).await, ok());
        assert_eq!(client.run(&["watch", "k1", "k2", "k3"], &ctx).await, ok());
        assert_eq!(
            client.run(&["WATCH"], &ctx).await,
            error("ERR wrong number of arguments for 'watch' command")
        );
        assert_eq!(client.run(&["MULTI"], &ctx).await, ok());
        assert_eq!(
            client.run(&["WATCH", "key"], &ctx).await,
            error("ERR WATCH inside MULTI is not allowed")
        );
        // The transaction is still open, and WATCH wasn't queued.
        assert_eq!(client.run(&["SET", "a", "1"], &ctx).await, queued());
        assert_eq!(
            client.run(&["EXEC"], &ctx).await,
            RespValue::Array(vec![ok()])
        );
    }

    #[tokio::test]
    async fn exec_runs_the_queue_when_no_watched_key_was_written() {
        let ctx = test_context();
        let (mut client, mut other) = (Client::new(1), Client::new(2));
        other.run(&["SET", "baz", "100"], &ctx).await;
        assert_eq!(client.run(&["WATCH", "baz"], &ctx).await, ok());
        assert_eq!(client.run(&["MULTI"], &ctx).await, ok());
        assert_eq!(client.run(&["SET", "caz", "400"], &ctx).await, queued());
        assert_eq!(client.run(&["INCR", "n"], &ctx).await, queued());
        assert_eq!(other.run(&["SET", "caz", "300"], &ctx).await, ok());
        assert_eq!(
            client.run(&["EXEC"], &ctx).await,
            RespValue::Array(vec![ok(), RespValue::Integer(1)])
        );
        assert_eq!(other.run(&["GET", "caz"], &ctx).await, bulk("400"));
    }

    #[tokio::test]
    async fn exec_aborts_when_a_watched_key_was_written_or_created() {
        // Each case: keys to watch, then the key another client writes.
        let cases: [(&[&str], &str); 4] = [
            (&["foo"], "foo"),
            (&["foo", "bar"], "foo"),
            (&["foo", "bar"], "bar"),
            (&["missing"], "missing"),
        ];
        for (watched, written) in cases {
            let ctx = test_context();
            run(&["SET", "foo", "100"], &ctx).await;
            run(&["SET", "bar", "200"], &ctx).await;
            let mut client = Client::new(1);
            let mut parts = vec!["WATCH"];
            parts.extend_from_slice(watched);
            assert_eq!(client.run(&parts, &ctx).await, ok());
            // Writing the value it already holds still counts as a change.
            run(&["SET", written, "100"], &ctx).await;
            assert_eq!(client.run(&["MULTI"], &ctx).await, ok());
            assert_eq!(client.run(&["SET", "bar", "300"], &ctx).await, queued());
            assert_eq!(
                client.run(&["EXEC"], &ctx).await,
                RespValue::NullArray,
                "{parts:?} then SET {written}"
            );
            let bar = if written == "bar" { "100" } else { "200" };
            assert_eq!(run(&["GET", "bar"], &ctx).await, bulk(bar), "{parts:?}");
        }
    }

    #[tokio::test]
    async fn exec_and_discard_clear_the_watched_keys() {
        let ctx = test_context();
        let mut client = Client::new(1);
        for end in ["EXEC", "DISCARD"] {
            client.run(&["WATCH", "foo"], &ctx).await;
            run(&["SET", "foo", "1"], &ctx).await;
            client.run(&["MULTI"], &ctx).await;
            client.run(&[end], &ctx).await;

            // A later transaction isn't affected by the earlier write.
            client.run(&["MULTI"], &ctx).await;
            client.run(&["SET", "bar", "2"], &ctx).await;
            assert_eq!(
                client.run(&["EXEC"], &ctx).await,
                RespValue::Array(vec![ok()]),
                "{end}"
            );
        }
    }

    #[tokio::test]
    async fn blocking_commands_in_a_transaction_reply_at_once() {
        let ctx = test_context();
        let mut client = Client::new(1);
        client.run(&["MULTI"], &ctx).await;
        for parts in [
            &["BLPOP", "q", "0"][..],
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "$"],
            &["RPUSH", "q", "a"],
            &["BLPOP", "q", "0"],
        ] {
            assert_eq!(client.run(parts, &ctx).await, queued());
        }
        assert_eq!(
            client.run(&["EXEC"], &ctx).await,
            RespValue::Array(vec![
                RespValue::NullArray,
                RespValue::NullArray,
                RespValue::Integer(1),
                RespValue::Array(vec![bulk("q"), bulk("a")]),
            ])
        );
    }

    #[tokio::test]
    async fn only_writes_that_succeed_mark_watched_keys() {
        let ctx = test_context();
        let (mut client, mut other) = (Client::new(1), Client::new(2));
        run(&["SET", "s", "x"], &ctx).await;
        client.run(&["WATCH", "s"], &ctx).await;
        assert!(matches!(
            run(&["LPUSH", "s", "a"], &ctx).await,
            RespValue::Error(_)
        ));
        client.run(&["MULTI"], &ctx).await;
        assert_eq!(client.run(&["EXEC"], &ctx).await, RespValue::Array(vec![]));

        // A write made by another client's transaction counts.
        client.run(&["WATCH", "s"], &ctx).await;
        other.run(&["MULTI"], &ctx).await;
        other.run(&["SET", "s", "y"], &ctx).await;
        other.run(&["EXEC"], &ctx).await;
        client.run(&["MULTI"], &ctx).await;
        assert_eq!(client.run(&["EXEC"], &ctx).await, RespValue::NullArray);
    }

    #[tokio::test]
    async fn writes_from_the_master_mark_watched_keys_on_a_replica() {
        let ctx = test_context();
        let mut client = Client::new(1);
        client.run(&["WATCH", "k"], &ctx).await;
        crate::app::apply_from_master(&ctx, &[test_command(&["SET", "k", "v"])]);
        client.run(&["MULTI"], &ctx).await;
        assert_eq!(client.run(&["EXEC"], &ctx).await, RespValue::NullArray);
    }

    #[tokio::test]
    async fn a_command_rejected_while_queuing_fails_the_transaction() {
        let ctx = test_context();
        let mut client = Client::new(1);
        client.run(&["WATCH", "w"], &ctx).await;
        client.run(&["MULTI"], &ctx).await;
        assert_eq!(client.run(&["SET", "a", "1"], &ctx).await, queued());
        assert_eq!(
            client.run(&["GET"], &ctx).await,
            error("ERR wrong number of arguments for 'get' command")
        );
        assert_eq!(client.run(&["SET", "b", "1"], &ctx).await, queued());
        assert_eq!(
            client.run(&["EXEC"], &ctx).await,
            error("EXECABORT Transaction discarded because of previous errors.")
        );
        assert_eq!(run(&["GET", "a"], &ctx).await, RespValue::NullBulkString);

        // The watches are cleared and the next transaction starts afresh.
        run(&["SET", "w", "1"], &ctx).await;
        client.run(&["MULTI"], &ctx).await;
        client.run(&["SET", "a", "1"], &ctx).await;
        assert_eq!(
            client.run(&["EXEC"], &ctx).await,
            RespValue::Array(vec![ok()])
        );
    }

    #[tokio::test]
    async fn a_runtime_error_fails_only_its_own_command() {
        let ctx = test_context();
        let mut client = Client::new(1);
        run(&["SET", "s", "x"], &ctx).await;
        client.run(&["MULTI"], &ctx).await;
        client.run(&["INCR", "s"], &ctx).await;
        client.run(&["SET", "a", "1"], &ctx).await;
        assert_eq!(
            client.run(&["EXEC"], &ctx).await,
            RespValue::Array(vec![
                error("ERR value is not an integer or out of range"),
                ok()
            ])
        );
    }

    #[tokio::test]
    async fn unwatch_forgets_the_watched_keys() {
        let ctx = test_context();
        let mut client = Client::new(1);
        assert_eq!(client.run(&["UNWATCH"], &ctx).await, ok());
        run(&["SET", "foo", "100"], &ctx).await;
        client.run(&["WATCH", "foo", "bar"], &ctx).await;
        run(&["SET", "foo", "200"], &ctx).await;
        assert_eq!(client.run(&["UNWATCH"], &ctx).await, ok());
        client.run(&["MULTI"], &ctx).await;
        client.run(&["SET", "foo", "400"], &ctx).await;
        // Queued inside a transaction; EXEC has already cleared the watches when it runs.
        assert_eq!(client.run(&["UNWATCH"], &ctx).await, queued());
        assert_eq!(
            client.run(&["EXEC"], &ctx).await,
            RespValue::Array(vec![ok(), ok()])
        );
        assert_eq!(run(&["GET", "foo"], &ctx).await, bulk("400"));
        assert_eq!(
            client.run(&["UNWATCH", "foo"], &ctx).await,
            error("ERR wrong number of arguments for 'unwatch' command")
        );
    }
}
