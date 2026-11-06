use crate::app::{
    apply_write,
    command::{Command, ParsedCommand},
    handlers::{dispatch, list, stream},
    read_only_error,
    replication::Role,
    AppContext, ConnectionState, RespValue,
};
use bytes::Bytes;
use std::sync::Arc;

/// Handles MULTI, EXEC, DISCARD and WATCH, which act on the connection's
/// transaction state. Returns `None` for any other command.
pub async fn handle_state(
    parsed: &ParsedCommand,
    client_id: u64,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    ctx: &Arc<AppContext>,
) -> Option<RespValue> {
    let in_transaction = matches!(state, ConnectionState::InTransaction);
    let reply = match parsed.command() {
        Command::Multi if in_transaction => error("ERR MULTI calls can not be nested"),
        Command::Multi => {
            *state = ConnectionState::InTransaction;
            queue.clear();
            ok()
        }
        Command::Exec if !in_transaction => error("ERR EXEC without MULTI"),
        Command::Exec => {
            *state = ConnectionState::Normal;
            exec(std::mem::take(queue), client_id, ctx).await
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
        _ => return None,
    };
    Some(reply)
}

/// Runs a transaction's queued commands with no other client's write in
/// between, or none of them (replying with a null array) if a key the client
/// watches was written since it watched it. Either way the client stops
/// watching its keys.
async fn exec(queue: Vec<ParsedCommand>, client_id: u64, ctx: &Arc<AppContext>) -> RespValue {
    // Every write touches its key while holding this lock, so no write can
    // land between the check and the last queued command.
    let _order = ctx.write_order.lock().await;
    if ctx.watches.unwatch(client_id) {
        return RespValue::NullArray;
    }
    let mut replies = Vec::with_capacity(queue.len());
    for parsed in queue {
        replies.push(run_queued(parsed, ctx).await);
    }
    RespValue::Array(replies)
}

/// Runs one queued command. The caller holds the write-order lock, so
/// blocking commands don't block: as in Redis, BLPOP and XREAD BLOCK reply at
/// once, with a null array if there's nothing to return.
async fn run_queued(parsed: ParsedCommand, ctx: &Arc<AppContext>) -> RespValue {
    let command = parsed.command();
    if command.is_write() && ctx.replication.role() == Role::Replica {
        return read_only_error();
    }
    match command {
        Command::BLPop => list::blpop_now(&parsed, ctx).unwrap_or_else(RespValue::from),
        Command::XRead => stream::xread(&parsed, &ctx.store, &ctx.waiters, false)
            .await
            .unwrap_or_else(RespValue::from),
        _ if command.is_write() => apply_write(ctx, parsed).await,
        _ => dispatch(ctx, parsed).await,
    }
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
        crate::app::apply_command(&ctx, test_command(&["SET", "k", "v"])).await;
        client.run(&["MULTI"], &ctx).await;
        assert_eq!(client.run(&["EXEC"], &ctx).await, RespValue::NullArray);
    }
}
