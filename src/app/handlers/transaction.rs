use crate::app::{command::ParsedCommand, AppContext, ConnectionState, RespValue};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Notify;

pub async fn handle_state(
    parsed: ParsedCommand,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    ctx: &Arc<AppContext>,
    wait_notify: &Arc<Notify>,
) -> Option<RespValue> {
    match parsed.command() {
        crate::app::command::Command::Multi => {
            if matches!(state, ConnectionState::InTransaction) {
                return Some(RespValue::Error(Bytes::from_static(
                    b"ERR MULTI calls can not be nested",
                )));
            }
            *state = ConnectionState::InTransaction;
            queue.clear();
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        }
        crate::app::command::Command::Exec => {
            if !matches!(state, ConnectionState::InTransaction) {
                return Some(RespValue::Error(Bytes::from_static(
                    b"ERR EXEC without MULTI",
                )));
            }
            *state = ConnectionState::Normal;
            let mut responses = Vec::with_capacity(queue.len());
            for cmd in std::mem::take(queue) {
                let response = Box::pin(crate::app::handle_command(
                    cmd,
                    ctx,
                    &mut ConnectionState::Normal,
                    &mut vec![],
                    wait_notify,
                ))
                .await;
                responses.push(response);
            }
            Some(RespValue::Array(responses))
        }
        crate::app::command::Command::Discard => {
            if !matches!(state, ConnectionState::InTransaction) {
                return Some(RespValue::Error(Bytes::from_static(
                    b"ERR DISCARD without MULTI",
                )));
            }
            *state = ConnectionState::Normal;
            queue.clear();
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        }
        _ => None,
    }
}
