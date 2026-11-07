use crate::app::{
    command::ParsedCommand, error::AppError, exec::Changes, handlers::Reply, protocol::RespValue,
    store::Keyspace,
};
use crate::config::Config;
use bytes::Bytes;
use tracing::warn;

/// DEL key [key ...]: deletes the keys and replies with how many existed.
pub fn del(parsed: &ParsedCommand, ks: &mut Keyspace, changes: &mut Changes) -> Reply {
    let mut deleted = 0;
    for key in parsed.args_from(0) {
        if ks.remove(key) {
            changes.wrote(key);
            deleted += 1;
        }
    }
    Ok(RespValue::Integer(deleted))
}

/// TYPE: the type of the value at a key.
pub fn key_type(parsed: &ParsedCommand, ks: &mut Keyspace) -> Reply {
    let key = parsed.required(0)?;
    let type_name = ks.type_name(key);
    Ok(RespValue::SimpleString(Bytes::from_static(
        type_name.as_bytes(),
    )))
}

pub fn config(parsed: &ParsedCommand, config: &Config) -> Reply {
    let verb = parsed.required(0)?;
    let key_bytes = parsed.required(1)?;
    if !verb.eq_ignore_ascii_case(b"get") {
        return Err(AppError::ValueError("CONFIG only supports GET".into()));
    }
    let key = std::str::from_utf8(key_bytes)
        .map_err(|_| AppError::ValueError("invalid config key".into()))?;

    let value = match key.to_lowercase().as_str() {
        "dir" => config.dir.to_string_lossy().into_owned(),
        "dbfilename" => config.dbfilename.clone(),
        "appendonly" => config.appendonly.clone(),
        "appenddirname" => config.appenddirname.clone(),
        "appendfilename" => config.appendfilename.clone(),
        "appendfsync" => config.appendfsync.clone(),
        _ => return Ok(RespValue::Array(vec![])),
    };
    Ok(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from(key.to_lowercase())),
        RespValue::BulkString(Bytes::from(value)),
    ]))
}

pub fn keys(parsed: &ParsedCommand, ks: &mut Keyspace) -> Reply {
    let pattern = parsed.required(0)?;
    if &pattern[..] != b"*" {
        warn!("Received KEYS command with non-'*' pattern, which is not supported.");
        return Ok(RespValue::Array(vec![]));
    }
    let keys = ks.keys();
    Ok(RespValue::Array(
        keys.into_iter().map(RespValue::BulkString).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use crate::app::{run_test as run, test_context, RespValue};
    use bytes::Bytes;

    fn pair(name: &str, value: &str) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::copy_from_slice(name.as_bytes())),
            RespValue::BulkString(Bytes::copy_from_slice(value.as_bytes())),
        ])
    }

    #[tokio::test]
    async fn config_get_returns_the_aof_options_and_dir() {
        let ctx = test_context();
        let cwd = std::env::current_dir().expect("current dir");
        let cases = [
            ("dir", cwd.to_string_lossy().into_owned()),
            ("appendonly", "no".into()),
            ("appenddirname", "appendonlydir".into()),
            ("appendfilename", "appendonly.aof".into()),
            ("appendfsync", "everysec".into()),
        ];
        for (name, value) in cases {
            assert_eq!(
                run(&["CONFIG", "GET", name], &ctx).await,
                pair(name, &value)
            );
        }
        assert_eq!(
            run(&["CONFIG", "GET", "unknown"], &ctx).await,
            RespValue::Array(vec![])
        );
    }
}
