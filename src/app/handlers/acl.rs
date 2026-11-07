//! ACL: the users connections are authenticated as. Every connection is
//! authenticated as the `default` user, which has the `nopass` flag.

use crate::app::{command::ParsedCommand, error::AppError, handlers::Reply, protocol::RespValue};
use bytes::Bytes;

/// ACL WHOAMI | ACL GETUSER username.
pub fn acl(parsed: &ParsedCommand) -> Reply {
    let subcommand = parsed.required(0)?;
    let name = String::from_utf8_lossy(subcommand).to_lowercase();
    let wrong_arity = || {
        AppError::ValueError(format!(
            "wrong number of arguments for 'acl|{name}' command"
        ))
    };
    match name.as_str() {
        "whoami" => {
            if parsed.arg(1).is_some() {
                return Err(wrong_arity());
            }
            Ok(RespValue::BulkString(Bytes::from_static(b"default")))
        }
        "getuser" => {
            let (Some(user), None) = (parsed.arg(1), parsed.arg(2)) else {
                return Err(wrong_arity());
            };
            if &user[..] != b"default" {
                return Ok(RespValue::NullBulkString);
            }
            Ok(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"flags")),
                RespValue::Array(vec![RespValue::BulkString(Bytes::from_static(b"nopass"))]),
            ]))
        }
        _ => Err(AppError::ValueError(format!(
            "unknown subcommand '{}'. Try ACL HELP.",
            String::from_utf8_lossy(subcommand)
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::app::{run_test as run, test_context, RespValue};
    use bytes::Bytes;

    fn bulk(s: &'static str) -> RespValue {
        RespValue::BulkString(Bytes::from_static(s.as_bytes()))
    }

    fn error(s: &'static str) -> RespValue {
        RespValue::Error(Bytes::from_static(s.as_bytes()))
    }

    #[tokio::test]
    async fn whoami_is_the_default_user() {
        let ctx = test_context();
        assert_eq!(run(&["ACL", "WHOAMI"], &ctx).await, bulk("default"));
        assert_eq!(run(&["acl", "whoami"], &ctx).await, bulk("default"));
    }

    #[tokio::test]
    async fn getuser_reports_the_default_users_nopass_flag() {
        let ctx = test_context();
        assert_eq!(
            run(&["ACL", "GETUSER", "default"], &ctx).await,
            RespValue::Array(vec![bulk("flags"), RespValue::Array(vec![bulk("nopass")])])
        );
        assert_eq!(
            run(&["ACL", "GETUSER", "alice"], &ctx).await,
            RespValue::NullBulkString
        );
    }

    #[tokio::test]
    async fn rejects_bad_arguments() {
        let ctx = test_context();
        let cases: [(&[&str], RespValue); 4] = [
            (
                &["ACL"],
                error("ERR wrong number of arguments for 'acl' command"),
            ),
            (
                &["ACL", "WHOAMI", "x"],
                error("ERR wrong number of arguments for 'acl|whoami' command"),
            ),
            (
                &["ACL", "GETUSER"],
                error("ERR wrong number of arguments for 'acl|getuser' command"),
            ),
            (
                &["ACL", "NOPE"],
                error("ERR unknown subcommand 'NOPE'. Try ACL HELP."),
            ),
        ];
        for (parts, expected) in cases {
            assert_eq!(run(parts, &ctx).await, expected, "{parts:?}");
        }
    }
}
