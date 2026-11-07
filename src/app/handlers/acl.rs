//! ACL: the users connections are authenticated as. Every connection is
//! authenticated as the `default` user, the only user there is.

use crate::app::{
    acl::Acl, command::ParsedCommand, error::AppError, handlers::Reply, protocol::RespValue,
};
use bytes::Bytes;

/// ACL WHOAMI | ACL GETUSER username | ACL SETUSER username [rule ...].
pub fn acl(parsed: &ParsedCommand, acl: &Acl) -> Reply {
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
            let (flags, passwords) = acl.flags_and_passwords();
            Ok(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"flags")),
                RespValue::Array(
                    flags
                        .into_iter()
                        .map(|flag| RespValue::BulkString(Bytes::from_static(flag.as_bytes())))
                        .collect(),
                ),
                RespValue::BulkString(Bytes::from_static(b"passwords")),
                RespValue::Array(
                    passwords
                        .into_iter()
                        .map(|hash| RespValue::BulkString(Bytes::from(hash)))
                        .collect(),
                ),
            ]))
        }
        "setuser" => {
            let user = parsed.arg(1).ok_or_else(wrong_arity)?;
            if &user[..] != b"default" {
                return Err(AppError::ValueError(
                    "only the default user is supported".into(),
                ));
            }
            // Every rule is checked before any is applied, as in Redis.
            let rules = parsed.args_from(2);
            if let Some(rule) = rules.iter().find(|rule| !rule.starts_with(b">")) {
                return Err(AppError::ValueError(format!(
                    "Error in ACL SETUSER modifier '{}': Syntax error",
                    String::from_utf8_lossy(rule)
                )));
            }
            for rule in rules {
                acl.add_password(&rule[1..]);
            }
            Ok(RespValue::SimpleString(Bytes::from_static(b"OK")))
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

    /// ACL GETUSER's reply for a user with `flags` and `passwords`.
    fn user(flags: &[&'static str], passwords: &[&'static str]) -> RespValue {
        RespValue::Array(vec![
            bulk("flags"),
            RespValue::Array(flags.iter().map(|f| bulk(f)).collect()),
            bulk("passwords"),
            RespValue::Array(passwords.iter().map(|p| bulk(p)).collect()),
        ])
    }

    const MYPASSWORD: &str = "89e01536ac207279409d4de1e5253e01f4a1769e696db0d6062ca9b8f56767c8";

    #[tokio::test]
    async fn getuser_reports_the_default_users_nopass_flag() {
        let ctx = test_context();
        assert_eq!(
            run(&["ACL", "GETUSER", "default"], &ctx).await,
            user(&["nopass"], &[])
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

    #[tokio::test]
    async fn setuser_adds_a_hashed_password_and_clears_nopass() {
        let ctx = test_context();
        let ok = RespValue::SimpleString(Bytes::from_static(b"OK"));
        assert_eq!(
            run(&["ACL", "SETUSER", "default", ">mypassword"], &ctx).await,
            ok
        );
        assert_eq!(
            run(&["ACL", "GETUSER", "default"], &ctx).await,
            user(&[], &[MYPASSWORD])
        );
        // The same password isn't added twice; another one is added after it.
        run(
            &["ACL", "SETUSER", "default", ">mypassword", ">other"],
            &ctx,
        )
        .await;
        let RespValue::Array(reply) = run(&["ACL", "GETUSER", "default"], &ctx).await else {
            panic!("not an array");
        };
        let RespValue::Array(passwords) = &reply[3] else {
            panic!("no passwords");
        };
        assert_eq!(passwords.len(), 2);
        assert_eq!(passwords[0], bulk(MYPASSWORD));
    }

    #[tokio::test]
    async fn setuser_applies_no_rule_if_any_is_invalid() {
        let ctx = test_context();
        assert_eq!(
            run(&["ACL", "SETUSER", "default", ">pw", "on"], &ctx).await,
            error("ERR Error in ACL SETUSER modifier 'on': Syntax error")
        );
        assert_eq!(
            run(&["ACL", "SETUSER", "alice", ">pw"], &ctx).await,
            error("ERR only the default user is supported")
        );
        assert_eq!(
            run(&["ACL", "SETUSER"], &ctx).await,
            error("ERR wrong number of arguments for 'acl|setuser' command")
        );
        assert_eq!(
            run(&["ACL", "GETUSER", "default"], &ctx).await,
            user(&["nopass"], &[])
        );
    }
}
