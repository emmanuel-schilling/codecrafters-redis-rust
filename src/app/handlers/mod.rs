use crate::app::{
    command::{Command, ParsedCommand},
    error::AppError,
    exec::Changes,
    store::Keyspace,
    AppContext, RespValue,
};
use bytes::Bytes;

pub mod acl;
pub mod bitmap;
pub mod connection;
pub mod geo;
pub mod list;
pub mod pubsub;
pub mod replication;
pub mod server;
pub mod sorted_set;
pub mod stream;
pub mod string;
pub mod transaction;

/// A command's reply. An error becomes the error reply sent to the client.
pub type Reply = Result<RespValue, AppError>;

/// Runs a command against the locked keyspace and returns its reply. Write
/// handlers record what they change in `changes`. Commands that can wait
/// reply at once here, as inside a transaction; [`crate::app::handle_command`]
/// gives them their waiting form for clients.
pub fn execute(
    ctx: &AppContext,
    ks: &mut Keyspace,
    changes: &mut Changes,
    parsed: &ParsedCommand,
) -> RespValue {
    let reply = match parsed.command() {
        Command::Ping => connection::ping(),
        Command::Echo => connection::echo(parsed),

        Command::Set => string::set(parsed, ks, changes),
        Command::Get => string::get(parsed, ks),
        Command::StrLen => string::strlen(parsed, ks),
        Command::Incr => string::incr(parsed, ks, changes),
        Command::Del => server::del(parsed, ks, changes),

        Command::Config => server::config(parsed, &ctx.config),
        Command::Keys => server::keys(parsed, ks),
        Command::Type => server::key_type(parsed, ks),

        Command::Info => replication::info(parsed, &ctx.replication),
        Command::ReplConf => replication::replconf(),
        Command::Wait => replication::wait_now(parsed, &ctx.replication),

        Command::LPush => list::push(parsed, ks, changes, list::End::Front),
        Command::RPush => list::push(parsed, ks, changes, list::End::Back),
        Command::LPop => list::lpop(parsed, ks, changes),
        Command::LLen => list::llen(parsed, ks),
        Command::LRange => list::lrange(parsed, ks),
        Command::BLPop => list::blpop_now(parsed, ks, changes),

        Command::XAdd => stream::xadd(parsed, ks, changes),
        Command::XRange => stream::xrange(parsed, ks),
        Command::XRead => stream::xread_now(parsed, ks),

        Command::ZAdd => sorted_set::zadd(parsed, ks, changes),
        Command::ZCard => sorted_set::zcard(parsed, ks),
        Command::ZScore => sorted_set::zscore(parsed, ks),
        Command::ZRank => sorted_set::zrank(parsed, ks),
        Command::ZRange => sorted_set::zrange(parsed, ks),
        Command::ZRem => sorted_set::zrem(parsed, ks, changes),

        Command::GeoAdd => geo::geoadd(parsed, ks, changes),
        Command::GeoPos => geo::geopos(parsed, ks),
        Command::GeoDist => geo::geodist(parsed, ks),
        Command::GeoSearch => geo::geosearch(parsed, ks),

        Command::SetBit => bitmap::setbit(parsed, ks, changes),
        Command::GetBit => bitmap::getbit(parsed, ks),
        Command::BitCount => bitmap::bitcount(parsed, ks),
        Command::BitOp => bitmap::bitop(parsed, ks, changes),

        Command::Publish => pubsub::publish(parsed, &ctx.pubsub),

        Command::Acl => acl::acl(parsed, &ctx.acl),

        // Only reached inside EXEC, which has already cleared the watches.
        Command::Unwatch => Ok(RespValue::SimpleString(Bytes::from_static(b"OK"))),

        // The connection loop handles these (including AUTH), and the
        // transaction state handles MULTI, EXEC, DISCARD and WATCH, before a
        // command is executed;
        // anywhere else (queued in a transaction, or sent by a master) they're
        // unknown.
        Command::PSync
        | Command::Subscribe
        | Command::Unsubscribe
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Watch
        | Command::Auth => Err(AppError::UnknownCommand(parsed.command().to_string())),
    };
    reply.unwrap_or_else(RespValue::from)
}
