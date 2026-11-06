use crate::app::{
    command::{Command, ParsedCommand},
    error::AppError,
    AppContext, RespValue,
};

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

/// Runs a command and returns its reply.
pub async fn dispatch(ctx: &AppContext, parsed: ParsedCommand) -> RespValue {
    let store = &ctx.store;
    let reply = match parsed.command() {
        Command::Ping => connection::ping(),
        Command::Echo => connection::echo(&parsed),

        Command::Set => string::set(&parsed, store),
        Command::Get => string::get(&parsed, store),
        Command::Incr => string::incr(&parsed, store),
        Command::StrLen => string::strlen(&parsed, store),

        Command::Config => server::config(&parsed, &ctx.config),
        Command::Keys => server::keys(&parsed, store),
        Command::Type => server::key_type(&parsed, store),

        Command::Info => replication::info(&parsed, &ctx.replication),
        Command::ReplConf => replication::replconf(),
        Command::Wait => replication::wait(&parsed, &ctx.replication).await,

        Command::LPush => list::push(&parsed, store, list::End::Front),
        Command::RPush => list::push(&parsed, store, list::End::Back),
        Command::LPop => list::lpop(&parsed, store),
        Command::LLen => list::llen(&parsed, store),
        Command::LRange => list::lrange(&parsed, store),
        Command::BLPop => list::blpop(&parsed, ctx).await,

        Command::XAdd => stream::xadd(&parsed, store, &ctx.waiters),
        Command::XRange => stream::xrange(&parsed, store),
        Command::XRead => stream::xread(&parsed, store, &ctx.waiters).await,

        Command::ZAdd => sorted_set::zadd(&parsed, store),
        Command::ZCard => sorted_set::zcard(&parsed, store),
        Command::ZScore => sorted_set::zscore(&parsed, store),
        Command::ZRank => sorted_set::zrank(&parsed, store),
        Command::ZRange => sorted_set::zrange(&parsed, store),
        Command::ZRem => sorted_set::zrem(&parsed, store),

        Command::GeoAdd => geo::geoadd(&parsed, store),
        Command::GeoPos => geo::geopos(&parsed, store),
        Command::GeoDist => geo::geodist(&parsed, store),
        Command::GeoSearch => geo::geosearch(&parsed, store),

        Command::SetBit => bitmap::setbit(&parsed, store),
        Command::GetBit => bitmap::getbit(&parsed, store),
        Command::BitCount => bitmap::bitcount(&parsed, store),

        Command::Publish => pubsub::publish(&parsed, &ctx.pubsub),

        // The connection loop handles these, and the transaction state handles
        // MULTI, EXEC and DISCARD, before a command is dispatched; anywhere
        // else (queued in a transaction, or sent by a master) they're unknown.
        Command::PSync
        | Command::Subscribe
        | Command::Unsubscribe
        | Command::Multi
        | Command::Exec
        | Command::Discard => Err(AppError::UnknownCommand(parsed.command().to_string())),
    };
    reply.unwrap_or_else(RespValue::from)
}
