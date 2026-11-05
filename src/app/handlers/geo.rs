use crate::app::{
    command::ParsedCommand,
    error::AppError,
    geo,
    handlers::Reply,
    protocol::RespValue,
    sorted_set::SortedSet,
    store::{DataType, Store},
};
use bytes::Bytes;

pub fn geoadd(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let member = parsed.required(3)?;
    let longitude = parsed.float(1)?;
    let latitude = parsed.float(2)?;

    if !(-180.0..=180.0).contains(&longitude) || !(-85.05112878..=85.05112878).contains(&latitude) {
        return Err(AppError::ValueError(format!(
            "invalid longitude,latitude pair {longitude},{latitude}"
        )));
    }

    let score = geo::encode(latitude, longitude) as f64;
    store.with_keyspace(|ks| {
        let zset = ks.get_or_create_as(key, DataType::as_sorted_set, || {
            DataType::SortedSet(SortedSet::new())
        })?;
        Ok(RespValue::Integer(zset.add(score, member.clone()) as i64))
    })
}

pub fn geopos(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let members = parsed.args_from(1);
    store.with_keyspace(|ks| {
        let zset = ks.get_as(key, DataType::as_sorted_set)?.map(|zset| &*zset);
        let positions = members
            .iter()
            .map(|member| match zset.and_then(|zset| zset.score_of(member)) {
                Some(score) => {
                    let coords = geo::decode(score as u64);
                    RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from(coords.longitude.to_string())),
                        RespValue::BulkString(Bytes::from(coords.latitude.to_string())),
                    ])
                }
                None => RespValue::NullArray,
            })
            .collect();
        Ok(RespValue::Array(positions))
    })
}

pub fn geodist(parsed: &ParsedCommand, store: &Store) -> Reply {
    let key = parsed.required(0)?;
    let member1 = parsed.required(1)?;
    let member2 = parsed.required(2)?;
    store.with_keyspace(|ks| {
        let zset = ks.get_as(key, DataType::as_sorted_set)?.map(|zset| &*zset);
        let position = |member| {
            zset.and_then(|zset| zset.score_of(member))
                .map(|score| geo::decode(score as u64))
        };
        Ok(match (position(member1), position(member2)) {
            (Some(c1), Some(c2)) => {
                RespValue::BulkString(Bytes::from(format!("{:.4}", geo::distance(c1, c2))))
            }
            _ => RespValue::NullBulkString,
        })
    })
}

/// GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit
pub fn geosearch(parsed: &ParsedCommand, store: &Store) -> Reply {
    if parsed
        .arg(1)
        .is_none_or(|a| !a.eq_ignore_ascii_case(b"fromlonlat"))
        || parsed
            .arg(4)
            .is_none_or(|a| !a.eq_ignore_ascii_case(b"byradius"))
    {
        return Err(AppError::Syntax);
    }
    let key = parsed.required(0)?;
    let unit = parsed.required(6)?;
    let center = geo::Coordinates {
        longitude: parsed.float(2)?,
        latitude: parsed.float(3)?,
    };
    let radius = parsed.float(5)?;

    let radius_meters = match &unit.to_ascii_lowercase()[..] {
        b"m" => radius,
        b"km" => radius * 1000.0,
        b"ft" => radius * 0.3048,
        b"mi" => radius * 1609.34,
        _ => {
            return Err(AppError::ValueError(
                "unsupported unit provided. please use m, km, ft, mi".into(),
            ))
        }
    };

    store.with_keyspace(|ks| {
        let Some(zset) = ks.get_as(key, DataType::as_sorted_set)? else {
            return Ok(RespValue::Array(vec![]));
        };
        let results = zset
            .get_all_members()
            .into_iter()
            .filter(|member| {
                zset.score_of(member).is_some_and(|score| {
                    geo::distance(center, geo::decode(score.round() as u64)) <= radius_meters
                })
            })
            .map(RespValue::BulkString)
            .collect();
        Ok(RespValue::Array(results))
    })
}
