use crate::app::{
    command::{Command, ParsedCommand},
    geo,
    protocol::RespValue,
    sorted_set::SortedSet,
    store::{DataType, Store},
};
use bytes::Bytes;
use std::sync::Arc;

pub fn handle(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    match parsed.command() {
        Command::GeoAdd => handle_geoadd(parsed, store),
        Command::GeoPos => handle_geopos(parsed, store),
        Command::GeoDist => handle_geodist(parsed, store),
        Command::GeoSearch => handle_geosearch(parsed, store),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown geo command")),
    }
}

fn handle_geoadd(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(lon_bytes), Some(lat_bytes), Some(member)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2), parsed.arg(3))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'geoadd' command",
        ));
    };

    let longitude = match std::str::from_utf8(lon_bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };
    let latitude = match std::str::from_utf8(lat_bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };

    if !(-180.0..=180.0).contains(&longitude) {
        return RespValue::Error(Bytes::from(format!(
            "ERR invalid longitude,latitude pair {longitude},{latitude}"
        )));
    }
    if !(-85.05112878..=85.05112878).contains(&latitude) {
        return RespValue::Error(Bytes::from(format!(
            "ERR invalid longitude,latitude pair {longitude},{latitude}"
        )));
    }

    let score = geo::encode(latitude, longitude) as f64;
    store.with_keyspace(|ks| {
        match ks.get_or_create_as(key, DataType::as_sorted_set, || {
            DataType::SortedSet(SortedSet::new())
        }) {
            Ok(zset) => RespValue::Integer(zset.add(score, member.clone()) as i64),
            Err(e) => e.into(),
        }
    })
}

fn handle_geopos(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'geopos' command",
        ));
    };
    let members = parsed.args_from(1);

    store.with_keyspace(|ks| {
        let zset = match ks.get_as(key, DataType::as_sorted_set) {
            Ok(zset) => zset.map(|zset| &*zset),
            Err(e) => return e.into(),
        };
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
        RespValue::Array(positions)
    })
}

fn handle_geodist(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member1), Some(member2)) = (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'geodist' command",
        ));
    };

    store.with_keyspace(|ks| {
        let zset = match ks.get_as(key, DataType::as_sorted_set) {
            Ok(zset) => zset.map(|zset| &*zset),
            Err(e) => return e.into(),
        };
        let position = |member| {
            zset.and_then(|zset| zset.score_of(member))
                .map(|score| geo::decode(score as u64))
        };
        match (position(member1), position(member2)) {
            (Some(c1), Some(c2)) => {
                RespValue::BulkString(Bytes::from(format!("{:.4}", geo::distance(c1, c2))))
            }
            _ => RespValue::NullBulkString,
        }
    })
}

fn handle_geosearch(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    // GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit
    if parsed
        .arg(1)
        .is_none_or(|a| !a.eq_ignore_ascii_case(b"fromlonlat"))
        || parsed
            .arg(4)
            .is_none_or(|a| !a.eq_ignore_ascii_case(b"byradius"))
    {
        return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
    }

    let (Some(key), Some(lon_b), Some(lat_b), Some(radius_b), Some(unit_b)) = (
        parsed.arg(0),
        parsed.arg(2),
        parsed.arg(3),
        parsed.arg(5),
        parsed.arg(6),
    ) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'geosearch' command",
        ));
    };

    let center_lon = match std::str::from_utf8(lon_b)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };
    let center_lat = match std::str::from_utf8(lat_b)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };
    let radius = match std::str::from_utf8(radius_b)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };

    let radius_meters = match &unit_b.to_ascii_lowercase()[..] {
        b"m" => radius,
        b"km" => radius * 1000.0,
        b"ft" => radius * 0.3048,
        b"mi" => radius * 1609.34,
        _ => {
            return RespValue::Error(Bytes::from_static(
                b"ERR unsupported unit provided. please use m, km, ft, mi",
            ))
        }
    };

    let center_coords = geo::Coordinates {
        longitude: center_lon,
        latitude: center_lat,
    };

    store.with_keyspace(|ks| match ks.get_as(key, DataType::as_sorted_set) {
        Ok(Some(zset)) => {
            let mut results = Vec::new();
            for member in zset.get_all_members() {
                if let Some(score) = zset.score_of(&member) {
                    let member_coords = geo::decode(score.round() as u64);
                    let distance = geo::distance(center_coords, member_coords);
                    if distance <= radius_meters {
                        results.push(RespValue::BulkString(member));
                    }
                }
            }
            RespValue::Array(results)
        }
        Ok(None) => RespValue::Array(vec![]),
        Err(e) => e.into(),
    })
}
