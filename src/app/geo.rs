const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

const EARTH_RADIUS_METERS: f64 = 6372797.560856;

#[derive(Debug, Clone, Copy)]
pub struct Coordinates {
    pub latitude: f64,
    pub longitude: f64,
}

// --- Encoding Logic (from snippet 2) ---

fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

pub fn encode(latitude: f64, longitude: f64) -> u64 {
    let normalized_latitude = 2.0_f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_longitude = 2.0_f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

    let lat_int = normalized_latitude as u32;
    let lon_int = normalized_longitude as u32;

    interleave(lat_int, lon_int)
}

// --- Decoding Logic (from snippet 1) ---

fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555555555555555;
    result = (result | (result >> 1)) & 0x3333333333333333;
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
    ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> Coordinates {
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));

    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    Coordinates {
        latitude,
        longitude,
    }
}

pub fn decode(geo_code: u64) -> Coordinates {
    let y = geo_code >> 1;
    let x = geo_code;

    let grid_latitude_number = compact_int64_to_int32(x);
    let grid_longitude_number = compact_int64_to_int32(y);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

// --- Distance Logic ---

pub fn distance(c1: Coordinates, c2: Coordinates) -> f64 {
    let lat1_rad = c1.latitude.to_radians();
    let lat2_rad = c2.latitude.to_radians();
    let lon1_rad = c1.longitude.to_radians();
    let lon2_rad = c2.longitude.to_radians();

    let d_lat = lat2_rad - lat1_rad;
    let d_lon = lon2_rad - lon1_rad;

    let a =
        (d_lat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_METERS * c
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRECISION: f64 = 1e-6;

    struct GeoTestCase {
        name: &'static str,
        // Input coordinates from the `encode` snippet
        input_longitude: f64,
        input_latitude: f64,
        // Expected high-precision output coordinates from the `decode` snippet
        output_longitude: f64,
        output_latitude: f64,
        score: u64,
    }

    fn get_test_cases() -> Vec<GeoTestCase> {
        vec![
            GeoTestCase {
                name: "Bangkok",
                input_longitude: 100.5252,
                input_latitude: 13.7220,
                output_longitude: 100.52520006895065,
                output_latitude: 13.722000686932997,
                score: 3962257306574459,
            },
            GeoTestCase {
                name: "Beijing",
                input_longitude: 116.3972,
                input_latitude: 39.9075,
                output_longitude: 116.39719873666763,
                output_latitude: 39.9075003315814,
                score: 4069885364908765,
            },
            GeoTestCase {
                name: "Berlin",
                input_longitude: 13.4105,
                input_latitude: 52.5244,
                output_longitude: 13.410500586032867,
                output_latitude: 52.52439934649943,
                score: 3673983964876493,
            },
            GeoTestCase {
                name: "Copenhagen",
                input_longitude: 12.5655,
                input_latitude: 55.6759,
                output_longitude: 12.56549745798111,
                output_latitude: 55.67589927498264,
                score: 3685973395504349,
            },
            GeoTestCase {
                name: "New Delhi",
                input_longitude: 77.2167,
                input_latitude: 28.6667,
                output_longitude: 77.21670180559158,
                output_latitude: 28.666698899347338,
                score: 3631527070936756,
            },
            GeoTestCase {
                name: "Kathmandu",
                input_longitude: 85.3206,
                input_latitude: 27.7017,
                output_longitude: 85.3205993771553,
                output_latitude: 27.701700137333084,
                score: 3639507404773204,
            },
            GeoTestCase {
                name: "London",
                input_longitude: -0.1278,
                input_latitude: 51.5074,
                output_longitude: -0.12779921293258667,
                output_latitude: 51.50740077990134,
                score: 2163557714755072,
            },
            GeoTestCase {
                name: "New York",
                input_longitude: -74.0060,
                input_latitude: 40.7128,
                output_longitude: -74.00600105524063,
                output_latitude: 40.712798986951505,
                score: 1791873974549446,
            },
            GeoTestCase {
                name: "Paris",
                input_longitude: 2.3488,
                input_latitude: 48.8534,
                output_longitude: 2.348802387714386,
                output_latitude: 48.85340071224621,
                score: 3663832752681684,
            },
            GeoTestCase {
                name: "Sydney",
                input_longitude: 151.2093,
                input_latitude: -33.8688,
                output_longitude: 151.2092998623848,
                output_latitude: -33.86880091934156,
                score: 3252046221964352,
            },
            GeoTestCase {
                name: "Tokyo",
                input_longitude: 139.6917,
                input_latitude: 35.6895,
                output_longitude: 139.691701233387,
                output_latitude: 35.68950126697936,
                score: 4171231230197045,
            },
            GeoTestCase {
                name: "Vienna",
                input_longitude: 16.3707,
                input_latitude: 48.2064,
                output_longitude: 16.370699107646942,
                output_latitude: 48.20640046271915,
                score: 3673109836391743,
            },
        ]
    }

    #[test]
    fn test_encode_matches_known_scores() {
        for case in get_test_cases() {
            let actual_score = encode(case.input_latitude, case.input_longitude);
            assert_eq!(actual_score, case.score, "Encode failed for {}", case.name);
        }
    }

    #[test]
    fn test_decode_matches_known_coordinates() {
        for case in get_test_cases() {
            let decoded = decode(case.score);

            let lon_diff = (decoded.longitude - case.output_longitude).abs();
            let lat_diff = (decoded.latitude - case.output_latitude).abs();

            assert!(
                lon_diff < PRECISION,
                "Longitude decode failed for {}: expected {}, got {} (diff: {})",
                case.name,
                case.output_longitude,
                decoded.longitude,
                lon_diff
            );
            assert!(
                lat_diff < PRECISION,
                "Latitude decode failed for {}: expected {}, got {} (diff: {})",
                case.name,
                case.output_latitude,
                decoded.latitude,
                lat_diff
            );
        }
    }

    #[test]
    fn test_round_trip_precision() {
        for case in get_test_cases() {
            let score = encode(case.input_latitude, case.input_longitude);
            let decoded = decode(score);

            // We compare the decoded result against the high-precision output coordinates.
            let lon_diff = (decoded.longitude - case.output_longitude).abs();
            let lat_diff = (decoded.latitude - case.output_latitude).abs();

            assert!(
                lon_diff < PRECISION,
                "Longitude round-trip failed for {}: expected {}, got {} (diff: {})",
                case.name,
                case.output_longitude,
                decoded.longitude,
                lon_diff
            );
            assert!(
                lat_diff < PRECISION,
                "Latitude round-trip failed for {}: expected {}, got {} (diff: {})",
                case.name,
                case.output_latitude,
                decoded.latitude,
                lat_diff
            );
        }
    }

    #[test]
    fn test_boundary_values() {
        let boundary_cases = vec![
            // Center point
            ("Center", 0.0, 0.0),
            // Corners of the valid domain
            ("South-West", MIN_LONGITUDE, MIN_LATITUDE),
            ("South-East", MAX_LONGITUDE, MIN_LATITUDE),
            ("North-West", MIN_LONGITUDE, MAX_LATITUDE),
            ("North-East", MAX_LONGITUDE, MAX_LATITUDE),
            // Midpoints on the axes
            ("Equator-PrimeMeridian", 0.0, 0.0),
            ("Equator-AntiMeridian", 180.0, 0.0),
            ("NorthPole-Boundary", 0.0, MAX_LATITUDE),
            ("SouthPole-Boundary", 0.0, MIN_LATITUDE),
        ];

        // `decode` returns the centre of the 2^26 x 2^26 grid cell the input
        // falls in, so a round trip is off by at most half a cell per axis.
        // The small slack absorbs floating-point rounding at exactly half a cell.
        const SLACK: f64 = 1e-12;
        let lon_tolerance = LONGITUDE_RANGE / 2.0_f64.powi(27) + SLACK;
        let lat_tolerance = LATITUDE_RANGE / 2.0_f64.powi(27) + SLACK;

        for (name, longitude, latitude) in boundary_cases {
            let score = encode(latitude, longitude);
            let decoded = decode(score);

            let lon_diff = (decoded.longitude - longitude).abs();
            let lat_diff = (decoded.latitude - latitude).abs();

            assert!(
                lon_diff <= lon_tolerance,
                "Longitude round-trip failed for boundary case '{}': expected {}, got {} (diff: {})",
                name, longitude, decoded.longitude, lon_diff
            );
            assert!(
                lat_diff <= lat_tolerance,
                "Latitude round-trip failed for boundary case '{}': expected {}, got {} (diff: {})",
                name,
                latitude,
                decoded.latitude,
                lat_diff
            );
        }
    }
}
