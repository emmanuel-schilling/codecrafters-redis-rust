use crate::app::error::AppError;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// A type alias for a single field entry in a stream.
pub type StreamEntryField = (Bytes, Bytes);
/// A type alias for a single entry in a stream.
pub type StreamEntry = (StreamId, Vec<StreamEntryField>);
// Type alias for the complex structure returned by an XREAD command.
pub type XReadResult = Vec<(Bytes, Vec<StreamEntry>)>;

/// Represents a Stream Entry ID: <millisecondsTime>-<sequenceNumber>
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct StreamId {
    ms_time: u64,
    seq_no: u64,
}

impl StreamId {
    pub fn new(ms_time: u64, seq_no: u64) -> Self {
        Self { ms_time, seq_no }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms_time, self.seq_no)
    }
}

impl FromStr for StreamId {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(AppError::ValueError("Invalid stream ID format".into()));
        }
        let ms_time = parts[0]
            .parse::<u64>()
            .map_err(|_| AppError::ValueError("Invalid stream ID format: bad ms_time".into()))?;
        let seq_no = parts[1]
            .parse::<u64>()
            .map_err(|_| AppError::ValueError("Invalid stream ID format: bad seq_no".into()))?;
        Ok(StreamId { ms_time, seq_no })
    }
}

/// Represents a Redis Stream data structure.
#[derive(Debug, Clone, Default)]
pub struct Stream {
    /// Entries are stored in a BTreeMap, which keeps them sorted by ID.
    /// This is ideal for range queries.
    entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    last_generated_id: StreamId,
}

impl Stream {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a new entry to the stream, handling ID validation and generation.
    pub fn add(
        &mut self,
        id_spec: &str,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<StreamId, AppError> {
        let new_id = self.generate_id(id_spec)?;

        if new_id.ms_time == 0 && new_id.seq_no == 0 {
            return Err(AppError::ValueError(
                "The ID specified in XADD must be greater than 0-0".into(),
            ));
        }

        let last_entry_id = self.entries.last_key_value().map(|(id, _)| *id);

        if let Some(last_id) = last_entry_id {
            if new_id <= last_id {
                return Err(AppError::ValueError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .into(),
                ));
            }
        }

        self.entries.insert(new_id, fields);
        self.last_generated_id = new_id;
        Ok(new_id)
    }

    /// Generates a new StreamId based on the provided spec and the stream's state.
    fn generate_id(&self, id_spec: &str) -> Result<StreamId, AppError> {
        if id_spec == "*" {
            // A clock set before 1970 counts as time 0; IDs still increase
            // because of the checks against the last ID below.
            let mut ms_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since_epoch| since_epoch.as_millis() as u64);
            let mut seq_no = 0;
            if ms_time == self.last_generated_id.ms_time {
                seq_no = self.last_generated_id.seq_no + 1;
            } else if ms_time < self.last_generated_id.ms_time {
                ms_time = self.last_generated_id.ms_time;
                seq_no = self.last_generated_id.seq_no + 1;
            }
            Ok(StreamId::new(ms_time, seq_no))
        } else if let Some(time_part) = id_spec.strip_suffix("-*") {
            let ms_time = time_part
                .parse::<u64>()
                .map_err(|_| AppError::ValueError("Invalid stream ID format".into()))?;
            let mut seq_no = 0;

            if let Some(last_id) = self.entries.last_key_value().map(|(id, _)| *id) {
                if ms_time == last_id.ms_time {
                    seq_no = last_id.seq_no + 1;
                } else if ms_time < last_id.ms_time {
                    return Err(AppError::ValueError("The ID specified in XADD is equal or smaller than the target stream top item".into()));
                }
            } else if ms_time == 0 {
                seq_no = 1;
            }

            Ok(StreamId::new(ms_time, seq_no))
        } else {
            StreamId::from_str(id_spec)
        }
    }

    /// Retrieves a range of entries from the stream.
    pub fn range(&self, start_spec: &str, end_spec: &str) -> Result<Vec<StreamEntry>, AppError> {
        let start_id = self.parse_bound(start_spec, true)?;
        let end_id = self.parse_bound(end_spec, false)?;

        let range = self.entries.range(start_id..=end_id);
        let result = range.map(|(id, fields)| (*id, fields.clone())).collect();
        Ok(result)
    }

    /// Reads entries from a specific point in the stream.
    pub fn read_from(&self, start_spec: &str) -> Result<Vec<StreamEntry>, AppError> {
        let start_id = if start_spec == "$" {
            self.last_generated_id
        } else {
            StreamId::from_str(start_spec)?
        };

        let range = self
            .entries
            .range((Bound::Excluded(start_id), Bound::Unbounded));
        let result = range.map(|(id, fields)| (*id, fields.clone())).collect();
        Ok(result)
    }

    /// Helper to parse range bounds, including special characters '-' and '+'.
    fn parse_bound(&self, spec: &str, is_start: bool) -> Result<StreamId, AppError> {
        match spec {
            "-" => Ok(StreamId::new(0, 0)),
            "+" => Ok(StreamId::new(u64::MAX, u64::MAX)),
            s => {
                if !s.contains('-') {
                    let ms_time = s
                        .parse::<u64>()
                        .map_err(|_| AppError::ValueError("Invalid stream ID format".into()))?;
                    let seq_no = if is_start { 0 } else { u64::MAX };
                    Ok(StreamId::new(ms_time, seq_no))
                } else {
                    StreamId::from_str(s)
                }
            }
        }
    }

    pub fn last_id(&self) -> Option<StreamId> {
        self.entries.last_key_value().map(|(id, _)| *id)
    }
}
