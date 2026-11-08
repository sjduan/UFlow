use std::collections::BTreeMap;
use std::io::{self, Read, Write};

pub const BRIDGE_MAGIC: [u8; 8] = *b"UFSIMB01";
pub const BRIDGE_PROTOCOL_MAJOR: u16 = 1;
pub const BRIDGE_PROTOCOL_MINOR: u16 = 0;
pub const BRIDGE_MAX_BODY_BYTES: usize = 64 * 1024;
const FIXED_HEADER_BYTES: usize = 68;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum BridgeMessageType {
    Identity = 1,
    Probe = 2,
    Allocate = 3,
    Release = 4,
    InitializeDeterministic = 5,
    Publish = 6,
    Import = 7,
    Revoke = 8,
    Checksum = 9,
    Completion = 10,
    Error = 11,
    Shutdown = 12,
    Accepted = 13,
    // Explicit opt-in pool envelope. Old v1 peers reject unknown type 14.
    PoolControlV2 = 14,
}

impl TryFrom<u16> for BridgeMessageType {
    type Error = BridgeProtocolError;

    fn try_from(value: u16) -> Result<Self, BridgeProtocolError> {
        match value {
            1 => Ok(Self::Identity),
            2 => Ok(Self::Probe),
            3 => Ok(Self::Allocate),
            4 => Ok(Self::Release),
            5 => Ok(Self::InitializeDeterministic),
            6 => Ok(Self::Publish),
            7 => Ok(Self::Import),
            8 => Ok(Self::Revoke),
            9 => Ok(Self::Checksum),
            10 => Ok(Self::Completion),
            11 => Ok(Self::Error),
            12 => Ok(Self::Shutdown),
            13 => Ok(Self::Accepted),
            14 => Ok(Self::PoolControlV2),
            _ => Err(BridgeProtocolError::UnknownMessageType(value)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeFrame {
    pub protocol_major: u16,
    pub protocol_minor: u16,
    pub message_type: BridgeMessageType,
    pub flags: u16,
    pub request_id: u64,
    pub correlation_id: u64,
    pub execution_realm_id: String,
    pub node_id: String,
    pub boot_id: String,
    pub provider_session_id: String,
    pub session_generation: u64,
    pub status: i32,
    pub error_code: i32,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BridgeProtocolError {
    Truncated,
    BadMagic,
    UnsupportedMajor(u16),
    UnknownMessageType(u16),
    OversizedBody(usize),
    InvalidLength,
    BadChecksum,
    MissingIdentityField(&'static str),
    InvalidUtf8,
    Io(String),
}

fn checksum64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn take<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], BridgeProtocolError> {
    let end = cursor
        .checked_add(N)
        .ok_or(BridgeProtocolError::InvalidLength)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(BridgeProtocolError::Truncated)?
        .try_into()
        .map_err(|_| BridgeProtocolError::Truncated)?;
    *cursor = end;
    Ok(value)
}

impl BridgeFrame {
    pub fn request(
        message_type: BridgeMessageType,
        request_id: u64,
        correlation_id: u64,
        execution_realm_id: impl Into<String>,
        node_id: impl Into<String>,
        boot_id: impl Into<String>,
        provider_session_id: impl Into<String>,
        session_generation: u64,
        body: Vec<u8>,
    ) -> Self {
        Self {
            protocol_major: BRIDGE_PROTOCOL_MAJOR,
            protocol_minor: BRIDGE_PROTOCOL_MINOR,
            message_type,
            flags: 0,
            request_id,
            correlation_id,
            execution_realm_id: execution_realm_id.into(),
            node_id: node_id.into(),
            boot_id: boot_id.into(),
            provider_session_id: provider_session_id.into(),
            session_generation,
            status: 0,
            error_code: 0,
            body,
        }
    }

    pub fn validate(&self) -> Result<(), BridgeProtocolError> {
        if self.protocol_major != BRIDGE_PROTOCOL_MAJOR {
            return Err(BridgeProtocolError::UnsupportedMajor(self.protocol_major));
        }
        if self.request_id == 0 || self.correlation_id == 0 || self.session_generation == 0 {
            return Err(BridgeProtocolError::InvalidLength);
        }
        for (name, value) in [
            ("execution_realm_id", self.execution_realm_id.as_str()),
            ("node_id", self.node_id.as_str()),
            ("boot_id", self.boot_id.as_str()),
            ("provider_session_id", self.provider_session_id.as_str()),
        ] {
            if value.is_empty() {
                return Err(BridgeProtocolError::MissingIdentityField(name));
            }
            if value.len() > u16::MAX as usize {
                return Err(BridgeProtocolError::InvalidLength);
            }
        }
        if self.body.len() > BRIDGE_MAX_BODY_BYTES {
            return Err(BridgeProtocolError::OversizedBody(self.body.len()));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, BridgeProtocolError> {
        self.validate()?;
        let strings = [
            self.execution_realm_id.as_bytes(),
            self.node_id.as_bytes(),
            self.boot_id.as_bytes(),
            self.provider_session_id.as_bytes(),
        ];
        let mut out = Vec::with_capacity(
            FIXED_HEADER_BYTES
                + strings.iter().map(|value| value.len()).sum::<usize>()
                + self.body.len(),
        );
        out.extend_from_slice(&BRIDGE_MAGIC);
        put_u16(&mut out, self.protocol_major);
        put_u16(&mut out, self.protocol_minor);
        put_u16(&mut out, self.message_type as u16);
        put_u16(&mut out, self.flags);
        put_u64(&mut out, self.request_id);
        put_u64(&mut out, self.correlation_id);
        put_u64(&mut out, self.session_generation);
        put_u32(&mut out, self.body.len() as u32);
        put_i32(&mut out, self.status);
        put_i32(&mut out, self.error_code);
        put_u64(&mut out, checksum64(&self.body));
        for value in strings {
            put_u16(&mut out, value.len() as u16);
        }
        for value in strings {
            out.extend_from_slice(value);
        }
        out.extend_from_slice(&self.body);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BridgeProtocolError> {
        if bytes.len() < FIXED_HEADER_BYTES {
            return Err(BridgeProtocolError::Truncated);
        }
        let mut cursor = 0;
        if take::<8>(bytes, &mut cursor)? != BRIDGE_MAGIC {
            return Err(BridgeProtocolError::BadMagic);
        }
        let protocol_major = u16::from_be_bytes(take(bytes, &mut cursor)?);
        let protocol_minor = u16::from_be_bytes(take(bytes, &mut cursor)?);
        let message_type =
            BridgeMessageType::try_from(u16::from_be_bytes(take(bytes, &mut cursor)?))?;
        let flags = u16::from_be_bytes(take(bytes, &mut cursor)?);
        let request_id = u64::from_be_bytes(take(bytes, &mut cursor)?);
        let correlation_id = u64::from_be_bytes(take(bytes, &mut cursor)?);
        let session_generation = u64::from_be_bytes(take(bytes, &mut cursor)?);
        let body_len = u32::from_be_bytes(take(bytes, &mut cursor)?) as usize;
        let status = i32::from_be_bytes(take(bytes, &mut cursor)?);
        let error_code = i32::from_be_bytes(take(bytes, &mut cursor)?);
        let expected_checksum = u64::from_be_bytes(take(bytes, &mut cursor)?);
        let lengths = [
            u16::from_be_bytes(take(bytes, &mut cursor)?) as usize,
            u16::from_be_bytes(take(bytes, &mut cursor)?) as usize,
            u16::from_be_bytes(take(bytes, &mut cursor)?) as usize,
            u16::from_be_bytes(take(bytes, &mut cursor)?) as usize,
        ];
        if protocol_major != BRIDGE_PROTOCOL_MAJOR {
            return Err(BridgeProtocolError::UnsupportedMajor(protocol_major));
        }
        if body_len > BRIDGE_MAX_BODY_BYTES {
            return Err(BridgeProtocolError::OversizedBody(body_len));
        }
        let variable_len = lengths.iter().sum::<usize>();
        if cursor
            .checked_add(variable_len)
            .and_then(|value| value.checked_add(body_len))
            != Some(bytes.len())
        {
            return Err(BridgeProtocolError::InvalidLength);
        }
        let mut strings = Vec::with_capacity(4);
        for length in lengths {
            let end = cursor + length;
            strings.push(
                std::str::from_utf8(&bytes[cursor..end])
                    .map_err(|_| BridgeProtocolError::InvalidUtf8)?
                    .to_string(),
            );
            cursor = end;
        }
        let body = bytes[cursor..].to_vec();
        if checksum64(&body) != expected_checksum {
            return Err(BridgeProtocolError::BadChecksum);
        }
        let frame = Self {
            protocol_major,
            protocol_minor,
            message_type,
            flags,
            request_id,
            correlation_id,
            execution_realm_id: strings.remove(0),
            node_id: strings.remove(0),
            boot_id: strings.remove(0),
            provider_session_id: strings.remove(0),
            session_generation,
            status,
            error_code,
            body,
        };
        frame.validate()?;
        Ok(frame)
    }

    pub fn write_to(&self, writer: &mut impl Write) -> Result<(), BridgeProtocolError> {
        let encoded = self.encode()?;
        writer
            .write_all(&(encoded.len() as u32).to_be_bytes())
            .and_then(|_| writer.write_all(&encoded))
            .map_err(|error| BridgeProtocolError::Io(error.to_string()))
    }

    pub fn read_from(reader: &mut impl Read) -> Result<Self, BridgeProtocolError> {
        let mut length = [0u8; 4];
        reader
            .read_exact(&mut length)
            .map_err(|error| match error.kind() {
                io::ErrorKind::UnexpectedEof => BridgeProtocolError::Truncated,
                _ => BridgeProtocolError::Io(error.to_string()),
            })?;
        let length = u32::from_be_bytes(length) as usize;
        let max_frame = FIXED_HEADER_BYTES + 4 * u16::MAX as usize + BRIDGE_MAX_BODY_BYTES;
        if length < FIXED_HEADER_BYTES || length > max_frame {
            return Err(BridgeProtocolError::InvalidLength);
        }
        let mut bytes = vec![0u8; length];
        reader
            .read_exact(&mut bytes)
            .map_err(|error| BridgeProtocolError::Io(error.to_string()))?;
        Self::decode(&bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayDecision {
    New,
    Cached(Vec<u8>),
    Conflict,
}

#[derive(Default)]
pub struct ReplayCache {
    entries: BTreeMap<u64, (u64, Vec<u8>)>,
}

impl ReplayCache {
    pub fn observe(&self, frame: &BridgeFrame) -> ReplayDecision {
        let fingerprint = checksum64(&frame.encode().unwrap_or_default());
        match self.entries.get(&frame.request_id) {
            None => ReplayDecision::New,
            Some((existing, response)) if *existing == fingerprint => {
                ReplayDecision::Cached(response.clone())
            }
            Some(_) => ReplayDecision::Conflict,
        }
    }

    pub fn remember(&mut self, frame: &BridgeFrame, encoded_response: Vec<u8>) {
        let fingerprint = checksum64(&frame.encode().unwrap_or_default());
        self.entries
            .insert(frame.request_id, (fingerprint, encoded_response));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> BridgeFrame {
        BridgeFrame::request(
            BridgeMessageType::Probe,
            7,
            9,
            "simulated:ub_sim:run-1",
            "nodeA",
            "boot-1",
            "session-1",
            1,
            b"provider=obmm".to_vec(),
        )
    }

    #[test]
    fn roundtrip_keeps_required_identity() {
        let frame = request();
        assert_eq!(
            BridgeFrame::decode(&frame.encode().unwrap()).unwrap(),
            frame
        );
    }

    #[test]
    fn malformed_version_length_and_checksum_fail_closed() {
        let encoded = request().encode().unwrap();
        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 1;
        assert_eq!(
            BridgeFrame::decode(&bad_magic),
            Err(BridgeProtocolError::BadMagic)
        );
        let mut bad_version = encoded.clone();
        bad_version[8..10].copy_from_slice(&2u16.to_be_bytes());
        assert_eq!(
            BridgeFrame::decode(&bad_version),
            Err(BridgeProtocolError::UnsupportedMajor(2))
        );
        let mut bad_checksum = encoded.clone();
        *bad_checksum.last_mut().unwrap() ^= 1;
        assert_eq!(
            BridgeFrame::decode(&bad_checksum),
            Err(BridgeProtocolError::BadChecksum)
        );
        assert_eq!(
            BridgeFrame::decode(&encoded[..encoded.len() - 1]),
            Err(BridgeProtocolError::InvalidLength)
        );
    }

    #[test]
    fn replay_is_idempotent_and_conflict_fails() {
        let mut cache = ReplayCache::default();
        let frame = request();
        assert_eq!(cache.observe(&frame), ReplayDecision::New);
        cache.remember(&frame, b"terminal".to_vec());
        assert_eq!(
            cache.observe(&frame),
            ReplayDecision::Cached(b"terminal".to_vec())
        );
        let mut conflict = frame;
        conflict.body = b"provider=different".to_vec();
        assert_eq!(cache.observe(&conflict), ReplayDecision::Conflict);
    }
}
