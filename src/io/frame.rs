use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

use crate::io::{
    connection::ConnectionID,
    encoding::{
        DecodeError, EncodeError, variable_length, variable_length_decode, variable_length_encode,
    },
    packet::Token,
    stream::{Dir, StreamID},
};

mod frame_type {
    use std::ops::RangeInclusive;

    pub(super) const STREAM: RangeInclusive<u64> = 0x08..=0x0f;
    pub(super) const PADDING: u64 = 0x00;
    pub(super) const PING: u64 = 0x01;
    pub(super) const ACK: u64 = 0x02;
    pub(super) const ACK_ECN: u64 = 0x03;
    pub(super) const RESET_STREAM: u64 = 0x04;
    pub(super) const STOP_SENDING: u64 = 0x05;
    pub(super) const CRYPTO: u64 = 0x06;
    pub(super) const NEW_TOKEN: u64 = 0x07;
    pub(super) const MAX_DATA: u64 = 0x10;
    pub(super) const MAX_STREAM_DATA: u64 = 0x11;
    pub(super) const MAX_STREAMS_BIDIR: u64 = 0x12;
    pub(super) const MAX_STREAMS_UNI: u64 = 0x13;
    pub(super) const DATA_BLOCKED: u64 = 0x14;
    pub(super) const STREAM_DATA_BLOCKED: u64 = 0x15;
    pub(super) const STREAMS_BLOCKED_BIDIR: u64 = 0x16;
    pub(super) const STREAMS_BLOCKED_UNI: u64 = 0x17;
    pub(super) const NEW_CONNECTION_ID: u64 = 0x18;
    pub(super) const RETIRE_CONNECTION_ID: u64 = 0x19;
    pub(super) const PATH_CHALLENGE: u64 = 0x1a;
    pub(super) const PATH_RESPONSE: u64 = 0x1b;
    pub(super) const CONNECTION_CLOSE_QUIC_LAYER: u64 = 0x1c;
    pub(super) const CONNECTION_CLOSE_APP_LAYER: u64 = 0x1d;
    pub(super) const HANDSHAKE_DONE: u64 = 0x1e;
}

#[derive(Debug)]
pub(super) enum Frame {
    Padding,
    Ping,
    Ack {
        largest: usize,
        delay: usize,
        range_count: usize,
        first_range: usize,
        ranges: Vec<AckRange>,
        ecn: Option<EcnCounts>,
    },
    ResetStream {
        id: StreamID,
        error_code: usize, // TODO: Make spesific type?
        final_size: usize,
    },
    StopSending {
        id: StreamID,
        error_code: usize, // TODO: Make spesific type?
    },
    Crypto {
        offset: usize,
        data: Vec<u8>,
    },
    NewToken {
        token: Token,
    },
    Stream {
        id: StreamID,
        fin: bool,
        last: bool,
        offset: usize,
        data: Bytes,
    },
    MaxData {
        max_data: usize,
    },
    MaxStreamData {
        id: StreamID,
        max_stream_data: usize,
    },
    MaxStreams {
        dir: Dir,
        max_streams: usize,
    },
    DataBlocked {
        max_data: usize,
    },
    StreamDataBlocked {
        id: StreamID,
        max_stream_data: usize,
    },
    StreamsBlocked {
        dir: Dir,
        max_streams: usize,
    },
    NewConnectionId {
        seq_num: usize,
        retire: usize,
        cid: ConnectionID,
        stateless_reset_token: Token, // TODO: This should be fixed size. Different type?
    },
    RetireConnectionId {
        seq_num: usize,
    },
    PathChallenge {
        data: [u8; 8],
    },
    PathResponse {
        data: [u8; 8],
    },
    ConnectionCloseQuic {
        error_code: usize,
        frame_type: usize,
        reason_phrase: Vec<u8>,
    },
    ConnectionCloseApp {
        error_code: usize,
        reason_phrase: Vec<u8>,
    },
    HandshakeDone,
}

impl Frame {
    pub(crate) fn decode<B: Buf>(buf: &mut B) -> Result<Self, FrameDecodeError> {
        let t = variable_length_decode(buf)? as u64;

        use Frame::*;
        use frame_type::*;
        match t {
            PADDING => Ok(Padding),
            PING => Ok(Ping),
            ACK | ACK_ECN => {
                let largest = variable_length_decode(buf)?;
                let delay = variable_length_decode(buf)?;
                let range_count = variable_length_decode(buf)?;
                let first_range = variable_length_decode(buf)?;
                let mut ranges = Vec::with_capacity(range_count);

                // TODO: Actually handle ranges
                for _ in 0..range_count {
                    let gap = variable_length_decode(buf)?;
                    let length = variable_length_decode(buf)?;
                    ranges.push(AckRange { gap, length });
                }

                let ecn = if t == ACK_ECN {
                    let ect0 = variable_length_decode(buf)?;
                    let ect1 = variable_length_decode(buf)?;
                    let ecn = variable_length_decode(buf)?;
                    Some(EcnCounts { ect0, ect1, ecn })
                } else {
                    None
                };

                Ok(Ack {
                    largest,
                    delay,
                    range_count,
                    first_range,
                    ranges,
                    ecn,
                })
            }
            RESET_STREAM => {
                let id = StreamID::new_unchecked(variable_length_decode(buf)?);
                let error_code = variable_length_decode(buf)?;
                let final_size = variable_length_decode(buf)?;
                Ok(ResetStream {
                    id,
                    error_code,
                    final_size,
                })
            }
            STOP_SENDING => {
                let id = StreamID::new_unchecked(variable_length_decode(buf)?);
                let error_code = variable_length_decode(buf)?;
                Ok(StopSending { id, error_code })
            }
            CRYPTO => {
                let offset = variable_length_decode(buf)?;
                let length = variable_length_decode(buf)?;
                if buf.remaining() < length {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut data = vec![0u8; length];
                buf.copy_to_slice(&mut data);
                Ok(Crypto { offset, data })
            }
            NEW_TOKEN => {
                let length = variable_length_decode(buf)?;
                if buf.remaining() < length {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut token = vec![0u8; length];
                buf.copy_to_slice(&mut token);
                Ok(NewToken {
                    token: Token(token),
                })
            }
            MAX_DATA => {
                let max_data = variable_length_decode(buf)?;
                Ok(MaxData { max_data })
            }
            MAX_STREAM_DATA => {
                let id = StreamID::new_unchecked(variable_length_decode(buf)?);
                let max_stream_data = variable_length_decode(buf)?;
                Ok(MaxStreamData {
                    id,
                    max_stream_data,
                })
            }
            MAX_STREAMS_UNI | MAX_STREAMS_BIDIR => {
                let dir = if t == MAX_STREAMS_BIDIR {
                    Dir::Bidir
                } else {
                    Dir::Uni
                };
                let max_streams = variable_length_decode(buf)?;
                Ok(MaxStreams { dir, max_streams })
            }
            DATA_BLOCKED => {
                let max_data = variable_length_decode(buf)?;
                Ok(DataBlocked { max_data })
            }
            STREAM_DATA_BLOCKED => {
                let id = StreamID::new_unchecked(variable_length_decode(buf)?);
                let max_stream_data = variable_length_decode(buf)?;
                Ok(StreamDataBlocked {
                    id,
                    max_stream_data,
                })
            }
            STREAMS_BLOCKED_UNI | STREAMS_BLOCKED_BIDIR => {
                let dir = if t == STREAMS_BLOCKED_BIDIR {
                    Dir::Bidir
                } else {
                    Dir::Uni
                };
                let max_streams = variable_length_decode(buf)?;
                Ok(StreamsBlocked { dir, max_streams })
            }
            NEW_CONNECTION_ID => {
                let seq_num = variable_length_decode(buf)?;
                let retire = variable_length_decode(buf)?;
                let length = buf.try_get_u8().map_err(|_| DecodeError::NotEnoughData)?;
                if length > 20 || length == 0 {
                    return Err(FrameDecodeError::ProtocolViolation);
                }
                let cid = ConnectionID::from_buf(buf, length);
                if buf.remaining() < 16 {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut token = vec![0u8; 16]; // 128 bits
                buf.copy_to_slice(&mut token);

                Ok(NewConnectionId {
                    seq_num,
                    retire,
                    cid,
                    stateless_reset_token: Token(token),
                })
            }
            RETIRE_CONNECTION_ID => {
                let seq_num = variable_length_decode(buf)?;
                Ok(RetireConnectionId { seq_num })
            }
            PATH_CHALLENGE => {
                let mut data = [0u8; 8];
                buf.copy_to_slice(&mut data);
                Ok(PathChallenge { data })
            }
            PATH_RESPONSE => {
                let mut data = [0u8; 8];
                buf.copy_to_slice(&mut data);
                Ok(PathResponse { data })
            }
            CONNECTION_CLOSE_QUIC_LAYER => {
                let error_code = variable_length_decode(buf)?;
                let frame_type = variable_length_decode(buf)?;
                let length = variable_length_decode(buf)?;
                if buf.remaining() < length {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut reason_phrase = vec![0u8; length];
                buf.copy_to_slice(&mut reason_phrase);
                Ok(ConnectionCloseQuic {
                    error_code,
                    frame_type,
                    reason_phrase,
                })
            }
            CONNECTION_CLOSE_APP_LAYER => {
                let error_code = variable_length_decode(buf)?;
                let length = variable_length_decode(buf)?;
                if buf.remaining() < length {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut reason_phrase = vec![0u8; length];
                buf.copy_to_slice(&mut reason_phrase);
                Ok(ConnectionCloseApp {
                    error_code,
                    reason_phrase,
                })
            }
            HANDSHAKE_DONE => Ok(HandshakeDone),
            val if STREAM.contains(&val) => {
                let id = StreamID::new_unchecked(variable_length_decode(buf)?);
                let fin = t & 0x1 == 0x1;
                let offset = if t & 0x04 == 0x04 {
                    variable_length_decode(buf)?
                } else {
                    0
                };
                // TODO: refactor?
                let (data, last) = if t & 0x02 == 0x02 {
                    let len = variable_length_decode(buf)?;
                    if len > buf.remaining() {
                        return Err(FrameDecodeError::NotEnoughData);
                    }
                    (buf.copy_to_bytes(len), false)
                } else {
                    (buf.copy_to_bytes(buf.remaining()), false)
                };
                Ok(Frame::Stream {
                    id,
                    fin,
                    last,
                    offset,
                    data,
                })
            }
            _ => Err(FrameDecodeError::InvalidFrameType),
        }
    }

    pub(crate) fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), FrameEncodeError> {
        use frame_type::*;
        match self {
            Frame::Padding => {
                variable_length_encode(buf, PADDING as usize)?;
            }
            Frame::Ping => variable_length_encode(buf, PING as usize)?,
            Frame::Ack {
                largest,
                delay,
                range_count,
                first_range,
                ranges,
                ecn,
            } => {
                let t = if ecn.is_none() { ACK } else { ACK_ECN };
                variable_length_encode(buf, t as usize)?;
                variable_length_encode(buf, *largest)?;
                variable_length_encode(buf, *delay)?;
                variable_length_encode(buf, *range_count)?;
                variable_length_encode(buf, *first_range)?;

                for range in ranges.iter() {
                    variable_length_encode(buf, range.gap)?;
                    variable_length_encode(buf, range.length)?;
                }

                if let Some(ecn) = ecn {
                    variable_length_encode(buf, ecn.ect0)?;
                    variable_length_encode(buf, ecn.ect1)?;
                    variable_length_encode(buf, ecn.ecn)?;
                }
            }
            Frame::ResetStream {
                id,
                error_code,
                final_size,
            } => {
                variable_length_encode(buf, RESET_STREAM as usize)?;
                variable_length_encode(buf, id.value())?;
                variable_length_encode(buf, *error_code)?;
                variable_length_encode(buf, *final_size)?;
            }
            Frame::StopSending { id, error_code } => {
                variable_length_encode(buf, STOP_SENDING as usize)?;
                variable_length_encode(buf, id.value())?;
                variable_length_encode(buf, *error_code)?;
            }
            Frame::Crypto { offset, data } => {
                variable_length_encode(buf, CRYPTO as usize)?;
                variable_length_encode(buf, *offset)?;
                variable_length_encode(buf, data.len())?;
                buf.put_slice(data);
            }
            Frame::NewToken { token } => {
                variable_length_encode(buf, NEW_TOKEN as usize)?;
                variable_length_encode(buf, token.0.len())?;
                buf.put_slice(&token.0);
            }
            Frame::Stream {
                id,
                fin,
                last,
                offset,
                data,
            } => {
                let t = 0b00001000
                    + (if *offset > 0 { 0x4 } else { 0 })
                    + (if !last { 0x2 } else { 0 })
                    + (if *fin { 1 } else { 0 });

                variable_length_encode(buf, t)?;
                variable_length_encode(buf, id.value())?;
                if *offset > 0 {
                    variable_length_encode(buf, *offset)?;
                }
                if !last {
                    variable_length_encode(buf, data.len())?;
                }

                if buf.remaining_mut() < data.len() {
                    return Err(FrameEncodeError::TooSmallBuffer);
                }

                buf.put_slice(data); // TODO: Look at clone
            }
            Frame::MaxData { max_data } => {
                variable_length_encode(buf, MAX_DATA as usize)?;
                variable_length_encode(buf, *max_data)?;
            }
            Frame::MaxStreamData {
                id,
                max_stream_data,
            } => {
                variable_length_encode(buf, MAX_STREAM_DATA as usize)?;
                variable_length_encode(buf, id.value())?;
                variable_length_encode(buf, *max_stream_data)?;
            }
            Frame::MaxStreams { dir, max_streams } => {
                match dir {
                    Dir::Bidir => variable_length_encode(buf, MAX_STREAMS_BIDIR as usize)?,
                    Dir::Uni => variable_length_encode(buf, MAX_STREAMS_UNI as usize)?,
                };
                variable_length_encode(buf, *max_streams)?;
            }
            Frame::DataBlocked { max_data } => {
                variable_length_encode(buf, DATA_BLOCKED as usize)?;
                variable_length_encode(buf, *max_data)?;
            }
            Frame::StreamDataBlocked {
                id,
                max_stream_data,
            } => {
                variable_length_encode(buf, STREAM_DATA_BLOCKED as usize)?;
                variable_length_encode(buf, id.value())?;
                variable_length_encode(buf, *max_stream_data)?;
            }
            Frame::StreamsBlocked { dir, max_streams } => {
                match dir {
                    Dir::Bidir => variable_length_encode(buf, STREAMS_BLOCKED_BIDIR as usize)?,
                    Dir::Uni => variable_length_encode(buf, STREAMS_BLOCKED_UNI as usize)?,
                };
                variable_length_encode(buf, *max_streams)?;
            }
            Frame::NewConnectionId {
                seq_num,
                retire,
                cid,
                stateless_reset_token,
            } => {
                variable_length_encode(buf, NEW_CONNECTION_ID as usize)?;
                variable_length_encode(buf, *seq_num)?;
                variable_length_encode(buf, *retire)?;
                buf.put_u8(cid.len);
                buf.put_slice(&cid.buf[..cid.len as usize]);
                buf.put_slice(&stateless_reset_token.0);
            }
            Frame::RetireConnectionId { seq_num } => {
                variable_length_encode(buf, RETIRE_CONNECTION_ID as usize)?;
                variable_length_encode(buf, *seq_num)?;
            }
            Frame::PathChallenge { data } => {
                variable_length_encode(buf, PATH_CHALLENGE as usize)?;
                buf.put_slice(data);
            }
            Frame::PathResponse { data } => {
                variable_length_encode(buf, PATH_RESPONSE as usize)?;
                buf.put_slice(data);
            }
            Frame::ConnectionCloseQuic {
                error_code,
                frame_type,
                reason_phrase,
            } => {
                variable_length_encode(buf, CONNECTION_CLOSE_QUIC_LAYER as usize)?;
                variable_length_encode(buf, *error_code)?;
                variable_length_encode(buf, *frame_type)?;
                variable_length_encode(buf, reason_phrase.len())?;
                buf.put_slice(reason_phrase);
            }
            Frame::ConnectionCloseApp {
                error_code,
                reason_phrase,
            } => {
                variable_length_encode(buf, CONNECTION_CLOSE_APP_LAYER as usize)?;
                variable_length_encode(buf, *error_code)?;
                variable_length_encode(buf, reason_phrase.len())?;
                buf.put_slice(reason_phrase);
            }
            Frame::HandshakeDone => {
                variable_length_encode(buf, HANDSHAKE_DONE as usize)?;
            }
        };
        Ok(())
    }

    pub(super) fn stream_id(&self) -> Option<StreamID> {
        match self {
            Self::ResetStream { id, .. }
            | Self::Stream { id, .. }
            | Self::StopSending { id, .. }
            | Self::MaxStreamData { id, .. }
            | Self::StreamDataBlocked { id, .. } => Some(id.clone()),
            _ => None,
        }
    }

    pub(super) fn size(&self) -> usize {
        use Frame::*;
        use frame_type::*;
        match self {
            Padding => variable_length(PADDING),
            Ping => variable_length(PING),
            Ack {
                largest,
                delay,
                range_count,
                first_range,
                ranges,
                ecn,
            } => {
                let l = variable_length((*largest) as u64);
                let d = variable_length((*delay) as u64);
                let r = variable_length((*range_count) as u64);
                let f = variable_length((*first_range) as u64);
                let mut sum = l + d + r + f;
                for ack_range in ranges {
                    sum += variable_length(ack_range.gap as u64);
                    sum += variable_length(ack_range.length as u64);
                }
                if let Some(ecn) = ecn {
                    sum += variable_length(ecn.ect0 as u64);
                    sum += variable_length(ecn.ect1 as u64);
                    sum += variable_length(ecn.ecn as u64);
                }
                sum
            }
            ResetStream {
                id,
                error_code,
                final_size,
            } => todo!(),
            StopSending { id, error_code } => todo!(),
            Crypto { offset, data } => todo!(),
            NewToken { token } => todo!(),
            Stream {
                id,
                fin,
                last,
                offset,
                data,
            } => {
                let i = variable_length(id.value() as u64);
                let o = if *offset != 0 {
                    variable_length(*offset as u64)
                } else {
                    0
                };

                let l = if !*last {
                    variable_length(data.len() as u64)
                } else {
                    0
                };

                i + o + l + data.len()
            }
            MaxData { max_data } => todo!(),
            MaxStreamData {
                id,
                max_stream_data,
            } => todo!(),
            MaxStreams { dir, max_streams } => todo!(),
            DataBlocked { max_data } => todo!(),
            StreamDataBlocked {
                id,
                max_stream_data,
            } => todo!(),
            StreamsBlocked { dir, max_streams } => todo!(),
            NewConnectionId {
                seq_num,
                retire,
                cid,
                stateless_reset_token,
            } => todo!(),
            RetireConnectionId { seq_num } => todo!(),
            PathChallenge { data } => todo!(),
            PathResponse { data } => todo!(),
            ConnectionCloseQuic {
                error_code,
                frame_type,
                reason_phrase,
            } => todo!(),
            ConnectionCloseApp {
                error_code,
                reason_phrase,
            } => todo!(),
            HandshakeDone => todo!(),
        }
    }
}

#[derive(Debug)]
pub(super) struct AckRange {
    gap: usize,
    length: usize,
}

#[derive(Debug)]
pub(super) struct EcnCounts {
    ect0: usize,
    ect1: usize,
    ecn: usize,
}

pub(super) struct FrameIterator {
    buf: Bytes,
}

impl FrameIterator {
    pub(super) fn new(buf: Bytes) -> Self {
        Self { buf }
    }
}

impl Iterator for FrameIterator {
    type Item = Frame;

    fn next(&mut self) -> Option<Self::Item> {
        let next = Frame::decode(&mut self.buf);
        next.ok()
    }
}

#[derive(Debug, Error)]
pub(super) enum FrameDecodeError {
    #[error("Invalid frame type")]
    InvalidFrameType,
    #[error("Not enough data in the buffer when parsing")]
    NotEnoughData,
    #[error("Decode error: {0}")]
    Decode(#[from] DecodeError),
    #[error("PROTOCOL_VIOLATION")]
    ProtocolViolation,
}

#[derive(Debug, Error)]
pub(crate) enum FrameEncodeError {
    #[error("Buffer is not big enough")]
    TooSmallBuffer,
    #[error("Encode error: {0}")]
    Encode(#[from] EncodeError),
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn encode_decode_stream_frame() {
        let stream = Frame::Stream {
            id: StreamID::new_unchecked(1),
            fin: false,
            last: true,
            offset: 3,
            data: Bytes::from_static(&[0xff, 0xff, 0xff]),
        };

        let mut encoded = Vec::new();

        stream
            .encode(&mut encoded)
            .expect("Encoding should not fail");

        let mut encoded = Cursor::new(encoded);

        let output_stream = Frame::decode(&mut encoded).expect("Decoding should not fail");

        println!("Start: {:?}", stream);
        println!("Final: {:?}", output_stream);
        match output_stream {
            Frame::Stream {
                id,
                fin,
                last,
                offset,
                data,
            } => {
                assert_eq!(id.value(), 1);
                assert!(!fin);
                assert!(last);
                assert_eq!(offset, 3);
                assert_eq!(data, Bytes::from_static(&[0xff, 0xff, 0xff]));
            }
            _ => panic!("Got wrong frame type"),
        }
    }
}
