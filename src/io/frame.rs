use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

use crate::io::{
    connection::ConnectionID,
    encoding::{VarInt, VarIntError, variable_length, variable_length_encode},
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

#[derive(Debug, Clone)]
pub(super) enum Frame {
    Padding,
    Ping,
    Ack {
        largest: VarInt,
        delay: VarInt,
        range_count: VarInt,
        first_range: VarInt,
        ranges: Vec<AckRange>,
        ecn: Option<EcnCounts>,
    },
    ResetStream {
        id: StreamID,
        error_code: VarInt, // TODO: Make spesific type?
        final_size: VarInt,
    },
    StopSending {
        id: StreamID,
        error_code: VarInt, // TODO: Make spesific type?
    },
    Crypto {
        offset: VarInt,
        data: Vec<u8>,
    },
    NewToken {
        token: Token,
    },
    Stream {
        id: StreamID,
        fin: bool,
        last: bool,
        offset: VarInt,
        data: Bytes,
    },
    MaxData {
        max_data: VarInt,
    },
    MaxStreamData {
        id: StreamID,
        max_stream_data: VarInt,
    },
    MaxStreams {
        dir: Dir,
        max_streams: VarInt,
    },
    DataBlocked {
        max_data: VarInt,
    },
    StreamDataBlocked {
        id: StreamID,
        max_stream_data: VarInt,
    },
    StreamsBlocked {
        dir: Dir,
        max_streams: VarInt,
    },
    NewConnectionId {
        seq_num: VarInt,
        retire: VarInt,
        cid: ConnectionID,
        stateless_reset_token: Token, // TODO: This should be fixed size. Different type?
    },
    RetireConnectionId {
        seq_num: VarInt,
    },
    PathChallenge {
        data: [u8; 8],
    },
    PathResponse {
        data: [u8; 8],
    },
    ConnectionCloseQuic {
        error_code: VarInt,
        frame_type: VarInt,
        reason_phrase: Vec<u8>,
    },
    ConnectionCloseApp {
        error_code: VarInt,
        reason_phrase: Vec<u8>,
    },
    HandshakeDone,
}

impl Frame {
    pub(crate) fn decode<B: Buf>(buf: &mut B) -> Result<Self, FrameDecodeError> {
        let t = VarInt::decode(buf)?;

        use Frame::*;
        use frame_type::*;
        match t.0 {
            PADDING => Ok(Padding),
            PING => Ok(Ping),
            ACK | ACK_ECN => {
                let largest = VarInt::decode(buf)?;
                let delay = VarInt::decode(buf)?;
                let range_count = VarInt::decode(buf)?;
                let first_range = VarInt::decode(buf)?;
                let mut ranges = Vec::with_capacity(range_count.0 as usize);

                // TODO: Actually handle ranges
                for _ in 0..range_count.0 {
                    let gap = VarInt::decode(buf)?;
                    let length = VarInt::decode(buf)?;
                    ranges.push(AckRange { gap, length });
                }

                let ecn = if t.0 == ACK_ECN {
                    let ect0 = VarInt::decode(buf)?;
                    let ect1 = VarInt::decode(buf)?;
                    let ecn = VarInt::decode(buf)?;
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
                let id = StreamID::new_unchecked(VarInt::decode(buf)?.0 as usize);
                let error_code = VarInt::decode(buf)?;
                let final_size = VarInt::decode(buf)?;
                Ok(ResetStream {
                    id,
                    error_code,
                    final_size,
                })
            }
            STOP_SENDING => {
                let id = StreamID::new_unchecked(VarInt::decode(buf)?.0 as usize);
                let error_code = VarInt::decode(buf)?;
                Ok(StopSending { id, error_code })
            }
            CRYPTO => {
                let offset = VarInt::decode(buf)?;
                let length = VarInt::decode(buf)?;
                if buf.remaining() < length.0 as usize {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut data = vec![0u8; length.0 as usize];
                buf.copy_to_slice(&mut data);
                Ok(Crypto { offset, data })
            }
            NEW_TOKEN => {
                let length = VarInt::decode(buf)?;
                if buf.remaining() < length.0 as usize {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut token = vec![0u8; length.0 as usize];
                buf.copy_to_slice(&mut token);
                Ok(NewToken {
                    token: Token(token),
                })
            }
            MAX_DATA => {
                let max_data = VarInt::decode(buf)?;
                Ok(MaxData { max_data })
            }
            MAX_STREAM_DATA => {
                let id = StreamID::new_unchecked(VarInt::decode(buf)?.0 as usize);
                let max_stream_data = VarInt::decode(buf)?;
                Ok(MaxStreamData {
                    id,
                    max_stream_data,
                })
            }
            MAX_STREAMS_UNI | MAX_STREAMS_BIDIR => {
                let dir = if t.0 == MAX_STREAMS_BIDIR {
                    Dir::Bidir
                } else {
                    Dir::Uni
                };
                let max_streams = VarInt::decode(buf)?;
                Ok(MaxStreams { dir, max_streams })
            }
            DATA_BLOCKED => {
                let max_data = VarInt::decode(buf)?;
                Ok(DataBlocked { max_data })
            }
            STREAM_DATA_BLOCKED => {
                let id = StreamID::new_unchecked(VarInt::decode(buf)?.0 as usize);
                let max_stream_data = VarInt::decode(buf)?;
                Ok(StreamDataBlocked {
                    id,
                    max_stream_data,
                })
            }
            STREAMS_BLOCKED_UNI | STREAMS_BLOCKED_BIDIR => {
                let dir = if t.0 == STREAMS_BLOCKED_BIDIR {
                    Dir::Bidir
                } else {
                    Dir::Uni
                };
                let max_streams = VarInt::decode(buf)?;
                Ok(StreamsBlocked { dir, max_streams })
            }
            NEW_CONNECTION_ID => {
                let seq_num = VarInt::decode(buf)?;
                let retire = VarInt::decode(buf)?;
                let length = buf.try_get_u8().map_err(|_| VarIntError::NotEnoughData)?;
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
                let seq_num = VarInt::decode(buf)?;
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
                let error_code = VarInt::decode(buf)?;
                let frame_type = VarInt::decode(buf)?;
                let length = VarInt::decode(buf)?;
                if buf.remaining() < length.0 as usize {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut reason_phrase = vec![0u8; length.0 as usize];
                buf.copy_to_slice(&mut reason_phrase);
                Ok(ConnectionCloseQuic {
                    error_code,
                    frame_type,
                    reason_phrase,
                })
            }
            CONNECTION_CLOSE_APP_LAYER => {
                let error_code = VarInt::decode(buf)?;
                let length = VarInt::decode(buf)?;
                if buf.remaining() < length.0 as usize {
                    return Err(FrameDecodeError::NotEnoughData);
                }
                let mut reason_phrase = vec![0u8; length.0 as usize];
                buf.copy_to_slice(&mut reason_phrase);
                Ok(ConnectionCloseApp {
                    error_code,
                    reason_phrase,
                })
            }
            HANDSHAKE_DONE => Ok(HandshakeDone),
            val if STREAM.contains(&val) => {
                let id = StreamID::new_unchecked(VarInt::decode(buf)?.0 as usize);
                let fin = t.0 & 0x1 == 0x1;
                let last = t.0 & 0x2 == 0x2;
                let offset = if t.0 & 0x04 == 0x04 {
                    VarInt::decode(buf)?
                } else {
                    VarInt(0)
                };

                let data = if t.0 & 0x02 == 0x02 {
                    let len = VarInt::decode(buf)?;
                    if len.0 as usize > buf.remaining() {
                        return Err(FrameDecodeError::NotEnoughData);
                    }
                    buf.copy_to_bytes(len.0 as usize)
                } else {
                    buf.copy_to_bytes(buf.remaining())
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
                variable_length_encode(buf, PADDING)?;
            }
            Frame::Ping => variable_length_encode(buf, PING)?,
            Frame::Ack {
                largest,
                delay,
                range_count,
                first_range,
                ranges,
                ecn,
            } => {
                let t = if ecn.is_none() { ACK } else { ACK_ECN };
                VarInt(t).encode(buf)?;
                largest.encode(buf)?;
                delay.encode(buf)?;
                VarInt(ranges.len() as u64).encode(buf)?;
                first_range.encode(buf)?;

                for range in ranges.iter() {
                    range.gap.encode(buf)?;
                    range.length.encode(buf)?;
                }

                if let Some(ecn) = ecn {
                    ecn.ect0.encode(buf)?;
                    ecn.ect1.encode(buf)?;
                    ecn.ecn.encode(buf)?;
                }
            }
            Frame::ResetStream {
                id,
                error_code,
                final_size,
            } => {
                variable_length_encode(buf, RESET_STREAM)?;
                VarInt(id.value() as u64).encode(buf)?;
                error_code.encode(buf)?;
                final_size.encode(buf)?;
            }
            Frame::StopSending { id, error_code } => {
                variable_length_encode(buf, STOP_SENDING)?;
                VarInt(id.value() as u64).encode(buf)?;
                error_code.encode(buf)?;
            }
            Frame::Crypto { offset, data } => {
                variable_length_encode(buf, CRYPTO)?;
                offset.encode(buf)?;
                VarInt(data.len() as u64).encode(buf)?;
                buf.put_slice(data);
            }
            Frame::NewToken { token } => {
                variable_length_encode(buf, NEW_TOKEN)?;
                VarInt(token.0.len() as u64).encode(buf)?;
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
                    + (if offset.0 > 0 { 0x4 } else { 0 })
                    + (if *last { 0x2 } else { 0 })
                    + (if *fin { 1 } else { 0 });

                variable_length_encode(buf, t)?;
                variable_length_encode(buf, id.value() as u64)?;
                if offset.0 > 0 {
                    offset.encode(buf)?;
                }
                if *last {
                    variable_length_encode(buf, data.len() as u64)?;
                }

                if buf.remaining_mut() < data.len() {
                    return Err(FrameEncodeError::TooSmallBuffer);
                }

                buf.put_slice(data); // TODO: Look at clone
            }
            Frame::MaxData { max_data } => {
                variable_length_encode(buf, MAX_DATA)?;
                max_data.encode(buf)?;
            }
            Frame::MaxStreamData {
                id,
                max_stream_data,
            } => {
                variable_length_encode(buf, MAX_STREAM_DATA)?;
                variable_length_encode(buf, id.value() as u64)?;
                max_stream_data.encode(buf)?;
            }
            Frame::MaxStreams { dir, max_streams } => {
                match dir {
                    Dir::Bidir => variable_length_encode(buf, MAX_STREAMS_BIDIR)?,
                    Dir::Uni => variable_length_encode(buf, MAX_STREAMS_UNI)?,
                };
                max_streams.encode(buf)?;
            }
            Frame::DataBlocked { max_data } => {
                variable_length_encode(buf, DATA_BLOCKED)?;
                max_data.encode(buf)?;
            }
            Frame::StreamDataBlocked {
                id,
                max_stream_data,
            } => {
                variable_length_encode(buf, STREAM_DATA_BLOCKED)?;
                variable_length_encode(buf, id.value() as u64)?;
                max_stream_data.encode(buf)?;
            }
            Frame::StreamsBlocked { dir, max_streams } => {
                match dir {
                    Dir::Bidir => variable_length_encode(buf, STREAMS_BLOCKED_BIDIR)?,
                    Dir::Uni => variable_length_encode(buf, STREAMS_BLOCKED_UNI)?,
                };
                max_streams.encode(buf)?;
            }
            Frame::NewConnectionId {
                seq_num,
                retire,
                cid,
                stateless_reset_token,
            } => {
                variable_length_encode(buf, NEW_CONNECTION_ID)?;
                seq_num.encode(buf)?;
                retire.encode(buf)?;
                buf.put_u8(cid.len);
                buf.put_slice(&cid.buf[..cid.len as usize]);
                buf.put_slice(&stateless_reset_token.0);
            }
            Frame::RetireConnectionId { seq_num } => {
                variable_length_encode(buf, RETIRE_CONNECTION_ID)?;
                seq_num.encode(buf)?;
            }
            Frame::PathChallenge { data } => {
                variable_length_encode(buf, PATH_CHALLENGE)?;
                buf.put_slice(data);
            }
            Frame::PathResponse { data } => {
                variable_length_encode(buf, PATH_RESPONSE)?;
                buf.put_slice(data);
            }
            Frame::ConnectionCloseQuic {
                error_code,
                frame_type,
                reason_phrase,
            } => {
                variable_length_encode(buf, CONNECTION_CLOSE_QUIC_LAYER)?;
                error_code.encode(buf)?;
                frame_type.encode(buf)?;
                variable_length_encode(buf, reason_phrase.len() as u64)?;
                buf.put_slice(reason_phrase);
            }
            Frame::ConnectionCloseApp {
                error_code,
                reason_phrase,
            } => {
                variable_length_encode(buf, CONNECTION_CLOSE_APP_LAYER)?;
                error_code.encode(buf)?;
                variable_length_encode(buf, reason_phrase.len() as u64)?;
                buf.put_slice(reason_phrase);
            }
            Frame::HandshakeDone => {
                variable_length_encode(buf, HANDSHAKE_DONE)?;
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
                let t = variable_length(ACK);
                let l = largest.length();
                let d = delay.length();
                let r = range_count.length();
                let f = first_range.length();
                let mut sum = t + l + d + r + f;
                for ack_range in ranges {
                    sum += ack_range.gap.length();
                    sum += ack_range.length.length();
                }
                if let Some(ecn) = ecn {
                    sum += ecn.ect0.length();
                    sum += ecn.ect1.length();
                    sum += ecn.ecn.length();
                }
                sum
            }
            ResetStream {
                id,
                error_code,
                final_size,
            } => {
                let t = variable_length(RESET_STREAM);
                let i = variable_length(id.value() as u64);
                let e = error_code.length();
                let s = final_size.length();

                t + i + e + s
            }
            StopSending { id, error_code } => todo!(),
            Crypto { offset, data } => {
                let t = variable_length(CRYPTO);
                let o = offset.length();
                let l = variable_length(data.len() as u64);
                let d = data.len();
                t + o + l + d
            }
            NewToken { token } => todo!(),
            Stream {
                id,
                fin,
                last,
                offset,
                data,
            } => {
                let i = variable_length(id.value() as u64);
                let o = if offset.0 != 0 { offset.length() } else { 0 };

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
            HandshakeDone => variable_length(HANDSHAKE_DONE),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct AckRange {
    pub(super) gap: VarInt,
    pub(super) length: VarInt,
}

#[derive(Debug, Clone)]
pub(super) struct EcnCounts {
    ect0: VarInt,
    ect1: VarInt,
    ecn: VarInt,
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
    Decode(#[from] VarIntError),
    #[error("PROTOCOL_VIOLATION")]
    ProtocolViolation,
}

#[derive(Debug, Error)]
pub(crate) enum FrameEncodeError {
    #[error("Buffer is not big enough")]
    TooSmallBuffer,
    #[error("Encode error: {0}")]
    VarInt(#[from] VarIntError),
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
            offset: VarInt(3),
            data: Bytes::from_static(&[0xff, 0xff, 0xff]),
        };

        let mut encoded = Vec::new();

        stream
            .encode(&mut encoded)
            .expect("Encoding should not fail");

        println!("Encoded: {encoded:?}");

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
                assert_eq!(offset.0, 3);
                assert_eq!(data, Bytes::from_static(&[0xff, 0xff, 0xff]));
            }
            _ => panic!("Got wrong frame type"),
        }
    }

    #[test]
    fn test_ack_encode_decode() {
        let ack = Frame::Ack {
            largest: VarInt(0),
            delay: VarInt(0),
            range_count: VarInt(0),
            first_range: VarInt(0),
            ranges: Vec::new(),
            ecn: None,
        };

        let mut encoded = Vec::new();

        ack.encode(&mut encoded).expect("Encoding should not fail");

        let encoded = Bytes::from(encoded);

        let iter = FrameIterator::new(encoded);

        for output in iter {
            match output {
                Frame::Ack {
                    largest,
                    delay,
                    range_count,
                    first_range,
                    ranges,
                    ecn,
                } => {
                    assert_eq!(largest, 0);
                    assert_eq!(delay, 0);
                    assert_eq!(range_count, 0);
                    assert_eq!(first_range, 0);
                    assert_eq!(ranges.len(), 0);
                    assert!(ecn.is_none());
                }
                _ => panic!("Got wrong type"),
            }
        }
    }
}
