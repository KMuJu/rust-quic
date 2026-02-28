use std::fmt::Debug;

use bytes::{Buf, BufMut, BytesMut, TryGetError};
use thiserror::Error;

use crate::io::{
    connection::ConnectionID,
    encoding::{DecodeError, EncodeError, variable_length_decode, variable_length_encode},
};

#[derive(Debug, PartialEq, Eq)]
pub struct Token(pub Vec<u8>);

#[derive(Debug)]
pub enum Header {
    Initial {
        version: u32,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        token: Option<Token>,
        length: usize,
        packet_number: PacketNumber,
    },
    Long {
        dst_cid: ConnectionID,
        packet_number: PacketNumber,
    },
    Short {
        spin: bool,
        key_phase: bool,
        dst_cid: ConnectionID,
        packet_number: PacketNumber,
    },
}

pub struct Packet {
    pub header: Header,
    pub payload: BytesMut,
}

impl Packet {
    pub fn decode<B: Buf>(buf: &mut B, cid_len: u8) -> Result<Self, PacketError> {
        let header = Header::decode(buf, cid_len)?;
        let payload: BytesMut = buf.copy_to_bytes(buf.remaining()).into();

        Ok(Self { header, payload })
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), PacketError> {
        self.header.encode(buf)?;
        buf.put_slice(&self.payload);

        Ok(())
    }

    pub fn dst_cid(&self) -> ConnectionID {
        match self.header {
            Header::Initial { dst_cid, .. } => dst_cid,
            Header::Long { dst_cid, .. } => dst_cid,
            Header::Short { dst_cid, .. } => dst_cid,
        }
    }

    pub fn src_cid(&self) -> Option<ConnectionID> {
        match self.header {
            Header::Initial { src_cid, .. } => Some(src_cid),
            _ => None,
        }
    }

    pub fn packet_number(&self) -> &PacketNumber {
        match &self.header {
            Header::Initial { packet_number, .. } => packet_number,
            Header::Long { packet_number, .. } => packet_number,
            Header::Short { packet_number, .. } => packet_number,
        }
    }

    pub fn is_initial(&self) -> bool {
        matches!(self.header, Header::Initial { .. })
    }
}

impl Debug for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Packet")
            .field(&self.packet_number())
            .finish()
    }
}

pub enum PacketNumber {
    U8(u8),
    U16(u16),
    U24(u32),
    U32(u32),
}

impl PacketNumber {
    fn decode<B: Buf>(buf: &mut B, len: u8) -> Self {
        use PacketNumber::*;
        match len {
            1 => U8(buf.get_u8()),
            2 => U16(buf.get_u16()),
            3 => U24(buf.get_uint(3) as u32),
            4 => U32(buf.get_u32()),
            _ => unreachable!("Invalid length of packet number"),
        }
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        match self {
            PacketNumber::U8(x) => buf.put_u8(*x),
            PacketNumber::U16(x) => buf.put_u16(*x),
            PacketNumber::U24(x) => buf.put_u32(*x),
            PacketNumber::U32(x) => buf.put_u32(*x),
        }
    }

    fn len(&self) -> u8 {
        match self {
            PacketNumber::U8(_) => 1_u8,
            PacketNumber::U16(_) => 2_u8,
            PacketNumber::U24(_) => 3_u8,
            PacketNumber::U32(_) => 4_u8,
        }
    }

    fn u32(&self) -> u32 {
        match self {
            PacketNumber::U8(x) => *x as u32,
            PacketNumber::U16(x) => *x as u32,
            PacketNumber::U24(x) => *x,
            PacketNumber::U32(x) => *x,
        }
    }
}

impl From<usize> for PacketNumber {
    fn from(value: usize) -> Self {
        if value <= 255 {
            Self::U8(value as u8)
        } else if value <= 65_535 {
            Self::U16(value as u16)
        } else if value <= 16_777_215 {
            Self::U24(value as u32)
        } else {
            Self::U32(value as u32)
        }
    }
}

impl Debug for PacketNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PacketNumber").field(&self.u32()).finish()
    }
}

#[derive(Debug, Error)]
pub enum PacketError {
    #[error("Invalid header")]
    InvalidHeader,
    #[error("Decode error: {0}")]
    Decode(#[from] DecodeError),
    #[error("Not enough data in buffer error: {0}")]
    NotEnoughData(#[from] TryGetError),
    #[error("Encode error: {0}")]
    Encode(#[from] EncodeError),
}

impl Header {
    pub fn decode<B: Buf>(buf: &mut B, cid_len: u8) -> Result<Self, PacketError> {
        // FIX: will have to handle encryption and protected stuff and multiple packet types

        let first = buf.try_get_u8()?;
        match (first >> 7, first >> 6 & 1, first >> 5 & 1, first >> 4 & 1) {
            (1, 1, 0, 0) => {
                // TODO: Handle panics
                let packet_number_len = first & 0x3;
                let version = buf.try_get_u32()?;
                let dst_cid_len = buf.try_get_u8()?;
                let dst_cid = ConnectionID::from_buf(buf, dst_cid_len);
                let src_cid_len = buf.try_get_u8()?;
                let src_cid = ConnectionID::from_buf(buf, src_cid_len);
                if dst_cid_len > 20 || src_cid_len > 20 {
                    return Err(PacketError::InvalidHeader);
                }
                // TODO: Fix token
                let token_len = variable_length_decode(buf)?;
                let token = if token_len > 0 {
                    let mut v = vec![0u8; token_len];
                    buf.copy_to_slice(&mut v);
                    Some(Token(v))
                } else {
                    None
                };
                let length = variable_length_decode(buf)?;
                let packet_number = PacketNumber::decode(buf, packet_number_len);
                Ok(Self::Initial {
                    version,
                    dst_cid,
                    src_cid,
                    token,
                    length,
                    packet_number,
                })
            }
            (1, 1, 0, 1) => {
                // TODO: 0-RTT
                todo!()
            }
            (1, 1, 1, 0) => {
                // TODO: Handshake
                todo!()
            }
            (1, 1, 1, 1) => {
                // TODO: Retry
                todo!()
            }
            (0, 1, _, _) => {
                if buf.remaining() < 1 + cid_len as usize {
                    println!(
                        "Not enough buffer for first byte and destination connection id: {}",
                        buf.remaining()
                    );
                    return Err(PacketError::InvalidHeader);
                }
                let dst_cid = ConnectionID::from_buf(buf, cid_len);
                let packet_number_len = first & 0x3;
                if buf.remaining() < packet_number_len as usize {
                    println!("Not enough buffer for packet number");
                    return Err(PacketError::InvalidHeader);
                }

                Ok(Header::Short {
                    spin: first & 0x20 == 0x20,
                    key_phase: first & 0x04 == 0x04,
                    dst_cid,
                    packet_number: PacketNumber::decode(buf, packet_number_len),
                })
            }
            _ => {
                println!("Invalid first four bits");
                Err(PacketError::InvalidHeader)
            }
        }
    }

    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), PacketError> {
        match self {
            Header::Initial {
                version,
                dst_cid,
                src_cid,
                token,
                length,
                packet_number,
            } => {
                let first: u8 = 0b1100_0000 + (packet_number.len() & 0b11);
                buf.put_u8(first);
                buf.put_u32(*version);
                buf.put_u8(dst_cid.len);
                buf.put_slice(&dst_cid.buf[..dst_cid.len as usize]);
                buf.put_u8(src_cid.len);
                buf.put_slice(&src_cid.buf[..src_cid.len as usize]);
                // TODO: Fix token
                match token {
                    Some(t) => {
                        variable_length_encode(buf, t.0.len())?;
                        buf.put_slice(&t.0);
                    }
                    None => variable_length_encode(buf, 0)?,
                };
                variable_length_encode(buf, *length)?;
                packet_number.encode(buf);
            }
            Header::Long { .. } => todo!(),
            Header::Short {
                spin,
                key_phase,
                dst_cid,
                packet_number,
            } => {
                let first: u8 = 0b0100_0000
                    + (if *spin { 0x20 } else { 0 })
                    + (if *key_phase { 0x04 } else { 0 })
                    + (packet_number.len() & 0b11);
                buf.put_u8(first);
                buf.put_slice(&dst_cid.buf[..dst_cid.len as usize]);
                packet_number.encode(buf);
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn initial_header_encode_decode() -> Result<(), PacketError> {
        let mut bytes: Vec<u8> = Vec::new();
        let dst_cid_o = ConnectionID::from_buf(&mut Cursor::new([1]), 1);
        let src_cid_o = ConnectionID::from_buf(&mut Cursor::new([1]), 1);
        let header = Header::Initial {
            version: 1,
            dst_cid: dst_cid_o,
            src_cid: src_cid_o,
            token: Some(Token(vec![1, 2])),
            length: 0,
            packet_number: PacketNumber::U8(2),
        };
        header.encode(&mut bytes).unwrap();
        let output = Header::decode(&mut Cursor::new(bytes), 0)?;
        match output {
            Header::Initial {
                version,
                dst_cid,
                src_cid,
                token,
                length,
                packet_number,
            } => {
                assert_eq!(version, 1);
                assert_eq!(dst_cid, dst_cid);
                assert_eq!(src_cid, src_cid);
                assert_eq!(token, Some(Token(vec![1, 2])));
                assert_eq!(length, 0);
                assert!(matches!(packet_number, PacketNumber::U8(x) if x == 2));
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
