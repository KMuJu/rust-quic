use std::{fmt::Debug, io::Cursor};

use bytes::{Buf, BufMut, Bytes, TryGetError};
use thiserror::Error;

use crate::io::{
    connection::ConnectionID,
    crypto::CryptoFrame,
    encoding::{VarInt, VarIntError, variable_length_decode, variable_length_encode},
    frame::{Frame, FrameIterator},
    space::PacketSpace,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Token(pub Vec<u8>);

#[derive(Clone)]
pub enum Header {
    Initial {
        version: u32,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        token: Option<Token>,
        length: VarInt,
        packet_number: PacketNumber,
    },
    Handshake {
        version: u32,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        length: VarInt,
        packet_number: PacketNumber,
    },
    Short {
        spin: bool,
        key_phase: bool,
        dst_cid: ConnectionID,
        packet_number: PacketNumber,
    },
}

pub(super) struct Packet {
    pub header: Header,
    pub frames: Vec<Frame>,
}

impl Packet {
    pub fn decode<B: Buf>(buf: &mut B, cid_len: u8) -> Result<Self, PacketError> {
        let header = Header::decode(buf, cid_len)?;
        let payload: Bytes = buf.copy_to_bytes(buf.remaining());
        let frames = FrameIterator::new(payload).collect();

        Ok(Self { header, frames })
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), PacketError> {
        self.header.encode(buf)?;
        self.frames.iter().for_each(|f| f.encode(buf).unwrap());

        Ok(())
    }

    pub fn dst_cid(&self) -> ConnectionID {
        match self.header {
            Header::Initial { dst_cid, .. } => dst_cid,
            Header::Handshake { dst_cid, .. } => dst_cid,
            Header::Short { dst_cid, .. } => dst_cid,
        }
    }

    pub fn src_cid(&self) -> Option<ConnectionID> {
        match self.header {
            Header::Initial { src_cid, .. } => Some(src_cid),
            Header::Handshake { src_cid, .. } => Some(src_cid),
            _ => None,
        }
    }

    pub fn packet_number(&self) -> &PacketNumber {
        match &self.header {
            Header::Initial { packet_number, .. } => packet_number,
            Header::Handshake { packet_number, .. } => packet_number,
            Header::Short { packet_number, .. } => packet_number,
        }
    }

    pub fn is_initial(&self) -> bool {
        matches!(self.header, Header::Initial { .. })
    }

    pub fn is_client_hello(&self) -> bool {
        if !matches!(self.header, Header::Initial { .. }) {
            return false;
        }
        self.frames.iter().any(|f| {
            let Frame::Crypto { data, .. } = f else {
                return false;
            };
            let mut data = Cursor::new(data);
            let c_frame = CryptoFrame::decode(&mut data);
            matches!(c_frame, CryptoFrame::ClientHello)
        })
    }

    /// Is not ACK, PADDING or CONNECTION_CLOSE
    pub fn is_ack_eliciting(&self) -> bool {
        self.frames.iter().any(|f| f.is_eliciting())
    }

    pub fn is_in_flight(&self) -> bool {
        self.frames.iter().any(|f| !matches!(f, Frame::Ack { .. }))
    }

    pub(super) fn packet_space(&self) -> PacketSpace {
        match &self.header {
            Header::Initial { .. } => PacketSpace::Initial,
            Header::Handshake { .. } => PacketSpace::Handshake,
            Header::Short { .. } => PacketSpace::Data,
        }
    }
}

impl Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Header::{}",
            match self {
                Header::Initial { .. } => "Initial",
                Header::Handshake { .. } => "Handshake",
                Header::Short { .. } => "Short",
            }
        ))
    }
}

impl Debug for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Packet")
            .field("Header", &self.header)
            .field("Packet Number", &self.packet_number())
            .field("Frames", &self.frames)
            .finish()
    }
}

#[derive(Clone)]
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

    pub(super) fn u32(&self) -> u32 {
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
    #[error("VarInt error: {0}")]
    VarInt(#[from] VarIntError),
    #[error("Not enough data in buffer error: {0}")]
    NotEnoughData(#[from] TryGetError),
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
                    let mut v = vec![0u8; token_len as usize];
                    buf.copy_to_slice(&mut v);
                    Some(Token(v))
                } else {
                    None
                };
                let length = VarInt::decode(buf)?;
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
                let length = VarInt::decode(buf)?;
                let packet_number = PacketNumber::decode(buf, packet_number_len);
                Ok(Self::Handshake {
                    version,
                    dst_cid,
                    src_cid,
                    length,
                    packet_number,
                })
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
                        variable_length_encode(buf, t.0.len() as u64)?;
                        buf.put_slice(&t.0);
                    }
                    None => variable_length_encode(buf, 0)?,
                };
                length.encode(buf)?;
                packet_number.encode(buf);
            }
            Header::Handshake {
                version,
                dst_cid,
                src_cid,
                length,
                packet_number,
            } => {
                let first: u8 = 0b1110_0000 + (packet_number.len() & 0b11);
                buf.put_u8(first);
                buf.put_u32(*version);
                buf.put_u8(dst_cid.len);
                buf.put_slice(&dst_cid.buf[..dst_cid.len as usize]);
                buf.put_u8(src_cid.len);
                buf.put_slice(&src_cid.buf[..src_cid.len as usize]);
                length.encode(buf)?;
                packet_number.encode(buf);
            }
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
    use std::io::{self, Cursor};

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
            length: VarInt(0),
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
                assert_eq!(length.0, 0);
                assert!(matches!(packet_number, PacketNumber::U8(x) if x == 2));
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    #[test]
    fn test_is_client_hello() {
        let crypto_data = CryptoFrame::ClientHello;
        let mut c_data = Vec::new();
        crypto_data.encode(&mut c_data);
        let crypto_frame = Frame::Crypto {
            offset: VarInt(0),
            data: c_data,
        };

        let payload = vec![crypto_frame];

        let packet = Packet {
            header: Header::Initial {
                version: 1,
                dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
                src_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1]), 1),
                length: VarInt(1), // TODO: Handle length
                token: None,
                packet_number: PacketNumber::from(1),
            },
            frames: payload,
        };

        assert!(packet.is_client_hello());
    }
}
