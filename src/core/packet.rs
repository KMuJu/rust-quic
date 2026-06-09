use std::{fmt::Debug, io};

use bytes::{Buf, BufMut, BytesMut, TryGetError};
use thiserror::Error;

use crate::core::{
    connection::ConnectionID,
    crypto,
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

pub struct Packet {
    pub header: Header,
    pub(crate) frames: Vec<Frame>,
}

impl Packet {
    pub fn size(&self) -> usize {
        self.header.size() + self.frames.iter().map(|f| f.size()).sum::<usize>()
    }

    pub fn dst_cid(&self) -> ConnectionID {
        self.header.dst_cid()
    }

    pub fn src_cid(&self) -> Option<ConnectionID> {
        self.header.src_cid()
    }

    pub fn packet_number(&self) -> &PacketNumber {
        match &self.header {
            Header::Initial { packet_number, .. } => packet_number,
            Header::Handshake { packet_number, .. } => packet_number,
            Header::Short { packet_number, .. } => packet_number,
        }
    }

    pub fn is_initial(&self) -> bool {
        self.header.is_initial()
    }

    /// Is not ACK, PADDING or CONNECTION_CLOSE
    pub fn is_ack_eliciting(&self) -> bool {
        self.frames.iter().any(|f| f.is_eliciting())
    }

    pub fn is_in_flight(&self) -> bool {
        self.frames.iter().any(|f| !matches!(f, Frame::Ack { .. }))
    }

    pub(crate) fn packet_space(&self) -> PacketSpace {
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
    pub(super) fn decode<B: Buf>(buf: &mut B, len: u8) -> Self {
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

    pub(super) fn len(&self) -> u8 {
        match self {
            PacketNumber::U8(_) => 1_u8,
            PacketNumber::U16(_) => 2_u8,
            PacketNumber::U24(_) => 3_u8,
            PacketNumber::U32(_) => 4_u8,
        }
    }

    pub(super) fn u32(&self) -> u32 {
        match self {
            PacketNumber::U8(x) => u32::from(*x),
            PacketNumber::U16(x) => u32::from(*x),
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
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), PacketError> {
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

    pub(super) fn size(&self) -> usize {
        match self {
            Header::Initial {
                dst_cid,
                src_cid,
                token,
                length,
                packet_number,
                ..
            } => {
                1 + 4
                    + 1
                    + dst_cid.len as usize
                    + 1
                    + src_cid.len as usize
                    + match token {
                        Some(t) => VarInt(t.0.len() as u64).length() + t.0.len(),
                        None => 1,
                    }
                    + length.length()
                    + packet_number.len() as usize
            }
            Header::Handshake {
                dst_cid,
                src_cid,
                length,
                packet_number,
                ..
            } => {
                1 + 4
                    + 1
                    + dst_cid.len as usize
                    + 1
                    + src_cid.len as usize
                    + length.length()
                    + packet_number.len() as usize
            }
            Header::Short {
                dst_cid,
                packet_number,
                ..
            } => 1 + dst_cid.len as usize + packet_number.len() as usize,
        }
    }

    pub(super) fn packet_space(&self) -> PacketSpace {
        match self {
            Header::Initial { .. } => PacketSpace::Initial,
            Header::Handshake { .. } => PacketSpace::Handshake,
            Header::Short { .. } => PacketSpace::Data,
        }
    }
    pub fn dst_cid(&self) -> ConnectionID {
        match self {
            Header::Initial { dst_cid, .. } => *dst_cid,
            Header::Handshake { dst_cid, .. } => *dst_cid,
            Header::Short { dst_cid, .. } => *dst_cid,
        }
    }

    pub fn src_cid(&self) -> Option<ConnectionID> {
        match self {
            Header::Initial { src_cid, .. } => Some(*src_cid),
            Header::Handshake { src_cid, .. } => Some(*src_cid),
            _ => None,
        }
    }

    pub fn is_initial(&self) -> bool {
        matches!(self, Header::Initial { .. })
    }
}

#[derive(Debug)]
pub enum ProtectedHeader {
    Initial {
        version: u32,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        token: Option<Token>,
        length: VarInt,
    },
    Handshake {
        version: u32,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        length: VarInt,
    },
    Short {
        spin: bool,
        key_phase: bool,
        dst_cid: ConnectionID,
    },
}

pub struct ProtectedPacket {
    header: ProtectedHeader,
    /// The rest of the packet, can be multiple packets
    bytes: io::Cursor<BytesMut>,
}

impl ProtectedHeader {
    pub fn decode<B: Buf>(buf: &mut B, cid_len: u8) -> Result<Self, PacketError> {
        let first = buf.try_get_u8()?;
        match (first >> 7, first >> 6 & 1, first >> 5 & 1, first >> 4 & 1) {
            (1, 1, 0, 0) => {
                // TODO: Handle panics
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
                Ok(Self::Initial {
                    version,
                    dst_cid,
                    src_cid,
                    token,
                    length,
                })
            }
            (1, 1, 0, 1) => {
                // TODO: 0-RTT
                todo!()
            }
            (1, 1, 1, 0) => {
                // TODO: Handshake
                // TODO: Handle panics
                let version = buf.try_get_u32()?;
                let dst_cid_len = buf.try_get_u8()?;
                let dst_cid = ConnectionID::from_buf(buf, dst_cid_len);
                let src_cid_len = buf.try_get_u8()?;
                let src_cid = ConnectionID::from_buf(buf, src_cid_len);
                if dst_cid_len > 20 || src_cid_len > 20 {
                    return Err(PacketError::InvalidHeader);
                }
                let length = VarInt::decode(buf)?;
                Ok(Self::Handshake {
                    version,
                    dst_cid,
                    src_cid,
                    length,
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

                Ok(Self::Short {
                    spin: first & 0x20 == 0x20,
                    key_phase: first & 0x04 == 0x04,
                    dst_cid,
                })
            }
            _ => {
                println!("Invalid first four bits");
                Err(PacketError::InvalidHeader)
            }
        }
    }

    pub fn dst_cid(&self) -> ConnectionID {
        match self {
            Self::Initial { dst_cid, .. } => *dst_cid,
            Self::Handshake { dst_cid, .. } => *dst_cid,
            Self::Short { dst_cid, .. } => *dst_cid,
        }
    }

    pub fn src_cid(&self) -> Option<ConnectionID> {
        match self {
            Self::Initial { src_cid, .. } => Some(*src_cid),
            Self::Handshake { src_cid, .. } => Some(*src_cid),
            _ => None,
        }
    }

    pub fn is_initial(&self) -> bool {
        matches!(self, Self::Initial { .. })
    }
}

impl ProtectedPacket {
    pub fn new(data: BytesMut, cid_len: u8) -> Result<Self, PacketError> {
        let mut header_data = io::Cursor::new(data);
        Ok(Self {
            header: ProtectedHeader::decode(&mut header_data, cid_len)?,
            bytes: header_data,
        })
    }

    /// Decrypts packet
    // TODO: Handle multiple packets in one datagram
    pub fn unprotect(mut self, keys: &rustls::quic::Keys) -> Result<Packet, PacketError> {
        let pn = crypto::decrypt_header(keys, &mut self.bytes);
        let packet_number = pn.u32() as u64;

        // Only new information is the packet number
        // So I don't need to parse the header again
        let header = match self.header {
            ProtectedHeader::Initial {
                version,
                dst_cid,
                src_cid,
                token,
                length,
            } => Header::Initial {
                version,
                dst_cid,
                src_cid,
                token,
                length,
                packet_number: pn,
            },
            ProtectedHeader::Handshake {
                version,
                dst_cid,
                src_cid,
                length,
            } => Header::Handshake {
                version,
                dst_cid,
                src_cid,
                length,
                packet_number: pn,
            },
            ProtectedHeader::Short {
                spin,
                key_phase,
                dst_cid,
            } => Header::Short {
                spin,
                key_phase,
                dst_cid,
                packet_number: pn,
            },
        };

        let header_len = self.bytes.position(); // Is after the packet number
        let mut bytes = self.bytes.into_inner();
        let header_data = bytes.split_to(header_len as usize);
        let mut payload = bytes;

        let len = keys
            .remote
            .packet
            .decrypt_in_place(packet_number, &header_data, &mut payload)
            .unwrap()
            .len();

        payload.truncate(len);

        let frames = FrameIterator::new(payload.freeze()).collect();

        Ok(Packet { header, frames })
    }

    pub fn is_initial(&self) -> bool {
        self.header.is_initial()
    }

    pub fn dst_cid(&self) -> ConnectionID {
        self.header.dst_cid()
    }

    pub fn src_cid(&self) -> Option<ConnectionID> {
        self.header.src_cid()
    }

    pub(super) fn packet_space(&self) -> PacketSpace {
        match self.header {
            ProtectedHeader::Initial { .. } => PacketSpace::Initial,
            ProtectedHeader::Handshake { .. } => PacketSpace::Handshake,
            ProtectedHeader::Short { .. } => PacketSpace::Data,
        }
    }
}

impl Debug for ProtectedPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtectedPacket")
            .field("header", &self.header)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::io::{self, Cursor};

    use bytes::Bytes;

    use crate::core::{
        frame::Stream,
        stream::{Dir, StreamID},
    };

    use super::*;

    #[test]
    fn initial_header_encode() -> Result<(), PacketError> {
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

        let expected: Vec<u8> = vec![
            0b11_00_00_01, // First byte
            0,             // version
            0,             // version
            0,             // version
            1,             // version
            1,             // dst_cid
            1,             // dst_cid
            1,             // src_cid
            1,             // src_cid
            2,             // Token
            1,             // Token
            2,             // Token
            0,             // Length
            2,             // Packet number
        ];

        assert_eq!(bytes, expected);

        Ok(())
    }

    #[test]
    fn test_initial_size() {
        let header = Header::Initial {
            version: 1,
            dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
            src_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1]), 1),
            length: VarInt(1),
            token: None,
            packet_number: PacketNumber::from(1),
        };

        let mut encoded = Vec::new();
        header
            .encode(&mut encoded)
            .expect("Encoding should not fail");

        assert_eq!(header.size(), encoded.len());
    }

    #[test]
    fn test_handshake_size() {
        let header = Header::Handshake {
            version: 1,
            dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
            src_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1]), 1),
            length: VarInt(1),
            packet_number: PacketNumber::from(1),
        };

        let mut encoded = Vec::new();
        header
            .encode(&mut encoded)
            .expect("Encoding should not fail");

        assert_eq!(header.size(), encoded.len());
    }

    #[test]
    fn test_data_size() {
        let header = Header::Short {
            spin: false,
            key_phase: false,
            dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
            packet_number: PacketNumber::from(1),
        };

        let mut encoded = Vec::new();
        header
            .encode(&mut encoded)
            .expect("Encoding should not fail");

        assert_eq!(header.size(), encoded.len());
    }

    #[test]
    fn test_packet_size() {
        let mut c_data = vec![1, 2, 3, 4, 5];
        let crypto_frame = Frame::Crypto {
            offset: VarInt(0),
            data: c_data,
        };

        let stream_frame = Frame::Stream(Stream {
            id: StreamID::new_unchecked(1),
            fin: false,
            len: false,
            offset: VarInt(0),
            data: Bytes::new(),
        });

        let payload = vec![
            crypto_frame,
            stream_frame,
            Frame::Padding,
            Frame::Ping,
            Frame::MaxStreams {
                dir: Dir::Uni,
                max_streams: VarInt(3),
            },
        ];

        let packet = Packet {
            header: Header::Initial {
                version: 1,
                dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
                src_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1]), 1),
                length: VarInt(1),
                token: None,
                packet_number: PacketNumber::from(1),
            },
            frames: payload,
        };

        let mut encoded = Vec::new();
        packet
            .header
            .encode(&mut encoded)
            .expect("Encoding should not fail");

        packet
            .frames
            .iter()
            .for_each(|f| f.encode(&mut encoded).unwrap());

        assert_eq!(packet.size(), encoded.len());
    }
}
