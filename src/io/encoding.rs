use bytes::{Buf, BufMut};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VarIntError {
    #[error("Invalid frame for different reasons")]
    InvalidFrame,
    #[error("Not enough data in the buffer when parsing")]
    NotEnoughData,
    #[error("Variable length is too large")]
    InvalidVariableLength,
}

pub fn variable_length_decode<B: Buf>(buf: &mut B) -> Result<u64, VarIntError> {
    if !buf.has_remaining() {
        return Err(VarIntError::NotEnoughData);
    }
    let v = buf.get_u8();
    let prefix = v >> 6;
    let len = 1 << prefix;
    if buf.remaining() + 1 < len {
        return Err(VarIntError::NotEnoughData);
    }

    let mut data: u64 = v as u64 & 0x3f;

    for _ in 0..len - 1 {
        data = (data << 8) + buf.get_u8() as u64;
    }

    Ok(data)
}

pub fn variable_length_encode<B: BufMut>(buf: &mut B, data: u64) -> Result<(), VarIntError> {
    if data < 63 {
        buf.put_u8(data as u8);
    } else if data < 16383 {
        buf.put_u16((0b01 << 14) + data as u16);
    } else if data < 1073741823 {
        buf.put_u32((0b10 << 30) + data as u32);
    } else if data < 4611686018427387903 {
        buf.put_u64((0b11 << 62) + data);
    } else {
        return Err(VarIntError::InvalidVariableLength); // TODO: find better error
    }
    Ok(())
}

pub fn variable_length(data: u64) -> usize {
    if data < 63 {
        1
    } else if data < 16383 {
        2
    } else if data < 1073741823 {
        3
    } else if data < 4611686018427387903 {
        4
    } else {
        0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VarInt(pub u64);

impl VarInt {
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, VarIntError> {
        Ok(VarInt(variable_length_decode(buf)?))
    }
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), VarIntError> {
        variable_length_encode(buf, self.0)
    }
    pub fn length(&self) -> usize {
        variable_length(self.0)
    }
}

impl PartialEq<u64> for VarInt {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;
    #[test]
    fn test_variable_length_decode() -> Result<(), VarIntError> {
        let data = [0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c];
        let output = variable_length_decode(&mut Cursor::new(data))?;
        assert_eq!(output, 151_288_809_941_952_652);

        let data = [0x9d, 0x7f, 0x3e, 0x7d];
        let output = variable_length_decode(&mut Cursor::new(data))?;
        assert_eq!(output, 494_878_333);

        let data = [0x7b, 0xbd];
        let output = variable_length_decode(&mut Cursor::new(data))?;
        assert_eq!(output, 15_293);

        let data = [0x25];
        let output = variable_length_decode(&mut Cursor::new(data))?;
        assert_eq!(output, 37);

        Ok(())
    }

    #[test]
    fn test_variable_length_encode() -> Result<(), VarIntError> {
        let mut out: Vec<u8> = Vec::new();
        variable_length_encode(&mut out, 151_288_809_941_952_652)?;
        assert_eq!(out, vec![0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c]);

        let mut out: Vec<u8> = Vec::new();
        variable_length_encode(&mut out, 494_878_333)?;
        assert_eq!(out, vec![0x9d, 0x7f, 0x3e, 0x7d]);

        let mut out: Vec<u8> = Vec::new();
        variable_length_encode(&mut out, 15_293)?;
        assert_eq!(out, vec![0x7b, 0xbd]);

        let mut out: Vec<u8> = Vec::new();
        variable_length_encode(&mut out, 37)?;
        assert_eq!(out, vec![0x25]);

        Ok(())
    }
}
