use bytes::{Buf, BufMut};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("Invalid frame for different reasons")]
    InvalidFrame,
    #[error("Not enough data in the buffer when parsing")]
    NotEnoughData,
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("Variable length is too large")]
    InvalidVariableLength,
}

pub fn variable_length_decode<B: Buf>(buf: &mut B) -> Result<usize, DecodeError> {
    if !buf.has_remaining() {
        return Err(DecodeError::NotEnoughData);
    }
    let v = buf.get_u8();
    let prefix = v >> 6;
    let len = 1 << prefix;
    if buf.remaining() + 1 < len {
        return Err(DecodeError::NotEnoughData);
    }

    let mut data: usize = v as usize & 0x3f;

    for _ in 0..len - 1 {
        data = (data << 8) + buf.get_u8() as usize;
    }

    Ok(data)
}

pub fn variable_length_encode<B: BufMut>(buf: &mut B, data: usize) -> Result<(), EncodeError> {
    if data < 63 {
        buf.put_u8(data as u8);
    } else if data < 16383 {
        buf.put_u16((0b01 << 14) + data as u16);
    } else if data < 1073741823 {
        buf.put_u32((0b10 << 30) + data as u32);
    } else if data < 4611686018427387903 {
        buf.put_u64((0b11 << 62) + data as u64);
    } else {
        return Err(EncodeError::InvalidVariableLength); // TODO: find better error
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

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;
    #[test]
    fn test_variable_length_decode() -> Result<(), DecodeError> {
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
    fn test_variable_length_encode() -> Result<(), EncodeError> {
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
