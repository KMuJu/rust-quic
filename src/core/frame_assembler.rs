use std::{
    cmp::min,
    collections::{BTreeMap, VecDeque},
    io::{self},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, ReadBuf};

// Will handle buffering out of order frames
pub struct FrameAssembler {
    read_pos: u64,
    buffer: BTreeMap<u64, Bytes>,
    readable: VecDeque<Bytes>,
    waker: Option<Waker>,
}

impl FrameAssembler {
    pub fn new() -> Self {
        Self {
            read_pos: 0,
            buffer: BTreeMap::new(),
            readable: VecDeque::new(),
            waker: None,
        }
    }

    pub fn add(&mut self, mut data: Bytes, offset: u64) {
        // println!("State: [pos: {}]", self.read_pos);
        // println!("Adding data: {data:?}, {offset}");
        let data_len = data.remaining() as u64;
        if offset + data_len <= self.read_pos {
            return;
        }

        if offset == self.read_pos {
            self.readable.push_back(data);
            self.read_pos += data_len;
            self.handle_domino_effect();
        } else if offset < self.read_pos && offset + data_len > self.read_pos {
            // println!("Some duplicate data: {data:?}");
            data.advance((self.read_pos - offset) as usize);
            // println!("Advanced: {data:?}");
            let data_len = data.remaining() as u64;
            self.readable.push_back(data);
            self.read_pos += data_len;
            self.handle_domino_effect();
        } else {
            self.buffer.insert(offset, data);
        }

        if !self.readable.is_empty() {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }

        // println!("Buffer: {:?}", self.buffer);
        // println!("Readable: {:?}", self.readable);
    }

    fn handle_domino_effect(&mut self) {
        while let Some(entry) = self.buffer.first_entry() {
            let chunk_offset = *entry.key();
            if chunk_offset <= self.read_pos {
                let mut chunk = entry.remove();
                let chunk_len = chunk.len() as u64;

                if chunk_offset + chunk_len > self.read_pos {
                    let advance = (chunk_offset - self.read_pos) as usize;
                    chunk.advance(advance);
                    let chunk_len = chunk.remaining();
                    self.readable.push_back(chunk);
                    self.read_pos += chunk_len as u64;
                }
            } else {
                break;
            }
        }
    }

    fn try_fill_readable(&mut self) {
        if let Some(chunk) = self.buffer.remove(&self.read_pos) {
            self.read_pos += chunk.remaining() as u64;
            self.readable.push_back(chunk);
            self.handle_domino_effect();
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}

impl Default for FrameAssembler {
    fn default() -> Self {
        Self::new()
    }
}

impl io::Read for FrameAssembler {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.readable.is_empty() {
            self.try_fill_readable();
        }

        if self.readable.is_empty() {
            return Ok(0); // This signals "WouldBlock" or EOF depending on context
        }

        let mut sum = 0;
        while let Some(mut chunk) = self.readable.pop_front() {
            let amt = min(buf.len() - sum, chunk.remaining());
            if amt == 0 {
                break;
            }
            chunk.copy_to_slice(&mut buf[sum..sum + amt]);
            sum += amt;
        }

        Ok(sum)
    }
}

impl AsyncRead for FrameAssembler {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.readable.is_empty() {
            this.try_fill_readable();
        }

        if this.readable.is_empty() {
            this.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        // TODO: Is possible that the whole chunk is not read into the buf
        while let Some(chunk) = this.readable.pop_front() {
            let amt = min(buf.remaining(), chunk.remaining());
            if amt == 0 {
                break;
            }
            buf.put_slice(&chunk[..amt]);
        }

        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn test_assembler_read_full() -> io::Result<()> {
        let a_s: Vec<u8> = vec![1, 2, 3];
        let b_s: Vec<u8> = vec![4, 5, 6];
        let c_s: Vec<u8> = vec![7, 8, 9];
        let a = Bytes::from(a_s);
        let b = Bytes::from(b_s);
        let c = Bytes::from(c_s);
        let mut assempler = FrameAssembler::new();

        assempler.add(a, 0);
        assempler.add(b, 3);
        assempler.add(c, 6);

        let mut out = vec![0u8; 10];
        let n = assempler.read(&mut out)?;
        assert_eq!(n, 9);
        assert_eq!(out[..n], vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        Ok(())
    }

    #[test]
    fn test_assembler_read_out_of_order() -> io::Result<()> {
        let a_s: Vec<u8> = vec![1, 2, 3];
        let b_s: Vec<u8> = vec![4, 5, 6];
        let c_s: Vec<u8> = vec![7, 8, 9];
        let a = Bytes::from(a_s);
        let b = Bytes::from(b_s);
        let c = Bytes::from(c_s);
        let mut assempler = FrameAssembler::new();

        assempler.add(a, 0);
        assempler.add(c, 6);
        assempler.add(b, 3);

        let mut out = vec![0u8; 10];
        let n = assempler.read(&mut out)?;
        assert_eq!(n, 9);
        assert_eq!(out[..n], vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        Ok(())
    }

    #[test]
    fn test_assembler_buffering() -> io::Result<()> {
        let a_s: Vec<u8> = vec![1, 2, 3];
        let b_s: Vec<u8> = vec![4, 5, 6];
        let c_s: Vec<u8> = vec![7, 8, 9];
        let a = Bytes::from(a_s);
        let b = Bytes::from(b_s);
        let c = Bytes::from(c_s);
        let mut assempler = FrameAssembler::new();

        assempler.add(a, 0);
        assempler.add(c, 6);

        let mut out = vec![0u8; 10];
        let n = assempler.read(&mut out)?;
        assert_eq!(n, 3);
        assert_eq!(out[..n], vec![1, 2, 3]);

        assempler.add(b, 3);

        let mut out = vec![0u8; 10];
        let n = assempler.read(&mut out)?;
        assert_eq!(n, 6);
        assert_eq!(out[..n], vec![4, 5, 6, 7, 8, 9]);

        Ok(())
    }

    #[test]
    fn test_assembler_read_overlap_correct_order() -> io::Result<()> {
        let a_s: Vec<u8> = vec![1, 2, 3, 4];
        let b_s: Vec<u8> = vec![4, 5, 6];
        let c_s: Vec<u8> = vec![7, 8, 9];
        let a = Bytes::from(a_s);
        let b = Bytes::from(b_s);
        let c = Bytes::from(c_s);
        let mut assempler = FrameAssembler::new();

        assempler.add(a, 0);
        assempler.add(b, 3);
        assempler.add(c, 6);

        let mut out = vec![0u8; 10];
        let n = assempler.read(&mut out)?;
        assert_eq!(n, 9);
        assert_eq!(out[..n], vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        Ok(())
    }
}
