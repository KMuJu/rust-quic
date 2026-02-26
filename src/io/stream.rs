use std::{
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use crate::io::{
    connection::{Connection, ConnectionInner},
    frame::Frame,
    frame_assembler::FrameAssembler,
};

#[derive(Debug)]
pub enum Dir {
    Bidir,
    Uni,
}

#[derive(Debug)]
pub enum Side {
    Client,
    Server,
}

pub struct RecvStream {
    id: StreamID,
    assembler: FrameAssembler,
}

pub struct SendStream {
    id: StreamID,
    buffer: BytesMut,
    offset: u64,
}

pub struct SendStreamHandle {
    id: StreamID,
    connection: Arc<Mutex<ConnectionInner>>,
}

pub struct RecvStreamHandle {
    id: StreamID,
    connection: Arc<Mutex<ConnectionInner>>,
}

impl SendStreamHandle {
    pub fn new(id: StreamID, connection: Arc<Mutex<ConnectionInner>>) -> Self {
        Self { id, connection }
    }
}

impl RecvStream {
    pub(super) fn new(id: StreamID) -> Self {
        Self {
            id,
            assembler: FrameAssembler::new(),
        }
    }

    pub(super) fn handle_frame(&mut self, frame: Frame) {
        let Some(id) = frame.stream_id() else {
            unreachable!("Should not be possible");
        };
        if id != self.id {
            unreachable!("Stream id in frame does not match this stream's id");
        }
        match frame {
            Frame::ResetStream { .. } => todo!(),
            Frame::StopSending { .. } => todo!(),
            Frame::Stream {
                id: _,
                fin,
                last: _,
                offset,
                data,
            } => {
                // TODO: Handle fin bit
                self.assembler.add(data, offset as u64);
            }
            Frame::MaxStreamData { .. } => todo!(),
            Frame::StreamDataBlocked { .. } => todo!(),
            _ => (),
        };
    }
}

impl SendStream {
    pub(super) fn new(id: StreamID) -> Self {
        Self {
            id,
            buffer: BytesMut::new(),
            offset: 0,
        }
    }

    pub(super) fn handle_frame(&mut self, frame: Frame) {}

    pub(super) fn read_n(&mut self, buf: &mut BytesMut, size: usize) -> usize {
        let read = self.buffer.len().min(size);
        buf.put_slice(&self.buffer[..read]);
        self.offset += read as u64;
        self.buffer.advance(read);
        read
    }

    pub(super) fn empty(&self) -> bool {
        !self.buffer.has_remaining()
    }

    pub(super) fn offset(&self) -> u64 {
        self.offset
    }
}

impl RecvStreamHandle {
    pub fn new(id: StreamID, connection: Arc<Mutex<ConnectionInner>>) -> Self {
        Self { id, connection }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, ReadError> {
        let mut buf = ReadBuf::new(buf);
        poll_fn(|cx| {
            let before = buf.filled().len();
            let mut conn_inner = self.connection.lock().unwrap();
            let Some(recv) = conn_inner.recv(&self.id) else {
                return Poll::Ready(Err(ReadError::ClosedStream));
            };
            let pinned = Pin::new(&mut recv.assembler);
            let res = pinned
                .poll_read(cx, &mut buf)
                .map(|r| r.map(|_| buf.filled().len() - before))
                .map_err(ReadError::IO);

            res
        })
        .await
    }
}

impl SendStreamHandle {
    pub fn write(&mut self, buf: &[u8]) -> Result<usize, SendError> {
        let mut inner = self.connection.lock().unwrap();

        let Some(send) = inner.send(&self.id) else {
            // Stream does not exist, this means that something is wrong
            // TODO: Different error?
            return Err(SendError::ClosedStream);
        };
        send.buffer.put_slice(buf);
        inner.mark_pending_on_send(self.id.clone());
        println!("Buffered and marked pending");

        Ok(buf.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamID(pub usize);

impl StreamID {
    pub fn dir(&self) -> Dir {
        if self.0 & 0b10 == 0 {
            Dir::Bidir
        } else {
            Dir::Uni
        }
    }

    pub fn side(&self) -> Side {
        if self.0 & 0b1 == 0 {
            Side::Client
        } else {
            Side::Server
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("Closed stream")]
    ClosedStream,
    #[error("IO: {0}")]
    IO(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("Closed stream")]
    ClosedStream,
}
