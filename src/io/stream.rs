use std::{
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, ReadBuf};

use crate::io::{connection::ConnectionInner, frame::Frame, frame_assembler::FrameAssembler};

#[derive(Debug, Clone)]
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
    state: RecvState,
}

pub struct SendStream {
    id: StreamID,
    buffer: BytesMut,
    offset: u64,
    state: SendState,
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
            state: RecvState::Recv,
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
                len: _,
                offset,
                data,
            } => {
                // TODO: Handle fin bit
                if matches!(self.state, RecvState::Recv | RecvState::SizeKnown) {
                    self.assembler.add(data, offset.0);
                    if fin {
                        self.state = RecvState::SizeKnown;
                    }
                } else {
                    eprint!("Received data in wrong state");
                }
            }
            Frame::MaxStreamData { .. } => todo!(),
            Frame::StreamDataBlocked { .. } => todo!(),
            _ => (),
        };
    }

    pub(super) fn is_reset(&self) -> bool {
        matches!(self.state, RecvState::ResetRead | RecvState::ResetRecvd)
    }
}

enum RecvState {
    Recv,
    SizeKnown,
    DataRecv,
    DataRead,
    ResetRecvd,
    ResetRead,
}

enum SendState {
    Ready,
    Send,
    DataSent,
    DataRecvd,
    ResetSent,
    ResetRecvd,
}

impl SendStream {
    pub(super) fn new(id: StreamID) -> Self {
        Self {
            id,
            buffer: BytesMut::new(),
            offset: 0,
            state: SendState::Ready,
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

    pub(super) fn is_finished(&self) -> bool {
        matches!(
            self.state,
            // TODO: Should it be these states?
            SendState::DataSent
                | SendState::DataRecvd
                | SendState::ResetSent
                | SendState::ResetRecvd
        )
    }

    pub(super) fn is_reset(&self) -> bool {
        matches!(self.state, SendState::ResetSent | SendState::ResetRecvd)
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

        if !matches!(send.state, SendState::Ready | SendState::Send) {
            return Err(SendError::WrongState);
        }

        send.buffer.put_slice(buf);
        inner.mark_pending_on_send(self.id.clone());

        Ok(buf.len())
    }

    pub fn finish(&mut self) -> Result<(), SendError> {
        let mut inner = self.connection.lock().unwrap();

        let Some(send) = inner.send(&self.id) else {
            // Stream does not exist, this means that something is wrong
            // TODO: Different error?
            return Err(SendError::ClosedStream);
        };

        if !matches!(send.state, SendState::Send) {
            return Err(SendError::WrongState);
        }

        send.state = SendState::DataSent;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamID(usize);

impl StreamID {
    pub fn new_unchecked(value: usize) -> Self {
        Self(value)
    }

    pub fn new(mut value: usize, side: &Side, dir: Dir) -> Self {
        value -= value & 0b11;
        value += match side {
            Side::Client => 0,
            Side::Server => 0b1,
        };
        value += match dir {
            Dir::Bidir => 0,
            Dir::Uni => 0b10,
        };
        StreamID(value)
    }

    pub fn value(&self) -> usize {
        self.0
    }

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
    #[error("Wrong state")]
    WrongState,
}
