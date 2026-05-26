use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, ReadBuf};

use crate::{
    core::{
        frame::{Frame, Stream},
        frame_assembler::FrameAssembler,
    },
    io::connection::ReadError,
};

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

impl RecvStream {
    pub(crate) fn new(id: StreamID) -> Self {
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
            Frame::Stream(frame) => {
                self.handle_stream_frame(frame);
            }
            Frame::MaxStreamData { .. } => todo!(),
            Frame::StreamDataBlocked { .. } => todo!(),
            _ => (),
        };
    }

    pub(super) fn handle_stream_frame(&mut self, frame: Stream) {
        // TODO: Handle fin bit
        if matches!(self.state, RecvState::Recv | RecvState::SizeKnown) {
            self.assembler.add(frame.data, frame.offset.0);
            if frame.fin {
                self.state = RecvState::SizeKnown;
            }
        } else {
            eprint!("Received data in wrong state");
        }
    }

    pub(super) fn poll_read(
        &mut self,
        buf: &mut ReadBuf,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, ReadError>> {
        let pinned = Pin::new(&mut self.assembler);
        let before = buf.filled().len();
        let res = pinned
            .poll_read(cx, buf)
            .map(|r| r.map(|_| buf.filled().len() - before))
            .map_err(ReadError::IO);

        res
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

    pub(crate) fn can_send(&self) -> bool {
        matches!(self.state, SendState::Ready | SendState::Send)
    }

    pub(super) fn set_buffer(&mut self, buf: &[u8]) {
        self.buffer.put_slice(buf);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamID(usize);

impl StreamID {
    pub fn new_unchecked(value: usize) -> Self {
        Self(value)
    }

    pub fn new(mut value: usize, side: &Side, dir: Dir) -> Self {
        value <<= 2;
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
