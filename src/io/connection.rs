use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use bytes::{Buf, Bytes, BytesMut};
use futures::task::AtomicWaker;
use rand::{Rng, rngs::StdRng};

use crate::io::{
    endpoint::EndpointRef,
    frame::{Frame, FrameIterator},
    packet::{Header, Packet, PacketNumber},
    stream::{Dir, RecvStream, RecvStreamHandle, SendStream, SendStreamHandle, Side, StreamID},
};

pub struct ConnectionInner {
    id: ConnectionID,
    // Peer's cid
    peer_cid: ConnectionID,
    side: Side,
    recv: HashMap<StreamID, RecvStream>,
    send: HashMap<StreamID, SendStream>,

    // Streams that can be accepted
    bidir_acceptable: VecDeque<StreamID>,
    bidir_accept_waker: Option<Waker>,
    unidir_acceptable: VecDeque<StreamID>,
    unidir_accept_waker: Option<Waker>,

    // Streams that have data to send
    pending_recv: VecDeque<StreamID>,
    pending_send: VecDeque<StreamID>,

    // Packets to be sent by endpoint
    // Handshake stuff i guess
    pending_packets: VecDeque<Packet>,

    write_waker: Arc<AtomicWaker>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionID {
    pub len: u8,
    pub buf: [u8; 20],
}

impl ConnectionID {
    pub fn from_buf<B: Buf>(buf: &mut B, len: u8) -> Self {
        let mut id = Self {
            len,
            buf: [0u8; 20],
        };
        // TODO: Potential bug
        buf.copy_to_slice(&mut id.buf[..buf.remaining().min(len as usize)]);
        id
    }

    pub fn new(len: u8, rng: &mut StdRng) -> Self {
        let mut id = Self {
            len,
            buf: [0u8; 20],
        };
        rng.fill_bytes(&mut id.buf[..len as usize]);
        id
    }
}

impl Debug for ConnectionID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ConnectionID")
            .field(&&self.buf[..self.len as usize])
            .finish()
    }
}

impl ConnectionInner {
    pub fn handle_datagram(&mut self, packet: Packet) {
        println!("{:?}: Got {:?}", self.id, packet);
        let frameiter = FrameIterator::new(packet.payload.freeze());
        for frame in frameiter {
            self.handle_frame(frame);
        }
    }

    fn handle_frame(&mut self, frame: Frame) {
        match &frame {
            // These frames are sent by the sender
            Frame::StreamDataBlocked { id, .. }
            | Frame::ResetStream { id, .. }
            | Frame::Stream { id, .. } => {
                if !self.recv.contains_key(id) {
                    self.handle_new_stream(id);
                }

                let recv = self
                    .recv
                    .entry(id.clone())
                    .or_insert_with(|| RecvStream::new(id.clone()));

                recv.handle_frame(frame);
            }
            // These frames are sent by the receiver
            Frame::MaxStreamData { id, .. } | Frame::StopSending { id, .. } => {
                if !self.send.contains_key(id) {
                    self.handle_new_stream(id);
                }

                let send = self.send.get_mut(id).unwrap();

                send.handle_frame(frame);
            }
            _ => todo!(),
        };
    }

    fn handle_new_stream(&mut self, id: &StreamID) {
        // Handle new frame
        match id.dir() {
            Dir::Bidir => {
                self.send.insert(id.clone(), SendStream::new(id.clone()));
                self.recv.insert(id.clone(), RecvStream::new(id.clone()));
                self.bidir_acceptable.push_back(id.clone());
                if let Some(waker) = self.bidir_accept_waker.take() {
                    waker.wake();
                }
            }
            Dir::Uni => {
                self.recv.insert(id.clone(), RecvStream::new(id.clone()));
                self.unidir_acceptable.push_back(id.clone());
                if let Some(waker) = self.unidir_accept_waker.take() {
                    waker.wake();
                }
            }
        }
    }

    pub(super) fn mark_pending_on_recv(&mut self, id: StreamID) {
        self.pending_recv.push_back(id);
        self.write_waker.wake();
    }

    pub(super) fn mark_pending_on_send(&mut self, id: StreamID) {
        self.pending_send.push_back(id);
        self.write_waker.wake();
    }

    pub(super) fn recv(&mut self, id: &StreamID) -> Option<&mut RecvStream> {
        self.recv.get_mut(id)
    }

    pub(super) fn send(&mut self, id: &StreamID) -> Option<&mut SendStream> {
        self.send.get_mut(id)
    }
}

#[derive(Clone)]
pub struct Connection(Arc<Mutex<ConnectionInner>>);

impl Connection {
    pub(super) fn new(
        cid: ConnectionID,
        peer_cid: ConnectionID,
        side: Side,
        write_waker: Arc<AtomicWaker>,
    ) -> Self {
        Connection(Arc::new(Mutex::new(ConnectionInner {
            id: cid,
            peer_cid,
            side,
            recv: HashMap::new(),
            send: HashMap::new(),
            bidir_acceptable: VecDeque::new(),
            bidir_accept_waker: None,
            unidir_acceptable: VecDeque::new(),
            unidir_accept_waker: None,
            pending_recv: VecDeque::new(),
            pending_send: VecDeque::new(),
            pending_packets: VecDeque::new(),
            write_waker,
        })))
    }

    pub fn handle_datagram(&mut self, packet: Packet) {
        let mut inner = self.0.lock().unwrap();
        inner.handle_datagram(packet);
    }

    pub async fn accept_bidir(&self) -> (SendStreamHandle, RecvStreamHandle) {
        poll_fn(|cx| {
            let mut inner = self.0.lock().unwrap();
            if let Some(id) = inner.bidir_acceptable.pop_front() {
                Poll::Ready((
                    SendStreamHandle::new(id.clone(), self.0.clone()),
                    RecvStreamHandle::new(id, self.0.clone()),
                ))
            } else {
                inner.bidir_accept_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    pub async fn accept_uni(&self) -> RecvStreamHandle {
        poll_fn(|cx| {
            let mut inner = self.0.lock().unwrap();
            if let Some(id) = inner.bidir_acceptable.pop_front() {
                Poll::Ready(RecvStreamHandle::new(id, self.0.clone()))
            } else {
                inner.bidir_accept_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    pub fn open_bidir(&self) -> (RecvStreamHandle, SendStreamHandle) {
        let id = StreamID(1); // TODO: Handle :)
        let recv = RecvStream::new(id.clone());
        let send = SendStream::new(id.clone());

        let mut inner = self.0.lock().unwrap();
        inner.recv.insert(id.clone(), recv);
        inner.send.insert(id.clone(), send);

        let recv_handle = RecvStreamHandle::new(id.clone(), self.0.clone());
        let send_handle = SendStreamHandle::new(id.clone(), self.0.clone());

        (recv_handle, send_handle)
    }

    pub(super) fn get_packet(&mut self, mut mtu: usize) -> Option<Packet> {
        let orig_mtu = mtu;
        let mut inner = self.0.lock().unwrap();
        if let Some(packet) = inner.pending_packets.pop_front() {
            return Some(packet);
        }
        let cid = inner.peer_cid;

        println!("Pending: {:?}", inner.pending_send);
        let mut frames = Vec::new();
        while let Some(id) = inner.pending_send.pop_front() {
            let Some(send) = inner.send.get_mut(&id) else {
                continue;
            };
            let offset = send.offset();
            let mut data = BytesMut::with_capacity(mtu);
            let _ = send.read_n(&mut data, mtu);

            let frame = Frame::Stream {
                id: id.clone(),
                fin: false,
                last: false,
                offset: offset as usize,
                data: data.freeze(),
            };

            if !send.empty() {
                inner.pending_send.push_back(id);
            }

            // TODO: Fix, this is stupid
            mtu = mtu.saturating_sub(frame.size());

            frames.push(frame);
            if mtu == 0 {
                break;
            }
        }
        drop(inner);

        if frames.is_empty() {
            return None;
        }

        let mut payload = BytesMut::with_capacity(orig_mtu);
        frames.iter().for_each(|f| f.encode(&mut payload).unwrap());
        let packet = Packet {
            header: Header::Short {
                spin: false,
                key_phase: false,
                dst_cid: cid,
                packet_number: super::packet::PacketNumber::U8(1), // TODO: Actual packet number
            },
            payload,
        };
        Some(packet)
    }

    // Send initial packet to peer
    // TODO: Start handshake
    pub(super) fn init(&self) {
        let mut inner = self.0.lock().unwrap();
        // HACK: Wtf is this ??? (I dont care for now)
        let packet = Packet {
            header: Header::Initial {
                version: 1,
                dst_cid: inner.peer_cid,
                src_cid: inner.id,
                token: None,
                length: 1,
                packet_number: PacketNumber::U8(1u8),
            },
            payload: BytesMut::new(),
        };
        inner.pending_packets.push_back(packet);
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.lock().unwrap();
        f.write_fmt(format_args!(
            "Connection({:?}, {:?})",
            inner.id, inner.peer_cid
        ))
    }
}

pub struct IncomingInner {
    id: ConnectionID,
    src_cid: ConnectionID,
}

impl IncomingInner {
    pub fn new(id: ConnectionID, src_cid: ConnectionID) -> Self {
        Self { id, src_cid }
    }
}

pub struct Incoming {
    inner: IncomingInner,
    endpoint: EndpointRef,
}

impl Incoming {
    pub fn new(inner: IncomingInner, endpoint: EndpointRef) -> Self {
        Self { inner, endpoint }
    }
}

impl Future for Incoming {
    type Output = Connection;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(endpoint) = self.endpoint.state.try_lock() else {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        };

        let connection = endpoint.connections.get(&self.inner.id).unwrap();
        Poll::Ready(connection.clone())
    }
}
