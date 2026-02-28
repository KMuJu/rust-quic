use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::poll_fn,
    io::Cursor,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use futures::task::AtomicWaker;
use rand::{Rng, rngs::StdRng};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::io::{
    ConnectionToEndpoint, EndpointToConnection,
    endpoint::EndpointRef,
    frame::{Frame, FrameIterator},
    packet::{Header, Packet, PacketNumber},
    stream::{Dir, RecvStream, RecvStreamHandle, SendStream, SendStreamHandle, Side, StreamID},
};

pub struct ConnectionInner {
    id: ConnectionID,
    // Peer's cid
    peer_cid: ConnectionID,
    addr: SocketAddr,
    side: Side,
    recv: HashMap<StreamID, RecvStream>,
    send: HashMap<StreamID, SendStream>,

    // Streams that can be accepted
    bidir_acceptable: VecDeque<StreamID>,
    bidir_accept_waker: AtomicWaker,
    unidir_acceptable: VecDeque<StreamID>,
    unidir_accept_waker: AtomicWaker,

    // Streams that have data to send
    pending_recv: VecDeque<StreamID>,
    pending_send: VecDeque<StreamID>,

    // Packets to be sent by endpoint
    // Handshake stuff i guess
    pending_packets: VecDeque<Packet>,

    write_waker: AtomicWaker,

    incoming_data: UnboundedReceiver<EndpointToConnection>,
    outgoing_data: UnboundedSender<ConnectionToEndpoint>,

    packet_number: usize,
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
    pub fn handle_datagram(&mut self, buf: &[u8]) {
        let packet = match Packet::decode(&mut Cursor::new(buf), self.id.len) {
            Ok(p) => p,
            Err(e) => {
                println!("Error when decoding datagram: {e}");
                return;
            }
        };
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
                self.bidir_accept_waker.wake();
            }
            Dir::Uni => {
                self.recv.insert(id.clone(), RecvStream::new(id.clone()));
                self.unidir_acceptable.push_back(id.clone());
                self.unidir_accept_waker.wake();
            }
        }
    }

    pub(super) fn get_packet(&mut self, mut mtu: usize) -> Option<Packet> {
        let orig_mtu = mtu;
        if let Some(packet) = self.pending_packets.pop_front() {
            return Some(packet);
        }
        let cid = self.peer_cid;

        let mut frames = Vec::new();
        while let Some(id) = self.pending_send.pop_front() {
            let Some(send) = self.send.get_mut(&id) else {
                continue;
            };
            let offset = send.offset();
            let mut data = BytesMut::with_capacity(mtu);
            let _ = send.read_n(&mut data, mtu);
            let fin = send.is_finished();

            let frame = Frame::Stream {
                id: id.clone(),
                fin,
                last: false,
                offset: offset as usize,
                data: data.freeze(),
            };

            if !send.empty() {
                self.pending_send.push_back(id);
            }

            // TODO: Fix, this is stupid
            mtu = mtu.saturating_sub(frame.size());

            frames.push(frame);
            if mtu == 0 {
                break;
            }
        }

        if frames.is_empty() {
            return None;
        }

        let last = frames.last_mut().unwrap();
        match last {
            Frame::Stream { fin, .. } => *fin = true,
            _ => (),
        };

        let mut payload = BytesMut::with_capacity(orig_mtu);
        frames.iter().for_each(|f| f.encode(&mut payload).unwrap());
        let packet = Packet {
            header: Header::Short {
                spin: false,
                key_phase: false,
                dst_cid: cid,
                packet_number: PacketNumber::from(self.packet_number),
            },
            payload,
        };
        self.packet_number += 1;
        Some(packet)
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

#[derive(Debug)]
pub(super) struct ConnectionHandle {
    pub(super) addr: SocketAddr,
    pub(super) incoming_data: UnboundedSender<EndpointToConnection>,
}

#[derive(Clone)]
pub struct Connection {
    inner: Arc<Mutex<ConnectionInner>>,
}

impl Connection {
    pub(super) fn new(
        cid: ConnectionID,
        peer_cid: ConnectionID,
        addr: SocketAddr,
        side: Side,
        incoming_data: UnboundedReceiver<EndpointToConnection>,
        outgoing_data: UnboundedSender<ConnectionToEndpoint>,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ConnectionInner {
            id: cid,
            peer_cid,
            addr,
            side,
            recv: HashMap::new(),
            send: HashMap::new(),
            bidir_acceptable: VecDeque::new(),
            bidir_accept_waker: AtomicWaker::new(),
            unidir_acceptable: VecDeque::new(),
            unidir_accept_waker: AtomicWaker::new(),
            pending_recv: VecDeque::new(),
            pending_send: VecDeque::new(),
            pending_packets: VecDeque::new(),
            write_waker: AtomicWaker::new(),
            incoming_data,
            outgoing_data,
            packet_number: 0,
        }));
        let driver = ConnectionDriver {
            inner: inner.clone(),
        };

        tokio::spawn(driver);
        Connection { inner }
    }

    pub async fn accept_bidir(&self) -> (SendStreamHandle, RecvStreamHandle) {
        poll_fn(|cx| {
            let mut inner = self.inner.lock().unwrap();
            if let Some(id) = inner.bidir_acceptable.pop_front() {
                Poll::Ready((
                    SendStreamHandle::new(id.clone(), self.inner.clone()),
                    RecvStreamHandle::new(id, self.inner.clone()),
                ))
            } else {
                inner.bidir_accept_waker.register(cx.waker());
                Poll::Pending
            }
        })
        .await
    }

    pub async fn accept_uni(&self) -> RecvStreamHandle {
        poll_fn(|cx| {
            let mut inner = self.inner.lock().unwrap();
            if let Some(id) = inner.bidir_acceptable.pop_front() {
                Poll::Ready(RecvStreamHandle::new(id, self.inner.clone()))
            } else {
                inner.bidir_accept_waker.register(cx.waker());
                Poll::Pending
            }
        })
        .await
    }

    pub fn open_bidir(&self) -> (RecvStreamHandle, SendStreamHandle) {
        let mut inner = self.inner.lock().unwrap();
        let id = StreamID::new(1, &inner.side, Dir::Bidir); // TODO: Handle :)
        let recv = RecvStream::new(id.clone());
        let send = SendStream::new(id.clone());

        inner.recv.insert(id.clone(), recv);
        inner.send.insert(id.clone(), send);

        let recv_handle = RecvStreamHandle::new(id.clone(), self.inner.clone());
        let send_handle = SendStreamHandle::new(id.clone(), self.inner.clone());

        (recv_handle, send_handle)
    }

    // Send initial packet to peer
    // TODO: Start handshake
    pub(super) fn init(&self) {
        let mut inner = self.inner.lock().unwrap();
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
        let inner = self.inner.lock().unwrap();
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
        let endpoint = self.endpoint.state.lock().unwrap();

        let connection = endpoint.connections.get(&self.inner.id).unwrap();
        Poll::Ready(connection.clone())
    }
}

// Handles reeiving data from the endpoint and passing it to inner and seeing if pending data
// exists
struct ConnectionDriver {
    inner: Arc<Mutex<ConnectionInner>>,
}

impl Future for ConnectionDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.inner.lock().unwrap();
        conn.write_waker.register(cx.waker());

        while let Poll::Ready(data) = conn.incoming_data.poll_recv(cx) {
            let data = data.unwrap();
            conn.handle_datagram(&data.data);
        }

        while !conn.pending_send.is_empty() {
            let Some(packet) = conn.get_packet(1200) else {
                continue;
            };

            let mut buf = BytesMut::new();
            packet.encode(&mut buf).unwrap(); // WARN: Handle

            let to_endpoint = ConnectionToEndpoint {
                data: buf,
                addr: conn.addr,
            };
            conn.outgoing_data.send(to_endpoint).unwrap(); // WARN: Handle
        }

        Poll::Pending
    }
}
