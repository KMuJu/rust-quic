use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::poll_fn,
    io::Cursor,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};

use bytes::{Buf, BytesMut};
use futures::task::AtomicWaker;
use rand::{Rng, rngs::StdRng};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};

use crate::io::{
    ConnectionToEndpoint, EndpointToConnection,
    crypto::CryptoFrame,
    endpoint::EndpointRef,
    frame::{Frame, FrameIterator},
    packet::{Header, Packet, PacketNumber},
    stream::{Dir, RecvStream, RecvStreamHandle, SendStream, SendStreamHandle, Side, StreamID},
};

use super::encoding::VarInt;

pub struct ConnectionInner {
    id: ConnectionID,
    // Peer's cid
    peer_cid: ConnectionID,
    side: Side,

    connected: Option<oneshot::Sender<()>>,
    state: ConnectionState,

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

    // Wakes up when a stream has written data pending
    write_waker: AtomicWaker,

    packet_number: usize,
    stream_number: usize,
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
        buf.copy_to_slice(&mut id.buf[..len as usize]);
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
        f.debug_tuple("CID")
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
        match packet.header {
            Header::Short { .. } => {
                if !matches!(self.state, ConnectionState::Data) {
                    eprintln!("Got 1-RTT header in wrong state: {:?}", self.state);
                    return;
                }
                let frameiter = FrameIterator::new(packet.payload.freeze());
                for frame in frameiter {
                    println!("Frame");
                    self.handle_frame(frame);
                }
            }
            Header::Initial { .. } => {
                if !matches!(self.state, ConnectionState::Initial) {
                    eprintln!("Got initial header in wrong state: {:?}", self.state);
                    return;
                }
                // TODO: Handle sending two quic packets at a time
                // And do different stuff for client and server

                match self.side {
                    Side::Server => {
                        self.state = ConnectionState::Handshake;
                        let crypto_data = CryptoFrame::ServerHello;
                        let mut c_data = Vec::new();
                        crypto_data.encode(&mut c_data);
                        let crypto_frame = Frame::Crypto {
                            offset: VarInt(0),
                            data: c_data,
                        };
                        let mut packet = Packet {
                            header: Header::Handshake {
                                version: 1,
                                dst_cid: self.peer_cid,
                                src_cid: self.id,
                                length: VarInt(1), // TODO: Handle length
                                packet_number: PacketNumber::from(self.packet_number),
                            },
                            payload: BytesMut::new(),
                        };
                        crypto_frame.encode(&mut packet.payload).unwrap();
                        self.pending_packets.push_back(packet);
                    }
                    Side::Client => {
                        panic!("Got initial as client: THIS IS WRONG");
                    }
                }
            }
            Header::Handshake { .. } => {
                if !matches!(self.state, ConnectionState::Handshake) {
                    eprintln!("Got Handshake header in wrong state: {:?}", self.state);
                    return;
                }
                match self.side {
                    Side::Client => {
                        self.state = ConnectionState::Data;
                        let crypto_data = CryptoFrame::Finished;
                        let mut c_data = Vec::new();
                        crypto_data.encode(&mut c_data);
                        let crypto_frame = Frame::Crypto {
                            offset: VarInt(0),
                            data: c_data,
                        };
                        let mut packet = Packet {
                            header: Header::Handshake {
                                version: 1,
                                dst_cid: self.peer_cid,
                                src_cid: self.id,
                                length: VarInt(1), // TODO: Handle length
                                packet_number: PacketNumber::from(self.packet_number),
                            },
                            payload: BytesMut::new(),
                        };
                        crypto_frame.encode(&mut packet.payload).unwrap();
                        self.pending_packets.push_back(packet);
                    }
                    Side::Server => {
                        self.state = ConnectionState::Data;
                        if let Some(connected) = self.connected.take() {
                            connected.send(()).unwrap();
                        } else {
                            eprintln!("Connected already consumed. Something is wrong")
                        }
                        let frame = Frame::HandshakeDone;
                        let mut packet = Packet {
                            header: Header::Short {
                                spin: false,
                                key_phase: false,
                                dst_cid: self.peer_cid,
                                packet_number: PacketNumber::from(self.packet_number),
                            },
                            payload: BytesMut::new(),
                        };
                        frame.encode(&mut packet.payload).unwrap();
                        self.pending_packets.push_back(packet);
                    }
                }
            }
        }
    }

    fn handle_frame(&mut self, frame: Frame) {
        match &frame {
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
            Frame::MaxStreamData { id, .. } | Frame::StopSending { id, .. } => {
                if !self.send.contains_key(id) {
                    self.handle_new_stream(id);
                }

                let send = self.send.get_mut(id).unwrap();

                send.handle_frame(frame);
            }
            Frame::Crypto { offset, data } => {
                // TODO: This is just a mock
            }
            Frame::Padding => todo!(),
            Frame::Ping => todo!(),
            Frame::Ack {
                largest,
                delay,
                range_count,
                first_range,
                ranges,
                ecn,
            } => todo!(),
            Frame::NewToken { token } => todo!(),
            Frame::MaxData { max_data } => todo!(),
            Frame::MaxStreams { dir, max_streams } => todo!(),
            Frame::DataBlocked { max_data } => todo!(),
            Frame::StreamsBlocked { dir, max_streams } => todo!(),
            Frame::NewConnectionId {
                seq_num,
                retire,
                cid,
                stateless_reset_token,
            } => todo!(),
            Frame::RetireConnectionId { seq_num } => todo!(),
            Frame::PathChallenge { data } => todo!(),
            Frame::PathResponse { data } => todo!(),
            Frame::ConnectionCloseQuic {
                error_code,
                frame_type,
                reason_phrase,
            } => todo!(),
            Frame::ConnectionCloseApp {
                error_code,
                reason_phrase,
            } => todo!(),
            Frame::HandshakeDone => {
                // TODO: This is just a mock
                if let Some(connected) = self.connected.take() {
                    connected.send(()).unwrap();
                } else {
                    eprintln!("Connected already consumed. Something is wrong")
                }
            }
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
                offset: VarInt(offset),
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
        if let Frame::Stream { fin, .. } = last {
            *fin = true
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
        connected: oneshot::Sender<()>,
        incoming_data: UnboundedReceiver<EndpointToConnection>,
        outgoing_data: UnboundedSender<ConnectionToEndpoint>,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ConnectionInner {
            id: cid,
            peer_cid,
            side,
            connected: Some(connected),
            state: ConnectionState::Initial,
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
            packet_number: 0,
            stream_number: 0,
        }));
        let driver = ConnectionDriver {
            inner: inner.clone(),
            incoming_data,
            outgoing_data,
            addr,
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
            if let Some(id) = inner.unidir_acceptable.pop_front() {
                Poll::Ready(RecvStreamHandle::new(id, self.inner.clone()))
            } else {
                inner.unidir_accept_waker.register(cx.waker());
                Poll::Pending
            }
        })
        .await
    }

    pub fn open_bidir(&self) -> (RecvStreamHandle, SendStreamHandle) {
        let mut inner = self.inner.lock().unwrap();
        let id = StreamID::new(inner.stream_number, &inner.side, Dir::Bidir); // TODO: Handle :)
        inner.stream_number += 1;
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
                length: VarInt(1),
                packet_number: PacketNumber::U8(1u8),
            },
            payload: BytesMut::new(),
        };
        inner.pending_packets.push_back(packet);
        inner.state = ConnectionState::Handshake;
        inner.write_waker.wake();
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

#[derive(Debug)]
enum ConnectionState {
    Initial,
    Handshake,
    Data,
}

pub struct IncomingInner {
    id: ConnectionID,
    src_cid: ConnectionID,
    connected: oneshot::Receiver<()>,
}

impl IncomingInner {
    pub fn new(id: ConnectionID, src_cid: ConnectionID, connected: oneshot::Receiver<()>) -> Self {
        Self {
            id,
            src_cid,
            connected,
        }
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
        let this = self.get_mut();
        println!("Waiting for connected notification");
        ready!(Pin::new(&mut this.inner.connected).poll(cx)).unwrap();

        let endpoint = this.endpoint.state.lock().unwrap();

        let connection = endpoint.connections.get(&this.inner.id).unwrap();
        Poll::Ready(connection.clone())
    }
}

// Handles reeiving data from the endpoint and passing it to inner and seeing if pending data
// exists
struct ConnectionDriver {
    inner: Arc<Mutex<ConnectionInner>>,
    incoming_data: UnboundedReceiver<EndpointToConnection>,
    outgoing_data: UnboundedSender<ConnectionToEndpoint>,
    addr: SocketAddr,
}

impl Future for ConnectionDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.inner.lock().unwrap().write_waker.register(cx.waker());

        let mut buffer = Vec::new();

        loop {
            match this.incoming_data.poll_recv(cx) {
                Poll::Ready(Some(data)) => buffer.push(data),
                Poll::Ready(None) => return Poll::Ready(()), // channel closed
                Poll::Pending => break,
            }
        }

        if !buffer.is_empty() {
            let mut conn = this.inner.lock().unwrap();

            for data in buffer {
                conn.handle_datagram(&data.data);
            }
        }

        let packets: Vec<_> = {
            let mut conn = this.inner.lock().unwrap();
            let mut pkts = Vec::new();
            while let Some(packet) = conn.pending_packets.pop_front() {
                pkts.push(packet);
            }
            while !conn.pending_send.is_empty() {
                if let Some(p) = conn.get_packet(1200) {
                    pkts.push(p);
                } else {
                    break;
                }
            }
            pkts
        };

        for packet in packets {
            let mut buf = BytesMut::new();
            packet.encode(&mut buf).unwrap();
            this.outgoing_data
                .send(ConnectionToEndpoint {
                    data: buf,
                    addr: this.addr,
                })
                .unwrap();
        }

        Poll::Pending
    }
}
