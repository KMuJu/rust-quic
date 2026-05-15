use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::poll_fn,
    io::Cursor,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
    time::Duration,
};

use bytes::{Buf, BytesMut};
use futures::task::AtomicWaker;
use rand::{Rng, rngs::StdRng};
use tokio::{
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

use crate::io::{
    ConnectionToEndpoint, EndpointToConnection,
    crypto::CryptoFrame,
    endpoint::EndpointRef,
    frame::Frame,
    packet::{Header, Packet, PacketNumber},
    rtt_estimator::RttEstimator,
    socket::Socket,
    space::{
        PacketSpace, Space,
        SpaceEvent::{Crypto, HandshakeDone, RecvStreamFrame, RttSample, SendStreamFrame},
    },
    stream::{Dir, RecvStream, RecvStreamHandle, SendStream, SendStreamHandle, Side, StreamID},
    transport_parameters::{SharedConfig, TransportParam},
};

use super::encoding::VarInt;

pub struct ConnectionInner {
    id: ConnectionID,
    // Peer's cid
    peer_cid: ConnectionID,
    side: Side,

    connected: Option<oneshot::Sender<()>>,
    state: PacketSpace,

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

    // Wakes up when a stream has written data pending
    write_waker: AtomicWaker,

    stream_number: usize,

    initial_space: Space,
    handshake_space: Space,
    data_space: Space,

    transport_params: SharedConfig,
    rtt: RttEstimator,
    pto_backoff: u32,
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
        let p_space = packet.packet_space();
        println!(
            "{:?}({:?})[{:?}]: Got {:?}",
            self.id, self.state, self.side, packet
        );
        if !self
            .space_mut(p_space)
            .acker
            .received(packet.packet_number(), packet.is_ack_eliciting())
        {
            eprintln!(
                "ERROR: Got dulicate packet!({:?}): {}",
                p_space,
                packet.packet_number().u32()
            );
            return;
        }

        match packet.header {
            Header::Short { .. } => {
                if !matches!(self.state, PacketSpace::Data) {
                    eprintln!("Got 1-RTT header in wrong state: {:?}", self.state);
                    for frame in packet.frames.iter() {
                        println!("Frame: {frame:?}");
                    }
                    return;
                }
                let mut events = Vec::new();
                for frame in packet.frames {
                    if let Some(event) = self.data_space.handle_frame(frame, &self.rtt) {
                        events.push(event);
                    }
                }
                for event in events {
                    match event {
                        RecvStreamFrame(id, frame) => {
                            if !self.recv.contains_key(&id) {
                                self.handle_new_stream(&id);
                            }

                            let recv = self.recv.get_mut(&id).unwrap();

                            recv.handle_frame(frame);
                        }
                        SendStreamFrame(id, frame) => {
                            if !self.send.contains_key(&id) {
                                self.handle_new_stream(&id);
                            }

                            let send = self.send.get_mut(&id).unwrap();

                            send.handle_frame(frame);
                        }
                        HandshakeDone => {
                            self.consume_connected();
                        }
                        RttSample(latest_rtt, ack_delay) => {
                            self.rtt.update(
                                latest_rtt,
                                ack_delay,
                                &self.transport_params.max_ack_delay,
                                self.handshake_is_done(),
                            );
                        }
                        Crypto(var_int, items) => todo!(),
                    }
                }
                if matches!(self.side, Side::Client) {
                    self.handshake_space.discard();
                }
            }
            Header::Initial { .. } => {
                let is_client_hello = packet.is_client_hello();
                let mut events = Vec::new();
                // Handle initial frames in order to do acks
                for frame in packet.frames {
                    if let Some(event) = self.initial_space.handle_frame(frame, &self.rtt) {
                        events.push(event);
                    }
                }

                if matches!(self.side, Side::Client) || !matches!(self.state, PacketSpace::Initial)
                {
                    // Does not have to handle initial events for client or if not in initial state
                    return;
                }

                for event in events {
                    match event {
                        Crypto(offset, data) => {
                            let mut data = Cursor::new(data);
                            let c_frame = CryptoFrame::decode(&mut data);
                            if matches!(c_frame, CryptoFrame::ClientHello) {
                                self.state = PacketSpace::Handshake;

                                let crypto_data = CryptoFrame::ServerHello;
                                let mut c_data = Vec::new();
                                crypto_data.encode(&mut c_data);
                                let crypto_frame = Frame::Crypto {
                                    offset: VarInt(0),
                                    data: c_data,
                                };

                                self.handshake_space.push_back_frame(crypto_frame);

                                self.initial_space.discard();
                            }
                        }
                        RttSample(latest_rtt, ack_delay) => {
                            self.rtt.update(
                                latest_rtt,
                                ack_delay,
                                &self.transport_params.max_ack_delay,
                                self.handshake_is_done(),
                            );
                        }
                        RecvStreamFrame(_, _) | SendStreamFrame(_, _) | HandshakeDone => (),
                    }
                }
            }
            Header::Handshake { .. } => {
                let mut events = Vec::new();
                for frame in packet.frames {
                    if let Some(event) = self.handshake_space.handle_frame(frame, &self.rtt) {
                        events.push(event);
                    }
                }
                if !matches!(self.state, PacketSpace::Handshake) {
                    // Does not have to handle handshake events if not in handshake state
                    return;
                }
                for event in events {
                    match event {
                        Crypto(offset, data) => match self.side {
                            Side::Client => {
                                let mut data = Cursor::new(data);
                                let c_frame = CryptoFrame::decode(&mut data);
                                if matches!(c_frame, CryptoFrame::ServerHello) {
                                    self.state = PacketSpace::Data;
                                    let crypto_data = CryptoFrame::Finished;
                                    let mut c_data = Vec::new();
                                    crypto_data.encode(&mut c_data);
                                    let crypto_frame = Frame::Crypto {
                                        offset: VarInt(0),
                                        data: c_data,
                                    };
                                    self.handshake_space.push_back_frame(crypto_frame);
                                    self.initial_space.discard();
                                }
                            }
                            Side::Server => {
                                let mut data = Cursor::new(data);
                                let c_frame = CryptoFrame::decode(&mut data);
                                if matches!(c_frame, CryptoFrame::Finished) {
                                    self.state = PacketSpace::Data;
                                    self.consume_connected();
                                    let frame = Frame::HandshakeDone;
                                    self.data_space.push_back_frame(frame);
                                    self.handshake_space.discard();
                                }
                            }
                        },
                        RttSample(latest_rtt, ack_delay) => {
                            self.rtt.update(
                                latest_rtt,
                                ack_delay,
                                &self.transport_params.max_ack_delay,
                                self.handshake_is_done(),
                            );
                        }
                        RecvStreamFrame(_, _) | SendStreamFrame(_, _) | HandshakeDone => (),
                    }
                }
            }
        }
    }

    fn consume_connected(&mut self) {
        if let Some(connected) = self.connected.take() {
            connected.send(()).unwrap();
        } else {
            eprintln!("[ERROR]: Connected already consumed. Something is wrong")
        }
        self.pto_backoff = 0;
    }

    #[inline]
    fn space_mut(&mut self, p_space: PacketSpace) -> &mut Space {
        match p_space {
            PacketSpace::Initial => &mut self.initial_space,
            PacketSpace::Handshake => &mut self.handshake_space,
            PacketSpace::Data => &mut self.data_space,
        }
    }

    #[inline]
    fn space(&self, p_space: PacketSpace) -> &Space {
        match p_space {
            PacketSpace::Initial => &self.initial_space,
            PacketSpace::Handshake => &self.handshake_space,
            PacketSpace::Data => &self.data_space,
        }
    }

    fn add_lost_to_pending(&mut self, space: PacketSpace, lost: Vec<Frame>) {
        println!("Lost these frames({space:?})[{:?}]: {lost:?}", self.side);
        for frame in lost {
            // Only retransmitt stream frames for send streams that has not sent reset
            if let Frame::Stream { id, .. } = &frame {
                let stream = self.send(id);
                let Some(stream) = stream else {
                    continue;
                };
                if stream.is_reset() {
                    continue;
                }
            };
            self.space_mut(space).push_back_frame(frame);
        }
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

    fn peer_completed_address_validation(&self) -> bool {
        // TODO: Do properly
        self.handshake_is_done()
    }

    fn spaces(&self) -> impl Iterator<Item = &Space> {
        [&self.initial_space, &self.handshake_space, &self.data_space].into_iter()
    }

    fn next_ack_deadline(&self) -> Option<Instant> {
        self.spaces().filter_map(|s| s.acker.get_deadline()).min()
    }

    fn next_loss_deadline(&self) -> Option<Instant> {
        self.spaces().filter_map(|s| s.acker.get_loss_time()).min()
    }

    fn pto_deadline(&self) -> Option<Instant> {
        let duration = (self.rtt.smoothed + (self.rtt.variance * 4).max(Duration::from_millis(1)))
            * 2u32.pow(self.pto_backoff);

        let any_in_flight = self.spaces().any(|s| s.acker.has_packet_in_flight());

        if !any_in_flight {
            if !self.peer_completed_address_validation() {
                return Some(Instant::now() + duration);
            } else {
                return None;
            }
        }

        let mut pto_timeout: Option<Instant> = None;
        for space in PacketSpace::iter() {
            if !self.space(space).acker.has_packet_in_flight() {
                continue;
            }
            let mut duration = duration;
            if matches!(space, PacketSpace::Data) {
                if !self.handshake_is_done() {
                    return pto_timeout;
                }
                duration += self.transport_params.max_ack_delay * 2u32.pow(self.pto_backoff);
            }

            let Some(last) = self.space(space).acker.time_of_last_elicit() else {
                // Skip if space has sent no packet
                continue;
            };
            let t = last + duration;

            pto_timeout = match pto_timeout {
                Some(pto) => Some(pto.min(t)),
                None => Some(t),
            };
        }

        pto_timeout
    }

    fn get_packet(&mut self, p_space: PacketSpace, mut mtu: usize) -> Option<Packet> {
        let mut frames = Vec::new();

        while let Some(frame) = self.space_mut(p_space).pending_frames.front() {
            if mtu > frame.size() {
                let frame = self.space_mut(p_space).pending_frames.pop_front().unwrap();
                // Only retransmitt stream frames for send streams that has not sent reset
                if let Frame::Stream { id, .. } = &frame {
                    let stream = self.send.get(id);
                    let Some(stream) = stream else {
                        continue;
                    };
                    if stream.is_reset() {
                        continue;
                    }
                };
                mtu -= frame.size();
                frames.push(frame);
            } else {
                break;
            }
        }

        if let Some(ack) = self.space_mut(p_space).acker.get_ack() {
            if mtu > ack.size() {
                mtu -= ack.size();
                frames.push(ack);
            }
        };

        if matches!(p_space, PacketSpace::Data) {
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
                    len: true,
                    offset: VarInt(offset),
                    data: data.freeze(),
                };

                if !send.empty() {
                    self.pending_send.push_back(id);
                }

                let size = frame.size();
                if mtu > size {
                    mtu -= size;
                    frames.push(frame);
                } else {
                    self.space_mut(p_space).pending_frames.push_back(frame);
                    break;
                }
            }
        }

        if frames.is_empty() {
            return None;
        }

        let last = frames.last_mut().unwrap();
        if let Frame::Stream { fin, .. } = last {
            *fin = true
        };

        let pn = PacketNumber::from(self.space_mut(p_space).new_packet_num());

        let header = match p_space {
            PacketSpace::Initial => Header::Initial {
                dst_cid: self.peer_cid,
                src_cid: self.id,
                packet_number: pn,
                version: 0,
                token: None,
                length: VarInt(0),
            },
            PacketSpace::Handshake => Header::Handshake {
                dst_cid: self.peer_cid,
                src_cid: self.id,
                packet_number: pn,
                version: 0,
                length: VarInt(0),
            },
            PacketSpace::Data => Header::Short {
                spin: false,
                key_phase: false,
                dst_cid: self.peer_cid,
                packet_number: pn,
            },
        };

        // println!("Sending frames({space:?}): {frames:?}");

        let packet = Packet { header, frames };

        Some(packet)
    }

    fn build_pto_probe(&mut self, p_space: PacketSpace) -> Frame {
        if let Some(frame) = self.space_mut(p_space).acker.unacked_frame() {
            frame
        } else {
            Frame::Ping
        }
    }

    fn pto_space(&self) -> PacketSpace {
        if !self.initial_space.is_discarded() && self.initial_space.acker.has_packet_in_flight() {
            PacketSpace::Initial
        } else if !self.handshake_space.is_discarded()
            && self.handshake_space.acker.has_packet_in_flight()
        {
            PacketSpace::Handshake
        } else {
            PacketSpace::Data
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

    fn handshake_is_done(&self) -> bool {
        self.connected.is_none()
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
        let transport_params = Arc::new(TransportParam::new());
        let inner = Arc::new(Mutex::new(ConnectionInner {
            id: cid,
            peer_cid,
            side,
            connected: Some(connected),
            state: PacketSpace::Initial,
            recv: HashMap::new(),
            send: HashMap::new(),
            bidir_acceptable: VecDeque::new(),
            bidir_accept_waker: AtomicWaker::new(),
            unidir_acceptable: VecDeque::new(),
            unidir_accept_waker: AtomicWaker::new(),
            pending_recv: VecDeque::new(),
            pending_send: VecDeque::new(),
            write_waker: AtomicWaker::new(),
            stream_number: 0,

            initial_space: Space::new(PacketSpace::Initial, transport_params.clone()),
            handshake_space: Space::new(PacketSpace::Handshake, transport_params.clone()),
            data_space: Space::new(PacketSpace::Data, transport_params.clone()),
            pto_backoff: 0,
            transport_params: transport_params.clone(),
            rtt: RttEstimator::new(),
        }));
        let driver = ConnectionDriver {
            inner: inner.clone(),
            incoming_data,
            outgoing_data,
            addr,
            ack_timer: None,
            loss_timer: None,
            pto_timer: None,
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
        let id = StreamID::new(inner.stream_number, &inner.side, Dir::Bidir);
        // TODO: Have seperate counters for each?
        // Currently this will shrink the number of possible stream ids
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
    pub(super) fn init(&self) {
        let mut inner = self.inner.lock().unwrap();
        let crypto_data = CryptoFrame::ClientHello;
        let mut c_data = Vec::new();
        crypto_data.encode(&mut c_data);
        let crypto_frame = Frame::Crypto {
            offset: VarInt(0),
            data: c_data,
        };
        inner.initial_space.pending_frames.push_back(crypto_frame);
        inner.state = PacketSpace::Handshake;
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

pub struct Incoming<S: Socket> {
    inner: IncomingInner,
    endpoint: EndpointRef<S>,
}

impl<S: Socket> Incoming<S> {
    pub fn new(inner: IncomingInner, endpoint: EndpointRef<S>) -> Self {
        Self { inner, endpoint }
    }
}

impl<S: Socket> Future for Incoming<S> {
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
    ack_timer: Option<Pin<Box<tokio::time::Sleep>>>,
    loss_timer: Option<Pin<Box<tokio::time::Sleep>>>,
    pto_timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Future for ConnectionDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.inner.lock().unwrap().write_waker.register(cx.waker());
        this.inner
            .lock()
            .unwrap()
            .spaces()
            .for_each(|s| s.acker.register_waker(cx.waker()));

        let ack_fired = this
            .ack_timer
            .as_mut()
            .map(|t| t.as_mut().poll(cx).is_ready())
            .unwrap_or(false);
        let loss_fired = this
            .loss_timer
            .as_mut()
            .map(|t| t.as_mut().poll(cx).is_ready())
            .unwrap_or(false);
        let pto_fired = this
            .pto_timer
            .as_mut()
            .map(|t| t.as_mut().poll(cx).is_ready())
            .unwrap_or(false);
        if ack_fired {
            this.ack_timer = None;
        }
        if loss_fired {
            this.loss_timer = None;
        }
        if pto_fired {
            this.pto_timer = None;
        }

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

        if loss_fired {
            let mut inner = this.inner.lock().unwrap();
            println!("Loss fired in: {:?}", inner.side);
            for space in PacketSpace::iter() {
                let rtt = &inner.rtt.clone();
                let lost = inner.space_mut(space).acker.get_lost_frames(rtt);
                if !lost.is_empty() {
                    println!(
                        "Found timedout packets({space:?})[{:?}]: {lost:?}!!!!!!!!!!!!!!!!!!!!!!!",
                        inner.side
                    );
                }
                inner.add_lost_to_pending(space, lost);
            }
        }

        if pto_fired {
            let mut inner = this.inner.lock().unwrap();
            let p_space = inner.pto_space();
            let pto_frame = inner.build_pto_probe(p_space);
            inner.space_mut(p_space).pending_frames.push_back(pto_frame);
            inner.pto_backoff += 1;
        }

        let packets = {
            let mut conn = this.inner.lock().unwrap();
            let mut pkts = Vec::new();

            for space in PacketSpace::iter() {
                while let Some(p) = conn.get_packet(space, 1200) {
                    pkts.push(p);
                }
            }
            pkts
        };

        for packet in packets {
            let p_space = packet.packet_space();
            println!(
                "Sending packet({p_space:?})[{:?}]: {packet:?}",
                this.inner.lock().unwrap().side
            );

            if packet.is_ack_eliciting() {
                this.inner
                    .lock()
                    .unwrap()
                    .space_mut(p_space)
                    .acker
                    .sent_packet(
                        packet.packet_number().clone(),
                        packet.frames.clone(),
                        packet.is_in_flight(),
                    );
            }

            let mut buf = BytesMut::new();
            packet.encode(&mut buf).unwrap();
            this.outgoing_data
                .send(ConnectionToEndpoint {
                    data: buf,
                    addr: this.addr,
                })
                .unwrap();
        }

        let deadline = this.inner.lock().unwrap().next_ack_deadline();
        match (deadline, this.ack_timer.as_mut()) {
            (Some(deadline), Some(timer)) => {
                timer.as_mut().reset(deadline);
            }
            (Some(deadline), None) => {
                let mut sleep = Box::pin(tokio::time::sleep_until(deadline));
                let _ = sleep.as_mut().poll(cx); // register waker
                this.ack_timer = Some(sleep);
            }
            (None, _) => {
                this.ack_timer = None;
            }
        }

        let loss_deadline = this.inner.lock().unwrap().next_loss_deadline();
        match (loss_deadline, this.loss_timer.as_mut()) {
            (Some(d), Some(t)) => t.as_mut().reset(d),
            (Some(d), None) => {
                let mut sleep = Box::pin(tokio::time::sleep_until(d));
                let _ = sleep.as_mut().poll(cx);
                this.loss_timer = Some(sleep);
            }
            (None, _) => this.loss_timer = None,
        }

        let pto_deadline = this.inner.lock().unwrap().pto_deadline();
        match (pto_deadline, this.pto_timer.as_mut()) {
            (Some(d), Some(t)) => {
                t.as_mut().reset(d);
            }
            (Some(d), None) => {
                let mut sleep = Box::pin(tokio::time::sleep_until(d));
                let _ = sleep.as_mut().poll(cx); // register waker
                this.pto_timer = Some(sleep);
            }
            (None, _) => this.pto_timer = None,
        }

        Poll::Pending
    }
}
