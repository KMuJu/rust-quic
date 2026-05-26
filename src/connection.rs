use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::poll_fn,
    io::Cursor,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
    time::Duration,
};

use bytes::{Buf, BytesMut};
use futures::task::AtomicWaker;
use rand::{Rng, rngs::StdRng};
use tokio::{
    io::ReadBuf,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

use crate::{
    ConnectionToEndpoint, EndpointToConnection,
    congestion::CongestionController,
    crypto::CryptoFrame,
    encoding::VarInt,
    endpoint::EndpointRef,
    frame::{Ack, Frame, Stream},
    packet::{Header, Packet, PacketNumber},
    rtt_estimator::RttEstimator,
    socket::Socket,
    space::{PacketSpace, Space},
    stream::{Dir, ReadError, RecvStream, SendError, SendStream, Side, StreamID},
    timers::{Timer, TimerTable},
    transport_parameters::{SharedConfig, TransportParam},
};

#[derive(PartialEq, Eq, Debug)]
pub enum LifeCycle {
    Handshake,
    Connected,
    // TODO: Add closed and such
}

pub struct ConnectionCore {
    id: ConnectionID,
    // Peer's cid
    peer_cid: ConnectionID,
    side: Side,
    lifeycle: LifeCycle,

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

    // initial_space: Space,
    // handshake_space: Space,
    // data_space: Space,
    spaces: [Space; 3],

    transport_params: SharedConfig,
    rtt: RttEstimator,
    pto_count: u32,

    congestion: CongestionController,
    timers: TimerTable,
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

impl ConnectionCore {
    pub(crate) fn new(
        cid: ConnectionID,
        peer_cid: ConnectionID,
        side: Side,
        connected: oneshot::Sender<()>,
        transport_params: SharedConfig,
    ) -> Self {
        ConnectionCore {
            id: cid,
            peer_cid,
            side,
            lifeycle: LifeCycle::Handshake,
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

            spaces: [
                Space::new(PacketSpace::Initial, transport_params.clone()),
                Space::new(PacketSpace::Handshake, transport_params.clone()),
                Space::new(PacketSpace::Data, transport_params.clone()),
            ],
            pto_count: 0,
            transport_params: transport_params.clone(),
            rtt: RttEstimator::new(),
            congestion: CongestionController::new(transport_params.clone()),
            timers: TimerTable::new(),
        }
    }

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
        if !self.spaces[p_space]
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

        match self.lifeycle {
            LifeCycle::Handshake => {}
            LifeCycle::Connected => match packet.packet_space() {
                PacketSpace::Data => return self.handle_data_packet(packet),
                _ => {
                    eprintln!("Unexpected handshake packet when connected")
                }
            },
        };

        match &packet.header {
            Header::Initial { .. } | Header::Handshake { .. } => {
                if !self.spaces[packet.packet_space()].is_discarded() {
                    self.handle_handshake_packet(packet);
                }
            }
            Header::Short { .. } => self.handle_data_packet(packet),
        }
    }

    fn handle_data_packet(&mut self, packet: Packet) {
        let p_space = PacketSpace::Data;
        for frame in packet.frames {
            match frame {
                Frame::Padding => continue,
                Frame::Ping => {
                    if let Some(ack) = self.spaces[p_space].acker.get_ack() {
                        self.spaces[p_space].pending_frames.push_back(ack);
                    }
                }
                Frame::Ack(ack) => {
                    self.on_ack_received(p_space, ack);
                }
                Frame::ResetStream {
                    id,
                    error_code,
                    final_size,
                } => todo!(),
                Frame::StopSending { id, error_code } => todo!(),
                Frame::Crypto { offset, data } => {
                    eprintln!("[CRYPTO] Got crypto while established")
                }
                Frame::NewToken { token } => todo!(),
                Frame::Stream(stream) => {
                    if !self.recv.contains_key(&stream.id) {
                        self.handle_new_stream(&stream.id);
                    }

                    let recv = self.recv.get_mut(&stream.id).unwrap();
                    recv.handle_stream_frame(stream);
                }
                Frame::MaxData { max_data } => todo!(),
                Frame::MaxStreamData {
                    id,
                    max_stream_data,
                } => todo!(),
                Frame::MaxStreams { dir, max_streams } => todo!(),
                Frame::DataBlocked { max_data } => todo!(),
                Frame::StreamDataBlocked {
                    id,
                    max_stream_data,
                } => todo!(),
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
                    self.consume_connected();
                    self.spaces[PacketSpace::Initial].discard();
                    self.spaces[PacketSpace::Handshake].discard();
                }
            }
        }
    }

    fn handle_handshake_packet(&mut self, packet: Packet) {
        let p_space = packet.packet_space();
        for frame in packet.frames {
            match frame {
                Frame::Padding | Frame::Ping => {}
                Frame::Ack(ack) => self.on_ack_received(p_space, ack),
                Frame::Crypto { offset, data } => self.handle_crypto(p_space, offset, data),
                Frame::ConnectionCloseQuic {
                    error_code,
                    frame_type,
                    reason_phrase,
                } => todo!(),
                _ => {
                    unreachable!("Got illigal frame type when processing handshake packet")
                }
            }
        }
    }

    fn handle_crypto(&mut self, p_space: PacketSpace, offset: VarInt, data: Vec<u8>) {
        match p_space {
            PacketSpace::Initial => {
                if matches!(self.side, Side::Client) || !matches!(self.state, PacketSpace::Initial)
                {
                    // Does not have to handle initial events for client or if not in initial state
                    return;
                }
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

                    self.spaces[PacketSpace::Handshake].push_back_frame(crypto_frame);

                    self.spaces[PacketSpace::Initial].discard();
                }
            }
            PacketSpace::Handshake => {
                if !matches!(self.state, PacketSpace::Handshake) {
                    // Does not have to handle handshake events if not in handshake state
                    return;
                }

                match self.side {
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
                            self.spaces[PacketSpace::Handshake].push_back_frame(crypto_frame);
                            self.spaces[PacketSpace::Initial].discard();
                            self.lifeycle = LifeCycle::Connected;
                        }
                    }
                    Side::Server => {
                        let mut data = Cursor::new(data);
                        let c_frame = CryptoFrame::decode(&mut data);
                        if matches!(c_frame, CryptoFrame::Finished) {
                            self.state = PacketSpace::Data;
                            let frame = Frame::HandshakeDone;
                            self.spaces[PacketSpace::Data].push_back_frame(frame);
                            self.spaces[PacketSpace::Handshake].discard();

                            self.consume_connected();
                            self.lifeycle = LifeCycle::Connected;
                        }
                    }
                }
            }
            PacketSpace::Data => {}
        }
    }

    /// Follows RFC 9002 A.7
    fn on_ack_received(&mut self, p_space: PacketSpace, ack: Ack) {
        let mut acked = Vec::new();
        let mut current = ack.largest.0;

        // First range: largest down to largest - first_range
        for pn in (current - ack.first_range.0..=current).rev() {
            acked.push(pn as u32);
        }
        current -= ack.first_range.0;

        // Subsequent ranges: skip gap+2, then ack length+1
        for range in ack.ranges {
            current -= range.gap.0 + 2;
            for pn in (current - range.length.0..=current).rev() {
                acked.push(pn as u32);
            }
            current -= range.length.0;
        }

        let newly_acked: Vec<_> = acked
            .iter()
            .filter_map(|&pn| self.spaces[p_space].acker.acked(pn))
            .collect();

        if newly_acked.is_empty() {
            return;
        }

        let largest_acked = newly_acked.iter().max_by_key(|acked| acked.pn).unwrap();
        if largest_acked.pn == (ack.largest.0 as u32)
            && newly_acked.iter().any(|acked| acked.ack_eliciting)
        {
            let latest_rtt = Instant::now() - largest_acked.timestamp;
            let ack_delay =
                Duration::from_millis(ack.delay.0 << self.transport_params.ack_delay_exponent);
            self.rtt.update(
                latest_rtt,
                ack_delay,
                &self.transport_params.max_ack_delay,
                self.handshake_is_done(),
            );
        }

        let lost = self.spaces[p_space].acker.get_lost_frames(&self.rtt);
        if !lost.is_empty() {
            self.congestion.on_loss(lost.as_slice(), &self.rtt);
        }

        newly_acked
            .iter()
            .filter(|acked| acked.in_flight)
            .for_each(|acked| {
                self.congestion
                    .on_ack(acked.sent_bytes as u64, acked.timestamp)
            });

        if !lost.is_empty() {
            let lost_frames: Vec<_> = lost.into_iter().flat_map(|lost| lost.frames).collect();
            self.add_lost_to_pending(p_space, lost_frames);
        }

        if self.peer_completed_address_validation() {
            self.pto_count = 0;
        }
        self.set_loss_timer();
    }

    fn consume_connected(&mut self) {
        if let Some(connected) = self.connected.take() {
            connected.send(()).unwrap();
        } else {
            eprintln!("[ERROR]: Connected already consumed. Something is wrong")
        }
        self.pto_count = 0;
    }

    fn add_lost_to_pending(&mut self, p_space: PacketSpace, lost: Vec<Frame>) {
        // println!("Lost these frames({space:?})[{:?}]: {lost:?}", self.side);
        for frame in lost {
            // Only retransmitt stream frames for send streams that has not sent reset
            if let Frame::Stream(Stream { id, .. }) = &frame {
                let stream = self.send(id);
                let Some(stream) = stream else {
                    continue;
                };
                if stream.is_reset() {
                    continue;
                }
            };
            self.spaces[p_space].push_back_frame(frame);
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
        self.spaces.iter()
    }

    fn next_ack_deadline(&self) -> Option<Instant> {
        self.spaces().filter_map(|s| s.acker.get_deadline()).min()
    }

    /// Follows RFC 9002 A.8
    fn get_loss_time_and_space(&self) -> Option<(Instant, PacketSpace)> {
        self.spaces()
            .filter_map(|s| Some((s.acker.get_loss_time()?, s.space)))
            .min_by_key(|(time, _)| *time)
    }

    fn any_ack_eliciting_packet_in_flight(&self) -> bool {
        self.spaces()
            .any(|s| s.acker.has_ack_eliciting_packet_in_flight())
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        // Updates max ack delay timer because I could not get it to work if I set it other places
        let ack_deadline = self.next_ack_deadline();
        match ack_deadline {
            Some(d) => self.timers.set(Timer::MaxAckDelay, d),
            None => self.timers.reset(Timer::MaxAckDelay),
        }

        self.timers.next()
    }

    /// Follows RFC 9002 A.8
    ///
    /// Should be called after:
    /// - Loss detection timeout
    /// - Received ack
    /// - Packet sent
    fn set_loss_timer(&mut self) {
        let earliest = self.get_loss_time_and_space();

        if let Some((deadline, _)) = earliest {
            println!("[TIMER] Set loss detection timer");
            self.timers.set(Timer::LossDetection, deadline);
            return;
        }

        if !self.any_ack_eliciting_packet_in_flight() && self.peer_completed_address_validation() {
            self.timers.reset(Timer::LossDetection);
            println!("[TIMER] Reset loss detection timer");
            return;
        }

        let (pto, _) = self.get_pto_time_and_space().unwrap();
        // println!("[TIMER] Set PTO detection timer: {pto:?}");
        self.timers.set(Timer::LossDetection, pto);
    }

    /// Follows pseudocode described in RFC 9002 A.8
    fn get_pto_time_and_space(&self) -> Option<(Instant, PacketSpace)> {
        // RFC 9002 section 6.2.1
        let duration = (self.rtt.smoothed + (self.rtt.variance * 4).max(Duration::from_millis(1)))
            * 2u32.pow(self.pto_count);

        if !self.any_ack_eliciting_packet_in_flight() {
            if !self.peer_completed_address_validation() {
                // TODO: Should check for keys
                return Some((Instant::now() + duration, PacketSpace::Initial));
            } else {
                return None;
            }
        }

        let mut pto_timeout: Option<(Instant, PacketSpace)> = None;
        for p_space in [
            PacketSpace::Initial,
            PacketSpace::Handshake,
            PacketSpace::Data,
        ] {
            if !self.spaces[p_space]
                .acker
                .has_ack_eliciting_packet_in_flight()
            {
                continue;
            }
            let mut duration = duration;
            if matches!(p_space, PacketSpace::Data) {
                if !self.handshake_is_done() {
                    return pto_timeout;
                }
                duration += self.transport_params.max_ack_delay * 2u32.pow(self.pto_count);
            }

            let Some(last) = self.spaces[p_space].acker.time_of_last_elicit() else {
                // Skip if space has no sent packet
                continue;
            };
            let t = last + duration;

            pto_timeout = match pto_timeout {
                Some((pto, _)) if t < pto => Some((pto.min(t), p_space)),
                None => Some((t, p_space)),
                _ => pto_timeout,
            };
        }

        pto_timeout
    }

    fn handle_timeout(&mut self, time: Instant) {
        for timer in Timer::values() {
            if !self.timers.has_passed(timer, time) {
                continue;
            }

            self.timers.reset(timer);

            match timer {
                Timer::MaxAckDelay => {
                    // Don't need to do anything since the timer already wakes up connection driver
                }
                Timer::LossDetection => {
                    // Follows RFC 9002 A.9
                    let rtt = &self.rtt.clone();
                    if let Some((_, p_space)) = self.get_loss_time_and_space() {
                        let lost = self.spaces[p_space].acker.get_lost_frames(rtt);
                        self.congestion.on_loss(lost.as_slice(), rtt);
                        let lost_frames: Vec<_> =
                            lost.into_iter().flat_map(|lost| lost.frames).collect();

                        if !lost_frames.is_empty() {
                            println!(
                                "[TIMER] Found timedout packets({p_space:?})[{:?}]: {lost_frames:?}!!!!!!!!!!!!!!!!!!!!!!!",
                                self.side
                            );
                        }

                        self.add_lost_to_pending(p_space, lost_frames);
                        self.set_loss_timer();
                        return;
                    }

                    if !self.any_ack_eliciting_packet_in_flight() {
                        // TODO: Handle anti-deadlock packet
                        unimplemented!("Have not implemented anti-deadlock")
                    } else {
                        let p_space = self.pto_space();
                        let pto_frames = self.build_pto_probe(p_space);
                        self.spaces[p_space]
                            .pending_frames
                            .extend(pto_frames.into_iter());
                    }
                    self.pto_count += 1;
                    self.set_loss_timer();
                }
            }
        }
    }

    fn get_packet(&mut self, p_space: PacketSpace, mut mtu: usize) -> Option<Packet> {
        // println!("[ACK]: Getting packet for {p_space:?}");
        let pn = PacketNumber::from(self.spaces[p_space].packet_number);

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

        let size = header.size();
        if mtu < size {
            return None;
        }
        mtu -= size;

        let mut frames = Vec::new();

        while let Some(frame) = self.spaces[p_space].pending_frames.front() {
            if mtu > frame.size() {
                let frame = self.spaces[p_space].pending_frames.pop_front().unwrap();
                // Only retransmitt stream frames for send streams that has not sent reset
                if let Frame::Stream(Stream { id, .. }) = &frame {
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

        if let Some(ack) = self.spaces[p_space].acker.get_ack() {
            if mtu > ack.size() {
                mtu -= ack.size();
                frames.push(ack);
                self.timers.reset(Timer::MaxAckDelay);
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

                let frame = Frame::Stream(Stream {
                    id: id.clone(),
                    fin,
                    len: true,
                    offset: VarInt(offset),
                    data: data.freeze(),
                });

                if !send.empty() {
                    self.pending_send.push_back(id);
                }

                let size = frame.size();
                if mtu > size {
                    mtu -= size;
                    frames.push(frame);
                } else {
                    self.spaces[p_space].pending_frames.push_back(frame);
                    break;
                }
            }
        }

        if frames.is_empty() {
            return None;
        }

        let last = frames.last_mut().unwrap();
        if let Frame::Stream(Stream { fin, .. }) = last {
            *fin = true
        };

        let packet = Packet { header, frames };

        self.spaces[p_space].packet_number += 1;
        Some(packet)
    }

    fn build_pto_probe(&mut self, p_space: PacketSpace) -> Vec<Frame> {
        let unacked = self.spaces[p_space].acker.unacked_frames();
        if !unacked.is_empty() {
            unacked
        } else {
            vec![Frame::Ping]
        }
    }

    fn pto_space(&self) -> PacketSpace {
        if !self.spaces[PacketSpace::Initial].is_discarded()
            && self.spaces[PacketSpace::Initial]
                .acker
                .has_ack_eliciting_packet_in_flight()
        {
            PacketSpace::Initial
        } else if !self.spaces[PacketSpace::Handshake].is_discarded()
            && self.spaces[PacketSpace::Handshake]
                .acker
                .has_ack_eliciting_packet_in_flight()
        {
            PacketSpace::Handshake
        } else {
            PacketSpace::Data
        }
    }

    pub(super) fn packet_sent(
        &mut self,
        p_space: PacketSpace,
        pn: PacketNumber,
        frames: Vec<Frame>,
        is_in_flight: bool,
        is_ack_eliciting: bool,
    ) {
        self.spaces[p_space]
            .acker
            .sent_packet(pn, frames, is_in_flight, is_ack_eliciting);
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
pub struct ConnectionRef(Arc<Mutex<ConnectionIO>>);

impl Deref for ConnectionRef {
    type Target = Arc<Mutex<ConnectionIO>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct Connection {
    inner: ConnectionRef,
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
        let io = ConnectionRef(Arc::new(Mutex::new(ConnectionIO::new(
            cid,
            peer_cid,
            addr,
            side,
            transport_params,
            outgoing_data,
            connected,
        ))));
        let driver = ConnectionDriver {
            io: io.clone(),
            incoming_data,
            addr,
            timer: None,
        };

        tokio::spawn(driver);
        Connection { inner: io }
    }

    pub async fn accept_bidir(&self) -> (SendStreamHandle, RecvStreamHandle) {
        let id = poll_fn(|cx| {
            let mut inner = self.inner.lock().unwrap();
            inner.poll_accept_bidir(cx)
        })
        .await;
        (
            SendStreamHandle::new(id.clone(), self.inner.clone()),
            RecvStreamHandle::new(id.clone(), self.inner.clone()),
        )
    }

    pub async fn accept_uni(&self) -> RecvStreamHandle {
        let id = poll_fn(|cx| {
            let mut inner = self.inner.lock().unwrap();
            inner.poll_accept_unidir(cx)
        })
        .await;
        RecvStreamHandle::new(id.clone(), self.inner.clone())
    }

    pub fn open_bidir(&self) -> (RecvStreamHandle, SendStreamHandle) {
        let mut inner = self.inner.lock().unwrap();
        let id = inner.new_bidir();

        let recv_handle = RecvStreamHandle::new(id.clone(), self.inner.clone());
        let send_handle = SendStreamHandle::new(id.clone(), self.inner.clone());

        (recv_handle, send_handle)
    }

    // Send initial packet to peer
    pub(super) fn init(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.init();
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.write_fmt(format_args!(
            "Connection({:?}, {:?})",
            inner.core.id, inner.core.peer_cid
        ))
    }
}

pub struct IncomingInner {
    id: ConnectionID,
    // src_cid: ConnectionID,
    connected: oneshot::Receiver<()>,
}

impl IncomingInner {
    pub fn new(id: ConnectionID, connected: oneshot::Receiver<()>) -> Self {
        Self {
            id,
            // src_cid,
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

        let endpoint = this.endpoint.lock().unwrap();

        let connection = endpoint.connections.get(&this.inner.id).unwrap();
        Poll::Ready(connection.clone())
    }
}

pub struct SendStreamHandle {
    id: StreamID,
    connection: ConnectionRef,
}

pub struct RecvStreamHandle {
    id: StreamID,
    connection: ConnectionRef,
}

impl RecvStreamHandle {
    pub fn new(id: StreamID, connection: ConnectionRef) -> Self {
        Self { id, connection }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, ReadError> {
        let mut buf = ReadBuf::new(buf);
        poll_fn(|cx| {
            self.connection
                .lock()
                .unwrap()
                .poll_read(&mut buf, &self.id, cx)
        })
        .await
    }
}

impl SendStreamHandle {
    pub fn new(id: StreamID, connection: ConnectionRef) -> Self {
        Self { id, connection }
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize, SendError> {
        let mut inner = self.connection.lock().unwrap();
        inner.write(buf, self.id.clone())
    }
}

pub struct ConnectionIO {
    core: ConnectionCore,
    addr: SocketAddr,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    outgoing_data: UnboundedSender<ConnectionToEndpoint>,
}

impl ConnectionIO {
    pub(crate) fn new(
        cid: ConnectionID,
        peer_cid: ConnectionID,
        addr: SocketAddr,
        side: Side,
        transport_params: SharedConfig,
        outgoing_data: UnboundedSender<ConnectionToEndpoint>,
        // TODO: Remove
        connected: oneshot::Sender<()>,
    ) -> Self {
        let core = ConnectionCore::new(cid, peer_cid, side, connected, transport_params);
        Self {
            core,
            addr,
            timer: None,
            outgoing_data,
        }
    }

    pub(crate) fn poll_accept_bidir(&mut self, cx: &mut Context<'_>) -> Poll<StreamID> {
        if let Some(id) = self.core.bidir_acceptable.pop_front() {
            Poll::Ready(id)
        } else {
            self.core.bidir_accept_waker.register(cx.waker());
            Poll::Pending
        }
    }

    pub(crate) fn poll_accept_unidir(&mut self, cx: &mut Context<'_>) -> Poll<StreamID> {
        if let Some(id) = self.core.unidir_acceptable.pop_front() {
            Poll::Ready(id)
        } else {
            self.core.unidir_accept_waker.register(cx.waker());
            Poll::Pending
        }
    }

    pub(crate) fn new_bidir(&mut self) -> StreamID {
        let id = StreamID::new(self.core.stream_number, &self.core.side, Dir::Bidir);
        // TODO: Have seperate counters for each?
        // Currently this will shrink the number of possible stream ids
        self.core.stream_number += 1;
        let recv = RecvStream::new(id.clone());
        let send = SendStream::new(id.clone());

        self.core.recv.insert(id.clone(), recv);
        self.core.send.insert(id.clone(), send);

        id
    }

    pub(super) fn init(&mut self) {
        let crypto_data = CryptoFrame::ClientHello;
        let mut c_data = Vec::new();
        crypto_data.encode(&mut c_data);
        let crypto_frame = Frame::Crypto {
            offset: VarInt(0),
            data: c_data,
        };
        self.core.spaces[PacketSpace::Initial]
            .pending_frames
            .push_back(crypto_frame);
        self.core.state = PacketSpace::Handshake;
        self.core.write_waker.wake();
    }

    pub(crate) fn poll_read(
        &mut self,
        buf: &mut ReadBuf,
        id: &StreamID,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, ReadError>> {
        let Some(recv) = self.core.recv(id) else {
            return Poll::Ready(Err(ReadError::ClosedStream));
        };
        recv.poll_read(buf, cx)
    }

    pub(crate) fn write(&mut self, buf: &[u8], id: StreamID) -> Result<usize, SendError> {
        let Some(send) = self.core.send(&id) else {
            // Stream does not exist, this means that something is wrong
            // TODO: Different error?
            return Err(SendError::ClosedStream);
        };

        if !send.can_send() {
            return Err(SendError::WrongState);
        }

        send.set_buffer(buf);
        self.core.mark_pending_on_send(id);

        Ok(buf.len())
    }

    pub(crate) fn handle_data(&mut self, data: Vec<EndpointToConnection>) {
        for data in data {
            self.core.handle_datagram(&data.data);
        }
    }

    pub(crate) fn check_timeout(&mut self, cx: &mut Context<'_>) {
        let timer_fired = self
            .timer
            .as_mut()
            .map(|timer| timer.as_mut().poll(cx).is_ready())
            .unwrap_or(false);

        if timer_fired {
            self.timer = None;
            self.core.handle_timeout(Instant::now());
        }
    }

    pub(crate) fn drive_transmit(&mut self) {
        let mut packets = Vec::new();

        let mtu = 1200;
        for p_space in PacketSpace::iter() {
            if self.core.spaces[p_space].is_discarded() {
                continue;
            }
            while let Some(packet) = {
                let available = self.core.congestion.available_space();
                self.core.get_packet(p_space, mtu.min(available))
            } {
                self.core.congestion.on_packet_sent(packet.size() as u64);
                packets.push(packet);
            }
        }
        for packet in packets {
            println!(
                "Sending packet({:?})[{:?}]: {packet:?}",
                self.core.state, self.core.side
            );

            let mut buf = BytesMut::new();
            packet.encode(&mut buf).unwrap();
            self.outgoing_data
                .send(ConnectionToEndpoint {
                    data: buf,
                    addr: self.addr,
                })
                .unwrap();

            let is_in_flight = packet.is_in_flight();
            let is_ack_eliciting = packet.is_ack_eliciting();
            let p_space = packet.packet_space();
            self.core.packet_sent(
                p_space,
                packet.packet_number().clone(),
                packet.frames,
                is_in_flight,
                is_ack_eliciting,
            );

            if is_in_flight {
                self.core.set_loss_timer();
            }
        }
    }

    pub(crate) fn rearm_timer(&mut self, cx: &mut Context<'_>) {
        let deadline = self.core.next_deadline();
        match (deadline, self.timer.as_mut()) {
            (Some(deadline), Some(timer)) => {
                timer.as_mut().reset(deadline);
            }
            (Some(deadline), None) => {
                let mut sleep = Box::pin(tokio::time::sleep_until(deadline));
                let _ = sleep.as_mut().poll(cx); // register waker
                self.timer = Some(sleep);
            }
            (None, _) => {
                self.timer = None;
            }
        }
    }

    fn register_waker(&mut self, cx: &mut Context<'_>) {
        self.core.write_waker.register(cx.waker());
        self.core
            .spaces()
            .for_each(|space| space.acker.register_waker(cx.waker()));
    }
}

// Handles reeiving data from the endpoint and passing it to inner and seeing if pending data
// exists
struct ConnectionDriver {
    io: ConnectionRef,
    incoming_data: UnboundedReceiver<EndpointToConnection>,
    addr: SocketAddr,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Future for ConnectionDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.io.lock().unwrap().register_waker(cx);

        // Handle incomming packets
        let mut buffer = Vec::new();
        loop {
            match this.incoming_data.poll_recv(cx) {
                Poll::Ready(Some(data)) => buffer.push(data),
                Poll::Ready(None) => return Poll::Ready(()), // channel closed
                Poll::Pending => break,
            }
        }

        let mut conn = this.io.lock().unwrap();
        if !buffer.is_empty() {
            conn.handle_data(buffer);
        }

        // Handle timeout
        conn.check_timeout(cx);

        // Get packets from connection
        conn.drive_transmit();

        // Rearm timer
        conn.rearm_timer(cx);

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use futures::task::noop_waker_ref;

    use crate::ack_manager::Acker;

    use super::*;

    fn create_connection(
        sender: oneshot::Sender<()>,
        transport_params: SharedConfig,
    ) -> ConnectionCore {
        ConnectionCore {
            id: ConnectionID {
                len: 1,
                buf: [0; 20],
            },
            peer_cid: ConnectionID {
                len: 1,
                buf: [0; 20],
            },
            side: Side::Server,
            lifeycle: LifeCycle::Handshake,
            connected: Some(sender),
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
            spaces: [
                Space::new(PacketSpace::Initial, transport_params.clone()),
                Space::new(PacketSpace::Handshake, transport_params.clone()),
                Space::new(PacketSpace::Data, transport_params.clone()),
            ],
            transport_params: transport_params.clone(),
            rtt: RttEstimator::new(),
            pto_count: 0,
            congestion: CongestionController::new(transport_params),
            timers: TimerTable::new(),
        }
    }

    fn create_packet(
        p_space: PacketSpace,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        pn: PacketNumber,
        frames: Vec<Frame>,
    ) -> Bytes {
        let header = match p_space {
            PacketSpace::Initial => Header::Initial {
                dst_cid,
                src_cid,
                packet_number: pn,
                version: 0,
                token: None,
                length: VarInt(0),
            },
            PacketSpace::Handshake => Header::Handshake {
                dst_cid,
                src_cid,
                packet_number: pn,
                version: 0,
                length: VarInt(0),
            },
            PacketSpace::Data => Header::Short {
                spin: false,
                key_phase: false,
                dst_cid,
                packet_number: pn,
            },
        };

        let packet = Packet { header, frames };
        let mut buf = BytesMut::new();
        packet.encode(&mut buf).unwrap();
        buf.freeze()
    }

    fn get_packet(conn: &mut ConnectionCore, p_space: PacketSpace, mtu: usize) -> Packet {
        let Some(resp) = conn.get_packet(p_space, mtu) else {
            panic!("Could not get packet from {p_space:?} of mtu={mtu:?}");
        };
        resp
    }

    // TODO: Will have to update when the handshake updates
    fn server_initial_state(conn: &mut ConnectionCore) {
        let crypto_data = CryptoFrame::ClientHello;
        let mut c_data = Vec::new();
        crypto_data.encode(&mut c_data);
        let crypto_frame = Frame::Crypto {
            offset: VarInt(0),
            data: c_data,
        };

        let packet = create_packet(
            PacketSpace::Initial,
            conn.peer_cid,
            conn.id,
            PacketNumber::U8(0),
            vec![crypto_frame],
        );

        conn.handle_datagram(&packet);

        let Some(resp) = conn.get_packet(PacketSpace::Handshake, 1200) else {
            panic!("Could not get handshake packet after ClientHello");
        };

        assert!(matches!(resp.header, Header::Handshake { .. }));
        assert!(matches!(conn.state, PacketSpace::Handshake));
    }

    // TODO: Will have to update when the handshake updates
    fn server_handshake_state(conn: &mut ConnectionCore) {
        let crypto_data = CryptoFrame::Finished;
        let mut c_data = Vec::new();
        crypto_data.encode(&mut c_data);
        let crypto_frame = Frame::Crypto {
            offset: VarInt(0),
            data: c_data,
        };
        let packet = create_packet(
            PacketSpace::Handshake,
            conn.peer_cid,
            conn.id,
            PacketNumber::U8(0),
            vec![crypto_frame],
        );

        conn.handle_datagram(&packet);

        let Some(resp) = conn.get_packet(PacketSpace::Data, 1200) else {
            panic!("Could not get Handshake packet after Finished frame");
        };

        assert!(matches!(resp.header, Header::Short { .. }));
        assert!(matches!(conn.state, PacketSpace::Data));
        assert!(conn.handshake_is_done());
    }

    fn open_bidir(conn: &mut ConnectionCore) -> StreamID {
        let id = StreamID::new(conn.stream_number, &conn.side, Dir::Bidir);
        conn.stream_number += 1;
        let recv = RecvStream::new(id.clone());
        let send = SendStream::new(id.clone());

        conn.recv.insert(id.clone(), recv);
        conn.send.insert(id.clone(), send);

        id
    }

    fn send_stream_data_to(conn: &mut ConnectionCore, id: StreamID, data: Bytes, pn: &mut u8) {
        let frame = Frame::Stream(Stream {
            id,
            fin: false,
            len: false,
            offset: VarInt(0),
            data,
        });

        let packet = create_packet(
            PacketSpace::Data,
            conn.peer_cid,
            conn.id,
            PacketNumber::U8(*pn),
            vec![frame],
        );
        *pn += 1;

        conn.handle_datagram(&packet);
    }

    fn send_stream_data_from(conn: &mut ConnectionCore, id: StreamID, data: Bytes) -> Packet {
        let Some(send) = conn.send(&id) else {
            panic!("Could not find send stream with id={id:?}");
        };

        send.set_buffer(&data);
        println!("Wrote to send buffer");
        conn.pending_send.push_back(id);

        let packet = get_packet(conn, PacketSpace::Data, 1200);

        conn.spaces[PacketSpace::Data].acker.sent_packet(
            packet.packet_number().clone(),
            packet.frames.clone(),
            packet.is_in_flight(),
            packet.is_ack_eliciting(),
        );

        if packet.is_in_flight() {
            conn.set_loss_timer();
        }

        conn.congestion.on_packet_sent(packet.size() as u64);

        packet
    }

    fn send_ack_to(conn: &mut ConnectionCore, acker: &mut Acker, pn: &mut u8) {
        let frame = acker.get_ack().expect("Acker should not be cleared");

        let packet = create_packet(
            PacketSpace::Data,
            conn.peer_cid,
            conn.id,
            PacketNumber::U8(*pn),
            vec![frame],
        );

        *pn += 1;

        conn.handle_datagram(&packet);
    }

    #[test]
    fn test_handshake() {
        let (sender, mut receiver) = oneshot::channel();
        let transport_params = Arc::new(TransportParam::new());
        let mut connection = create_connection(sender, transport_params);

        server_initial_state(&mut connection);
        server_handshake_state(&mut connection);

        {
            // Checking if it notified the oneshot channel
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(Pin::new(&mut receiver).poll(&mut cx).is_ready());
            assert!(connection.handshake_is_done());
        }

        assert_eq!(connection.lifeycle, LifeCycle::Connected);
        assert_eq!(connection.state, PacketSpace::Data);
    }

    #[test]
    fn test_maxackdelay_timer() {
        let (sender, _receiver) = oneshot::channel();
        let transport_params = Arc::new(TransportParam::new());
        let mut connection = create_connection(sender, transport_params.clone());
        let mut pn = 0u8;
        server_initial_state(&mut connection);
        server_handshake_state(&mut connection);

        let id = open_bidir(&mut connection);
        send_stream_data_to(
            &mut connection,
            id.clone(),
            Bytes::from_static(&[1, 2, 3, 4, 5]),
            &mut pn,
        );

        // MaxAckDelay
        let next = connection.next_deadline();
        assert_eq!(next, connection.next_ack_deadline());

        let resp = get_packet(&mut connection, PacketSpace::Data, 1200);

        // Should only respond with an ack
        assert!(resp.frames.iter().all(|f| matches!(f, Frame::Ack { .. })));

        // Sent ack, so should be no MaxAckDelay deadline
        // No ack eliciting or in flight packets, so loss deadline should be None
        let next = connection.next_deadline();
        assert!(next.is_none());
    }

    #[test]
    fn test_loss_timer() {
        let (sender, _receiver) = oneshot::channel();
        let transport_params = Arc::new(TransportParam::new());
        let mut connection = create_connection(sender, transport_params.clone());
        let mut acker = Acker::new(transport_params);
        let mut pn = 0u8;
        server_initial_state(&mut connection);
        server_handshake_state(&mut connection);

        let id = open_bidir(&mut connection);
        // Sending one stream packets and acking
        let sending_bytes = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let sent_packet = send_stream_data_from(&mut connection, id.clone(), sending_bytes.clone());
        acker.received(sent_packet.packet_number(), sent_packet.is_ack_eliciting());

        // Testing loss
        let next = connection.next_deadline();
        assert!(next.is_some());

        // The loss should be pto since only no ack has been received
        let (pto, p_space) = connection.get_pto_time_and_space().unwrap();
        assert_eq!(next.unwrap(), pto);
        assert_eq!(p_space, PacketSpace::Data);

        // Conn sends two packets
        let sending_bytes = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let unacked = send_stream_data_from(&mut connection, id.clone(), sending_bytes.clone());
        let sent_packet = send_stream_data_from(&mut connection, id.clone(), sending_bytes.clone());
        // Only last packet is acked -> first should set loss timer when conn receives ack frame
        acker.received(sent_packet.packet_number(), sent_packet.is_ack_eliciting());

        // Sending ack
        // Should set loss time
        send_ack_to(&mut connection, &mut acker, &mut pn);
        let next = connection.next_deadline();

        // The loss should be packet loss time since a packet before the acked packet is unacked
        assert!(next.is_some());
        let (loss, p_space) = connection.get_loss_time_and_space().unwrap();
        assert_eq!(next.unwrap(), loss);
        assert_eq!(p_space, PacketSpace::Data);

        // Now the unacked packet is acked, so it should reset the loss timer
        acker.received(unacked.packet_number(), unacked.is_ack_eliciting());
        send_ack_to(&mut connection, &mut acker, &mut pn);
        let next = connection.next_deadline();
        assert!(next.is_none());
        assert_eq!(connection.pto_count, 0);
    }

    #[test]
    fn test_send_data_from() {
        let (sender, _receiver) = oneshot::channel();
        let transport_params = Arc::new(TransportParam::new());
        let mut connection = create_connection(sender, transport_params.clone());
        let mut acker = Acker::new(transport_params);
        server_initial_state(&mut connection);
        server_handshake_state(&mut connection);

        let id = open_bidir(&mut connection);
        // Sending one stream packets and acking
        let sending_bytes = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let sent_packet = send_stream_data_from(&mut connection, id.clone(), sending_bytes.clone());
        acker.received(sent_packet.packet_number(), sent_packet.is_ack_eliciting());

        let Some(Frame::Stream(frame)) = sent_packet
            .frames
            .iter()
            .find(|f| matches!(f, Frame::Stream(_)))
        else {
            panic!("No stream frame after sending data");
        };
        assert_eq!(frame.data, sending_bytes);
    }
}
