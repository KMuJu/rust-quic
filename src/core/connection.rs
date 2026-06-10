use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Buf, Bytes, BytesMut};
use rand::{Rng, rngs::StdRng};
use tokio::{io::ReadBuf, time::Instant};

use crate::{
    core::{
        congestion::CongestionController,
        crypto::{self, CryptoConn, handshake_type},
        encoding::VarInt,
        frame::{Ack, Frame, Stream},
        packet::{Header, Packet, PacketNumber, ProtectedPacket},
        rtt_estimator::RttEstimator,
        space::{PacketSpace, Space},
        stream::{Dir, RecvStream, SendStream, Side, StreamID},
        timers::{Timer, TimerTable},
        transport_parameters::SharedConfig,
    },
    io::connection::{ReadError, SendError},
};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum LifeCycle {
    Handshake,
    Connected,
    // TODO: Add closed and such
}

/// Output events from the connection
#[derive(PartialEq, Eq, Debug)]
pub enum ConnEvent {
    HandshakeDone,
    CloseConnection,
    Stream(StreamEvent),
}

#[derive(PartialEq, Eq, Debug)]
pub enum StreamEvent {
    NewStream { id: StreamID },
    ClosedStream { id: StreamID },
    Readable { id: StreamID },
    Writeable { id: StreamID },
}

pub struct ConnectionCore {
    id: ConnectionID,
    // Peer's cid
    peer_cid: ConnectionID,
    side: Side,
    lifecycle: LifeCycle,

    recv: HashMap<StreamID, RecvStream>,
    send: HashMap<StreamID, SendStream>,

    // Streams that can be accepted
    bidir_acceptable: VecDeque<StreamID>,
    unidir_acceptable: VecDeque<StreamID>,

    // Streams that have data to send
    pending_recv: VecDeque<StreamID>,
    pending_send: VecDeque<StreamID>,

    stream_number: usize,

    highest_space_with_keys: PacketSpace,
    spaces: [Space; 3],

    transport_params: SharedConfig,
    rtt: RttEstimator,
    pto_count: u32,

    congestion: CongestionController,
    timers: TimerTable,

    conn_event_queue: VecDeque<ConnEvent>,

    crypto: CryptoConn,
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
        crypto: CryptoConn,
        transport_params: SharedConfig,
    ) -> Self {
        let mut conn = ConnectionCore {
            id: cid,
            peer_cid,
            side,
            lifecycle: LifeCycle::Handshake,
            recv: HashMap::new(),
            send: HashMap::new(),
            bidir_acceptable: VecDeque::new(),
            unidir_acceptable: VecDeque::new(),
            pending_recv: VecDeque::new(),
            pending_send: VecDeque::new(),
            stream_number: 0,
            highest_space_with_keys: PacketSpace::Initial,
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
            conn_event_queue: VecDeque::new(),
            crypto,
        };

        {
            let cid = match side {
                Side::Client => cid,
                Side::Server => peer_cid,
            };
            let initial_keys = conn.crypto.initial_keys(&cid.buf[..cid.len as usize]);

            conn.spaces[PacketSpace::Initial].set_keys(initial_keys);
        }

        conn
    }

    pub fn handle_datagram(&mut self, protected: ProtectedPacket) {
        let p_space = protected.packet_space();
        let keys = self.spaces[p_space]
            .keys
            .as_ref()
            .unwrap_or_else(|| panic!("Don't have keys for {:?}", p_space));
        let packet = match protected.unprotect(keys, self.spaces[p_space].largest_received) {
            Ok(p) => p,
            Err(e) => {
                println!("Error when decoding datagram: {e}");
                return;
            }
        };
        let p_space = packet.packet_space();
        let pn = packet
            .packet_number()
            .expand(self.spaces[p_space].largest_received + 1);
        println!(
            "{:?}({:?})[{:?}]: Got {:?}",
            self.id, self.lifecycle, self.side, packet
        );
        if !self.spaces[p_space]
            .acker
            .received(pn, packet.is_ack_eliciting())
        {
            eprintln!("ERROR: Got dulicate packet!({:?}): {}", p_space, pn);
            return;
        }

        self.spaces[p_space].largest_received = self.spaces[p_space].largest_received.max(pn);

        match self.lifecycle {
            LifeCycle::Handshake => {}
            LifeCycle::Connected => match packet.packet_space() {
                PacketSpace::Data => return self.handle_data_packet(packet),
                _ => {
                    eprintln!("Unexpected handshake packet when connected")
                }
            },
        };

        match &packet.header {
            Header::Initial { .. } => {
                if self.spaces[packet.packet_space()].is_discarded() {
                    return;
                }

                self.handle_handshake_packet(packet);
            }
            Header::Handshake { .. } => {
                println!("[CRYPTO] GOT HANDSHAKE PACKET");
                if self.spaces[packet.packet_space()].is_discarded() {
                    return;
                }

                self.handle_handshake_packet(packet);
                if self.crypto.is_handshaking() {
                    return;
                }

                self.lifecycle = LifeCycle::Connected;
                self.consume_connected();

                match self.side {
                    Side::Client => {}
                    Side::Server => {
                        self.spaces[PacketSpace::Data].push_back_frame(Frame::HandshakeDone);
                        self.spaces[PacketSpace::Handshake].discard();
                    }
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
                Frame::Crypto { offset, data } => self.read_crypto(p_space, offset, data),
                Frame::ConnectionCloseQuic {
                    error_code,
                    frame_type,
                    reason_phrase,
                } => todo!(),
                _ => {
                    unreachable!(
                        "Got illigal frame type when processing handshake packet: {frame:?}"
                    )
                }
            }
        }

        self.write_crypto();
    }

    // TODO: Actually use a crypto stream from an assembler
    fn read_crypto(&mut self, p_space: PacketSpace, offset: VarInt, data: Vec<u8>) {
        println!("[CRYPTO] got crypto frame: {:?}", handshake_type(&data));
        self.crypto.read_handshake(&data).unwrap();
    }

    fn write_crypto(&mut self) {
        loop {
            let p_space = self.highest_space_with_keys;
            let mut crypto_data = Vec::new();
            let keys = self.crypto.write_crypto(&mut crypto_data);
            if let Some(keys) = keys {
                debug_assert!(
                    p_space != PacketSpace::Data,
                    "Can't upgrade crypto during Data packet space"
                );
                match p_space {
                    PacketSpace::Initial => self.upgrade_crypto(PacketSpace::Handshake, keys),
                    PacketSpace::Handshake => self.upgrade_crypto(PacketSpace::Data, keys),
                    PacketSpace::Data => unreachable!(),
                }
            }

            if crypto_data.is_empty() {
                if p_space == self.highest_space_with_keys {
                    break;
                } else {
                    continue;
                }
            }

            let crypto_frame = Frame::Crypto {
                offset: VarInt(0),
                data: crypto_data,
            };

            self.spaces[p_space].push_back_frame(crypto_frame);
        }
    }

    /// Follows RFC 9002 A.7
    fn on_ack_received(&mut self, p_space: PacketSpace, ack: Ack) {
        let mut acked = Vec::new();
        let mut current = ack.largest.0;

        // First range: largest down to largest - first_range
        for pn in (current - ack.first_range.0..=current).rev() {
            acked.push(pn);
        }
        current -= ack.first_range.0;

        // Subsequent ranges: skip gap+2, then ack length+1
        for range in ack.ranges {
            current -= range.gap.0 + 2;
            for pn in (current - range.length.0..=current).rev() {
                acked.push(pn);
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
        if largest_acked.pn == ack.largest.0 && newly_acked.iter().any(|acked| acked.ack_eliciting)
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

        let lost = self.spaces[p_space]
            .acker
            .get_lost_frames(&self.rtt, Instant::now());
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
        self.conn_event_queue.push_back(ConnEvent::HandshakeDone);
        self.lifecycle = LifeCycle::Connected;
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
                self.send.insert(*id, SendStream::new(*id));
                self.recv.insert(*id, RecvStream::new(*id));
                self.bidir_acceptable.push_back(*id);
            }
            Dir::Uni => {
                self.recv.insert(*id, RecvStream::new(*id));
                self.unidir_acceptable.push_back(*id);
            }
        }

        self.conn_event_queue
            .push_back(ConnEvent::Stream(StreamEvent::NewStream { id: *id }));
    }

    fn peer_completed_address_validation(&self) -> bool {
        // TODO: Do properly
        self.handshake_is_done()
    }

    pub(crate) fn next_ack_deadline(&self) -> Option<Instant> {
        self.spaces
            .iter()
            .filter_map(|s| s.acker.get_deadline())
            .min()
    }

    /// Follows RFC 9002 A.8
    pub(super) fn get_loss_time_and_space(&self) -> Option<(Instant, PacketSpace)> {
        self.spaces
            .iter()
            .filter_map(|s| Some((s.acker.get_loss_time()?, s.space)))
            .min_by_key(|(time, _)| *time)
    }

    fn any_ack_eliciting_packet_in_flight(&self) -> bool {
        self.spaces
            .iter()
            .any(|s| s.acker.has_ack_eliciting_packet_in_flight())
    }

    pub fn next_deadline(&mut self) -> Option<Instant> {
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
    pub fn set_loss_timer(&mut self) {
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
    pub(super) fn get_pto_time_and_space(&self) -> Option<(Instant, PacketSpace)> {
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

    pub(crate) fn handle_timeout(&mut self, time: Instant) {
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
                        let lost = self.spaces[p_space].acker.get_lost_frames(rtt, time);
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

    fn get_packet(&mut self, p_space: PacketSpace, mtu: usize) -> Option<Packet> {
        let available = self.congestion.available_space();
        let mut mtu = mtu.min(available);
        let pn = PacketNumber::new(
            self.spaces[p_space].packet_number,
            self.spaces[p_space].acker.largest_acked.unwrap_or(0),
        );

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
                    id,
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

    pub fn get_encrypted_packet(&mut self, p_space: PacketSpace, mtu: usize) -> Option<Bytes> {
        let packet = self.get_packet(p_space, mtu)?;
        let keys = self.spaces[p_space]
            .keys
            .as_ref()
            .unwrap_or_else(|| panic!("Need {p_space:?} keys in order to encrypt packet"));
        let bytes = crypto::encrypt_packet(&packet, keys, self.spaces[p_space].largest_received);

        let size = packet.size() as u64;
        let is_in_flight = packet.is_in_flight();
        let is_ack_eliciting = packet.is_ack_eliciting();
        let p_space = packet.packet_space();
        self.packet_sent(
            p_space,
            packet.packet_number().clone(),
            packet.frames,
            is_in_flight,
            is_ack_eliciting,
            size,
        );

        Some(bytes)
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

    pub(crate) fn packet_sent(
        &mut self,
        p_space: PacketSpace,
        pn: PacketNumber,
        frames: Vec<Frame>,
        is_in_flight: bool,
        is_ack_eliciting: bool,
        sent_bytes: u64,
    ) {
        self.congestion.on_packet_sent(sent_bytes);
        let pn = pn.expand(self.spaces[p_space].largest_received + 1);
        self.spaces[p_space]
            .acker
            .sent_packet(pn, frames, is_in_flight, is_ack_eliciting);

        if is_in_flight {
            self.set_loss_timer();
        }
    }

    pub fn recv(&mut self, id: &StreamID) -> Option<&mut RecvStream> {
        self.recv.get_mut(id)
    }

    pub fn send(&mut self, id: &StreamID) -> Option<&mut SendStream> {
        self.send.get_mut(id)
    }

    /// Handshake is done if the lifecycle is past the handshake stage
    /// Should not be able to get back to handshake state afterwards
    fn handshake_is_done(&self) -> bool {
        self.lifecycle > LifeCycle::Handshake
    }

    pub(crate) fn new_bidir(&mut self) -> StreamID {
        let id = StreamID::new(self.stream_number, &self.side, Dir::Bidir);
        // TODO: Have seperate counters for each?
        // Currently this will shrink the number of possible stream ids
        self.stream_number += 1;
        let recv = RecvStream::new(id);
        let send = SendStream::new(id);

        self.recv.insert(id, recv);
        self.send.insert(id, send);

        id
    }

    pub(crate) fn init(&mut self) {
        self.write_crypto();
    }

    pub(crate) fn poll_read(
        &mut self,
        buf: &mut ReadBuf,
        id: &StreamID,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, ReadError>> {
        let Some(recv) = self.recv(id) else {
            return Poll::Ready(Err(ReadError::ClosedStream));
        };
        recv.poll_read(buf, cx)
    }

    // TODO: Handle max data
    // Will have to wait if max data is hit
    pub(crate) fn write(&mut self, buf: &[u8], id: StreamID) -> Result<usize, SendError> {
        let Some(send) = self.send(&id) else {
            // Stream does not exist, this means that something is wrong
            // TODO: Different error?
            return Err(SendError::ClosedStream);
        };

        if !send.can_send() {
            return Err(SendError::WrongState);
        }

        send.set_buffer(buf);
        self.pending_send.push_back(id);

        Ok(buf.len())
    }

    pub(crate) fn is_discarded(&self, p_space: PacketSpace) -> bool {
        self.spaces[p_space].is_discarded()
    }

    pub(crate) fn poll_accept_bidir(&mut self) -> Option<StreamID> {
        self.bidir_acceptable.pop_front()
    }

    pub(crate) fn poll_accept_unidir(&mut self) -> Option<StreamID> {
        self.unidir_acceptable.pop_front()
    }

    pub(crate) fn poll_conn_event(&mut self) -> Option<ConnEvent> {
        self.conn_event_queue.pop_front()
    }

    fn upgrade_crypto(&mut self, p_space: PacketSpace, keys: rustls::quic::Keys) {
        self.spaces[p_space].set_keys(keys);
        self.highest_space_with_keys = p_space;
    }
}
