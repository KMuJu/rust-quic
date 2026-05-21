use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;

use crate::io::{
    ack_manager::Acker,
    congestion::CongestionController,
    encoding::VarInt,
    frame::{AckRange, EcnCounts, Frame, StreamData},
    rtt_estimator::RttEstimator,
    stream::StreamID,
    transport_parameters::SharedConfig,
};

#[derive(Debug, Clone, Copy)]
pub(super) enum PacketSpace {
    Initial,
    Handshake,
    Data,
}

impl PacketSpace {
    pub(super) fn iter() -> impl Iterator<Item = Self> {
        [Self::Initial, Self::Handshake, Self::Data].into_iter()
    }
}
pub(super) struct RttSample {
    pub(super) latest_rtt: Duration,
    pub(super) ack_delay: Duration,
}

pub(super) enum SpaceEvent {
    RecvStreamFrame(StreamID, Frame),
    SendStreamFrame(StreamID, Frame),
    Crypto { offset: VarInt, data: Vec<u8> },

    Ack(Option<RttSample>),

    HandshakeDone,
}

pub(super) struct Space {
    pub(super) space: PacketSpace,
    discarded: bool,
    pub(super) acker: Acker,
    pub(super) pending_frames: VecDeque<Frame>,
    pub(super) packet_number: usize,
    transport_params: SharedConfig,
}

impl Space {
    pub(super) fn new(space: PacketSpace, transport_params: SharedConfig) -> Self {
        Self {
            space,
            discarded: false,
            acker: Acker::new(transport_params.clone()),
            pending_frames: VecDeque::new(),
            packet_number: 0,
            transport_params,
        }
    }

    pub(super) fn handle_frame(
        &mut self,
        frame: Frame,
        rtt: &RttEstimator,
        congestion: &mut CongestionController,
    ) -> Option<SpaceEvent> {
        if self.discarded {
            return None;
        }
        match &frame {
            Frame::StreamDataBlocked { id, .. }
            | Frame::ResetStream { id, .. }
            | Frame::Stream(StreamData { id, .. }) => {
                if matches!(self.space, PacketSpace::Data) {
                    return Some(SpaceEvent::RecvStreamFrame(id.clone(), frame));
                }
            }
            Frame::MaxStreamData { id, .. } | Frame::StopSending { id, .. } => {
                if matches!(self.space, PacketSpace::Data) {
                    return Some(SpaceEvent::SendStreamFrame(id.clone(), frame));
                }
            }
            Frame::Crypto { offset, data } => {
                return Some(SpaceEvent::Crypto {
                    offset: *offset,
                    data: data.clone(),
                });
            }
            Frame::Padding => {}
            Frame::Ping => {
                if let Some(ack) = self.acker.get_ack() {
                    self.pending_frames.push_back(ack);
                }
            }
            Frame::Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            } => {
                return self.handle_ack_frame(
                    largest,
                    delay,
                    first_range,
                    ranges,
                    ecn,
                    rtt,
                    congestion,
                );
            }
            Frame::NewToken { .. } => todo!(),
            Frame::MaxData { .. } => todo!(),
            Frame::MaxStreams { .. } => todo!(),
            Frame::DataBlocked { .. } => todo!(),
            Frame::StreamsBlocked { .. } => todo!(),
            Frame::NewConnectionId { .. } => todo!(),
            Frame::RetireConnectionId { .. } => todo!(),
            Frame::PathChallenge { .. } => todo!(),
            Frame::PathResponse { .. } => todo!(),
            Frame::ConnectionCloseQuic { .. } => todo!(),
            Frame::ConnectionCloseApp { .. } => todo!(),
            Frame::HandshakeDone => {
                // TODO: This is just a mock
                if matches!(self.space, PacketSpace::Data) {
                    return Some(SpaceEvent::HandshakeDone);
                }
            }
        };

        None
    }

    fn handle_ack_frame(
        &mut self,
        largest: &VarInt,
        delay: &VarInt,
        first_range: &VarInt,
        ranges: &Vec<AckRange>,
        ecn: &Option<EcnCounts>,
        rtt: &RttEstimator,
        congestion: &mut CongestionController,
    ) -> Option<SpaceEvent> {
        let mut acked = Vec::new();
        let mut current = largest.0;

        // First range: largest down to largest - first_range
        for pn in (current - first_range.0..=current).rev() {
            acked.push(pn as u32);
        }
        current -= first_range.0;

        // Subsequent ranges: skip gap+2, then ack length+1
        for range in ranges {
            current -= range.gap.0 + 2;
            for pn in (current - range.length.0..=current).rev() {
                acked.push(pn as u32);
            }
            current -= range.length.0;
        }

        let newly_acked: Vec<_> = acked
            .iter()
            .filter_map(|&pn| self.acker.acked(pn))
            .collect();

        if newly_acked.is_empty() {
            return None;
        }

        let mut sample = None;
        let largest_acked = newly_acked.iter().max_by_key(|acked| acked.pn).unwrap();
        if largest_acked.pn == (largest.0 as u32)
            && newly_acked.iter().any(|acked| acked.ack_eliciting)
        {
            sample = Some(RttSample {
                latest_rtt: Instant::now() - largest_acked.timestamp,
                ack_delay: Duration::from_millis(
                    delay.0 << self.transport_params.ack_delay_exponent,
                ),
            })
        }

        let lost_packets = self.acker.get_lost_frames(rtt);
        if !lost_packets.is_empty() {
            congestion.on_loss(lost_packets.as_slice(), rtt);
        }

        newly_acked
            .iter()
            .filter(|acked| acked.in_flight)
            .for_each(|acked| congestion.on_ack(acked.sent_bytes as u64, acked.timestamp));

        Some(SpaceEvent::Ack(sample))
    }

    fn add_lost_to_pending(&mut self, lost: Vec<Frame>) {
        // println!("Lost these frames({space:?})[{:?}]: {lost:?}", self.side);
        for frame in lost {
            self.pending_frames.push_back(frame);
        }
    }

    pub(super) fn discard(&mut self) {
        self.discarded = true;
        self.acker.clear();
        self.pending_frames = VecDeque::new();
    }

    pub(super) fn is_discarded(&self) -> bool {
        self.discarded
    }

    pub(super) fn push_back_frame(&mut self, frame: Frame) {
        self.pending_frames.push_back(frame);
    }

    pub(super) fn new_packet_num(&mut self) -> usize {
        let pn = self.packet_number;
        self.packet_number += 1;
        pn
    }
}
