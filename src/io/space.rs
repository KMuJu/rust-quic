use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;

use crate::io::{
    ack_manager::Acker,
    encoding::VarInt,
    frame::{AckRange, EcnCounts, Frame},
    rtt_estimator::RttEstimator,
    stream::StreamID,
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

pub(super) enum SpaceEvent {
    RecvStreamFrame(StreamID, Frame),
    SendStreamFrame(StreamID, Frame),
    Crypto(VarInt, Vec<u8>),

    RttSample(Duration, Duration),

    HandshakeDone,
}

pub(super) struct Space {
    space: PacketSpace,
    discarded: bool,
    pub(super) acker: Acker,
    pub(super) pending_frames: VecDeque<Frame>,
    packet_number: usize,
}

impl Space {
    pub(super) fn new(space: PacketSpace) -> Self {
        Self {
            space,
            discarded: false,
            acker: Acker::new(),
            pending_frames: VecDeque::new(),
            packet_number: 0,
        }
    }

    pub(super) fn handle_frame(&mut self, frame: Frame, rtt: &RttEstimator) -> Option<SpaceEvent> {
        if self.discarded {
            return None;
        }
        match &frame {
            Frame::StreamDataBlocked { id, .. }
            | Frame::ResetStream { id, .. }
            | Frame::Stream { id, .. } => {
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
                return Some(SpaceEvent::Crypto(*offset, data.clone()));
            }
            Frame::Padding => todo!(),
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
                return self.handle_ack_frame(largest, delay, first_range, ranges, ecn, rtt);
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

        // println!("Acked({space:?})[{:?}]: {acked:?}", self.side);

        let largest_sent_at = self.acker.sent_timestamp(largest.0 as u32);
        for ack in acked {
            self.acker.acked(ack);
        }

        let lost = self.acker.get_lost_frames(rtt);
        self.add_lost_to_pending(lost);

        if let Some(largest_sent_at) = largest_sent_at {
            return Some(SpaceEvent::RttSample(
                Instant::now() - largest_sent_at,
                Duration::from_millis(delay.0 << 3),
                // TODO: Handle transport parameter ack_delay_exponent
            ));
        }
        None
    }

    fn add_lost_to_pending(&mut self, lost: Vec<Frame>) {
        // println!("Lost these frames({space:?})[{:?}]: {lost:?}", self.side);
        for frame in lost {
            self.pending_frames.push_back(frame);
        }
    }

    pub(super) fn discard(&mut self) {
        self.discarded = false;
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
