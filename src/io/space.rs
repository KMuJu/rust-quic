use std::collections::VecDeque;

use crate::io::{
    ack_manager::Acker,
    encoding::VarInt,
    frame::{AckRange, EcnCounts, Frame},
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

    HandshakeDone,
}

pub(super) struct Space {
    space: PacketSpace,
    pub(super) acker: Acker,
    pub(super) pending_frames: VecDeque<Frame>,
    packet_number: usize,
}

impl Space {
    pub(super) fn new(space: PacketSpace) -> Self {
        Self {
            space,
            acker: Acker::new(),
            pending_frames: VecDeque::new(),
            packet_number: 0,
        }
    }

    pub(super) fn handle_frame(&mut self, frame: Frame) -> Option<SpaceEvent> {
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
            Frame::Ping => todo!(),
            Frame::Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            } => {
                self.handle_ack_frame(largest, delay, first_range, ranges, ecn);
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
    ) {
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

        for ack in acked {
            self.acker.acked(ack);
        }

        // TODO: Handle
        // - declare lost: pn + 3 <= largest  (packet threshold)
        // - declare lost: sent_at + time_threshold <= largest_sent_at  (time threshold)
        // - re-enqueue retransmittable frames from lost packets
        // - arm loss_timer for remaining unacked packets that might still be lost
        // - arm PTO timer (separate, fires if no ACK arrives within ~3× RTT)

        let lost = self.acker.get_lost_frames(largest.0 as u32);
        self.add_lost_to_pending(lost);
    }

    fn add_lost_to_pending(&mut self, lost: Vec<Frame>) {
        // println!("Lost these frames({space:?})[{:?}]: {lost:?}", self.side);
        for frame in lost {
            self.pending_frames.push_back(frame);
        }
    }

    pub(super) fn clear(&mut self) {
        self.acker.clear();
        self.pending_frames = VecDeque::new();
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
