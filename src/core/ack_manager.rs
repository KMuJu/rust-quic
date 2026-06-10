use std::{collections::BTreeMap, ops::RangeInclusive, time::Duration};

use range_set::RangeSet;
use tokio::time::Instant;

use crate::core::{
    encoding::VarInt,
    frame::{Ack, AckRange, Frame},
    packet::{DecodedPacketNumber, PacketNumber},
    rtt_estimator::RttEstimator,
    transport_parameters::SharedConfig,
};

const PACKET_THRESHOLD: DecodedPacketNumber = 3;

pub(super) struct SentPacket {
    pub(super) frames: Vec<Frame>,
    pub(super) pn: DecodedPacketNumber,
    pub(super) timestamp: Instant,
    pub(super) sent_bytes: usize,
    pub(super) ack_eliciting: bool,
    pub(super) in_flight: bool, // if it counts towards congestion
}

struct LargestReceived {
    packet_number: DecodedPacketNumber,
    timestamp: Instant,
}

pub(super) struct Acker {
    cleared: bool,
    transport_params: SharedConfig,
    received: RangeSet<[RangeInclusive<DecodedPacketNumber>; 8]>,
    sent: BTreeMap<DecodedPacketNumber, SentPacket>,
    eliciting_count: u32,
    ack_deadline: Option<Instant>,
    loss_time: Option<Instant>,
    pub(super) largest_acked: Option<DecodedPacketNumber>,
    largest_received: Option<LargestReceived>,
    last_elicit_time: Option<Instant>,

    need_to_send_ack: bool,
}

impl Acker {
    pub(super) fn new(transport_params: SharedConfig) -> Self {
        Self {
            cleared: false,
            transport_params,
            received: RangeSet::new(),
            sent: BTreeMap::new(),
            eliciting_count: 0,
            ack_deadline: None,
            loss_time: None,
            largest_acked: None,
            largest_received: None,
            last_elicit_time: None,
            need_to_send_ack: false,
        }
    }

    /// # Returns
    /// true if not duplicate
    /// false if duplicate
    pub(super) fn received(&mut self, pn: DecodedPacketNumber, elicit: bool) -> bool {
        if self.cleared {
            return true;
        }

        if self.received.insert(pn) {
            if let Some(largest) = &self.largest_received {
                if pn > largest.packet_number {
                    self.largest_received = Some(LargestReceived {
                        packet_number: pn,
                        timestamp: Instant::now(),
                    })
                }
            } else {
                self.largest_received = Some(LargestReceived {
                    packet_number: pn,
                    timestamp: Instant::now(),
                })
            }
            if elicit {
                self.eliciting_count += 1;
                if self.eliciting_count >= 2 {
                    self.need_to_send_ack = true;
                    println!("Eliciting packet count >= 2: NEED TO SEND ACK");
                }
                if self.ack_deadline.is_none() {
                    self.ack_deadline = Some(Instant::now() + self.transport_params.max_ack_delay);
                }
            }

            return true;
        }

        false
    }

    pub(super) fn get_deadline(&self) -> Option<Instant> {
        if self.cleared {
            return None;
        }
        self.ack_deadline
    }

    pub(super) fn get_loss_time(&self) -> Option<Instant> {
        if self.cleared {
            return None;
        }
        self.loss_time
    }

    pub(super) fn get_ack(&mut self) -> Option<Frame> {
        if !self.cleared && self.eliciting_count != 0 {
            self.eliciting_count = 0;
            self.ack_deadline = None;
            Some(self.build_frame())
        } else {
            None
        }
    }

    fn build_frame(&self) -> Frame {
        let mut iter = self.received.as_ref().iter().rev();

        let first = iter.next().unwrap();
        let largest = *first.end();
        let first_range = first.end() - first.start();

        let largest_time = match &self.largest_received {
            Some(l) => l.timestamp,
            None => panic!("Tried to build frame before having received packets"),
        };

        let delay_micros = (Instant::now() - largest_time).as_micros();
        let encoded_delay = delay_micros >> self.transport_params.ack_delay_exponent;

        let mut ranges = Vec::new();
        let mut prev_end = *first.start();

        for range in iter {
            let gap = prev_end - range.end() - 2;
            let length = range.end() - range.start();
            ranges.push(AckRange {
                gap: VarInt(gap),
                length: VarInt(length),
            });
            prev_end = *range.start();
        }

        Frame::Ack(Ack {
            largest: VarInt(largest),
            delay: VarInt(encoded_delay as u64),
            first_range: VarInt(first_range),
            ranges,
            ecn: None,
        })
    }

    pub(super) fn sent_packet(
        &mut self,
        pn: DecodedPacketNumber,
        in_frames: Vec<Frame>,
        in_flight: bool,
        ack_eliciting: bool,
    ) {
        if self.cleared {
            return;
        }
        if ack_eliciting {
            self.last_elicit_time = Some(Instant::now());
        }

        // Only retransmit retransmitable frames
        // TODO: Handle MAX_* frames
        let mut frames = Vec::new();
        for frame in in_frames {
            if frame.is_retransmittable() {
                frames.push(frame);
            }
        }
        if frames.is_empty() {
            return;
        }

        let sent = SentPacket {
            pn,
            timestamp: Instant::now(),
            sent_bytes: frames.iter().map(|f| f.size()).sum(),
            ack_eliciting,
            in_flight,
            frames,
        };

        self.sent.insert(pn, sent);
    }

    /// Roughly follows RFC 9002 A.10
    pub(super) fn get_lost_frames(&mut self, rtt: &RttEstimator, now: Instant) -> Vec<SentPacket> {
        let mut lost = Vec::new();
        if self.cleared {
            return lost;
        }
        self.loss_time = None;
        assert!(
            self.largest_acked.is_some(),
            "Largest acked was not set: {:?}",
            self.largest_acked
        );
        let largest_acked = self.largest_acked.unwrap();
        let loss_delay = rtt.latest_rtt.max(rtt.smoothed) * 9 / 8;
        let loss_delay = loss_delay.max(Duration::from_millis(1));
        let loss_send_time = now - loss_delay;

        let mut lost_pns = Vec::new();
        for (pn, s) in self.sent.iter() {
            if *pn > largest_acked {
                continue;
            }

            if s.timestamp <= loss_send_time || largest_acked > pn + PACKET_THRESHOLD {
                lost_pns.push(*pn);
            } else if let Some(deadline) = self.loss_time {
                self.loss_time = Some(deadline.min(s.timestamp + loss_delay));
            } else {
                self.loss_time = Some(s.timestamp + loss_delay);
            }
        }

        for pn in lost_pns {
            let sent = self.sent.remove(&pn).unwrap();
            lost.push(sent);
        }

        lost
    }

    pub(super) fn acked(&mut self, packet_number: DecodedPacketNumber) -> Option<SentPacket> {
        self.largest_acked = match self.largest_acked {
            Some(l) => Some(l.max(packet_number)),
            None => Some(packet_number),
        };
        self.sent.remove(&packet_number)
    }

    pub(super) fn unacked_frames(&self) -> Vec<Frame> {
        self.sent
            .values()
            .find(|&s| s.in_flight && s.ack_eliciting)
            .map(|s| s.frames.clone())
            .unwrap_or(Vec::new())
    }

    pub(super) fn clear(&mut self) {
        self.cleared = true;
        self.sent.clear();
        self.received.clear();
        self.ack_deadline = None;
    }

    pub(super) fn has_ack_eliciting_packet_in_flight(&self) -> bool {
        self.sent
            .iter()
            .any(|(_, sent)| sent.ack_eliciting && sent.in_flight)
    }

    pub(super) fn time_of_last_elicit(&self) -> Option<Instant> {
        self.last_elicit_time
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::core::transport_parameters::TransportParam;

    use super::*;

    #[test]
    fn test_build_frame() {
        let mut acker = Acker::new(Arc::new(TransportParam::new()));
        assert!(acker.received(1, false));
        assert!(acker.received(2, false));
        assert!(acker.received(3, false));
        assert!(acker.received(5, false));
        assert!(acker.received(6, false));
        assert!(acker.received(9, false));
        let frame = acker.build_frame();

        println!("Frame: {frame:?}");
        match frame {
            Frame::Ack(Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            }) => {
                assert_eq!(largest.0, 9);
                assert_eq!(first_range.0, 0);
                assert_eq!(ranges.len(), 2);

                assert_eq!(ranges.len(), 2);
                assert_eq!(ranges[0].gap, 1);
                assert_eq!(ranges[0].length, 1);
                assert_eq!(ranges[1].gap, 0);
                assert_eq!(ranges[1].length, 2);
            }
            _ => panic!("Can only be ack frame type"),
        }
    }

    #[test]
    fn test_build_frame_simple() {
        let mut acker = Acker::new(Arc::new(TransportParam::new()));
        assert!(acker.received(1, false));
        assert!(acker.received(2, false));
        assert!(acker.received(3, false));
        let frame = acker.build_frame();

        println!("Frame: {frame:?}");
        match frame {
            Frame::Ack(Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            }) => {
                assert_eq!(largest.0, 3);
                assert_eq!(first_range.0, 2);
                assert_eq!(ranges.len(), 0);

                assert_eq!(ranges.len(), 0);
            }
            _ => panic!("Can only be ack frame type"),
        }
    }

    #[test]
    fn test_build_frame_first_range() {
        let mut acker = Acker::new(Arc::new(TransportParam::new()));
        assert!(acker.received(1, false));
        assert!(acker.received(2, false));
        assert!(acker.received(3, false));
        assert!(acker.received(5, false));
        assert!(acker.received(6, false));
        assert!(acker.received(8, false));
        assert!(acker.received(9, false));
        let frame = acker.build_frame();

        println!("Frame: {frame:?}");
        match frame {
            Frame::Ack(Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            }) => {
                assert_eq!(largest.0, 9);
                assert_eq!(first_range.0, 1);
                assert_eq!(ranges.len(), 2);

                assert_eq!(ranges.len(), 2);
                assert_eq!(ranges[0].gap, 0);
                assert_eq!(ranges[0].length, 1);
                assert_eq!(ranges[1].gap, 0);
                assert_eq!(ranges[1].length, 2);
            }
            _ => panic!("Can only be ack frame type"),
        }
    }

    #[test]
    fn test_lost_frames() {
        let mut acker = Acker::new(Arc::new(TransportParam::new()));
        let frames = vec![Frame::Ping, Frame::HandshakeDone];

        acker.sent_packet(1, frames.clone(), true, true);
        acker.sent_packet(2, frames.clone(), true, true);
        acker.sent_packet(5, frames.clone(), true, true);
        acker.sent_packet(6, frames.clone(), true, true);
        acker.acked(6);

        let lost: Vec<_> = acker
            .get_lost_frames(&RttEstimator::new(), Instant::now())
            .iter()
            .flat_map(|lost| lost.frames.clone())
            .collect();
        println!("Lost: {lost:?}");

        assert!(matches!(lost[0], Frame::Ping));
        assert!(matches!(lost[1], Frame::HandshakeDone));
        assert!(matches!(lost[2], Frame::Ping));
        assert!(matches!(lost[3], Frame::HandshakeDone));
    }
}
