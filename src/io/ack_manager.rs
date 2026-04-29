use std::{collections::BTreeMap, ops::RangeInclusive, task::Waker, time::Duration};

use futures::task::AtomicWaker;
use range_set::RangeSet;
use tokio::time::Instant;

use crate::io::{
    encoding::VarInt,
    frame::{AckRange, Frame},
    packet::PacketNumber,
    rtt_estimator::RttEstimator,
};

struct SentPacket {
    timestamp: Instant,
    frames: Vec<Frame>,
    sent_bytes: usize,
    in_flight: bool, // if it counts towards congestion
}

struct LargestReceived {
    packet_number: u32,
    timestamp: Instant,
}

pub(super) struct Acker {
    cleared: bool,
    received: RangeSet<[RangeInclusive<u32>; 8]>,
    sent: BTreeMap<u32, SentPacket>,
    eliciting_count: u32,
    ack_deadline: Option<Instant>,
    largest_acked: u32,
    largest_received: Option<LargestReceived>,

    waker: AtomicWaker,
}

impl Acker {
    pub(super) fn new() -> Self {
        Self {
            cleared: false,
            received: RangeSet::new(),
            sent: BTreeMap::new(),
            eliciting_count: 0,
            ack_deadline: None,
            largest_acked: 0,
            largest_received: None,
            waker: AtomicWaker::new(),
        }
    }

    pub(super) fn register_waker(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    /// # Returns
    /// true if not duplicate
    /// false if duplicate
    pub(super) fn received(&mut self, pn: &PacketNumber, elicit: bool) -> bool {
        if self.cleared {
            return true;
        }
        let pn = pn.u32();

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
                    self.waker.wake();
                    println!("Eliciting packet count >= 2: NEED TO SEND ACK");
                }
                if self.ack_deadline.is_none() {
                    self.ack_deadline = Some(Instant::now() + Duration::from_millis(25));
                }
            }

            return true;
        }

        false
    }

    pub(super) fn get_deadline(&self) -> Option<Instant> {
        self.ack_deadline
    }

    pub(super) fn get_next_loss(&self, rtt: &RttEstimator) -> Option<Instant> {
        if self.cleared {
            return None;
        }
        let time_threshold = (rtt.smoothed * 9 / 8).max(Duration::from_micros(1));
        self.sent
            .values()
            .map(|s| s.timestamp)
            .min()
            .map(|t| t + time_threshold)
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
        let encoded_delay = delay_micros >> 3; // TODO: ack_delay_exponent

        let mut ranges = Vec::new();
        let mut prev_end = *first.start();

        for range in iter {
            let gap = prev_end - range.end() - 2;
            let length = range.end() - range.start();
            ranges.push(AckRange {
                gap: VarInt(gap as u64),
                length: VarInt(length as u64),
            });
            prev_end = *range.start();
        }

        Frame::Ack {
            largest: VarInt(largest as u64),
            delay: VarInt(encoded_delay as u64),
            first_range: VarInt(first_range as u64),
            ranges,
            ecn: None,
        }
    }

    pub(super) fn sent_packet(
        &mut self,
        packet_number: PacketNumber,
        in_frames: Vec<Frame>,
        in_flight: bool,
    ) {
        if self.cleared {
            return;
        }
        let pn = packet_number.u32();

        // Only retransmit retransmitable frames
        // TODO: Handle MAX_* frames
        let mut frames = Vec::new();
        for frame in in_frames {
            if frame.is_retransmittable() {
                frames.push(frame);
            }
        }

        let sent = SentPacket {
            timestamp: Instant::now(),
            sent_bytes: frames.iter().map(|f| f.size()).sum(),
            frames,
            in_flight,
        };

        self.sent.insert(pn, sent);
    }

    pub(super) fn get_lost_frames(&mut self, largest: u32, rtt: &RttEstimator) -> Vec<Frame> {
        let mut lost = Vec::new();
        if self.cleared {
            return lost;
        }
        let largest_sent_at = self.sent.get(&largest).map(|s| s.timestamp);

        let lost_pns: Vec<_> = self
            .sent
            .keys()
            .filter(|&pn| pn + 3 <= largest)
            .copied()
            .collect();

        for pn in lost_pns {
            let sent = self.sent.remove(&pn).unwrap();
            for frame in sent.frames {
                lost.push(frame);
            }
        }

        if let Some(lst) = largest_sent_at {
            let mut time_lost = self.get_time_lost_frames(lst, rtt);
            lost.append(&mut time_lost);
        }

        if !lost.is_empty() {
            self.waker.wake();
        }

        lost
    }

    pub(super) fn get_time_lost_frames(&mut self, lst: Instant, rtt: &RttEstimator) -> Vec<Frame> {
        // TODO: Handle packets lost based on time
        // time_threshold = max(9/8 * smoothed_rtt, 1ms)
        // packet is lost if:
        //   largest_sent_at - packet.sent_at >= time_threshold

        let mut lost = Vec::new();
        if self.cleared {
            return lost;
        }

        let time_threshold = (rtt.smoothed * 9 / 8).max(Duration::from_micros(1));
        let time_lost: Vec<u32> = self
            .sent
            .iter()
            .filter(|&(pn, s)| *pn < self.largest_acked && s.timestamp + time_threshold <= lst)
            .map(|(pn, _)| *pn)
            .collect();

        for pn in time_lost {
            let sent = self.sent.remove(&pn).unwrap();
            for frame in sent.frames {
                lost.push(frame);
            }
        }

        lost
    }

    pub(super) fn acked(&mut self, packet_number: u32) {
        self.largest_acked = self.largest_acked.max(packet_number);
        self.sent.remove(&packet_number);
    }

    /// Returns the timestamp if it is present
    /// If it is present, then that means that it is ack-eliciting
    pub(super) fn sent_timestamp(&self, packet_number: u32) -> Option<Instant> {
        self.sent.get(&packet_number).map(|s| s.timestamp)
    }

    pub(super) fn unacked_frame(&self) -> Option<Frame> {
        self.sent
            .values()
            .filter_map(|s| s.frames.first())
            .next()
            .cloned()
    }

    pub(super) fn clear(&mut self) {
        self.cleared = true;
        self.sent.clear();
        self.received.clear();
        self.ack_deadline = None;
    }

    pub(super) fn earliest_sent(&self) -> Option<Instant> {
        self.sent.values().map(|s| s.timestamp).min()
    }

    pub(super) fn has_packet_in_flight(&self) -> bool {
        !self.sent.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_frame() {
        let mut acker = Acker::new();
        assert!(acker.received(&PacketNumber::U8(1), false));
        assert!(acker.received(&PacketNumber::U8(2), false));
        assert!(acker.received(&PacketNumber::U8(3), false));
        assert!(acker.received(&PacketNumber::U8(5), false));
        assert!(acker.received(&PacketNumber::U8(6), false));
        assert!(acker.received(&PacketNumber::U8(9), false));
        let frame = acker.build_frame();

        println!("Frame: {frame:?}");
        match frame {
            Frame::Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            } => {
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
        let mut acker = Acker::new();
        assert!(acker.received(&PacketNumber::U8(1), false));
        assert!(acker.received(&PacketNumber::U8(2), false));
        assert!(acker.received(&PacketNumber::U8(3), false));
        let frame = acker.build_frame();

        println!("Frame: {frame:?}");
        match frame {
            Frame::Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            } => {
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
        let mut acker = Acker::new();
        assert!(acker.received(&PacketNumber::U8(1), false));
        assert!(acker.received(&PacketNumber::U8(2), false));
        assert!(acker.received(&PacketNumber::U8(3), false));
        assert!(acker.received(&PacketNumber::U8(5), false));
        assert!(acker.received(&PacketNumber::U8(6), false));
        assert!(acker.received(&PacketNumber::U8(8), false));
        assert!(acker.received(&PacketNumber::U8(9), false));
        let frame = acker.build_frame();

        println!("Frame: {frame:?}");
        match frame {
            Frame::Ack {
                largest,
                delay,
                first_range,
                ranges,
                ecn,
            } => {
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
        let mut acker = Acker::new();
        let frames = vec![Frame::Ping, Frame::HandshakeDone];

        acker.sent_packet(PacketNumber::U8(1), frames.clone(), true);
        acker.sent_packet(PacketNumber::U8(2), frames.clone(), true);
        acker.sent_packet(PacketNumber::U8(5), frames.clone(), true);
        acker.sent_packet(PacketNumber::U8(6), frames.clone(), true);

        let lost = acker.get_lost_frames(6, &RttEstimator::new());
        println!("Lost: {lost:?}");

        assert!(matches!(lost[0], Frame::Ping));
        assert!(matches!(lost[1], Frame::HandshakeDone));
        assert!(matches!(lost[2], Frame::Ping));
        assert!(matches!(lost[3], Frame::HandshakeDone));
    }
}
