use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::task::AtomicWaker;
use range_set::{RangeSet, range_set};
use tokio::time::Instant;

use crate::io::{
    encoding::VarInt,
    frame::{AckRange, Frame},
    packet::{Header, Packet, PacketNumber},
};

struct SentPacket {
    timestamp: Instant,
    header: Header,
    frames: Vec<Frame>,
}

pub(super) struct Acker {
    received: RangeSet<[RangeInclusive<u32>; 8]>,
    sent: BTreeMap<u32, SentPacket>,
    eliciting_count: u32,
    ack_deadline: Option<Instant>,

    waker: AtomicWaker,
}

impl Acker {
    pub(super) fn new() -> Self {
        Self {
            received: RangeSet::new(),
            sent: BTreeMap::new(),
            eliciting_count: 0,
            ack_deadline: None,
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
        let pn = pn.u32();

        if self.received.insert(pn) {
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

    // TODO: Refactor
    pub(super) fn get_ack(&mut self) -> Option<Frame> {
        if self.eliciting_count != 0 {
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

        let mut ranges = Vec::new();
        let mut prev_end = *first.start();

        for range in iter {
            println!("Range: {range:?}");
            let gap = prev_end - range.end() - 1;
            let length = range.end() - range.start();
            ranges.push(AckRange {
                gap: VarInt(gap as u64),
                length: VarInt(length as u64),
            });
            prev_end = *range.start();
        }

        Frame::Ack {
            largest: VarInt(largest as u64),
            delay: VarInt(0),
            range_count: VarInt(ranges.len() as u64),
            first_range: VarInt(first_range as u64),
            ranges,
            ecn: None,
        }
    }

    pub(super) fn sent_packet(
        &mut self,
        packet_number: PacketNumber,
        header: Header,
        frames: Vec<Frame>,
    ) {
        let pn = packet_number.u32();

        let sent = SentPacket {
            timestamp: Instant::now(),
            header,
            frames,
        };

        self.sent.insert(pn, sent);
    }

    pub(super) fn get_lost_frames(&mut self, largest: u32) -> Vec<Frame> {
        let mut lost = Vec::new();

        let lost_pns: Vec<_> = self
            .sent
            .keys()
            .cloned()
            .filter(|pn| pn + 3 <= largest)
            .collect();

        for pn in lost_pns {
            let mut sent = self.sent.remove(&pn).unwrap();
            lost.append(&mut sent.frames);
        }

        // TODO: Handle packets lost based on time
        // time_threshold = max(9/8 * smoothed_rtt, 1ms)
        // packet is lost if:
        //   largest_sent_at - packet.sent_at >= time_threshold

        lost
    }

    pub(super) fn acked(&mut self, packet_number: u32) {
        self.sent.remove(&packet_number);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::io::connection::ConnectionID;

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
                range_count,
                first_range,
                ranges,
                ecn,
            } => {
                assert_eq!(largest.0, 9);
                assert_eq!(first_range.0, 0);
                assert_eq!(range_count.0, 2);

                assert_eq!(ranges.len(), 2);
                assert_eq!(ranges[0].gap, 2);
                assert_eq!(ranges[0].length, 1);
                assert_eq!(ranges[1].gap, 1);
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
                range_count,
                first_range,
                ranges,
                ecn,
            } => {
                assert_eq!(largest.0, 3);
                assert_eq!(first_range.0, 2);
                assert_eq!(range_count.0, 0);

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
                range_count,
                first_range,
                ranges,
                ecn,
            } => {
                assert_eq!(largest.0, 9);
                assert_eq!(first_range.0, 1);
                assert_eq!(range_count.0, 2);

                assert_eq!(ranges.len(), 2);
                assert_eq!(ranges[0].gap, 1);
                assert_eq!(ranges[0].length, 1);
                assert_eq!(ranges[1].gap, 1);
                assert_eq!(ranges[1].length, 2);
            }
            _ => panic!("Can only be ack frame type"),
        }
    }

    #[test]
    fn test_lost_frames() {
        let mut acker = Acker::new();
        let pn = PacketNumber::from(0);
        let header = Header::Short {
            spin: false,
            key_phase: false,
            dst_cid: ConnectionID::from_buf(&mut Cursor::new([1]), 1),
            packet_number: pn,
        };
        let frames = vec![Frame::Padding, Frame::HandshakeDone];

        acker.sent_packet(PacketNumber::U8(1), header.clone(), frames.clone());
        acker.sent_packet(PacketNumber::U8(2), header.clone(), frames.clone());
        acker.sent_packet(PacketNumber::U8(5), header.clone(), frames.clone());
        acker.sent_packet(PacketNumber::U8(6), header.clone(), frames.clone());

        let lost = acker.get_lost_frames(6);

        assert!(matches!(lost[0], Frame::Padding));
        assert!(matches!(lost[1], Frame::HandshakeDone));
        assert!(matches!(lost[2], Frame::Padding));
        assert!(matches!(lost[3], Frame::HandshakeDone));
    }
}
