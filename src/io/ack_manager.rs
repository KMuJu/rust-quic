use std::{
    collections::{BTreeMap, BTreeSet},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::task::AtomicWaker;
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
    received: BTreeSet<u32>,
    sent: BTreeMap<u32, SentPacket>,
    eliciting_count: u32,
    ack_deadline: Option<Instant>,

    waker: AtomicWaker,
}

impl Acker {
    pub(super) fn new() -> Self {
        Self {
            received: BTreeSet::new(),
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
        let mut iter = self.received.iter().rev();
        let largest = *iter.next().unwrap();
        let delay = VarInt(0);

        let mut last = largest;
        let mut first_range = 0;
        let mut gap = 0;

        for &pn in iter.by_ref() {
            if last - 1 > pn {
                gap = (last - pn) as u64 - 1;
                last = pn;
                break;
            }
            last = pn;
            first_range += 1;
        }

        let mut ranges = Vec::new();
        let mut length = 0;

        for &pn in iter {
            if last - 1 > pn {
                let range = AckRange {
                    gap: VarInt(gap),
                    length: VarInt(length),
                };
                ranges.push(range);
                gap = (last - pn) as u64 - 1;
                length = 0;
            }
            last = pn;
            length += 1;
        }

        if length > 0 {
            let range = AckRange {
                gap: VarInt(gap),
                length: VarInt(length - 1),
            };
            ranges.push(range);
        }

        Frame::Ack {
            largest: VarInt(largest as u64),
            delay,
            range_count: VarInt(ranges.len() as u64),
            first_range: VarInt(first_range),
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

    pub(super) fn acked(&mut self, packet_number: u32) {
        self.sent.remove(&packet_number);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_frame() {
        let mut acker = Acker::new();
        acker.received(&PacketNumber::U8(1), false);
        acker.received(&PacketNumber::U8(2), false);
        acker.received(&PacketNumber::U8(3), false);
        acker.received(&PacketNumber::U8(5), false);
        acker.received(&PacketNumber::U8(6), false);
        acker.received(&PacketNumber::U8(9), false);
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
        acker.received(&PacketNumber::U8(1), false);
        acker.received(&PacketNumber::U8(2), false);
        acker.received(&PacketNumber::U8(3), false);
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
        acker.received(&PacketNumber::U8(1), false);
        acker.received(&PacketNumber::U8(2), false);
        acker.received(&PacketNumber::U8(3), false);
        acker.received(&PacketNumber::U8(5), false);
        acker.received(&PacketNumber::U8(6), false);
        acker.received(&PacketNumber::U8(8), false);
        acker.received(&PacketNumber::U8(9), false);
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
}
