use std::{
    collections::{BTreeMap, BTreeSet},
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use tokio::time::Instant;

use crate::io::{
    encoding::VarInt,
    frame::{AckRange, Frame},
    packet::PacketNumber,
};

struct SentPacket;

pub(super) struct Acker {
    received: BTreeSet<u32>,
    sent: BTreeMap<u32, SentPacket>,
    eliciting_count: u32,
    ack_deadline: Option<Instant>,

    waker: AtomicWaker,
    has_received: bool, // Since last ack frame
}

impl Acker {
    pub(super) fn new() -> Self {
        Self {
            received: BTreeSet::new(),
            sent: BTreeMap::new(),
            eliciting_count: 0,
            ack_deadline: None,
            waker: AtomicWaker::new(),
            has_received: false,
        }
    }

    /// # Returns
    /// true if not duplicate
    /// false if duplicate
    pub(super) fn received(&mut self, pn: &PacketNumber, elicit: bool) -> bool {
        let pn = pn.u32();

        if self.received.insert(pn) {
            self.has_received = true;
            self.waker.wake();
            if elicit {
                self.eliciting_count += 1;
            }
            return true;
        }

        false
    }

    pub(super) fn get_deadline(&self) -> Option<Instant> {
        self.ack_deadline
    }

    pub(super) fn poll_ack(&mut self, cx: &mut Context<'_>) -> Poll<Frame> {
        self.waker.register(cx.waker());
        if self.has_received {
            Poll::Ready(self.build_frame())
        } else {
            Poll::Pending
        }
    }

    // TODO: Refactor
    pub(super) fn get_ack(&mut self) -> Option<Frame> {
        if self.has_received && self.eliciting_count != 0 {
            self.eliciting_count = 0;
            Some(self.build_frame())
        } else {
            None
        }
    }

    pub(super) fn mark_acked(&mut self) {
        self.has_received = false;
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
