use std::collections::VecDeque;

use bytes::Buf;
use rand::{Rng, rngs::StdRng};

pub type InternalCID = u64;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnectionID {
    pub seq_num: u64,
    pub len: u8,
    pub buf: [u8; 20],
}

impl std::hash::Hash for ConnectionID {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.len.hash(state);
        self.buf.hash(state);
    }
}

impl ConnectionID {
    pub fn from_buf<B: Buf>(buf: &mut B, len: u8) -> Self {
        let mut id = Self {
            seq_num: 0,
            len,
            buf: [0u8; 20],
        };
        // TODO: Potential bug
        buf.copy_to_slice(&mut id.buf[..len as usize]);
        id
    }

    pub fn new(seq_num: u64, len: u8, rng: &mut StdRng) -> Self {
        let mut id = Self {
            seq_num,
            len,
            buf: [0u8; 20],
        };
        rng.fill_bytes(&mut id.buf[..len as usize]);
        id
    }
}

impl std::fmt::Debug for ConnectionID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CID")
            .field(&&self.buf[..self.len as usize])
            .finish()
    }
}

pub struct ConnectionIDQueue {
    pub current: ConnectionID,
    queue: VecDeque<ConnectionID>,
}

impl ConnectionIDQueue {
    pub fn new(current: ConnectionID) -> Self {
        Self {
            current,
            queue: VecDeque::new(),
        }
    }

    pub fn add_cid(&mut self, new: ConnectionID) {
        self.queue.push_back(new);
    }

    pub fn retire(&mut self, seq_num: u64) {
        if self.current.seq_num == seq_num {
            assert!(
                !self.queue.is_empty(),
                "[ConnectionIDQueue] Tried to retire cid with empty connection id queue"
            );

            self.current = self.queue.pop_front().unwrap();
        } else {
            self.queue.retain(|cid| cid.seq_num != seq_num);
        }
    }

    pub fn retire_prior(&mut self, internal_cid: InternalCID) {
        self.queue.retain(|cid| cid.seq_num >= internal_cid);

        if self.current.seq_num < internal_cid {
            assert!(
                !self.queue.is_empty(),
                "[ConnectionIDQueue] No cids left after retiring prior"
            );

            self.current = self.queue.pop_front().unwrap();
        }
    }

    pub fn reset(&mut self, current: ConnectionID) {
        self.current = current;
        self.queue.clear();
    }
}
