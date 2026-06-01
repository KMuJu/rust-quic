use std::{
    collections::VecDeque,
    ops::{Index, IndexMut},
};

use crate::core::{ack_manager::Acker, frame::Frame, transport_parameters::SharedConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketSpace {
    Initial,
    Handshake,
    Data,
}

impl Index<PacketSpace> for [Space; 3] {
    type Output = Space;

    fn index(&self, index: PacketSpace) -> &Self::Output {
        &self.as_ref()[index as usize]
    }
}

impl IndexMut<PacketSpace> for [Space; 3] {
    fn index_mut(&mut self, index: PacketSpace) -> &mut Self::Output {
        &mut self.as_mut()[index as usize]
    }
}

impl PacketSpace {
    pub fn iter() -> impl Iterator<Item = Self> {
        [Self::Initial, Self::Handshake, Self::Data].into_iter()
    }
}

pub(crate) struct Space {
    pub(super) space: PacketSpace,
    discarded: bool,
    pub(super) acker: Acker,
    pub(super) pending_frames: VecDeque<Frame>,
    pub(super) packet_number: usize,
    pub(super) keys: Option<rustls::quic::Keys>,
}

impl Space {
    pub(super) fn new(space: PacketSpace, transport_params: SharedConfig) -> Self {
        Self {
            space,
            discarded: false,
            acker: Acker::new(transport_params.clone()),
            pending_frames: VecDeque::new(),
            packet_number: 0,
            keys: None,
        }
    }

    pub(super) fn set_keys(&mut self, keys: rustls::quic::Keys) {
        self.keys = Some(keys)
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
