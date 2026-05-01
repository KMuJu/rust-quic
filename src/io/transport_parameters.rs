// TODO: Actually handle

use std::{sync::Arc, time::Duration};

pub(super) struct TransportParam {
    pub(super) max_ack_delay: Duration,
    pub(super) ack_delay_exponent: u32,
}

pub(super) type SharedConfig = Arc<TransportParam>;

impl TransportParam {
    pub(super) fn new() -> Self {
        Self {
            max_ack_delay: Duration::from_micros(1),
            ack_delay_exponent: 3,
        }
    }
}
