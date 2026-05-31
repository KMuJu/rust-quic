// TODO: Actually handle

use std::{sync::Arc, time::Duration};

pub(crate) struct TransportParam {
    pub(crate) max_ack_delay: Duration,
    pub(crate) ack_delay_exponent: u32,
}

pub(crate) type SharedConfig = Arc<TransportParam>;

impl TransportParam {
    pub(crate) fn new() -> Self {
        Self {
            max_ack_delay: Duration::from_millis(25),
            ack_delay_exponent: 3,
        }
    }
}
