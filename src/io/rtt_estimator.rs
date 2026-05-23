use std::time::Duration;

use tokio::time::Instant;

#[derive(Clone)]
pub(super) struct RttEstimator {
    pub(super) smoothed: Duration,
    pub(super) variance: Duration,
    pub(super) min_rtt: Duration,
    pub(super) latest_rtt: Duration,
    pub(super) first_rtt: Option<Instant>,
}

impl RttEstimator {
    pub(super) fn new() -> Self {
        Self {
            smoothed: Duration::from_millis(333),
            variance: Duration::from_millis(166),
            min_rtt: Duration::ZERO,
            latest_rtt: Duration::ZERO,
            first_rtt: None,
        }
    }

    pub(super) fn new_params(
        smoothed: Duration,
        variance: Duration,
        first_rtt: Option<Instant>,
    ) -> Self {
        Self {
            smoothed,
            variance,
            min_rtt: Duration::ZERO,
            latest_rtt: Duration::ZERO,
            first_rtt,
        }
    }

    /// Follows RFC 9002 A.8
    pub(super) fn update(
        &mut self,
        latest_rtt: Duration,
        mut ack_delay: Duration,
        max_ack_delay: &Duration,
        handshake_done: bool,
    ) {
        self.latest_rtt = latest_rtt;
        if self.first_rtt.is_none() {
            self.first_rtt = Some(Instant::now());
            self.min_rtt = latest_rtt;
            self.smoothed = latest_rtt;
            self.variance = latest_rtt / 2;
            return;
        }
        self.min_rtt = self.min_rtt.min(latest_rtt);
        if handshake_done {
            ack_delay = ack_delay.min(*max_ack_delay);
        }

        let mut adjusted = latest_rtt;
        if latest_rtt >= self.min_rtt + ack_delay {
            adjusted -= ack_delay;
        }

        self.smoothed = (7 * self.smoothed + adjusted) / 8;

        let diff = if self.smoothed > adjusted {
            self.smoothed - adjusted
        } else {
            adjusted - self.smoothed
        };

        self.variance = (3 * self.variance + diff) / 4;
    }
}
