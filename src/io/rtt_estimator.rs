use std::time::Duration;

#[derive(Clone)]
pub(super) struct RttEstimator {
    first: bool,
    pub(super) smoothed: Duration,
    pub(super) variance: Duration,
    pub(super) min_rtt: Duration,
    pub(super) latest_rtt: Duration,
}

impl RttEstimator {
    pub(super) fn new() -> Self {
        Self {
            first: true,
            smoothed: Duration::from_millis(333),
            variance: Duration::from_millis(166),
            min_rtt: Duration::new(0, 0),
            latest_rtt: Duration::from_secs(0),
        }
    }

    pub(super) fn update(
        &mut self,
        latest_rtt: Duration,
        mut ack_delay: Duration,
        max_ack_delay: &Duration,
        handshake_done: bool,
    ) {
        self.latest_rtt = latest_rtt;
        if self.first {
            self.first = false;
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
