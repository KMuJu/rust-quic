use std::time::Duration;

#[derive(Clone)]
pub(super) struct RttEstimator {
    first: bool,
    pub(super) smoothed: Duration,
    pub(super) variance: Duration,
    pub(super) min_rtt: Duration,
}

impl RttEstimator {
    pub(super) fn new() -> Self {
        Self {
            first: true,
            smoothed: Duration::from_millis(333),
            variance: Duration::from_millis(10),
            min_rtt: Duration::new(0, 0),
        }
    }
}
