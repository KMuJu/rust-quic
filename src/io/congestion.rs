use std::time::Duration;

pub(super) struct CongestionController;

impl CongestionController {
    pub(super) fn new() -> Self {
        Self
    }

    pub(super) fn get_smoothed_rtt(&self) -> Duration {
        Duration::from_millis(200)
    }
}
