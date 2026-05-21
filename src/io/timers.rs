use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(super) enum Timer {
    MaxAckDelay,
    LossDetection,
}

impl Timer {
    pub(super) fn values() -> [Timer; 2] {
        [Self::MaxAckDelay, Self::LossDetection]
    }
}

pub(super) struct TimerTable {
    deadlines: [Option<Instant>; 2],
}

impl TimerTable {
    pub(super) fn new() -> Self {
        Self {
            deadlines: [None; 2],
        }
    }

    pub(super) fn get(&self, timer: Timer) -> Option<Instant> {
        self.deadlines[timer as usize]
    }

    pub(super) fn set(&mut self, timer: Timer, deadline: Instant) {
        self.deadlines[timer as usize] = Some(deadline);
    }

    pub(super) fn reset(&mut self, timer: Timer) {
        self.deadlines[timer as usize] = None;
    }

    pub(super) fn has_passed(&self, timer: Timer, deadline: Instant) -> bool {
        self.get(timer).is_some_and(|d| d < deadline)
    }

    pub(super) fn next(&self) -> Option<Instant> {
        self.deadlines.iter().filter_map(|d| *d).min()
    }
}
