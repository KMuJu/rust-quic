use std::{f64, time::Duration};

use tokio::time::Instant;

use crate::{
    ack_manager::SentPacket, rtt_estimator::RttEstimator, transport_parameters::SharedConfig,
};

const MAXIMUM_DATAGRAM_SIZE: u64 = 1200;
const INITIAL_WINDOW: f64 = (10 * MAXIMUM_DATAGRAM_SIZE) as f64;
const LOSS_REDUCTION_FACTOR: f64 = 0.5;
const MINIMUM_CONGESTION_WINDOW: f64 = (2 * MAXIMUM_DATAGRAM_SIZE) as f64;
const PERSISTENT_CONGESTION_THRESHOLD: u32 = 3;

/// Implements the new reno congestion controller algorithm
pub(super) struct CongestionController {
    transport_params: SharedConfig,
    cwnd: f64,
    ssthresh: f64,
    in_flight: u64,
    congestion_recovery_start_time: Option<Instant>,
}

impl CongestionController {
    pub(super) fn new(transport_params: SharedConfig) -> Self {
        Self {
            transport_params,
            cwnd: INITIAL_WINDOW,
            ssthresh: f64::INFINITY,
            in_flight: 0,
            congestion_recovery_start_time: None,
        }
    }

    pub(super) fn available_space(&self) -> usize {
        (self.cwnd as usize).saturating_sub(self.in_flight as usize)
    }

    pub(super) fn on_packet_sent(&mut self, sent_bytes: u64) {
        self.in_flight += sent_bytes;
    }

    pub(super) fn on_ack(&mut self, acked_bytes: u64, time_sent: Instant) {
        self.in_flight -= acked_bytes;
        if self.in_congestion_recovery(time_sent) {
            return;
        }
        if self.cwnd < self.ssthresh {
            self.cwnd += acked_bytes as f64;
        } else {
            self.cwnd += (MAXIMUM_DATAGRAM_SIZE * acked_bytes) as f64 / self.cwnd;
        }
    }

    pub(super) fn on_loss(&mut self, packets: &[SentPacket], rtt: &RttEstimator) {
        if packets.is_empty() {
            return;
        }
        let mut sent_time_of_last_loss: Option<Instant> = None;
        for sent in packets {
            self.in_flight -= sent.sent_bytes as u64;
            sent_time_of_last_loss = match sent_time_of_last_loss {
                Some(s) => Some(s.max(sent.timestamp)),
                None => Some(sent.timestamp),
            };
        }

        if let Some(sent_time) = sent_time_of_last_loss {
            self.on_congestion_event(sent_time);
        }
        let Some(first_rtt_sample) = rtt.first_rtt else {
            return;
        };

        let pc_lost: Vec<_> = packets
            .iter()
            .filter(|s| s.timestamp > first_rtt_sample)
            .collect();

        if self.in_persistent_congestion(&pc_lost, rtt) {
            self.cwnd = MINIMUM_CONGESTION_WINDOW;
            self.congestion_recovery_start_time = None;
        }
    }

    pub(super) fn on_congestion_event(&mut self, sent_time: Instant) {
        if self.in_congestion_recovery(sent_time) {
            return;
        }

        self.congestion_recovery_start_time = Some(Instant::now());
        self.ssthresh = self.cwnd * LOSS_REDUCTION_FACTOR;
        self.cwnd = self.ssthresh.max(MINIMUM_CONGESTION_WINDOW);
    }

    fn in_congestion_recovery(&self, sent_time: Instant) -> bool {
        if let Some(time) = self.congestion_recovery_start_time {
            return sent_time <= time;
        }
        false
    }

    fn in_persistent_congestion(&self, lost: &[&SentPacket], rtt: &RttEstimator) -> bool {
        if lost.len() < 2 {
            return false;
        }

        if rtt.first_rtt.is_none() {
            return false;
        }

        let earliest = lost.iter().map(|p| p.timestamp).min().unwrap();
        let latest = lost.iter().map(|p| p.timestamp).max().unwrap();

        let duration = self.persistent_congestion_duration(rtt);
        if latest.duration_since(earliest) < duration {
            return false;
        }

        // TODO: Handle condition:
        // Across all packet number spaces, none of the packets sent between
        // the send times of these two packets are acknowledged;

        true
    }

    fn persistent_congestion_duration(&self, rtt: &RttEstimator) -> Duration {
        // (smoothed_rtt + max(4*rttvar, kGranularity) + max_ack_delay) * kPersistentCongestionThreshold
        (rtt.smoothed
            + (4 * rtt.variance).max(Duration::from_millis(1))
            + self.transport_params.max_ack_delay)
            * PERSISTENT_CONGESTION_THRESHOLD
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time;

    use crate::transport_parameters::TransportParam;

    use super::*;

    fn make_rtt(
        smoothed: Duration,
        variance: Duration,
        first_rtt: Option<Instant>,
    ) -> RttEstimator {
        RttEstimator::new_params(smoothed, variance, first_rtt)
    }

    fn make_packet(timestamp: Instant, sent_bytes: usize) -> SentPacket {
        SentPacket {
            frames: Vec::new(),
            pn: 0,
            timestamp,
            sent_bytes,
            ack_eliciting: true,
            in_flight: true,
        }
    }

    fn make_controller() -> CongestionController {
        CongestionController::new(Arc::new(TransportParam::new()))
    }
}
