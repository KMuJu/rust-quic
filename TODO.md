# ACKING

- [ ] Handle receiving packets and acking them
  - [x] Store ack packets
    - Done in ack_manager.rs
  - [x] Include ack frames in packets
  - [ ] Handle ack timers

From Claude:
Receive side (per packet number space):
  - received: RangeSet<u64>          // for building ACK frames
  - ack_eliciting_since_last_ack: u32
  - ack_deadline: Option<Instant>

Send side:
  - sent_packets: BTreeMap<u64, SentPacketInfo>  // for loss detection
  - largest_acked: u64
  - rtt: RttEstimator
  - loss_timer: Option<Instant>
  - pto_timer: Option<Instant>
  - congestion: CongestionController

# QUIC ACK Management Implementation Checklist

## Receive-Side Tracking
- [x] `RangeSet<u64>` data structure to track received packet numbers with gaps
- [x] Per packet number space tracking (Initial, Handshake, 1-RTT)
- [x] Record packet number on every received packet
- [x] Duplicate detection (ignore already-seen packet numbers)
- [x] Flag each received packet as ACK-eliciting or not

## Building ACK Frames
- [x] Encode `RangeSet` into ACK frame ranges (largest + gap/range pairs)
- [x] Only ACK within the correct packet number space
- [x] Track count of unacknowledged ACK-eliciting packets received
- [x] Reset ACK-eliciting counter after sending an ACK
- [ ] Clear acknowledged ranges after peer has ACKed our ACK (ACK of ACK)

## ACK Timing
- [x] Start a `max_ack_delay` timer (25ms) on receiving an ACK-eliciting packet
- [x] Send ACK immediately after receiving 2 ACK-eliciting packets without ACKing
- [x] Cancel/reset the timer whenever an ACK is sent
- [x] Wire the timer into `ConnectionDriver` so it wakes the driver on expiry

## Processing Received ACKs (send-side)
- [x] Record every sent ACK-eliciting packet: number, frames, send timestamp
- [x] Mark packets as acknowledged when their number appears in a received ACK
- [ ] Update RTT estimate using send timestamp and `ack_delay` field
- [x] Detect lost packets via packet threshold (3 packets acknowledged after it)
- [x] Detect lost packets via time threshold (packet sent long enough ago)
- [x] Re-enqueue frames from lost packets for retransmission

## Retransmission
- [x] Store sent packet info in `BTreeMap<u64, SentPacketInfo>`
- [x] On loss, retransmit frames (not bytes) in a new packet with a new number
- [ ] PTO (Probe Timeout) timer: fire after ~3× RTT with no ACK
- [ ] On PTO, send a probe packet (PING or retransmit unacked data)
- [ ] Reset PTO timer whenever an ACK is received

## Congestion Control
- [ ] Track congestion window (`cwnd`)
- [ ] Track bytes in flight
- [ ] Slow start: increase `cwnd` on each ACK
- [ ] Congestion avoidance (AIMD): increase `cwnd` more slowly after threshold
- [ ] Reduce `cwnd` on loss detection
- [ ] Block `get_packet` from sending if bytes in flight would exceed `cwnd`
