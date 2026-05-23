# Timers

Uses one timer in `ConnectionDriver` and stores the deadline in `TimerTable` in `ConnectionInner`.
`TimerTable` is polled in order to set the `tokio::time::Sleep` in the `ConnectionDriver`.

## MaxAckDelay

Currently just loops over spaces `ack_deadline` and gets the lowest.
This is a bit wrong, but I could not get it to align with this result any other way.
The `ack_deadline` is set in `AckManager` when receiving packets and reset when an ack frame is built.

### Handle timeout

`handle_timeout` does nothing when this fires, since the sleep will wake the driver when it fires.
Meaning we don't need to handle this.

## Loss detection

Follows rfc9002.
Loss timer handle both acknowledgement based time threshold as well as PTO probes.

Sets the loss timer for these cases:
- Loss detection timeout
- Received ack
- Packet sent

The ack based deadline is set for each space when receiving an ack and there is an unacked packet with smaller packet number
than the largest acked. `AckManager` stores the smallest time sent for these packets and adds `loss_delay`.
When setting loss timer, pick the smallest `loss_time` among the different spaces.

The PTO probe deadline is set when there is no ack based deadline in `set_loss_timer`.
This value is the smallest time of last eliciting packet sent plus a duration which is calculated from the rtt.

### Handle timeout

If there is a loss time this will add lost packets to the pending frames queue, then rearm the loss timer.
When there is any ack eliciting packets in flight, then it handles sending pto probes and rearming the loss timer.
The pto probe is cloned frames from unacked packets that are `ack_eliciting` and `in_flight`.
If there are no such packets, then send a `Ping` frame.
