# Reliability integration contract

Each Stop-and-Wait, Go-Back-N, or Selective Repeat sender owns its own queue, ACK semantics, and timer scheduling. It uses `ReliabilityController` only at existing event boundaries:

1. After an original send, call `on_packet_sent(sequence, sent_at)`.
2. After a retransmission, call `on_packet_sent(sequence, sent_at, retransmission=True)`.
3. When an ACK confirms a payload, call `on_acknowledged(sequence, acknowledged_at, payload_bytes)`.
4. When a timer expires, call `on_timeout()` and use the returned RTO for the replacement timer.
5. At the configured observation interval, call `evaluate(now=...)` and apply its rate/window decision.

`TransportMode.P3_BASELINE` exposes the configured fixed window/rate while retaining identical RTT and metrics instrumentation. `TransportMode.DART` delegates decisions to the adaptive engine. This creates a fair P3-versus-DART comparison: the ARQ implementation and measurements are shared; only transmission-parameter policy differs.
