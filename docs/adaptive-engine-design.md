# Adaptive engine contract

The adaptive engine receives a `MetricsSnapshot` from the reliability layer and returns an `AdaptiveDecision`; it never performs socket I/O or retransmission itself. The reliability layer applies the decision's bounded window and packet rate.

States are deterministic: `STARTUP` holds until enough packets are observed, then cautiously increases into `STABLE`. `STABLE` increases one configured step on healthy observations. Sustained loss, retransmission, timeouts, or RTT inflation moves to `CONGESTION_DETECTED` and decreases once. The next observation enters `RECOVERY`, which holds until the configured number of healthy observations, then returns to `STABLE` with a single conservative increase.

All thresholds, rate/window bounds, increase steps, and reduction factor are `AdaptiveConfig` fields. This prevents unbounded growth and avoids repeated reductions caused by a single degradation event.
