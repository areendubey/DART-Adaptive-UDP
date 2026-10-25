"""Reliability-facing integration for fixed P3 and adaptive DART operation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .adaptive import AdaptationAction, AdaptiveDecision, AdaptiveEngine, AdaptiveState
from .metrics import MetricsSnapshot, TransferMetrics
from .rtt import RtoConfig, RttEstimator


class TransportMode(str, Enum):
    """The same ARQ loop can select fixed P3 or adaptive DART behavior."""

    P3_BASELINE = "p3"
    DART = "dart"


@dataclass(frozen=True, slots=True)
class ControllerConfig:
    """Parameters shared by an ARQ sender regardless of its implementation."""

    mode: TransportMode = TransportMode.DART
    fixed_window: int = 4
    fixed_rate_packets_per_second: float = 100.0

    def __post_init__(self) -> None:
        if self.fixed_window < 1 or self.fixed_rate_packets_per_second <= 0:
            raise ValueError("fixed transport parameters must be positive")


class ReliabilityController:
    """Connects ARQ event hooks to transport measurements and DART decisions.

    An ARQ sender calls ``on_packet_sent``, ``on_acknowledged``, and
    ``on_timeout`` at its existing event boundaries. It then calls ``evaluate``
    at an observation interval and applies the returned window/rate. This class
    intentionally owns no sockets, packet queues, or retransmission loop.
    """

    def __init__(
        self,
        config: ControllerConfig | None = None,
        *,
        rto_config: RtoConfig | None = None,
        adaptive_engine: AdaptiveEngine | None = None,
        metrics: TransferMetrics | None = None,
    ) -> None:
        self.config = config or ControllerConfig()
        self.rtt = RttEstimator(rto_config)
        self.metrics = metrics or TransferMetrics()
        self.engine = adaptive_engine or AdaptiveEngine()
        if self.config.mode is TransportMode.DART:
            self._window = self.engine.window
            self._rate = self.engine.rate_packets_per_second
            self._state = AdaptiveState.STARTUP
        else:
            self._window = self.config.fixed_window
            self._rate = self.config.fixed_rate_packets_per_second
            self._state = AdaptiveState.STABLE
        self.metrics.record_adaptation(
            rate=self._rate,
            window=self._window,
            state=self._state.value,
            changed=False,
        )

    @property
    def rto_seconds(self) -> float:
        """Return the current ARQ timer value."""
        return self.rtt.rto_seconds

    def on_packet_sent(self, sequence: int, sent_at: float, *, retransmission: bool = False) -> None:
        """Record one original or retransmitted ARQ packet transmission."""
        self.metrics.record_sent(retransmission=retransmission)
        if retransmission:
            self.rtt.record_retransmission(sequence, sent_at)
        else:
            self.rtt.record_send(sequence, sent_at)

    def on_acknowledged(self, sequence: int, acknowledged_at: float, payload_bytes: int) -> float | None:
        """Record delivery and update RTT only when Karn's rule permits it."""
        sample = self.rtt.acknowledge(sequence, acknowledged_at)
        self.metrics.record_delivery(payload_bytes)
        self.metrics.record_rtt(self.rtt.snapshot(), sample)
        return sample

    def on_timeout(self) -> float:
        """Record a timeout/loss event and return the backed-off RTO for ARQ reuse."""
        self.metrics.record_timeout()
        self.metrics.record_loss()
        return self.rtt.backoff_after_timeout()

    def evaluate(self, *, now: float) -> AdaptiveDecision:
        """Produce the current fixed-baseline or DART transmission decision."""
        if self.config.mode is TransportMode.P3_BASELINE:
            decision = AdaptiveDecision(
                state=AdaptiveState.STABLE,
                action=AdaptationAction.HOLD,
                window=self._window,
                rate_packets_per_second=self._rate,
                reason="fixed P3 baseline",
            )
        else:
            decision = self.engine.observe(self.metrics.snapshot(now=now))
            self._window = decision.window
            self._rate = decision.rate_packets_per_second
            self._state = decision.state
        self.metrics.record_adaptation(
            rate=decision.rate_packets_per_second,
            window=decision.window,
            state=decision.state.value,
            changed=decision.changed,
        )
        return decision

    def snapshot(self, *, now: float) -> MetricsSnapshot:
        """Expose one canonical snapshot to logging and experiment code."""
        return self.metrics.snapshot(now=now)
