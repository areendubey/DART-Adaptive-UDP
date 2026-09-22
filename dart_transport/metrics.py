"""A single source of transport measurements for ARQ, DART, and experiments."""

from __future__ import annotations

from dataclasses import dataclass
import time

from .rtt import RttSnapshot


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    """Immutable observation passed from reliability code to the adaptive engine."""

    packets_sent: int
    packets_delivered: int
    packets_lost: int
    retransmissions: int
    timeout_events: int
    bytes_delivered: int
    elapsed_seconds: float
    loss_rate: float
    retransmission_rate: float
    timeout_rate: float
    goodput_bytes_per_second: float
    smoothed_rtt_seconds: float | None
    rtt_variation_seconds: float | None
    minimum_rtt_seconds: float | None
    rto_seconds: float
    rtt_samples: int
    current_rate_packets_per_second: float
    current_window: int
    adaptive_state: str
    adaptation_events: int


class TransferMetrics:
    """Collects counts centrally so reported and adaptive calculations cannot diverge."""

    def __init__(self, *, started_at: float | None = None) -> None:
        self._started_at = time.monotonic() if started_at is None else started_at
        self.packets_sent = 0
        self.packets_delivered = 0
        self.packets_lost = 0
        self.retransmissions = 0
        self.timeout_events = 0
        self.bytes_delivered = 0
        self._minimum_rtt: float | None = None
        self._rtt = RttSnapshot(None, None, 1.0, 0)
        self._rate = 0.0
        self._window = 0
        self._adaptive_state = "STARTUP"
        self._adaptation_events = 0

    def record_sent(self, *, retransmission: bool = False) -> None:
        self.packets_sent += 1
        if retransmission:
            self.retransmissions += 1

    def record_delivery(self, payload_bytes: int) -> None:
        if payload_bytes < 0:
            raise ValueError("payload bytes cannot be negative")
        self.packets_delivered += 1
        self.bytes_delivered += payload_bytes

    def record_loss(self) -> None:
        self.packets_lost += 1

    def record_timeout(self) -> None:
        self.timeout_events += 1

    def record_rtt(self, snapshot: RttSnapshot, sample_seconds: float | None = None) -> None:
        self._rtt = snapshot
        if sample_seconds is not None:
            self._minimum_rtt = sample_seconds if self._minimum_rtt is None else min(self._minimum_rtt, sample_seconds)

    def record_adaptation(self, *, rate: float, window: int, state: str, changed: bool) -> None:
        self._rate = rate
        self._window = window
        self._adaptive_state = state
        if changed:
            self._adaptation_events += 1

    def snapshot(self, *, now: float | None = None) -> MetricsSnapshot:
        current = time.monotonic() if now is None else now
        elapsed = max(0.000_001, current - self._started_at)
        return MetricsSnapshot(
            packets_sent=self.packets_sent,
            packets_delivered=self.packets_delivered,
            packets_lost=self.packets_lost,
            retransmissions=self.retransmissions,
            timeout_events=self.timeout_events,
            bytes_delivered=self.bytes_delivered,
            elapsed_seconds=elapsed,
            loss_rate=self.packets_lost / self.packets_sent if self.packets_sent else 0.0,
            retransmission_rate=self.retransmissions / self.packets_sent if self.packets_sent else 0.0,
            timeout_rate=self.timeout_events / self.packets_sent if self.packets_sent else 0.0,
            goodput_bytes_per_second=self.bytes_delivered / elapsed,
            smoothed_rtt_seconds=self._rtt.smoothed_rtt_seconds,
            rtt_variation_seconds=self._rtt.rtt_variation_seconds,
            minimum_rtt_seconds=self._minimum_rtt,
            rto_seconds=self._rtt.rto_seconds,
            rtt_samples=self._rtt.samples,
            current_rate_packets_per_second=self._rate,
            current_window=self._window,
            adaptive_state=self._adaptive_state,
            adaptation_events=self._adaptation_events,
        )
