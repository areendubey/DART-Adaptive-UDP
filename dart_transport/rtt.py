"""Jacobson/Karels RTT estimation with Karn's ambiguity rule."""

from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True, slots=True)
class RtoConfig:
    """Explicit, conservative bounds for retransmission timeout estimation."""

    initial_rto_seconds: float = 1.0
    minimum_rto_seconds: float = 0.1
    maximum_rto_seconds: float = 60.0
    alpha: float = 1 / 8
    beta: float = 1 / 4
    clock_granularity_seconds: float = 0.001

    def __post_init__(self) -> None:
        if not 0 < self.minimum_rto_seconds <= self.initial_rto_seconds <= self.maximum_rto_seconds:
            raise ValueError("RTO bounds must contain the initial RTO")
        if not 0 < self.alpha < 1 or not 0 < self.beta < 1:
            raise ValueError("alpha and beta must be between zero and one")
        if self.clock_granularity_seconds <= 0:
            raise ValueError("clock granularity must be positive")


@dataclass(frozen=True, slots=True)
class RttSnapshot:
    """Current RTT estimator state for metrics and logging."""

    smoothed_rtt_seconds: float | None
    rtt_variation_seconds: float | None
    rto_seconds: float
    samples: int


@dataclass(slots=True)
class _OutstandingPacket:
    sent_at: float
    retransmitted: bool = False


class RttEstimator:
    """Tracks in-flight sends and only samples RTT for unambiguous ACKs."""

    def __init__(self, config: RtoConfig | None = None) -> None:
        self.config = config or RtoConfig()
        self._rto = self.config.initial_rto_seconds
        self._srtt: float | None = None
        self._rttvar: float | None = None
        self._samples = 0
        self._outstanding: dict[int, _OutstandingPacket] = {}

    @property
    def rto_seconds(self) -> float:
        return self._rto

    def record_send(self, sequence: int, sent_at: float) -> None:
        """Start RTT measurement for an original transmission."""
        self._outstanding[sequence] = _OutstandingPacket(sent_at=sent_at)

    def record_retransmission(self, sequence: int, sent_at: float) -> None:
        """Apply Karn's rule: the packet remains tracked but becomes unsampleable."""
        outstanding = self._outstanding.get(sequence)
        if outstanding is None:
            self._outstanding[sequence] = _OutstandingPacket(sent_at=sent_at, retransmitted=True)
        else:
            outstanding.sent_at = sent_at
            outstanding.retransmitted = True

    def acknowledge(self, sequence: int, acknowledged_at: float) -> float | None:
        """Record an ACK and return its RTT sample, or ``None`` under Karn's rule."""
        outstanding = self._outstanding.pop(sequence, None)
        if outstanding is None or outstanding.retransmitted:
            return None
        sample = acknowledged_at - outstanding.sent_at
        if not math.isfinite(sample) or sample <= 0:
            return None
        self._update(sample)
        return sample

    def backoff_after_timeout(self) -> float:
        """Exponentially back off RTO after a timeout, while preserving configured bounds."""
        self._rto = min(self.config.maximum_rto_seconds, self._rto * 2)
        return self._rto

    def snapshot(self) -> RttSnapshot:
        return RttSnapshot(self._srtt, self._rttvar, self._rto, self._samples)

    def _update(self, sample: float) -> None:
        if self._srtt is None:
            self._srtt = sample
            self._rttvar = sample / 2
        else:
            assert self._rttvar is not None
            self._rttvar = (1 - self.config.beta) * self._rttvar + self.config.beta * abs(self._srtt - sample)
            self._srtt = (1 - self.config.alpha) * self._srtt + self.config.alpha * sample
        assert self._rttvar is not None
        candidate = self._srtt + max(self.config.clock_granularity_seconds, 4 * self._rttvar)
        self._rto = min(self.config.maximum_rto_seconds, max(self.config.minimum_rto_seconds, candidate))
        self._samples += 1
