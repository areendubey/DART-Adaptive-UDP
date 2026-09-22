"""A bounded and explainable DART transmission-parameter decision engine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging

from .metrics import MetricsSnapshot

LOGGER = logging.getLogger(__name__)


class AdaptiveState(str, Enum):
    STARTUP = "STARTUP"
    STABLE = "STABLE"
    CONGESTION_DETECTED = "CONGESTION_DETECTED"
    RECOVERY = "RECOVERY"


class AdaptationAction(str, Enum):
    INCREASE = "INCREASE"
    DECREASE = "DECREASE"
    HOLD = "HOLD"


@dataclass(frozen=True, slots=True)
class AdaptiveConfig:
    """Conservative, inspectable policy limits; no tuning constants are hidden in code."""

    initial_window: int = 4
    minimum_window: int = 1
    maximum_window: int = 64
    initial_rate_packets_per_second: float = 100.0
    minimum_rate_packets_per_second: float = 10.0
    maximum_rate_packets_per_second: float = 1_000.0
    window_increase_step: int = 1
    rate_increase_step: float = 10.0
    decrease_factor: float = 0.7
    minimum_observation_packets: int = 10
    loss_threshold: float = 0.05
    severe_loss_threshold: float = 0.15
    retransmission_threshold: float = 0.10
    timeout_threshold: float = 0.05
    rtt_increase_factor: float = 1.5
    recovery_healthy_observations: int = 3

    def __post_init__(self) -> None:
        if not 1 <= self.minimum_window <= self.initial_window <= self.maximum_window:
            raise ValueError("window bounds must contain initial window")
        if not 0 < self.minimum_rate_packets_per_second <= self.initial_rate_packets_per_second <= self.maximum_rate_packets_per_second:
            raise ValueError("rate bounds must contain initial rate")
        if self.window_increase_step < 1 or self.rate_increase_step <= 0:
            raise ValueError("increase steps must be positive")
        if not 0 < self.decrease_factor < 1:
            raise ValueError("decrease factor must be between zero and one")
        if self.minimum_observation_packets < 1 or self.recovery_healthy_observations < 1:
            raise ValueError("observation counts must be positive")


@dataclass(frozen=True, slots=True)
class AdaptiveDecision:
    """A pure integration contract: reliability applies this, the engine never touches sockets."""

    state: AdaptiveState
    action: AdaptationAction
    window: int
    rate_packets_per_second: float
    reason: str

    @property
    def changed(self) -> bool:
        return self.action is not AdaptationAction.HOLD


class AdaptiveEngine:
    """Changes rate/window gradually, using sustained loss, timeout, and RTT signals."""

    def __init__(self, config: AdaptiveConfig | None = None) -> None:
        self.config = config or AdaptiveConfig()
        self.state = AdaptiveState.STARTUP
        self.window = self.config.initial_window
        self.rate_packets_per_second = self.config.initial_rate_packets_per_second
        self._healthy_observations = 0

    def observe(self, metrics: MetricsSnapshot) -> AdaptiveDecision:
        """Return the next bounded parameter decision for one reliability observation."""
        if metrics.packets_sent < self.config.minimum_observation_packets:
            return self._decision(AdaptationAction.HOLD, "insufficient observations")
        degraded, severe, reason = self._degradation(metrics)
        if self.state is AdaptiveState.CONGESTION_DETECTED:
            self.state = AdaptiveState.RECOVERY
            self._healthy_observations = 0
            return self._decision(AdaptationAction.HOLD, "entering recovery")
        if degraded:
            self.state = AdaptiveState.CONGESTION_DETECTED
            self._healthy_observations = 0
            return self._decrease(reason if severe else f"network degradation: {reason}")
        if self.state is AdaptiveState.RECOVERY:
            self._healthy_observations += 1
            if self._healthy_observations < self.config.recovery_healthy_observations:
                return self._decision(AdaptationAction.HOLD, "recovery stability check")
            self.state = AdaptiveState.STABLE
            return self._increase("recovery completed")
        if self.state is AdaptiveState.STARTUP:
            self.state = AdaptiveState.STABLE
            return self._increase("healthy startup")
        return self._increase("healthy network")

    def _degradation(self, metrics: MetricsSnapshot) -> tuple[bool, bool, str]:
        signals: list[str] = []
        severe = metrics.loss_rate >= self.config.severe_loss_threshold
        if metrics.loss_rate >= self.config.loss_threshold:
            signals.append(f"loss={metrics.loss_rate:.1%}")
        if metrics.retransmission_rate >= self.config.retransmission_threshold:
            signals.append(f"retransmission={metrics.retransmission_rate:.1%}")
        if metrics.timeout_rate >= self.config.timeout_threshold:
            signals.append(f"timeouts={metrics.timeout_rate:.1%}")
        if (
            metrics.smoothed_rtt_seconds is not None
            and metrics.minimum_rtt_seconds is not None
            and metrics.smoothed_rtt_seconds >= metrics.minimum_rtt_seconds * self.config.rtt_increase_factor
        ):
            signals.append("RTT increase")
        return bool(signals), severe, ", ".join(signals) or "healthy"

    def _increase(self, reason: str) -> AdaptiveDecision:
        self.window = min(self.config.maximum_window, self.window + self.config.window_increase_step)
        self.rate_packets_per_second = min(
            self.config.maximum_rate_packets_per_second,
            self.rate_packets_per_second + self.config.rate_increase_step,
        )
        return self._decision(AdaptationAction.INCREASE, reason)

    def _decrease(self, reason: str) -> AdaptiveDecision:
        self.window = max(self.config.minimum_window, int(self.window * self.config.decrease_factor))
        self.rate_packets_per_second = max(
            self.config.minimum_rate_packets_per_second,
            self.rate_packets_per_second * self.config.decrease_factor,
        )
        return self._decision(AdaptationAction.DECREASE, reason)

    def _decision(self, action: AdaptationAction, reason: str) -> AdaptiveDecision:
        decision = AdaptiveDecision(self.state, action, self.window, self.rate_packets_per_second, reason)
        LOGGER.info(
            "[ADAPT] state=%s action=%s window=%d rate=%.2fpps reason=%s",
            decision.state.value,
            decision.action.value,
            decision.window,
            decision.rate_packets_per_second,
            decision.reason,
        )
        return decision
