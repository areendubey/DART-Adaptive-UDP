"""
DART Adaptive Decision Engine
Author: Areen Dubey (Primary Owner)
Course: CS-30003 CSE 34
Catalogue Foundation: P3 Reliable Data Transfer over UDP + Adaptive Transmission Strategy

Implements a deterministic, bounded, and explainable adaptive transmission engine:
  - Finite State Machine: STARTUP, STABLE, CONGESTION_DETECTED, RECOVERY
  - Bounded transmission window scaling [min_window, max_window]
  - Adaptive packet pacing and timeout boundary adjustments
  - RTT inflation detection, loss thresholding, and fast recovery
  - FixedBaselineEngine for reproducible static P3 comparison
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, List, Optional

LOGGER = logging.getLogger("dart.adaptive")


class AdaptiveState(str, Enum):
    STARTUP = "STARTUP"
    STABLE = "STABLE"
    CONGESTION_DETECTED = "CONGESTION_DETECTED"
    RECOVERY = "RECOVERY"


class AdaptationAction(str, Enum):
    INCREASE = "INCREASE"
    DECREASE = "DECREASE"
    HOLD = "HOLD"


class BaseAdaptiveEngine(ABC):
    """Abstract interface for all transmission adaptation engines."""

    @abstractmethod
    def on_rtt_sample(self, rtt: float) -> None:
        """Called upon receiving a fresh, non-retransmitted RTT sample."""
        pass

    @abstractmethod
    def on_loss_event(self) -> None:
        """Called upon timeout or packet loss detection."""
        pass

    @abstractmethod
    def get_transmission_window(self) -> int:
        """Returns the bounded transmission window size."""
        pass

    @abstractmethod
    def get_pacing_interval_ms(self) -> float:
        """Returns transmission pacing delay in milliseconds."""
        pass

    @abstractmethod
    def get_state(self) -> str:
        """Returns the current state name for telemetry and UI visualization."""
        pass


class FixedBaselineEngine(BaseAdaptiveEngine):
    """
    Fixed-parameter Reliable UDP baseline.
    Serves as the static P3 baseline control group for comparative experiments.
    """

    def __init__(self, fixed_window: int = 1, fixed_pacing_ms: float = 0.0):
        self.fixed_window = max(1, fixed_window)
        self.fixed_pacing_ms = max(0.0, fixed_pacing_ms)

    def on_rtt_sample(self, rtt: float) -> None:
        pass

    def on_loss_event(self) -> None:
        pass

    def get_transmission_window(self) -> int:
        return self.fixed_window

    def get_pacing_interval_ms(self) -> float:
        return self.fixed_pacing_ms

    def get_state(self) -> str:
        return "STATIC_P3"


@dataclass
class DARTConfig:
    """Conservative, inspectable policy limits for bounded adaptation."""
    initial_window: int = 4
    min_window: int = 1
    max_window: int = 32
    window_increase_step: int = 1
    decrease_factor: float = 0.5
    rtt_inflation_threshold: float = 1.35
    recovery_healthy_required: int = 3
    initial_pacing_ms: float = 0.0
    max_pacing_ms: float = 50.0


class DARTAdaptiveEngine(BaseAdaptiveEngine):
    """
    DART State-Machine Driven Adaptive Engine.
    Dynamically adjusts window and pacing based on network conditions.
    """

    def __init__(self, config: Optional[DARTConfig] = None):
        self.config = config or DARTConfig()
        self.state = AdaptiveState.STARTUP
        self.window = self.config.initial_window
        self.pacing_ms = self.config.initial_pacing_ms
        self.rtt_history: List[float] = []
        self.srtt: Optional[float] = None
        self.min_rtt: Optional[float] = None
        self.healthy_observations = 0
        self.loss_events_count = 0
        self.total_adjustments = 0

    def on_rtt_sample(self, rtt: float) -> None:
        """Processes fresh RTT sample and performs bounded additive adjustments."""
        self.rtt_history.append(rtt)
        if len(self.rtt_history) > 50:
            self.rtt_history.pop(0)

        if self.min_rtt is None or rtt < self.min_rtt:
            self.min_rtt = rtt

        # Update smoothed RTT
        if self.srtt is None:
            self.srtt = rtt
        else:
            self.srtt = 0.875 * self.srtt + 0.125 * rtt

        # Check for RTT inflation indicating bufferbloat or queuing
        is_rtt_inflated = (
            self.min_rtt is not None and
            self.srtt > self.min_rtt * self.config.rtt_inflation_threshold
        )

        if is_rtt_inflated:
            # Latency is growing: hold or slightly pace down to avoid drop
            self.pacing_ms = min(self.config.max_pacing_ms, self.pacing_ms + 1.0)
            return

        # Handle State transitions
        if self.state == AdaptiveState.STARTUP:
            # Gentle ramp up
            self.window = min(self.config.max_window, self.window + self.config.window_increase_step)
            if self.window >= min(8, self.config.max_window):
                self.state = AdaptiveState.STABLE

        elif self.state == AdaptiveState.STABLE:
            # Additive Increase when healthy
            self.healthy_observations += 1
            if self.healthy_observations >= 2:
                self.window = min(self.config.max_window, self.window + self.config.window_increase_step)
                self.pacing_ms = max(0.0, self.pacing_ms - 0.5)
                self.healthy_observations = 0

        elif self.state == AdaptiveState.RECOVERY:
            self.healthy_observations += 1
            if self.healthy_observations >= self.config.recovery_healthy_required:
                self.state = AdaptiveState.STABLE
                self.window = min(self.config.max_window, self.window + self.config.window_increase_step)
                self.healthy_observations = 0

    def on_loss_event(self) -> None:
        """Multiplicative decrease and transition to CONGESTION_DETECTED on loss."""
        self.loss_events_count += 1
        self.total_adjustments += 1
        self.state = AdaptiveState.CONGESTION_DETECTED
        self.healthy_observations = 0

        # Multiplicative decrease
        new_window = int(self.window * self.config.decrease_factor)
        self.window = max(self.config.min_window, new_window)

        # Apply protective pacing
        self.pacing_ms = min(self.config.max_pacing_ms, self.pacing_ms + 2.0)

        # Transition immediately to recovery state to observe subsequent packets
        self.state = AdaptiveState.RECOVERY

    def get_transmission_window(self) -> int:
        return self.window

    def get_pacing_interval_ms(self) -> float:
        return self.pacing_ms

    def get_state(self) -> str:
        return self.state.value

    def telemetry_snapshot(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "window": self.window,
            "pacing_ms": round(self.pacing_ms, 2),
            "srtt_ms": round((self.srtt or 0.0) * 1000.0, 2),
            "min_rtt_ms": round((self.min_rtt or 0.0) * 1000.0, 2),
            "loss_events": self.loss_events_count
        }
