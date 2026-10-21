"""
DART Adaptive Decision Engine Interface
Assigned Primary Owner: Areen Dubey
Catalogue Foundation: P3 Reliable Data Transfer over UDP + Adaptive Transmission Strategy

Provides bounded parameter adaptation hooks (window size, pacing rate, timeout bounds)
driven by measured RTT, loss, and retransmission metrics.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseAdaptiveEngine(ABC):
    """Abstract interface for transmission adaptation engines."""

    @abstractmethod
    def on_rtt_sample(self, rtt: float) -> None:
        """Called upon receiving a new RTT measurement."""
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


class FixedBaselineEngine(BaseAdaptiveEngine):
    """
    Fixed-parameter Reliable UDP baseline.
    Serves as the P3 baseline control group for comparative experiments.
    """

    def __init__(self, fixed_window: int = 1, fixed_pacing_ms: float = 0.0):
        self.fixed_window = fixed_window
        self.fixed_pacing_ms = fixed_pacing_ms

    def on_rtt_sample(self, rtt: float) -> None:
        pass

    def on_loss_event(self) -> None:
        pass

    def get_transmission_window(self) -> int:
        return self.fixed_window

    def get_pacing_interval_ms(self) -> float:
        return self.fixed_pacing_ms


class DARTAdaptiveEngine(BaseAdaptiveEngine):
    """
    DART Dynamic Adaptive Engine.
    Implements bounded parameter adjustment based on measured channel metrics.
    Areen Dubey will finalize the decision heuristic in Week 06.
    """

    def __init__(
        self,
        min_window: int = 1,
        max_window: int = 16,
        initial_window: int = 1
    ):
        self.min_window = min_window
        self.max_window = max_window
        self.current_window = initial_window
        self.rtt_history = []
        self.loss_events_count = 0

    def on_rtt_sample(self, rtt: float) -> None:
        self.rtt_history.append(rtt)
        # Bounded additive increase when latency is stable
        if len(self.rtt_history) > 3:
            recent_avg = sum(self.rtt_history[-3:]) / 3.0
            if rtt <= recent_avg * 1.1:
                self.current_window = min(self.max_window, self.current_window + 1)

    def on_loss_event(self) -> None:
        self.loss_events_count += 1
        # Bounded multiplicative decrease on congestion / loss
        self.current_window = max(self.min_window, self.current_window // 2)

    def get_transmission_window(self) -> int:
        return self.current_window

    def get_pacing_interval_ms(self) -> float:
        return 0.0
