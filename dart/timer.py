"""
Adaptive RTO Estimator implementing Jacobson/Karels Algorithm and Karn's Algorithm (RFC 6298 compliant).
"""

import time
from typing import Optional


class AdaptiveRTOEstimator:
    """
    Implements Jacobson/Karels dynamic RTO estimation:
      - SRTT (Smoothed Round-Trip Time)
      - RTTVAR (Round-Trip Time Variation)
      - RTO = SRTT + max(G, 4 * RTTVAR)
    Includes Karn's algorithm:
      - Ambiguous RTT samples (from retransmitted packets) are IGNORED.
      - Exponential backoff on timeout: RTO = min(RTO * 2, max_rto).
      - Backoff resets upon acknowledgement of a clean (non-retransmitted) packet.
    """

    def __init__(
        self,
        initial_rto: float = 1.0,
        min_rto: float = 0.1,
        max_rto: float = 10.0,
        alpha: float = 0.125,  # 1/8
        beta: float = 0.25,    # 1/4
        clock_granularity: float = 0.001  # 1ms clock granularity (G)
    ):
        self.initial_rto = initial_rto
        self.min_rto = min_rto
        self.max_rto = max_rto
        self.alpha = alpha
        self.beta = beta
        self.granularity = clock_granularity

        # State variables
        self.srtt: Optional[float] = None
        self.rttvar: Optional[float] = None
        self._current_rto: float = initial_rto
        self._backoff_multiplier: float = 1.0

        # Diagnostics & Tracking
        self.sample_count: int = 0
        self.timeout_count: int = 0
        self.last_rtt_sample: Optional[float] = None

    @property
    def rto(self) -> float:
        """Returns the current effective RTO in seconds with backoff applied."""
        effective_rto = self._current_rto * self._backoff_multiplier
        return max(self.min_rto, min(self.max_rto, effective_rto))

    def update_rtt(self, rtt_sample: float, is_retransmission: bool = False):
        """
        Processes a newly measured RTT sample.
        Karn's Rule: if packet was retransmitted, DO NOT update SRTT/RTTVAR.
        """
        if is_retransmission:
            # Karn's algorithm: ignore retransmitted samples to avoid ACK ambiguity
            return

        if rtt_sample <= 0:
            return

        self.last_rtt_sample = rtt_sample
        self.sample_count += 1
        # Non-retransmitted ACK resets exponential backoff multiplier
        self._backoff_multiplier = 1.0

        if self.srtt is None:
            # First measurement (RFC 6298 section 2.2)
            self.srtt = rtt_sample
            self.rttvar = rtt_sample / 2.0
            self._current_rto = self.srtt + max(self.granularity, 4.0 * self.rttvar)
        else:
            # Subsequent measurements (RFC 6298 section 2.3)
            diff = abs(self.srtt - rtt_sample)
            self.rttvar = (1.0 - self.beta) * self.rttvar + self.beta * diff
            self.srtt = (1.0 - self.alpha) * self.srtt + self.alpha * rtt_sample
            self._current_rto = self.srtt + max(self.granularity, 4.0 * self.rttvar)

        # Clamp base RTO within bounds
        self._current_rto = max(self.min_rto, min(self.max_rto, self._current_rto))

    def on_timeout(self):
        """
        Called when a retransmission timeout occurs.
        Karn's Rule: applies exponential backoff to RTO.
        """
        self.timeout_count += 1
        # Double the backoff multiplier
        self._backoff_multiplier = min(32.0, self._backoff_multiplier * 2.0)

    def reset(self):
        """Resets the estimator to initial state."""
        self.srtt = None
        self.rttvar = None
        self._current_rto = self.initial_rto
        self._backoff_multiplier = 1.0
        self.sample_count = 0
        self.timeout_count = 0
        self.last_rtt_sample = None
