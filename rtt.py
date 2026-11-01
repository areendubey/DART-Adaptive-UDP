"""RTT estimation and RTO (stand-in for dart_transport.rtt.RttEstimator).

Jacobson/Karels (RFC 6298):
    first sample R:   SRTT = R,  RTTVAR = R / 2
    later samples R:  RTTVAR = (1 - BETA) * RTTVAR + BETA * |SRTT - R|
                      SRTT   = (1 - ALPHA) * SRTT + ALPHA * R
    RTO = SRTT + K * RTTVAR       (then clamped to [min_rto, max_rto])

Karn's algorithm has two halves:
    1. Never feed update() a sample taken from a retransmitted packet
       (the caller, GoBackNSender, is responsible for this half).
    2. On a timeout, double the RTO (on_timeout).

Backoff reset (design choice, same as real TCP stacks):
    Go-Back-N resends the whole window after a timeout, so under heavy loss
    almost every packet is "retransmitted" and clean samples are rare. If the
    backed-off RTO were kept until a clean sample, it would stay at the maximum
    even though ACKs are clearly flowing. So when an ACK moves the window
    forward, the caller calls reset_backoff(), which recomputes the RTO from
    SRTT/RTTVAR. Samples are STILL never taken from retransmitted packets.
"""

from dart.constants import DEFAULT_TIMEOUT_S, RTO_MAX_S, RTO_MIN_S

ALPHA = 0.125
BETA = 0.25
K = 4


class RttEstimator:
    def __init__(self, initial_rto=DEFAULT_TIMEOUT_S, min_rto=RTO_MIN_S, max_rto=RTO_MAX_S):
        if initial_rto <= 0:
            raise ValueError("initial_rto must be > 0")
        if min_rto <= 0 or max_rto < min_rto:
            raise ValueError("need 0 < min_rto <= max_rto")
        self.min_rto = min_rto
        self.max_rto = max_rto
        self.srtt = None
        self.rttvar = None
        self.samples = 0
        self._initial_rto = initial_rto
        self._rto = self._clamp(initial_rto)

    def _clamp(self, value):
        if value < self.min_rto:
            return self.min_rto
        if value > self.max_rto:
            return self.max_rto
        return value

    @property
    def rto(self):
        """Current retransmission timeout in seconds."""
        return self._rto

    def update(self, sample):
        """Feed one RTT sample (seconds) from a packet that was NOT retransmitted."""
        if sample <= 0:
            return
        if self.srtt is None:
            self.srtt = sample
            self.rttvar = sample / 2.0
        else:
            difference = self.srtt - sample
            if difference < 0:
                difference = -difference
            self.rttvar = (1 - BETA) * self.rttvar + BETA * difference
            self.srtt = (1 - ALPHA) * self.srtt + ALPHA * sample
        self.samples = self.samples + 1
        self._rto = self._clamp(self.srtt + K * self.rttvar)

    def reset_backoff(self):
        """Undo timeout backoff after forward progress (see module docstring)."""
        if self.srtt is None:
            self._rto = self._clamp(self._initial_rto)
        else:
            self._rto = self._clamp(self.srtt + K * self.rttvar)

    def on_timeout(self):
        """Exponential backoff after a retransmission timeout (Karn, part 2)."""
        self._rto = self._clamp(self._rto * 2)
