"""
rto.py  -  "how long do I wait before retransmitting?"

Every ARQ asks an RTOEstimator, so we can swap strategies without touching ARQ code:
    FixedRTO           -> the baseline (needed for the "sensitivity to RTO tuning" experiment)
    JacobsonKarelsRTO  -> adaptive RTO (Week 5: Areen + Kartikey own the final version;
                          this is a small reference so SR can be tested today)

Contract (keep these 3 method names and everyone stays compatible):
    current()            -> seconds to wait now
    on_rtt_sample(rtt)   -> called with a clean RTT measurement (Karn's rule is applied by the ARQ)
    on_timeout()         -> called when a timer expired (adaptive versions back off)
"""


class RTOEstimator:
    def current(self) -> float:
        raise NotImplementedError

    def on_rtt_sample(self, rtt: float) -> None:
        pass

    def on_timeout(self) -> None:
        pass


class FixedRTO(RTOEstimator):
    def __init__(self, rto: float = 0.2):
        self._rto = rto

    def current(self) -> float:
        return self._rto


class JacobsonKarelsRTO(RTOEstimator):
    """RFC 6298 style:  RTO = SRTT + 4*RTTVAR, doubled after every timeout (back-off)."""

    def __init__(self, initial: float = 0.2, min_rto: float = 0.02, max_rto: float = 2.0):
        self.srtt = None
        self.rttvar = None
        self.min_rto, self.max_rto = min_rto, max_rto
        self._rto = initial

    def current(self) -> float:
        return self._rto

    def on_rtt_sample(self, rtt: float) -> None:
        if self.srtt is None:                                   # first measurement
            self.srtt, self.rttvar = rtt, rtt / 2
        else:                                                   # alpha = 1/8, beta = 1/4
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
            self.srtt = 0.875 * self.srtt + 0.125 * rtt
        self._rto = min(max(self.srtt + 4 * self.rttvar, self.min_rto), self.max_rto)

    def on_timeout(self) -> None:
        self._rto = min(self._rto * 2, self.max_rto)
