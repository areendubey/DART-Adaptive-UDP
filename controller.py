"""Reliability controller interface (stand-in for dart_transport's
ReliabilityController) plus the P3 static controller.

GoBackNSender calls these hooks, and asks the controller how big the window
may be. That is all the sender needs, so it can run in two modes:

    P3 Static mode      -> StaticController(window)   (fixed window)
    DART Adaptive mode  -> Areen's adaptive controller (window changes)

Hooks:
    on_packet_sent(seq, is_retransmit)
    on_acknowledged(ack, newly_acked, rtt_sample)   rtt_sample is None when
                                                    Karn's rule skipped it
    on_timeout()
Property:
    window_size   -> how many packets may be outstanding right now
"""


class ReliabilityController:
    """Base class: does nothing, keeps a fixed window."""

    def __init__(self, window_size=8):
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        self._window_size = window_size

    @property
    def window_size(self):
        return self._window_size

    def on_packet_sent(self, seq, is_retransmit):
        pass

    def on_acknowledged(self, ack, newly_acked, rtt_sample):
        pass

    def on_timeout(self):
        pass


class StaticController(ReliabilityController):
    """P3 baseline: the window never changes."""
