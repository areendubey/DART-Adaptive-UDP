"""
config.py  -  the two objects every module shares.

Config : all tunable numbers in ONE place (window, RTO limits, ...).
Stats  : counters every ARQ fills in, so experiments/plots work for any protocol.
         Kartikey's logger calls stats.to_row(); Areen's engine reads the counters.
"""
import time
from dataclasses import dataclass, field


@dataclass
class Config:
    payload_size: int = 1024      # bytes of file data per packet
    window_size: int = 8          # STARTING sender window (DART may change it at runtime)
    min_window: int = 1           # DART's lower bound
    max_window: int = 64          # DART's upper bound; receiver buffers up to this many
    initial_rto: float = 0.2      # seconds
    min_rto: float = 0.02
    max_rto: float = 2.0
    max_retries: int = 30         # give up on one packet after this many retransmissions
    linger: float = 1.0           # receiver keeps re-ACKing this long after the file is complete
    idle_timeout: float = 30.0    # receiver gives up if nothing arrives for this long


@dataclass
class Stats:
    packets_sent: int = 0         # everything put on the wire (incl. retransmissions)
    retransmissions: int = 0      # packets sent again
    timeouts: int = 0             # timer expiries (in Selective Repeat: 1 per retransmission)
    acks_received: int = 0        # sender: valid, new ACKs
    bytes_delivered: int = 0      # file bytes acked (sender) / written to disk (receiver)
    duplicates: int = 0           # receiver: packets it had already seen
    corrupt: int = 0              # receiver: packets dropped by checksum
    out_of_order: int = 0         # receiver: packets that had to wait in the buffer
    rtt_samples: list = field(default_factory=list)     # seconds (Karn-filtered)
    window_trace: list = field(default_factory=list)    # [(seconds_since_start, window)]
    t_start: float = 0.0
    t_end: float = 0.0

    def start(self):
        self.t_start = time.monotonic()

    def stop(self):
        self.t_end = time.monotonic()

    @property
    def duration(self) -> float:
        return self.t_end - self.t_start

    @property
    def goodput_kbps(self) -> float:
        """Useful file bits per second (retransmissions do NOT count)."""
        return self.bytes_delivered * 8 / 1000 / self.duration if self.duration > 0 else 0.0

    @property
    def avg_rtt_ms(self) -> float:
        s = self.rtt_samples
        return 1000 * sum(s) / len(s) if s else 0.0

    def to_row(self) -> dict:
        """Flat dict of the numbers we plot; merge with experiment settings before writing CSV."""
        return {
            "duration_s": round(self.duration, 4),
            "goodput_kbps": round(self.goodput_kbps, 2),
            "retransmissions": self.retransmissions,
            "timeouts": self.timeouts,
            "packets_sent": self.packets_sent,
            "avg_rtt_ms": round(self.avg_rtt_ms, 3),
        }
