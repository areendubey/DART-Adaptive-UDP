"""
DART Metrics, Event Logging, and Transfer Statistics
"""

import time
import json
import csv
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class PacketEvent:
    timestamp: float
    event_type: str  # "SEND", "RECV", "ACK", "RETRANSMIT", "TIMEOUT", "DROP", "CORRUPT", "DUP", "REORDER"
    seq_num: int
    ack_num: int
    size_bytes: int
    rtt_ms: Optional[float] = None
    current_rto_ms: Optional[float] = None
    info: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": round(self.timestamp, 4),
            "event_type": self.event_type,
            "seq_num": self.seq_num,
            "ack_num": self.ack_num,
            "size_bytes": self.size_bytes,
            "rtt_ms": round(self.rtt_ms, 2) if self.rtt_ms is not None else None,
            "current_rto_ms": round(self.current_rto_ms, 2) if self.current_rto_ms is not None else None,
            "info": self.info
        }


@dataclass
class TransferStats:
    total_data_bytes: int = 0
    total_packets_sent: int = 0
    total_packets_retransmitted: int = 0
    total_acks_received: int = 0
    total_timeouts: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    integrity_verified: bool = False
    source_sha256: str = ""
    received_sha256: str = ""
    rtt_samples: List[float] = field(default_factory=list)
    events: List[PacketEvent] = field(default_factory=list)

    def mark_start(self):
        self.start_time = time.time()

    def mark_end(self):
        self.end_time = time.time()

    @property
    def elapsed_time(self) -> float:
        if self.end_time is None:
            return max(0.0001, time.time() - self.start_time)
        return max(0.0001, self.end_time - self.start_time)

    @property
    def goodput_kb_s(self) -> float:
        """Effective goodput in Kilobytes per second (useful payload / time)."""
        return (self.total_data_bytes / 1024.0) / self.elapsed_time

    @property
    def goodput_mbps(self) -> float:
        """Effective goodput in Megabits per second."""
        return (self.total_data_bytes * 8.0 / 1_000_000.0) / self.elapsed_time

    @property
    def retransmission_overhead_pct(self) -> float:
        """Retransmission overhead as a percentage of total transmissions."""
        if self.total_packets_sent == 0:
            return 0.0
        return (self.total_packets_retransmitted / self.total_packets_sent) * 100.0

    @property
    def avg_rtt_ms(self) -> float:
        if not self.rtt_samples:
            return 0.0
        return (sum(self.rtt_samples) / len(self.rtt_samples)) * 1000.0

    def add_event(self, event_type: str, seq: int = 0, ack: int = 0, size: int = 0,
                  rtt: Optional[float] = None, rto: Optional[float] = None, info: str = ""):
        event = PacketEvent(
            timestamp=time.time(),
            event_type=event_type,
            seq_num=seq,
            ack_num=ack,
            size_bytes=size,
            rtt_ms=rtt * 1000.0 if rtt is not None else None,
            current_rto_ms=rto * 1000.0 if rto is not None else None,
            info=info
        )
        self.events.append(event)

    def summary(self) -> Dict[str, Any]:
        return {
            "total_data_bytes": self.total_data_bytes,
            "elapsed_time_sec": round(self.elapsed_time, 4),
            "goodput_kb_s": round(self.goodput_kb_s, 2),
            "goodput_mbps": round(self.goodput_mbps, 3),
            "total_packets_sent": self.total_packets_sent,
            "total_packets_retransmitted": self.total_packets_retransmitted,
            "retransmission_overhead_pct": round(self.retransmission_overhead_pct, 2),
            "total_timeouts": self.total_timeouts,
            "avg_rtt_ms": round(self.avg_rtt_ms, 2),
            "integrity_verified": self.integrity_verified,
            "source_sha256": self.source_sha256,
            "received_sha256": self.received_sha256
        }

    def export_csv(self, filepath: str):
        """Exports all packet-level events to a CSV file."""
        if not self.events:
            return
        keys = ["timestamp", "event_type", "seq_num", "ack_num", "size_bytes", "rtt_ms", "current_rto_ms", "info"]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for ev in self.events:
                writer.writerow(ev.to_dict())
