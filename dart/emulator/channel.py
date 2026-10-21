"""
DART Network Channel Emulator
Author: Kartikey Singh (Primary Owner)
Catalogue Foundation: P3 Reliable Data Transfer over UDP

Emulates realistic, degraded network channel conditions with deterministic reproducibility:
  - Packet Loss (configurable drop rate)
  - Packet Corruption (bit/byte flipping causing RFC 1071 checksum failures)
  - Packet Duplication (configurable duplicate rate)
  - Packet Reordering (configurable inversion of consecutive packet arrivals)
  - Propagation Delay & Jitter (normal/uniform distribution)
  - Configurable PRNG Seed for deterministic, reproducible benchmarking
"""

import random
import time
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Any


@dataclass
class EmulatorStats:
    total_packets_seen: int = 0
    packets_dropped: int = 0
    packets_corrupted: int = 0
    packets_duplicated: int = 0
    packets_reordered: int = 0
    packets_delayed: int = 0

    def summary(self) -> Dict[str, Any]:
        return {
            "total_seen": self.total_packets_seen,
            "dropped": self.packets_dropped,
            "corrupted": self.packets_corrupted,
            "duplicated": self.packets_duplicated,
            "reordered": self.packets_reordered,
            "delayed": self.packets_delayed,
            "effective_loss_rate": (self.packets_dropped / max(1, self.total_packets_seen)) * 100.0
        }


@dataclass
class EmulatorConfig:
    loss_rate: float = 0.0          # Probability of packet loss in [0.0, 1.0]
    corruption_rate: float = 0.0    # Probability of bit/byte corruption in [0.0, 1.0]
    duplication_rate: float = 0.0   # Probability of packet duplication in [0.0, 1.0]
    reorder_rate: float = 0.0       # Probability of reordering consecutive packets in [0.0, 1.0]
    delay_mean_ms: float = 0.0      # Average one-way propagation delay in milliseconds
    delay_jitter_ms: float = 0.0    # Random jitter (+/- jitter) around delay_mean_ms
    seed: Optional[int] = None      # Deterministic PRNG seed for reproducible experiments


class NetworkChannelEmulator:
    """
    Deterministic Channel Impairment Engine.
    Processes packets through simulated network imperfections.
    """

    def __init__(self, config: Optional[EmulatorConfig] = None):
        self.config = config or EmulatorConfig()
        self.rng = random.Random(self.config.seed) if self.config.seed is not None else random.Random()
        self.stats = EmulatorStats()
        self._reorder_buffer: Optional[bytes] = None

    def reset(self, new_seed: Optional[int] = None):
        """Resets emulator state and reinitializes RNG seed."""
        if new_seed is not None:
            self.config.seed = new_seed
        self.rng = random.Random(self.config.seed) if self.config.seed is not None else random.Random()
        self.stats = EmulatorStats()
        self._reorder_buffer = None

    def _calculate_delay(self) -> float:
        """Calculates transit delay in seconds based on mean delay and jitter."""
        if self.config.delay_mean_ms <= 0:
            return 0.0
        jitter = 0.0
        if self.config.delay_jitter_ms > 0:
            jitter = self.rng.uniform(-self.config.delay_jitter_ms, self.config.delay_jitter_ms)
        delay_ms = max(0.0, self.config.delay_mean_ms + jitter)
        return delay_ms / 1000.0

    def _corrupt_packet(self, packet_bytes: bytes) -> bytes:
        """Inverts 1 to 4 bytes in the packet to simulate corruption in transmission."""
        if len(packet_bytes) == 0:
            return packet_bytes
        data = bytearray(packet_bytes)
        num_flips = min(len(data), self.rng.randint(1, 4))
        indices = self.rng.sample(range(len(data)), num_flips)
        for idx in indices:
            data[idx] ^= self.rng.randint(1, 255)
        return bytes(data)

    def process_packet(self, packet_bytes: bytes) -> List[Tuple[bytes, float]]:
        """
        Applies network channel impairments to an incoming packet.
        Returns a list of tuples: [(deliverable_bytes, delay_in_seconds), ...]
        If packet is dropped, returns an empty list [].
        """
        self.stats.total_packets_seen += 1

        # 1. Packet Loss Check
        if self.config.loss_rate > 0 and self.rng.random() < self.config.loss_rate:
            self.stats.packets_dropped += 1
            return []

        # 2. Packet Corruption Check
        corrupted = False
        processed_bytes = packet_bytes
        if self.config.corruption_rate > 0 and self.rng.random() < self.config.corruption_rate:
            processed_bytes = self._corrupt_packet(processed_bytes)
            self.stats.packets_corrupted += 1
            corrupted = True

        base_delay = self._calculate_delay()
        if base_delay > 0:
            self.stats.packets_delayed += 1

        # 3. Packet Reordering Check
        # If reordering triggers and buffer is empty, hold this packet back
        if self.config.reorder_rate > 0 and self._reorder_buffer is None and self.rng.random() < self.config.reorder_rate:
            self._reorder_buffer = processed_bytes
            self.stats.packets_reordered += 1
            return []

        deliverables: List[Tuple[bytes, float]] = []

        # If a previously buffered packet was waiting to be overtaken, release it now
        if self._reorder_buffer is not None:
            # Deliver current packet first (earlier delay)
            deliverables.append((processed_bytes, base_delay))
            # Deliver previously held packet shortly after
            delayed_held = base_delay + self._calculate_delay() + 0.005  # Slight offset ensuring arrival order inversion
            deliverables.append((self._reorder_buffer, delayed_held))
            self._reorder_buffer = None
        else:
            deliverables.append((processed_bytes, base_delay))

        # 4. Packet Duplication Check
        if self.config.duplication_rate > 0 and self.rng.random() < self.config.duplication_rate:
            # Duplicate arrives with a small delay jitter
            dup_delay = base_delay + (self.rng.uniform(0.005, 0.020))
            deliverables.append((processed_bytes, dup_delay))
            self.stats.packets_duplicated += 1

        return deliverables

    def flush(self) -> List[Tuple[bytes, float]]:
        """Flushes any buffered packet remaining in reorder queue."""
        if self._reorder_buffer is not None:
            buf = self._reorder_buffer
            self._reorder_buffer = None
            return [(buf, self._calculate_delay())]
        return []
