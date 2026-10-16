"""Deterministic datagram dropper for GBN reliability tests only.

This is not the team channel emulator (Kartikey Singh). It only lets
Anushka Roy's correctness tests inject loss, ACK loss, and corruption.
"""

from __future__ import annotations

import random
import socket
from typing import Optional


class LossySocket:
    def __init__(
        self,
        sock: socket.socket,
        drop_prob: float = 0.0,
        seed: int = 1,
        corrupt_prob: float = 0.0,
        drop_outbound: bool = True,
        drop_inbound: bool = False,
    ) -> None:
        self._sock = sock
        self.drop_prob = drop_prob
        self.corrupt_prob = corrupt_prob
        self.drop_outbound = drop_outbound
        self.drop_inbound = drop_inbound
        self.rng = random.Random(seed)
        self.dropped_out = 0
        self.dropped_in = 0
        self.corrupted = 0
        self.sent = 0
        self.received = 0

    def bind(self, address) -> None:
        self._sock.bind(address)

    def getsockname(self):
        return self._sock.getsockname()

    def settimeout(self, value: Optional[float]) -> None:
        self._sock.settimeout(value)

    def close(self) -> None:
        self._sock.close()

    def sendto(self, data: bytes, addr) -> int:
        if self.drop_outbound and self.rng.random() < self.drop_prob:
            self.dropped_out += 1
            return len(data)
        payload = data
        if self.rng.random() < self.corrupt_prob and payload:
            buf = bytearray(payload)
            buf[-1] ^= 0xFF
            payload = bytes(buf)
            self.corrupted += 1
        self.sent += 1
        return self._sock.sendto(payload, addr)

    def recvfrom(self, bufsize: int):
        while True:
            data, addr = self._sock.recvfrom(bufsize)
            if self.drop_inbound and self.rng.random() < self.drop_prob:
                self.dropped_in += 1
                continue
            self.received += 1
            return data, addr


class DropFirstN:
    """Drop the first N outbound datagrams, then pass everything through."""

    def __init__(self, sock: socket.socket, drops: int) -> None:
        self._sock = sock
        self.left = drops

    def settimeout(self, value: Optional[float]) -> None:
        self._sock.settimeout(value)

    def recvfrom(self, bufsize: int):
        return self._sock.recvfrom(bufsize)

    def sendto(self, data: bytes, addr) -> int:
        if self.left > 0:
            self.left -= 1
            return len(data)
        return self._sock.sendto(data, addr)
