"""
dev_channel.py  -  a TINY stand-in for the team channel emulator, so Selective Repeat can be
tested today. Kartikey's real emulator (delay, jitter, time-varying loss, ...) replaces it.

The only thing SR needs from a channel is the 3-method socket interface, so ANY emulator that
wraps a socket with  sendto / recvfrom / settimeout  plugs in with zero changes to SR.

Faults are applied on sendto() and are seeded -> reproducible experiments.
"""
import random
import socket
import threading
from typing import Optional, Tuple

from config import Config, Stats
from rto import RTOEstimator
from selective_repeat import SRReceiver, SRSender


class FaultySocket:
    def __init__(self, sock, loss=0.0, dup=0.0, corrupt=0.0, reorder=0.0, seed=0):
        self._s = sock
        self.loss, self.dup, self.corrupt, self.reorder = loss, dup, corrupt, reorder
        self._rng = random.Random(seed)
        self._held = None                                   # one packet parked to be reordered

    def sendto(self, data: bytes, addr) -> None:
        r = self._rng
        if r.random() < self.loss:
            return                                          # dropped
        if r.random() < self.corrupt:                       # flip one byte
            b = bytearray(data)
            b[r.randrange(len(b))] ^= 0xFF
            data = bytes(b)
        for _ in range(2 if r.random() < self.dup else 1):  # maybe duplicate
            self._deliver(data, addr)

    def _deliver(self, data: bytes, addr) -> None:
        if self._held is None and self._rng.random() < self.reorder:
            self._held = (data, addr)                       # park it; send after the NEXT packet
            return
        self._s.sendto(data, addr)
        if self._held:
            held, self._held = self._held, None
            self._s.sendto(*held)

    def recvfrom(self, n):
        return self._s.recvfrom(n)

    def settimeout(self, t):
        self._s.settimeout(t)


def run_local_transfer(src: str, dst: str, cfg: Optional[Config] = None,
                       rto: Optional[RTOEstimator] = None, seed: int = 0,
                       window: Optional[int] = None,
                       **faults) -> Tuple[Stats, Stats]:
    """Send `src` -> `dst` over localhost UDP through a faulty channel. Returns (sender, receiver) stats."""
    cfg = cfg or Config()
    rs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rs.bind(("127.0.0.1", 0))
    ss.bind(("127.0.0.1", 0))
    receiver = SRReceiver(FaultySocket(rs, seed=seed * 2 + 1, **faults), cfg)
    sender = SRSender(FaultySocket(ss, seed=seed * 2, **faults), rs.getsockname(), cfg, rto)
    if window:
        sender.window_size = window

    err = []
    def _rx():
        try:
            receiver.receive_file(dst)
        except Exception as e:                              # surface thread errors in the caller
            err.append(e)
    t = threading.Thread(target=_rx)
    t.start()
    try:
        sender.send_file(src)
    finally:
        t.join()
        rs.close()
        ss.close()
    if err:
        raise err[0]
    return sender.stats, receiver.stats
