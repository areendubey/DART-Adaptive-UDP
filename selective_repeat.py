"""
selective_repeat.py  --  DART / CS-30003 / CSE 34  (Owner: Savvy Jatrana)

Selective Repeat ARQ over UDP, built on the team's shared modules:
    config.Config   -> payload_size, window_size, min_window, max_window,
                       initial_rto, min_rto, max_rto, max_retries, linger, idle_timeout
    config.Stats    -> filled exactly as documented in config.py (SRStats subclasses it
                       and only ADDS fields, so stats.to_row() is identical to the
                       other ARQs; stats.extended_row() adds SR-specific extras)
    rto.RTOEstimator-> current() / on_rtt_sample(rtt) / on_timeout()

Public API (used by cli.py and dev_channel.py)
    SRSender(sock, peer, cfg=None, rto=None, controller=None, dupthresh=<cfg or 3>)
        .send_file(path) / .send_bytes(data) / .send_stream(fobj) -> SRStats
        .window_size   read/write at any time (clamped to [cfg.min_window, cfg.max_window])
                       -> this is the DART adaptive-engine hook
        .stats
    SRReceiver(sock, cfg=None)
        .receive_file(path) / .receive_stream(fobj) -> SRStats
        .stats

Socket contract: only sendto / recvfrom / settimeout are used, so a real UDP socket or any
emulator wrapper (FaultySocket, Kartikey's emulator, ...) plugs in unchanged.

Mechanics
  * per-packet timers in a min-heap (lazy invalidation)            -> O(log W) per event
  * cumulative ACK + echo ACK + 64-bit SACK bitmap (any window size works: echo ACKs
    cover packets beyond the bitmap)
  * optional SACK fast retransmit (dupthresh; None/0 disables)
  * Karn's rule: RTT samples only from never-retransmitted packets
  * rto.on_timeout() called once per timeout EVENT (not per packet) -> no exponential blow-up
  * receiver buffers out-of-order data (up to cfg.max_window), streams in-order to disk
  * SHA-256 of the file travels in FIN; FINACK reports match/mismatch (stats.verified)

Sequence numbers are packet indices (32-bit).
"""
from __future__ import annotations

import hashlib
import heapq
import io
import socket
import struct
import time
import zlib
from dataclasses import dataclass
from typing import NamedTuple, Optional

from config import Config, Stats
from rto import JacobsonKarelsRTO, RTOEstimator

# --------------------------------------------------------------------------
# Packet format
# --------------------------------------------------------------------------
DATA, ACK, FIN, FINACK = 1, 2, 3, 4
_VALID_TYPES = (DATA, ACK, FIN, FINACK)

# type, flags, seq, cum_ack, echo, sack_bitmap, payload_len, crc32
_HDR = struct.Struct("!BBIIIQHI")
HDR_LEN = _HDR.size                      # 28 bytes
SACK_BITS = 64
MAX_PAYLOAD = 65507 - HDR_LEN
_CRC = struct.Struct("!I")


class Packet(NamedTuple):
    type: int
    seq: int
    ack: int
    echo: int
    sack: int
    payload: bytes


def pack(ptype: int, seq: int = 0, ack: int = 0, echo: int = 0,
         sack: int = 0, payload: bytes = b"") -> bytes:
    """Serialise a packet; CRC32 covers header (crc field zeroed) + payload."""
    hdr = _HDR.pack(ptype, 0, seq, ack, echo, sack, len(payload), 0)
    crc = zlib.crc32(payload, zlib.crc32(hdr)) & 0xFFFFFFFF
    return hdr[:-4] + _CRC.pack(crc) + payload


def unpack(data: bytes) -> Optional[Packet]:
    """Parse + verify. Returns None for corrupt/truncated/foreign datagrams."""
    if len(data) < HDR_LEN:
        return None
    ptype, _flags, seq, ack, echo, sack, length, crc = _HDR.unpack_from(data)
    if ptype not in _VALID_TYPES or len(data) != HDR_LEN + length:
        return None
    payload = data[HDR_LEN:]
    calc = zlib.crc32(payload, zlib.crc32(data[:HDR_LEN - 4] + b"\0\0\0\0"))
    if calc & 0xFFFFFFFF != crc:
        return None
    return Packet(ptype, seq, ack, echo, sack, payload)


class TransferError(Exception):
    pass


# --------------------------------------------------------------------------
# Stats: the team's Stats + a few SR-only extras (to_row() stays identical)
# --------------------------------------------------------------------------
@dataclass
class SRStats(Stats):
    fast_retransmits: int = 0     # sender: SACK-triggered early retransmissions
    max_inflight: int = 0         # sender: peak (next_seq - base); never exceeds window
    fin_confirmed: bool = False   # sender: FINACK received
    verified: bool = False        # SHA-256 matched (sender: via FINACK, receiver: local check)
    out_of_window: int = 0        # receiver: dropped, beyond the receive window
    max_buffered: int = 0         # receiver: peak out-of-order buffer occupancy
    acks_sent: int = 0            # receiver

    def extended_row(self) -> dict:
        row = self.to_row()
        row.update(fast_retransmits=self.fast_retransmits, max_inflight=self.max_inflight,
                   verified=self.verified, duplicates=self.duplicates,
                   corrupt=self.corrupt, out_of_order=self.out_of_order,
                   out_of_window=self.out_of_window, max_buffered=self.max_buffered)
        return row


def _tune_socket(sock, bufsize: int = 4 << 20) -> None:
    """Bigger kernel buffers on a real socket; silently skipped for wrappers."""
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        try:
            sock.setsockopt(socket.SOL_SOCKET, opt, bufsize)
        except (AttributeError, OSError):
            pass


def make_udp_socket(bind=None, bufsize: int = 4 << 20) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _tune_socket(s, bufsize)
    if bind is not None:
        s.bind(bind)
    return s


# --------------------------------------------------------------------------
# Sender
# --------------------------------------------------------------------------
class _Slot:
    __slots__ = ("seq", "wire", "length", "sent_at", "deadline", "retries", "acked", "skips")

    def __init__(self, seq: int, wire: bytes, length: int):
        self.seq = seq
        self.wire = wire            # pre-serialised: retransmit costs no re-pack
        self.length = length
        self.sent_at = 0.0
        self.deadline = 0.0
        self.retries = 0
        self.acked = False
        self.skips = 0


_UNSET = object()


class SRSender:
    """controller (optional): on_ack(sender, rtt_sample_or_None) and
    on_loss(sender, kind in {"timeout","fast"}); it may set sender.window_size."""

    def __init__(self, sock, peer, cfg: Optional[Config] = None,
                 rto: Optional[RTOEstimator] = None, controller=None,
                 dupthresh=_UNSET, max_fin_retries: int = 8):
        self.cfg = cfg = cfg or Config()
        if not 0 < cfg.payload_size <= MAX_PAYLOAD:
            raise ValueError("cfg.payload_size out of range")
        self.sock, self.peer, self.mss = sock, peer, cfg.payload_size
        self.rto = rto if rto is not None else JacobsonKarelsRTO(
            cfg.initial_rto, cfg.min_rto, cfg.max_rto)
        self.controller = controller
        self.max_retries = cfg.max_retries
        self.max_fin_retries = max_fin_retries
        # Config has no dupthresh field; add one there to tune it from experiments.
        self.dupthresh = getattr(cfg, "dupthresh", 3) if dupthresh is _UNSET else dupthresh
        _tune_socket(sock)
        self.stats = SRStats()
        self._window = cfg.min_window
        self.window_size = cfg.window_size

    # -- adjustable window (DART hook) ---------------------------------------
    @property
    def window_size(self) -> int:
        return self._window

    @window_size.setter
    def window_size(self, w) -> None:
        w = min(max(int(w), self.cfg.min_window), self.cfg.max_window)
        if w != self._window or not self.stats.window_trace:
            self._window = w
            self._trace()

    window = window_size                     # alias

    def _trace(self) -> None:
        st = self.stats
        t = (time.monotonic() - st.t_start) if st.t_start else 0.0
        st.window_trace.append((t, self._window))

    # -- public API ------------------------------------------------------------
    def send_file(self, path: str) -> SRStats:
        with open(path, "rb") as f:
            return self.send_stream(f)

    def send_bytes(self, data: bytes) -> SRStats:
        return self.send_stream(io.BytesIO(data))

    def _init_state(self) -> None:
        self.stats = SRStats()
        self.stats.start()
        self._trace()                         # window at t = 0
        self._inflight: dict[int, _Slot] = {}
        self._heap: list = []
        self._base = self._nxt = 0
        self._hasher = hashlib.sha256()
        self._finack: Optional[bytes] = None

    def send_stream(self, fobj) -> SRStats:
        self._init_state()
        try:
            self._data_phase(fobj)
            self._fin_phase()
        finally:
            self.stats.stop()
        return self.stats

    # -- internals -------------------------------------------------------------
    def _valid(self, entry) -> bool:
        slot = self._inflight.get(entry[1])
        return slot is not None and not slot.acked and slot.deadline == entry[0]

    def _send(self, wire: bytes) -> None:
        try:
            self.sock.sendto(wire, self.peer)
        except (BlockingIOError, InterruptedError, ConnectionError):
            pass                              # treated as loss; the timer recovers

    def _transmit(self, slot: _Slot, now: float) -> None:
        self._send(slot.wire)
        slot.sent_at = now
        slot.deadline = now + self.rto.current()
        heapq.heappush(self._heap, (slot.deadline, slot.seq))
        self.stats.packets_sent += 1

    def _retransmit(self, slot: _Slot, now: float, kind: str) -> None:
        slot.retries += 1
        if slot.retries > self.max_retries:
            raise TransferError(f"packet {slot.seq}: gave up after {self.max_retries} retransmissions")
        slot.skips = 0
        self.stats.retransmissions += 1
        if kind == "fast":
            self.stats.fast_retransmits += 1
        self._transmit(slot, now)
        if kind == "fast" and self.controller:
            self.controller.on_loss(self, "fast")

    def _wait(self, timeout: float) -> None:
        """Block up to `timeout` for a datagram, then drain what else is queued.
        Only settimeout/recvfrom are used, so any socket wrapper works."""
        sock, st = self.sock, self.stats
        sock.settimeout(max(timeout, 1e-4))   # 0 would mean non-blocking
        while True:
            try:
                data, _addr = sock.recvfrom(2048)
            except (socket.timeout, BlockingIOError, InterruptedError):
                return
            except ConnectionError:           # ICMP unreachable on some OSes
                continue
            sock.settimeout(0)                # drain the rest without blocking
            pkt = unpack(data)
            if pkt is None:
                st.corrupt += 1
            elif pkt.type == ACK:
                self._on_ack(pkt, time.monotonic())
            elif pkt.type == FINACK:
                self._finack = pkt.payload

    def _data_phase(self, fobj) -> None:
        st, inflight, heap, read = self.stats, self._inflight, self._heap, fobj.read
        eof = False
        while True:
            now = time.monotonic()
            while not eof and self._nxt - self._base < self._window:   # fill window
                chunk = read(self.mss)
                if not chunk:
                    eof = True
                    break
                self._hasher.update(chunk)
                slot = _Slot(self._nxt, pack(DATA, seq=self._nxt, payload=chunk), len(chunk))
                inflight[slot.seq] = slot
                self._transmit(slot, now)
                self._nxt += 1
                out = self._nxt - self._base
                if out > st.max_inflight:
                    st.max_inflight = out
            if eof and not inflight:
                return
            while heap and not self._valid(heap[0]):
                heapq.heappop(heap)
            timeout = max(0.0, heap[0][0] - now) if heap else 0.05
            self._wait(timeout)
            self._expire(time.monotonic())

    def _mark(self, slot: _Slot) -> None:
        slot.acked = True
        self.stats.bytes_delivered += slot.length

    def _on_ack(self, pkt: Packet, now: float) -> None:
        st, inflight = self.stats, self._inflight
        top, sample = -1, None

        # 1) echo ACK -- the only source of RTT samples (Karn)
        slot = inflight.get(pkt.echo)
        if slot is not None and not slot.acked:
            self._mark(slot)
            top = slot.seq
            if slot.retries == 0:
                sample = now - slot.sent_at

        # 2) cumulative ACK
        for s in range(self._base, min(pkt.ack, self._nxt)):
            slot = inflight.get(s)
            if slot is not None and not slot.acked:
                self._mark(slot)
                if s > top:
                    top = s

        # 3) SACK bitmap: bit i <=> seq (ack + 1 + i) received
        bm, s = pkt.sack, pkt.ack + 1
        while bm:
            if bm & 1:
                slot = inflight.get(s)
                if slot is not None and not slot.acked:
                    self._mark(slot)
                    if s > top:
                        top = s
            bm >>= 1
            s += 1

        base = self._base                     # slide window
        while True:
            slot = inflight.get(base)
            if slot is None or not slot.acked:
                break
            del inflight[base]
            base += 1
        self._base = base

        if top < 0:
            return                            # duplicate ACK: nothing new
        st.acks_received += 1                 # "valid, new ACKs"
        if sample is not None:
            self.rto.on_rtt_sample(sample)
            st.rtt_samples.append(sample)

        # SACK fast retransmit: a hole skipped by `dupthresh` newer acked packets
        if self.dupthresh and top > base:
            for s in range(base, top):
                slot = inflight.get(s)
                if slot is not None and not slot.acked:
                    slot.skips += 1
                    if slot.skips == self.dupthresh:
                        self._retransmit(slot, now, "fast")

        if self.controller:
            self.controller.on_ack(self, sample)

    def _expire(self, now: float) -> None:
        heap = self._heap
        expired = []
        while heap and heap[0][0] <= now:
            entry = heapq.heappop(heap)
            if self._valid(entry):
                expired.append(self._inflight[entry[1]])
        if not expired:
            return
        self.stats.timeouts += len(expired)   # SR: one timer expiry per retransmission
        self.rto.on_timeout()                 # once per event, not per packet
        for slot in expired:
            self._retransmit(slot, now, "timeout")
        if self.controller:
            self.controller.on_loss(self, "timeout")

    def _fin_phase(self) -> None:
        fin = pack(FIN, seq=self._nxt, payload=self._hasher.digest())
        # All data is already acked, so FIN is best-effort.  Retry often enough that several
        # attempts fit inside the receiver's linger window, and keep the FIN phase out of the
        # data-path RTO estimator (no on_timeout() here).
        interval = max(0.01, min(self.rto.current(), self.cfg.linger / 4))
        for _ in range(self.max_fin_retries + 1):
            self._send(fin)
            end = time.monotonic() + interval
            while self._finack is None:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    break
                self._wait(remaining)
            if self._finack is not None:
                break
        self.stats.fin_confirmed = self._finack is not None
        self.stats.verified = self._finack == b"\x01"


# --------------------------------------------------------------------------
# Receiver
# --------------------------------------------------------------------------
class SRReceiver:
    def __init__(self, sock, cfg: Optional[Config] = None):
        self.cfg = cfg = cfg or Config()
        self.sock = sock
        self.window = cfg.max_window          # buffers up to max_window packets
        self.idle_timeout = cfg.idle_timeout
        self.linger = cfg.linger              # keep re-ACKing a retransmitted FIN
        _tune_socket(sock)
        self._reset(None)

    def receive_file(self, path: str) -> SRStats:
        with open(path, "wb") as f:
            return self.receive_stream(f)

    def _reset(self, fobj) -> None:
        self._out = fobj
        self.base = 0
        self.buf: dict[int, bytes] = {}
        self.hasher = hashlib.sha256()
        self.stats = SRStats()
        self.peer = None

    def receive_stream(self, fobj) -> SRStats:
        self._reset(fobj)
        sock, st = self.sock, self.stats
        fin_done_at = None
        try:
            while True:
                if fin_done_at is None:
                    sock.settimeout(self.idle_timeout)
                else:
                    remaining = fin_done_at - time.monotonic()
                    if remaining <= 0:
                        break
                    sock.settimeout(remaining)
                try:
                    data, addr = sock.recvfrom(65535)
                except socket.timeout:
                    if fin_done_at is not None:
                        break
                    raise TransferError("receiver idle timeout")
                except ConnectionError:
                    continue
                pkt = unpack(data)
                if pkt is None:
                    st.corrupt += 1
                    continue
                if self.peer is None:
                    self.peer = addr
                    st.start()                # clock starts at the first valid packet
                elif addr != self.peer:
                    continue
                if pkt.type == DATA:
                    self._on_data(pkt)
                elif pkt.type == FIN:
                    if self._on_fin(pkt):
                        if fin_done_at is None:
                            st.stop()         # completion time excludes the linger
                        # every FIN (re)transmission extends the linger, so a sender that is
                        # still retrying after a lost FINACK always finds us listening
                        fin_done_at = time.monotonic() + self.linger
        finally:
            if not st.t_start:
                st.start()
            if not st.t_end:
                st.stop()
            if fobj is not None:
                fobj.flush()
        return st

    def _deliver(self, payload: bytes) -> None:
        self._out.write(payload)
        self.hasher.update(payload)
        self.stats.bytes_delivered += len(payload)

    def _on_data(self, pkt: Packet) -> None:
        seq, st, buf = pkt.seq, self.stats, self.buf
        if seq < self.base or seq in buf:
            st.duplicates += 1
        elif seq >= self.base + self.window:
            st.out_of_window += 1
        elif seq == self.base:
            self._deliver(pkt.payload)
            self.base += 1
            while self.base in buf:
                self._deliver(buf.pop(self.base))
                self.base += 1
        else:
            buf[seq] = pkt.payload
            st.out_of_order += 1
            if len(buf) > st.max_buffered:
                st.max_buffered = len(buf)
        self._send_ack(seq)

    def _send_ack(self, echo: int) -> None:
        base, buf, bm = self.base, self.buf, 0
        if len(buf) < SACK_BITS:              # cheaper of the two scans
            for s in buf:
                d = s - base - 1
                if 0 <= d < SACK_BITS:
                    bm |= 1 << d
        else:
            for d in range(SACK_BITS):
                if base + 1 + d in buf:
                    bm |= 1 << d
        try:
            self.sock.sendto(pack(ACK, ack=base, echo=echo, sack=bm), self.peer)
            self.stats.acks_sent += 1
        except (BlockingIOError, InterruptedError, ConnectionError):
            pass

    def _on_fin(self, pkt: Packet) -> bool:
        if pkt.seq != self.base:              # data still missing
            self._send_ack(pkt.seq)
            return False
        ok = pkt.payload == self.hasher.digest()
        self.stats.verified = ok
        try:
            self.sock.sendto(
                pack(FINACK, seq=pkt.seq, payload=b"\x01" if ok else b"\x00"), self.peer)
        except (BlockingIOError, InterruptedError, ConnectionError):
            pass
        return True
