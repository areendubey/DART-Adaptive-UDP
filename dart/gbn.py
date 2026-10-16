"""Go-Back-N reliable transfer over UDP.

Anushka Roy (24051912) — Week 03 ownership:
    Go-Back-N, cumulative ACK / retransmission handling,
    reliability testing and protocol correctness validation.

Classic GBN:
    Sender keeps packets in [base, nextseqnum) outstanding (window N).
    A single retransmission timer covers the oldest unacked packet.
    Timeout retransmits every outstanding packet from base.
    Receiver accepts only the in-order next sequence number and
    returns a cumulative ACK of the last correctly received seq.
    Out-of-order, duplicate, and corrupt packets are not delivered.
"""

from __future__ import annotations

import hashlib
import logging
import socket
import time
from dataclasses import dataclass
from typing import BinaryIO

from dart.constants import (
    DEFAULT_MSS,
    DEFAULT_TIMEOUT_S,
    DEFAULT_WINDOW_SIZE,
    SEQ_MODULUS,
    SOCKET_BUFFER,
)
from dart.packet import Packet, PacketError, PacketType

log = logging.getLogger("dart.gbn")


def seq_lt(a: int, b: int, modulus: int = SEQ_MODULUS) -> bool:
    """True if a is strictly before b in serial-number space (RFC 1982-style)."""
    return ((b - a) & (modulus - 1)) < (modulus // 2) and a != b


def seq_le(a: int, b: int, modulus: int = SEQ_MODULUS) -> bool:
    return a == b or seq_lt(a, b, modulus)


def seq_add(a: int, n: int = 1, modulus: int = SEQ_MODULUS) -> int:
    return (a + n) % modulus


@dataclass
class GBNStats:
    bytes_payload: int = 0
    data_packets_sent: int = 0
    packets_retransmitted: int = 0
    acks_received: int = 0
    timeouts: int = 0
    duplicate_acks: int = 0
    duration_s: float = 0.0
    sha256: str = ""
    window_size: int = 0

    def as_dict(self) -> dict:
        return {
            "bytes_payload": self.bytes_payload,
            "data_packets_sent": self.data_packets_sent,
            "packets_retransmitted": self.packets_retransmitted,
            "acks_received": self.acks_received,
            "timeouts": self.timeouts,
            "duplicate_acks": self.duplicate_acks,
            "duration_s": round(self.duration_s, 6),
            "sha256": self.sha256,
            "window_size": self.window_size,
        }


class GoBackNSender:
    """Pipelined GBN sender with cumulative ACK and Go-Back-N retransmission."""

    def __init__(
        self,
        sock: socket.socket,
        dest: tuple[str, int],
        window_size: int = DEFAULT_WINDOW_SIZE,
        timeout: float = DEFAULT_TIMEOUT_S,
        mss: int = DEFAULT_MSS,
    ) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        if mss < 1:
            raise ValueError("mss must be >= 1")
        self._sock = sock
        self._dest = dest
        self.window_size = window_size
        self.timeout = timeout
        self.mss = mss
        self.stats = GBNStats(window_size=window_size)
        self._base = 0
        self._nextseq = 0
        self._buffer: dict[int, bytes] = {}

    def send_bytes(self, data: bytes) -> GBNStats:
        started = time.perf_counter()
        hasher = hashlib.sha256()
        hasher.update(data)
        offset = 0
        while offset < len(data):
            chunk = data[offset : offset + self.mss]
            self._wait_for_window_slot()
            self._send_data(self._nextseq, chunk)
            self._nextseq = seq_add(self._nextseq)
            offset += len(chunk)
            self.stats.bytes_payload += len(chunk)
        self._wait_for_window_slot()
        self._send_control(self._nextseq, PacketType.FIN)
        self._nextseq = seq_add(self._nextseq)
        self._drain_until(self._nextseq)
        self.stats.duration_s = time.perf_counter() - started
        self.stats.sha256 = hasher.hexdigest()
        return self.stats

    def send_file(self, path: str) -> GBNStats:
        with open(path, "rb") as handle:
            return self.send_bytes(handle.read())

    def _outstanding(self) -> int:
        return (self._nextseq - self._base) % SEQ_MODULUS

    def _wait_for_window_slot(self) -> None:
        while self._outstanding() >= self.window_size:
            self._recv_ack_or_retransmit()

    def _drain_until(self, target_base: int) -> None:
        while seq_lt(self._base, target_base):
            self._recv_ack_or_retransmit()

    def _send_data(self, seq: int, payload: bytes) -> None:
        packet = Packet(ptype=PacketType.DATA, seq=seq, ack=0, payload=payload)
        encoded = packet.encode()
        self._buffer[seq] = encoded
        self._transmit(encoded, retransmit=False)
        self.stats.data_packets_sent += 1
        log.debug("DATA seq=%s len=%s base=%s next=%s", seq, len(payload), self._base, self._nextseq)

    def _send_control(self, seq: int, ptype: PacketType) -> None:
        packet = Packet(ptype=ptype, seq=seq, ack=0, payload=b"")
        encoded = packet.encode()
        self._buffer[seq] = encoded
        self._transmit(encoded, retransmit=False)
        log.debug("%s seq=%s", ptype.name, seq)

    def _transmit(self, encoded: bytes, retransmit: bool) -> None:
        self._sock.sendto(encoded, self._dest)
        if retransmit:
            self.stats.packets_retransmitted += 1

    def _retransmit_window(self) -> None:
        self.stats.timeouts += 1
        seq = self._base
        while seq_lt(seq, self._nextseq):
            encoded = self._buffer.get(seq)
            if encoded is None:
                break
            self._transmit(encoded, retransmit=True)
            log.info("retransmit seq=%s (timeout #%s)", seq, self.stats.timeouts)
            seq = seq_add(seq)

    def _recv_ack_or_retransmit(self) -> None:
        self._sock.settimeout(self.timeout)
        try:
            raw, _addr = self._sock.recvfrom(SOCKET_BUFFER)
        except socket.timeout:
            self._retransmit_window()
            return
        try:
            pkt = Packet.decode(raw)
        except PacketError as exc:
            log.debug("ignore bad ACK datagram: %s", exc)
            return
        if not pkt.is_ack():
            return
        self._handle_cumulative_ack(pkt.ack)

    def _handle_cumulative_ack(self, ack: int) -> None:
        """ACK n means every packet with seq <= n has been received in order."""
        self.stats.acks_received += 1
        if self._nextseq == self._base:
            return
        last_sent = seq_add(self._nextseq, -1)
        in_flight = seq_le(self._base, ack) and seq_le(ack, last_sent)
        if not in_flight:
            if seq_lt(ack, self._base):
                self.stats.duplicate_acks += 1
                log.debug("duplicate/old ACK %s (base=%s)", ack, self._base)
            else:
                log.debug("ignore ACK %s beyond last sent %s", ack, last_sent)
            return
        new_base = seq_add(ack)
        if new_base == self._base:
            self.stats.duplicate_acks += 1
            log.debug("duplicate ACK %s", ack)
            return
        released = 0
        seq = self._base
        while seq_lt(seq, new_base):
            self._buffer.pop(seq, None)
            seq = seq_add(seq)
            released += 1
        self._base = new_base
        log.debug("cumulative ACK %s -> base=%s released=%s", ack, self._base, released)


class GoBackNReceiver:
    """In-order GBN receiver with cumulative acknowledgements."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._expected = 0
        self._last_ack_seq: Optional[int] = None
        self._peer: Optional[tuple[str, int]] = None
        self.bytes_received = 0
        self.packets_accepted = 0
        self.packets_discarded = 0
        self.acks_sent = 0
        self.sha256 = ""

    def receive_bytes(self, idle_timeout: float = 5.0) -> bytes:
        chunks: list[bytes] = []
        hasher = hashlib.sha256()
        self._sock.settimeout(idle_timeout)
        while True:
            try:
                raw, addr = self._sock.recvfrom(SOCKET_BUFFER)
            except socket.timeout as exc:
                raise TimeoutError("receiver idle timeout before FIN") from exc
            self._peer = addr
            try:
                pkt = Packet.decode(raw)
            except PacketError:
                self.packets_discarded += 1
                self._reack()
                continue
            if pkt.ptype not in (PacketType.DATA, PacketType.FIN):
                self.packets_discarded += 1
                continue
            if pkt.seq != self._expected:
                self.packets_discarded += 1
                log.debug("discard seq=%s expected=%s", pkt.seq, self._expected)
                self._reack()
                continue
            if pkt.is_data():
                chunks.append(pkt.payload)
                hasher.update(pkt.payload)
                self.bytes_received += len(pkt.payload)
                self.packets_accepted += 1
                self._ack(pkt.seq)
                self._expected = seq_add(self._expected)
                continue
            # In-order FIN: acknowledge it and close the transfer.
            self.packets_accepted += 1
            self._ack(pkt.seq)
            self._expected = seq_add(self._expected)
            self.sha256 = hasher.hexdigest()
            return b"".join(chunks)

    def receive_file(self, path: str, idle_timeout: float = 5.0) -> bytes:
        data = self.receive_bytes(idle_timeout=idle_timeout)
        with open(path, "wb") as handle:
            handle.write(data)
        return data

    def receive_to(self, handle: BinaryIO, idle_timeout: float = 5.0) -> bytes:
        data = self.receive_bytes(idle_timeout=idle_timeout)
        handle.write(data)
        return data

    def _ack(self, seq: int) -> None:
        if self._peer is None:
            return
        encoded = Packet(ptype=PacketType.ACK, seq=0, ack=seq).encode()
        self._sock.sendto(encoded, self._peer)
        self._last_ack_seq = seq
        self.acks_sent += 1
        log.debug("ACK %s", seq)

    def _reack(self) -> None:
        if self._last_ack_seq is None or self._peer is None:
            return
        encoded = Packet(ptype=PacketType.ACK, seq=0, ack=self._last_ack_seq).encode()
        self._sock.sendto(encoded, self._peer)
        self.acks_sent += 1
        log.debug("re-ACK %s", self._last_ack_seq)


def _cli() -> None:
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="DART Go-Back-N file transfer")
    parser.add_argument("--role", choices=("sender", "receiver"), required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--file", dest="path", default="")
    parser.add_argument("--out", default="received.bin")
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW_SIZE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--mss", type=int, default=DEFAULT_MSS)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        if args.role == "receiver":
            sock.bind((args.host, args.port))
            print(f"GBN receiver listening on {sock.getsockname()}")
            rx = GoBackNReceiver(sock)
            data = rx.receive_file(args.out, idle_timeout=120.0)
            print(f"wrote {len(data)} bytes to {args.out} sha256={rx.sha256}")
        else:
            if not args.path:
                raise SystemExit("--file is required for sender")
            payload = Path(args.path).read_bytes()
            sock.bind(("0.0.0.0", 0))
            tx = GoBackNSender(
                sock,
                (args.host, args.port),
                window_size=args.window,
                timeout=args.timeout,
                mss=args.mss,
            )
            stats = tx.send_bytes(payload)
            print(stats.as_dict())
    finally:
        sock.close()


if __name__ == "__main__":
    _cli()
