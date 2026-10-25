"""
DART Go-Back-N (GBN) ARQ Protocol Implementation
Integration & Architecture: Anushka Roy & Areen Dubey
Catalogue Foundation: P3 Reliable Data Transfer over UDP

Features:
  - Sliding send window [base, base + N - 1] with cumulative ACKs
  - Single retransmission timer covering the oldest unacknowledged packet
  - Dynamic Jacobson/Karels RTT estimation & Karn's algorithm
  - Pluggable DART Adaptive Engine for bounded dynamic window scaling
  - End-to-end SHA-256 file integrity verification
"""

import socket
import time
import os
import json
import hashlib
from typing import Optional, Callable, Dict, Any, List

from dart.packet import (
    DARTPacket,
    MAX_PAYLOAD_SIZE,
    FLAG_SYN,
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN
)
from dart.timer import AdaptiveRTOEstimator
from dart.stats import TransferStats
from dart.arq.base import BaseARQSender, BaseARQReceiver
from dart.adaptive.engine import BaseAdaptiveEngine, FixedBaselineEngine


class GoBackNSender(BaseARQSender):
    """
    Go-Back-N ARQ Sender with Pipelining, Cumulative ACKs, and Adaptive Hooks.
    """

    def __init__(
        self,
        host: str,
        port: int,
        window_size: int = 5,
        packet_size: int = MAX_PAYLOAD_SIZE,
        initial_rto: float = 0.5,
        min_rto: float = 0.05,
        max_rto: float = 5.0,
        max_retransmissions: int = 25,
        adaptive_engine: Optional[BaseAdaptiveEngine] = None
    ):
        super().__init__(host, port, packet_size)
        self.initial_window = max(1, window_size)
        self.adaptive_engine = adaptive_engine or FixedBaselineEngine(fixed_window=self.initial_window)
        self.max_retransmissions = max_retransmissions
        self.rto_estimator = AdaptiveRTOEstimator(
            initial_rto=initial_rto,
            min_rto=min_rto,
            max_rto=max_rto
        )
        self.sock: Optional[socket.socket] = None

    def _init_socket(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.01)  # Non-blocking tick for event loop

    def send_file(
        self,
        file_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> TransferStats:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")

        file_size = os.path.getsize(file_path)
        with open(file_path, "rb") as f:
            data = f.read()

        return self.send_bytes(
            data=data,
            progress_callback=progress_callback,
            filename=os.path.basename(file_path)
        )

    def send_bytes(
        self,
        data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        filename: str = "payload.bin"
    ) -> TransferStats:
        self._init_socket()
        self.stats = TransferStats()
        self.stats.start()

        # Chunk payload into packet-sized fragments
        chunks: List[bytes] = []
        offset = 0
        while offset < len(data):
            chunks.append(data[offset : offset + self.packet_size])
            offset += self.packet_size

        if not chunks:
            chunks.append(b"")

        total_chunks = len(chunks)
        sha256_hash = hashlib.sha256(data).hexdigest()

        # Phase 1: Connection Handshake (SYN -> SYN-ACK)
        if not self._handshake_syn(filename, len(data), total_chunks, sha256_hash):
            raise ConnectionError("GBN handshake failed: Receiver did not respond with SYN-ACK")

        # Phase 2: Sliding Window Data Transfer
        base = 0
        next_seq = 0
        send_times: Dict[int, float] = {}
        retransmitted_seqs: set = set()
        timer_start: Optional[float] = None
        consecutive_timeouts = 0

        while base < total_chunks:
            current_window = self.adaptive_engine.get_transmission_window()
            pacing_ms = self.adaptive_engine.get_pacing_interval_ms()

            # Transmit while within window
            while next_seq < total_chunks and (next_seq - base) < current_window:
                pkt = DARTPacket.create_data(
                    seq_num=next_seq,
                    data=chunks[next_seq],
                    window_size=current_window
                )
                self.sock.sendto(pkt.pack(), (self.host, self.port))
                now = time.perf_counter()
                send_times[next_seq] = now
                self.stats.record_packet_sent(len(chunks[next_seq]))

                if base == next_seq:
                    timer_start = now

                next_seq += 1
                if pacing_ms > 0:
                    time.sleep(pacing_ms / 1000.0)

            # Receive ACKs
            current_rto = self.rto_estimator.current_rto
            try:
                raw_ack, _ = self.sock.recvfrom(2048)
                ack_pkt = DARTPacket.unpack(raw_ack)

                if ack_pkt.is_ack and ack_pkt.ack_num >= base:
                    # Cumulative ACK received
                    acked_seq = ack_pkt.ack_num
                    now = time.perf_counter()

                    # RTT sample for oldest non-retransmitted packet acknowledged
                    if acked_seq in send_times and acked_seq not in retransmitted_seqs:
                        sample_rtt = now - send_times[acked_seq]
                        self.rto_estimator.on_rtt_sample(sample_rtt)
                        self.adaptive_engine.on_rtt_sample(sample_rtt)
                        self.stats.record_rtt_sample(sample_rtt)

                    self.stats.record_ack_received()
                    consecutive_timeouts = 0
                    base = acked_seq + 1

                    if progress_callback:
                        progress_callback(min(base * self.packet_size, len(data)), len(data))

                    if base == next_seq:
                        timer_start = None  # No outstanding packets
                    else:
                        timer_start = now  # Reset timer for remaining packets

            except socket.timeout:
                pass
            except ValueError:
                pass  # Corrupt ACK packet dropped

            # Timer check
            if timer_start is not None and (time.perf_counter() - timer_start) > self.rto_estimator.current_rto:
                # Timeout occurred! Retransmit window from base to next_seq - 1
                self.stats.record_timeout()
                self.rto_estimator.on_timeout()
                self.adaptive_engine.on_loss_event()
                consecutive_timeouts += 1

                if consecutive_timeouts > self.max_retransmissions:
                    raise ConnectionResetError(f"GBN transfer aborted: exceeded {self.max_retransmissions} timeouts")

                # Retransmit all unacked packets from base
                now = time.perf_counter()
                timer_start = now
                for seq in range(base, next_seq):
                    re_pkt = DARTPacket.create_data(
                        seq_num=seq,
                        data=chunks[seq],
                        window_size=current_window
                    )
                    self.sock.sendto(re_pkt.pack(), (self.host, self.port))
                    send_times[seq] = now
                    retransmitted_seqs.add(seq)
                    self.stats.record_retransmission()

        # Phase 3: Teardown (FIN -> FIN-ACK)
        self._teardown_fin(total_chunks, sha256_hash)
        self.stats.finish()

        try:
            self.sock.close()
        except Exception:
            pass

        return self.stats

    def _handshake_syn(self, filename: str, size: int, chunks: int, sha256_hash: str) -> bool:
        meta = {"filename": filename, "size": size, "chunks": chunks, "sha256": sha256_hash}
        syn_pkt = DARTPacket.create_syn(seq_num=0, metadata=meta)
        packed = syn_pkt.pack()

        for _ in range(15):
            self.sock.sendto(packed, (self.host, self.port))
            try:
                self.sock.settimeout(0.3)
                raw, _ = self.sock.recvfrom(2048)
                resp = DARTPacket.unpack(raw)
                if resp.is_syn and resp.is_ack:
                    self.sock.settimeout(0.01)
                    return True
            except (socket.timeout, ValueError):
                continue
        return False

    def _teardown_fin(self, total_chunks: int, sha256_hash: str):
        fin_pkt = DARTPacket.create_fin(seq_num=total_chunks, file_hash=sha256_hash)
        packed = fin_pkt.pack()

        for _ in range(10):
            self.sock.sendto(packed, (self.host, self.port))
            try:
                self.sock.settimeout(0.2)
                raw, _ = self.sock.recvfrom(2048)
                resp = DARTPacket.unpack(raw)
                if resp.is_fin and resp.is_ack:
                    break
            except (socket.timeout, ValueError):
                continue


class GoBackNReceiver(BaseARQReceiver):
    """
    Go-Back-N ARQ Receiver with strict in-order delivery and cumulative ACKs.
    """

    def __init__(self, listen_host: str = "0.0.0.0", listen_port: int = 9000):
        super().__init__(listen_host, listen_port)
        self.sock: Optional[socket.socket] = None

    def receive_file(
        self,
        output_path: str,
        timeout_seconds: Optional[float] = 30.0
    ) -> TransferStats:
        data = self.receive_bytes(timeout_seconds=timeout_seconds)
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(data)
        return self.stats

    def receive_bytes(self, timeout_seconds: Optional[float] = 30.0) -> bytes:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))
        self.sock.settimeout(timeout_seconds)
        self.stats = TransferStats()
        self.stats.start()

        expected_seq = 0
        received_chunks: Dict[int, bytes] = {}
        sender_addr = None
        expected_sha256 = ""
        total_chunks_expected: Optional[int] = None

        try:
            while True:
                try:
                    raw_data, addr = self.sock.recvfrom(4096)
                    sender_addr = addr
                    pkt = DARTPacket.unpack(raw_data)
                except (socket.timeout, ValueError):
                    continue

                # Handle SYN
                if pkt.is_syn:
                    try:
                        meta = json.loads(pkt.payload.decode("utf-8"))
                        expected_sha256 = meta.get("sha256", "")
                        total_chunks_expected = meta.get("chunks", None)
                    except Exception:
                        pass
                    syn_ack = DARTPacket(
                        magic=0xDA87,
                        version=1,
                        flags=FLAG_SYN | FLAG_ACK,
                        seq_num=0,
                        ack_num=0,
                        window_size=pkt.window_size,
                        checksum=0,
                        payload=b""
                    )
                    self.sock.sendto(syn_ack.pack(), sender_addr)
                    continue

                # Handle DATA
                if pkt.is_data:
                    if pkt.seq_num == expected_seq:
                        # In-order packet accepted!
                        received_chunks[expected_seq] = pkt.payload
                        self.stats.record_packet_sent(len(pkt.payload))
                        expected_seq += 1

                    # Cumulative ACK for highest consecutively received packet
                    ack_num = expected_seq - 1
                    if ack_num >= 0:
                        ack_pkt = DARTPacket.create_ack(ack_num=ack_num)
                        self.sock.sendto(ack_pkt.pack(), sender_addr)
                        self.stats.record_ack_received()
                    continue

                # Handle FIN
                if pkt.is_fin:
                    fin_ack = DARTPacket(
                        magic=0xDA87,
                        version=1,
                        flags=FLAG_FIN | FLAG_ACK,
                        seq_num=pkt.seq_num,
                        ack_num=pkt.seq_num,
                        window_size=1,
                        checksum=0,
                        payload=b""
                    )
                    # Transmit multiple FIN-ACKs to ensure delivery
                    for _ in range(3):
                        self.sock.sendto(fin_ack.pack(), sender_addr)
                    break

        finally:
            self.stats.finish()
            try:
                self.sock.close()
            except Exception:
                pass

        # Reassemble payload
        assembled = bytearray()
        for seq in sorted(received_chunks.keys()):
            assembled.extend(received_chunks[seq])

        actual_sha256 = hashlib.sha256(assembled).hexdigest()
        if expected_sha256 and expected_sha256 != actual_sha256:
            raise ValueError(f"GBN integrity check failed: expected {expected_sha256}, got {actual_sha256}")

        return bytes(assembled)
