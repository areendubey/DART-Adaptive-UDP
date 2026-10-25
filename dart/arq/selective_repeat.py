"""
DART Selective Repeat (SR) ARQ Protocol Implementation
Integration & Architecture: Savvy Jatrana & Areen Dubey
Catalogue Foundation: P3 Reliable Data Transfer over UDP

Features:
  - Individual per-packet retransmission timers (Selective Repeat)
  - Selective Acknowledgements (ACKing individual packets)
  - Receiver out-of-order buffering and in-order stream assembly
  - Jacobson/Karels dynamic RTO estimation with Karn's rule
  - Pluggable DART Adaptive Engine for dynamic window adaptation
  - Cryptographic SHA-256 source/received file integrity verification
"""

import socket
import time
import os
import json
import hashlib
from typing import Optional, Callable, Dict, Any, List, Set

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


class SelectiveRepeatSender(BaseARQSender):
    """
    Selective Repeat ARQ Sender with Individual Packet Timers and Adaptive Windowing.
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
        self.sock.settimeout(0.01)

    def send_file(
        self,
        file_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> TransferStats:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")

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

        chunks: List[bytes] = []
        offset = 0
        while offset < len(data):
            chunks.append(data[offset : offset + self.packet_size])
            offset += self.packet_size

        if not chunks:
            chunks.append(b"")

        total_chunks = len(chunks)
        sha256_hash = hashlib.sha256(data).hexdigest()

        # Phase 1: Connection Handshake
        if not self._handshake_syn(filename, len(data), total_chunks, sha256_hash):
            raise ConnectionError("SR handshake failed: Receiver did not respond with SYN-ACK")

        # Phase 2: Selective Repeat Data Transmission
        send_base = 0
        next_seq = 0
        acked_packets: Set[int] = set()
        send_times: Dict[int, float] = {}
        timer_starts: Dict[int, float] = {}
        retransmission_counts: Dict[int, int] = {}
        retransmitted_seqs: Set[int] = set()

        while send_base < total_chunks:
            current_window = self.adaptive_engine.get_transmission_window()
            pacing_ms = self.adaptive_engine.get_pacing_interval_ms()

            # Transmit while within window
            while next_seq < total_chunks and (next_seq - send_base) < current_window:
                pkt = DARTPacket.create_data(
                    seq_num=next_seq,
                    data=chunks[next_seq],
                    window_size=current_window
                )
                self.sock.sendto(pkt.pack(), (self.host, self.port))
                now = time.perf_counter()
                send_times[next_seq] = now
                timer_starts[next_seq] = now
                retransmission_counts[next_seq] = 0
                self.stats.record_packet_sent(len(chunks[next_seq]))

                next_seq += 1
                if pacing_ms > 0:
                    time.sleep(pacing_ms / 1000.0)

            # Receive ACKs (Non-blocking with short timeout)
            try:
                raw_ack, _ = self.sock.recvfrom(2048)
                ack_pkt = DARTPacket.unpack(raw_ack)

                if ack_pkt.is_ack:
                    acked_seq = ack_pkt.ack_num
                    now = time.perf_counter()

                    if acked_seq not in acked_packets and acked_seq < total_chunks:
                        acked_packets.add(acked_seq)
                        self.stats.record_ack_received()

                        # Stop individual packet timer
                        if acked_seq in timer_starts:
                            del timer_starts[acked_seq]

                        # Sample RTT only if packet was never retransmitted (Karn's algorithm)
                        if acked_seq in send_times and acked_seq not in retransmitted_seqs:
                            sample_rtt = now - send_times[acked_seq]
                            self.rto_estimator.on_rtt_sample(sample_rtt)
                            self.adaptive_engine.on_rtt_sample(sample_rtt)
                            self.stats.record_rtt_sample(sample_rtt)

                        # Slide window base forward past contiguous acknowledged packets
                        while send_base in acked_packets:
                            send_base += 1

                        if progress_callback:
                            progress_callback(min(send_base * self.packet_size, len(data)), len(data))

            except (socket.timeout, ValueError):
                pass

            # Per-Packet Timer Expiry Checks
            now = time.perf_counter()
            current_rto = self.rto_estimator.current_rto

            for seq in range(send_base, min(next_seq, send_base + current_window)):
                if seq not in acked_packets and seq in timer_starts:
                    if (now - timer_starts[seq]) > current_rto:
                        # Selective Timeout: Retransmit ONLY this specific timed-out packet!
                        self.stats.record_timeout()
                        self.rto_estimator.on_timeout()
                        self.adaptive_engine.on_loss_event()

                        retransmission_counts[seq] = retransmission_counts.get(seq, 0) + 1
                        if retransmission_counts[seq] > self.max_retransmissions:
                            raise ConnectionResetError(f"SR transfer aborted: packet {seq} exceeded {self.max_retransmissions} retries")

                        re_pkt = DARTPacket.create_data(
                            seq_num=seq,
                            data=chunks[seq],
                            window_size=current_window
                        )
                        self.sock.sendto(re_pkt.pack(), (self.host, self.port))
                        timer_starts[seq] = now
                        retransmitted_seqs.add(seq)
                        self.stats.record_retransmission()

        # Phase 3: Teardown
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


class SelectiveRepeatReceiver(BaseARQReceiver):
    """
    Selective Repeat ARQ Receiver with Out-of-Order Buffering and In-Order Delivery.
    """

    def __init__(self, listen_host: str = "0.0.0.0", listen_port: int = 9000, window_size: int = 16):
        super().__init__(listen_host, listen_port)
        self.window_size = window_size
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

        rcv_base = 0
        received_buffer: Dict[int, bytes] = {}
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
                        window_size=self.window_size,
                        checksum=0,
                        payload=b""
                    )
                    self.sock.sendto(syn_ack.pack(), sender_addr)
                    continue

                # Handle DATA
                if pkt.is_data:
                    seq = pkt.seq_num

                    # Send Selective ACK for the received packet
                    ack_pkt = DARTPacket.create_ack(ack_num=seq)
                    self.sock.sendto(ack_pkt.pack(), sender_addr)
                    self.stats.record_ack_received()

                    # Accept within receive window [rcv_base, rcv_base + window_size - 1]
                    if rcv_base <= seq < (rcv_base + self.window_size * 2):
                        if seq not in received_buffer:
                            received_buffer[seq] = pkt.payload
                            self.stats.record_packet_sent(len(pkt.payload))

                        # Advance rcv_base past consecutively received chunks
                        while rcv_base in received_buffer:
                            rcv_base += 1
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
                    for _ in range(3):
                        self.sock.sendto(fin_ack.pack(), sender_addr)
                    break

        finally:
            self.stats.finish()
            try:
                self.sock.close()
            except Exception:
                pass

        # Reassemble payload in strict order
        assembled = bytearray()
        for seq in sorted(received_buffer.keys()):
            assembled.extend(received_buffer[seq])

        actual_sha256 = hashlib.sha256(assembled).hexdigest()
        if expected_sha256 and expected_sha256 != actual_sha256:
            raise ValueError(f"Selective Repeat integrity check failed: expected {expected_sha256}, got {actual_sha256}")

        return bytes(assembled)
