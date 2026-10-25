"""
DART Stop-and-Wait ARQ Protocol Implementation
Author: Kartikey Singh (Primary Owner)
Course: CS-30003 CSE 34

Implements strict Stop-and-Wait Reliable Data Transfer over UDP:
- Chunking, sequence numbering, and RFC 1071 checksum verification.
- Jacobson/Karels dynamic RTT estimation and Karn's algorithm.
- Cryptographic SHA-256 source/received file integrity verification.
- Comprehensive metric collection and structured event logging.
"""

import socket
import time
import os
import json
import hashlib
from typing import Optional, Callable, Dict, Any

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


class StopAndWaitSender(BaseARQSender):
    """
    Stop-and-Wait ARQ Sender.
    Transmits one packet at a time and waits for its individual ACK before advancing.
    """

    def __init__(
        self,
        host: str,
        port: int,
        packet_size: int = MAX_PAYLOAD_SIZE,
        initial_rto: float = 0.5,
        min_rto: float = 0.1,
        max_rto: float = 5.0,
        max_retransmissions: int = 25
    ):
        super().__init__(host, port, packet_size)
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
        self.sock.settimeout(self.rto_estimator.rto)

    def send_file(
        self,
        file_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> TransferStats:
        """Transfers a local file reliably to the target receiver."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        hasher = hashlib.sha256()

        # Read and compute SHA-256
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        source_sha256 = hasher.hexdigest()

        with open(file_path, "rb") as f:
            raw_data = f.read()

        return self._send_data_internal(
            data=raw_data,
            filename=os.path.basename(file_path),
            source_sha256=source_sha256,
            progress_callback=progress_callback
        )

    def send_bytes(
        self,
        data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> TransferStats:
        """Transfers raw bytes reliably to the receiver."""
        hasher = hashlib.sha256()
        hasher.update(data)
        source_sha256 = hasher.hexdigest()

        return self._send_data_internal(
            data=data,
            filename="data.bin",
            source_sha256=source_sha256,
            progress_callback=progress_callback
        )

    def _send_data_internal(
        self,
        data: bytes,
        filename: str,
        source_sha256: str,
        progress_callback: Optional[Callable[[int, int], None]]
    ) -> TransferStats:
        self.stats = TransferStats()
        self.stats.mark_start()
        self.stats.total_data_bytes = len(data)
        self.stats.source_sha256 = source_sha256

        self._init_socket()
        dest_addr = (self.host, self.port)

        # Split data into MSS chunks
        chunks = [data[i:i + self.packet_size] for i in range(0, len(data), self.packet_size)]
        total_chunks = len(chunks)
        if total_chunks == 0:
            chunks = [b""]
            total_chunks = 1

        metadata = {
            "filename": filename,
            "filesize": len(data),
            "total_chunks": total_chunks,
            "sha256": source_sha256
        }

        try:
            # 1. Connection Handshake (SYN -> SYN-ACK)
            self._perform_handshake(metadata, dest_addr)

            # 2. Data Transmission (Strict Stop-and-Wait ARQ)
            for seq_num, chunk in enumerate(chunks):
                self._send_chunk_stop_and_wait(seq_num, chunk, dest_addr)
                if progress_callback:
                    progress_callback(seq_num + 1, total_chunks)

            # 3. Teardown Phase (FIN -> FIN-ACK)
            self._perform_teardown(total_chunks, dest_addr)

        finally:
            self.stats.mark_end()
            if self.sock:
                self.sock.close()
                self.sock = None

        return self.stats

    def _perform_handshake(self, metadata: Dict[str, Any], dest_addr: tuple):
        """Sends SYN and waits for SYN-ACK with exponential backoff on timeout."""
        syn_pkt = DARTPacket.create_syn(seq_num=0, metadata=metadata)
        syn_bytes = syn_pkt.pack()

        retransmits = 0
        while retransmits < self.max_retransmissions:
            self.sock.settimeout(self.rto_estimator.rto)
            t_send = time.time()
            self.sock.sendto(syn_bytes, dest_addr)
            self.stats.total_packets_sent += 1
            self.stats.add_event("SEND", seq=0, size=len(syn_bytes), rto=self.rto_estimator.rto, info="SYN")

            try:
                raw_ack, _ = self.sock.recvfrom(2048)
                ack_pkt = DARTPacket.unpack(raw_ack)
                if ack_pkt.is_ack and ack_pkt.is_syn:
                    rtt = time.time() - t_send
                    self.rto_estimator.update_rtt(rtt, is_retransmission=(retransmits > 0))
                    self.stats.rtt_samples.append(rtt)
                    self.stats.total_acks_received += 1
                    self.stats.add_event("ACK", ack=0, rtt=rtt, rto=self.rto_estimator.rto, info="SYN-ACK")
                    return
            except (socket.timeout, TimeoutError):
                retransmits += 1
                self.stats.total_timeouts += 1
                self.stats.total_packets_retransmitted += 1
                self.rto_estimator.on_timeout()
                self.stats.add_event("TIMEOUT", seq=0, rto=self.rto_estimator.rto, info=f"SYN timeout {retransmits}")
            except ValueError:
                # Corrupted packet ignored, wait for timeout
                pass

        raise ConnectionError(f"Failed to establish handshake with {dest_addr} after {retransmits} attempts.")

    def _send_chunk_stop_and_wait(self, seq_num: int, chunk: bytes, dest_addr: tuple):
        """Transmits one DATA packet and blocks until its specific ACK is received."""
        pkt = DARTPacket.create_data(seq_num=seq_num, data=chunk)
        pkt_bytes = pkt.pack()

        retransmits = 0
        while retransmits < self.max_retransmissions:
            self.sock.settimeout(self.rto_estimator.rto)
            t_send = time.time()
            self.sock.sendto(pkt_bytes, dest_addr)
            self.stats.total_packets_sent += 1
            if retransmits > 0:
                self.stats.total_packets_retransmitted += 1
                self.stats.add_event("RETRANSMIT", seq=seq_num, size=len(pkt_bytes), rto=self.rto_estimator.rto)
            else:
                self.stats.add_event("SEND", seq=seq_num, size=len(pkt_bytes), rto=self.rto_estimator.rto)

            # Wait for corresponding ACK
            while True:
                try:
                    raw_ack, _ = self.sock.recvfrom(2048)
                    ack_pkt = DARTPacket.unpack(raw_ack)

                    if ack_pkt.is_ack and ack_pkt.ack_num == seq_num:
                        # Correct ACK received!
                        rtt = time.time() - t_send
                        self.rto_estimator.update_rtt(rtt, is_retransmission=(retransmits > 0))
                        self.stats.rtt_samples.append(rtt)
                        self.stats.total_acks_received += 1
                        self.stats.add_event("ACK", ack=seq_num, rtt=rtt, rto=self.rto_estimator.rto)
                        return  # Advance to next packet!
                    else:
                        # Out of sequence or duplicate ACK: ignore and keep waiting until timeout
                        continue

                except (socket.timeout, TimeoutError):
                    # Timeout expired! Retransmit packet
                    retransmits += 1
                    self.stats.total_timeouts += 1
                    self.rto_estimator.on_timeout()
                    self.stats.add_event("TIMEOUT", seq=seq_num, rto=self.rto_estimator.rto, info=f"Attempt {retransmits}")
                    break  # Break inner loop to retransmit
                except ValueError:
                    # Corrupted ACK (bad checksum) -> treat as lost ACK and wait for timeout
                    continue

        raise TimeoutError(f"Transmission failed for packet seq={seq_num} after {retransmits} retransmissions.")

    def _perform_teardown(self, total_chunks: int, dest_addr: tuple):
        """Sends FIN and waits for FIN-ACK to ensure clean teardown."""
        fin_pkt = DARTPacket.create_fin(seq_num=total_chunks)
        fin_bytes = fin_pkt.pack()

        retransmits = 0
        while retransmits < 5:
            self.sock.settimeout(self.rto_estimator.rto)
            self.sock.sendto(fin_bytes, dest_addr)
            self.stats.total_packets_sent += 1
            self.stats.add_event("SEND", seq=total_chunks, info="FIN")

            try:
                raw_ack, _ = self.sock.recvfrom(2048)
                ack_pkt = DARTPacket.unpack(raw_ack)
                if ack_pkt.is_ack and ack_pkt.is_fin:
                    self.stats.add_event("ACK", ack=total_chunks, info="FIN-ACK")
                    return
            except (socket.timeout, TimeoutError):
                retransmits += 1
            except ValueError:
                pass


class StopAndWaitReceiver(BaseARQReceiver):
    """
    Stop-and-Wait ARQ Receiver.
    Verifies checksum, enforces strict in-order delivery, re-acknowledges duplicates,
    and conducts cryptographic SHA-256 hash validation upon transfer completion.
    """

    def __init__(self, listen_host: str = "0.0.0.0", listen_port: int = 9000):
        super().__init__(listen_host, listen_port)
        self.sock: Optional[socket.socket] = None
        self.is_running = False

    def receive_file(
        self,
        output_path: str,
        timeout_seconds: Optional[float] = 30.0
    ) -> TransferStats:
        """Listens for an incoming Stop-and-Wait file transfer and saves to output_path."""
        self.stats = TransferStats()
        self.stats.mark_start()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))
        if timeout_seconds:
            self.sock.settimeout(timeout_seconds)

        self.is_running = True
        expected_seq = 0
        received_chunks: Dict[int, bytes] = {}
        metadata: Dict[str, Any] = {}
        client_addr = None

        hasher = hashlib.sha256()

        try:
            # 1. Handshake Phase
            while self.is_running:
                try:
                    raw_data, client_addr = self.sock.recvfrom(65535)
                except (socket.timeout, TimeoutError):
                    raise TimeoutError(f"Receiver timed out waiting for connection on port {self.listen_port}")

                try:
                    pkt = DARTPacket.unpack(raw_data)
                except ValueError:
                    # Corrupted packet: drop silently
                    self.stats.add_event("CORRUPT", info="Handshake packet corrupted")
                    continue

                if pkt.is_syn:
                    metadata = json.loads(pkt.payload.decode("utf-8"))
                    self.stats.source_sha256 = metadata.get("sha256", "")
                    self.stats.add_event("RECV", seq=pkt.seq_num, size=len(raw_data), info="SYN")

                    # Send SYN-ACK
                    syn_ack = DARTPacket.create_syn_ack(ack_num=pkt.seq_num)
                    syn_ack_bytes = syn_ack.pack()
                    self.sock.sendto(syn_ack_bytes, client_addr)
                    self.stats.add_event("ACK", ack=pkt.seq_num, size=len(syn_ack_bytes), info="SYN-ACK")
                    break

            # 2. Data Reception Loop
            total_chunks = metadata.get("total_chunks", 0)

            while self.is_running:
                try:
                    raw_data, client_addr = self.sock.recvfrom(65535)
                except (socket.timeout, TimeoutError):
                    raise TimeoutError("Receiver timed out waiting for data packets.")

                try:
                    pkt = DARTPacket.unpack(raw_data)
                except ValueError:
                    # RFC 1071 Checksum validation failed! Packet corrupted during transit.
                    self.stats.add_event("CORRUPT", info="Data packet checksum failed")
                    continue

                if pkt.is_fin:
                    # Sender signaling transfer completion
                    self.stats.add_event("RECV", seq=pkt.seq_num, info="FIN")
                    fin_ack = DARTPacket.create_fin_ack(ack_num=pkt.seq_num)
                    self.sock.sendto(fin_ack.pack(), client_addr)
                    self.stats.add_event("ACK", ack=pkt.seq_num, info="FIN-ACK")
                    break

                if pkt.is_data:
                    self.stats.add_event("RECV", seq=pkt.seq_num, size=len(pkt.payload))

                    if pkt.seq_num == expected_seq:
                        # In-order packet received!
                        received_chunks[pkt.seq_num] = pkt.payload
                        hasher.update(pkt.payload)
                        self.stats.total_data_bytes += len(pkt.payload)

                        # Send ACK
                        ack_pkt = DARTPacket.create_ack(ack_num=pkt.seq_num)
                        ack_bytes = ack_pkt.pack()
                        self.sock.sendto(ack_bytes, client_addr)
                        self.stats.add_event("ACK", ack=pkt.seq_num, size=len(ack_bytes))

                        expected_seq += 1

                    elif pkt.seq_num < expected_seq:
                        # Duplicate packet: sender may have retransmitted because our previous ACK was delayed/lost
                        ack_pkt = DARTPacket.create_ack(ack_num=pkt.seq_num)
                        self.sock.sendto(ack_pkt.pack(), client_addr)
                        self.stats.add_event("ACK", ack=pkt.seq_num, info="Duplicate ACK")

                    else:
                        # Out of order packet (seq_num > expected_seq):
                        # Re-send ACK for last acknowledged packet
                        last_ack = expected_seq - 1 if expected_seq > 0 else 0
                        ack_pkt = DARTPacket.create_ack(ack_num=last_ack)
                        self.sock.sendto(ack_pkt.pack(), client_addr)
                        self.stats.add_event("ACK", ack=last_ack, info="Out-of-order NACK/ACK")

            # 3. Assemble and Write Output File
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            with open(output_path, "wb") as f:
                for i in range(expected_seq):
                    if i in received_chunks:
                        f.write(received_chunks[i])

            # 4. Cryptographic File Integrity Verification
            received_sha256 = hasher.hexdigest()
            self.stats.received_sha256 = received_sha256
            self.stats.integrity_verified = (
                bool(self.stats.source_sha256) and (self.stats.source_sha256 == received_sha256)
            )

        finally:
            self.stats.mark_end()
            self.is_running = False
            if self.sock:
                self.sock.close()
                self.sock = None

        return self.stats

    def receive_bytes(self, timeout_seconds: Optional[float] = 30.0) -> bytes:
        """In-memory convenience method to receive raw bytes."""
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            self.receive_file(tmp_path, timeout_seconds=timeout_seconds)
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
