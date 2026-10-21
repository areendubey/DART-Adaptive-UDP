"""
DART Bidirectional UDP Channel Proxy
Author: Kartikey Singh (Primary Owner)
Catalogue Foundation: P3 Reliable Data Transfer over UDP

A multi-threaded UDP proxy that sits between Sender and Receiver.
Intercepts bidirectional traffic and subjects it to configurable loss,
corruption, delay, jitter, duplication, and reordering.
"""

import socket
import threading
import time
import heapq
from typing import Optional, Tuple, Dict, Any

from dart.emulator.channel import NetworkChannelEmulator, EmulatorConfig


class ChannelProxy:
    """
    Bidirectional UDP Proxy that applies channel impairments in real time.
    """

    def __init__(
        self,
        listen_host: str = "127.0.0.1",
        listen_port: int = 8500,
        target_host: str = "127.0.0.1",
        target_port: int = 9000,
        forward_config: Optional[EmulatorConfig] = None,
        reverse_config: Optional[EmulatorConfig] = None
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port

        self.forward_emulator = NetworkChannelEmulator(forward_config or EmulatorConfig())
        # If reverse_config is not explicitly provided, mirror forward_config with separate RNG state
        rev_conf = reverse_config or EmulatorConfig(
            loss_rate=self.forward_emulator.config.loss_rate,
            corruption_rate=self.forward_emulator.config.corruption_rate,
            duplication_rate=self.forward_emulator.config.duplication_rate,
            reorder_rate=self.forward_emulator.config.reorder_rate,
            delay_mean_ms=self.forward_emulator.config.delay_mean_ms,
            delay_jitter_ms=self.forward_emulator.config.delay_jitter_ms,
            seed=(self.forward_emulator.config.seed + 9999) if self.forward_emulator.config.seed is not None else None
        )
        self.reverse_emulator = NetworkChannelEmulator(rev_conf)

        self.sock: Optional[socket.socket] = None
        self.client_addr: Optional[Tuple[str, int]] = None
        self.is_running = False

        # Priority Queue for scheduled delivery: (delivery_timestamp, packet_counter, data_bytes, dest_addr)
        self._delivery_heap = []
        self._heap_lock = threading.Lock()
        self._heap_cond = threading.Condition(self._heap_lock)
        self._counter = 0

        self._rx_thread: Optional[threading.Thread] = None
        self._tx_thread: Optional[threading.Thread] = None

    def start(self):
        """Starts the proxy server and background dispatch threads."""
        if self.is_running:
            return

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))
        self.sock.settimeout(0.5)

        self.is_running = True

        self._rx_thread = threading.Thread(target=self._receive_loop, daemon=True, name="ProxyRx")
        self._tx_thread = threading.Thread(target=self._dispatch_loop, daemon=True, name="ProxyTx")

        self._rx_thread.start()
        self._tx_thread.start()

    def stop(self):
        """Stops the proxy and releases socket resources."""
        self.is_running = False
        with self._heap_cond:
            self._heap_cond.notify_all()

        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

        if self._rx_thread and self._rx_thread.is_alive():
            self._rx_thread.join(timeout=1.0)
        if self._tx_thread and self._tx_thread.is_alive():
            self._tx_thread.join(timeout=1.0)

    def _schedule_packet(self, data: bytes, delay_seconds: float, dest_addr: Tuple[str, int]):
        """Schedules packet to be sent at time.time() + delay_seconds."""
        delivery_time = time.time() + delay_seconds
        with self._heap_cond:
            self._counter += 1
            heapq.heappush(self._delivery_heap, (delivery_time, self._counter, data, dest_addr))
            self._heap_cond.notify()

    def _receive_loop(self):
        """Listens on UDP socket and directs traffic through appropriate emulator."""
        target_addr = (self.target_host, self.target_port)

        while self.is_running:
            try:
                data, src_addr = self.sock.recvfrom(65535)
            except (socket.timeout, TimeoutError):
                continue
            except OSError:
                break

            # Traffic classification
            if src_addr == target_addr:
                # Reverse direction (Receiver/Server -> Sender/Client)
                if self.client_addr is not None:
                    deliverables = self.reverse_emulator.process_packet(data)
                    for pkt_bytes, delay in deliverables:
                        self._schedule_packet(pkt_bytes, delay, self.client_addr)
            else:
                # Forward direction (Sender/Client -> Receiver/Server)
                self.client_addr = src_addr
                deliverables = self.forward_emulator.process_packet(data)
                for pkt_bytes, delay in deliverables:
                    self._schedule_packet(pkt_bytes, delay, target_addr)

    def _dispatch_loop(self):
        """Transmits scheduled packets when their delay deadline arrives."""
        while self.is_running:
            with self._heap_cond:
                while self.is_running and not self._delivery_heap:
                    self._heap_cond.wait(timeout=0.05)

                if not self.is_running:
                    break

                now = time.time()
                earliest_time, _, _, _ = self._delivery_heap[0]
                time_to_wait = earliest_time - now

                if time_to_wait > 0.001:
                    self._heap_cond.wait(timeout=time_to_wait)
                    continue

                # Pop earliest deliverable
                _, _, data, dest_addr = heapq.heappop(self._delivery_heap)

            # Send packet
            if self.sock and self.is_running:
                try:
                    self.sock.sendto(data, dest_addr)
                except Exception:
                    pass

    def get_stats(self) -> Dict[str, Any]:
        """Returns consolidated forward and reverse channel emulation statistics."""
        return {
            "forward_channel": self.forward_emulator.stats.summary(),
            "reverse_channel": self.reverse_emulator.stats.summary()
        }
