"""
DART Selective Repeat ARQ Protocol Implementation
Assigned Primary Owner: Savvy Jatrana
Catalogue Foundation: P3 Reliable Data Transfer over UDP

This module defines the Selective Repeat (SR) sliding window protocol interface.
Savvy Jatrana will implement the individual per-packet timers, selective ACKs,
and out-of-order receive buffering logic here.
"""

from typing import Optional, Callable
from dart.stats import TransferStats
from dart.arq.base import BaseARQSender, BaseARQReceiver


class SelectiveRepeatSender(BaseARQSender):
    """
    Selective Repeat ARQ Sender.
    Maintains a send window and individual retransmission timers per packet.
    Retransmits only specifically timed-out or unacknowledged packets.
    """

    def __init__(
        self,
        host: str,
        port: int,
        window_size: int = 5,
        packet_size: int = 1400,
        initial_rto: float = 0.5
    ):
        super().__init__(host, port, packet_size)
        self.window_size = window_size
        self.initial_rto = initial_rto

    def send_file(
        self,
        file_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> TransferStats:
        # TODO (Savvy Jatrana): Implement selective repeat sender logic
        raise NotImplementedError("SelectiveRepeatSender.send_file is scheduled for Week 04 by Savvy Jatrana.")

    def send_bytes(
        self,
        data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> TransferStats:
        # TODO (Savvy Jatrana): Implement selective repeat bytes transfer
        raise NotImplementedError("SelectiveRepeatSender.send_bytes is scheduled for Week 04 by Savvy Jatrana.")


class SelectiveRepeatReceiver(BaseARQReceiver):
    """
    Selective Repeat ARQ Receiver.
    Buffers correctly received out-of-order packets within the receive window [rcv_base, rcv_base + N - 1]
    and sends selective ACKs. Delivers in-order chunks to upper layer as base advances.
    """

    def __init__(self, listen_host: str = "0.0.0.0", listen_port: int = 9000, window_size: int = 5):
        super().__init__(listen_host, listen_port)
        self.window_size = window_size

    def receive_file(
        self,
        output_path: str,
        timeout_seconds: Optional[float] = 30.0
    ) -> TransferStats:
        # TODO (Savvy Jatrana): Implement out-of-order buffering receiver
        raise NotImplementedError("SelectiveRepeatReceiver.receive_file is scheduled for Week 04 by Savvy Jatrana.")

    def receive_bytes(self, timeout_seconds: Optional[float] = 30.0) -> bytes:
        raise NotImplementedError("SelectiveRepeatReceiver.receive_bytes is scheduled for Week 04 by Savvy Jatrana.")
