"""
DART Go-Back-N ARQ Protocol Implementation
Assigned Primary Owner: Anushka Roy
Catalogue Foundation: P3 Reliable Data Transfer over UDP

This module defines the Go-Back-N (GBN) sliding window protocol interface.
Anushka Roy will implement the sliding window sender buffer, cumulative ACKs,
and single-timer retransmission logic here.
"""

from typing import Optional, Callable
from dart.stats import TransferStats
from dart.arq.base import BaseARQSender, BaseARQReceiver


class GoBackNSender(BaseARQSender):
    """
    Go-Back-N ARQ Sender (Sliding Window, Cumulative ACKs).
    Maintains a send window of size N [base, base + N - 1].
    Retransmits all unacknowledged packets in the window on timeout.
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
        # TODO (Anushka Roy): Implement sliding window file transfer
        raise NotImplementedError("GoBackNSender.send_file is scheduled for Week 03 by Anushka Roy.")

    def send_bytes(
        self,
        data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> TransferStats:
        # TODO (Anushka Roy): Implement sliding window bytes transfer
        raise NotImplementedError("GoBackNSender.send_bytes is scheduled for Week 03 by Anushka Roy.")


class GoBackNReceiver(BaseARQReceiver):
    """
    Go-Back-N ARQ Receiver.
    Accepts packets strictly in-order and issues cumulative ACKs.
    Discards out-of-order packets and re-ACKs the highest consecutively received seq_num.
    """

    def __init__(self, listen_host: str = "0.0.0.0", listen_port: int = 9000):
        super().__init__(listen_host, listen_port)

    def receive_file(
        self,
        output_path: str,
        timeout_seconds: Optional[float] = 30.0
    ) -> TransferStats:
        # TODO (Anushka Roy): Implement cumulative ACK receiver
        raise NotImplementedError("GoBackNReceiver.receive_file is scheduled for Week 03 by Anushka Roy.")

    def receive_bytes(self, timeout_seconds: Optional[float] = 30.0) -> bytes:
        raise NotImplementedError("GoBackNReceiver.receive_bytes is scheduled for Week 03 by Anushka Roy.")
