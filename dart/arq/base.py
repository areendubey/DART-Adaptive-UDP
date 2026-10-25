"""
Pluggable ARQ Protocol Interface Base Classes for DART Transport
Enables modular integration across Stop-and-Wait, Go-Back-N, and Selective Repeat.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Callable
from dart.stats import TransferStats


class ARQType(str, Enum):
    STOP_AND_WAIT = "stop_and_wait"
    GO_BACK_N = "go_back_n"
    SELECTIVE_REPEAT = "selective_repeat"


class BaseARQSender(ABC):
    """
    Abstract Base Class for all ARQ Sender implementations.
    Implemented by:
      - Kartikey Singh: StopAndWaitSender
      - Anushka Roy: GoBackNSender
      - Savvy Jatrana: SelectiveRepeatSender
    """

    def __init__(self, host: str, port: int, packet_size: int = 1400):
        self.host = host
        self.port = port
        self.packet_size = packet_size
        self.stats = TransferStats()

    @abstractmethod
    def send_file(self, file_path: str, progress_callback: Optional[Callable[[int, int], None]] = None) -> TransferStats:
        """
        Transfers a file reliably to the receiver.
        Returns detailed TransferStats upon completion.
        """
        pass

    @abstractmethod
    def send_bytes(self, data: bytes, progress_callback: Optional[Callable[[int, int], None]] = None) -> TransferStats:
        """
        Transfers raw in-memory bytes reliably to the receiver.
        """
        pass

    def get_stats(self) -> TransferStats:
        return self.stats


class BaseARQReceiver(ABC):
    """
    Abstract Base Class for all ARQ Receiver implementations.
    Implemented by:
      - Kartikey Singh: StopAndWaitReceiver
      - Anushka Roy: GoBackNReceiver
      - Savvy Jatrana: SelectiveRepeatReceiver
    """

    def __init__(self, listen_host: str, listen_port: int):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.stats = TransferStats()

    @abstractmethod
    def receive_file(self, output_path: str, timeout_seconds: Optional[float] = None) -> TransferStats:
        """
        Listens for incoming transfer and writes assembled file to output_path.
        Verifies SHA-256 integrity hash against sender's metadata.
        """
        pass

    @abstractmethod
    def receive_bytes(self, timeout_seconds: Optional[float] = None) -> bytes:
        """
        Listens for incoming transfer and returns assembled bytes.
        """
        pass

    def get_stats(self) -> TransferStats:
        return self.stats
