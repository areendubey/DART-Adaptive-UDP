"""
DART ARQ Protocols Package
Provides factory and unified access to Stop-and-Wait, Go-Back-N, and Selective Repeat.
"""

from dart.arq.base import ARQType, BaseARQSender, BaseARQReceiver
from dart.arq.stop_and_wait import StopAndWaitSender, StopAndWaitReceiver
from dart.arq.go_back_n import GoBackNSender, GoBackNReceiver
from dart.arq.selective_repeat import SelectiveRepeatSender, SelectiveRepeatReceiver


def get_sender(arq_type: ARQType, host: str, port: int, **kwargs) -> BaseARQSender:
    """Factory helper to instantiate the designated ARQ sender."""
    if arq_type == ARQType.STOP_AND_WAIT or arq_type == "sw" or arq_type == "stop_and_wait":
        return StopAndWaitSender(host, port, **kwargs)
    elif arq_type == ARQType.GO_BACK_N or arq_type == "gbn" or arq_type == "go_back_n":
        return GoBackNSender(host, port, **kwargs)
    elif arq_type == ARQType.SELECTIVE_REPEAT or arq_type == "sr" or arq_type == "selective_repeat":
        return SelectiveRepeatSender(host, port, **kwargs)
    raise ValueError(f"Unknown ARQ type: {arq_type}")


def get_receiver(arq_type: ARQType, listen_host: str = "0.0.0.0", listen_port: int = 9000, **kwargs) -> BaseARQReceiver:
    """Factory helper to instantiate the designated ARQ receiver."""
    if arq_type == ARQType.STOP_AND_WAIT or arq_type == "sw" or arq_type == "stop_and_wait":
        return StopAndWaitReceiver(listen_host, listen_port, **kwargs)
    elif arq_type == ARQType.GO_BACK_N or arq_type == "gbn" or arq_type == "go_back_n":
        return GoBackNReceiver(listen_host, listen_port, **kwargs)
    elif arq_type == ARQType.SELECTIVE_REPEAT or arq_type == "sr" or arq_type == "selective_repeat":
        return SelectiveRepeatReceiver(listen_host, listen_port, **kwargs)
    raise ValueError(f"Unknown ARQ type: {arq_type}")


__all__ = [
    "ARQType",
    "BaseARQSender",
    "BaseARQReceiver",
    "StopAndWaitSender",
    "StopAndWaitReceiver",
    "GoBackNSender",
    "GoBackNReceiver",
    "SelectiveRepeatSender",
    "SelectiveRepeatReceiver",
    "get_sender",
    "get_receiver"
]
