"""Protocol-neutral foundations for Dynamic Adaptive Reliable Transport."""

from .adaptive import AdaptiveConfig, AdaptiveDecision, AdaptiveEngine, AdaptiveState
from .metrics import MetricsSnapshot, TransferMetrics
from .packet import Packet, PacketDecodeError, PacketFlag, PacketType
from .rtt import RtoConfig, RttEstimator
from .integration import ControllerConfig, ReliabilityController, TransportMode
from .integrity import IntegrityResult, sha256_file, verify_file_integrity

__all__ = [
    "AdaptiveConfig",
    "AdaptiveDecision",
    "AdaptiveEngine",
    "AdaptiveState",
    "ControllerConfig",
    "IntegrityResult",
    "MetricsSnapshot",
    "Packet",
    "PacketDecodeError",
    "PacketFlag",
    "PacketType",
    "RtoConfig",
    "RttEstimator",
    "ReliabilityController",
    "TransportMode",
    "TransferMetrics",
    "sha256_file",
    "verify_file_integrity",
]
