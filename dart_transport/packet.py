"""DART's versioned, checksummed UDP packet codec."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, IntFlag
import struct
import time
import zlib


class PacketDecodeError(ValueError):
    """Raised when wire data is not a valid DART packet."""


class PacketType(IntEnum):
    """Packet meanings shared by all ARQ implementations."""

    DATA = 1
    ACK = 2
    FIN = 3
    FIN_ACK = 4


class PacketFlag(IntFlag):
    """Optional packet properties without changing the base header."""

    NONE = 0
    RETRANSMISSION = 1
    SELECTIVE_ACK = 2


MAGIC = b"DART"
VERSION = 1
MAX_PAYLOAD_SIZE = 1_200
# magic, version, type, flags, session id, sequence, acknowledgement,
# timestamp (microseconds), payload length, CRC-32
HEADER_FORMAT = "!4sBBHIIIQHI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
_CHECKSUM_OFFSET = HEADER_SIZE - struct.calcsize("!I")


@dataclass(frozen=True, slots=True)
class Packet:
    """An immutable DART datagram with explicit byte-order and integrity checks."""

    packet_type: PacketType
    session_id: int
    sequence: int = 0
    acknowledgement: int = 0
    payload: bytes = b""
    flags: PacketFlag = PacketFlag.NONE
    timestamp_us: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be bytes")
        if len(self.payload) > MAX_PAYLOAD_SIZE:
            raise ValueError(f"payload exceeds {MAX_PAYLOAD_SIZE} bytes")
        for name, value, maximum in (
            ("session_id", self.session_id, 0xFFFF_FFFF),
            ("sequence", self.sequence, 0xFFFF_FFFF),
            ("acknowledgement", self.acknowledgement, 0xFFFF_FFFF),
            ("timestamp_us", self.timestamp_us, 0xFFFF_FFFF_FFFF_FFFF),
        ):
            if not 0 <= value <= maximum:
                raise ValueError(f"{name} is outside its unsigned wire range")

    @classmethod
    def data(
        cls, session_id: int, sequence: int, payload: bytes, *, retransmission: bool = False
    ) -> "Packet":
        """Build a data packet with a transmission timestamp."""
        flags = PacketFlag.RETRANSMISSION if retransmission else PacketFlag.NONE
        return cls(
            packet_type=PacketType.DATA,
            session_id=session_id,
            sequence=sequence,
            payload=payload,
            flags=flags,
            timestamp_us=time.monotonic_ns() // 1_000,
        )

    @classmethod
    def acknowledgement_for(
        cls, session_id: int, acknowledgement: int, *, selective: bool = False
    ) -> "Packet":
        """Build an ACK; selective ACK semantics are indicated by a flag."""
        flags = PacketFlag.SELECTIVE_ACK if selective else PacketFlag.NONE
        return cls(
            packet_type=PacketType.ACK,
            session_id=session_id,
            acknowledgement=acknowledgement,
            flags=flags,
            timestamp_us=time.monotonic_ns() // 1_000,
        )

    def to_bytes(self) -> bytes:
        """Encode a complete datagram with CRC-32 over header and payload."""
        header_without_checksum = struct.pack(
            HEADER_FORMAT,
            MAGIC,
            VERSION,
            int(self.packet_type),
            int(self.flags),
            self.session_id,
            self.sequence,
            self.acknowledgement,
            self.timestamp_us,
            len(self.payload),
            0,
        )
        checksum = zlib.crc32(header_without_checksum + self.payload) & 0xFFFF_FFFF
        header = header_without_checksum[:_CHECKSUM_OFFSET] + struct.pack("!I", checksum)
        return header + self.payload

    @classmethod
    def from_bytes(cls, datagram: bytes) -> "Packet":
        """Validate and decode a datagram; malformed input never reaches ARQ state."""
        if len(datagram) < HEADER_SIZE:
            raise PacketDecodeError("datagram is shorter than the DART header")
        fields = struct.unpack(HEADER_FORMAT, datagram[:HEADER_SIZE])
        (
            magic,
            version,
            packet_type,
            flags,
            session_id,
            sequence,
            acknowledgement,
            timestamp_us,
            payload_length,
            transmitted_checksum,
        ) = fields
        if magic != MAGIC:
            raise PacketDecodeError("invalid DART magic")
        if version != VERSION:
            raise PacketDecodeError(f"unsupported DART version: {version}")
        if payload_length > MAX_PAYLOAD_SIZE:
            raise PacketDecodeError("payload length exceeds configured maximum")
        if len(datagram) != HEADER_SIZE + payload_length:
            raise PacketDecodeError("datagram length does not match header payload length")
        header_without_checksum = datagram[:_CHECKSUM_OFFSET] + b"\x00\x00\x00\x00"
        actual_checksum = zlib.crc32(header_without_checksum + datagram[HEADER_SIZE:]) & 0xFFFF_FFFF
        if actual_checksum != transmitted_checksum:
            raise PacketDecodeError("CRC-32 mismatch")
        try:
            decoded_type = PacketType(packet_type)
            decoded_flags = PacketFlag(flags)
        except ValueError as error:
            raise PacketDecodeError("unknown packet type or flags") from error
        return cls(
            packet_type=decoded_type,
            session_id=session_id,
            sequence=sequence,
            acknowledgement=acknowledgement,
            payload=datagram[HEADER_SIZE:],
            flags=decoded_flags,
            timestamp_us=timestamp_us,
        )
