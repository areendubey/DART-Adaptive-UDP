"""
DART Packet Specification and Serialization (RFC 1071 Checksum Compliant)

Binary Header Format (16 bytes fixed, Big-Endian '!H B B I I H H'):
+-----------------------+-----------------------+
|     Magic (2B)        |  Ver (1B)  | Flag(1B) |
+-----------------------+-----------------------+
|                 Sequence Number (4B)          |
+-----------------------------------------------+
|             Acknowledgement Number (4B)       |
+-----------------------+-----------------------+
|   Window Size (2B)    |     Checksum (2B)     |
+-----------------------+-----------------------+
|                 Payload (0 to MSS B)          |
+-----------------------------------------------+
"""

import struct
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any

# Magic number identifying DART packets: 0xDA87
DART_MAGIC = 0xDA87
DART_VERSION = 1
HEADER_FORMAT = "!H B B I I H H"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # Exactly 16 bytes
MAX_PAYLOAD_SIZE = 1400  # Conservative MSS to prevent IP fragmentation (1500 MTU - 20 IP - 8 UDP - 16 DART = 1456)

# Protocol Flags (bitmask)
FLAG_DATA = 0x01
FLAG_ACK  = 0x02
FLAG_SYN  = 0x04
FLAG_FIN  = 0x08
FLAG_RST  = 0x10
FLAG_NACK = 0x20


def compute_internet_checksum(data: bytes) -> int:
    """
    Computes standard 16-bit One's Complement Internet Checksum (RFC 1071).
    Used by IPv4, TCP, and UDP headers.
    """
    if len(data) % 2 != 0:
        data = data + b'\x00'  # Pad with 0 byte if odd length

    total_sum = 0
    # Process 16-bit words (2 bytes each)
    for i in range(0, len(data), 2):
        word = (data[i] << 8) + data[i + 1]
        total_sum += word
        # Fold 32-bit carry bits back into lower 16 bits
        while total_sum > 0xFFFF:
            total_sum = (total_sum & 0xFFFF) + (total_sum >> 16)

    # Invert bits for one's complement
    checksum = (~total_sum) & 0xFFFF
    return checksum


@dataclass
class DARTPacket:
    seq_num: int
    ack_num: int
    flags: int
    window_size: int = 1
    checksum: int = 0
    payload: bytes = b""
    magic: int = DART_MAGIC
    version: int = DART_VERSION

    @property
    def is_data(self) -> bool:
        return bool(self.flags & FLAG_DATA)

    @property
    def is_ack(self) -> bool:
        return bool(self.flags & FLAG_ACK)

    @property
    def is_syn(self) -> bool:
        return bool(self.flags & FLAG_SYN)

    @property
    def is_fin(self) -> bool:
        return bool(self.flags & FLAG_FIN)

    @property
    def is_rst(self) -> bool:
        return bool(self.flags & FLAG_RST)

    @property
    def is_nack(self) -> bool:
        return bool(self.flags & FLAG_NACK)

    def pack(self) -> bytes:
        """
        Serializes packet into binary bytes including calculated RFC 1071 checksum.
        Header is packed with checksum field set to 0, then checksum is computed
        over (header + payload), and finally inserted into the header.
        """
        header_zero_cksum = struct.pack(
            HEADER_FORMAT,
            self.magic,
            self.version,
            self.flags,
            self.seq_num,
            self.ack_num,
            self.window_size,
            0  # Checksum field = 0 during calculation
        )
        calculated_checksum = compute_internet_checksum(header_zero_cksum + self.payload)
        self.checksum = calculated_checksum

        final_header = struct.pack(
            HEADER_FORMAT,
            self.magic,
            self.version,
            self.flags,
            self.seq_num,
            self.ack_num,
            self.window_size,
            self.checksum
        )
        return final_header + self.payload

    @classmethod
    def unpack(cls, raw_bytes: bytes) -> "DARTPacket":
        """
        Deserializes binary data into DARTPacket and validates checksum and magic header.
        Raises ValueError if packet is malformed or corrupted.
        """
        if len(raw_bytes) < HEADER_SIZE:
            raise ValueError(f"Packet too short: {len(raw_bytes)} bytes < min header {HEADER_SIZE}")

        magic, version, flags, seq_num, ack_num, window_size, received_checksum = struct.unpack(
            HEADER_FORMAT, raw_bytes[:HEADER_SIZE]
        )

        if magic != DART_MAGIC:
            raise ValueError(f"Invalid magic number: {hex(magic)} != {hex(DART_MAGIC)}")

        payload = raw_bytes[HEADER_SIZE:]

        # Verify RFC 1071 checksum:
        # Re-pack with checksum=0 and compare computed checksum
        header_zero_cksum = struct.pack(
            HEADER_FORMAT,
            magic,
            version,
            flags,
            seq_num,
            ack_num,
            window_size,
            0
        )
        expected_checksum = compute_internet_checksum(header_zero_cksum + payload)
        if received_checksum != expected_checksum:
            raise ValueError(
                f"Checksum mismatch! Received 0x{received_checksum:04x}, expected 0x{expected_checksum:04x}"
            )

        return cls(
            seq_num=seq_num,
            ack_num=ack_num,
            flags=flags,
            window_size=window_size,
            checksum=received_checksum,
            payload=payload,
            magic=magic,
            version=version
        )

    # --- Convenience Factory Constructors ---

    @classmethod
    def create_syn(cls, seq_num: int, metadata: Dict[str, Any]) -> "DARTPacket":
        """Creates a SYN packet carrying file metadata (filename, size, sha256) as JSON payload."""
        payload_bytes = json.dumps(metadata).encode("utf-8")
        return cls(
            seq_num=seq_num,
            ack_num=0,
            flags=FLAG_SYN,
            payload=payload_bytes
        )

    @classmethod
    def create_syn_ack(cls, ack_num: int) -> "DARTPacket":
        """Creates a SYN-ACK packet confirming handshake."""
        return cls(
            seq_num=0,
            ack_num=ack_num,
            flags=FLAG_SYN | FLAG_ACK,
            payload=b""
        )

    @classmethod
    def create_data(cls, seq_num: int, data: bytes, window_size: int = 1) -> "DARTPacket":
        """Creates a DATA packet with chunk payload."""
        return cls(
            seq_num=seq_num,
            ack_num=0,
            flags=FLAG_DATA,
            window_size=window_size,
            payload=data
        )

    @classmethod
    def create_ack(cls, ack_num: int, window_size: int = 1) -> "DARTPacket":
        """Creates an ACK packet acknowledging received sequence number."""
        return cls(
            seq_num=0,
            ack_num=ack_num,
            flags=FLAG_ACK,
            window_size=window_size,
            payload=b""
        )

    @classmethod
    def create_fin(cls, seq_num: int, file_hash: str = "") -> "DARTPacket":
        """Creates a FIN packet signaling end of transmission."""
        return cls(
            seq_num=seq_num,
            ack_num=0,
            flags=FLAG_FIN,
            payload=file_hash.encode("utf-8") if file_hash else b""
        )

    @classmethod
    def create_fin_ack(cls, ack_num: int) -> "DARTPacket":
        """Creates a FIN-ACK acknowledging teardown."""
        return cls(
            seq_num=0,
            ack_num=ack_num,
            flags=FLAG_FIN | FLAG_ACK,
            payload=b""
        )
