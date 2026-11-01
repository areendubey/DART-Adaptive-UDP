"""Unified DART packet (stand-in for dart_transport.packet.Packet).

On-wire header (28 bytes, network order):
    magic:4s "DART" | version:u8 | flags:u8 | seq:u32 | ack:u32 |
    timestamp_us:u64 | payload_len:u16 | crc32:u32
    payload: payload_len bytes

* flags     : FLAG_DATA, FLAG_ACK or FLAG_FIN
* timestamp : microseconds. The sender stamps DATA packets; the receiver
              echoes the stamp back in the ACK so the sender can measure RTT.
* crc32     : covers the whole packet with the crc32 field set to zero.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum

from dart.constants import (
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN,
    HEADER_FORMAT,
    HEADER_SIZE,
    MAGIC,
    MAX_PAYLOAD,
    PROTOCOL_VERSION,
)


class PacketType(IntEnum):
    DATA = 1
    ACK = 2
    FIN = 3


_TYPE_TO_FLAG = {
    PacketType.DATA: FLAG_DATA,
    PacketType.ACK: FLAG_ACK,
    PacketType.FIN: FLAG_FIN,
}
_FLAG_TO_TYPE = {
    FLAG_DATA: PacketType.DATA,
    FLAG_ACK: PacketType.ACK,
    FLAG_FIN: PacketType.FIN,
}


class PacketError(ValueError):
    """Malformed, truncated, or corrupt packet."""


@dataclass(frozen=True, slots=True)
class Packet:
    ptype: PacketType
    seq: int
    ack: int
    payload: bytes = b""
    version: int = PROTOCOL_VERSION
    timestamp: int = 0  # microseconds; 0 means "no timestamp"

    def encode(self) -> bytes:
        if len(self.payload) > MAX_PAYLOAD:
            raise PacketError(f"payload too large: {len(self.payload)}")
        flags = _TYPE_TO_FLAG[PacketType(self.ptype)]
        header_no_crc = struct.pack(
            HEADER_FORMAT,
            MAGIC,
            self.version,
            flags,
            self.seq & 0xFFFFFFFF,
            self.ack & 0xFFFFFFFF,
            self.timestamp & 0xFFFFFFFFFFFFFFFF,
            len(self.payload),
            0,
        )
        crc = zlib.crc32(header_no_crc + self.payload) & 0xFFFFFFFF
        header = struct.pack(
            HEADER_FORMAT,
            MAGIC,
            self.version,
            flags,
            self.seq & 0xFFFFFFFF,
            self.ack & 0xFFFFFFFF,
            self.timestamp & 0xFFFFFFFFFFFFFFFF,
            len(self.payload),
            crc,
        )
        return header + self.payload

    @staticmethod
    def decode(raw: bytes) -> Packet:
        if len(raw) < HEADER_SIZE:
            raise PacketError("truncated header")
        magic, version, flags, seq, ack, timestamp, payload_len, crc = struct.unpack(
            HEADER_FORMAT, raw[:HEADER_SIZE]
        )
        if magic != MAGIC:
            raise PacketError("bad magic")
        if version != PROTOCOL_VERSION:
            raise PacketError(f"unsupported version {version}")
        if len(raw) != HEADER_SIZE + payload_len:
            raise PacketError("length mismatch")
        payload = raw[HEADER_SIZE:]
        header_no_crc = raw[: HEADER_SIZE - 4] + b"\x00\x00\x00\x00"
        if zlib.crc32(header_no_crc + payload) & 0xFFFFFFFF != crc:
            raise PacketError("crc mismatch")
        kind = _FLAG_TO_TYPE.get(flags)
        if kind is None:
            raise PacketError(f"unknown flags {flags:#x}")
        return Packet(
            ptype=kind,
            seq=seq,
            ack=ack,
            payload=payload,
            version=version,
            timestamp=timestamp,
        )

    def is_data(self) -> bool:
        return self.ptype == PacketType.DATA

    def is_ack(self) -> bool:
        return self.ptype == PacketType.ACK

    def is_fin(self) -> bool:
        return self.ptype == PacketType.FIN
