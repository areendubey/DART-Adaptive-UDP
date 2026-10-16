"""DART packet encode/decode used by Go-Back-N.

On-wire header (14 bytes, network order):
    version:u8 | ptype:u8 | seq:u32 | ack:u32 | payload_len:u16 | checksum:u16
    payload: payload_len bytes

Checksum covers the whole packet with the checksum field set to zero.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

from dart.checksum import internet_checksum
from dart.constants import HEADER_FORMAT, HEADER_SIZE, MAX_PAYLOAD, PROTOCOL_VERSION


class PacketType(IntEnum):
    DATA = 1
    ACK = 2
    FIN = 3


class PacketError(ValueError):
    """Malformed, truncated, or corrupt packet."""


@dataclass(frozen=True, slots=True)
class Packet:
    ptype: PacketType
    seq: int
    ack: int
    payload: bytes = b""
    version: int = PROTOCOL_VERSION

    def encode(self) -> bytes:
        if len(self.payload) > MAX_PAYLOAD:
            raise PacketError(f"payload too large: {len(self.payload)}")
        header_wo_csum = struct.pack(
            HEADER_FORMAT,
            self.version,
            int(self.ptype),
            self.seq & 0xFFFFFFFF,
            self.ack & 0xFFFFFFFF,
            len(self.payload),
            0,
        )
        body = header_wo_csum + self.payload
        csum = internet_checksum(body)
        header = struct.pack(
            HEADER_FORMAT,
            self.version,
            int(self.ptype),
            self.seq & 0xFFFFFFFF,
            self.ack & 0xFFFFFFFF,
            len(self.payload),
            csum,
        )
        return header + self.payload

    @staticmethod
    def decode(raw: bytes) -> Packet:
        if len(raw) < HEADER_SIZE:
            raise PacketError("truncated header")
        version, ptype, seq, ack, payload_len, _csum = struct.unpack(
            HEADER_FORMAT, raw[:HEADER_SIZE]
        )
        if version != PROTOCOL_VERSION:
            raise PacketError(f"unsupported version {version}")
        if len(raw) < HEADER_SIZE + payload_len:
            raise PacketError("truncated payload")
        payload = raw[HEADER_SIZE : HEADER_SIZE + payload_len]
        if internet_checksum(raw[: HEADER_SIZE + payload_len]) != 0:
            raise PacketError("checksum mismatch")
        try:
            kind = PacketType(ptype)
        except ValueError as exc:
            raise PacketError(f"unknown packet type {ptype}") from exc
        return Packet(ptype=kind, seq=seq, ack=ack, payload=payload, version=version)

    def is_data(self) -> bool:
        return self.ptype == PacketType.DATA

    def is_ack(self) -> bool:
        return self.ptype == PacketType.ACK

    def is_fin(self) -> bool:
        return self.ptype == PacketType.FIN
