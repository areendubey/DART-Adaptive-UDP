"""Packet encode/decode and checksum tests."""

import unittest

import struct
import zlib

from dart.constants import HEADER_SIZE, MAGIC
from dart.packet import Packet, PacketError, PacketType


class PacketTests(unittest.TestCase):
    def test_roundtrip_data(self) -> None:
        pkt = Packet(ptype=PacketType.DATA, seq=7, ack=0, payload=b"hello-gbn")
        decoded = Packet.decode(pkt.encode())
        self.assertEqual(decoded.ptype, PacketType.DATA)
        self.assertEqual(decoded.seq, 7)
        self.assertEqual(decoded.payload, b"hello-gbn")

    def test_roundtrip_ack(self) -> None:
        pkt = Packet(ptype=PacketType.ACK, seq=0, ack=42)
        decoded = Packet.decode(pkt.encode())
        self.assertTrue(decoded.is_ack())
        self.assertEqual(decoded.ack, 42)

    def test_corruption_detected(self) -> None:
        raw = bytearray(Packet(PacketType.DATA, seq=1, ack=0, payload=b"abc").encode())
        raw[-1] ^= 0x01
        with self.assertRaises(PacketError):
            Packet.decode(bytes(raw))

    def test_truncated_header(self) -> None:
        with self.assertRaises(PacketError):
            Packet.decode(b"\x00\x01")

    def test_crc32_self_consistent(self) -> None:
        encoded = Packet(PacketType.FIN, seq=3, ack=0).encode()
        stored_crc = struct.unpack("!I", encoded[HEADER_SIZE - 4 : HEADER_SIZE])[0]
        zeroed = encoded[: HEADER_SIZE - 4] + b"\x00\x00\x00\x00" + encoded[HEADER_SIZE:]
        self.assertEqual(zlib.crc32(zeroed) & 0xFFFFFFFF, stored_crc)

    def test_header_starts_with_dart_magic_and_is_28_bytes(self) -> None:
        encoded = Packet(PacketType.DATA, seq=0, ack=0, payload=b"xy").encode()
        self.assertEqual(encoded[:4], MAGIC)
        self.assertEqual(HEADER_SIZE, 28)
        self.assertEqual(len(encoded), HEADER_SIZE + 2)

    def test_timestamp_survives_roundtrip(self) -> None:
        pkt = Packet(PacketType.ACK, seq=0, ack=5, timestamp=123456789)
        self.assertEqual(Packet.decode(pkt.encode()).timestamp, 123456789)

    def test_flags_are_single_bits(self) -> None:
        self.assertEqual(Packet(PacketType.DATA, 0, 0).encode()[5], 0x01)
        self.assertEqual(Packet(PacketType.ACK, 0, 0).encode()[5], 0x02)
        self.assertEqual(Packet(PacketType.FIN, 0, 0).encode()[5], 0x04)

    def test_bad_magic_rejected(self) -> None:
        raw = bytearray(Packet(PacketType.DATA, 1, 0, b"abc").encode())
        raw[0:4] = b"NOPE"
        with self.assertRaises(PacketError):
            Packet.decode(bytes(raw))

    def test_every_single_byte_flip_is_detected(self) -> None:
        raw = Packet(PacketType.DATA, 9, 0, b"payload-bytes").encode()
        for position in range(len(raw)):
            damaged = bytearray(raw)
            damaged[position] ^= 0xFF
            with self.assertRaises(PacketError, msg=f"byte {position}"):
                Packet.decode(bytes(damaged))


if __name__ == "__main__":
    unittest.main()
