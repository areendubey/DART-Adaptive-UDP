"""Packet encode/decode and checksum tests."""

import unittest

from dart.checksum import internet_checksum
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

    def test_checksum_self_consistent(self) -> None:
        encoded = Packet(PacketType.FIN, seq=3, ack=0).encode()
        self.assertEqual(internet_checksum(encoded), 0)


if __name__ == "__main__":
    unittest.main()
