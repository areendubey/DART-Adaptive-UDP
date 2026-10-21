"""
Unit Tests for DART Packet Serialization and RFC 1071 Checksum
"""

import unittest
import struct
from dart.packet import (
    DARTPacket,
    compute_internet_checksum,
    DART_MAGIC,
    FLAG_SYN,
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN
)


class TestDARTPacket(unittest.TestCase):

    def test_checksum_computation(self):
        test_bytes = b"Hello, DART Reliable UDP Transport!"
        cksum = compute_internet_checksum(test_bytes)
        self.assertIsInstance(cksum, int)
        self.assertGreaterEqual(cksum, 0)
        self.assertLessEqual(cksum, 0xFFFF)

    def test_packet_pack_unpack_roundtrip(self):
        payload = b"Reliable chunk payload #42"
        pkt = DARTPacket.create_data(seq_num=42, data=payload, window_size=1)
        raw = pkt.pack()

        unpacked = DARTPacket.unpack(raw)
        self.assertEqual(unpacked.magic, DART_MAGIC)
        self.assertEqual(unpacked.seq_num, 42)
        self.assertEqual(unpacked.ack_num, 0)
        self.assertTrue(unpacked.is_data)
        self.assertFalse(unpacked.is_ack)
        self.assertEqual(unpacked.payload, payload)
        self.assertEqual(unpacked.checksum, pkt.checksum)

    def test_syn_packet(self):
        meta = {"filename": "test.pdf", "size": 1024, "sha256": "abc123"}
        syn = DARTPacket.create_syn(seq_num=0, metadata=meta)
        raw = syn.pack()

        unpacked = DARTPacket.unpack(raw)
        self.assertTrue(unpacked.is_syn)
        self.assertFalse(unpacked.is_ack)
        self.assertEqual(unpacked.seq_num, 0)

    def test_ack_packet(self):
        ack = DARTPacket.create_ack(ack_num=15)
        raw = ack.pack()

        unpacked = DARTPacket.unpack(raw)
        self.assertTrue(unpacked.is_ack)
        self.assertEqual(unpacked.ack_num, 15)
        self.assertEqual(unpacked.payload, b"")

    def test_corruption_detection(self):
        pkt = DARTPacket.create_data(seq_num=5, data=b"Critical in-transit data")
        raw = bytearray(pkt.pack())

        # Corrupt a single byte in the payload
        raw[-1] ^= 0xFF

        # Unpack should fail with ValueError due to checksum mismatch
        with self.assertRaises(ValueError) as ctx:
            DARTPacket.unpack(bytes(raw))
        self.assertIn("Checksum mismatch", str(ctx.exception))

    def test_invalid_magic_number(self):
        pkt = DARTPacket.create_ack(ack_num=1)
        raw = bytearray(pkt.pack())
        # Corrupt magic number (first 2 bytes)
        raw[0] = 0x00
        raw[1] = 0x00

        with self.assertRaises(ValueError) as ctx:
            DARTPacket.unpack(bytes(raw))


if __name__ == "__main__":
    unittest.main()
