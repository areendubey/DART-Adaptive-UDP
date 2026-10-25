import unittest
import struct

from dart_transport.packet import HEADER_SIZE, Packet, PacketDecodeError, PacketFlag, PacketType
from dart.packet import (
    DARTPacket,
    compute_internet_checksum,
    DART_MAGIC,
    FLAG_SYN,
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN,
)


class PacketTests(unittest.TestCase):
    def test_data_packet_round_trip_preserves_header_and_payload(self) -> None:
        packet = Packet(PacketType.DATA, session_id=7, sequence=9, payload=b"DART", flags=PacketFlag.RETRANSMISSION, timestamp_us=123)

        decoded = Packet.from_bytes(packet.to_bytes())

        self.assertEqual(decoded, packet)
        self.assertEqual(len(packet.to_bytes()), HEADER_SIZE + 4)

    def test_checksum_rejects_corruption(self) -> None:
        encoded = bytearray(Packet(PacketType.DATA, session_id=1, payload=b"safe").to_bytes())
        encoded[-1] ^= 0x01

        with self.assertRaisesRegex(PacketDecodeError, "CRC-32"):
            Packet.from_bytes(bytes(encoded))

    def test_decoder_rejects_truncated_and_length_mismatched_datagrams(self) -> None:
        with self.assertRaisesRegex(PacketDecodeError, "shorter"):
            Packet.from_bytes(b"DART")

        encoded = Packet(PacketType.ACK, session_id=1).to_bytes() + b"unexpected"
        with self.assertRaisesRegex(PacketDecodeError, "length"):
            Packet.from_bytes(encoded)


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
