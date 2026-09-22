import unittest

from dart_transport.packet import HEADER_SIZE, Packet, PacketDecodeError, PacketFlag, PacketType


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
