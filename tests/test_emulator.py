"""
Unit Tests for DART Network Channel Emulator
Author: Kartikey Singh (Primary Owner)
"""

import unittest
from dart.emulator.channel import NetworkChannelEmulator, EmulatorConfig
from dart.packet import DARTPacket


class TestChannelEmulator(unittest.TestCase):

    def test_deterministic_seed_reproducibility(self):
        """Validates that identical seeds produce identical drop/corruption sequences."""
        config1 = EmulatorConfig(loss_rate=0.3, corruption_rate=0.2, seed=12345)
        config2 = EmulatorConfig(loss_rate=0.3, corruption_rate=0.2, seed=12345)

        emu1 = NetworkChannelEmulator(config1)
        emu2 = NetworkChannelEmulator(config2)

        dummy_packet = DARTPacket.create_data(seq_num=1, data=b"Determinism check").pack()

        results1 = [emu1.process_packet(dummy_packet) for _ in range(50)]
        results2 = [emu2.process_packet(dummy_packet) for _ in range(50)]

        for r1, r2 in zip(results1, results2):
            self.assertEqual(len(r1), len(r2))
            if r1:
                # Compare delivered bytes and delays
                self.assertEqual(r1[0][0], r2[0][0])
                self.assertEqual(r1[0][1], r2[0][1])

    def test_loss_rate_zero(self):
        """Loss rate 0.0 should never drop packets."""
        emu = NetworkChannelEmulator(EmulatorConfig(loss_rate=0.0, seed=42))
        dummy = b"test packet"
        for _ in range(100):
            res = emu.process_packet(dummy)
            self.assertEqual(len(res), 1)
        self.assertEqual(emu.stats.packets_dropped, 0)

    def test_loss_rate_hundred_percent(self):
        """Loss rate 1.0 should drop 100% of packets."""
        emu = NetworkChannelEmulator(EmulatorConfig(loss_rate=1.0, seed=42))
        dummy = b"test packet"
        for _ in range(50):
            res = emu.process_packet(dummy)
            self.assertEqual(len(res), 0)
        self.assertEqual(emu.stats.packets_dropped, 50)

    def test_corruption_triggers_checksum_error(self):
        """Corrupted packets must fail unpack checksum validation."""
        emu = NetworkChannelEmulator(EmulatorConfig(corruption_rate=1.0, seed=42))
        pkt = DARTPacket.create_data(seq_num=7, data=b"integrity test").pack()

        deliverables = emu.process_packet(pkt)
        self.assertEqual(len(deliverables), 1)
        corrupted_bytes, _ = deliverables[0]

        with self.assertRaises(ValueError):
            DARTPacket.unpack(corrupted_bytes)

    def test_duplication(self):
        """Duplication rate 1.0 should deliver 2 packets."""
        emu = NetworkChannelEmulator(EmulatorConfig(duplication_rate=1.0, seed=42))
        deliverables = emu.process_packet(b"dup test")
        self.assertEqual(len(deliverables), 2)
        self.assertEqual(emu.stats.packets_duplicated, 1)

    def test_reordering(self):
        """Reordering should hold back packet and deliver with subsequent packet."""
        emu = NetworkChannelEmulator(EmulatorConfig(reorder_rate=1.0, seed=42))
        p1 = b"packet 1"
        p2 = b"packet 2"

        # First packet should be held in reorder buffer
        res1 = emu.process_packet(p1)
        self.assertEqual(len(res1), 0)

        # Second packet should release p2 first, then p1
        res2 = emu.process_packet(p2)
        self.assertEqual(len(res2), 2)
        self.assertEqual(res2[0][0], p2)
        self.assertEqual(res2[1][0], p1)


if __name__ == "__main__":
    unittest.main()
