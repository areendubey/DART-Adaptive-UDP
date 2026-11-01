"""Go-Back-N protocol correctness on a lossless channel."""

import hashlib
import unittest

from dart.gbn import seq_add, seq_le, seq_lt
from tests.harness import run_gbn_transfer


class SequenceSpaceTests(unittest.TestCase):
    def test_order(self) -> None:
        self.assertTrue(seq_lt(1, 2))
        self.assertTrue(seq_le(2, 2))
        self.assertFalse(seq_lt(2, 1))
        self.assertEqual(seq_add(2**32 - 1), 0)


class GBNCorrectnessTests(unittest.TestCase):
    def test_empty_payload(self) -> None:
        received, stats, rx = run_gbn_transfer(b"", window_size=4, mss=32)
        self.assertEqual(received, b"")
        self.assertEqual(stats.sha256, hashlib.sha256(b"").hexdigest())
        self.assertEqual(rx.sha256, stats.sha256)
        self.assertEqual(stats.packets_retransmitted, 0)

    def test_single_packet(self) -> None:
        payload = b"one-window-unit"
        received, stats, rx = run_gbn_transfer(payload, window_size=4, mss=64)
        self.assertEqual(received, payload)
        self.assertEqual(rx.sha256, stats.sha256)
        self.assertEqual(stats.bytes_payload, len(payload))
        self.assertEqual(stats.data_packets_sent, 1)
        self.assertEqual(stats.packets_retransmitted, 0)

    def test_multi_window_file(self) -> None:
        payload = bytes((i * 7) % 256 for i in range(5000))
        received, stats, rx = run_gbn_transfer(
            payload, window_size=8, mss=200, timeout=0.4
        )
        self.assertEqual(received, payload)
        self.assertEqual(rx.sha256, hashlib.sha256(payload).hexdigest())
        self.assertGreater(stats.data_packets_sent, 8)
        self.assertEqual(stats.packets_retransmitted, 0)

    def test_window_one_still_reliable(self) -> None:
        payload = b"abcdefghijklmnopqrstuvwxyz" * 20
        received, stats, _rx = run_gbn_transfer(
            payload, window_size=1, mss=16, timeout=0.3
        )
        self.assertEqual(received, payload)
        self.assertEqual(stats.window_size, 1)

    def test_integrity_hash_matches(self) -> None:
        payload = b"DART-GBN-integrity-check\n" * 100
        received, stats, rx = run_gbn_transfer(payload, window_size=6, mss=40)
        self.assertEqual(received, payload)
        self.assertEqual(stats.sha256, rx.sha256)
        self.assertEqual(len(stats.sha256), 64)


if __name__ == "__main__":
    unittest.main()
