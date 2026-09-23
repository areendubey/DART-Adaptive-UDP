"""
Run:  python -m unittest -v test_sr

Level 1: ReorderBuffer logic (no network)          -> "is my buffering correct?"
Level 2: end-to-end over localhost with faults      -> "does the whole file arrive intact?"
Every end-to-end test compares SHA-256 of source and received file (same check as the final demo).
"""
import hashlib
import os
import random
import tempfile
import unittest

from config import Config
from dev_channel import run_local_transfer
from protocol import DATA, Packet
from rto import JacobsonKarelsRTO
from selective_repeat import ReorderBuffer

FAST = Config(initial_rto=0.03, min_rto=0.01, linger=0.3, window_size=8, max_retries=200)


def sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def pkt(seq):
    return Packet(DATA, seq=seq, payload=bytes([seq]))


class TestPacket(unittest.TestCase):
    def test_roundtrip_and_corruption(self):
        p = Packet(DATA, seq=7, payload=b"hello")
        self.assertEqual(Packet.decode(p.encode()), p)
        bad = bytearray(p.encode())
        bad[-1] ^= 1
        self.assertIsNone(Packet.decode(bytes(bad)))
        self.assertIsNone(Packet.decode(b"short"))


class TestReorderBuffer(unittest.TestCase):
    def test_in_order(self):
        b = ReorderBuffer(4)
        status, out = b.offer(pkt(0))
        self.assertEqual((status, [p.seq for p in out]), ("in_order", [0]))

    def test_gap_is_buffered_then_released(self):
        b = ReorderBuffer(4)
        self.assertEqual(b.offer(pkt(2))[0], "buffered")
        self.assertEqual(b.offer(pkt(1))[0], "buffered")
        status, out = b.offer(pkt(0))                       # gap filled -> 0,1,2 come out together
        self.assertEqual((status, [p.seq for p in out]), ("in_order", [0, 1, 2]))
        self.assertEqual(b.base, 3)

    def test_duplicates_and_window_limit(self):
        b = ReorderBuffer(2)
        b.offer(pkt(0))
        self.assertEqual(b.offer(pkt(0))[0], "duplicate")   # already delivered
        b.offer(pkt(2))
        self.assertEqual(b.offer(pkt(2))[0], "duplicate")   # already buffered
        self.assertEqual(b.offer(pkt(5))[0], "out_of_window")


class TestEndToEnd(unittest.TestCase):
    def transfer(self, size, seed=1, cfg=FAST, **kw):
        with tempfile.TemporaryDirectory() as d:
            src, dst = os.path.join(d, "src.bin"), os.path.join(d, "dst.bin")
            with open(src, "wb") as f:
                f.write(random.Random(42).randbytes(size))
            tx, rx = run_local_transfer(src, dst, cfg, seed=seed, **kw)
            self.assertEqual(sha256(src), sha256(dst), "file corrupted or incomplete")
            return tx, rx

    def test_clean_channel(self):
        tx, _ = self.transfer(50_000)
        self.assertEqual(tx.retransmissions, 0)

    def test_empty_file_and_odd_size(self):
        self.transfer(0)
        self.transfer(1024 * 5 + 17)

    def test_loss(self):
        tx, _ = self.transfer(60_000, loss=0.2)
        self.assertGreater(tx.retransmissions, 0)

    def test_duplication_corruption_reordering(self):
        _, rx = self.transfer(60_000, dup=0.2, corrupt=0.1, reorder=0.3)
        self.assertGreater(rx.duplicates + rx.corrupt + rx.out_of_order, 0)

    def test_everything_at_once(self):
        self.transfer(60_000, loss=0.15, dup=0.1, corrupt=0.05, reorder=0.2)

    def test_window_of_one_still_works(self):               # behaves like Stop-and-Wait
        self.transfer(20_000, window=1, loss=0.1)

    def test_adaptive_rto(self):
        self.transfer(40_000, rto=JacobsonKarelsRTO(0.05, 0.01, 0.5), loss=0.1)


if __name__ == "__main__":
    unittest.main()
