"""
test_selective_repeat.py -- unit + integration tests for selective_repeat.py
Run (from the folder containing all project files):
    python -m unittest -v test_selective_repeat        # or: pytest -v

Layers
  * unit tests            packet format, receiver buffering/SACK, Karn, Stats contract
  * proxy integration     real UDP through UdpProxy (seeded loss/dup/corrupt/reorder/delay)
  * team-module tests     SR driven through dev_channel.run_local_transfer + config + rto
"""
import hashlib
import heapq
import io
import os
import random
import select
import tempfile
import threading
import time
import unittest

from config import Config, Stats
from dev_channel import run_local_transfer
from rto import FixedRTO, JacobsonKarelsRTO, RTOEstimator
from selective_repeat import (
    ACK, DATA, FIN, HDR_LEN, Packet, SRReceiver, SRSender, SRStats,
    TransferError, make_udp_socket, pack, unpack, _Slot,
)


def make_cfg(**kw):
    """Small packets + short timers so tests run fast."""
    base = dict(payload_size=512, window_size=16, initial_rto=0.1, min_rto=0.05,
                max_rto=0.5, idle_timeout=20.0, linger=0.3)
    base.update(kw)
    return Config(**base)


def fast_rto(cfg=None):
    cfg = cfg or make_cfg()
    return JacobsonKarelsRTO(cfg.initial_rto, cfg.min_rto, cfg.max_rto)


def blob(n, seed=0):
    return random.Random(seed).randbytes(n)


class SpyRTO(RTOEstimator):
    """Records every call the ARQ makes to the RTO estimator."""
    def __init__(self, rto=0.1):
        self._rto, self.samples, self.timeouts = rto, [], 0

    def current(self):
        return self._rto

    def on_rtt_sample(self, rtt):
        self.samples.append(rtt)

    def on_timeout(self):
        self.timeouts += 1


# --------------------------------------------------------------------------
# Channel emulator: UDP proxy between sender and receiver (both directions)
# --------------------------------------------------------------------------
class UdpProxy(threading.Thread):
    def __init__(self, target, *, loss=0.0, dup=0.0, corrupt=0.0, reorder=0.0,
                 delay=0.0, jitter=0.0, reorder_extra=0.01, seed=1):
        super().__init__(daemon=True)
        self.sock = make_udp_socket(("127.0.0.1", 0))
        self.sock.setblocking(False)
        self.addr = self.sock.getsockname()
        self.target, self.client = target, None
        self.loss, self.dup, self.corrupt, self.reorder = loss, dup, corrupt, reorder
        self.delay, self.jitter, self.reorder_extra = delay, jitter, reorder_extra
        self.rng = random.Random(seed)
        self.q, self._n = [], 0
        self.dropped = self.corrupted = self.duplicated = 0
        self._halt = threading.Event()

    def stop(self):
        self._halt.set()
        self.join(2)
        self.sock.close()

    def _ingest(self, data, src, now):
        if src == self.target:
            dst = self.client
        else:
            self.client, dst = src, self.target
        if dst is None:
            return
        r = self.rng
        if r.random() < self.loss:
            self.dropped += 1
            return
        copies = 1
        if r.random() < self.dup:
            copies, self.duplicated = 2, self.duplicated + 1
        for _ in range(copies):
            d = data
            if r.random() < self.corrupt:
                b = bytearray(d)
                b[r.randrange(len(b))] ^= 0xFF
                d, self.corrupted = bytes(b), self.corrupted + 1
            delay = self.delay + r.uniform(0, self.jitter)
            if r.random() < self.reorder:
                delay += self.reorder_extra
            heapq.heappush(self.q, (now + delay, self._n, dst, d))
            self._n += 1

    def run(self):
        s, q = self.sock, self.q
        while not self._halt.is_set():
            now = time.monotonic()
            timeout = 0.02 if not q else max(0.0, min(0.02, q[0][0] - now))
            ready, _, _ = select.select([s], [], [], timeout)
            now = time.monotonic()
            if ready:
                while True:
                    try:
                        data, src = s.recvfrom(65535)
                    except (BlockingIOError, ConnectionError):
                        break
                    except OSError:
                        return
                    self._ingest(data, src, now)
            now = time.monotonic()
            while q and q[0][0] <= now:
                _, _, dst, data = heapq.heappop(q)
                try:
                    s.sendto(data, dst)
                except OSError:
                    pass


def transfer(data, *, cfg=None, rto=None, controller=None, seed=1,
             sender_kw=None, **impair):
    """Send `data` through the emulator -> (received_bytes, sender_stats, receiver_stats, proxy)."""
    cfg = cfg or make_cfg()
    rsock = make_udp_socket(("127.0.0.1", 0))
    proxy = UdpProxy(rsock.getsockname(), seed=seed, **impair)
    proxy.start()
    ssock = make_udp_socket()
    out, box = io.BytesIO(), {}

    def rx():
        try:
            box["stats"] = SRReceiver(rsock, cfg).receive_stream(out)
        except Exception as e:                      # surfaced in the main thread
            box["err"] = e

    t = threading.Thread(target=rx, daemon=True)
    t.start()
    sender = SRSender(ssock, proxy.addr, cfg, rto or fast_rto(cfg), controller=controller,
                      **(sender_kw or {}))
    try:
        sstats = sender.send_stream(io.BytesIO(data))
        t.join(20)
    finally:
        proxy.stop()
        ssock.close()
        rsock.close()
    if "err" in box:
        raise box["err"]
    return out.getvalue(), sstats, box["stats"], proxy


# --------------------------------------------------------------------------
# Unit tests
# --------------------------------------------------------------------------
class PacketTests(unittest.TestCase):
    def test_roundtrip(self):
        p = unpack(pack(DATA, seq=7, ack=3, echo=5, sack=0b101, payload=b"hello"))
        self.assertEqual((p.type, p.seq, p.ack, p.echo, p.sack, p.payload),
                         (DATA, 7, 3, 5, 0b101, b"hello"))

    def test_empty_payload(self):
        self.assertEqual(unpack(pack(ACK, ack=9)).ack, 9)

    def test_every_single_byte_corruption_detected(self):
        wire = pack(DATA, seq=1, payload=b"x" * 40)
        for i in range(len(wire)):
            bad = bytearray(wire)
            bad[i] ^= 0x01
            self.assertIsNone(unpack(bytes(bad)), f"byte {i} corruption undetected")

    def test_truncated_and_garbage(self):
        wire = pack(DATA, seq=1, payload=b"abc")
        self.assertIsNone(unpack(wire[:-1]))
        self.assertIsNone(unpack(wire[:HDR_LEN - 1]))
        self.assertIsNone(unpack(b"\x00" * 64))


class FakeSock:
    def __init__(self):
        self.sent = []

    def sendto(self, data, addr):
        self.sent.append(unpack(data))


class ReceiverBufferingTests(unittest.TestCase):
    def setUp(self):
        self.sock, self.out = FakeSock(), io.BytesIO()
        self.rx = SRReceiver(self.sock, make_cfg(max_window=8))
        self.rx._reset(self.out)
        self.rx.peer = ("peer", 1)

    def feed(self, seq, payload=None):
        self.rx._on_data(unpack(pack(DATA, seq=seq, payload=payload or bytes([seq]))))
        return self.sock.sent[-1]

    def test_in_order(self):
        self.feed(0); a = self.feed(1)
        self.assertEqual((a.ack, a.echo, a.sack), (2, 1, 0))
        self.assertEqual(self.out.getvalue(), b"\x00\x01")
        self.assertEqual(self.rx.stats.bytes_delivered, 2)

    def test_out_of_order_buffer_sack_and_flush(self):
        a = self.feed(2)
        self.assertEqual((a.ack, a.echo, a.sack), (0, 2, 0b10))   # seq 2 = base+1+1
        a = self.feed(1)
        self.assertEqual(a.sack, 0b11)
        self.assertEqual(self.out.getvalue(), b"")
        a = self.feed(0)
        self.assertEqual((a.ack, a.sack), (3, 0))
        self.assertEqual(self.out.getvalue(), b"\x00\x01\x02")
        self.assertEqual(self.rx.stats.max_buffered, 2)
        self.assertEqual(self.rx.stats.out_of_order, 2)

    def test_duplicates_not_rewritten_but_still_acked(self):
        self.feed(0); self.feed(0); self.feed(3); a = self.feed(3)
        self.assertEqual(self.out.getvalue(), b"\x00")
        self.assertEqual(self.rx.stats.duplicates, 2)
        self.assertEqual(a.ack, 1)

    def test_out_of_window_dropped(self):
        self.feed(8)                                  # window = max_window = 8
        self.assertEqual(self.rx.stats.out_of_window, 1)
        self.assertEqual(self.rx.buf, {})

    def test_sack_scan_path_for_large_buffers(self):
        rx = SRReceiver(self.sock, make_cfg(max_window=500))
        rx._reset(io.BytesIO()); rx.peer = ("p", 1)
        for s in range(1, 100, 2):                    # 50 buffered (< 64 -> dict scan)
            rx._on_data(unpack(pack(DATA, seq=s, payload=b"x")))
        for s in range(100, 130):                     # > 64 buffered -> range scan
            rx._on_data(unpack(pack(DATA, seq=s, payload=b"x")))
        self.assertGreaterEqual(len(rx.buf), 64)
        expect = sum(1 << d for d in range(64) if (1 + d) in rx.buf)
        self.assertEqual(self.sock.sent[-1].sack, expect)

    def test_fin_with_missing_data_is_not_complete(self):
        self.feed(0)
        self.assertFalse(self.rx._on_fin(unpack(pack(FIN, seq=5, payload=b"\0" * 32))))

    def test_fin_digest_verified(self):
        self.feed(0, b"abc")
        good = hashlib.sha256(b"abc").digest()
        self.assertTrue(self.rx._on_fin(unpack(pack(FIN, seq=1, payload=good))))
        self.assertTrue(self.rx.stats.verified)
        self.rx.stats.verified = False
        self.assertTrue(self.rx._on_fin(unpack(pack(FIN, seq=1, payload=b"\0" * 32))))
        self.assertFalse(self.rx.stats.verified)


class TeamContractTests(unittest.TestCase):
    """SR must honour the shared Config / Stats / RTOEstimator contracts."""

    def test_window_clamped_to_config_bounds(self):
        s = SRSender(FakeSock(), ("x", 1), Config(window_size=8, min_window=2, max_window=20))
        self.assertEqual(s.window_size, 8)
        s.window_size = 3
        self.assertEqual(s.window, 3)                 # alias
        s.window_size = 0
        self.assertEqual(s.window_size, 2)            # min_window
        s.window_size = 999
        self.assertEqual(s.window_size, 20)           # max_window

    def test_defaults_without_cfg_and_rto(self):
        s = SRSender(FakeSock(), ("x", 1))
        self.assertEqual(s.window_size, Config().window_size)
        self.assertGreater(s.rto.current(), 0)

    def test_stats_is_team_stats_with_identical_row(self):
        _, s, r, _ = transfer(blob(5000))
        for st in (s, r):
            self.assertIsInstance(st, Stats)
            self.assertIsInstance(st, SRStats)
            self.assertEqual(set(st.to_row()), set(Stats().to_row()))   # same CSV columns
            self.assertTrue(set(st.to_row()) < set(st.extended_row()))

    def test_stats_field_meaning(self):
        data = blob(40_000)
        _, s, r, _ = transfer(data, loss=0.1, seed=21)
        self.assertEqual(s.bytes_delivered, len(data))      # sender: bytes acked
        self.assertEqual(r.bytes_delivered, len(data))      # receiver: bytes written
        self.assertEqual(s.packets_sent, -(-len(data) // 512) + s.retransmissions)
        self.assertEqual(s.timeouts + s.fast_retransmits, s.retransmissions)
        self.assertGreater(s.duration, 0)
        self.assertGreater(s.goodput_kbps, 0)
        self.assertTrue(all(x > 0 for x in s.rtt_samples))
        self.assertTrue(all(len(p) == 2 for p in s.window_trace))   # (t, window)
        self.assertEqual(s.window_trace[0][1], 16)

    def test_karn_no_sample_from_retransmitted_packet(self):
        spy = SpyRTO()
        s = SRSender(FakeSock(), ("x", 1), make_cfg(), spy)
        s._init_state()
        for seq, retries in ((0, 1), (1, 0)):
            slot = _Slot(seq, b"", 10)
            slot.retries, slot.sent_at = retries, 1.0
            s._inflight[seq] = slot
        s._nxt = 2
        s._on_ack(Packet(ACK, 0, 0, 0, 0, b""), 1.5)     # echo of seq 0, retransmitted
        self.assertEqual(spy.samples, [])
        s._on_ack(Packet(ACK, 0, 1, 1, 0, b""), 1.5)     # echo of seq 1, clean
        self.assertEqual(spy.samples, [0.5])

    def test_rto_backoff_once_per_timeout_event(self):
        spy = SpyRTO()
        s = SRSender(FakeSock(), ("x", 1), make_cfg(), spy)
        s._init_state()
        for seq in range(5):                              # five packets expire together
            slot = _Slot(seq, pack(DATA, seq=seq, payload=b"x"), 1)
            slot.deadline = 1.0
            s._inflight[seq] = slot
            heapq.heappush(s._heap, (1.0, seq))
        s._nxt = 5
        s._expire(2.0)
        self.assertEqual(spy.timeouts, 1)                 # one backoff, not five
        self.assertEqual(s.stats.timeouts, 5)
        self.assertEqual(s.stats.retransmissions, 5)

    def test_duplicate_ack_not_counted_as_new(self):
        s = SRSender(FakeSock(), ("x", 1), make_cfg(), SpyRTO())
        s._init_state()
        slot = _Slot(0, b"", 10)
        s._inflight[0] = slot
        s._nxt = 1
        s._on_ack(Packet(ACK, 1, 0, 0, 0, b""), 1.0)
        s._on_ack(Packet(ACK, 1, 0, 0, 0, b""), 1.1)      # duplicate
        self.assertEqual(s.stats.acks_received, 1)
        self.assertEqual(s.stats.bytes_delivered, 10)


# --------------------------------------------------------------------------
# Integration tests (real UDP through the emulator)
# --------------------------------------------------------------------------
class TransferTests(unittest.TestCase):
    def check(self, data, **kw):
        got, s, r, proxy = transfer(data, **kw)
        self.assertEqual(hashlib.sha256(got).digest(), hashlib.sha256(data).digest())
        self.assertTrue(s.fin_confirmed)
        self.assertTrue(s.verified and r.verified)
        return s, r, proxy

    def test_clean_channel(self):
        s, r, _ = self.check(blob(60_000))
        self.assertEqual(s.retransmissions, 0)
        self.assertEqual(r.duplicates, 0)

    def test_edge_sizes(self):
        for n in (0, 1, 511, 512, 513, 5 * 512):
            with self.subTest(size=n):
                self.check(blob(n, seed=n))

    def test_window_one_degenerates_to_stop_and_wait(self):
        s, _, _ = self.check(blob(10_000), cfg=make_cfg(window_size=1))
        self.assertEqual(s.max_inflight, 1)

    def test_moderate_loss(self):
        s, _, p = self.check(blob(100_000), loss=0.10, seed=3)
        self.assertGreater(p.dropped, 0)
        self.assertGreater(s.retransmissions, 0)

    def test_retransmissions_are_selective(self):
        s, _, _ = self.check(blob(100_000), loss=0.05, seed=4)
        self.assertLess(s.retransmissions, 0.4 * s.packets_sent)

    def test_high_loss(self):
        self.check(blob(30_000), loss=0.25, seed=5, cfg=make_cfg(window_size=8))

    def test_duplication_and_corruption(self):
        s, r, p = self.check(blob(60_000), dup=0.2, corrupt=0.1, seed=6)
        self.assertGreater(p.corrupted, 0)
        self.assertGreater(r.corrupt + s.corrupt, 0)       # CRC rejected them

    def test_reordering_and_jitter(self):
        _, r, _ = self.check(blob(80_000), reorder=0.3, reorder_extra=0.02,
                             jitter=0.005, delay=0.005, seed=7)
        self.assertGreater(r.out_of_order, 0)

    def test_everything_at_once(self):
        self.check(blob(60_000), loss=0.08, dup=0.1, corrupt=0.05, reorder=0.2,
                   jitter=0.004, seed=8)

    def test_window_never_exceeded(self):
        for w in (2, 8, 32):
            with self.subTest(window=w):
                s, _, _ = self.check(blob(50_000), cfg=make_cfg(window_size=w),
                                     loss=0.05, seed=9)
                self.assertLessEqual(s.max_inflight, w)

    def test_window_larger_than_sack_bitmap(self):
        # 100 > 64 SACK bits: echo ACKs keep it correct
        self.check(blob(120_000, 1), cfg=make_cfg(window_size=100, max_window=128),
                   loss=0.05, seed=10)

    def test_fast_retransmit_toggle(self):
        s, _, _ = self.check(blob(60_000), sender_kw=dict(dupthresh=None), loss=0.08, seed=11)
        self.assertEqual(s.fast_retransmits, 0)

    def test_fixed_rto_estimator(self):
        est = FixedRTO(0.08)
        self.check(blob(40_000), loss=0.1, seed=12, rto=est)
        self.assertEqual(est.current(), 0.08)

    def test_jacobson_karels_learns_channel_delay(self):
        est = JacobsonKarelsRTO(0.3, 0.02, 1.0)
        s, _, _ = self.check(blob(40_000), delay=0.02, seed=13, rto=est)
        self.assertGreater(len(s.rtt_samples), 5)
        self.assertGreater(est.srtt, 0.03)                 # ~40 ms round trip
        self.assertLess(est.srtt, 0.2)
        self.assertGreater(s.avg_rtt_ms, 30)

    def test_controller_can_change_window_midflight(self):
        class Ctl:
            n = 0
            def on_ack(self, snd, sample):
                self.n += 1
                if self.n == 30:
                    snd.window_size = 4
                elif self.n == 60:
                    snd.window_size = 32
            def on_loss(self, snd, kind):
                pass

        s, _, _ = self.check(blob(80_000), cfg=make_cfg(max_window=32), loss=0.03,
                             seed=14, controller=Ctl())
        self.assertTrue({4, 32} <= {w for _, w in s.window_trace})
        self.assertLessEqual(s.max_inflight, 32)

    def test_dead_peer_raises(self):
        dead = make_udp_socket(("127.0.0.1", 0))
        ssock = make_udp_socket()
        cfg = make_cfg(window_size=2, max_retries=3, initial_rto=0.02, min_rto=0.02, max_rto=0.05)
        snd = SRSender(ssock, dead.getsockname(), cfg, FixedRTO(0.02))
        try:
            with self.assertRaises(TransferError):
                snd.send_bytes(blob(2000))
        finally:
            ssock.close()
            dead.close()


# --------------------------------------------------------------------------
# Through the team's dev_channel (FaultySocket 3-method interface)
# --------------------------------------------------------------------------
class DevChannelTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "src.bin")
        self.dst = os.path.join(self.tmp.name, "dst.bin")
        self.data = blob(60_000, 5)
        with open(self.src, "wb") as f:
            f.write(self.data)

    def tearDown(self):
        self.tmp.cleanup()

    def run_it(self, **kw):
        s, r = run_local_transfer(self.src, self.dst, **kw)
        with open(self.dst, "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).digest(),
                             hashlib.sha256(self.data).digest())
        self.assertEqual(s.bytes_delivered, len(self.data))
        self.assertEqual(r.bytes_delivered, len(self.data))
        self.assertIsInstance(s.to_row(), dict)
        return s, r

    def test_clean(self):
        s, r = self.run_it(cfg=Config(window_size=8, initial_rto=0.1), seed=1)
        self.assertEqual(s.retransmissions, 0)
        self.assertTrue(s.verified and r.verified)

    def test_all_faults_all_rto_estimators(self):
        cfg = Config(window_size=8, initial_rto=0.1, linger=0.5)
        for name, est in (("fixed", FixedRTO(0.1)),
                          ("jk", JacobsonKarelsRTO(0.1, cfg.min_rto, cfg.max_rto)),
                          ("default", None)):
            with self.subTest(rto=name):
                self.run_it(cfg=cfg, rto=est, seed=2,
                            loss=0.1, dup=0.05, corrupt=0.05, reorder=0.1)

    def test_window_override_and_trace(self):
        s, _ = self.run_it(cfg=Config(window_size=16, initial_rto=0.1), window=4,
                           seed=3, loss=0.05)
        self.assertLessEqual(s.max_inflight, 4)
        self.assertEqual(s.window_trace[0][1], 4)

    def test_seeded_runs_are_reproducible_in_fault_pattern(self):
        cfg = Config(window_size=8, initial_rto=0.1)
        a, _ = self.run_it(cfg=cfg, seed=7, loss=0.1)
        b, _ = self.run_it(cfg=cfg, seed=7, loss=0.1)
        self.assertEqual(a.bytes_delivered, b.bytes_delivered)


if __name__ == "__main__":
    unittest.main(verbosity=2)
