"""Tests for the four integration changes:

  1. Dynamic RTO (Jacobson/Karels + Karn)
  2. Unified packet header      (see test_gbn_packet.py)
  3. ReliabilityController hooks (P3 Static and an adaptive-style controller)
  4. Channel emulator swap point (tests/harness.py: make_channel)
"""

import socket
import unittest

from dart.compat import Packet, PacketType, ReliabilityController, RttEstimator
from dart.constants import RTO_MAX_S, RTO_MIN_S
from dart.gbn import GoBackNReceiver, GoBackNSender, _now_us
from tests.harness import run_gbn_transfer


# ---------------------------------------------------------------------------
# 1a. RttEstimator on its own
# ---------------------------------------------------------------------------
class RttEstimatorTests(unittest.TestCase):
    def test_initial_rto_is_used_before_any_sample(self) -> None:
        est = RttEstimator(initial_rto=0.5)
        self.assertEqual(est.rto, 0.5)
        self.assertIsNone(est.srtt)

    def test_first_sample(self) -> None:
        est = RttEstimator(initial_rto=1.0, min_rto=0.01, max_rto=10.0)
        est.update(0.2)
        self.assertAlmostEqual(est.srtt, 0.2)
        self.assertAlmostEqual(est.rttvar, 0.1)
        self.assertAlmostEqual(est.rto, 0.2 + 4 * 0.1)   # 0.6

    def test_second_sample_follows_rfc6298(self) -> None:
        est = RttEstimator(initial_rto=1.0, min_rto=0.01, max_rto=10.0)
        est.update(0.2)
        est.update(0.4)
        rttvar = 0.75 * 0.1 + 0.25 * abs(0.2 - 0.4)       # 0.125
        srtt = 0.875 * 0.2 + 0.125 * 0.4                  # 0.225
        self.assertAlmostEqual(est.rttvar, rttvar)
        self.assertAlmostEqual(est.srtt, srtt)
        self.assertAlmostEqual(est.rto, srtt + 4 * rttvar)

    def test_rto_is_clamped(self) -> None:
        est = RttEstimator(initial_rto=1.0, min_rto=0.1, max_rto=2.0)
        est.update(0.0001)
        self.assertEqual(est.rto, 0.1)                     # lower clamp
        big = RttEstimator(initial_rto=1.0, min_rto=0.1, max_rto=2.0)
        big.update(5.0)
        self.assertEqual(big.rto, 2.0)                     # upper clamp

    def test_timeout_doubles_rto_up_to_max(self) -> None:
        est = RttEstimator(initial_rto=0.3, min_rto=0.1, max_rto=1.0)
        est.on_timeout()
        self.assertAlmostEqual(est.rto, 0.6)
        est.on_timeout()
        self.assertEqual(est.rto, 1.0)
        est.on_timeout()
        self.assertEqual(est.rto, 1.0)

    def test_reset_backoff_goes_back_to_estimate(self) -> None:
        est = RttEstimator(initial_rto=1.0, min_rto=0.01, max_rto=10.0)
        est.update(0.2)
        normal = est.rto
        est.on_timeout()
        est.on_timeout()
        self.assertGreater(est.rto, normal)
        est.reset_backoff()
        self.assertAlmostEqual(est.rto, normal)

    def test_non_positive_sample_ignored(self) -> None:
        est = RttEstimator(initial_rto=0.5)
        est.update(0.0)
        est.update(-1.0)
        self.assertIsNone(est.srtt)

    def test_invalid_arguments(self) -> None:
        with self.assertRaises(ValueError):
            RttEstimator(initial_rto=0)
        with self.assertRaises(ValueError):
            RttEstimator(min_rto=1.0, max_rto=0.5)


# ---------------------------------------------------------------------------
# 1b. Sender uses the estimator and obeys Karn's algorithm
# ---------------------------------------------------------------------------
class SenderRttTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.rx_raw.bind(("127.0.0.1", 0))
        self.tx_raw.bind(("127.0.0.1", 0))
        self.dest = self.rx_raw.getsockname()

    def tearDown(self) -> None:
        self.rx_raw.close()
        self.tx_raw.close()

    def make_sender(self) -> GoBackNSender:
        est = RttEstimator(initial_rto=0.4, min_rto=0.01, max_rto=5.0)
        return GoBackNSender(self.tx_raw, self.dest, window_size=4, timeout=0.4,
                             mss=8, estimator=est)

    def test_clean_ack_gives_an_rtt_sample(self) -> None:
        sender = self.make_sender()
        sender._send_data(0, b"a")
        sender._nextseq = 1
        # pretend the receiver echoed a stamp taken 20 ms ago
        sender._handle_cumulative_ack(0, _now_us() - 20_000)
        self.assertEqual(sender.stats.rtt_samples, 1)
        self.assertEqual(sender.estimator.samples, 1)
        self.assertGreaterEqual(sender.estimator.srtt, 0.02)

    def test_karn_no_sample_from_retransmitted_packet(self) -> None:
        sender = self.make_sender()
        sender._send_data(0, b"a")
        sender._nextseq = 1
        sender._on_timer_expired()                      # seq 0 is now retransmitted
        sender._handle_cumulative_ack(0, _now_us() - 20_000)
        self.assertEqual(sender.stats.rtt_samples, 0)
        self.assertEqual(sender.stats.rtt_samples_skipped, 1)
        self.assertEqual(sender.estimator.samples, 0)

    def test_ack_without_timestamp_gives_no_sample(self) -> None:
        sender = self.make_sender()
        sender._send_data(0, b"a")
        sender._nextseq = 1
        sender._handle_cumulative_ack(0, 0)
        self.assertEqual(sender.stats.rtt_samples, 0)
        self.assertEqual(sender.stats.rtt_samples_skipped, 0)

    def test_timeout_backs_rto_off_and_progress_undoes_it(self) -> None:
        sender = self.make_sender()
        sender._send_data(0, b"a")
        sender._send_data(1, b"b")
        sender._nextseq = 2
        before = sender.estimator.rto
        sender._on_timer_expired()
        self.assertAlmostEqual(sender.estimator.rto, before * 2)
        self.assertEqual(sender.stats.timeouts, 1)
        sender._handle_cumulative_ack(0, 0)             # window moves again
        self.assertAlmostEqual(sender.estimator.rto, before)

    def test_timer_stops_when_everything_acked(self) -> None:
        sender = self.make_sender()
        sender._send_data(0, b"a")
        sender._nextseq = 1
        self.assertIsNotNone(sender._timer_deadline)
        sender._handle_cumulative_ack(0, 0)
        self.assertIsNone(sender._timer_deadline)

    def test_duplicate_ack_does_not_restart_timer(self) -> None:
        sender = self.make_sender()
        sender._send_data(0, b"a")
        sender._send_data(1, b"b")
        sender._nextseq = 2
        sender._handle_cumulative_ack(0, 0)
        deadline = sender._timer_deadline
        sender._handle_cumulative_ack(0, 0)             # duplicate
        self.assertEqual(sender._timer_deadline, deadline)

    def test_receiver_echoes_timestamp_and_reack_has_none(self) -> None:
        receiver = GoBackNReceiver(self.rx_raw)
        self.tx_raw.settimeout(2.0)
        self.rx_raw.settimeout(2.0)
        sender_addr = self.tx_raw.getsockname()
        # feed the receiver by hand: one DATA with timestamp, then an old duplicate
        self.tx_raw.sendto(Packet(PacketType.DATA, 0, 0, b"x", timestamp=777).encode(), self.dest)
        raw, addr = self.rx_raw.recvfrom(65535)
        receiver._peer = addr
        pkt = Packet.decode(raw)
        receiver._ack(pkt.seq, pkt.timestamp)
        ack = Packet.decode(self.tx_raw.recvfrom(65535)[0])
        self.assertTrue(ack.is_ack())
        self.assertEqual(ack.ack, 0)
        self.assertEqual(ack.timestamp, 777)
        receiver._reack()
        again = Packet.decode(self.tx_raw.recvfrom(65535)[0])
        self.assertEqual(again.timestamp, 0)
        self.assertEqual(sender_addr, addr)


class SenderRtoEndToEndTests(unittest.TestCase):
    def test_rto_adapts_down_on_a_fast_path(self) -> None:
        payload = bytes(range(256)) * 20
        received, stats, _rx = run_gbn_transfer(payload, window_size=8, mss=200,
                                                timeout=1.5)
        self.assertEqual(received, payload)
        self.assertGreater(stats.rtt_samples, 0)
        self.assertLess(stats.srtt_s, 0.05)             # loopback RTT is tiny
        self.assertLess(stats.final_rto_s, 1.5)         # moved away from the initial RTO
        self.assertGreaterEqual(stats.final_rto_s, RTO_MIN_S)
        self.assertEqual(stats.timeouts, 0)

    def test_rto_stays_inside_limits_under_loss(self) -> None:
        payload = b"loss-and-rto" * 60
        received, stats, _rx = run_gbn_transfer(payload, window_size=4, mss=32,
                                                timeout=0.12, loss_rate=0.2, seed=5,
                                                idle_timeout=30.0)
        self.assertEqual(received, payload)
        self.assertGreaterEqual(stats.final_rto_s, RTO_MIN_S)
        self.assertLessEqual(stats.final_rto_s, RTO_MAX_S)

    def test_karn_skips_happen_under_loss(self) -> None:
        payload = b"karn" * 400
        received, stats, _rx = run_gbn_transfer(payload, window_size=6, mss=32,
                                                timeout=0.12, loss_rate=0.25, seed=3,
                                                idle_timeout=30.0)
        self.assertEqual(received, payload)
        self.assertGreater(stats.packets_retransmitted, 0)
        self.assertGreater(stats.rtt_samples_skipped, 0)


# ---------------------------------------------------------------------------
# 3. ReliabilityController hooks
# ---------------------------------------------------------------------------
class RecordingController(ReliabilityController):
    """Static window, records every hook call."""

    def __init__(self, window_size: int) -> None:
        super().__init__(window_size)
        self.first_sends = 0
        self.resends = 0
        self.acked_total = 0
        self.timeouts = 0
        self.samples = []
        self.max_outstanding = 0

    def on_packet_sent(self, seq, is_retransmit):
        if is_retransmit:
            self.resends += 1
        else:
            self.first_sends += 1
            outstanding = self.first_sends - self.acked_total
            if outstanding > self.max_outstanding:
                self.max_outstanding = outstanding

    def on_acknowledged(self, ack, newly_acked, rtt_sample):
        self.acked_total += newly_acked
        if rtt_sample is not None:
            self.samples.append(rtt_sample)

    def on_timeout(self):
        self.timeouts += 1


class GrowShrinkController(ReliabilityController):
    """Toy 'adaptive mode': +1 per ACK up to a cap, halve on timeout.

    Only used to prove GBN follows a window that changes while running.
    Areen's real adaptive engine plugs into the same three hooks.
    """

    def __init__(self, start: int = 1, cap: int = 12) -> None:
        super().__init__(start)
        self.cap = cap
        self.history = [start]

    def on_acknowledged(self, ack, newly_acked, rtt_sample):
        if self._window_size < self.cap:
            self._window_size += 1
            self.history.append(self._window_size)

    def on_timeout(self):
        new_size = self._window_size // 2
        if new_size < 1:
            new_size = 1
        self._window_size = new_size
        self.history.append(new_size)


class ControllerHookTests(unittest.TestCase):
    def test_static_mode_hooks_count_every_packet(self) -> None:
        ctrl = RecordingController(window_size=5)
        payload = b"h" * 1000
        received, stats, _rx = run_gbn_transfer(payload, window_size=5, mss=100,
                                                controller=ctrl)
        self.assertEqual(received, payload)
        total = 10 + 1                                     # data + FIN
        self.assertEqual(ctrl.first_sends, total)
        self.assertEqual(ctrl.acked_total, total)
        self.assertEqual(ctrl.resends, stats.packets_retransmitted)
        self.assertEqual(ctrl.timeouts, stats.timeouts)

    def test_static_window_is_never_exceeded(self) -> None:
        ctrl = RecordingController(window_size=3)
        payload = b"w" * 3000
        received, _stats, _rx = run_gbn_transfer(payload, window_size=3, mss=100,
                                                 controller=ctrl)
        self.assertEqual(received, payload)
        self.assertLessEqual(ctrl.max_outstanding, 3)

    def test_hooks_see_retransmissions_and_timeouts_under_loss(self) -> None:
        ctrl = RecordingController(window_size=4)
        payload = b"loss-hooks" * 80
        received, stats, _rx = run_gbn_transfer(payload, window_size=4, mss=40,
                                                timeout=0.12, loss_rate=0.25, seed=21,
                                                idle_timeout=30.0, controller=ctrl)
        self.assertEqual(received, payload)
        self.assertGreater(ctrl.timeouts, 0)
        self.assertEqual(ctrl.timeouts, stats.timeouts)
        self.assertEqual(ctrl.resends, stats.packets_retransmitted)
        self.assertEqual(ctrl.acked_total, ctrl.first_sends)   # everything got ACKed once

    def test_rtt_samples_reach_the_controller(self) -> None:
        ctrl = RecordingController(window_size=6)
        run_gbn_transfer(b"s" * 2000, window_size=6, mss=100, controller=ctrl)
        self.assertGreater(len(ctrl.samples), 0)
        for sample in ctrl.samples:
            self.assertGreater(sample, 0)

    def test_default_is_static_mode_with_the_given_window(self) -> None:
        _data, stats, _rx = run_gbn_transfer(b"d" * 500, window_size=7, mss=50)
        self.assertEqual(stats.window_size, 7)
        self.assertEqual(stats.min_window_seen, 7)
        self.assertEqual(stats.max_window_seen, 7)

    def test_adaptive_mode_changes_window_and_still_delivers(self) -> None:
        ctrl = GrowShrinkController(start=1, cap=12)
        payload = bytes((i * 13) % 256 for i in range(6000))
        received, stats, rx = run_gbn_transfer(payload, window_size=1, mss=100,
                                               controller=ctrl)
        self.assertEqual(received, payload)
        self.assertEqual(rx.sha256, stats.sha256)
        self.assertEqual(stats.min_window_seen, 1)
        self.assertGreater(stats.max_window_seen, 4)       # the window really grew

    def test_adaptive_mode_survives_loss_with_shrinking_window(self) -> None:
        ctrl = GrowShrinkController(start=8, cap=12)
        payload = b"adaptive-loss" * 100
        received, stats, rx = run_gbn_transfer(payload, window_size=8, mss=40,
                                               timeout=0.12, loss_rate=0.3, seed=8,
                                               idle_timeout=40.0, controller=ctrl)
        self.assertEqual(received, payload)
        self.assertEqual(rx.sha256, stats.sha256)
        self.assertGreater(stats.timeouts, 0)
        self.assertLess(min(ctrl.history), 8)              # it shrank after a timeout

    def test_window_below_one_is_treated_as_one(self) -> None:
        class Zero(ReliabilityController):
            @property
            def window_size(self):
                return 0

        sender_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sender = GoBackNSender(sender_sock, ("127.0.0.1", 9), controller=Zero(1))
            self.assertEqual(sender.window_size, 1)
        finally:
            sender_sock.close()


# ---------------------------------------------------------------------------
# 4. Channel emulator swap point
# ---------------------------------------------------------------------------
class ChannelSwapPointTests(unittest.TestCase):
    def test_make_channel_has_the_three_calls_gbn_needs(self) -> None:
        from tests.harness import make_channel
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            channel = make_channel(raw, drop_prob=0.0, seed=1)
            for name in ("sendto", "recvfrom", "settimeout"):
                self.assertTrue(callable(getattr(channel, name)), name)
        finally:
            raw.close()


if __name__ == "__main__":
    unittest.main()
