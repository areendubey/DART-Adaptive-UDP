import unittest

from dart_transport.adaptive import AdaptationAction, AdaptiveConfig, AdaptiveEngine, AdaptiveState
from dart_transport.metrics import TransferMetrics
from dart_transport.rtt import RttSnapshot


def _metrics(*, sent: int = 10, lost: int = 0, retransmissions: int = 0, timeouts: int = 0, srtt: float = 0.1, minimum: float = 0.1):
    metrics = TransferMetrics(started_at=0.0)
    metrics.packets_sent = sent
    metrics.packets_lost = lost
    metrics.retransmissions = retransmissions
    metrics.timeout_events = timeouts
    metrics.record_rtt(RttSnapshot(srtt, 0.01, 0.2, 1), minimum)
    return metrics.snapshot(now=1.0)


class AdaptiveTests(unittest.TestCase):
    def test_healthy_startup_gradually_increases_bounded_parameters(self) -> None:
        engine = AdaptiveEngine()

        decision = engine.observe(_metrics())

        self.assertIs(decision.state, AdaptiveState.STABLE)
        self.assertIs(decision.action, AdaptationAction.INCREASE)
        self.assertEqual(decision.window, 5)
        self.assertEqual(decision.rate_packets_per_second, 110.0)

    def test_loss_transitions_to_congestion_and_decreases_once_before_recovery(self) -> None:
        engine = AdaptiveEngine()
        engine.observe(_metrics())

        congested = engine.observe(_metrics(lost=2))
        recovery = engine.observe(_metrics())

        self.assertIs(congested.state, AdaptiveState.CONGESTION_DETECTED)
        self.assertIs(congested.action, AdaptationAction.DECREASE)
        self.assertIs(recovery.state, AdaptiveState.RECOVERY)
        self.assertIs(recovery.action, AdaptationAction.HOLD)

    def test_minimum_bounds_prevent_over_aggressive_decrease(self) -> None:
        config = AdaptiveConfig(initial_window=1, minimum_window=1, maximum_window=2, initial_rate_packets_per_second=10.0, minimum_rate_packets_per_second=10.0, maximum_rate_packets_per_second=20.0)
        engine = AdaptiveEngine(config)

        decision = engine.observe(_metrics(lost=10))

        self.assertEqual(decision.window, 1)
        self.assertEqual(decision.rate_packets_per_second, 10.0)
