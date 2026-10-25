import unittest

from dart_transport.adaptive import AdaptationAction, AdaptiveConfig, AdaptiveEngine
from dart_transport.integration import ControllerConfig, ReliabilityController, TransportMode
from dart_transport.metrics import TransferMetrics


class ReliabilityControllerTests(unittest.TestCase):
    def test_fixed_baseline_preserves_its_configured_parameters(self) -> None:
        controller = ReliabilityController(
            ControllerConfig(mode=TransportMode.P3_BASELINE, fixed_window=8, fixed_rate_packets_per_second=250.0),
            metrics=TransferMetrics(started_at=0.0),
        )
        controller.on_packet_sent(1, 0.0)
        controller.on_timeout()

        decision = controller.evaluate(now=1.0)

        self.assertIs(decision.action, AdaptationAction.HOLD)
        self.assertEqual(decision.window, 8)
        self.assertEqual(decision.rate_packets_per_second, 250.0)

    def test_dart_receives_arq_events_and_adapts_from_loss(self) -> None:
        engine = AdaptiveEngine(AdaptiveConfig(minimum_observation_packets=1))
        controller = ReliabilityController(
            ControllerConfig(mode=TransportMode.DART),
            adaptive_engine=engine,
            metrics=TransferMetrics(started_at=0.0),
        )
        controller.on_packet_sent(1, 0.0)
        controller.on_timeout()

        decision = controller.evaluate(now=1.0)

        self.assertIs(decision.action, AdaptationAction.DECREASE)
        snapshot = controller.snapshot(now=1.0)
        self.assertEqual(snapshot.timeout_events, 1)
        self.assertEqual(snapshot.packets_lost, 1)
        self.assertEqual(snapshot.adaptation_events, 1)

    def test_retransmitted_ack_updates_delivery_but_not_rtt(self) -> None:
        controller = ReliabilityController(metrics=TransferMetrics(started_at=0.0))
        controller.on_packet_sent(3, 0.0)
        controller.on_packet_sent(3, 1.0, retransmission=True)

        sample = controller.on_acknowledged(3, 1.2, payload_bytes=512)
        snapshot = controller.snapshot(now=2.0)

        self.assertIsNone(sample)
        self.assertEqual(snapshot.packets_delivered, 1)
        self.assertEqual(snapshot.rtt_samples, 0)
        self.assertEqual(snapshot.bytes_delivered, 512)
