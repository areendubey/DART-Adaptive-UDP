import unittest

from dart_transport.rtt import RtoConfig, RttEstimator


class RttTests(unittest.TestCase):
    def test_first_sample_uses_jacobson_karels_initialization(self) -> None:
        estimator = RttEstimator()
        estimator.record_send(1, 10.0)

        self.assertAlmostEqual(estimator.acknowledge(1, 10.2) or 0.0, 0.2)
        snapshot = estimator.snapshot()
        self.assertAlmostEqual(snapshot.smoothed_rtt_seconds or 0.0, 0.2)
        self.assertAlmostEqual(snapshot.rtt_variation_seconds or 0.0, 0.1)
        self.assertAlmostEqual(snapshot.rto_seconds, 0.6)

    def test_retransmitted_packet_is_not_an_rtt_sample_under_karn(self) -> None:
        estimator = RttEstimator()
        estimator.record_send(1, 0.0)
        estimator.record_retransmission(1, 1.0)

        self.assertIsNone(estimator.acknowledge(1, 1.1))
        self.assertEqual(estimator.snapshot().samples, 0)

    def test_timeout_backoff_respects_configured_maximum(self) -> None:
        estimator = RttEstimator(RtoConfig(initial_rto_seconds=1.0, maximum_rto_seconds=3.0))

        self.assertEqual(estimator.backoff_after_timeout(), 2.0)
        self.assertEqual(estimator.backoff_after_timeout(), 3.0)
        self.assertEqual(estimator.backoff_after_timeout(), 3.0)
