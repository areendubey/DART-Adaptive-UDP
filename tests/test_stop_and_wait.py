"""
Integration Tests for DART Stop-and-Wait ARQ Protocol
Author: Kartikey Singh (Primary Owner)
"""

import unittest
import threading
import tempfile
import os
import hashlib
import time

from dart.arq.stop_and_wait import StopAndWaitSender, StopAndWaitReceiver
from dart.emulator.channel import EmulatorConfig
from dart.emulator.proxy import ChannelProxy
from experiments.benchmark import find_free_port


class TestStopAndWaitARQ(unittest.TestCase):

    def _run_transfer_test(self, test_data: bytes, emulator_config: EmulatorConfig) -> bool:
        """Helper that transfers data through the channel proxy and validates SHA-256."""
        src_sha = hashlib.sha256(test_data).hexdigest()

        with tempfile.NamedTemporaryFile(delete=False) as f_src, tempfile.NamedTemporaryFile(delete=False) as f_dst:
            src_path = f_src.name
            dst_path = f_dst.name
            f_src.write(test_data)
            f_src.flush()

        rx_port = find_free_port()
        proxy_port = find_free_port()

        proxy = ChannelProxy(
            listen_host="127.0.0.1",
            listen_port=proxy_port,
            target_host="127.0.0.1",
            target_port=rx_port,
            forward_config=emulator_config
        )
        proxy.start()

        rx = StopAndWaitReceiver(listen_host="127.0.0.1", listen_port=rx_port)
        tx = StopAndWaitSender(host="127.0.0.1", port=proxy_port, initial_rto=0.3, min_rto=0.05)

        rx_thread = threading.Thread(target=lambda: rx.receive_file(dst_path, timeout_seconds=25.0), daemon=True)

        try:
            rx_thread.start()
            time.sleep(0.05)

            tx_stats = tx.send_file(src_path)
            rx_thread.join(timeout=5.0)

            with open(dst_path, "rb") as f:
                dst_data = f.read()

            dst_sha = hashlib.sha256(dst_data).hexdigest()
            return src_sha == dst_sha

        finally:
            proxy.stop()
            if os.path.exists(src_path):
                os.remove(src_path)
            if os.path.exists(dst_path):
                os.remove(dst_path)

    def test_transfer_clean_channel(self):
        """Validates error-free file transfer on clean channel."""
        data = b"Testing DART Stop-and-Wait on perfectly clean channel. " * 50
        success = self._run_transfer_test(data, EmulatorConfig(loss_rate=0.0, seed=100))
        self.assertTrue(success, "Clean channel file integrity verification failed!")

    def test_transfer_lossy_channel(self):
        """Validates Stop-and-Wait reliably recovers dropped packets with 10% loss."""
        data = b"Testing DART Stop-and-Wait recovery over 10% packet drop rate. " * 40
        config = EmulatorConfig(loss_rate=0.10, seed=200)
        success = self._run_transfer_test(data, config)
        self.assertTrue(success, "Lossy channel transfer failed to recover all packets or hash mismatch!")

    def test_transfer_corrupted_channel(self):
        """Validates Stop-and-Wait detects checksum corruption and successfully recovers."""
        data = b"Testing DART Stop-and-Wait recovery over 10% corruption rate. " * 30
        config = EmulatorConfig(corruption_rate=0.10, seed=300)
        success = self._run_transfer_test(data, config)
        self.assertTrue(success, "Corrupted channel transfer failed to maintain file integrity!")

    def test_transfer_delayed_channel(self):
        """Validates Stop-and-Wait under propagation delay and jitter."""
        data = b"Testing DART Stop-and-Wait with propagation delay and jitter. " * 30
        config = EmulatorConfig(delay_mean_ms=10.0, delay_jitter_ms=2.0, seed=400)
        success = self._run_transfer_test(data, config)
        self.assertTrue(success, "Delayed channel transfer failed!")


if __name__ == "__main__":
    unittest.main()
