"""
End-to-End Integration Tests for DART Pluggable ARQs:
Stop-and-Wait, Go-Back-N, and Selective Repeat through the Channel Emulator.
"""

import unittest
import threading
import tempfile
import os
import hashlib
import time

from dart.arq import get_sender, get_receiver, ARQType
from dart.emulator.channel import EmulatorConfig
from dart.emulator.proxy import ChannelProxy
from experiments.benchmark import find_free_port
from dart.adaptive.engine import DARTAdaptiveEngine, FixedBaselineEngine


class TestDARTPluggableARQs(unittest.TestCase):

    def _run_protocol_transfer(
        self,
        arq_type: ARQType,
        test_data: bytes,
        emulator_config: EmulatorConfig,
        adaptive: bool = False
    ) -> bool:
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

        rx = get_receiver(arq_type, listen_host="127.0.0.1", listen_port=rx_port)

        engine = DARTAdaptiveEngine() if adaptive else FixedBaselineEngine(fixed_window=4)
        tx_kwargs = {"initial_rto": 0.25, "min_rto": 0.05}
        if arq_type != ARQType.STOP_AND_WAIT:
            tx_kwargs["adaptive_engine"] = engine
            tx_kwargs["window_size"] = 4

        tx = get_sender(arq_type, host="127.0.0.1", port=proxy_port, **tx_kwargs)

        rx_thread = threading.Thread(target=lambda: rx.receive_file(dst_path, timeout_seconds=15.0), daemon=True)

        try:
            rx_thread.start()
            time.sleep(0.04)

            tx_stats = tx.send_file(src_path)
            rx_thread.join(timeout=8.0)

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

    def test_stop_and_wait_clean_channel(self):
        data = b"Testing Stop-and-Wait ARQ over clean UDP channel. " * 20
        config = EmulatorConfig(seed=12)
        success = self._run_protocol_transfer(ARQType.STOP_AND_WAIT, data, config)
        self.assertTrue(success, "Stop-and-Wait clean channel transfer failed")

    def test_gbn_clean_channel(self):
        data = b"Testing Go-Back-N reliable transmission over clean UDP channel. " * 30
        config = EmulatorConfig(seed=42)
        success = self._run_protocol_transfer(ARQType.GO_BACK_N, data, config)
        self.assertTrue(success, "GBN clean channel transfer failed")

    def test_gbn_with_loss_and_recovery(self):
        data = b"Testing GBN under lossy channel with packet drops and timeouts. " * 20
        config = EmulatorConfig(loss_rate=0.15, delay_mean_ms=5.0, seed=101)
        success = self._run_protocol_transfer(ARQType.GO_BACK_N, data, config)
        self.assertTrue(success, "GBN lossy transfer failed")

    def test_sr_clean_channel(self):
        data = b"Testing Selective Repeat ARQ out-of-order buffering and selective ACKs. " * 30
        config = EmulatorConfig(seed=99)
        success = self._run_protocol_transfer(ARQType.SELECTIVE_REPEAT, data, config)
        self.assertTrue(success, "SR clean channel transfer failed")

    def test_sr_with_loss_and_reorder(self):
        data = b"Testing Selective Repeat with loss, corruption, and packet reordering. " * 25
        config = EmulatorConfig(loss_rate=0.10, reorder_rate=0.15, seed=777)
        success = self._run_protocol_transfer(ARQType.SELECTIVE_REPEAT, data, config)
        self.assertTrue(success, "SR lossy/reordered transfer failed")

    def test_dart_adaptive_mode(self):
        data = b"Testing DART Dynamic Adaptive Engine parameter scaling during transfer. " * 40
        config = EmulatorConfig(loss_rate=0.08, delay_mean_ms=10.0, seed=2026)
        success = self._run_protocol_transfer(ARQType.SELECTIVE_REPEAT, data, config, adaptive=True)
        self.assertTrue(success, "DART adaptive transfer failed")


if __name__ == "__main__":
    unittest.main()
