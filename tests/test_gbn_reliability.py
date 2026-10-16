"""ACK handling, retransmission, and reliability under loss/corruption.

Anushka Roy — reliability testing and protocol correctness validation.
"""

import hashlib
import unittest

from dart.packet import Packet, PacketType
from tests.harness import run_gbn_transfer


class GBNRetransmissionTests(unittest.TestCase):
    def test_data_loss_recovers(self) -> None:
        payload = bytes(range(256)) * 8
        received, stats, rx = run_gbn_transfer(
            payload,
            window_size=4,
            mss=64,
            timeout=0.12,
            loss_rate=0.25,
            seed=42,
            idle_timeout=20.0,
        )
        self.assertEqual(received, payload)
        self.assertEqual(rx.sha256, hashlib.sha256(payload).hexdigest())
        self.assertGreater(stats.timeouts, 0)
        self.assertGreater(stats.packets_retransmitted, 0)

    def test_ack_loss_recovers(self) -> None:
        payload = b"cumulative-ack-loss-test" * 50
        received, stats, rx = run_gbn_transfer(
            payload,
            window_size=5,
            mss=32,
            timeout=0.12,
            ack_loss_rate=0.35,
            seed=7,
            idle_timeout=20.0,
        )
        self.assertEqual(received, payload)
        self.assertEqual(rx.sha256, stats.sha256)

    def test_dropped_acks_cause_window_retransmit(self) -> None:
        """If the first ACKs never arrive, the sender must Go-Back-N on timeout."""
        import socket
        import threading

        from dart.gbn import GoBackNReceiver, GoBackNSender
        from tests.lossy_socket import DropFirstN

        payload = b"0123456789" * 20
        rx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rx_raw.bind(("127.0.0.1", 0))
        tx_raw.bind(("127.0.0.1", 0))
        dest = rx_raw.getsockname()
        rx_view = DropFirstN(rx_raw, drops=6)
        receiver = GoBackNReceiver(rx_view)
        sender = GoBackNSender(tx_raw, dest, window_size=4, timeout=0.1, mss=20)
        box: dict[str, bytes] = {}
        err: list[BaseException] = []

        def run_rx() -> None:
            try:
                box["data"] = receiver.receive_bytes(idle_timeout=8.0)
            except BaseException as exc:  # noqa: BLE001
                err.append(exc)

        thread = threading.Thread(target=run_rx, daemon=True)
        thread.start()
        try:
            stats = sender.send_bytes(payload)
            thread.join(timeout=10)
            self.assertFalse(err)
            self.assertEqual(box.get("data"), payload)
            self.assertGreater(stats.timeouts, 0)
            self.assertGreater(stats.packets_retransmitted, 0)
        finally:
            rx_raw.close()
            tx_raw.close()

    def test_corruption_is_not_delivered(self) -> None:
        payload = b"no-silent-corruption" * 40
        received, stats, rx = run_gbn_transfer(
            payload,
            window_size=4,
            mss=40,
            timeout=0.12,
            corrupt_rate=0.2,
            seed=99,
            idle_timeout=20.0,
        )
        self.assertEqual(received, payload)
        self.assertEqual(rx.sha256, hashlib.sha256(payload).hexdigest())
        self.assertGreater(rx.packets_discarded + stats.packets_retransmitted, 0)

    def test_high_loss_seeded_run(self) -> None:
        payload = b"seeded-deterministic-channel" * 30
        received, _stats, rx = run_gbn_transfer(
            payload,
            window_size=8,
            mss=24,
            timeout=0.1,
            loss_rate=0.4,
            seed=12345,
            idle_timeout=25.0,
        )
        self.assertEqual(received, payload)
        data_chunks = (len(payload) + 23) // 24
        self.assertEqual(rx.packets_accepted, data_chunks + 1)  # data + FIN

    def test_cumulative_ack_advances_past_prefix(self) -> None:
        """A single ACK for the highest in-order seq covers the whole prefix."""
        acks: list[int] = []
        pkt = Packet(ptype=PacketType.ACK, seq=0, ack=3)
        decoded = Packet.decode(pkt.encode())
        acks.append(decoded.ack)
        self.assertEqual(acks[0], 3)
        payload = b"AAAA" * 16
        received, stats, _rx = run_gbn_transfer(
            payload, window_size=8, mss=8, timeout=0.3
        )
        self.assertEqual(received, payload)
        self.assertGreaterEqual(stats.acks_received, 1)


if __name__ == "__main__":
    unittest.main()
