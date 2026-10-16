"""Targeted cumulative-ACK and out-of-order GBN behaviour."""

import socket
import threading
import time
import unittest

from dart.gbn import GoBackNReceiver, GoBackNSender
from dart.packet import Packet, PacketType


class OutOfOrderAndAckTests(unittest.TestCase):
    def test_out_of_order_data_is_discarded_until_gap_filled(self) -> None:
        rx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rx_raw.bind(("127.0.0.1", 0))
        tx_raw.bind(("127.0.0.1", 0))
        dest = rx_raw.getsockname()
        receiver = GoBackNReceiver(rx_raw)
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
            late = Packet(PacketType.DATA, seq=1, ack=0, payload=b"B").encode()
            tx_raw.sendto(late, dest)
            time.sleep(0.08)
            first = Packet(PacketType.DATA, seq=0, ack=0, payload=b"A").encode()
            tx_raw.sendto(first, dest)
            time.sleep(0.08)
            # seq=1 was discarded; it must be sent again after seq=0
            tx_raw.sendto(late, dest)
            time.sleep(0.08)
            fin = Packet(PacketType.FIN, seq=2, ack=0).encode()
            tx_raw.sendto(fin, dest)
            thread.join(timeout=5)
            self.assertFalse(err)
            self.assertEqual(box.get("data"), b"AB")
            self.assertGreaterEqual(receiver.packets_discarded, 1)
        finally:
            rx_raw.close()
            tx_raw.close()

    def test_cumulative_ack_releases_whole_prefix(self) -> None:
        rx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rx_raw.bind(("127.0.0.1", 0))
        tx_raw.bind(("127.0.0.1", 0))
        dest = rx_raw.getsockname()
        sender = GoBackNSender(tx_raw, dest, window_size=8, timeout=0.5, mss=8)
        # Three DATA packets then a cumulative ACK for seq=2.
        sender._send_data(0, b"0")
        sender._send_data(1, b"1")
        sender._send_data(2, b"2")
        sender._nextseq = 3
        ack = Packet(PacketType.ACK, seq=0, ack=2).encode()
        sender._handle_cumulative_ack(Packet.decode(ack).ack)
        self.assertEqual(sender._base, 3)
        self.assertEqual(sender._buffer, {})
        rx_raw.close()
        tx_raw.close()


if __name__ == "__main__":
    unittest.main()
